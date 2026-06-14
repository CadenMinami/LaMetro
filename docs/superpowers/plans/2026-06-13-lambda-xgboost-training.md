# Lambda XGBoost Training Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the quota-blocked SageMaker training-job state with a container-image Lambda that trains XGBoost and emits a SageMaker-compatible `model.tar.gz`, so the nightly pipeline produces a model at ~$0 and unblocks Phase 7c.

**Architecture:** One Step Functions state changes (`Train`: `sagemaker:createTrainingJob.sync` → `lambda:invoke` of a new `train_model` container Lambda). `evaluate_model` becomes source-agnostic (reads metric/URI from the event when present, else falls back to `describe_training_job`). The SageMaker training state stays in the CDK behind a `useSagemakerTraining` context flag for clean flip-back once AWS grants the quota.

**Tech Stack:** Python 3.12, xgboost 1.7.x (pinned to match the SageMaker XGBoost 1.7-1 inference container), numpy<2, boto3, AWS CDK v2 (TypeScript) `DockerImageFunction` (x86_64), pytest + `unittest.mock`.

**Spec:** `docs/superpowers/specs/2026-06-13-lambda-xgboost-training-design.md`

---

## File map

| File | Status | Responsibility |
|---|---|---|
| `lambdas/train_model/handler.py` | create | Parse CSV, split, train XGBoost, package `model.tar.gz`, S3 I/O, handler |
| `lambdas/train_model/requirements.txt` | create | `xgboost==1.7.*`, `numpy<2` |
| `lambdas/train_model/Dockerfile` | create | Container image for the Lambda (x86_64) |
| `lambdas/train_model/tests/test_handler.py` | create | Unit tests for the pure functions + handler |
| `lambdas/evaluate_model/handler.py` | modify | Accept `candidate_metric`/`candidate_model_uri` from event |
| `lambdas/evaluate_model/tests/test_handler.py` | modify | Test the event-driven branch |
| `cdk/lib/ml-stack.ts` | modify | Add `train_model` DockerImageFunction; flag-gated Train + Evaluate wiring |
| `.github/workflows/pr-checks.yml` | modify | Document container Lambda; keep it out of the zip-build loop |

**Conventions (match existing lambdas):**
- `from __future__ import annotations`; type hints everywhere.
- Module-level lazy boto3 client via `_s3()` / global singleton (see `promote_model/handler.py`).
- Tests import `from lambdas.train_model import handler` (pyproject sets `pythonpath = ["."]`, `--import-mode=importlib`). **No `__init__.py` in `tests/`.**
- Run tests with `pytest` from repo root.

---

## Task 1: `train_model` — CSV parsing + deterministic split

**Files:**
- Create: `lambdas/train_model/handler.py`
- Create: `lambdas/train_model/requirements.txt`
- Create: `lambdas/train_model/tests/test_handler.py`

- [ ] **Step 1: Create `requirements.txt`**

```
# Pinned to the SageMaker XGBoost 1.7-1 inference container 7c serves with,
# so a booster pickled here unpickles there. numpy<2 because xgboost 1.7
# predates numpy 2.0's C-API changes.
xgboost==1.7.6
numpy<2
```

- [ ] **Step 2: Write the failing tests**

```python
"""Unit tests for the train-model Lambda (Lambda XGBoost training fallback)."""

from __future__ import annotations

import gzip
import io
import pickle
import tarfile

import numpy as np

from lambdas.train_model import handler


def test_parse_training_csv_splits_label_first_column():
    raw = b"100.0,5,8,2,10.0,11.0,12.0,20.5,0.0\n150.0,6,9,3,11.0,12.0,13.0,21.0,0.1\n"
    X, y = handler.parse_training_csv(raw)
    assert y.tolist() == [100.0, 150.0]
    assert X.shape == (2, 8)
    assert X[0].tolist() == [5, 8, 2, 10.0, 11.0, 12.0, 20.5, 0.0]


def test_parse_training_csv_single_row():
    raw = b"100.0,5,8,2,10.0,11.0,12.0,20.5,0.0\n"
    X, y = handler.parse_training_csv(raw)
    assert X.shape == (1, 8)
    assert y.tolist() == [100.0]


def test_split_is_deterministic_and_both_sides_nonempty():
    X = np.arange(40, dtype=float).reshape(10, 4)
    y = np.arange(10, dtype=float)
    Xtr, ytr, Xval, yval = handler.split(X, y, frac=0.8, seed=0)
    assert len(ytr) == 8 and len(yval) == 2
    # Deterministic: same seed → same partition.
    Xtr2, ytr2, Xval2, yval2 = handler.split(X, y, frac=0.8, seed=0)
    assert np.array_equal(ytr, ytr2) and np.array_equal(yval, yval2)


def test_split_single_row_falls_back_to_train_as_val():
    X = np.array([[1.0, 2.0]])
    y = np.array([3.0])
    Xtr, ytr, Xval, yval = handler.split(X, y)
    assert len(ytr) == 1 and len(yval) == 1
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest lambdas/train_model/tests/test_handler.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'lambdas.train_model'`

- [ ] **Step 4: Implement `handler.py` (parsing + split only)**

```python
"""Train-model Lambda — Lambda XGBoost training fallback.

Replaces the SageMaker training job (account training quota is 0). Reads the
gzipped CSV the Athena UNLOAD writes to training-sets/run=<id>/, trains XGBoost,
and writes a SageMaker-XGBoost-compatible model.tar.gz to
training-jobs/run=<id>/output/. Returns the validation RMSE + artifact URI so
the existing evaluate/promote states are unchanged.
"""

from __future__ import annotations

import gzip
import io
import logging
import pickle
import tarfile
from typing import Any
from urllib.parse import urlparse

import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Mirrors the hyperparameters in the SageMaker Train state (ml-stack.ts).
HYPERPARAMS = {
    "objective": "reg:squarederror",
    "max_depth": 6,
    "eta": 0.1,
    "subsample": 0.8,
}
NUM_ROUND = 200
METRIC_NAME = "validation:rmse"

_s3_client = None


def _s3():
    global _s3_client
    if _s3_client is None:
        import boto3
        _s3_client = boto3.client("s3")
    return _s3_client


def _split_s3(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def parse_training_csv(raw: bytes) -> tuple[np.ndarray, np.ndarray]:
    """Header-less CSV, label in column 0; returns (X, y)."""
    arr = np.loadtxt(io.BytesIO(raw), delimiter=",")
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    return arr[:, 1:], arr[:, 0]


def split(
    X: np.ndarray, y: np.ndarray, *, frac: float = 0.8, seed: int = 0
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Deterministic train/validation split. Both sides non-empty when n>=2;
    single-row input evaluates on the training row (portfolio first-model guard).
    """
    n = X.shape[0]
    rng = np.random.RandomState(seed)
    idx = rng.permutation(n)
    if n < 2:
        return X, y, X, y
    cut = min(max(int(n * frac), 1), n - 1)
    tr, va = idx[:cut], idx[cut:]
    return X[tr], y[tr], X[va], y[va]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest lambdas/train_model/tests/test_handler.py -v`
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add lambdas/train_model/handler.py lambdas/train_model/requirements.txt lambdas/train_model/tests/test_handler.py
git commit -m "train_model: CSV parsing + deterministic split (TDD)"
```

---

## Task 2: `train_model` — train + package artifact

**Files:**
- Modify: `lambdas/train_model/handler.py`
- Modify: `lambdas/train_model/tests/test_handler.py`

- [ ] **Step 1: Add failing tests**

```python
def test_train_and_eval_returns_booster_and_float_rmse():
    rng = np.random.RandomState(1)
    X = rng.rand(60, 8)
    y = X[:, 0] * 100.0 + rng.rand(60)  # learnable signal
    Xtr, ytr, Xval, yval = handler.split(X, y, seed=0)
    booster, rmse = handler.train_and_eval(Xtr, ytr, Xval, yval, num_round=20)
    assert isinstance(rmse, float)
    assert rmse >= 0.0
    import xgboost as xgb
    assert isinstance(booster, xgb.Booster)


def test_package_model_contains_xgboost_model_and_round_trips():
    rng = np.random.RandomState(2)
    X = rng.rand(40, 8)
    y = X[:, 0] * 50.0
    Xtr, ytr, Xval, yval = handler.split(X, y, seed=0)
    booster, _ = handler.train_and_eval(Xtr, ytr, Xval, yval, num_round=10)

    blob = handler.package_model(booster)
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        names = tar.getnames()
        assert "xgboost-model" in names
        member = tar.extractfile("xgboost-model").read()
    import xgboost as xgb
    restored = pickle.loads(member)
    assert isinstance(restored, xgb.Booster)
    # Restored booster predicts (proves the pickle is a usable model).
    preds = restored.predict(xgb.DMatrix(Xval))
    assert preds.shape[0] == Xval.shape[0]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/train_model/tests/test_handler.py -k "train_and_eval or package_model" -v`
Expected: FAIL — `AttributeError: module ... has no attribute 'train_and_eval'`

- [ ] **Step 3: Implement `train_and_eval` + `package_model`**

Add to `handler.py`:

```python
def train_and_eval(
    Xtr: np.ndarray,
    ytr: np.ndarray,
    Xval: np.ndarray,
    yval: np.ndarray,
    *,
    params: dict | None = None,
    num_round: int = NUM_ROUND,
) -> tuple[Any, float]:
    """Train XGBoost; return (booster, validation RMSE)."""
    import xgboost as xgb

    dtrain = xgb.DMatrix(Xtr, label=ytr)
    dval = xgb.DMatrix(Xval, label=yval)
    booster = xgb.train(
        params or HYPERPARAMS,
        dtrain,
        num_boost_round=num_round,
        evals=[(dval, "validation")],
        verbose_eval=False,
    )
    preds = booster.predict(dval)
    rmse = float(np.sqrt(np.mean((preds - yval) ** 2)))
    return booster, rmse


def package_model(booster: Any) -> bytes:
    """Tar.gz containing the booster pickled as `xgboost-model` — the exact
    layout the SageMaker XGBoost inference container's default model_fn loads.
    """
    model_bytes = pickle.dumps(booster)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="xgboost-model")
        info.size = len(model_bytes)
        tar.addfile(info, io.BytesIO(model_bytes))
    return buf.getvalue()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest lambdas/train_model/tests/test_handler.py -k "train_and_eval or package_model" -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add lambdas/train_model/handler.py lambdas/train_model/tests/test_handler.py
git commit -m "train_model: XGBoost train + SageMaker-compatible model.tar.gz packaging (TDD)"
```

---

## Task 3: `train_model` — S3 read/write + `lambda_handler`

**Files:**
- Modify: `lambdas/train_model/handler.py`
- Modify: `lambdas/train_model/tests/test_handler.py`

- [ ] **Step 1: Add failing tests**

```python
def test_read_training_set_concatenates_gzipped_parts():
    from unittest.mock import MagicMock

    s3 = MagicMock()
    s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "training-sets/run=R/part-0.csv.gz"},
            {"Key": "training-sets/run=R/part-1.csv.gz"},
        ]
    }
    p0 = gzip.compress(b"1.0,2.0,3.0\n")
    p1 = gzip.compress(b"4.0,5.0,6.0\n")
    bodies = {"training-sets/run=R/part-0.csv.gz": p0,
              "training-sets/run=R/part-1.csv.gz": p1}

    def _get(Bucket, Key):
        body = MagicMock()
        body.read.return_value = bodies[Key]
        return {"Body": body}

    s3.get_object.side_effect = _get
    raw = handler._read_training_set(s3, "s3://bkt/training-sets/run=R/")
    X, y = handler.parse_training_csv(raw)
    assert y.tolist() == [1.0, 4.0]


def test_lambda_handler_trains_uploads_and_returns_metric(monkeypatch):
    from unittest.mock import MagicMock

    # Build a small learnable training set as gzipped CSV.
    rng = np.random.RandomState(3)
    rows = []
    for _ in range(60):
        feats = rng.rand(8)
        label = feats[0] * 100.0
        rows.append(",".join(str(v) for v in [label, *feats]))
    csv = ("\n".join(rows) + "\n").encode()

    s3 = MagicMock()
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "training-sets/run=R/p.csv.gz"}]}
    body = MagicMock()
    body.read.return_value = gzip.compress(csv)
    s3.get_object.return_value = {"Body": body}
    put_calls = {}
    s3.put_object.side_effect = lambda **kw: put_calls.update(kw)

    monkeypatch.setattr(handler, "_s3", lambda: s3)

    event = {
        "training_set_uri": "s3://bkt/training-sets/run=R/",
        "output_model_uri": "s3://bkt/training-jobs/run=R/output/model.tar.gz",
    }
    out = handler.lambda_handler(event, MagicMock())

    assert out["metric_name"] == "validation:rmse"
    assert isinstance(out["candidate_metric"], float)
    assert out["candidate_model_uri"] == "s3://bkt/training-jobs/run=R/output/model.tar.gz"
    # Uploaded to the right place, and the body is a valid tar.gz with the model.
    assert put_calls["Bucket"] == "bkt"
    assert put_calls["Key"] == "training-jobs/run=R/output/model.tar.gz"
    with tarfile.open(fileobj=io.BytesIO(put_calls["Body"]), mode="r:gz") as tar:
        assert "xgboost-model" in tar.getnames()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/train_model/tests/test_handler.py -k "read_training_set or lambda_handler" -v`
Expected: FAIL — `_read_training_set` / `lambda_handler` not defined.

- [ ] **Step 3: Implement `_read_training_set` + `lambda_handler`**

Add to `handler.py`:

```python
def _read_training_set(s3, prefix_uri: str) -> bytes:
    """List + read all CSV parts under the prefix; gunzip; concatenate."""
    bucket, prefix = _split_s3(prefix_uri)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    parts: list[bytes] = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if key.endswith("/"):
            continue
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        parts.append(body.rstrip(b"\n"))
    return b"\n".join(p for p in parts if p) + b"\n"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    training_set_uri = event["training_set_uri"]
    output_model_uri = event["output_model_uri"]

    s3 = _s3()
    raw = _read_training_set(s3, training_set_uri)
    X, y = parse_training_csv(raw)
    Xtr, ytr, Xval, yval = split(X, y)
    booster, rmse = train_and_eval(Xtr, ytr, Xval, yval)
    artifact = package_model(booster)

    out_bucket, out_key = _split_s3(output_model_uri)
    s3.put_object(
        Bucket=out_bucket,
        Key=out_key,
        Body=artifact,
        ContentType="application/x-tar",
    )

    result = {
        "candidate_metric": rmse,
        "candidate_model_uri": output_model_uri,
        "metric_name": METRIC_NAME,
    }
    logger.info(str(result))
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest lambdas/train_model/tests/test_handler.py -v`
Expected: PASS (all train_model tests)

- [ ] **Step 5: Commit**

```bash
git add lambdas/train_model/handler.py lambdas/train_model/tests/test_handler.py
git commit -m "train_model: S3 read/write + lambda_handler (TDD)"
```

---

## Task 4: `train_model` — Dockerfile (container image)

**Files:**
- Create: `lambdas/train_model/Dockerfile`
- Create: `lambdas/train_model/.dockerignore`

- [ ] **Step 1: Write the Dockerfile**

```dockerfile
# Container-image Lambda for XGBoost training. We use a container (not a zip
# Lambda) because xgboost + numpy exceed the 250 MB unzipped layer limit.
#
# x86_64 (default platform) on purpose:
#   - CI builds it natively (no QEMU emulation), and
#   - it binary-matches the x86 SageMaker XGBoost 1.7-1 inference container
#     that Phase 7c serves the pickled booster with.
#
# Base: AWS's Lambda Python 3.12 image — ships the Lambda Runtime Interface
# Client, so the container speaks the Lambda invoke API out of the box.
FROM public.ecr.aws/lambda/python:3.12

# Install deps into the Lambda task root. ${LAMBDA_TASK_ROOT} is /var/task.
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# App code.
COPY handler.py ${LAMBDA_TASK_ROOT}/

# CMD = "<module>.<function>". The base image's entrypoint runs this handler.
CMD [ "handler.lambda_handler" ]
```

- [ ] **Step 2: Write `.dockerignore`**

```
tests/
.build/
__pycache__/
*.pyc
```

- [ ] **Step 3: Build the image locally to verify it builds**

Run: `docker build --platform linux/amd64 -t la-metro-train-model lambdas/train_model`
Expected: build succeeds (final line `naming to docker.io/library/la-metro-train-model`). On Apple Silicon this builds under emulation — slow the first time, cached after. If Docker isn't running, start Docker Desktop first.

- [ ] **Step 4: Commit**

```bash
git add lambdas/train_model/Dockerfile lambdas/train_model/.dockerignore
git commit -m "train_model: container image Dockerfile (x86_64, xgboost)"
```

---

## Task 5: `evaluate_model` — source-agnostic candidate resolution

**Files:**
- Modify: `lambdas/evaluate_model/handler.py:90-120` (the `lambda_handler` body)
- Modify: `lambdas/evaluate_model/tests/test_handler.py`

- [ ] **Step 1: Add a failing test for the event-driven branch**

Append to `lambdas/evaluate_model/tests/test_handler.py`:

```python
def test_lambda_handler_uses_event_metric_without_describing_job(monkeypatch):
    # Lambda training path: metric + URI arrive in the event; SageMaker is
    # never called.
    sm = MagicMock()
    s3 = MagicMock()

    def _raise(*a, **k):
        raise type("NoSuchKey", (Exception,), {})()
    s3.get_object.side_effect = _raise  # no deployed model

    monkeypatch.setattr(handler, "_sagemaker", lambda: sm)
    monkeypatch.setattr(handler, "_s3", lambda: s3)

    event = {
        "candidate_metric": 73.5,
        "candidate_model_uri": "s3://bkt/training-jobs/run=R/output/model.tar.gz",
        "metric_name": "validation:rmse",
        "models_prefix_uri": "s3://bkt/models/",
    }
    out = handler.lambda_handler(event, MagicMock())
    assert out["promote"] is True
    assert out["candidate_metric"] == 73.5
    assert out["candidate_model_uri"].endswith("/model.tar.gz")
    sm.describe_training_job.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest lambdas/evaluate_model/tests/test_handler.py::test_lambda_handler_uses_event_metric_without_describing_job -v`
Expected: FAIL — current handler calls `describe_training_job` and KeyErrors on missing `training_job_name`.

- [ ] **Step 3: Update `lambda_handler` to resolve from the event first**

Replace the body of `lambda_handler` in `lambdas/evaluate_model/handler.py` (currently lines ~90–112) with:

```python
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    models_prefix_uri = event["models_prefix_uri"]

    # Lambda training path: the train_model Lambda hands us the metric + URI
    # directly. SageMaker flip-back path: only training_job_name is present,
    # so we describe the job to recover them.
    if event.get("candidate_metric") is not None:
        candidate_metric: float | None = float(event["candidate_metric"])
        candidate_model_uri = event.get("candidate_model_uri")
    else:
        job = _sagemaker().describe_training_job(
            TrainingJobName=event["training_job_name"]
        )
        candidate_metric = extract_validation_metric(job)
        candidate_model_uri = job.get("ModelArtifacts", {}).get("S3ModelArtifacts")

    deployed_metric = _read_deployed_metric(models_prefix_uri)

    promote = (
        candidate_metric is not None
        and candidate_model_uri is not None
        and should_promote(candidate_metric, deployed_metric)
    )

    result = {
        "promote": promote,
        "candidate_metric": candidate_metric,
        "deployed_metric": deployed_metric,
        "candidate_model_uri": candidate_model_uri,
        "metric_name": VALIDATION_METRIC_NAME,
    }
    logger.info(str(result))
    return result
```

- [ ] **Step 4: Run the full evaluate_model test suite**

Run: `pytest lambdas/evaluate_model/tests/test_handler.py -v`
Expected: PASS (existing SageMaker-path tests still pass + the new one)

- [ ] **Step 5: Commit**

```bash
git add lambdas/evaluate_model/handler.py lambdas/evaluate_model/tests/test_handler.py
git commit -m "evaluate_model: resolve candidate from event, fall back to DescribeTrainingJob"
```

---

## Task 6: CDK — `train_model` DockerImageFunction + flag-gated wiring

**Files:**
- Modify: `cdk/lib/ml-stack.ts`

- [ ] **Step 1: Add the imports + context flag (top of the constructor body)**

At the top of `cdk/lib/ml-stack.ts`, add the ecr-assets import after the existing imports (line ~13):

```typescript
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
```

Immediately inside `constructor(...)` after `super(scope, id, props);`, add:

```typescript
    // Flip-back switch: false (default) uses the container Lambda trainer;
    // true uses the managed SageMaker training job (needs training quota > 0).
    // Deploy with: cdk deploy -c useSagemakerTraining=true
    const useSagemakerTraining =
      this.node.tryGetContext('useSagemakerTraining') === true ||
      this.node.tryGetContext('useSagemakerTraining') === 'true';
```

- [ ] **Step 2: Define the `train_model` DockerImageFunction (Lambda mode only)**

Add this just BEFORE the `// ---- Step Functions state machine ----` comment (≈ line 273), so `trainFn` exists when we build the definition:

```typescript
    // ---- train_model container Lambda (XGBoost trainer) ----
    // Container image because xgboost+numpy exceed the zip layer limit.
    // x86_64 so CI builds natively and the artifact matches the x86 SageMaker
    // XGBoost inference container 7c serves it with. Only created in Lambda
    // mode — when flipped to SageMaker training we skip building the image.
    let trainFn: lambda.DockerImageFunction | undefined;
    if (!useSagemakerTraining) {
      const trainName = 'la-metro-train-model';
      const trainLog = new logs.LogGroup(this, 'TrainModelFnLogs', {
        logGroupName: `/aws/lambda/${trainName}`,
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      trainFn = new lambda.DockerImageFunction(this, 'TrainModelFn', {
        functionName: trainName,
        code: lambda.DockerImageCode.fromImageAsset(
          path.join(__dirname, '..', '..', 'lambdas', 'train_model'),
          { platform: Platform.LINUX_AMD64 },
        ),
        architecture: lambda.Architecture.X86_64,
        memorySize: 3008,            // headroom for xgboost; trains in seconds
        timeout: cdk.Duration.minutes(5),
        logGroup: trainLog,
        description: 'Lambda XGBoost trainer (SageMaker training quota fallback).',
      });
      props.archiveBucket.grantRead(trainFn, 'training-sets/*');
      props.archiveBucket.grantReadWrite(trainFn, 'training-jobs/*');
    }
```

- [ ] **Step 3: Build the flag-gated `Train` state + `Evaluate` payload**

Find the `definition` object (≈ line 303). Replace the `Train:` state and the `Evaluate:` state's `Payload` with flag-driven versions. Just BEFORE `const definition = {`, insert:

```typescript
    const sagemakerTrainState = {
      Type: 'Task',
      Resource: 'arn:aws:states:::sagemaker:createTrainingJob.sync',
      Parameters: {
        'TrainingJobName.$':
          "States.Format('la-metro-delay-{}', $.context.run_id)",
        AlgorithmSpecification: {
          TrainingImage: '746614075791.dkr.ecr.us-west-2.amazonaws.com/sagemaker-xgboost:1.7-1',
          TrainingInputMode: 'File',
          MetricDefinitions: [
            { Name: 'validation:rmse', Regex: '.*\\[.*\\]#011validation-rmse:([0-9\\.]+).*' },
            { Name: 'train:rmse',      Regex: '.*\\[.*\\]#011train-rmse:([0-9\\.]+).*' },
          ],
        },
        RoleArn: trainingRole.roleArn,
        ResourceConfig: { InstanceType: 'ml.m5.large', InstanceCount: 1, VolumeSizeInGB: 10 },
        StoppingCondition: { MaxRuntimeInSeconds: 600 },
        HyperParameters: {
          objective: 'reg:squarederror', num_round: '200',
          max_depth: '6', eta: '0.1', subsample: '0.8',
        },
        InputDataConfig: [{
          ChannelName: 'train',
          DataSource: {
            S3DataSource: {
              S3DataType: 'S3Prefix',
              'S3Uri.$':
                "States.Format('{}/training-sets/run={}/', '" +
                archiveBucketUri + "', $.context.run_id)",
              S3DataDistributionType: 'FullyReplicated',
            },
          },
          ContentType: 'text/csv',
          CompressionType: 'Gzip',
        }],
        OutputDataConfig: {
          'S3OutputPath.$':
            "States.Format('{}/training-jobs/run={}/', '" +
            archiveBucketUri + "', $.context.run_id)",
        },
      },
      ResultPath: '$.training',
      Next: 'Evaluate',
      Catch: [{ ErrorEquals: ['States.ALL'], Next: 'FailedTerminal' }],
    };

    const lambdaTrainState = {
      Type: 'Task',
      Resource: 'arn:aws:states:::lambda:invoke',
      Parameters: {
        FunctionName: trainFn ? trainFn.functionArn : '',
        Payload: {
          'training_set_uri.$':
            "States.Format('{}/training-sets/run={}/', '" +
            archiveBucketUri + "', $.context.run_id)",
          'output_model_uri.$':
            "States.Format('{}/training-jobs/run={}/output/model.tar.gz', '" +
            archiveBucketUri + "', $.context.run_id)",
        },
      },
      ResultSelector: { 'result.$': '$.Payload' },
      ResultPath: '$.training',
      Next: 'Evaluate',
      Catch: [{ ErrorEquals: ['States.ALL'], Next: 'FailedTerminal' }],
    };

    const trainState = useSagemakerTraining ? sagemakerTrainState : lambdaTrainState;

    const evaluatePayload = useSagemakerTraining
      ? {
          'training_job_name.$': '$.training.TrainingJobName',
          'models_prefix_uri': `${archiveBucketUri}/models/`,
        }
      : {
          'candidate_metric.$': '$.training.result.candidate_metric',
          'candidate_model_uri.$': '$.training.result.candidate_model_uri',
          'metric_name.$': '$.training.result.metric_name',
          'models_prefix_uri': `${archiveBucketUri}/models/`,
        };
```

Then in the `definition.States` object, replace the entire `Train: { ... }` literal with:

```typescript
        Train: trainState,
```

and replace the `Evaluate:` state's `Parameters` block with:

```typescript
          Parameters: {
            FunctionName: evaluateFn.functionArn,
            Payload: evaluatePayload,
          },
```

(Leave `ResultSelector`, `ResultPath: '$.eval'`, and `Next: 'BranchOnEval'` on the Evaluate state unchanged.)

- [ ] **Step 4: Grant the SFN role permission to invoke `train_model`**

In the `sfnRole` inline policy (≈ line 492), replace the `lambda:InvokeFunction` statement with one that conditionally includes `trainFn`:

```typescript
            new iam.PolicyStatement({
              actions: ['lambda:InvokeFunction'],
              resources: [
                sufficiencyFn.functionArn,
                evaluateFn.functionArn,
                promoteFn.functionArn,
                ...(trainFn ? [trainFn.functionArn] : []),
              ],
            }),
```

- [ ] **Step 5: Type-check and synth**

Run:
```bash
cd cdk && npx tsc --noEmit && npx cdk synth LaMetro-MLStack --quiet
```
Expected: no TS errors; synth succeeds. `cdk synth` builds the `train_model` Docker image (Docker must be running). The synthesized `Train` state should be a `lambda:invoke` of `la-metro-train-model`.

- [ ] **Step 6: Commit**

```bash
cd /Users/caden/awsProject
git add cdk/lib/ml-stack.ts
git commit -m "MLStack: train_model container Lambda + useSagemakerTraining flip-back flag"
```

---

## Task 7: CI — keep container Lambda out of the zip-build loop; ensure deps

**Files:**
- Modify: `.github/workflows/pr-checks.yml`

- [ ] **Step 1: Confirm the python job installs train_model deps**

The `python` job already loops `for f in lambdas/*/requirements.txt; do pip install -r "$f"; done`, which now installs `xgboost==1.7.6` + `numpy<2` so the train_model tests import cleanly. **No change needed** — verify by reading the step. (If xgboost install time is a concern, that's a future optimization, not a blocker.)

- [ ] **Step 2: Document that `train_model` is a container Lambda (not a zip asset)**

In the `cdk` job's "Build Lambda assets" step, update the comment above the `for d in ...` loop so a future reader knows the omission of `train_model` is deliberate:

```yaml
      - name: Build Lambda assets
        run: |
          chmod +x scripts/build-lambda.sh
          # train_model is intentionally absent: it's a container-image Lambda
          # (Dockerfile), built by `cdk synth` below, not a zip asset.
          for d in ingestion enrichment query_api aggregation websocket user_api post_confirmation feature_snapshot data_sufficiency_check evaluate_model promote_model; do
            scripts/build-lambda.sh "$d"
          done
```

- [ ] **Step 3: Verify the cdk job can build the image**

`cdk synth` in the `cdk` job builds the `train_model` image. GitHub `ubuntu-latest` has a running Docker daemon and the image is x86_64, so it builds natively. **No workflow change required** for this — confirm by re-reading the `cdk synth` step. (If a future run shows Docker missing, add `- uses: docker/setup-buildx-action@v3` before the synth step.)

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/pr-checks.yml
git commit -m "CI: document train_model as a container Lambda (excluded from zip build loop)"
```

---

## Task 8: Deploy + manual run verification

> Cost ≈ $0 (Lambda invoke of a few seconds; no SageMaker instance). Safe to run.

- [ ] **Step 1: Run the full test suite + synth once more**

```bash
cd /Users/caden/awsProject && pytest && cd cdk && npx tsc --noEmit && npx cdk synth LaMetro-MLStack --quiet
```
Expected: all green.

- [ ] **Step 2: Deploy MLStack (Lambda training mode is the default)**

```bash
cd /Users/caden/awsProject/cdk && npx cdk deploy LaMetro-MLStack --require-approval never
```
Expected: stack updates; the `Train` state is now `lambda:invoke`; `la-metro-train-model` function created from the image.

- [ ] **Step 3: Start an execution and watch it**

```bash
SM_ARN="arn:aws:states:us-west-2:087781607093:stateMachine:la-metro-nightly-training"
aws stepfunctions start-execution --state-machine-arn "$SM_ARN" --input '{}' --query 'executionArn' --output text
```
Poll `aws stepfunctions describe-execution --execution-arn <arn> --query status`.
Expected flow: `GenerateRunId → ExtractFeatures → CheckSufficiency (sufficient) → Train (lambda) → Evaluate → Promote → SUCCEEDED`.

- [ ] **Step 4: Verify the model artifacts landed**

```bash
B=lametro-storagestack-archivebucket9decbf5d-mg7byceonzyn
aws s3 ls s3://$B/models/current/
aws s3 ls s3://$B/models/ --recursive | grep 'v='
```
Expected: `models/current/model.tar.gz`, `models/current/metrics.json`, and `models/v=2026-06-13/model.tar.gz`.

- [ ] **Step 5: Confirm `metrics.json` content**

```bash
aws s3 cp s3://$B/models/current/metrics.json - 2>/dev/null
```
Expected JSON with `validation_metric` (the RMSE), `metric_name: validation:rmse`, `promoted_version: v=2026-06-13`.

- [ ] **Step 6: Mark the plan + spec done**

```bash
cd /Users/caden/awsProject
git add docs/superpowers/plans/2026-06-13-lambda-xgboost-training.md
git commit -m "Lambda XGBoost training: plan complete, model promoted to models/current/"
```

---

## Self-review notes (author)

- **Spec coverage:** Component 1 (train_model) → Tasks 1–4; Component 2 (evaluate_model source-agnostic) → Task 5; Component 3 (CDK + flag) → Task 6; Component 4 (CI/Docker) → Task 7; deploy/verify → Task 8. Error-handling (empty CSV → Train Catch; single-row split fallback; version pin) covered in Tasks 1/2/4.
- **Type/shape consistency:** train_model returns `{candidate_metric, candidate_model_uri, metric_name}`; the Lambda Evaluate payload reads exactly those off `$.training.result.*`; evaluate_model consumes `candidate_metric`/`candidate_model_uri`; promote_model (unchanged) consumes `candidate_model_uri`/`candidate_metric`/`metric_name`. `metrics.json` key is `validation_metric` (written by promote, read by evaluate) — unchanged.
- **Flip-back:** `useSagemakerTraining=true` selects `sagemakerTrainState` + the `training_job_name` evaluate payload; the existing SageMaker `trainingRole` and the SFN role's SageMaker actions remain in the stack, so no further edits are needed to flip back.
- **Placeholder check:** `lambdaTrainState.FunctionName` falls back to `''` only when `trainFn` is undefined (SageMaker mode), in which case `trainState` is the SageMaker state and `lambdaTrainState` is unused — so the empty string is never synthesized.
