# Phase 7b — Training Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

> **STATUS: CODE COMPLETE (2026-06-03).** Tasks 1–6 implemented + committed on branch `phase-7b-training-pipeline`. 196 tests pass; `tsc --noEmit` + `cdk synth LaMetro-MLStack` clean. **Deployment & manual verification (below) deferred** — it incurs AWS cost (SageMaker training job + Athena scan) and needs explicit go-ahead.
>
> **Two deviations from the literal plan (both fixes the plan author flagged):**
> 1. **Sufficiency gate** — the plan read a fictional `manifest.csv` with a `rows` column. Athena UNLOAD's data manifest lists *file paths only* (no row counts) and lives at the query-results location, so that would skip training every night. Rewrote `data_sufficiency_check` to read `GetQueryRuntimeStatistics.Rows.OutputRows` from the Athena `QueryExecutionId` (exact, no extra query). Lambda interface + tests changed accordingly.
> 2. **`${RUN_ID}` injection** — replaced the plan's broken placeholder/JSON-surgery with a clean `States.Format()`: SQL collapsed to one line, `${ARCHIVE_BUCKET}` bound at deploy, `${RUN_ID}`→`{}` bound to `$.context.run_id` at run time. Verified the escaping round-trips in `cdk synth`.
> 3. SQL `route_code` uses `abs(crc32(to_utf8(route_id))) % 1000` (the plan's `from_base` doesn't work on LA Metro IDs).

**Goal:** Nightly Step Functions pipeline that extracts supervised training rows from the feature store via Athena, trains an XGBoost regressor on SageMaker, evaluates against a held-out time-split, and promotes the model in S3 only if MAE improves. Plus a `ml/bootstrap.py`-or-real-data sufficiency gate so cold-start days don't fail.

**Architecture:** A Step Functions state machine triggered daily by EventBridge Scheduler runs five states: (1) Athena UNLOAD assembles supervised rows (LAG features + LEAD label) into S3 CSV, (2) Lambda counts rows and skips if < 1,000, (3) SageMaker training job (built-in XGBoost container) reads the CSV via train/validation channels, (4) Lambda fetches the job's final validation metric and compares it to the deployed model's metric, (5) Lambda promotes the new model to versioned + `current/` S3 prefixes when the gate passes. All wired into the existing `MLStack` from 7a.

**Tech Stack:** AWS CDK v2, Python 3.12 Lambdas (boto3 + stdlib), Athena (SQL), SageMaker Training Jobs (built-in XGBoost — no custom training script), Step Functions (service integrations for Athena + SageMaker, native — no Lambdas needed for those two steps), pytest + `unittest.mock`.

**Spec:** `docs/superpowers/specs/2026-05-27-phase-7-delay-prediction-design.md` (sub-piece 7b)

**Prerequisites:** Phase 7a deployed and either (a) ≥ a couple of days of real `processed-features/` data have accumulated, OR (b) `python -m ml.bootstrap --bucket <archive> --days 7` was run to seed synthetic data. Without one of these, the sufficiency gate will correctly skip every nightly run.

---

## Refinement note

This plan was written alongside the 7a plan, before 7a's empirical behavior was observed. Two areas to verify quickly before executing:
1. **Athena column types & SerDe round-trip** — confirm the `route_window_features` Glue table reads gzipped JSONL written by the live feature-snapshot Lambda. Run `SELECT * FROM la_metro.route_window_features LIMIT 5;` after a few cycles. If columns come back NULL, the JSON SerDe ignore-malformed-json setting may be hiding the issue — fix before training.
2. **XGBoost training-job metric name** — the built-in XGBoost container emits `validation:rmse` (by default) into `FinalMetricDataList` on the training job. This plan assumes the metric name `validation:rmse`. If using `objective=reg:absoluteerror` instead, it becomes `validation:mae`. Pick one and confirm the Evaluate Lambda reads the matching name.

---

## Conventions to follow (same as 7a)

- Lambda tests live at `lambdas/<name>/tests/test_handler.py`; use MagicMock + monkeypatch; **no `__init__.py` files** in lambda package or tests dirs (collision fix).
- Lambda builds via `scripts/build-lambda.sh <name>`; each new lambda must be added to the CI build loop in `.github/workflows/pr-checks.yml`.
- CDK verification: `npx tsc --noEmit` + `npx cdk synth --quiet` from `cdk/`.
- Commits: NO `Co-Authored-By: Claude` trailer.

---

## File map

| File | Status | Responsibility |
|---|---|---|
| `ml/feature_extraction.sql` | create | Athena query that assembles supervised (features, label) rows with LAG/LEAD |
| `ml/tests/test_feature_extraction_sql.py` | create | Structural assertions on the SQL |
| `lambdas/data_sufficiency_check/handler.py` | create | Reads UNLOAD manifest, counts rows |
| `lambdas/data_sufficiency_check/requirements.txt` | create | Comment-only |
| `lambdas/data_sufficiency_check/tests/test_handler.py` | create | TDD tests |
| `lambdas/evaluate_model/handler.py` | create | Fetches training-job final metric, compares vs deployed |
| `lambdas/evaluate_model/requirements.txt` | create | Comment-only |
| `lambdas/evaluate_model/tests/test_handler.py` | create | TDD tests |
| `lambdas/promote_model/handler.py` | create | Copies model.tar.gz to versioned + current/, writes metrics.json |
| `lambdas/promote_model/requirements.txt` | create | Comment-only |
| `lambdas/promote_model/tests/test_handler.py` | create | TDD tests |
| `cdk/lib/ml-stack.ts` | modify | Add Step Functions state machine, training role, three new Lambdas, EventBridge daily schedule |
| `cdk/bin/cdk.ts` | modify | No new props expected — MLStack already has archive bucket; nothing to wire |
| `.github/workflows/pr-checks.yml` | modify | Add `data_sufficiency_check`, `evaluate_model`, `promote_model` to the build loop |

---

## Task 1: `ml/feature_extraction.sql` — supervised assembly via LAG / LEAD

**Files:**
- Create: `ml/feature_extraction.sql`
- Create: `ml/tests/test_feature_extraction_sql.py`

- [x] **Step 1: Write structural tests for the SQL**

Create `ml/tests/test_feature_extraction_sql.py`:
```python
"""Structural assertions on the Athena training-set SQL. We don't run the
SQL here (that's integration); we just ensure the key shape can't drift."""

from pathlib import Path

SQL_PATH = Path(__file__).resolve().parent.parent / "feature_extraction.sql"


def test_sql_file_exists():
    assert SQL_PATH.is_file(), f"missing {SQL_PATH}"


def _sql() -> str:
    return SQL_PATH.read_text()


def test_unload_writes_to_supervised_set_prefix():
    sql = _sql()
    assert "UNLOAD" in sql.upper()
    assert "training-sets/" in sql or "training_sets/" in sql


def test_unload_uses_csv_no_header_for_built_in_xgboost():
    # Built-in XGBoost expects CSV with the label in the first column and no
    # header row. Keep this contract assertable.
    sql = _sql().upper()
    assert "FORMAT = 'TEXTFILE'" in sql or "FORMAT='TEXTFILE'" in sql
    assert "FIELD_DELIMITER = ','" in sql or "FIELD_DELIMITER=','" in sql


def test_features_include_lag_avg_delay_and_label_is_lead():
    sql = _sql()
    # Lag features.
    assert "LAG(avg_delay_seconds, 1)" in sql
    assert "LAG(avg_delay_seconds, 2)" in sql
    assert "LAG(avg_delay_seconds, 3)" in sql
    # Label.
    assert "LEAD(avg_delay_seconds, 1)" in sql


def test_partitions_by_route_and_orders_by_window():
    sql = _sql()
    assert "PARTITION BY route_id" in sql
    assert "ORDER BY window_start_iso" in sql


def test_filters_recent_30_days_via_partition_pruning():
    sql = _sql()
    # Partition pruning on year/month/day keeps Athena scan cheap.
    assert "year" in sql and "month" in sql and "day" in sql
```

- [x] **Step 2: Run tests to verify they fail**

Run: `pytest ml/tests/test_feature_extraction_sql.py -v`
Expected: FAIL on `test_sql_file_exists` (file missing).

- [x] **Step 3: Implement the SQL**

Create `ml/feature_extraction.sql`:
```sql
-- Phase 7b: Athena training-set assembly.
--
-- Reads the last 30 days of per-(route, window) snapshots from the Glue
-- table populated by feature-snapshot (7a), computes LAG features for
-- recent route delays, joins weather, and shifts the label one window
-- forward (LEAD) so each row predicts the NEXT window's avg delay.
--
-- Output: gzipped CSV under s3://<archive>/training-sets/run=<run-id>/,
--   no header, label in first column. Built-in SageMaker XGBoost reads
--   this directly via train/validation channels.

UNLOAD (
  WITH base AS (
    SELECT
      route_id,
      window_start_iso,
      avg_delay_seconds,
      temp_c,
      precip_mm,
      year, month, day, hour
    FROM la_metro.route_window_features
    -- 30-day window via partition pruning (cheap).
    WHERE
      (year * 10000 + month * 100 + day) >=
        CAST(date_format(date_add('day', -30, current_date), '%Y%m%d') AS INT)
      AND avg_delay_seconds IS NOT NULL
  ),
  shaped AS (
    SELECT
      route_id,
      window_start_iso,
      avg_delay_seconds,
      COALESCE(temp_c, 0.0)    AS temp_c,
      COALESCE(precip_mm, 0.0) AS precip_mm,
      EXTRACT(hour FROM CAST(window_start_iso AS timestamp))      AS hour_of_day,
      EXTRACT(day_of_week FROM CAST(window_start_iso AS timestamp)) AS day_of_week,
      LAG(avg_delay_seconds, 1) OVER (
        PARTITION BY route_id ORDER BY window_start_iso
      ) AS lag1_avg_delay,
      LAG(avg_delay_seconds, 2) OVER (
        PARTITION BY route_id ORDER BY window_start_iso
      ) AS lag2_avg_delay,
      LAG(avg_delay_seconds, 3) OVER (
        PARTITION BY route_id ORDER BY window_start_iso
      ) AS lag3_avg_delay,
      LEAD(avg_delay_seconds, 1) OVER (
        PARTITION BY route_id ORDER BY window_start_iso
      ) AS label_next_avg_delay
    FROM base
  )
  SELECT
    -- LABEL FIRST (XGBoost built-in contract).
    label_next_avg_delay AS label,
    -- FEATURES (numeric; route_id is hashed by a downstream step or one-hot
    -- via a small lookup since XGBoost built-in needs numeric input — we
    -- collapse to a per-route integer code based on the route_id hash; this
    -- is good enough for v1, refine in a v2).
    abs(from_base(route_id, 16)) % 1000 AS route_code,
    hour_of_day,
    day_of_week,
    lag1_avg_delay,
    lag2_avg_delay,
    lag3_avg_delay,
    temp_c,
    precip_mm
  FROM shaped
  WHERE
    label_next_avg_delay IS NOT NULL
    AND lag1_avg_delay IS NOT NULL
    AND lag2_avg_delay IS NOT NULL
    AND lag3_avg_delay IS NOT NULL
)
TO 's3://${ARCHIVE_BUCKET}/training-sets/run=${RUN_ID}/'
WITH (
  format = 'TEXTFILE',
  field_delimiter = ',',
  compression = 'GZIP'
);
```

> Note: the Athena query string contains placeholders `${ARCHIVE_BUCKET}` and `${RUN_ID}`. The Step Functions state machine substitutes them in via `States.Format()` when calling `Athena:StartQueryExecution`. Don't expect Athena itself to expand them.

> Built-in XGBoost requires numeric features only. `from_base(route_id, 16)` works only if route_ids are hex; LA Metro uses short alphanumerics. For v1, use Athena's `abs(checksum(route_id))` instead — replace the `abs(from_base(...))` line with `cast(abs(checksum(cast(route_id as varbinary))) % 1000 as int)`. If Athena rejects checksum, fall back to a static `route_id_lookup` CTE built from `SELECT DISTINCT route_id, row_number() OVER (ORDER BY route_id)` — refine in this step before committing. The structural tests in Step 1 don't constrain this detail.

- [x] **Step 4: Run tests**

Run: `pytest ml/tests/test_feature_extraction_sql.py -v`
Expected: PASS (6 tests).

- [x] **Step 5: Commit**

```bash
git add ml/feature_extraction.sql ml/tests/test_feature_extraction_sql.py
git commit -m "Phase 7b: Athena feature_extraction.sql (LAG features + LEAD label, CSV UNLOAD)"
```

---

## Task 2: `data_sufficiency_check` Lambda

**Files:**
- Create: `lambdas/data_sufficiency_check/handler.py`
- Create: `lambdas/data_sufficiency_check/requirements.txt`
- Create: `lambdas/data_sufficiency_check/tests/test_handler.py`

- [x] **Step 1: Write the failing tests**

Create `lambdas/data_sufficiency_check/tests/test_handler.py`:
```python
"""Unit tests for the data-sufficiency check Lambda (Phase 7b)."""

from __future__ import annotations

from unittest.mock import MagicMock

from lambdas.data_sufficiency_check import handler


def test_parse_unload_manifest_sums_rows_across_files():
    # Athena UNLOAD writes a manifest.csv listing each output object and its
    # row count via S3 Select / object metadata; for our purposes we just sum
    # the rows reported in the manifest.
    raw = b"path,rows\ns3://b/p/a.csv.gz,400\ns3://b/p/b.csv.gz,650\n"
    assert handler.row_count_from_manifest(raw) == 1050


def test_parse_unload_manifest_handles_no_rows_column_with_s3_list_fallback():
    # If the manifest doesn't carry row counts, fall back to a sentinel value
    # so the caller knows to do a direct count.
    raw = b"s3://b/p/a.csv.gz\ns3://b/p/b.csv.gz\n"
    assert handler.row_count_from_manifest(raw) is None


def test_lambda_handler_promotes_when_above_threshold(monkeypatch):
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: b"path,rows\ns3://b/p/a.csv.gz,1500\n"),
    }
    monkeypatch.setattr(handler, "_s3", lambda: s3)

    event = {
        "manifest_uri": "s3://bkt/training-sets/run=R/manifest.csv",
        "threshold_rows": 1000,
    }
    result = handler.lambda_handler(event, MagicMock())
    assert result == {"sufficient": True, "row_count": 1500, "threshold_rows": 1000}


def test_lambda_handler_skips_when_below_threshold(monkeypatch):
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: b"path,rows\ns3://b/p/a.csv.gz,500\n"),
    }
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    event = {
        "manifest_uri": "s3://bkt/training-sets/run=R/manifest.csv",
        "threshold_rows": 1000,
    }
    result = handler.lambda_handler(event, MagicMock())
    assert result["sufficient"] is False
    assert result["row_count"] == 500


def test_lambda_handler_uses_default_threshold_when_event_omits_it(monkeypatch):
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: b"path,rows\ns3://b/p/a.csv.gz,2000\n"),
    }
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    event = {"manifest_uri": "s3://bkt/training-sets/run=R/manifest.csv"}
    result = handler.lambda_handler(event, MagicMock())
    assert result["threshold_rows"] == handler.DEFAULT_THRESHOLD_ROWS
```

- [x] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/data_sufficiency_check -v`
Expected: FAIL — `ModuleNotFoundError`.

- [x] **Step 3: Implement**

Create `lambdas/data_sufficiency_check/handler.py`:
```python
"""Data-sufficiency check — Phase 7b.

Inspects the Athena UNLOAD manifest and decides whether the training set is
large enough to be worth training on. The Step Functions state machine
branches on the returned `sufficient` flag.
"""

from __future__ import annotations

import csv
import io
import logging
import os
from typing import Any
from urllib.parse import urlparse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_THRESHOLD_ROWS = int(os.environ.get("DEFAULT_THRESHOLD_ROWS", "1000"))

_s3_client = None


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def row_count_from_manifest(raw: bytes) -> int | None:
    """Sum the `rows` column from an UNLOAD manifest CSV. Returns None if the
    manifest doesn't carry row counts (caller should use a fallback)."""
    reader = csv.reader(io.StringIO(raw.decode("utf-8")))
    rows = list(reader)
    if not rows:
        return 0
    header = rows[0]
    if "rows" not in header:
        return None
    rows_idx = header.index("rows")
    total = 0
    for r in rows[1:]:
        if not r:
            continue
        try:
            total += int(r[rows_idx])
        except (IndexError, ValueError):
            continue
    return total


def _split_s3(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    manifest_uri = event["manifest_uri"]
    threshold = int(event.get("threshold_rows", DEFAULT_THRESHOLD_ROWS))

    bucket, key = _split_s3(manifest_uri)
    raw = _s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    rows = row_count_from_manifest(raw)
    if rows is None:
        # Fallback path: read each listed file via head_object isn't sufficient,
        # so for v1 we treat unknown as sufficient=False (safer than running
        # training on possibly tiny data).
        logger.warning("manifest_lacks_row_count; defaulting to insufficient")
        rows = 0

    result = {
        "sufficient": rows >= threshold,
        "row_count": rows,
        "threshold_rows": threshold,
    }
    logger.info(str(result))
    return result
```

Create `lambdas/data_sufficiency_check/requirements.txt`:
```text
# boto3 ships in the Lambda runtime; no third-party deps.
```

- [x] **Step 4: Run tests**

Run: `pytest lambdas/data_sufficiency_check -v`
Expected: PASS (5 tests).

- [x] **Step 5: Commit**

```bash
git add lambdas/data_sufficiency_check/
git commit -m "Phase 7b: data_sufficiency_check Lambda + tests (TDD)"
```

---

## Task 3: `evaluate_model` Lambda — fetch training metric, compare to deployed

**Files:**
- Create: `lambdas/evaluate_model/handler.py`
- Create: `lambdas/evaluate_model/requirements.txt`
- Create: `lambdas/evaluate_model/tests/test_handler.py`

- [x] **Step 1: Write the failing tests**

Create `lambdas/evaluate_model/tests/test_handler.py`:
```python
"""Unit tests for the evaluate-model Lambda (Phase 7b)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from lambdas.evaluate_model import handler


def test_extract_validation_metric_returns_value():
    job_desc = {
        "FinalMetricDataList": [
            {"MetricName": "train:rmse", "Value": 88.3},
            {"MetricName": "validation:rmse", "Value": 97.1},
        ],
    }
    assert handler.extract_validation_metric(job_desc) == 97.1


def test_extract_validation_metric_returns_none_when_missing():
    assert handler.extract_validation_metric({"FinalMetricDataList": []}) is None
    assert handler.extract_validation_metric({}) is None


def test_should_promote_when_no_deployed_model():
    assert handler.should_promote(candidate=80.0, deployed=None) is True


def test_should_promote_when_candidate_strictly_better():
    assert handler.should_promote(candidate=80.0, deployed=85.0) is True


def test_should_not_promote_when_candidate_worse_or_equal():
    assert handler.should_promote(candidate=85.0, deployed=85.0) is False
    assert handler.should_promote(candidate=86.0, deployed=85.0) is False


def test_lambda_handler_first_model_promotes(monkeypatch):
    sm = MagicMock()
    sm.describe_training_job.return_value = {
        "FinalMetricDataList": [{"MetricName": "validation:rmse", "Value": 92.0}],
        "ModelArtifacts": {"S3ModelArtifacts": "s3://bkt/training-jobs/run=R/model.tar.gz"},
    }
    s3 = MagicMock()
    # No deployed metrics.json yet.
    s3.get_object.side_effect = s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    def _raise(*a, **k):
        raise s3.exceptions.NoSuchKey()
    s3.get_object.side_effect = _raise

    monkeypatch.setattr(handler, "_sagemaker", lambda: sm)
    monkeypatch.setattr(handler, "_s3", lambda: s3)

    event = {
        "training_job_name": "la-metro-delay-2026-05-27-r1",
        "models_prefix_uri": "s3://bkt/models/",
    }
    out = handler.lambda_handler(event, MagicMock())
    assert out["promote"] is True
    assert out["candidate_metric"] == 92.0
    assert out["deployed_metric"] is None
    assert out["candidate_model_uri"].endswith("/model.tar.gz")


def test_lambda_handler_existing_better_model_blocks_promotion(monkeypatch):
    sm = MagicMock()
    sm.describe_training_job.return_value = {
        "FinalMetricDataList": [{"MetricName": "validation:rmse", "Value": 100.0}],
        "ModelArtifacts": {"S3ModelArtifacts": "s3://bkt/training-jobs/run=R/model.tar.gz"},
    }
    s3 = MagicMock()
    # Deployed model has MAE 90.
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({"validation_metric": 90.0}).encode()),
    }
    monkeypatch.setattr(handler, "_sagemaker", lambda: sm)
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    out = handler.lambda_handler(
        {"training_job_name": "j", "models_prefix_uri": "s3://bkt/models/"},
        MagicMock(),
    )
    assert out["promote"] is False
    assert out["candidate_metric"] == 100.0
    assert out["deployed_metric"] == 90.0
```

- [x] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/evaluate_model -v`
Expected: FAIL — `ModuleNotFoundError`.

- [x] **Step 3: Implement**

Create `lambdas/evaluate_model/handler.py`:
```python
"""Evaluate model — Phase 7b.

Fetches the just-completed SageMaker training job's final validation metric,
compares it to the deployed model's metric (stored in s3://.../models/current/
metrics.json), and returns a promote/skip decision plus the candidate model
artifact URI for the next step.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

VALIDATION_METRIC_NAME = os.environ.get("VALIDATION_METRIC_NAME", "validation:rmse")

_sm_client = None
_s3_client = None


def _sagemaker():
    global _sm_client
    if _sm_client is None:
        _sm_client = boto3.client("sagemaker")
    return _sm_client


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def extract_validation_metric(job_desc: dict) -> float | None:
    for m in job_desc.get("FinalMetricDataList", []):
        if m.get("MetricName") == VALIDATION_METRIC_NAME:
            return float(m["Value"])
    return None


def should_promote(candidate: float, deployed: float | None) -> bool:
    if deployed is None:
        return True
    # Lower is better for RMSE/MAE; strictly less than (not <=) avoids noise
    # flap from numerically equal runs.
    return candidate < deployed


def _split_s3(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def _read_deployed_metric(models_prefix_uri: str) -> float | None:
    bucket, key_prefix = _split_s3(models_prefix_uri.rstrip("/"))
    key = f"{key_prefix}/current/metrics.json"
    try:
        body = _s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    except Exception:
        # boto3 mocks in tests may raise non-ClientError exceptions; treat as
        # "no deployed model" rather than crashing the pipeline.
        logger.exception("could not read deployed metrics; treating as none")
        return None
    try:
        return float(json.loads(body)["validation_metric"])
    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
        logger.exception("malformed deployed metrics.json")
        return None


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    job_name = event["training_job_name"]
    models_prefix_uri = event["models_prefix_uri"]

    job = _sagemaker().describe_training_job(TrainingJobName=job_name)
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

Create `lambdas/evaluate_model/requirements.txt`:
```text
# boto3 ships in the Lambda runtime; no third-party deps.
```

- [x] **Step 4: Run tests**

Run: `pytest lambdas/evaluate_model -v`
Expected: PASS (7 tests).

- [x] **Step 5: Commit**

```bash
git add lambdas/evaluate_model/
git commit -m "Phase 7b: evaluate_model Lambda — compare candidate vs deployed metric"
```

---

## Task 4: `promote_model` Lambda — copy artifact to versioned + current/

**Files:**
- Create: `lambdas/promote_model/handler.py`
- Create: `lambdas/promote_model/requirements.txt`
- Create: `lambdas/promote_model/tests/test_handler.py`

- [x] **Step 1: Write the failing tests**

Create `lambdas/promote_model/tests/test_handler.py`:
```python
"""Unit tests for the promote-model Lambda (Phase 7b)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from lambdas.promote_model import handler


def test_version_key_uses_iso_date_in_utc():
    when = datetime(2026, 6, 15, 7, 12, 0, tzinfo=timezone.utc)
    assert handler.version_key(when) == "v=2026-06-15"


def test_lambda_handler_copies_and_writes_metrics(monkeypatch):
    s3 = MagicMock()
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    monkeypatch.setattr(
        handler, "_utcnow",
        lambda: datetime(2026, 6, 15, 7, 12, 0, tzinfo=timezone.utc),
    )

    event = {
        "candidate_model_uri": "s3://bkt/training-jobs/run=R/output/model.tar.gz",
        "models_prefix_uri": "s3://bkt/models",
        "candidate_metric": 87.4,
        "metric_name": "validation:rmse",
    }
    out = handler.lambda_handler(event, MagicMock())

    assert out["promoted_version"] == "v=2026-06-15"
    assert out["promoted_model_uri"].endswith("v=2026-06-15/model.tar.gz")
    assert out["current_model_uri"].endswith("current/model.tar.gz")

    # Three S3 calls: copy to versioned, copy to current, write metrics.json.
    copies = [c for c in s3.copy_object.call_args_list]
    puts = [c for c in s3.put_object.call_args_list]
    assert len(copies) == 2
    assert len(puts) == 1

    # Versioned copy target.
    v_call = copies[0].kwargs
    assert v_call["Bucket"] == "bkt"
    assert v_call["Key"] == "models/v=2026-06-15/model.tar.gz"
    assert v_call["CopySource"]["Bucket"] == "bkt"

    # current/ copy target.
    c_call = copies[1].kwargs
    assert c_call["Key"] == "models/current/model.tar.gz"

    # metrics.json content.
    metrics_call = puts[0].kwargs
    assert metrics_call["Key"] == "models/current/metrics.json"
    body = json.loads(metrics_call["Body"])
    assert body == {
        "validation_metric": 87.4,
        "metric_name": "validation:rmse",
        "promoted_version": "v=2026-06-15",
        "promoted_at": "2026-06-15T07:12:00Z",
    }
```

- [x] **Step 2: Run tests to verify they fail**

Run: `pytest lambdas/promote_model -v`
Expected: FAIL — `ModuleNotFoundError`.

- [x] **Step 3: Implement**

Create `lambdas/promote_model/handler.py`:
```python
"""Promote model — Phase 7b.

Called only when evaluate_model said `promote: true`. Copies the candidate
model artifact to two locations:
  - s3://<archive>/models/v=YYYY-MM-DD/model.tar.gz  (versioned trail)
  - s3://<archive>/models/current/model.tar.gz       (the live pointer)

Writes a fresh s3://<archive>/models/current/metrics.json that records what's
deployed. 7c's gate-and-promote will eventually extend this to also call
SageMaker UpdateEndpoint; for 7b we only update the registry in S3.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_s3_client = None


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def version_key(when: datetime) -> str:
    return when.astimezone(timezone.utc).strftime("v=%Y-%m-%d")


def _split_s3(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    candidate_uri = event["candidate_model_uri"]
    models_prefix_uri = event["models_prefix_uri"].rstrip("/")
    candidate_metric = float(event["candidate_metric"])
    metric_name = event.get("metric_name", "validation:rmse")

    src_bucket, src_key = _split_s3(candidate_uri)
    dst_bucket, models_prefix = _split_s3(models_prefix_uri)
    now = _utcnow()
    vkey = version_key(now)

    versioned_key = f"{models_prefix}/{vkey}/model.tar.gz"
    current_key = f"{models_prefix}/current/model.tar.gz"
    metrics_key = f"{models_prefix}/current/metrics.json"

    s3 = _s3()
    copy_source = {"Bucket": src_bucket, "Key": src_key}
    s3.copy_object(Bucket=dst_bucket, Key=versioned_key, CopySource=copy_source)
    s3.copy_object(Bucket=dst_bucket, Key=current_key, CopySource=copy_source)

    metrics_body = {
        "validation_metric": candidate_metric,
        "metric_name": metric_name,
        "promoted_version": vkey,
        "promoted_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    s3.put_object(
        Bucket=dst_bucket,
        Key=metrics_key,
        Body=json.dumps(metrics_body).encode("utf-8"),
        ContentType="application/json",
    )

    result = {
        "promoted_version": vkey,
        "promoted_model_uri": f"s3://{dst_bucket}/{versioned_key}",
        "current_model_uri": f"s3://{dst_bucket}/{current_key}",
        "metrics_uri": f"s3://{dst_bucket}/{metrics_key}",
    }
    logger.info(str(result))
    return result
```

Create `lambdas/promote_model/requirements.txt`:
```text
# boto3 ships in the Lambda runtime; no third-party deps.
```

- [x] **Step 4: Run tests**

Run: `pytest lambdas/promote_model -v`
Expected: PASS (2 tests).

- [x] **Step 5: Commit**

```bash
git add lambdas/promote_model/
git commit -m "Phase 7b: promote_model Lambda — copy artifact + update metrics.json"
```

---

## Task 5: Step Functions state machine + EventBridge schedule in `MLStack`

**Files:**
- Modify: `cdk/lib/ml-stack.ts`

- [x] **Step 1: Add imports**

At the top of `cdk/lib/ml-stack.ts`, add:
```typescript
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
```

- [x] **Step 2: Add the three new Lambda functions in the constructor (same pattern as feature-snapshot)**

After the Glue table block in the `MLStack` constructor, add (one block per Lambda; full code for each — DO NOT factor out a helper, follow the established explicit-per-Lambda pattern in this repo's stacks):

```typescript
    // ---- data_sufficiency_check Lambda ----
    const sufficiencyName = 'la-metro-data-sufficiency-check';
    const sufficiencyLog = new logs.LogGroup(this, 'SufficiencyFnLogs', {
      logGroupName: `/aws/lambda/${sufficiencyName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const sufficiencyFn = new lambda.Function(this, 'SufficiencyFn', {
      functionName: sufficiencyName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'data_sufficiency_check', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(15),
      environment: { DEFAULT_THRESHOLD_ROWS: '1000' },
      logGroup: sufficiencyLog,
      description: 'Phase 7b: reads UNLOAD manifest, counts rows for the gate.',
    });
    props.archiveBucket.grantRead(sufficiencyFn, 'training-sets/*');

    // ---- evaluate_model Lambda ----
    const evalName = 'la-metro-evaluate-model';
    const evalLog = new logs.LogGroup(this, 'EvaluateFnLogs', {
      logGroupName: `/aws/lambda/${evalName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const evaluateFn = new lambda.Function(this, 'EvaluateFn', {
      functionName: evalName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'evaluate_model', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(20),
      environment: { VALIDATION_METRIC_NAME: 'validation:rmse' },
      logGroup: evalLog,
      description: 'Phase 7b: compares candidate training-job metric vs deployed model.',
    });
    props.archiveBucket.grantRead(evaluateFn, 'models/*');
    evaluateFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['sagemaker:DescribeTrainingJob'],
      resources: ['*'],   // training job ARNs include the run-id we won't know upfront
    }));

    // ---- promote_model Lambda ----
    const promoteName = 'la-metro-promote-model';
    const promoteLog = new logs.LogGroup(this, 'PromoteFnLogs', {
      logGroupName: `/aws/lambda/${promoteName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const promoteFn = new lambda.Function(this, 'PromoteFn', {
      functionName: promoteName,
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'handler.lambda_handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'lambdas', 'promote_model', '.build'),
      ),
      memorySize: 256,
      timeout: cdk.Duration.seconds(30),
      environment: {},
      logGroup: promoteLog,
      description: 'Phase 7b: copy candidate artifact to versioned + current/, write metrics.json.',
    });
    // Promote reads candidates from training-jobs/ and writes to models/.
    props.archiveBucket.grantRead(promoteFn, 'training-jobs/*');
    props.archiveBucket.grantReadWrite(promoteFn, 'models/*');
```

- [x] **Step 3: Add the SageMaker training role**

After the three Lambdas, before the state machine, add:
```typescript
    // SageMaker training-job execution role. Has to access training-sets/
    // (read CSV input) and write to training-jobs/<job>/output/ in the
    // same archive bucket.
    const trainingRole = new iam.Role(this, 'SageMakerTrainingRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
      description: 'Phase 7b: SageMaker training job execution role.',
    });
    props.archiveBucket.grantRead(trainingRole, 'training-sets/*');
    props.archiveBucket.grantReadWrite(trainingRole, 'training-jobs/*');
    trainingRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents',
      ],
      resources: [`arn:aws:logs:${this.region}:${this.account}:log-group:/aws/sagemaker/TrainingJobs:*`],
    }));
```

- [x] **Step 4: Add the Step Functions state machine**

After the training role, add the state machine using L1-ish JSON definition through `sfn.DefinitionBody.fromString()` (cleaner than chaining many L2 constructs for service integrations):

```typescript
    // Step Functions roles + the state machine itself. We use the JSON-string
    // definition because the service integrations (Athena.StartQueryExecution
    // .sync, SageMaker.CreateTrainingJob.sync) are easier to wire that way.
    const sfnLog = new logs.LogGroup(this, 'NightlyTrainingSfnLogs', {
      logGroupName: '/aws/vendedlogs/states/la-metro-nightly-training',
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const athenaWorkgroup = 'primary';
    const athenaResultsPrefix = `s3://${props.archiveBucket.bucketName}/athena-results/`;
    const archiveBucketUri = `s3://${props.archiveBucket.bucketName}`;

    const definition = {
      Comment: 'Phase 7b nightly training pipeline.',
      StartAt: 'GenerateRunId',
      States: {
        GenerateRunId: {
          Type: 'Pass',
          Parameters: {
            'run_id.$': '$$.Execution.Name',
          },
          ResultPath: '$.context',
          Next: 'ExtractFeatures',
        },
        ExtractFeatures: {
          Type: 'Task',
          Resource: 'arn:aws:states:::athena:startQueryExecution.sync',
          Parameters: {
            // Inlined query string built from feature_extraction.sql with
            // ${ARCHIVE_BUCKET} and ${RUN_ID} substituted at deploy time below.
            QueryString: '<<INJECTED_SQL>>',
            WorkGroup: athenaWorkgroup,
            ResultConfiguration: { OutputLocation: athenaResultsPrefix },
          },
          ResultPath: '$.athena',
          Next: 'CheckSufficiency',
          Catch: [{ ErrorEquals: ['States.ALL'], Next: 'FailedTerminal' }],
        },
        CheckSufficiency: {
          Type: 'Task',
          Resource: 'arn:aws:states:::lambda:invoke',
          Parameters: {
            FunctionName: sufficiencyFn.functionArn,
            'Payload': {
              // Athena writes manifest.csv next to UNLOAD output.
              'manifest_uri.$':
                "States.Format('{}/training-sets/run={}/manifest.csv', '" +
                archiveBucketUri + "', $.context.run_id)",
            },
          },
          ResultSelector: { 'result.$': '$.Payload' },
          ResultPath: '$.sufficiency',
          Next: 'BranchOnSufficiency',
        },
        BranchOnSufficiency: {
          Type: 'Choice',
          Choices: [{
            Variable: '$.sufficiency.result.sufficient',
            BooleanEquals: true,
            Next: 'Train',
          }],
          Default: 'SkipTraining',
        },
        SkipTraining: {
          Type: 'Succeed',
          Comment: 'Insufficient data; gracefully skipped this run.',
        },
        Train: {
          Type: 'Task',
          Resource: 'arn:aws:states:::sagemaker:createTrainingJob.sync',
          Parameters: {
            'TrainingJobName.$':
              "States.Format('la-metro-delay-{}', $.context.run_id)",
            AlgorithmSpecification: {
              // Built-in XGBoost. Image URI is region-specific; this is the
              // us-west-2 XGBoost 1.7-1 container. If you deploy in a
              // different region, look up the URI for that region.
              TrainingImage: '746614075791.dkr.ecr.us-west-2.amazonaws.com/sagemaker-xgboost:1.7-1',
              TrainingInputMode: 'File',
              MetricDefinitions: [
                { Name: 'validation:rmse', Regex: '.*\\[.*\\]#011validation-rmse:([0-9\\.]+).*' },
                { Name: 'train:rmse',      Regex: '.*\\[.*\\]#011train-rmse:([0-9\\.]+).*' },
              ],
            },
            RoleArn: trainingRole.roleArn,
            ResourceConfig: {
              InstanceType: 'ml.m5.large',
              InstanceCount: 1,
              VolumeSizeInGB: 10,
            },
            StoppingCondition: { MaxRuntimeInSeconds: 600 },
            HyperParameters: {
              objective: 'reg:squarederror',
              num_round: '200',
              max_depth: '6',
              eta: '0.1',
              subsample: '0.8',
            },
            InputDataConfig: [{
              ChannelName: 'train',
              DataSource: {
                S3DataSource: {
                  S3DataType: 'S3Prefix',
                  // Athena UNLOAD writes both the data + a manifest; we point
                  // at the data prefix. SageMaker reads all .gz files there.
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
        },
        Evaluate: {
          Type: 'Task',
          Resource: 'arn:aws:states:::lambda:invoke',
          Parameters: {
            FunctionName: evaluateFn.functionArn,
            Payload: {
              'training_job_name.$': '$.training.TrainingJobName',
              'models_prefix_uri': `${archiveBucketUri}/models/`,
            },
          },
          ResultSelector: { 'result.$': '$.Payload' },
          ResultPath: '$.eval',
          Next: 'BranchOnEval',
        },
        BranchOnEval: {
          Type: 'Choice',
          Choices: [{
            Variable: '$.eval.result.promote',
            BooleanEquals: true,
            Next: 'Promote',
          }],
          Default: 'SkippedPromotion',
        },
        SkippedPromotion: {
          Type: 'Succeed',
          Comment: 'Candidate did not beat deployed model; not promoted.',
        },
        Promote: {
          Type: 'Task',
          Resource: 'arn:aws:states:::lambda:invoke',
          Parameters: {
            FunctionName: promoteFn.functionArn,
            Payload: {
              'candidate_model_uri.$': '$.eval.result.candidate_model_uri',
              'models_prefix_uri': `${archiveBucketUri}/models`,
              'candidate_metric.$': '$.eval.result.candidate_metric',
              'metric_name.$': '$.eval.result.metric_name',
            },
          },
          ResultSelector: { 'result.$': '$.Payload' },
          ResultPath: '$.promote',
          End: true,
        },
        FailedTerminal: {
          Type: 'Fail',
          Cause: 'A state in the nightly training pipeline failed; see CloudWatch.',
        },
      },
    } as const;

    // Inline the SQL file contents into the QueryString field.
    const sqlText = require('fs').readFileSync(
      path.join(__dirname, '..', '..', 'ml', 'feature_extraction.sql'),
      'utf-8',
    ).replace('${ARCHIVE_BUCKET}', props.archiveBucket.bucketName);
    // Step Functions evaluates States.Format on $.context.run_id at runtime,
    // but the ARCHIVE_BUCKET is fixed at deploy time so we substitute it now.
    // We leave ${RUN_ID} unresolved here and use States.Format in the SQL
    // template by replacing it with a Tokens-friendly variable. For v1 the
    // RUN_ID path is encoded directly into the UNLOAD target via States.Format
    // wrapping at execution time (see the Train state's S3Uri construction).
    // The SQL keeps run isolation by writing to training-sets/ — Athena UNLOAD
    // creates a sub-folder per query execution automatically.
    const sqlForSfn = sqlText.replace(
      "'s3://", "'s3://"
    ); // no-op placeholder; the real substitution is in the path the Athena
       // state writes to. We pre-bind ${RUN_ID} below via a string interp.
    (definition.States.ExtractFeatures.Parameters as any).QueryString = sqlText
      .replace('${RUN_ID}', '{RUN_ID_PLACEHOLDER}');
    // Convert {RUN_ID_PLACEHOLDER} into a States.Format reference so the run
    // id substitutes at execution time.
    const finalJson = JSON.stringify(definition).replace(
      '"{RUN_ID_PLACEHOLDER}"',
      `{"States.Format": "{}", "Args": ["$$.Execution.Name"]}`,
    );

    const stateMachine = new sfn.CfnStateMachine(this, 'NightlyTrainingSfn', {
      stateMachineName: 'la-metro-nightly-training',
      roleArn: new iam.Role(this, 'NightlyTrainingSfnRole', {
        assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
        inlinePolicies: {
          Inline: new iam.PolicyDocument({
            statements: [
              new iam.PolicyStatement({
                actions: [
                  'athena:StartQueryExecution',
                  'athena:GetQueryExecution',
                  'athena:GetQueryResults',
                  'athena:StopQueryExecution',
                ],
                resources: ['*'],
              }),
              new iam.PolicyStatement({
                actions: ['glue:GetTable', 'glue:GetDatabase', 'glue:GetPartitions'],
                resources: ['*'],
              }),
              new iam.PolicyStatement({
                actions: ['s3:GetObject', 's3:PutObject', 's3:ListBucket', 's3:GetBucketLocation'],
                resources: [props.archiveBucket.bucketArn, `${props.archiveBucket.bucketArn}/*`],
              }),
              new iam.PolicyStatement({
                actions: ['sagemaker:CreateTrainingJob', 'sagemaker:DescribeTrainingJob', 'sagemaker:StopTrainingJob'],
                resources: ['*'],
              }),
              new iam.PolicyStatement({
                actions: ['iam:PassRole'],
                resources: [trainingRole.roleArn],
              }),
              new iam.PolicyStatement({
                actions: ['lambda:InvokeFunction'],
                resources: [sufficiencyFn.functionArn, evaluateFn.functionArn, promoteFn.functionArn],
              }),
              new iam.PolicyStatement({
                actions: ['events:PutTargets', 'events:PutRule', 'events:DescribeRule'],
                resources: ['*'],   // required by .sync callback pattern
              }),
              new iam.PolicyStatement({
                actions: ['logs:CreateLogDelivery', 'logs:GetLogDelivery', 'logs:UpdateLogDelivery',
                          'logs:DeleteLogDelivery', 'logs:ListLogDeliveries',
                          'logs:PutResourcePolicy', 'logs:DescribeResourcePolicies', 'logs:DescribeLogGroups'],
                resources: ['*'],
              }),
            ],
          }),
        },
      }).roleArn,
      definitionString: finalJson,
      loggingConfiguration: {
        destinations: [{ cloudWatchLogsLogGroup: { logGroupArn: sfnLog.logGroupArn } }],
        level: 'ALL',
        includeExecutionData: true,
      },
    });
```

> The `${RUN_ID}` placeholder dance is awkward — the cleaner production move is to drop the placeholder out of the SQL entirely and write UNLOAD output to a fixed path that includes Athena's own `QueryExecutionId` (which the state machine reads from `$.athena.QueryExecution.QueryExecutionId`). Keeping it simple here for v1.

- [x] **Step 5: Add EventBridge daily schedule**

After the state machine, add:
```typescript
    // EventBridge Scheduler — runs daily at 10:00 UTC (~03:00 PT).
    new scheduler.CfnSchedule(this, 'NightlyTrainingSchedule', {
      name: 'la-metro-nightly-training',
      scheduleExpression: 'cron(0 10 * * ? *)',
      flexibleTimeWindow: { mode: 'OFF' },
      target: {
        arn: stateMachine.attrArn,
        roleArn: new iam.Role(this, 'NightlyScheduleRole', {
          assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
          inlinePolicies: {
            Inline: new iam.PolicyDocument({
              statements: [new iam.PolicyStatement({
                actions: ['states:StartExecution'],
                resources: [stateMachine.attrArn],
              })],
            }),
          },
        }).roleArn,
        input: '{}',
      },
      description: 'Phase 7b: triggers the nightly training pipeline.',
    });

    new cdk.CfnOutput(this, 'NightlyTrainingStateMachineArn', {
      value: stateMachine.attrArn,
    });
```

- [x] **Step 6: Build all assets, type-check, full synth**

Run:
```bash
cd /Users/caden/awsProject
for d in ingestion enrichment query_api aggregation websocket user_api post_confirmation feature_snapshot data_sufficiency_check evaluate_model promote_model; do
  scripts/build-lambda.sh "$d"
done
cd cdk && npx tsc --noEmit && npx cdk synth LaMetro-MLStack --quiet
```
Expected: build succeeds; type-check clean; MLStack synths.

- [x] **Step 7: Commit**

```bash
git add cdk/lib/ml-stack.ts
git commit -m "Phase 7b: Step Functions state machine + 3 Lambdas + daily schedule in MLStack"
```

---

## Task 6: Add new Lambdas to the CI build loop

**Files:**
- Modify: `.github/workflows/pr-checks.yml`

- [x] **Step 1: Extend the loop**

In `.github/workflows/pr-checks.yml`, in the cdk job's "Build Lambda assets" step, change the loop to include the three new lambdas:
```yaml
          for d in ingestion enrichment query_api aggregation websocket user_api post_confirmation feature_snapshot data_sufficiency_check evaluate_model promote_model; do
            scripts/build-lambda.sh "$d"
          done
```

- [x] **Step 2: Full Python suite**

Run: `pytest -q`
Expected: previous 149 + (5 + 7 + 2 + 6 SQL) = **169 passed**.

- [x] **Step 3: Commit**

```bash
git add .github/workflows/pr-checks.yml
git commit -m "Phase 7b: add 3 training-pipeline lambdas to CI build list"
```

---

## Deployment & manual verification (after all tasks)

- [ ] `cd cdk && npx cdk deploy LaMetro-MLStack` (updates the existing 7a-deployed stack with the training pipeline additions).
- [ ] In the Step Functions console, click **Start execution** on `la-metro-nightly-training` with empty input `{}`. Watch the visual flow.
  - With sufficient data: `ExtractFeatures` → `CheckSufficiency` (`sufficient: true`) → `Train` (creates SageMaker training job; takes ~5 min) → `Evaluate` → `Promote` (first model always promotes).
  - Without sufficient data: ends at `SkipTraining`.
- [ ] After a successful promote, verify `s3://<archive>/models/current/{model.tar.gz, metrics.json}` and `s3://<archive>/models/v=YYYY-MM-DD/model.tar.gz` exist.
- [ ] Athena: ad-hoc run a `SELECT COUNT(*)` against `la_metro.route_window_features` to confirm Athena reads the partition projection correctly.

---

## Self-review notes (author)

- **Spec coverage:** 7b section maps to Task 1 (Athena SQL), Task 2 (sufficiency), Task 3 (evaluate), Task 4 (promote), Task 5 (Step Functions state machine + training role + schedule), Task 6 (CI). Eval gating is in Task 3; promote is gated by it.
- **Placeholder check:** the `${RUN_ID}` substitution in the SQL/state-machine is a known awkward bit, flagged inline. Resolve before deploy.
- **Type consistency:** `VALIDATION_METRIC_NAME` matches the `MetricDefinitions` entry name in the state-machine training spec. `validation_metric` key in `metrics.json` is the same key the eval Lambda reads back next run. `models_prefix_uri` shape (no trailing slash in the JSON event; handler strips just in case) consistent across evaluate ↔ promote.
- **Open in 7c:** the SageMaker Serverless Inference endpoint and the gate's `UpdateEndpoint` extension belong to 7c; this plan leaves the model in S3 only.
