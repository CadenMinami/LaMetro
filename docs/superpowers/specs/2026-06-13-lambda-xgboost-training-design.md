# Lambda XGBoost Training Fallback — Design

> **Status: design, pending implementation.** Date: 2026-06-13.
> Branch: `phase-7b-training-pipeline`.

## Problem

Phase 7b's nightly training pipeline is built, deployed, and runs correctly
end-to-end — except the `Train` state fails. A fresh AWS account ships with a
**SageMaker training-job quota of 0** for every `ml.*` instance type, and our
request to raise `ml.m5.large` to 2 was **denied**:

> "We are unable to approve this request at this time. We recommend building
> more account history before we can process this type of increase."

This blocks the only thing missing for a working ML demo: a trained
`model.tar.gz` in `s3://<archive>/models/current/`, which Phase 7c's inference
endpoint needs.

### Key constraint check (decisive)

Only **training** and **processing** job quotas are 0. Everything else is open:

| Capability | Quota | Usable |
|---|---|---|
| Training job (`ml.m5.large`) | 0 | ❌ |
| Processing job | 0 | ❌ |
| Serverless Inference (concurrency 10, 25 endpoints) | nonzero | ✅ |
| Endpoint hosting (`ml.m5.large` = 4) | nonzero | ✅ |

So **7c's serverless inference endpoint is not blocked.** We only need another
way to produce the model artifact. A SageMaker training job is just one way to
make that file; XGBoost on our dataset (tens of thousands of rows, 30 days)
trains in under a second anywhere.

## Goals

1. Produce a `model.tar.gz` the SageMaker XGBoost **inference** container loads
   with zero custom inference code, so 7c is unblocked unchanged.
2. Keep the pipeline self-sufficient: the nightly Step Functions run stops
   failing and promotes a first model.
3. Preserve a clean **flip-back** to a managed SageMaker training job once the
   account ages and the quota is granted — the SageMaker code stays in the repo.
4. ~$0 cost. No SageMaker instance ever launches.

## Non-goals

- Not re-architecting the pipeline. One state's compute changes; the rest
  (Athena extract, sufficiency gate, evaluate, promote) is untouched.
- Not improving the model (feature set, hyperparameters, route encoding are
  unchanged from 7b). This is a compute swap, not an ML change.
- Not removing SageMaker. Serverless inference (7c) stays; managed training
  returns via a flag later.

## Architecture

```
ExtractFeatures (Athena UNLOAD) → CheckSufficiency → [Train] → Evaluate → Promote
                                                        ↑
                        was:  sagemaker:createTrainingJob.sync   (quota=0 ❌)
                        now:  lambda:invoke  train_model          (container, $0 ✅)
```

### Component 1 — `lambdas/train_model/` (container-image Lambda)

- **Reads** the gzipped CSV parts the Athena UNLOAD already writes to
  `s3://<archive>/training-sets/run=<run-id>/`. No header; column 0 is the
  label; 8 numeric features
  (`route_code, hour_of_day, day_of_week, lag1, lag2, lag3, temp_c, precip_mm`).
- **Splits** deterministically 80/20 (train/validation).
- **Trains** XGBoost with the same hyperparameters the state machine uses today:
  `objective=reg:squarederror, num_round=200, max_depth=6, eta=0.1, subsample=0.8`.
- **Computes** validation RMSE.
- **Packages** the booster pickled as a file named `xgboost-model` into
  `model.tar.gz`. This is exactly what the SageMaker built-in XGBoost algorithm
  writes and what the inference container's default `model_fn` loads.
  - **Version pin:** `xgboost==1.7.*`, matching the SageMaker XGBoost **1.7-1**
    inference container 7c uses. A booster pickled by a different major version
    may not unpickle in the container — this pin is the compatibility contract.
- **Uploads** to `s3://<archive>/training-jobs/run=<run-id>/output/model.tar.gz`.
- **Returns** `{candidate_metric: <rmse>, candidate_model_uri: <s3 uri>,
  metric_name: "validation:rmse"}`.

Packaging: `DockerImageFunction` (CDK `DockerImageCode.fromImageAsset`), because
xgboost + numpy exceed the 250 MB zip/layer limit. Memory 2–3 GB; trains in
~1–5 s on this data; 15-min timeout is ample.

### Component 2 — `evaluate_model` becomes source-agnostic

Today `evaluate_model` calls `describe_training_job` to fetch the final metric
and `ModelArtifacts.S3ModelArtifacts`. Change: **prefer `candidate_metric` and
`candidate_model_uri` from the event when present**; fall back to
`describe_training_job(training_job_name)` when they are not. This keeps the
handler working for:

- the Lambda training path (metric + URI arrive in the event), and
- the SageMaker flip-back (only `training_job_name` arrives).

The deployed-metric read, the `should_promote` comparison, and the output shape
(`promote, candidate_metric, deployed_metric, candidate_model_uri, metric_name`)
are unchanged.

### Component 3 — CDK + flip-back flag

- Add `train_model` as a `DockerImageFunction` in `MLStack`.
- Select the `Train` state by CDK context flag `useSagemakerTraining`
  (default **false** → Lambda). The existing `createTrainingJob.sync` state
  stays in the code, chosen when the flag is true.
- When Lambda mode: the `Train` state is a `lambda:invoke` of `train_model`,
  result on `$.training`; the `Evaluate` state passes
  `candidate_metric.$ = $.training.result.candidate_metric` and
  `candidate_model_uri.$ = $.training.result.candidate_model_uri`.
- Flip-back later = set `useSagemakerTraining=true` + redeploy. One flag.

### Component 4 — CI / Docker

The container Lambda builds via Docker at synth/deploy. CI's `pr-checks.yml`
gains a Docker build step for this one Lambda (`docker` is available on GitHub
runners); the other zip Lambdas keep `scripts/build-lambda.sh` unchanged. The
Dockerfile is commented part-by-part (the user is learning Docker).

## Data flow

1. Athena UNLOAD → `training-sets/run=<id>/*.csv.gz` (unchanged from 7b).
2. `train_model` reads those parts → trains → writes
   `training-jobs/run=<id>/output/model.tar.gz` → returns metric + URI.
3. `evaluate_model` compares candidate RMSE vs deployed `metrics.json` →
   promote decision.
4. `promote_model` (unchanged) copies the artifact to `models/current/` and
   `models/v=YYYY-MM-DD/`, writes `metrics.json` with `validation_metric`.

## Error handling

- **Empty / malformed CSV:** train_model raises; the `Train` state's existing
  `Catch → FailedTerminal` surfaces it. (The sufficiency gate upstream already
  guarantees ≥1,000 rows, so this is a guard, not an expected path.)
- **Single-row-per-route edge:** the 80/20 split must not produce an empty
  validation set; if validation would be empty, fall back to evaluating on the
  training rows and log a warning (acceptable for a portfolio first model).
- **xgboost version drift:** pinned in `requirements.txt` and asserted in a
  test that round-trips a pickled booster.

## Testing (TDD)

Pure functions, pytest, small in-memory CSV fixtures:

- `parse_training_csv(bytes) -> (X, y)` — header-less, label-first parsing.
- `split(X, y) -> (Xtr, ytr, Xval, yval)` — deterministic 80/20.
- `train_and_eval(...) -> (booster, rmse)` — trains, returns validation RMSE.
- `package_model(booster) -> bytes` — assert the tar contains `xgboost-model`
  and that a fresh `pickle.load` of it round-trips to a usable booster.
- S3 read/write mocked (`unittest.mock` / `moto`).
- `evaluate_model`: new test for the event-driven branch (metric + URI in
  event, no `describe_training_job` call).

## Cost

- `train_model` invoke: a few seconds of 2 GB Lambda ≈ **$0**.
- No SageMaker instance launches: **$0**.
- Athena: unchanged (~7 MB scanned, under the 10 MB minimum, ≈ $0).
- Net: the nightly pipeline becomes self-sufficient at effectively no cost.

## Interview narrative

"A fresh AWS account had a SageMaker training-job quota of 0 and the increase
was denied for lack of account history. Since only *training* was blocked (not
serverless inference), I containerized XGBoost training in a Lambda that emits a
SageMaker-compatible `model.tar.gz`, and gated the pipeline's training step on a
flag so it swaps back to a managed SageMaker training job once the account
ages — no other code changes." Demonstrates reading quota errors precisely,
routing around a constraint without over-rebuilding, and designing for reversal.
