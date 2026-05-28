# Phase 7 — Delay Prediction — Design

**Date:** 2026-05-27
**Status:** Approved (design), pending implementation plans
**Phase:** 7 of the LA Metro Reliability Platform build sequence
**Builds on:** Phase 6 (auth + alerts) — merged

## Goal

Train and serve a model that predicts each route's **average delay for the upcoming ~15-minute window**, surfaced on the route detail page alongside the live aggregates. XGBoost regression on tabular features, trained nightly via Step Functions, served from a SageMaker Serverless Inference endpoint.

The deliverable is a *working* prediction shown in the UI plus the AWS ML-pipeline architecture behind it — interview signal first, model accuracy second.

## The data-label gap (the foundational decision)

Before Phase 7 there is **no durable, labeled training data**: Firehose tees off the Kinesis stream *before* enrichment, so the S3 archive (`raw-events/`) is raw positions without `delay_seconds`. The computed delay only lives in `hot-vehicles` (1-hour TTL) and `route-aggregates` (5-min × per-route avg/p95, 7-day TTL in DynamoDB). Phase 7 must start by fixing that.

This also drives the **build sequence:** 7a ships a durable feature store and must run for a stretch before 7b/7c are worth building. We spec the whole architecture now (this doc) but implement and deploy 7a first, accumulate ~weeks of data, then implement 7b and 7c.

## Key decisions (and the alternatives rejected)

1. **Prediction target → route-level next-window avg delay.**
   *Rejected:* per-stop arrival delay (closer to product UX but needs per-stop-delay history + upstream-stop features we don't archive — too much new plumbing); per-vehicle current delay (redundant — already computed in enrichment).

2. **Inference serving → SageMaker Serverless Inference endpoint + inference Lambda.**
   The spec-mandated choice; preserves the SageMaker-flagship signal at ~$3/mo idle.
   *Rejected:* batch-precompute → DynamoDB (cheapest, arguably smartest for low-cardinality predictions, but loses the live-endpoint talking point); Lambda-hosted XGBoost (loses the managed-inference signal).

3. **Feature set → `[route_id, hour_of_day, day_of_week, lag1/lag2/lag3 avg delay, temp_c, precip_mm]`** with `route_id` one-hot/target-encoded by the SageMaker built-in.
   Weather via **Open-Meteo** (free, no API key, one HTTP call per snapshot cycle for an LA point).
   *Rejected:* features-only-from-time-and-route (essentially a historical-average baseline — too thin to call ML); `current_temp` from a paid provider (unjustified cost for portfolio); `upstream_stop_delay` (data we don't archive; reserved for v2).

4. **Feature-store writer → a separate `feature-snapshot` Lambda on a 5-min EventBridge schedule**, not piggybacking on the Aggregation Lambda.
   *Why:* aggregation runs every minute and updates the same 5-min bucket repeatedly; writing to S3 every minute creates dupes. A separate snapshot Lambda runs once per closed window, reads the canonical row from `route-aggregates`, attaches weather, and writes one consolidated record set per (route, window). Clean separation of hot stats from durable features.
   *Rejected:* aggregation-writes-too (dupe complexity); DynamoDB → S3 nightly export (extra hop, daily granularity loses per-window weather alignment).

5. **Training cadence → nightly Step Functions via EventBridge**, with a data-sufficiency gate that skips gracefully if Athena returns < 5,000 rows.
   *Rejected:* on-demand training (loses the orchestration signal); train-on-every-window (cost + churn).

6. **Eval gating → deploy only if MAE improves vs the deployed model** (or it's the first ever).
   Keeps a deployable trail in S3 (`models/v=YYYY-MM-DD-nightly/`) regardless.

## Sub-piece overview (build order: 7a → 7b → 7c)

| Piece | What it ships | When it's worth building |
|---|---|---|
| **7a — Data foundation** | `feature-snapshot` Lambda, weather capture, S3 `processed-features/` store, Glue table for Athena, `MLStack` skeleton | Now. Must run a stretch (≥ ~2 weeks recommended) before 7b is useful. |
| **7b — Training pipeline** | Step Functions state machine, Athena `feature_extraction.sql`, SageMaker training job, eval + gating, model registry in S3 | After 7a has accumulated ≥5k labeled rows. |
| **7c — Inference serving + frontend** | SageMaker Serverless endpoint, `inference` Lambda + `/routes/{routeId}/prediction` route, route-detail UI | After 7b has produced at least one deployable model. |

## 7a — Data foundation

### `feature-snapshot` Lambda
- **Schedule:** EventBridge rule, `rate(5 minutes)`, offset to fire ~30s after each closed 5-min window boundary (so the Aggregation Lambda has had multiple passes to converge the bucket).
- **Inputs:** scans `route-aggregates` for the just-closed `window_start_iso`; calls Open-Meteo once for LA (lat 34.05, lon -118.24) for current temp + precip.
- **Output:** writes one newline-JSON object per (route, window) to `s3://la-metro-archive-{env}/processed-features/year=YYYY/month=MM/day=DD/hour=HH/window=YYYY-MM-DDTHH:MM:SSZ.jsonl.gz`. Each object:
  ```json
  {
    "route_id": "720",
    "window_start_iso": "2026-05-27T12:00:00Z",
    "avg_delay_seconds": 87,
    "p95_delay_seconds": 240,
    "on_time_pct": 71.4,
    "vehicle_count": 9,
    "temp_c": 22.1,
    "precip_mm": 0.0,
    "ingested_at": "2026-05-27T12:05:30Z"
  }
  ```
- **Failure handling:** if Open-Meteo is unreachable, write the record without weather fields (don't drop the row); if `route-aggregates` is empty for the window, log + exit. Never crash the schedule.

### Glue table
- DB: `la_metro`; table: `route_window_features` over `s3://.../processed-features/`.
- Partitions: `year`, `month`, `day`, `hour` (Hive-style, no crawler needed — defined statically in CDK).
- Format: JSON SerDe over gzipped JSON-lines. (Parquet conversion is a v2 optimization; v1 keeps writes simple.)
- Athena queries this table for both training and ad-hoc analysis.

### `MLStack`
- New CDK stack holding: the feature-snapshot Lambda + its schedule + log group, the Glue DB + table (CfnDatabase / CfnTable), and grants for the Lambda (read `route-aggregates`, write `processed-features/`).
- Later phases extend this same stack with the Step Functions state machine and the SageMaker endpoint.

## 7b — Training pipeline

### Athena `feature_extraction.sql`
- Reads `route_window_features` for the last 30 days.
- Produces one row per (route, window) with:
  - **Features:** `route_id`, `hour_of_day = HOUR(window_start)`, `day_of_week = DAY_OF_WEEK(window_start)`, `lag1_avg_delay` / `lag2_avg_delay` / `lag3_avg_delay` (previous 3 windows' avg delay for the same route via `LAG()` window functions), `temp_c`, `precip_mm`.
  - **Label:** `next_window_avg_delay = LEAD(avg_delay_seconds, 1) OVER (PARTITION BY route_id ORDER BY window_start)`.
- Filters out rows where any lag or the label is NULL (boundary rows).
- UNLOAD to `s3://.../training-sets/run=<run-id>/data.parquet`.

### Step Functions state machine
- **Trigger:** EventBridge Scheduler, daily at 03:00 PT.
- **States:**
  1. `ExtractFeatures` — Athena `StartQueryExecution` on the UNLOAD SQL. Wait for completion. On failure → fail terminal.
  2. `CheckDataSufficiency` — small Lambda reads the UNLOAD manifest, counts rows. If < **5,000** → `SkipTraining` terminal state (success, log only).
  3. `Train` — SageMaker `CreateTrainingJob` using the **built-in XGBoost** container, ml.m5.large, 1 instance, ~5 min budget. Hyperparameters: `objective=reg:squarederror`, `num_round=200`, `max_depth=6`, `eta=0.1`, `subsample=0.8`. Output: `s3://.../models/candidate/run=<run-id>/model.tar.gz`.
  4. `Evaluate` — Lambda runs the candidate on the held-out time-split test set (last 20% of windows by `window_start`), computes MAE, fetches deployed model's MAE from `s3://.../models/current/metrics.json` (missing = +∞).
  5. `GateAndPromote` — Choice state: if `candidate_mae < deployed_mae` (or deployed missing), copy candidate to `s3://.../models/v=YYYY-MM-DD/` and update `current/metrics.json`, then `UpdateEndpoint` to point Serverless Inference at the new model. Else → terminal `SkipPromotion`.
- **Failure isolation:** any state's failure logs to CloudWatch and ends the run; never silently leaves a half-deployed model.

### Models on S3
```
s3://la-metro-archive-{env}/models/
  current/metrics.json          # the live model's metadata + MAE
  v=2026-06-15/                 # each promoted version, dated
    model.tar.gz
    metrics.json
  candidate/run=<run-id>/       # latest training output, pre-gate
    model.tar.gz
    metrics.json
```

## 7c — Inference serving + frontend

### SageMaker Serverless Inference
- One endpoint, named `la-metro-delay-predictor`, configured for the **built-in XGBoost** container, `MemorySizeInMB=1024`, `MaxConcurrency=5`. Scales to zero; ~$3/mo idle.
- Initially deployed by 7b's `GateAndPromote` state on the first promoted model. (CDK creates the endpoint shell; the model is set by the state machine.)

### `inference` Lambda
- Behind a new public REST route: `GET /routes/{routeId}/prediction`.
- Assembles the live feature vector at request time:
  - Reads the last 3 windows' `avg_delay_seconds` for the route from `route-aggregates` (lag1/2/3).
  - Computes `hour_of_day` and `day_of_week` from current LA time.
  - Fetches current weather from a small cache (a DynamoDB row with 5-min TTL, populated by the same weather call the snapshot Lambda makes — avoids hitting Open-Meteo on every prediction request).
- Calls the SageMaker endpoint with the feature vector.
- Returns:
  ```json
  {
    "route_id": "720",
    "predicted_next_window_avg_delay_seconds": 132,
    "typical_for_hour_dow_seconds": 90,   // historical avg from Athena, cached daily
    "as_of": "2026-05-27T12:34:00Z",
    "model_version": "v=2026-06-15"
  }
  ```
- **Graceful degradation:** if the endpoint is cold/unavailable, return 503 with a clear error (UI shows "—").

### Frontend
- `frontend/lib/api.ts`: add `fetchRoutePrediction(routeId)`.
- `frontend/app/route/page.tsx`: under the latest-aggregate stats, add a "Predicted next ~15 min: **+X min** (typical +Y min)" card, color-coded with the existing delay palette.

## Cost (within the project's $30/mo dev cap)

- **Open-Meteo:** free, 1 call / 5 min = 288/day. Well under their 10k/day limit.
- **Glue:** table-only definition, no crawler runtime. ~$0.
- **Athena:** training UNLOAD scans gzipped JSON for 30 days (a few hundred MB) — cents/run.
- **SageMaker training job:** nightly, ml.m5.large for ~5 min = a few cents per run.
- **SageMaker Serverless Inference:** scales to zero, ~$3/mo with portfolio-scale traffic. The dominant ongoing cost; still well within budget.
- **Step Functions:** standard workflow, negligible (~$0.025 per state transition × ~6 states/run nightly).

## Testing

Match the established conventions (`pytest` + `unittest.mock` — **not** moto). Per the recent fix, `tests/` dirs in new lambdas must NOT contain `__init__.py`.

- **`feature-snapshot` Lambda:** window-record construction, weather-attach, S3-write call shape, failure-mode (Open-Meteo down → row still written without weather, empty aggregates → exit clean).
- **`inference` Lambda:** feature-vector assembly from lags + weather cache + time; endpoint-call shape; 503 path.
- **Training scripts (`ml/train.py`, `ml/evaluate.py`):** unit-test feature engineering, the time-split, and MAE computation; the SageMaker job itself is integration (smoke via a tiny local run, not in unit tests).
- **CDK:** `tsc --noEmit` + `cdk synth` for `MLStack`.

## Out of scope (→ README "future work")

- **Per-stop directional predictions** ("arriving in 7 min at MY stop"). The route-level prediction is the v1; per-stop reserves `stop_id` for v2 and would need a per-stop labeled feature store.
- **Multi-feature weather** (humidity, wind, snowfall) and forecast horizons. v1 uses current observation only.
- **Drift monitoring / SageMaker Model Monitor.** The eval-gating loop is the v1 quality bar.
- **Parquet conversion of `processed-features/`.** JSON-lines is fine at this volume; Parquet is a v2 optimization.
- **A/B between model versions.** Single-active model via the gated promote.

## Implementation sequence (for the planning step)

We have one design doc (this) and three implementation plans. Of the three, **7a is the only one buildable today**; 7b/7c plans assume 7a is shipped and producing data. We can write all three plans now, but the 7b/7c plans must explicitly note that any schema details depend on what 7a actually produces and may need a final refinement pass once 7a is live.
