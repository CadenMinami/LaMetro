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

2. **Inference serving → 5-minute precompute Lambda → SageMaker Serverless endpoint → `route-predictions` DynamoDB table → API reads from DDB.**
   Both signals at once: the SageMaker Serverless endpoint still exists and is invoked every cycle (~150 calls per 5 min) — flagship-service signal preserved — but user-facing API requests hit DynamoDB, so reads are instant and never pay an endpoint cold start. Adds the extra "recognized predictions are low-cardinality and precomputed them" engineering signal. The original draft of this spec had user requests calling the endpoint directly; reversed after honest review.
   *Rejected:* on-demand endpoint calls per user request (cold starts on the user-facing path, no real benefit at this cardinality); Lambda-hosted XGBoost (loses the managed-inference signal entirely).

3. **Feature set → `[route_id, hour_of_day, day_of_week, lag1/lag2/lag3 avg delay, temp_c, precip_mm]`** with `route_id` one-hot/target-encoded by the SageMaker built-in.
   Weather via **Open-Meteo** (free, no API key, one HTTP call per snapshot cycle for an LA point).
   *Rejected:* features-only-from-time-and-route (essentially a historical-average baseline — too thin to call ML); `current_temp` from a paid provider (unjustified cost for portfolio); `upstream_stop_delay` (data we don't archive; reserved for v2).

4. **Feature-store writer → a separate `feature-snapshot` Lambda on a 5-min EventBridge schedule**, not piggybacking on the Aggregation Lambda.
   *Why:* aggregation runs every minute and updates the same 5-min bucket repeatedly; writing to S3 every minute creates dupes. A separate snapshot Lambda runs once per closed window, reads the canonical row from `route-aggregates`, attaches weather, and writes one consolidated record set per (route, window). Clean separation of hot stats from durable features.
   *Rejected:* aggregation-writes-too (dupe complexity); DynamoDB → S3 nightly export (extra hop, daily granularity loses per-window weather alignment).

5. **Training cadence → nightly Step Functions via EventBridge**, with a data-sufficiency gate that skips gracefully if Athena returns < **1,000 rows** (~3 hours of accumulated data). Early models will be noisy — that's accepted; the eval gate only promotes when MAE improves, so early noise can't degrade the deployed model.
   Plus a **bootstrap path** (`ml/bootstrap.py`): seeds `processed-features/` with replayed-historical or synthetic data so 7b/7c are demoable without waiting weeks for real data to accumulate on a 10-week portfolio timeline.
   *Rejected:* on-demand training (loses the orchestration signal); train-on-every-window (cost + churn); a high gate of 5,000+ rows (would block any demo for days/weeks).

6. **Eval gating → deploy only if MAE improves vs the deployed model** (or it's the first ever).
   Keeps a deployable trail in S3 (`models/v=YYYY-MM-DD-nightly/`) regardless.

7. **Product framing → "trendline / directional signal", not a standalone number.**
   Route-level next-window avg delay sitting next to the *current* avg delay doesn't add obvious user value on its own — the model is largely an autocorrelation of the last few windows. The UI frames it directionally ("currently +5 min, predicted +8 min") so the value is in the *trend*, not the absolute prediction. Per-stop arrival prediction would be the real product win and is documented as v2 (reserved by Phase 6's `stop_id` field).

## Sub-piece overview (build order: 7a → 7b → 7c)

| Piece | What it ships | When it's worth building |
|---|---|---|
| **7a — Data foundation** | `feature-snapshot` Lambda, weather capture, S3 `processed-features/` store, Glue table for Athena, `MLStack` skeleton | Now. Must run a stretch (≥ ~2 weeks recommended) before 7b is useful. |
| **7b — Training pipeline** | Step Functions state machine, Athena `feature_extraction.sql`, SageMaker training job, eval + gating, model registry in S3 | After 7a has accumulated ≥5k labeled rows. |
| **7c — Inference serving + frontend** | SageMaker Serverless endpoint, `precompute-predictions` Lambda, `route-predictions` DDB table, `/routes/{routeId}/prediction` extension to `query-api`, trendline UI on the route-detail page | After 7b has produced at least one deployable model. |

## 7a — Data foundation

### `feature-snapshot` Lambda
- **Schedule:** EventBridge rule, `rate(5 minutes)`. The Lambda always snapshots the **second-to-last closed window** (the one starting ~10 min before `now`), not the most recent one. Aggregation runs every minute and rewrites the same 5-min bucket until it closes; reading the second-to-last window guarantees the bucket has been fully settled for 5+ min and eliminates the read-while-being-written race.
- **Inputs:** scans `route-aggregates` for the chosen `window_start_iso`; calls Open-Meteo once for LA (lat 34.05, lon -118.24) for current temp + precip.
- **Output:**
  1. Writes one newline-JSON object per (route, window) to `s3://la-metro-archive-{env}/processed-features/year=YYYY/month=MM/day=DD/hour=HH/window=YYYY-MM-DDTHH:MM:SSZ.jsonl.gz`. Each object:
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
  2. Also upserts the latest weather observation into a tiny `weather-cache` DynamoDB table (PK `id="la"`, attrs `temp_c`, `precip_mm`, `observed_at`, `ttl_epoch=now+10min`). The `precompute-predictions` Lambda (7c) reads this row instead of calling Open-Meteo itself — one weather API call per cycle, not two.
- **Failure handling:** if Open-Meteo is unreachable, write the S3 record without weather fields (don't drop the row) and skip the cache upsert; if `route-aggregates` is empty for the window, log + exit. Never crash the schedule.

### `weather-cache` DynamoDB table
- Lives in `StorageStack`. PK `id` (string), attrs as above. On-demand billing, single-row table — effectively free.

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
  2. `CheckDataSufficiency` — small Lambda reads the UNLOAD manifest, counts rows. If < **1,000** → `SkipTraining` terminal state (success, log only).
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

### Bootstrap path (`ml/bootstrap.py`)
- Standalone script (run locally with AWS creds, not part of the live pipeline).
- Generates ~7 days of synthetic per-(route, window) records with plausible delay distributions (hour-of-day pattern + weekday/weekend differences + Gaussian noise) plus pseudo-weather. Writes them under `s3://.../processed-features/year=…/` partitioned the same way the live snapshot would.
- Lets 7b/7c be built and demoed against a populated feature store on day one, without waiting for live data. The bootstrap data is overwritten/aged out as real snapshots accumulate (Athena queries by date partition, so simply running for ≥7 days makes synthetic data fall out of the training window naturally).

## 7c — Inference serving + frontend

### SageMaker Serverless Inference endpoint
- One endpoint, named `la-metro-delay-predictor`, configured for the **built-in XGBoost** container, `MemorySizeInMB=1024`, `MaxConcurrency=5`. Scales to zero.
- Initially deployed by 7b's `GateAndPromote` state on the first promoted model. (CDK creates the endpoint shell; the model is set by the state machine.)
- **No user request ever hits this endpoint directly** — it's invoked only by the precompute Lambda below.

### `precompute-predictions` Lambda
- **Schedule:** EventBridge, `rate(5 minutes)`, offset ~1 min after the `feature-snapshot` Lambda so the latest lag data has just landed in `route-aggregates`.
- For each route with recent data in `route-aggregates`:
  - Assembles the live feature vector: `lag1/lag2/lag3` from `route-aggregates`, `hour_of_day` + `day_of_week` from current LA time, `temp_c` + `precip_mm` from the latest snapshot's weather.
  - Calls the SageMaker endpoint with the feature vector.
  - Writes one row to the `route-predictions` table.
- **Failure handling:** any per-route endpoint error logs and skips that route; one bad route never blocks the cycle.
- **Weather source:** the Lambda reads the `weather-cache` DDB row (one `get_item` on `id="la"`) populated by `feature-snapshot`. Avoids any direct Open-Meteo call from this Lambda. If the cache row is missing or stale (older than TTL), the precompute Lambda still runs but emits `temp_c`/`precip_mm` as `null` for that cycle.

### `route-predictions` DynamoDB table
- Lives in `StorageStack` alongside `route-aggregates`.
- PK: `route_id` (single row per route — overwritten every 5-min cycle). On-demand billing. 15-min TTL on `ttl_epoch` so stale rows can't linger if precompute stops.
- Attributes: `predicted_next_window_avg_delay_seconds` (int), `current_avg_delay_seconds` (int — copied from the latest aggregate for the trendline framing), `as_of` (ISO), `model_version` (str), `window_start_iso` (str), `ttl_epoch` (int).

### API + frontend
- The existing **public** query API (`/routes/{routeId}/aggregates`) gains a sibling route: `GET /routes/{routeId}/prediction`, served by the existing `query-api` Lambda (extend it — no new Lambda) by reading the row from `route-predictions`.
- Response:
  ```json
  {
    "route_id": "720",
    "predicted_next_window_avg_delay_seconds": 132,
    "current_avg_delay_seconds": 75,
    "as_of": "2026-05-27T12:34:00Z",
    "model_version": "v=2026-06-15"
  }
  ```
- Returns 404 when no row exists (cold start / no recent data); the UI then hides the card. No 503 / cold-start path because there's no endpoint call on the user-facing read.
- **Frontend:** `frontend/lib/api.ts` adds `fetchRoutePrediction(routeId)`. `frontend/app/route/page.tsx` adds a **trendline** card next to the headline stats:
  > **Trending:** currently **+5 min**, predicted **+8 min** ↑
  - Color from the existing delay palette; arrow ↑/↓/→ based on `sign(predicted - current)`; magnitude badge based on `|predicted - current|`.

## Cost (within the project's $30/mo dev cap)

- **Open-Meteo:** free, 1 call / 5 min = 288/day. Well under their 10k/day limit.
- **Glue:** table-only definition, no crawler runtime. ~$0.
- **Athena:** training UNLOAD scans gzipped JSON for 30 days (a few hundred MB) — cents/run.
- **SageMaker training job:** nightly, ml.m5.large for ~5 min = a few cents per run.
- **SageMaker Serverless Inference:** scales to zero. Under precompute, the endpoint is invoked ~150 routes × 288 windows/day ≈ 43k/day; small XGBoost requests are sub-second, so estimated cost ~$3–8/mo. Still the dominant ongoing cost and well within budget.
- **`route-predictions` DDB:** ~43k writes/day on-demand = pennies/mo. Reads dwarfed by writes.
- **Step Functions:** standard workflow, negligible (~$0.025 per state transition × ~6 states/run nightly).

## Testing

Match the established conventions (`pytest` + `unittest.mock` — **not** moto). Per the recent fix, `tests/` dirs in new lambdas must NOT contain `__init__.py`.

- **`feature-snapshot` Lambda:** window-selection (always second-to-last closed window), record construction, weather-attach, S3-write call shape, failure-mode (Open-Meteo down → row still written without weather, empty aggregates → exit clean).
- **`precompute-predictions` Lambda:** feature-vector assembly from lags + weather + time; per-route endpoint-call shape; per-route failure isolation (one bad route doesn't block the cycle); writes to `route-predictions` with correct TTL.
- **`query-api` `/prediction` extension:** reads from `route-predictions` and shapes the response; returns 404 when no row exists.
- **Training scripts (`ml/train.py`, `ml/evaluate.py`):** unit-test feature engineering, the time-split, and MAE computation; the SageMaker job itself is integration (smoke via a tiny local run, not in unit tests).
- **`ml/bootstrap.py`:** generates expected number of (route, window) records, shape matches what the live snapshot writes, time partitioning is correct.
- **CDK:** `tsc --noEmit` + `cdk synth` for `MLStack`.

## Out of scope (→ README "future work")

- **Per-stop directional predictions** ("arriving in 7 min at MY stop"). The route-level prediction is the v1; per-stop reserves `stop_id` for v2 and would need a per-stop labeled feature store.
- **Multi-feature weather** (humidity, wind, snowfall) and forecast horizons. v1 uses current observation only.
- **Drift monitoring / SageMaker Model Monitor.** The eval-gating loop is the v1 quality bar.
- **Parquet conversion of `processed-features/`.** JSON-lines is fine at this volume; Parquet is a v2 optimization.
- **A/B between model versions.** Single-active model via the gated promote.

## Implementation sequence (for the planning step)

We have one design doc (this) and three implementation plans. Of the three, **7a is the only one buildable today**; 7b/7c plans assume 7a is shipped and producing data. We can write all three plans now, but the 7b/7c plans must explicitly note that any schema details depend on what 7a actually produces and may need a final refinement pass once 7a is live.
