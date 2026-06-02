# Feature Backfill — Design Spec

**Date:** 2026-06-02
**Status:** Approved (design)
**Phase context:** Step 2 of the ML data→model chain (7a deployed → **backfill** → 7b training pipeline).

## Problem

Phase 7a (MLStack) is now deployed: `feature_snapshot` writes per-`(route, 5-min window)`
records to `s3://<archive>/processed-features/`, catalogued by the Glue table
`la_metro.route_window_features`. But it only produces features *going forward*, and
ingestion is now scale-to-zero gated, so live features accrue slowly.

Meanwhile ~26 days of **real** LA Metro GTFS-RT positions (May 7 – Jun 2, ~47M position
records, ~7,240 gzipped objects) sit in `s3://<archive>/raw-events/`. We need those turned
into the same feature table so Phase 7b can train a model with a **genuine MAE** — the
portfolio "finding" — instead of synthetic data.

## Goal

A one-time, re-runnable local Python script (`ml/backfill_features.py`) that reads the raw
events, computes real schedule-deviation delays, aggregates them into 5-minute windows, and
writes records in the **exact same path + schema** as `feature_snapshot` — so Athena sees
backfilled and live rows as one uniform table.

## Decisions (locked)

- **Runner: local Python script.** Not Glue / Step Functions. It's a one-time job; standing
  up managed batch infra for a single run is over-engineering and costs money. Cost ≈ $0
  (S3 GET/PUT only). Defensible in an interview: the *ongoing* feature path is AWS-native
  (the `feature_snapshot` Lambda); the one-off backfill is a script.
- **Real data, real delays.** Reuse the existing schedule-deviation algorithm, not an
  approximation.
- **Output is identical to `feature_snapshot`:** same `processed-features/year=…/month=…/day=…/hour=…/`
  partitioning, same gzip-JSONL format, same column set. One feature table, two producers.
- **Include historical weather** via Open-Meteo's free Archive API so `temp_c`/`precip_mm`
  are populated (the model uses temperature as a feature).
- **On-demand training downstream** (7b is a separate spec): not in scope here.

## Architecture

Processed **day-by-day** (bounds memory and lets a failed day be re-run in isolation):

1. **Read + parse.** Stream each day's `raw-events/.../*.gz`. The Firehose archive is
   **concatenated JSON** (no newline delimiter), so parse with a streaming
   `json.JSONDecoder().raw_decode` loop — *not* line splitting. Keep only records with a
   non-empty `route_id` and `trip_id` (deadheading vehicles have neither and can't be scored).

2. **Dedupe to latest-per-`(vehicle, 5-min window)`.** The key performance move: collapse
   ~47M raw points down to roughly what the live pipeline actually scored (the most recent
   position per vehicle per window). Drops shapely work by ~5–10×.

3. **Compute delay.** For each kept position, compute `delay_seconds` via
   `lambdas/shared/deviation`, loading **that day's GTFS static version** from
   `gtfs-static/v=…` (pick the version whose date is the latest `≤` the event date; fall back
   to last-known-good on mismatch — mirrors the live enrichment behavior). Off-route /
   no-schedule positions yield a null delay (still counted; delay fields null) — same contract
   as the live pipeline.

4. **Aggregate per `(route_id, 5-min window)`** using the same statistics as
   `aggregation/handler.py`: `avg_delay_seconds`, `p95_delay_seconds`, `on_time_pct`,
   `vehicle_count`.

5. **Join weather.** One Open-Meteo Archive API call per day yields hourly `temp_c` /
   `precip_mm`; attach the hour's observation to each window in that day. Weather fields are
   omitted (not nulled) when unavailable, matching `build_feature_record`'s contract so Athena
   can distinguish "unknown" from "no rain".

6. **Write output.** Gzip-JSONL objects to `processed-features/…` using the same partition
   layout as `feature_snapshot`, **but with a deterministic key** — replace the random
   `uuid4().hex[:8]` suffix with a stable token derived from the window (e.g.
   `…/window=<iso>-backfill.jsonl.gz`). This makes re-running a day **idempotent**: the same
   window always writes to the same key, so a re-run overwrites rather than appending
   duplicate objects. (The live `feature_snapshot` keeps its random suffix; backfill targets
   past dates the Lambda never wrote, so the two never collide.)

## Reuse (don't reinvent)

- `lambdas/shared/deviation.py` — schedule-deviation algorithm.
- `lambdas/shared/gtfs_static.py` — GTFS schedule/shape loader.
- `lambdas/aggregation/handler.py` — per-route window stat helpers (avg/p95/on-time/count).
- `lambdas/feature_snapshot/handler.py` — `build_feature_record`, `_s3_key_for_window`,
  gzip-JSONL write shape (so output matches exactly).

The script is a CLI mirroring `ml/bootstrap.py` (e.g. `python -m ml.backfill_features
--bucket <archive> --start 2026-05-07 --end 2026-06-02 [--workers N]`).

## Performance

The shapely pass over millions of deduped points is the slow part. Mitigations: day-by-day
processing, the dedupe in step 2, and optional `multiprocessing` across days/workers. A
one-time run in the minutes-to-low-tens-of-minutes range is acceptable. **No silent
sampling** — if any cap or skip is applied, the script logs exactly what was dropped.

## Testing (TDD)

Pure functions get unit tests first:
- Concatenated-JSON parser: multiple objects, no delimiter, trailing whitespace, malformed
  tail.
- Routed-record filter: empty `route_id`/`trip_id` skipped.
- Dedupe: keeps the latest `vehicle_timestamp` per `(vehicle, window)`.
- Window aggregation: avg/p95/on-time/count against a hand-built fixture (mirror existing
  aggregation tests).
- GTFS-version-for-date selection: picks latest `≤` date; fallback on miss.
- Weather attach: hour→window mapping; omit-when-missing.
- Output record matches `feature_snapshot`'s schema/key exactly.

S3 and Open-Meteo are mocked at their boundaries (MagicMock), consistent with the repo's
existing lambda tests. End-to-end validation is a real run against one day, then an Athena
`COUNT(*)` + sample-row check that backfilled features are queryable.

## Out of scope

- The 7b training pipeline (Step Functions → SageMaker XGBoost → eval → promote) — separate
  spec/plan.
- Changing the live `feature_snapshot` path or the Glue schema.
- Cleaning up the 2 empty orphan archive buckets (tracked separately, ~$0).

## Success criteria

- `route_window_features` contains real, schema-valid feature rows spanning May 7 – Jun 2,
  queryable in Athena.
- Delays are computed via the real deviation algorithm (not synthetic).
- Re-running a day is idempotent.
- Script cost ≈ $0; no managed infra left running.
