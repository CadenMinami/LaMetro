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
--
-- ${ARCHIVE_BUCKET} is bound at deploy time (CDK). ${RUN_ID} is bound at
-- execution time by the Step Functions ExtractFeatures state via
-- States.Format on $$.Execution.Name.

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
        CAST(date_format(date_add('day', -30, current_date), '%Y%m%d') AS INTEGER)
      AND avg_delay_seconds IS NOT NULL
  ),
  shaped AS (
    SELECT
      route_id,
      window_start_iso,
      avg_delay_seconds,
      COALESCE(temp_c, 0.0)    AS temp_c,
      COALESCE(precip_mm, 0.0) AS precip_mm,
      -- window_start_iso is ISO-8601 UTC ('2026-05-07T05:00:00Z'). Trino's
      -- CAST(varchar AS timestamp) rejects the 'T'/'Z'; from_iso8601_timestamp
      -- parses it (as timestamp with time zone, in UTC).
      EXTRACT(hour FROM from_iso8601_timestamp(window_start_iso))      AS hour_of_day,
      EXTRACT(day_of_week FROM from_iso8601_timestamp(window_start_iso)) AS day_of_week,
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
    -- FEATURES (numeric only — XGBoost built-in needs numeric input).
    -- route_id is collapsed to a stable integer code via a deterministic
    -- hash. This is an ordinal-meaningless code (route 70 vs 71 have no
    -- ordering relationship), good enough for v1; a per-route one-hot or
    -- target-encoding is a v2 refinement.
    abs(crc32(to_utf8(route_id))) % 1000 AS route_code,
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
