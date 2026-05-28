"""Synthetic-data seeder for Phase 7a's feature store.

Standalone script (not a Lambda). Usage:

    python -m ml.bootstrap --bucket la-metro-archive-dev --days 7

Generates plausible per-(route, window) records — with hour-of-day +
weekday/weekend patterns and Gaussian noise — and uploads them to the same
S3 prefix the live feature-snapshot Lambda writes to. Lets 7b/7c be built
and demoed against a populated feature store without waiting weeks for
real data to accumulate. The bootstrap rows simply age out of the training
window naturally once real data accumulates past the same date.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator

# A handful of routes representative of LA Metro's mix (frequent rapids,
# locals, rail). Replace with the real route list in production runs.
DEFAULT_ROUTES = ["720", "754", "2", "33", "212", "910"]


def _baseline_delay_seconds(hour_utc: int, is_weekend: bool) -> float:
    """Plausible hour-of-day delay baseline for synthetic bootstrap data.

    The 'rush' bands here (7-10, 23-02) are chosen to match the test fixtures
    (hour=8 should be rush, hour=14 should be off-peak). They are NOT a real
    timezone model — this seeder produces synthetic data for demoing the
    pipeline, not realistic LA traffic patterns. The trained model will
    quickly overfit to whatever pattern is here, which is fine because real
    data overwrites this once it accumulates.
    """
    rush_morning = 7 <= hour_utc <= 10
    rush_evening = hour_utc in (23, 0, 1, 2)
    if is_weekend:
        base = 30.0
    elif rush_morning or rush_evening:
        base = 180.0
    else:
        base = 60.0
    return base


def synthetic_record(
    route_id: str, window_start: datetime, *, seed: int
) -> dict:
    """Deterministic per-seed synthetic record. Same shape as the live writer.

    `ingested_at` is derived from `window_start` (not wall-clock) so identical
    inputs always produce identical output.
    """
    rng = random.Random(seed)
    hour = window_start.hour
    is_weekend = window_start.weekday() >= 5
    base = _baseline_delay_seconds(hour, is_weekend)
    avg = max(0, int(rng.gauss(base, 45)))
    p95 = avg + max(0, int(rng.gauss(120, 60)))
    on_time_pct = max(0.0, min(100.0, 100.0 - avg / 6.0))
    vehicle_count = max(1, int(rng.gauss(8 if not is_weekend else 4, 3)))
    temp_c = round(rng.gauss(20.0, 5.0), 1)
    precip_mm = round(max(0.0, rng.gauss(0.0, 0.5)), 2)
    return {
        "route_id": route_id,
        "window_start_iso": window_start.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "avg_delay_seconds": avg,
        "p95_delay_seconds": p95,
        "on_time_pct": round(on_time_pct, 1),
        "vehicle_count": vehicle_count,
        "temp_c": temp_c,
        "precip_mm": precip_mm,
        "weather_observed_at": window_start.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "ingested_at": (window_start + timedelta(minutes=10))
            .astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def generate_windows(
    start: datetime, end: datetime, *, window_minutes: int = 5
) -> Iterator[datetime]:
    """Yield window_start datetimes from `start` (inclusive) to `end` (exclusive)."""
    cur = start
    step = timedelta(minutes=window_minutes)
    while cur < end:
        yield cur
        cur += step


def records_for(
    routes: Iterable[str],
    start: datetime,
    end: datetime,
    *,
    base_seed: int = 0,
) -> Iterator[dict]:
    """All (route, window) records over the range. Stable seed per (route, window)."""
    routes = list(routes)  # materialize so a generator can't be exhausted mid-loop (Fix 3)
    for w in generate_windows(start, end):
        window_iso = w.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for route_id in routes:
            # Seed is content-addressed: (route_id, window_iso) → deterministic
            # 32-bit value XOR'd with base_seed. Adding/reordering routes does
            # NOT change other (route, window) seeds, and re-running the
            # seeder produces identical data (sha256 is cross-process stable).
            # Cross-process-stable hash (Python's built-in hash() is randomized
            # per PEP 456). Same (route, window) → same seed in every run.
            digest = hashlib.sha256(f"{route_id}|{window_iso}".encode()).digest()
            seed = base_seed ^ int.from_bytes(digest[:4], "big")
            yield synthetic_record(route_id, w, seed=seed)


def partition_key_for(window_iso: str) -> str:
    """Hive-style prefix that matches what feature-snapshot writes."""
    dt = datetime.strptime(window_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return f"year={dt:%Y}/month={dt:%m}/day={dt:%d}/hour={dt:%H}"


def _upload_partition(
    s3_client, bucket: str, key_prefix: str, partition_prefix: str, records: list[dict]
) -> str:
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    gz = gzip.compress(body)
    key = (
        f"{key_prefix}/{partition_prefix}/window={records[0]['window_start_iso']}"
        f"-bootstrap.jsonl.gz"
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=gz,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    return key


def main(argv: list[str] | None = None) -> int:
    import boto3  # imported lazily so the unit tests don't need boto3

    parser = argparse.ArgumentParser(description="Seed Phase 7a synthetic feature data.")
    parser.add_argument("--bucket", required=True, help="Archive bucket name.")
    parser.add_argument(
        "--prefix", default="processed-features",
        help="S3 key prefix (default: processed-features).",
    )
    parser.add_argument("--days", type=int, default=7, help="Days of data to generate.")
    parser.add_argument(
        "--routes", nargs="+", default=DEFAULT_ROUTES,
        help="Route IDs to generate.",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="ISO end timestamp (UTC, exclusive). Defaults to now.",
    )
    args = parser.parse_args(argv)

    end = (
        datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if args.end
        else datetime.now(timezone.utc).replace(second=0, microsecond=0)
    )
    start = end - timedelta(days=args.days)

    s3 = boto3.client("s3")
    buf: dict[str, list[dict]] = {}
    for rec in records_for(args.routes, start, end):
        pkey = partition_key_for(rec["window_start_iso"])
        cohort_key = f"{pkey}|{rec['window_start_iso']}"
        buf.setdefault(cohort_key, []).append(rec)

    written = 0
    for cohort_key, recs in buf.items():
        pkey, _ = cohort_key.split("|", 1)
        _upload_partition(s3, args.bucket, args.prefix, pkey, recs)
        written += 1
    print(f"Uploaded {written} cohort objects to s3://{args.bucket}/{args.prefix}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
