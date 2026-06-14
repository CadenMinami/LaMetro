"""Backfill route_window_features from raw GTFS-RT events in S3.

One-time local script. See docs/superpowers/specs/2026-06-02-feature-backfill-design.md.
"""

from __future__ import annotations

import argparse
import gzip
import json
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Iterator
from zoneinfo import ZoneInfo

from lambdas.aggregation import handler as agg
from lambdas.feature_snapshot import handler as fs
from lambdas.shared import deviation, gtfs_static

LA_TZ = ZoneInfo("America/Los_Angeles")
WINDOW_MINUTES = 5

_DECODER = json.JSONDecoder()


def iter_json_objects(raw: bytes) -> Iterator[dict[str, Any]]:
    """Yield each JSON object from a Firehose blob of *concatenated* JSON
    (no delimiter between objects). Stops at the first undecodable tail."""
    text = raw.decode("utf-8")
    i, n = 0, len(text)
    while i < n:
        # Skip inter-object whitespace/newlines.
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        try:
            obj, end = _DECODER.raw_decode(text, i)
        except json.JSONDecodeError:
            break
        yield obj
        i = end


def is_routed(rec: dict[str, Any]) -> bool:
    """True only when the record can be schedule-matched (has route + trip)."""
    return bool(rec.get("route_id")) and bool(rec.get("trip_id"))


def seconds_into_service_day(epoch: int) -> int:
    """Seconds since LA-local midnight for a unix timestamp. Used as the
    service-day clock the GTFS schedule is expressed in. (Owl trips that cross
    midnight fall outside the schedule window and yield a null delay — an
    accepted edge for aggregate features.)"""
    local = datetime.fromtimestamp(int(epoch), tz=LA_TZ)
    return local.hour * 3600 + local.minute * 60 + local.second


def window_start_iso(epoch: int) -> str:
    """Floor a unix timestamp to its 5-min UTC window start, ISO-Z."""
    dt = datetime.fromtimestamp(int(epoch), tz=timezone.utc)
    floored = dt.replace(
        minute=(dt.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
        second=0, microsecond=0,
    )
    return floored.strftime("%Y-%m-%dT%H:%M:%SZ")


def dedupe_latest(records: "Iterable[dict[str, Any]]") -> dict[tuple[str, str], dict[str, Any]]:
    """Keep the newest position per (vehicle_id, window). The perf move: this is
    roughly what the live pipeline scored — one position per vehicle per window."""
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for r in records:
        ts = int(r["vehicle_timestamp"])
        key = (r["vehicle_id"], window_start_iso(ts))
        cur = best.get(key)
        if cur is None or ts > int(cur["vehicle_timestamp"]):
            best[key] = r
    return best


def delay_for_record(rec: dict[str, Any], gtfs: "gtfs_static.GTFSStatic") -> int | None:
    """Schedule deviation (sec) for one position, or None if not computable."""
    trip_id = rec["trip_id"]
    shape = gtfs.shape_for_trip(trip_id)
    schedule = gtfs.schedule_for_trip(trip_id)
    if shape is None or not schedule:
        return None
    return deviation.compute_delay_seconds(
        shape, schedule,
        float(rec["lat"]), float(rec["lon"]),
        seconds_into_service_day(rec["vehicle_timestamp"]),
    )


def records_for_window(
    window_iso: str,
    vehicles: list[dict[str, Any]],
    weather: dict | None,
    ingested_at_iso: str,
) -> list[dict[str, Any]]:
    """Aggregate one window's vehicles into per-route feature records, matching
    the live feature_snapshot schema exactly."""
    by_route = agg.aggregate_by_route(vehicles)   # {route_id: {count, avg, p95, on_time}}
    out: list[dict[str, Any]] = []
    for route_id, a in by_route.items():
        agg_row = {
            "route_id": route_id,
            "window_start_iso": window_iso,
            "avg_delay_seconds": a["avg_delay_seconds"],
            "p95_delay_seconds": a["p95_delay_seconds"],
            "on_time_pct": a["on_time_pct"],
            "vehicle_count": a["vehicle_count"],
        }
        out.append(fs.build_feature_record(agg_row, weather, ingested_at_iso))
    return out


ARCHIVE_URL = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude=34.05&longitude=-118.24"
    "&hourly=temperature_2m,precipitation&timezone=UTC"
    "&start_date={start}&end_date={end}"
)


def parse_archive_weather(body: bytes) -> dict[str, dict[str, Any]]:
    """Index Open-Meteo Archive hourly arrays by their 'YYYY-MM-DDTHH:MM' key."""
    doc = json.loads(body)
    hourly = doc.get("hourly") or {}
    times = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    precs = hourly.get("precipitation") or []
    out: dict[str, dict[str, Any]] = {}
    for t, temp, prec in zip(times, temps, precs):
        if temp is None or prec is None:
            continue
        out[t] = {"temp_c": float(temp), "precip_mm": float(prec), "observed_at": t}
    return out


def weather_for_window(window_iso: str, idx: dict[str, dict[str, Any]]) -> dict | None:
    """Look up the archive hour ('YYYY-MM-DDTHH:00') covering this window."""
    hour_key = window_iso[:13] + ":00"   # '2026-05-07T19:05:00Z' -> '2026-05-07T19:00'
    return idx.get(hour_key)


def fetch_archive_weather(start_date: str, end_date: str) -> dict[str, dict[str, Any]]:
    """Fetch the whole date-range's hourly weather in one call. Returns {} on failure."""
    url = ARCHIVE_URL.format(start=start_date, end=end_date)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:  # noqa: S310 fixed URL
            return parse_archive_weather(resp.read())
    except Exception:
        return {}


PROCESSED_PREFIX = "processed-features"


def backfill_s3_key(window_iso: str) -> str:
    """Deterministic key (no random suffix) so re-running a day overwrites
    rather than duplicating. '-backfill' distinguishes from live feature_snapshot."""
    dt = datetime.strptime(window_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (
        f"{PROCESSED_PREFIX}"
        f"/year={dt:%Y}/month={dt:%m}/day={dt:%d}/hour={dt:%H}"
        f"/window={window_iso}-backfill.jsonl.gz"
    )


def write_window_records(s3, bucket: str, window_iso: str, rows: list[dict[str, Any]]) -> str:
    """Gzip + PUT one JSONL object for a window. Returns the key."""
    body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
    key = backfill_s3_key(window_iso)
    s3.put_object(
        Bucket=bucket, Key=key, Body=gzip.compress(body),
        ContentType="application/x-ndjson", ContentEncoding="gzip",
    )
    return key


def list_day_keys(s3, bucket: str, date_str: str) -> list[str]:
    """All raw-event object keys for a YYYY-MM-DD date (paginated)."""
    y, m, d = date_str.split("-")
    prefix = f"raw-events/year={y}/month={m}/day={d}/"
    keys: list[str] = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys.extend(o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".gz"))
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return keys


def read_gz(s3, bucket: str, key: str) -> bytes:
    """Raw gzip bytes of an S3 object."""
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def process_day(
    s3, bucket: str, date_str: str, gtfs, weather_idx: dict, ingested_at_iso: str,
) -> tuple[int, int]:
    """Backfill one date. Returns (windows_written, records_written)."""
    # 1. Read + parse + filter to routed positions.
    routed: list[dict[str, Any]] = []
    for key in list_day_keys(s3, bucket, date_str):
        for rec in iter_json_objects(gzip.decompress(read_gz(s3, bucket, key))):
            if is_routed(rec):
                routed.append(rec)

    # 2. Dedupe to latest per (vehicle, window).
    deduped = dedupe_latest(routed)

    # 3. Attach delay, group by window.
    by_window: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (vehicle_id, window_iso), rec in deduped.items():
        delay = delay_for_record(rec, gtfs)
        by_window[window_iso].append({"route_id": rec["route_id"], "delay_seconds": delay})

    # 4. Aggregate + write one object per window.
    windows_written = records_written = 0
    for window_iso, vehicles in by_window.items():
        rows = records_for_window(
            window_iso, vehicles, weather_for_window(window_iso, weather_idx), ingested_at_iso,
        )
        if rows:
            write_window_records(s3, bucket, window_iso, rows)
            windows_written += 1
            records_written += len(rows)
    return windows_written, records_written


def daterange(start_date: str, end_date: str) -> list[str]:
    """Inclusive list of YYYY-MM-DD strings from start to end."""
    d0 = date.fromisoformat(start_date)
    d1 = date.fromisoformat(end_date)
    out, cur = [], d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def main(argv: list[str] | None = None) -> int:
    import boto3

    p = argparse.ArgumentParser(description="Backfill route_window_features from raw events.")
    p.add_argument("--bucket", required=True, help="Archive bucket name.")
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD.")
    p.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive).")
    args = p.parse_args(argv)

    s3 = boto3.client("s3")
    gtfs = gtfs_static.load_from_s3(args.bucket, shapes=True)   # current pointer covers the window
    weather_idx = fetch_archive_weather(args.start, args.end)
    ingested_at_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    total_w = total_r = 0
    for day in daterange(args.start, args.end):
        w, r = process_day(s3, args.bucket, day, gtfs, weather_idx, ingested_at_iso)
        total_w += w
        total_r += r
        print(f"{day}: {w} windows, {r} records")
    print(f"DONE: {total_w} windows, {total_r} feature records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
