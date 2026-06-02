"""Backfill route_window_features from raw GTFS-RT events in S3.

One-time local script. See docs/superpowers/specs/2026-06-02-feature-backfill-design.md.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
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
