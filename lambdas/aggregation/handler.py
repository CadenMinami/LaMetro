"""Aggregation Lambda — runs every minute on EventBridge.

Reads the current state of `hot-vehicles`, groups vehicles by route, and writes
one row per route into `route-aggregates` keyed by the current 5-min bucket.
Each minute updates the same bucket; the final write before the bucket closes
becomes the canonical value once `last_updated` ages out of the window.

Phase 4b only: `delay_seconds` is null on every vehicle until 4c lands the
deviation algorithm. We still emit `vehicle_count` and a stub for the delay
fields so downstream API + frontend can be wired up against real data.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import boto3
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HOT_TABLE = os.environ.get("HOT_VEHICLES_TABLE_NAME", "")
AGG_TABLE = os.environ.get("ROUTE_AGGREGATES_TABLE_NAME", "")
GSI_NAME = os.environ.get("HOT_VEHICLES_ROUTE_GSI", "route_id-last_updated-index")
WINDOW_MINUTES = int(os.environ.get("AGGREGATION_WINDOW_MINUTES", "5"))
TTL_DAYS = int(os.environ.get("AGGREGATE_TTL_DAYS", "7"))
# A vehicle is "on time" if its delay is in [-60s, +60s]. Keep this configurable
# so we can revisit once we see the real delay distribution.
ON_TIME_TOLERANCE_SECONDS = int(os.environ.get("ON_TIME_TOLERANCE_SECONDS", "60"))

_ddb_resource = None


def get_table(name: str):
    global _ddb_resource
    if _ddb_resource is None:
        _ddb_resource = boto3.resource("dynamodb")
    return _ddb_resource.Table(name)


def floor_to_window(now: datetime, window_minutes: int = WINDOW_MINUTES) -> datetime:
    """Return the start of the window-aligned bucket containing `now`."""
    minute_of_hour = now.minute - (now.minute % window_minutes)
    return now.replace(minute=minute_of_hour, second=0, microsecond=0)


def iso_z(when: datetime) -> str:
    """Format a UTC datetime as ISO-8601 with a trailing Z (matches what
    ingestion writes to last_updated, so string comparisons line up)."""
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def scan_recent_vehicles(
    table, window_start: datetime, window_end: datetime
) -> Iterable[dict[str, Any]]:
    """Scan hot-vehicles for rows updated within [window_start, window_end).

    A scan is fine here: ~1700 rows per cycle is a single page. Eventually
    consistent (cheaper) — we don't need point-in-time accuracy for an
    aggregate.
    """
    start_iso = iso_z(window_start)
    end_iso = iso_z(window_end)
    # Filter expression on a non-key attribute means DynamoDB still reads every
    # item in the table — but at our scale (~1700 items) that's pennies.
    filter_expr = Attr("last_updated").between(start_iso, end_iso)
    last_evaluated = None
    while True:
        kwargs: dict[str, Any] = {"FilterExpression": filter_expr}
        if last_evaluated:
            kwargs["ExclusiveStartKey"] = last_evaluated
        resp = table.scan(**kwargs)
        yield from resp.get("Items", [])
        last_evaluated = resp.get("LastEvaluatedKey")
        if not last_evaluated:
            return


def _to_int(v: Any) -> int | None:
    """DynamoDB delivers numbers as Decimal via the resource API. Cast safely."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def percentile(sorted_values: list[float], p: float) -> float:
    """Linear-interpolation percentile on a *sorted* list. Empty → raises."""
    if not sorted_values:
        raise ValueError("percentile of empty list")
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    k = (len(sorted_values) - 1) * (p / 100.0)
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return float(sorted_values[lo])
    frac = k - lo
    return float(sorted_values[lo]) * (1 - frac) + float(sorted_values[hi]) * frac


def aggregate_by_route(
    vehicles: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Group vehicles by route_id, compute count + delay stats per route."""
    by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for v in vehicles:
        route_id = v.get("route_id")
        if not route_id:
            # Empty route_id = deadhead/layover. Skip — these aren't running a
            # service, so they shouldn't count toward route reliability.
            continue
        by_route[route_id].append(v)

    out: dict[str, dict[str, Any]] = {}
    for route_id, vs in by_route.items():
        delays = sorted(
            d for v in vs if (d := _to_int(v.get("delay_seconds"))) is not None
        )
        agg: dict[str, Any] = {"vehicle_count": len(vs)}
        if delays:
            agg["avg_delay_seconds"] = int(sum(delays) / len(delays))
            agg["p95_delay_seconds"] = int(percentile(delays, 95))
            on_time = sum(
                1 for d in delays if abs(d) <= ON_TIME_TOLERANCE_SECONDS
            )
            agg["on_time_pct"] = round(on_time / len(delays) * 100, 1)
        else:
            # Phase 4b: every vehicle has delay_seconds=None, so we land here
            # for every route. Frontend can render "—" for these fields until
            # 4c lights up real numbers.
            agg["avg_delay_seconds"] = None
            agg["p95_delay_seconds"] = None
            agg["on_time_pct"] = None
        out[route_id] = agg
    return out


def write_aggregates(
    table,
    aggregates: dict[str, dict[str, Any]],
    window_start: datetime,
) -> int:
    """Batch-write one row per route. Returns count of items written."""
    bucket_iso = iso_z(window_start)
    ttl_epoch = int((window_start + timedelta(days=TTL_DAYS)).timestamp())
    written = 0
    with table.batch_writer() as batch:
        for route_id, agg in aggregates.items():
            item: dict[str, Any] = {
                "route_id": route_id,
                "window_start_iso": bucket_iso,
                "vehicle_count": agg["vehicle_count"],
                "ttl_epoch": ttl_epoch,
                "updated_at_iso": iso_z(datetime.now(timezone.utc)),
            }
            # DynamoDB rejects None values — only write the delay fields when
            # we actually have data.
            if agg["avg_delay_seconds"] is not None:
                item["avg_delay_seconds"] = agg["avg_delay_seconds"]
                item["p95_delay_seconds"] = agg["p95_delay_seconds"]
                item["on_time_pct"] = str(agg["on_time_pct"])  # avoid Decimal mismatch
            batch.put_item(Item=item)
            written += 1
    return written


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    started = time.monotonic()
    if not HOT_TABLE or not AGG_TABLE:
        raise RuntimeError(
            "HOT_VEHICLES_TABLE_NAME and ROUTE_AGGREGATES_TABLE_NAME must be set"
        )

    now = datetime.now(timezone.utc)
    window_start = floor_to_window(now)
    window_end = window_start + timedelta(minutes=WINDOW_MINUTES)

    hot = get_table(HOT_TABLE)
    agg_table = get_table(AGG_TABLE)

    vehicles = list(scan_recent_vehicles(hot, window_start, window_end))
    aggregates = aggregate_by_route(vehicles)
    written = write_aggregates(agg_table, aggregates, window_start)

    elapsed_ms = int((time.monotonic() - started) * 1000)
    log = {
        "ok": True,
        "elapsed_ms": elapsed_ms,
        "window_start": iso_z(window_start),
        "vehicles_in_window": len(vehicles),
        "routes_written": written,
    }
    logger.info(json.dumps(log))
    return log
