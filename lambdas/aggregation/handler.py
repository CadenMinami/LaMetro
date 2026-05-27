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
from boto3.dynamodb.conditions import Attr, Key

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
GEOFENCES_TABLE = os.environ.get("GEOFENCES_TABLE_NAME", "")
GEOFENCES_ROUTE_GSI = os.environ.get("GEOFENCES_ROUTE_GSI", "route_id-index")
NOTIFICATIONS_TABLE = os.environ.get("NOTIFICATIONS_TABLE_NAME", "")
ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", "900"))  # 15 min
NOTIFICATION_TTL_DAYS = int(os.environ.get("NOTIFICATION_TTL_DAYS", "7"))

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


def geofence_breaches(
    geofences: Iterable[dict[str, Any]],
    avg_delay: int,
    now_epoch: int,
    cooldown: int = ALERT_COOLDOWN_SECONDS,
) -> list[dict[str, Any]]:
    """Pure decision: which geofences should fire for this route right now?

    A geofence fires when it is enabled, the route's avg delay exceeds its
    threshold, and its cooldown window has elapsed since the last alert.
    """
    out: list[dict[str, Any]] = []
    for gf in geofences:
        if not gf.get("enabled", False):
            continue
        threshold = _to_int(gf.get("threshold_seconds"))
        # Strictly greater than the threshold = breach ("over your N min alert").
        # Equality does not fire, matching the notification copy.
        if threshold is None or avg_delay <= threshold:
            continue
        last = _to_int(gf.get("last_alerted_epoch")) or 0
        if now_epoch - last < cooldown:
            continue
        out.append(gf)
    return out


def build_notification_item(
    user_id: str,
    route_id: str,
    avg_delay: int,
    threshold: int,
    now: datetime,
    geofence_id: str,
) -> dict[str, Any]:
    minutes = round(avg_delay / 60)
    # `now` is fixed for the whole aggregation cycle, so suffix the sort key
    # with geofence_id to keep it unique when one user has several geofences
    # firing in the same cycle (otherwise the later put_item overwrites the
    # earlier). The leading ISO timestamp preserves chronological sort order.
    created_at = (
        f"{now.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')}#{geofence_id}"
    )
    return {
        "user_id": user_id,
        "created_at": created_at,
        "route_id": route_id,
        "delay_seconds": avg_delay,
        "threshold_seconds": threshold,
        "message": f"Route {route_id} is running ~{minutes} min late "
                   f"(over your {round(threshold / 60)} min alert).",
        "read": False,
        "ttl_epoch": int((now + timedelta(days=NOTIFICATION_TTL_DAYS)).timestamp()),
    }


def evaluate_geofences(
    geofences_table,
    notifications_table,
    aggregates: dict[str, dict[str, Any]],
    now: datetime,
) -> int:
    """For each route with a real avg delay, find breaching geofences, write a
    notification per breach, and stamp last_alerted_epoch. Returns alert count.
    """
    now_epoch = int(now.timestamp())
    fired = 0
    for route_id, agg in aggregates.items():
        avg_delay = agg.get("avg_delay_seconds")
        if avg_delay is None:
            continue
        # Drain all GSI pages — a popular route could have more geofence rows
        # than fit in a single 1 MB query page.
        items: list[dict[str, Any]] = []
        last_key = None
        while True:
            kwargs: dict[str, Any] = {
                "IndexName": GEOFENCES_ROUTE_GSI,
                "KeyConditionExpression": Key("route_id").eq(route_id),
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = geofences_table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        breaches = geofence_breaches(items, int(avg_delay), now_epoch)
        for gf in breaches:
            threshold = _to_int(gf.get("threshold_seconds")) or 0
            notifications_table.put_item(
                Item=build_notification_item(
                    gf["user_id"], route_id, int(avg_delay), threshold, now, gf["geofence_id"]
                )
            )
            geofences_table.update_item(
                Key={"user_id": gf["user_id"], "geofence_id": gf["geofence_id"]},
                UpdateExpression="SET last_alerted_epoch = :e",
                ExpressionAttributeValues={":e": now_epoch},
            )
            fired += 1
    return fired


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

    alerts_fired = 0
    if GEOFENCES_TABLE and NOTIFICATIONS_TABLE:
        try:
            alerts_fired = evaluate_geofences(
                get_table(GEOFENCES_TABLE),
                get_table(NOTIFICATIONS_TABLE),
                aggregates,
                now,
            )
        except Exception:
            # Alerting must never take down the aggregation cycle.
            logger.exception("geofence_evaluation_failed")

    elapsed_ms = int((time.monotonic() - started) * 1000)
    log = {
        "ok": True,
        "elapsed_ms": elapsed_ms,
        "window_start": iso_z(window_start),
        "vehicles_in_window": len(vehicles),
        "routes_written": written,
        "alerts_fired": alerts_fired,
    }
    logger.info(json.dumps(log))
    return log
