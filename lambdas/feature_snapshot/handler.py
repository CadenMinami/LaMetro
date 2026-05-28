"""Feature-snapshot Lambda — Phase 7a.

Runs every 5 min on EventBridge. Reads the second-to-last closed 5-min window
from route-aggregates (via the window_start_iso GSI), fetches one Open-Meteo
observation for LA, writes a single gzipped JSON-lines object per cycle to
s3://.../processed-features/year=…/, and upserts the weather observation
to the weather-cache DDB row (so the precompute-predictions Lambda in 7c
doesn't also call Open-Meteo).

Reading the SECOND-to-last closed window guarantees the Aggregation Lambda
(which rewrites the current 5-min bucket every minute until it closes) has
fully settled the row before we snapshot it. Eliminates a read-while-being-
written race.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

WINDOW_MINUTES = 5


def second_to_last_closed_window_iso(now: datetime) -> str:
    """Return the ISO of the window that started ~10 min before `now`,
    floored to a 5-minute boundary. That's the window we snapshot.
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    # Floor `now` to the current 5-min boundary (the *open* window's start).
    floored = now.replace(
        minute=(now.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
        second=0,
        microsecond=0,
    )
    target = floored - timedelta(minutes=2 * WINDOW_MINUTES)
    return target.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_open_meteo_response(body: bytes) -> dict | None:
    """Parse Open-Meteo's /v1/forecast `current` block into our shape, or
    return None on any parse / shape failure (caller treats as missing weather).
    """
    try:
        doc = json.loads(body)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    current = doc.get("current") if isinstance(doc, dict) else None
    if not isinstance(current, dict):
        return None
    if current.get("temperature_2m") is None or current.get("precipitation") is None:
        return None
    return {
        "temp_c": current["temperature_2m"],
        "precip_mm": current["precipitation"],
        "observed_at": current.get("time", ""),
    }


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_feature_record(
    agg_row: dict[str, Any],
    weather: dict | None,
    ingested_at_iso: str,
) -> dict[str, Any]:
    """One (route, window) JSON record for the feature store. Weather fields
    are omitted entirely (not nulled) when Open-Meteo was unreachable, so
    Athena can distinguish "weather unknown" from "no rain"."""
    rec: dict[str, Any] = {
        "route_id": agg_row.get("route_id"),
        "window_start_iso": agg_row.get("window_start_iso"),
        "avg_delay_seconds": _to_int(agg_row.get("avg_delay_seconds")),
        "p95_delay_seconds": _to_int(agg_row.get("p95_delay_seconds")),
        "on_time_pct": _to_float(agg_row.get("on_time_pct")),
        "vehicle_count": _to_int(agg_row.get("vehicle_count")),
        "ingested_at": ingested_at_iso,
    }
    if weather is not None:
        rec["temp_c"] = weather.get("temp_c")
        rec["precip_mm"] = weather.get("precip_mm")
        if weather.get("observed_at"):
            rec["weather_observed_at"] = weather["observed_at"]
    return rec


# ---------------------------------------------------------------------------
# Orchestration — AWS clients, weather fetch, S3 write, lambda_handler
# ---------------------------------------------------------------------------

import gzip  # noqa: E402  (stdlib, safe to import after module-level helpers)
import os
import urllib.request
from uuid import uuid4

import boto3
from boto3.dynamodb.conditions import Key

ROUTE_AGGREGATES_TABLE = os.environ.get("ROUTE_AGGREGATES_TABLE_NAME", "")
ROUTE_AGGREGATES_WINDOW_GSI = os.environ.get(
    "ROUTE_AGGREGATES_WINDOW_GSI", "window_start_iso-index"
)
WEATHER_CACHE_TABLE = os.environ.get("WEATHER_CACHE_TABLE_NAME", "")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET", "")
PROCESSED_FEATURES_PREFIX = os.environ.get(
    "PROCESSED_FEATURES_PREFIX", "processed-features"
)
WEATHER_CACHE_TTL_SECONDS = int(os.environ.get("WEATHER_CACHE_TTL_SECONDS", "600"))

# LA Metro service area center, used as the single weather query point.
OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=34.05&longitude=-118.24"
    "&current=temperature_2m,precipitation"
    "&timezone=UTC"
)
OPEN_METEO_TIMEOUT_SECONDS = 4

_ddb = None
_ra_table = None
_wc_table = None
_s3_client = None


def _ddb_resource():
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb


def _route_aggregates():
    global _ra_table
    if _ra_table is None:
        _ra_table = _ddb_resource().Table(ROUTE_AGGREGATES_TABLE)
    return _ra_table


def _weather_cache():
    global _wc_table
    if _wc_table is None:
        _wc_table = _ddb_resource().Table(WEATHER_CACHE_TABLE)
    return _wc_table


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _utcnow() -> datetime:
    """Indirection so tests can freeze the clock."""
    return datetime.now(timezone.utc)


def fetch_weather() -> dict | None:
    """Call Open-Meteo for current LA observation. Returns None on any failure
    (caller treats as missing — record still gets written without weather)."""
    try:
        with urllib.request.urlopen(  # noqa: S310 - fixed URL, no user input
            OPEN_METEO_URL, timeout=OPEN_METEO_TIMEOUT_SECONDS
        ) as resp:
            body = resp.read()
    except Exception:
        logger.exception("open_meteo_fetch_failed")
        return None
    return parse_open_meteo_response(body)


def query_window_rows(window_iso: str) -> list[dict[str, Any]]:
    """Read every route's aggregate row for one window via the GSI, draining
    all pages."""
    table = _route_aggregates()
    out: list[dict[str, Any]] = []
    last_key = None
    while True:
        kwargs: dict[str, Any] = {
            "IndexName": ROUTE_AGGREGATES_WINDOW_GSI,
            "KeyConditionExpression": Key("window_start_iso").eq(window_iso),
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.query(**kwargs)
        out.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return out


def upsert_weather_cache(weather: dict, now: datetime) -> None:
    """Single-row cache used by the precompute-predictions Lambda (7c)."""
    _weather_cache().put_item(
        Item={
            "id": "la",
            "temp_c": weather["temp_c"],
            "precip_mm": weather["precip_mm"],
            "observed_at": weather.get("observed_at", ""),
            "cached_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ttl_epoch": int(now.timestamp()) + WEATHER_CACHE_TTL_SECONDS,
        }
    )


def _s3_key_for_window(window_iso: str) -> str:
    """processed-features/year=YYYY/month=MM/day=DD/hour=HH/window=…uuid.jsonl.gz"""
    dt = datetime.strptime(window_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (
        f"{PROCESSED_FEATURES_PREFIX}"
        f"/year={dt:%Y}/month={dt:%m}/day={dt:%d}/hour={dt:%H}"
        f"/window={window_iso}-{uuid4().hex[:8]}.jsonl.gz"
    )


def write_records_to_s3(records: list[dict[str, Any]], window_iso: str) -> str:
    """Gzip + put a single JSONL object. Returns the S3 key."""
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    gz = gzip.compress(body)
    key = _s3_key_for_window(window_iso)
    _s3().put_object(
        Bucket=ARCHIVE_BUCKET,
        Key=key,
        Body=gz,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    return key


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    if not (ROUTE_AGGREGATES_TABLE and WEATHER_CACHE_TABLE and ARCHIVE_BUCKET):
        raise RuntimeError(
            "Missing required env: ROUTE_AGGREGATES_TABLE_NAME, "
            "WEATHER_CACHE_TABLE_NAME, ARCHIVE_BUCKET"
        )

    now = _utcnow()
    window_iso = second_to_last_closed_window_iso(now)
    ingested_at_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    weather = fetch_weather()
    rows = query_window_rows(window_iso)

    records = [build_feature_record(r, weather, ingested_at_iso) for r in rows]

    s3_key = None
    if records:
        s3_key = write_records_to_s3(records, window_iso)

    if weather is not None:
        upsert_weather_cache(weather, now)

    log = {
        "ok": True,
        "window_start_iso": window_iso,
        "records_written": len(records),
        "weather_cached": weather is not None,
        "s3_key": s3_key,
    }
    logger.info(json.dumps(log))
    return log
