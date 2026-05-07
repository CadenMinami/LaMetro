"""Enrichment Lambda — Kinesis trigger → DynamoDB hot-vehicles.

Phase 4c: each Kinesis record is decoded, the vehicle's schedule deviation
is computed against GTFS static loaded from S3, and the position +
delay_seconds is written to hot-vehicles.

Cold-start cost: ~5-15 s the first invocation after deploy (S3 fetch + dict
→ Shapely conversion). Subsequent invocations reuse the module-level cache
until a new GTFS feed is loaded into the bucket.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import boto3

# Shared deviation algorithm — copied into this lambda's .build by the
# build script. See scripts/build-lambda.sh for the layout.
from lambdas.shared import deviation, gtfs_static

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("HOT_VEHICLES_TABLE_NAME", "")
GEOHASH_PRECISION = int(os.environ.get("GEOHASH_PRECISION", "6"))
TTL_SECONDS = int(os.environ.get("HOT_VEHICLE_TTL_SECONDS", "3600"))
GTFS_BUCKET = os.environ.get("GTFS_STATIC_BUCKET", "")
GTFS_POINTER_KEY = os.environ.get("GTFS_STATIC_POINTER_KEY", "gtfs-static/current.txt")
AGENCY_TZ = ZoneInfo(os.environ.get("AGENCY_TIMEZONE", "America/Los_Angeles"))

_dynamodb = None
_table = None
_gtfs: gtfs_static.GTFSStatic | None = None

GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def encode_geohash(lat: float, lon: float, precision: int = GEOHASH_PRECISION) -> str:
    """Encode (lat, lon) to a base32 geohash string of the given precision."""
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    bits: list[int] = []
    even = True
    while len(bits) < precision * 5:
        if even:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon >= mid:
                bits.append(1)
                lon_range[0] = mid
            else:
                bits.append(0)
                lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                bits.append(1)
                lat_range[0] = mid
            else:
                bits.append(0)
                lat_range[1] = mid
        even = not even

    out = []
    for i in range(0, len(bits), 5):
        chunk = bits[i : i + 5]
        idx = (chunk[0] << 4) | (chunk[1] << 3) | (chunk[2] << 2) | (chunk[3] << 1) | chunk[4]
        out.append(GEOHASH_BASE32[idx])
    return "".join(out)


def get_table():
    global _dynamodb, _table
    if _table is None:
        if not TABLE_NAME:
            raise RuntimeError("HOT_VEHICLES_TABLE_NAME env var not set")
        _dynamodb = boto3.resource("dynamodb")
        _table = _dynamodb.Table(TABLE_NAME)
    return _table


def get_gtfs() -> gtfs_static.GTFSStatic | None:
    """Lazy-load GTFS static on first invocation. Returns None (rather than
    raising) when the bucket isn't configured or the load fails — the lambda
    keeps writing positions, just without delay_seconds, instead of crashing
    the whole Kinesis batch."""
    global _gtfs
    if _gtfs is not None:
        return _gtfs
    if not GTFS_BUCKET:
        return None
    try:
        _gtfs = gtfs_static.load_from_s3(GTFS_BUCKET, GTFS_POINTER_KEY)
    except Exception:
        logger.exception("gtfs_load_failed")
        return None
    return _gtfs


def seconds_into_service_day(rt_epoch: int, tz: ZoneInfo = AGENCY_TZ) -> int:
    """Convert a Unix epoch timestamp to seconds since the agency's local
    midnight. GTFS service-day times can exceed 86400 for trips that
    started yesterday; we don't try to model that here — late-night trips
    will simply fall outside the schedule's distance range and return
    delay=None, which is safe."""
    local = datetime.fromtimestamp(rt_epoch, tz=tz)
    return local.hour * 3600 + local.minute * 60 + local.second


def decode_kinesis_record(record: dict[str, Any]) -> dict[str, Any]:
    """Kinesis events arrive base64-encoded under record['kinesis']['data']."""
    raw = base64.b64decode(record["kinesis"]["data"])
    return json.loads(raw)


def compute_delay_for_event(
    event: dict[str, Any], gtfs: gtfs_static.GTFSStatic | None
) -> int | None:
    """Look up the trip in GTFS static, run the deviation algorithm, return
    delay in seconds (or None when no defensible answer)."""
    if gtfs is None:
        return None
    trip_id = event.get("trip_id") or ""
    if not trip_id:
        return None
    shape = gtfs.shape_for_trip(trip_id)
    schedule = gtfs.schedule_for_trip(trip_id)
    if shape is None or schedule is None:
        return None
    rt_ts = event.get("vehicle_timestamp") or event.get("feed_timestamp")
    if not rt_ts:
        return None
    return deviation.compute_delay_seconds(
        shape=shape,
        schedule=schedule,
        vehicle_lat=event["lat"],
        vehicle_lon=event["lon"],
        seconds_into_service_day=seconds_into_service_day(int(rt_ts)),
    )


def to_dynamo_item(
    event: dict[str, Any], gtfs: gtfs_static.GTFSStatic | None
) -> dict[str, Any] | None:
    """Convert a parsed vehicle event to a DynamoDB item dict, or None to skip."""
    lat = event.get("lat")
    lon = event.get("lon")
    vehicle_id = event.get("vehicle_id")
    if lat is None or lon is None or not vehicle_id:
        return None

    now = int(time.time())
    iso_ts = datetime.fromtimestamp(
        event.get("vehicle_timestamp") or now, tz=timezone.utc
    ).isoformat().replace("+00:00", "Z")

    item: dict[str, Any] = {
        "geohash": encode_geohash(lat, lon),
        "vehicle_id": vehicle_id,
        "trip_id": event.get("trip_id") or "",
        "lat": str(lat),
        "lon": str(lon),
        "last_updated": iso_ts,
        "ttl_epoch": now + TTL_SECONDS,
    }

    delay = compute_delay_for_event(event, gtfs)
    if delay is not None:
        item["delay_seconds"] = delay

    # Only include route_id when populated. The route_id-last_updated-index GSI
    # rejects items whose key attribute is an empty string.
    route_id = event.get("route_id")
    # If GTFS-RT omitted route_id but we know the trip statically, fill it in.
    if not route_id and gtfs is not None:
        trip_id = event.get("trip_id") or ""
        route_id = gtfs.trip_route.get(trip_id, "")
    if route_id:
        item["route_id"] = route_id
    if event.get("bearing") is not None:
        item["bearing"] = str(event["bearing"])
    if event.get("speed_mps") is not None:
        item["speed_mps"] = str(event["speed_mps"])
    return item


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    records = event.get("Records", [])
    written = 0
    skipped = 0
    errors = 0
    delays_computed = 0

    gtfs = get_gtfs()
    table = get_table()
    with table.batch_writer(overwrite_by_pkeys=["geohash", "vehicle_id"]) as batch:
        for record in records:
            try:
                parsed = decode_kinesis_record(record)
                item = to_dynamo_item(parsed, gtfs)
                if item is None:
                    skipped += 1
                    continue
                if "delay_seconds" in item:
                    delays_computed += 1
                batch.put_item(Item=item)
                written += 1
            except Exception:
                logger.exception("enrichment_record_failed")
                errors += 1

    summary = {
        "ok": True,
        "records_seen": len(records),
        "written": written,
        "skipped": skipped,
        "errors": errors,
        "delays_computed": delays_computed,
        "gtfs_loaded": gtfs is not None,
    }
    logger.info(json.dumps(summary))
    return summary
