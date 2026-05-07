"""Enrichment Lambda — Kinesis trigger → DynamoDB hot-vehicles.

Phase 2: minimal version. Decode each Kinesis record, compute geohash from
lat/lon, write to hot-vehicles. Real schedule-deviation enrichment lands in
Phase 4 — for now `delay_seconds` stays null.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("HOT_VEHICLES_TABLE_NAME", "")
GEOHASH_PRECISION = int(os.environ.get("GEOHASH_PRECISION", "6"))
TTL_SECONDS = int(os.environ.get("HOT_VEHICLE_TTL_SECONDS", "3600"))

_dynamodb = None
_table = None

GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def encode_geohash(lat: float, lon: float, precision: int = GEOHASH_PRECISION) -> str:
    """Encode (lat, lon) to a base32 geohash string of the given precision.

    Implementation follows the standard geohash algorithm (gustavo niemeyer).
    Pure-python so we don't bundle a C extension.
    """
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


def decode_kinesis_record(record: dict[str, Any]) -> dict[str, Any]:
    """Kinesis events arrive base64-encoded under record['kinesis']['data']."""
    raw = base64.b64decode(record["kinesis"]["data"])
    return json.loads(raw)


def to_dynamo_item(event: dict[str, Any]) -> dict[str, Any] | None:
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
        "lat": str(lat),  # DynamoDB doesn't accept Python floats; use Decimal-friendly strings
        "lon": str(lon),
        "last_updated": iso_ts,
        "ttl_epoch": now + TTL_SECONDS,
        # Phase 2: no enrichment yet. delay_seconds populated in Phase 4.
        "delay_seconds": None,
    }
    # Only include route_id when populated. The route_id-last_updated-index GSI
    # rejects items whose key attribute is an empty string; out-of-service
    # vehicles (deadheading, layover) have route_id="" and would fail the
    # whole BatchWriteItem if we set it. Omitting the attribute means the
    # item simply won't be indexed by route_id, which is what we want.
    route_id = event.get("route_id")
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

    table = get_table()
    # batch_writer auto-batches up to 25 items per WriteRequest and retries
    # unprocessed items.
    with table.batch_writer(overwrite_by_pkeys=["geohash", "vehicle_id"]) as batch:
        for record in records:
            try:
                parsed = decode_kinesis_record(record)
                item = to_dynamo_item(parsed)
                if item is None:
                    skipped += 1
                    continue
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
    }
    logger.info(json.dumps(summary))
    return summary
