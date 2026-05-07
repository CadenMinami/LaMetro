"""Query Lambda — GET /vehicles?bbox=...

Phase 2 strategy: compute the set of geohash prefixes that cover the bbox at
a precision one shorter than our partition key, then Query each partition
with a `begins_with(geohash, prefix)` would be expensive — instead we
enumerate all precision-6 geohash cells inside the bbox and Query each.

For a city-scale bbox this is ~50-200 partitions, fast in parallel via
boto3's threaded sessions. For Phase 2 we keep it sequential and simple;
optimize later if p95 latency becomes a concern.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("HOT_VEHICLES_TABLE_NAME", "")
GEOHASH_PRECISION = int(os.environ.get("GEOHASH_PRECISION", "6"))
DEFAULT_LIMIT = 500
MAX_LIMIT = 1000
# Upper bound on bbox area in degrees² (~50km × 50km ≈ 0.45° × 0.45° at LA latitude).
MAX_BBOX_DEG2 = 0.5

GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

_dynamodb = None
_table = None


def _table_handle():
    global _dynamodb, _table
    if _table is None:
        if not TABLE_NAME:
            raise RuntimeError("HOT_VEHICLES_TABLE_NAME env var not set")
        _dynamodb = boto3.resource("dynamodb")
        _table = _dynamodb.Table(TABLE_NAME)
    return _table


def encode_geohash(lat: float, lon: float, precision: int = GEOHASH_PRECISION) -> str:
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    bits: list[int] = []
    even = True
    while len(bits) < precision * 5:
        if even:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon >= mid:
                bits.append(1); lon_range[0] = mid
            else:
                bits.append(0); lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                bits.append(1); lat_range[0] = mid
            else:
                bits.append(0); lat_range[1] = mid
        even = not even
    out = []
    for i in range(0, len(bits), 5):
        c = bits[i : i + 5]
        idx = (c[0] << 4) | (c[1] << 3) | (c[2] << 2) | (c[3] << 1) | c[4]
        out.append(GEOHASH_BASE32[idx])
    return "".join(out)


# Precision-6 geohash cells at LA latitude (~34°) are ~0.0055° tall × 0.0070°
# wide. Sample at HALF the cell size in each axis (Nyquist-style) to guarantee
# every cell intersecting the bbox is sampled at least once. A coarser step
# misses cells along the edges; a finer step adds DDB queries with no benefit.
_CELL_LAT_STEP = 0.0025
_CELL_LON_STEP = 0.0035


def covering_geohashes(lon_min: float, lat_min: float, lon_max: float, lat_max: float) -> set[str]:
    """Sample lat/lon grid points across the bbox and collect their geohashes.

    Includes the bbox edges explicitly so we never miss a cell that the bbox
    barely crosses into.
    """
    cells: set[str] = set()
    lat = lat_min
    while lat <= lat_max:
        lon = lon_min
        while lon <= lon_max:
            cells.add(encode_geohash(lat, lon, GEOHASH_PRECISION))
            lon += _CELL_LON_STEP
        # right edge of this row
        cells.add(encode_geohash(lat, lon_max, GEOHASH_PRECISION))
        lat += _CELL_LAT_STEP
    # top edge
    lon = lon_min
    while lon <= lon_max:
        cells.add(encode_geohash(lat_max, lon, GEOHASH_PRECISION))
        lon += _CELL_LON_STEP
    cells.add(encode_geohash(lat_max, lon_max, GEOHASH_PRECISION))
    return cells


def _parse_bbox(raw: str) -> tuple[float, float, float, float]:
    parts = [float(x) for x in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must have 4 comma-separated floats")
    lon_min, lat_min, lon_max, lat_max = parts
    if not (-180 <= lon_min < lon_max <= 180):
        raise ValueError("invalid lon range")
    if not (-90 <= lat_min < lat_max <= 90):
        raise ValueError("invalid lat range")
    return lon_min, lat_min, lon_max, lat_max


def _response(status: int, body: dict | list) -> dict:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
        },
        "body": json.dumps(body, default=str),
    }


def _vehicle_in_bbox(item: dict, lon_min: float, lat_min: float, lon_max: float, lat_max: float) -> bool:
    try:
        lat = float(item["lat"])
        lon = float(item["lon"])
    except (KeyError, ValueError, TypeError):
        return False
    return lon_min <= lon <= lon_max and lat_min <= lat <= lat_max


def lambda_handler(event: dict[str, Any], context: Any) -> dict:
    qs = event.get("queryStringParameters") or {}
    bbox_raw = qs.get("bbox")
    if not bbox_raw:
        return _response(400, {"error": "missing_bbox"})

    try:
        lon_min, lat_min, lon_max, lat_max = _parse_bbox(bbox_raw)
    except ValueError:
        return _response(400, {"error": "invalid_bbox"})

    bbox_area = (lon_max - lon_min) * (lat_max - lat_min)
    if bbox_area > MAX_BBOX_DEG2:
        return _response(400, {"error": "bbox_too_large"})

    route_filter = qs.get("route_id")
    try:
        limit = min(int(qs.get("limit", DEFAULT_LIMIT)), MAX_LIMIT)
    except ValueError:
        limit = DEFAULT_LIMIT

    table = _table_handle()
    cells = covering_geohashes(lon_min, lat_min, lon_max, lat_max)
    logger.info(json.dumps({"cells": len(cells), "bbox": bbox_raw}))

    # Same vehicle can have stale rows in old geohash cells until TTL fires
    # (up to ~1h). Collect everything keyed by vehicle_id, then keep only the
    # most recent last_updated per vehicle.
    by_vehicle: dict[str, dict] = {}
    for cell in cells:
        resp = table.query(KeyConditionExpression=Key("geohash").eq(cell))
        for item in resp.get("Items", []):
            if not _vehicle_in_bbox(item, lon_min, lat_min, lon_max, lat_max):
                continue
            if route_filter and item.get("route_id") != route_filter:
                continue
            vehicle_id = item.get("vehicle_id")
            if not vehicle_id:
                continue
            existing = by_vehicle.get(vehicle_id)
            if existing and (existing.get("last_updated") or "") >= (item.get("last_updated") or ""):
                continue
            by_vehicle[vehicle_id] = item

    vehicles: list[dict] = []
    for item in by_vehicle.values():
        vehicles.append({
            "vehicle_id": item.get("vehicle_id"),
            "route_id": item.get("route_id") or "",
            "trip_id": item.get("trip_id") or "",
            "lat": float(item["lat"]),
            "lon": float(item["lon"]),
            "bearing": float(item["bearing"]) if item.get("bearing") else None,
            "speed_mps": float(item["speed_mps"]) if item.get("speed_mps") else None,
            "delay_seconds": item.get("delay_seconds"),
            "last_updated": item.get("last_updated"),
        })
        if len(vehicles) >= limit:
            break

    # as_of: most recent last_updated across the result set
    as_of = max((v["last_updated"] for v in vehicles if v.get("last_updated")), default=None)
    return _response(200, {
        "count": len(vehicles),
        "as_of": as_of,
        "vehicles": vehicles,
    })
