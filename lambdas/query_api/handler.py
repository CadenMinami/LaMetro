"""Query Lambda — backs the public REST API.

Endpoints:

    GET /vehicles?bbox=lon_min,lat_min,lon_max,lat_max[&route_id=…&limit=…]
        Returns active vehicles in the bbox. delay_seconds is included when
        the enrichment Lambda has computed one.

    GET /routes/{routeId}/aggregates?[since=ISO][&limit=N]
        Returns recent 5-min-bucket aggregates for one route. Newest first.
        Drives the route detail page in the dashboard.

API Gateway dispatches based on `event['resource']`. Path params arrive in
`event['pathParameters']`; query params in `event['queryStringParameters']`.
"""

from __future__ import annotations

import json
import logging
import os
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HOT_TABLE_NAME = os.environ.get("HOT_VEHICLES_TABLE_NAME", "")
AGG_TABLE_NAME = os.environ.get("ROUTE_AGGREGATES_TABLE_NAME", "")
GEOHASH_PRECISION = int(os.environ.get("GEOHASH_PRECISION", "6"))
DEFAULT_LIMIT = 500
MAX_LIMIT = 1000
MAX_BBOX_DEG2 = 0.5
DEFAULT_AGG_LIMIT = 288  # 24h of 5-min buckets

GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

_dynamodb = None
_hot_table = None
_agg_table = None


def _hot():
    global _dynamodb, _hot_table
    if _hot_table is None:
        if not HOT_TABLE_NAME:
            raise RuntimeError("HOT_VEHICLES_TABLE_NAME env var not set")
        _dynamodb = boto3.resource("dynamodb")
        _hot_table = _dynamodb.Table(HOT_TABLE_NAME)
    return _hot_table


def _agg():
    global _dynamodb, _agg_table
    if _agg_table is None:
        if not AGG_TABLE_NAME:
            raise RuntimeError("ROUTE_AGGREGATES_TABLE_NAME env var not set")
        _dynamodb = _dynamodb or boto3.resource("dynamodb")
        _agg_table = _dynamodb.Table(AGG_TABLE_NAME)
    return _agg_table


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


_CELL_LAT_STEP = 0.0025
_CELL_LON_STEP = 0.0035


def covering_geohashes(lon_min: float, lat_min: float, lon_max: float, lat_max: float) -> set[str]:
    cells: set[str] = set()
    lat = lat_min
    while lat <= lat_max:
        lon = lon_min
        while lon <= lon_max:
            cells.add(encode_geohash(lat, lon, GEOHASH_PRECISION))
            lon += _CELL_LON_STEP
        cells.add(encode_geohash(lat, lon_max, GEOHASH_PRECISION))
        lat += _CELL_LAT_STEP
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
        "body": json.dumps(body, default=_json_default),
    }


def _json_default(o: Any) -> Any:
    """DynamoDB resource API returns numbers as Decimal. Convert to int when
    integral, else float, so JSON consumers get clean values rather than
    string-coerced Decimals from `default=str`."""
    if isinstance(o, Decimal):
        return int(o) if o == int(o) else float(o)
    return str(o)


def _vehicle_in_bbox(item: dict, lon_min: float, lat_min: float, lon_max: float, lat_max: float) -> bool:
    try:
        lat = float(item["lat"])
        lon = float(item["lon"])
    except (KeyError, ValueError, TypeError):
        return False
    return lon_min <= lon <= lon_max and lat_min <= lat <= lat_max


def _decimal_to_number(v: Any) -> Any:
    """Coerce a single Decimal value to int/float for the response payload.
    Leaves other types untouched (including None)."""
    if isinstance(v, Decimal):
        return int(v) if v == int(v) else float(v)
    return v


def handle_vehicles(event: dict[str, Any]) -> dict:
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

    table = _hot()
    cells = covering_geohashes(lon_min, lat_min, lon_max, lat_max)

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
            "delay_seconds": _decimal_to_number(item.get("delay_seconds")),
            "last_updated": item.get("last_updated"),
        })
        if len(vehicles) >= limit:
            break

    as_of = max((v["last_updated"] for v in vehicles if v.get("last_updated")), default=None)
    return _response(200, {"count": len(vehicles), "as_of": as_of, "vehicles": vehicles})


def handle_route_aggregates(event: dict[str, Any]) -> dict:
    path_params = event.get("pathParameters") or {}
    route_id = path_params.get("routeId") or ""
    if not route_id:
        return _response(400, {"error": "missing_route_id"})

    qs = event.get("queryStringParameters") or {}
    try:
        limit = min(int(qs.get("limit", DEFAULT_AGG_LIMIT)), DEFAULT_AGG_LIMIT * 2)
    except ValueError:
        limit = DEFAULT_AGG_LIMIT
    since = qs.get("since")  # optional ISO string lower bound

    if since:
        cond = Key("route_id").eq(route_id) & Key("window_start_iso").gte(since)
    else:
        cond = Key("route_id").eq(route_id)

    table = _agg()
    resp = table.query(
        KeyConditionExpression=cond,
        ScanIndexForward=False,  # newest bucket first
        Limit=limit,
    )

    windows: list[dict] = []
    for item in resp.get("Items", []):
        windows.append({
            "window_start_iso": item.get("window_start_iso"),
            "vehicle_count": _decimal_to_number(item.get("vehicle_count")),
            # These three are absent in 4b-era rows and present from 4c on.
            "avg_delay_seconds": _decimal_to_number(item.get("avg_delay_seconds")),
            "p95_delay_seconds": _decimal_to_number(item.get("p95_delay_seconds")),
            "on_time_pct": float(item["on_time_pct"]) if item.get("on_time_pct") is not None else None,
            "updated_at_iso": item.get("updated_at_iso"),
        })

    return _response(200, {"route_id": route_id, "count": len(windows), "windows": windows})


def lambda_handler(event: dict[str, Any], context: Any) -> dict:
    """Dispatch based on the API Gateway resource path. Unknown paths 404."""
    resource = event.get("resource") or event.get("path") or ""
    logger.info(json.dumps({"resource": resource, "method": event.get("httpMethod")}))

    if resource == "/vehicles":
        return handle_vehicles(event)
    if resource == "/routes/{routeId}/aggregates":
        return handle_route_aggregates(event)
    return _response(404, {"error": "not_found", "resource": resource})
