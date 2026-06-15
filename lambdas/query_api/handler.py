"""Query Lambda — backs the public REST API.

Endpoints:

    GET /vehicles?bbox=lon_min,lat_min,lon_max,lat_max[&route_id=…&limit=…]
        Returns active vehicles in the bbox. delay_seconds is included when
        the enrichment Lambda has computed one.

    GET /routes/{routeId}/aggregates?[since=ISO][&limit=N]
        Returns recent 5-min-bucket aggregates for one route. Newest first.
        Drives the route detail page in the dashboard.

    GET /routes/{routeId}/prediction
        Returns the latest precomputed next-window delay prediction for one
        route (single row from route-predictions), or 404 if none exists.

    GET /stops
        Returns the agency's full stops list (lightweight: id/name/lat/lon
        + the route_ids that visit each). Frontend caches per session.

    GET /stops/{stopId}/arrivals?[limit=N&horizon_minutes=M]
        Returns the next N scheduled arrivals at this stop, with live vehicle
        matched by trip_id when available. predicted_arrival folds in any
        delay_seconds the enrichment Lambda has set.

API Gateway dispatches based on `event['resource']`. Path params arrive in
`event['pathParameters']`; query params in `event['queryStringParameters']`.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HOT_TABLE_NAME = os.environ.get("HOT_VEHICLES_TABLE_NAME", "")
AGG_TABLE_NAME = os.environ.get("ROUTE_AGGREGATES_TABLE_NAME", "")
ROUTE_PREDICTIONS_TABLE_NAME = os.environ.get("ROUTE_PREDICTIONS_TABLE_NAME", "")
GEOHASH_PRECISION = int(os.environ.get("GEOHASH_PRECISION", "6"))
GTFS_STATIC_BUCKET = os.environ.get("GTFS_STATIC_BUCKET", "")
GTFS_STATIC_POINTER_KEY = os.environ.get("GTFS_STATIC_POINTER_KEY", "gtfs-static/current.txt")
HOT_VEHICLES_ROUTE_GSI = os.environ.get("HOT_VEHICLES_ROUTE_GSI", "route_id-last_updated-index")
DEFAULT_LIMIT = 500
MAX_LIMIT = 1000
MAX_BBOX_DEG2 = 0.5
DEFAULT_AGG_LIMIT = 288  # 24h of 5-min buckets

# Arrivals API tuning. The horizon is the wall-clock window we scan forward;
# the per-route GSI page size caps how many recent live vehicles we consider
# per route (more than enough — at most a few dozen per route at any moment).
DEFAULT_ARRIVAL_LIMIT = 5
MAX_ARRIVAL_LIMIT = 20
DEFAULT_HORIZON_MINUTES = 60
MIN_HORIZON_MINUTES = 5
MAX_HORIZON_MINUTES = 180
GSI_PAGE_SIZE = 50

# LA Metro is, well, in LA. ZoneInfo handles DST automatically — the Lambda
# build adds the `tzdata` package so this never falls back to UTC at runtime.
_LA_TZ = ZoneInfo("America/Los_Angeles")

GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

_dynamodb = None
_hot_table = None
_agg_table = None
_predictions_table = None
_gtfs_static = None


def _gtfs():
    """Lazy-load the parsed GTFS static pickle once per cold start.

    Imports `lambdas.shared.gtfs_static` lazily because that module pulls in
    boto3 and (when shapes=True) shapely; keeping the import inside the
    function means a /vehicles request never pays the cost.
    """
    global _gtfs_static
    if _gtfs_static is None:
        if not GTFS_STATIC_BUCKET:
            raise RuntimeError("GTFS_STATIC_BUCKET env var not set")
        from lambdas.shared.gtfs_static import load_from_s3
        _gtfs_static = load_from_s3(
            GTFS_STATIC_BUCKET,
            GTFS_STATIC_POINTER_KEY,
            shapes=False,  # arrivals API doesn't need geometry
        )
    return _gtfs_static


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


def _predictions():
    global _dynamodb, _predictions_table
    if _predictions_table is None:
        if not ROUTE_PREDICTIONS_TABLE_NAME:
            raise RuntimeError("ROUTE_PREDICTIONS_TABLE_NAME env var not set")
        _dynamodb = _dynamodb or boto3.resource("dynamodb")
        _predictions_table = _dynamodb.Table(ROUTE_PREDICTIONS_TABLE_NAME)
    return _predictions_table


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


def handle_route_prediction(event: dict[str, Any]) -> dict:
    path_params = event.get("pathParameters") or {}
    route_id = path_params.get("routeId") or ""
    if not route_id:
        return _response(400, {"error": "missing_route_id"})
    resp = _predictions().get_item(Key={"route_id": route_id})
    item = resp.get("Item")
    if not item:
        return _response(404, {"error": "no_prediction", "route_id": route_id})
    return _response(200, {
        "route_id": item.get("route_id"),
        "predicted_next_window_avg_delay_seconds":
            _decimal_to_number(item.get("predicted_next_window_avg_delay_seconds")),
        "current_avg_delay_seconds":
            _decimal_to_number(item.get("current_avg_delay_seconds")),
        "model_version": item.get("model_version"),
        "window_start_iso": item.get("window_start_iso"),
        "as_of": item.get("as_of"),
    })


def _routes_per_stop(static) -> dict[str, list[str]]:
    """Pivot stop_arrivals into a stable {stop_id: [route_id, ...]} mapping.

    Cached on the static object after first build so repeat /stops calls in
    the same warm container don't re-walk ~2M arrival rows. Stop_arrivals
    rarely changes within a session, and the result is ~13k stops × a few
    routes each.
    """
    cached = getattr(static, "_routes_per_stop", None)
    if cached is not None:
        return cached
    out: dict[str, list[str]] = {}
    for stop_id, rows in static.stop_arrivals.items():
        seen: set[str] = set()
        ordered: list[str] = []
        for _trip_id, route_id, _arr_s, _seq in rows:
            if route_id and route_id not in seen:
                seen.add(route_id)
                ordered.append(route_id)
        out[stop_id] = ordered
    # Stash on the static object — frozen=False on GTFSStatic so this is fine.
    try:
        object.__setattr__(static, "_routes_per_stop", out)
    except Exception:
        pass
    return out


def handle_list_stops(event: dict[str, Any]) -> dict:
    """GET /stops — full agency stops list. Cacheable per feed_version."""
    try:
        static = _gtfs()
    except Exception:
        logger.exception("gtfs_load_failed")
        return _response(503, {"error": "gtfs_unavailable"})

    routes_per_stop = _routes_per_stop(static)
    payload: list[dict[str, Any]] = []
    for stop_id, meta in static.stops.items():
        payload.append({
            "id": stop_id,
            "name": meta.get("name", ""),
            "lat": meta.get("lat"),
            "lon": meta.get("lon"),
            "routes": routes_per_stop.get(stop_id, []),
        })
    return _response(200, {
        "version": static.feed_version,
        "count": len(payload),
        "stops": payload,
    })


def _iso_z(when: dt.datetime) -> str:
    """ISO-8601 with trailing Z, in UTC. Matches what the rest of the API uses."""
    return when.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _live_vehicles_for_routes(table, route_ids: set[str]) -> dict[str, dict[str, Any]]:
    """Fan-out the route_id GSI for each route. Returns {trip_id: latest_item}."""
    by_trip: dict[str, dict[str, Any]] = {}
    for route_id in route_ids:
        if not route_id:
            continue
        resp = table.query(
            IndexName=HOT_VEHICLES_ROUTE_GSI,
            KeyConditionExpression=Key("route_id").eq(route_id),
            ScanIndexForward=False,  # newest last_updated first
            Limit=GSI_PAGE_SIZE,
        )
        for item in resp.get("Items", []):
            trip_id = item.get("trip_id") or ""
            if not trip_id:
                continue
            existing = by_trip.get(trip_id)
            # Keep the freshest item per trip — same vehicle's stale rows can
            # appear in the index until TTL fires.
            if existing and (existing.get("last_updated") or "") >= (item.get("last_updated") or ""):
                continue
            by_trip[trip_id] = item
    return by_trip


def handle_stop_arrivals(event: dict[str, Any]) -> dict:
    """GET /stops/{stopId}/arrivals — top-N upcoming arrivals at one stop."""
    path_params = event.get("pathParameters") or {}
    stop_id = path_params.get("stopId") or ""
    if not stop_id:
        return _response(400, {"error": "missing_stop_id"})

    qs = event.get("queryStringParameters") or {}
    try:
        limit = int(qs.get("limit", DEFAULT_ARRIVAL_LIMIT))
    except (TypeError, ValueError):
        limit = DEFAULT_ARRIVAL_LIMIT
    limit = max(1, min(limit, MAX_ARRIVAL_LIMIT))

    try:
        horizon_minutes = int(qs.get("horizon_minutes", DEFAULT_HORIZON_MINUTES))
    except (TypeError, ValueError):
        return _response(400, {"error": "invalid_horizon"})
    if horizon_minutes < MIN_HORIZON_MINUTES or horizon_minutes > MAX_HORIZON_MINUTES:
        return _response(400, {"error": "invalid_horizon"})

    try:
        static = _gtfs()
    except Exception:
        logger.exception("gtfs_load_failed")
        return _response(503, {"error": "gtfs_unavailable"})

    if stop_id not in static.stops:
        return _response(404, {"error": "stop_not_found", "stop_id": stop_id})

    now_utc = dt.datetime.now(dt.timezone.utc)
    now_local = now_utc.astimezone(_LA_TZ)
    horizon_seconds = horizon_minutes * 60

    candidates = static.arrivals_for_stop(stop_id, now_local, horizon_seconds)

    routes_needed = {a.route_id for a in candidates if a.route_id}
    live_by_trip = _live_vehicles_for_routes(_hot(), routes_needed) if routes_needed else {}

    arrivals_out: list[dict[str, Any]] = []
    for arr in candidates:
        wall_scheduled = static.absolute_arrival_time(arr, _LA_TZ)
        live = live_by_trip.get(arr.trip_id)
        delay_seconds: int | None = None
        vehicle_id: str | None = None
        if live is not None:
            vehicle_id = live.get("vehicle_id") or None
            raw_delay = live.get("delay_seconds")
            if isinstance(raw_delay, Decimal):
                delay_seconds = int(raw_delay)
            elif isinstance(raw_delay, (int, float)):
                delay_seconds = int(raw_delay)

        wall_predicted = wall_scheduled + dt.timedelta(seconds=delay_seconds or 0)
        delta_seconds = (wall_predicted - now_utc).total_seconds()
        # `due` = within the lookback window or about to arrive (<60s).
        # `departed` = already passed (only possible inside lookback).
        if live is not None:
            if delta_seconds < 0:
                status = "departed"
            elif delta_seconds < 60:
                status = "due"
            else:
                status = "live"
        else:
            status = "scheduled"

        arrivals_out.append({
            "route_id": arr.route_id,
            "trip_id": arr.trip_id,
            "scheduled_arrival": _iso_z(wall_scheduled),
            "predicted_arrival": _iso_z(wall_predicted),
            "predicted_minutes": int(delta_seconds // 60) if delta_seconds >= 0
                                  else -int((-delta_seconds + 59) // 60),
            "delay_seconds": delay_seconds,
            "status": status,
            "vehicle_id": vehicle_id,
            "stop_sequence": arr.stop_sequence,
        })

    arrivals_out.sort(key=lambda a: a["predicted_arrival"])
    arrivals_out = arrivals_out[:limit]

    stop_meta = static.stops.get(stop_id, {})
    return _response(200, {
        "stop_id": stop_id,
        "stop_name": stop_meta.get("name", ""),
        "as_of": _iso_z(now_utc),
        "horizon_minutes": horizon_minutes,
        "arrivals": arrivals_out,
    })


def lambda_handler(event: dict[str, Any], context: Any) -> dict:
    """Dispatch based on the API Gateway resource path. Unknown paths 404."""
    resource = event.get("resource") or event.get("path") or ""
    logger.info(json.dumps({"resource": resource, "method": event.get("httpMethod")}))

    if resource == "/vehicles":
        return handle_vehicles(event)
    if resource == "/routes/{routeId}/aggregates":
        return handle_route_aggregates(event)
    if resource == "/routes/{routeId}/prediction":
        return handle_route_prediction(event)
    if resource == "/stops":
        return handle_list_stops(event)
    if resource == "/stops/{stopId}/arrivals":
        return handle_stop_arrivals(event)
    return _response(404, {"error": "not_found", "resource": resource})
