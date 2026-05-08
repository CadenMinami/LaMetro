"""WebSocket fan-out helper for the enrichment Lambda.

After each Kinesis batch lands in `hot-vehicles`, the enrichment Lambda
calls `fan_out()` to deliver the batch's vehicle updates to every
matching subscriber over WebSocket.

Match rules per connection:
  - `subscribed_bbox` is required. Connections without one (haven't sent a
    `subscribe` yet) get nothing — they're only kept alive by the connect
    handler so the disconnect handler has a row to delete.
  - `subscribed_route_id` is optional; when set, only vehicles on that
    route_id are sent.

We cache the connection list in module memory and refresh every
`CONNECTIONS_CACHE_TTL_S` seconds. With ~10 concurrent users, the table
is tiny so a full scan once a minute is fine.

Stale connections (returned a `GoneException` from the management API)
are deleted in a `BatchWriteItem` after the broadcast.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

CONNECTIONS_CACHE_TTL_S = 60.0


@dataclass(frozen=True, slots=True)
class Connection:
    """A subscriber's filter, parsed from a connections-table row."""
    connection_id: str
    bbox: tuple[float, float, float, float] | None  # (minLon, minLat, maxLon, maxLat)
    route_id: str | None


@dataclass(slots=True)
class _Cache:
    """Scoped cache so unit tests can reset it without touching globals."""
    connections: list[Connection]
    fetched_at: float


_cache: _Cache = _Cache([], 0.0)
_ddb_resource = None
_management_clients: dict[str, Any] = {}


def _ddb():
    global _ddb_resource
    if _ddb_resource is None:
        _ddb_resource = boto3.resource("dynamodb")
    return _ddb_resource


def _management_client(callback_url: str):
    """One client per callback URL. Lambda can be reused across deploys
    that change WebSocket APIs (rare); this avoids constructing a new
    boto3 client on every batch."""
    client = _management_clients.get(callback_url)
    if client is None:
        client = boto3.client("apigatewaymanagementapi", endpoint_url=callback_url)
        _management_clients[callback_url] = client
    return client


def _row_to_connection(row: dict[str, Any]) -> Connection | None:
    cid = row.get("connection_id")
    if not cid:
        return None
    bbox_raw = row.get("subscribed_bbox") or {}
    try:
        bbox = (
            float(bbox_raw["minLon"]),
            float(bbox_raw["minLat"]),
            float(bbox_raw["maxLon"]),
            float(bbox_raw["maxLat"]),
        )
    except (KeyError, ValueError, TypeError):
        bbox = None
    route_id = row.get("subscribed_route_id") or None
    return Connection(connection_id=cid, bbox=bbox, route_id=route_id)


def _scan_connections(table_name: str) -> list[Connection]:
    table = _ddb().Table(table_name)
    out: list[Connection] = []
    last_key = None
    while True:
        kwargs: dict[str, Any] = {}
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**kwargs)
        for row in resp.get("Items", []):
            conn = _row_to_connection(row)
            if conn is not None:
                out.append(conn)
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            return out


def get_connections(
    table_name: str,
    *,
    now: float | None = None,
    force_refresh: bool = False,
) -> list[Connection]:
    """Return the current subscriber list, refreshed at most once per
    cache TTL window."""
    if now is None:
        now = time.monotonic()
    if force_refresh or now - _cache.fetched_at > CONNECTIONS_CACHE_TTL_S:
        _cache.connections = _scan_connections(table_name)
        _cache.fetched_at = now
    return _cache.connections


def reset_cache() -> None:
    """Test helper. Production code never calls this — the TTL refreshes."""
    _cache.connections = []
    _cache.fetched_at = 0.0


def _vehicle_matches(
    v: dict[str, Any], conn: Connection
) -> bool:
    """Bbox containment + optional route filter. The vehicle dict mirrors
    what the enrichment Lambda writes to DDB (lat/lon may be strings —
    the resource API stringifies floats via the Decimal path)."""
    if conn.bbox is None:
        return False
    if conn.route_id and v.get("route_id") != conn.route_id:
        return False
    try:
        lat = float(v["lat"])
        lon = float(v["lon"])
    except (KeyError, ValueError, TypeError):
        return False
    min_lon, min_lat, max_lon, max_lat = conn.bbox
    return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat


def _to_wire(v: dict[str, Any]) -> dict[str, Any]:
    """Strip DynamoDB-specific types (Decimal) and shape what the browser
    needs. Mirrors the field set the REST /vehicles endpoint returns."""
    def num(x: Any) -> Any:
        if isinstance(x, Decimal):
            return int(x) if x == int(x) else float(x)
        return x
    return {
        "vehicle_id": v.get("vehicle_id"),
        "route_id": v.get("route_id") or "",
        "trip_id": v.get("trip_id") or "",
        "lat": float(v["lat"]),
        "lon": float(v["lon"]),
        "bearing": float(v["bearing"]) if v.get("bearing") not in (None, "") else None,
        "speed_mps": float(v["speed_mps"]) if v.get("speed_mps") not in (None, "") else None,
        "delay_seconds": num(v.get("delay_seconds")),
        "last_updated": v.get("last_updated"),
    }


def fan_out(
    vehicles: list[dict[str, Any]],
    *,
    callback_url: str,
    connections_table: str,
    now: float | None = None,
) -> dict[str, int]:
    """Deliver this batch's vehicles to each matching subscriber.

    Returns a small summary dict the caller can log:
      sent       — count of WebSocket POSTs that succeeded
      stale      — connections we removed because they 410'd
      skipped    — connections that matched zero vehicles in this batch

    Errors other than `GoneException` are logged and swallowed — one bad
    socket shouldn't fail the whole enrichment batch.
    """
    if not vehicles:
        return {"sent": 0, "stale": 0, "skipped": 0}

    connections = get_connections(connections_table, now=now)
    if not connections:
        return {"sent": 0, "stale": 0, "skipped": 0}

    client = _management_client(callback_url)
    sent = 0
    stale_ids: list[str] = []
    skipped = 0
    for conn in connections:
        matched = [v for v in vehicles if _vehicle_matches(v, conn)]
        if not matched:
            skipped += 1
            continue
        payload = {
            "type": "positions",
            "vehicles": [_to_wire(v) for v in matched],
        }
        try:
            client.post_to_connection(
                ConnectionId=conn.connection_id,
                Data=json.dumps(payload).encode("utf-8"),
            )
            sent += 1
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("GoneException", "410"):
                stale_ids.append(conn.connection_id)
            else:
                logger.warning(
                    "ws_post_failed connection=%s code=%s", conn.connection_id, code
                )

    if stale_ids:
        _delete_stale(connections_table, stale_ids)

    return {"sent": sent, "stale": len(stale_ids), "skipped": skipped}


def _delete_stale(table_name: str, connection_ids: Iterable[str]) -> None:
    """BatchWriteItem can delete up to 25 items at a time. We chunk
    accordingly. Cache is invalidated so the next fan_out re-scans without
    the dead rows."""
    ids = list(connection_ids)
    if not ids:
        return
    table = _ddb().Table(table_name)
    with table.batch_writer() as batch:
        for cid in ids:
            batch.delete_item(Key={"connection_id": cid})
    # Force a refresh on the next call — these rows were in our cache.
    _cache.fetched_at = 0.0
