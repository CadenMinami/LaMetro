"""WebSocket connection-manager Lambda.

API Gateway WebSocket APIs route every frame to a Lambda based on the
`routeKey`. We dispatch on three keys:

    $connect       — new WebSocket opens. Write a row in
                     websocket-connections with a 2h TTL. Subscription is
                     empty until the client sends a `subscribe` action.

    $disconnect    — client closed (clean) or timeout (idle). Delete the row.

    subscribe      — client tells us which bbox / optional route_id it cares
                     about. We update the row in place. The Enrichment
                     Lambda's broadcast scan reads these fields to filter
                     which positions land on which client.

The body for `subscribe` looks like:
    {"action": "subscribe",
     "bbox": {"minLon": -118.40, "minLat": 33.95,
              "maxLon": -118.15, "maxLat": 34.15},
     "route_id": "720-13196"}    # optional
"""

from __future__ import annotations

import json
import logging
import os
import time
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CONNECTIONS_TABLE = os.environ.get("CONNECTIONS_TABLE_NAME", "")
TTL_SECONDS = int(os.environ.get("CONNECTION_TTL_SECONDS", str(2 * 60 * 60)))

_dynamodb = None
_table = None


def _get_table():
    global _dynamodb, _table
    if _table is None:
        if not CONNECTIONS_TABLE:
            raise RuntimeError("CONNECTIONS_TABLE_NAME env var not set")
        _dynamodb = boto3.resource("dynamodb")
        _table = _dynamodb.Table(CONNECTIONS_TABLE)
    return _table


def _ok(body: dict | None = None) -> dict:
    return {"statusCode": 200, "body": json.dumps(body or {"ok": True})}


def _err(code: int, msg: str) -> dict:
    return {"statusCode": code, "body": json.dumps({"error": msg})}


def handle_connect(connection_id: str) -> dict:
    """Write a fresh row. The subscribe handler will fill in filter fields."""
    now = int(time.time())
    _get_table().put_item(
        Item={
            "connection_id": connection_id,
            "connected_at": now,
            "ttl_epoch": now + TTL_SECONDS,
        }
    )
    logger.info(json.dumps({"action": "connect", "connection_id": connection_id}))
    return _ok()


def handle_disconnect(connection_id: str) -> dict:
    try:
        _get_table().delete_item(Key={"connection_id": connection_id})
    except ClientError:
        # Table missing or transient error — disconnect already happened on
        # the gateway side, so we don't fail the response.
        logger.exception("disconnect_delete_failed")
    logger.info(json.dumps({"action": "disconnect", "connection_id": connection_id}))
    return _ok()


def _parse_bbox(raw: dict | None) -> dict | None:
    """Validate the {minLon, minLat, maxLon, maxLat} shape. Returns the dict
    coerced to floats, or None if the shape is unusable."""
    if not isinstance(raw, dict):
        return None
    try:
        # Validate as floats first — math is easier — then convert to
        # Decimal for DynamoDB. The resource API rejects native floats.
        floats = {
            "minLon": float(raw["minLon"]),
            "minLat": float(raw["minLat"]),
            "maxLon": float(raw["maxLon"]),
            "maxLat": float(raw["maxLat"]),
        }
    except (KeyError, ValueError, TypeError):
        return None
    if not (-180 <= floats["minLon"] < floats["maxLon"] <= 180):
        return None
    if not (-90 <= floats["minLat"] < floats["maxLat"] <= 90):
        return None
    if (floats["maxLon"] - floats["minLon"]) * (floats["maxLat"] - floats["minLat"]) > 0.5:
        return None
    # str() round-trip avoids the spurious-precision Decimal-from-float
    # warning that boto3 emits.
    return {k: Decimal(str(v)) for k, v in floats.items()}


def handle_subscribe(connection_id: str, body: dict) -> dict:
    bbox = _parse_bbox(body.get("bbox"))
    if bbox is None:
        return _err(400, "invalid_bbox")
    route_id = body.get("route_id")
    if route_id is not None and not isinstance(route_id, str):
        return _err(400, "invalid_route_id")

    expr_values: dict[str, Any] = {":bbox": bbox, ":updated": int(time.time())}
    update = "SET subscribed_bbox = :bbox, subscribed_at = :updated"
    if route_id:
        update += ", subscribed_route_id = :route_id"
        expr_values[":route_id"] = route_id
    else:
        # Drop the filter when the client sends no route_id. Use REMOVE in a
        # second clause; DynamoDB allows both SET and REMOVE in one update.
        update += " REMOVE subscribed_route_id"

    try:
        _get_table().update_item(
            Key={"connection_id": connection_id},
            UpdateExpression=update,
            ExpressionAttributeValues=expr_values,
        )
    except ClientError as exc:
        logger.exception("subscribe_update_failed")
        return _err(500, str(exc))

    # Don't put Decimal `bbox` in the log/response — json.dumps doesn't
    # encode Decimal natively. Stringify before logging; echo only the
    # fields the client cares about.
    logger.info(
        json.dumps(
            {
                "action": "subscribe",
                "connection_id": connection_id,
                "bbox": {k: float(v) for k, v in bbox.items()},
                "route_id": route_id,
            }
        )
    )
    return _ok({"ok": True, "route_id": route_id})


def lambda_handler(event: dict[str, Any], context: Any) -> dict:
    ctx = event.get("requestContext") or {}
    route_key = ctx.get("routeKey", "")
    connection_id = ctx.get("connectionId", "")
    if not connection_id:
        return _err(400, "missing_connection_id")

    if route_key == "$connect":
        return handle_connect(connection_id)
    if route_key == "$disconnect":
        return handle_disconnect(connection_id)
    if route_key == "subscribe":
        try:
            body = json.loads(event.get("body") or "{}")
        except json.JSONDecodeError:
            return _err(400, "invalid_json")
        if not isinstance(body, dict):
            return _err(400, "invalid_body")
        return handle_subscribe(connection_id, body)

    return _err(400, f"unknown_route_key:{route_key}")
