"""Authenticated user-api Lambda.

Routes (all behind a Cognito User Pool authorizer on API Gateway):

    GET    /geofences                       list the caller's geofences
    POST   /geofences                       create one
    DELETE /geofences/{geofenceId}          delete one
    GET    /notifications                   list recent notifications (newest first)
    PATCH  /notifications/{notificationId}  mark one read
    GET    /me                              read the caller's profile/prefs
    PUT    /me                              update email_alerts_enabled

The caller's identity is ALWAYS the verified `sub` claim from the Cognito
authorizer — never anything in the request body. That's the whole security
model: a user can only ever touch rows under their own user_id partition.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USERS_TABLE = os.environ.get("USERS_TABLE_NAME", "")
GEOFENCES_TABLE = os.environ.get("GEOFENCES_TABLE_NAME", "")
NOTIFICATIONS_TABLE = os.environ.get("NOTIFICATIONS_TABLE_NAME", "")

# Allowed thresholds match the frontend dropdown (3/5/10 min). Enforce a sane
# range server-side regardless of what the client sends.
MIN_THRESHOLD_SECONDS = 60
MAX_THRESHOLD_SECONDS = 3600
NOTIFICATIONS_LIMIT = 50

_ddb = None
_users_t = None
_geofences_t = None
_notifications_t = None


def _table(name: str):
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb.Table(name)


def _users():
    global _users_t
    if _users_t is None:
        _users_t = _table(USERS_TABLE)
    return _users_t


def _geofences():
    global _geofences_t
    if _geofences_t is None:
        _geofences_t = _table(GEOFENCES_TABLE)
    return _geofences_t


def _notifications():
    global _notifications_t
    if _notifications_t is None:
        _notifications_t = _table(NOTIFICATIONS_TABLE)
    return _notifications_t


def new_geofence_id() -> str:
    return f"gf-{uuid.uuid4().hex[:12]}"


def _json_default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return int(o) if o == int(o) else float(o)
    return str(o)


def _response(status: int, body: dict | list | None = None) -> dict:
    out = {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
        "body": "" if body is None else json.dumps(body, default=_json_default),
    }
    return out


def user_id_from_event(event: dict[str, Any]) -> str | None:
    claims = (
        (event.get("requestContext") or {}).get("authorizer") or {}
    ).get("claims") or {}
    return claims.get("sub")


def _parse_body(event: dict[str, Any]) -> dict:
    raw = event.get("body")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


# ----- /geofences -----

def list_geofences(user_id: str) -> dict:
    resp = _geofences().query(KeyConditionExpression=Key("user_id").eq(user_id))
    items = resp.get("Items", [])
    return _response(200, {"count": len(items), "geofences": items})


def create_geofence(user_id: str, body: dict) -> dict:
    route_id = (body.get("route_id") or "").strip()
    if not route_id:
        return _response(400, {"error": "missing_route_id"})
    try:
        threshold = int(body.get("threshold_seconds"))
    except (TypeError, ValueError):
        return _response(400, {"error": "invalid_threshold"})
    if not (MIN_THRESHOLD_SECONDS <= threshold <= MAX_THRESHOLD_SECONDS):
        return _response(400, {"error": "threshold_out_of_range"})

    geofence_id = new_geofence_id()
    item = {
        "user_id": user_id,
        "geofence_id": geofence_id,
        "route_id": route_id,
        "stop_id": None,  # reserved for v2 per-stop directional geofences
        "threshold_seconds": threshold,
        "label": (body.get("label") or f"Route {route_id}")[:120],
        "enabled": True,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_alerted_epoch": 0,
    }
    _geofences().put_item(Item=item)
    return _response(201, {"geofence_id": geofence_id, "geofence": item})


def delete_geofence(user_id: str, geofence_id: str) -> dict:
    if not geofence_id:
        return _response(400, {"error": "missing_geofence_id"})
    _geofences().delete_item(Key={"user_id": user_id, "geofence_id": geofence_id})
    return _response(204)


# ----- /notifications -----

def list_notifications(user_id: str) -> dict:
    resp = _notifications().query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False,  # newest first
        Limit=NOTIFICATIONS_LIMIT,
    )
    items = resp.get("Items", [])
    out = []
    unread = 0
    for it in items:
        is_read = bool(it.get("read"))
        if not is_read:
            unread += 1
        out.append({
            "id": it.get("created_at"),
            "route_id": it.get("route_id"),
            "delay_seconds": it.get("delay_seconds"),
            "threshold_seconds": it.get("threshold_seconds"),
            "message": it.get("message"),
            "read": is_read,
            "created_at": it.get("created_at"),
        })
    return _response(200, {"unread_count": unread, "notifications": out})


def mark_notification_read(user_id: str, notification_id: str) -> dict:
    if not notification_id:
        return _response(400, {"error": "missing_notification_id"})
    _notifications().update_item(
        Key={"user_id": user_id, "created_at": notification_id},
        UpdateExpression="SET #r = :true",
        ExpressionAttributeNames={"#r": "read"},
        ExpressionAttributeValues={":true": True},
    )
    return _response(200, {"id": notification_id, "read": True})


# ----- /me -----

def get_me(user_id: str, email: str) -> dict:
    resp = _users().get_item(Key={"user_id": user_id})
    item = resp.get("Item")
    if not item:
        # The PostConfirmation trigger normally seeds this; fall back gracefully.
        item = {"user_id": user_id, "email": email, "email_alerts_enabled": False, "home_routes": []}
    return _response(200, item)


def put_me(user_id: str, body: dict) -> dict:
    enabled = body.get("email_alerts_enabled")
    if not isinstance(enabled, bool):
        return _response(400, {"error": "invalid_email_alerts_enabled"})
    _users().update_item(
        Key={"user_id": user_id},
        UpdateExpression="SET email_alerts_enabled = :v",
        ExpressionAttributeValues={":v": enabled},
    )
    return _response(200, {"email_alerts_enabled": enabled})


def lambda_handler(event: dict[str, Any], context: Any) -> dict:
    user_id = user_id_from_event(event)
    if not user_id:
        return _response(401, {"error": "unauthenticated"})

    resource = event.get("resource") or ""
    method = (event.get("httpMethod") or "").upper()
    path_params = event.get("pathParameters") or {}
    claims = ((event.get("requestContext") or {}).get("authorizer") or {}).get("claims") or {}

    logger.info(json.dumps({"resource": resource, "method": method}))

    if resource == "/geofences":
        if method == "GET":
            return list_geofences(user_id)
        if method == "POST":
            return create_geofence(user_id, _parse_body(event))
    elif resource == "/geofences/{geofenceId}":
        if method == "DELETE":
            return delete_geofence(user_id, path_params.get("geofenceId") or "")
    elif resource == "/notifications":
        if method == "GET":
            return list_notifications(user_id)
    elif resource == "/notifications/{notificationId}":
        if method == "PATCH":
            return mark_notification_read(user_id, path_params.get("notificationId") or "")
    elif resource == "/me":
        if method == "GET":
            return get_me(user_id, claims.get("email", ""))
        if method == "PUT":
            return put_me(user_id, _parse_body(event))

    return _response(404, {"error": "not_found", "resource": resource, "method": method})
