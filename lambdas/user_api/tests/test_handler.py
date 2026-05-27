"""Unit tests for the authenticated user-api Lambda."""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from lambdas.user_api import handler


def _event(resource, method, *, sub="user-1", body=None, path_params=None):
    return {
        "resource": resource,
        "httpMethod": method,
        "pathParameters": path_params,
        "body": json.dumps(body) if body is not None else None,
        "requestContext": {"authorizer": {"claims": {"sub": sub, "email": "r@x.com"}}},
    }


def test_user_id_from_claims():
    ev = _event("/me", "GET", sub="abc")
    assert handler.user_id_from_event(ev) == "abc"


def test_user_id_missing_claims_returns_none():
    assert handler.user_id_from_event({"requestContext": {}}) is None


def test_unauthenticated_request_401(monkeypatch):
    ev = _event("/me", "GET")
    ev["requestContext"] = {}
    resp = handler.lambda_handler(ev, MagicMock())
    assert resp["statusCode"] == 401


def test_create_geofence(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "_geofences", lambda: table)
    monkeypatch.setattr(handler, "new_geofence_id", lambda: "gf-fixed")

    ev = _event("/geofences", "POST", body={"route_id": "720", "threshold_seconds": 300, "label": "720 to UCLA"})
    resp = handler.lambda_handler(ev, MagicMock())

    assert resp["statusCode"] == 201
    item = table.put_item.call_args.kwargs["Item"]
    assert item["user_id"] == "user-1"
    assert item["geofence_id"] == "gf-fixed"
    assert item["route_id"] == "720"
    assert item["threshold_seconds"] == 300
    assert item["enabled"] is True
    assert item["stop_id"] is None  # reserved for v2
    body = json.loads(resp["body"])
    assert body["geofence_id"] == "gf-fixed"


def test_create_geofence_validation(monkeypatch):
    monkeypatch.setattr(handler, "_geofences", lambda: MagicMock())
    # Missing route_id
    resp = handler.lambda_handler(_event("/geofences", "POST", body={"threshold_seconds": 300}), MagicMock())
    assert resp["statusCode"] == 400
    # Out-of-range threshold
    resp = handler.lambda_handler(
        _event("/geofences", "POST", body={"route_id": "2", "threshold_seconds": 5}), MagicMock()
    )
    assert resp["statusCode"] == 400


def test_list_geofences_scoped_to_user(monkeypatch):
    table = MagicMock()
    table.query.return_value = {"Items": [
        {"user_id": "user-1", "geofence_id": "gf-1", "route_id": "720",
         "threshold_seconds": Decimal("300"), "enabled": True, "stop_id": None,
         "label": "x", "created_at": "2026-05-26T00:00:00Z"},
    ]}
    monkeypatch.setattr(handler, "_geofences", lambda: table)

    resp = handler.lambda_handler(_event("/geofences", "GET"), MagicMock())
    assert resp["statusCode"] == 200
    assert table.query.called
    body = json.loads(resp["body"])
    assert body["geofences"][0]["threshold_seconds"] == 300  # Decimal coerced


def test_delete_geofence(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "_geofences", lambda: table)
    resp = handler.lambda_handler(
        _event("/geofences/{geofenceId}", "DELETE", path_params={"geofenceId": "gf-1"}), MagicMock()
    )
    assert resp["statusCode"] == 204
    key = table.delete_item.call_args.kwargs["Key"]
    assert key == {"user_id": "user-1", "geofence_id": "gf-1"}


def test_list_notifications(monkeypatch):
    table = MagicMock()
    table.query.return_value = {"Items": [
        {"user_id": "user-1", "created_at": "2026-05-26T12:00:00.000001Z",
         "route_id": "720", "delay_seconds": Decimal("360"),
         "threshold_seconds": Decimal("300"), "message": "Route 720 running ~6 min late",
         "read": False},
    ]}
    monkeypatch.setattr(handler, "_notifications", lambda: table)
    resp = handler.lambda_handler(_event("/notifications", "GET"), MagicMock())
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["unread_count"] == 1
    assert body["notifications"][0]["id"] == "2026-05-26T12:00:00.000001Z"
    assert body["notifications"][0]["delay_seconds"] == 360


def test_mark_notification_read(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "_notifications", lambda: table)
    resp = handler.lambda_handler(
        _event("/notifications/{notificationId}", "PATCH",
               path_params={"notificationId": "2026-05-26T12:00:00.000001Z"}), MagicMock()
    )
    assert resp["statusCode"] == 200
    kwargs = table.update_item.call_args.kwargs
    assert kwargs["Key"] == {"user_id": "user-1", "created_at": "2026-05-26T12:00:00.000001Z"}
    # Must guard against the upsert that would otherwise create a malformed row.
    assert "attribute_exists" in kwargs["ConditionExpression"]


def test_mark_notification_read_missing_row_404(monkeypatch):
    # PATCHing a stale/expired id must 404, not silently create a phantom row.
    table = MagicMock()
    table.update_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem"
    )
    monkeypatch.setattr(handler, "_notifications", lambda: table)
    resp = handler.lambda_handler(
        _event("/notifications/{notificationId}", "PATCH",
               path_params={"notificationId": "does-not-exist"}), MagicMock()
    )
    assert resp["statusCode"] == 404


def test_get_me(monkeypatch):
    table = MagicMock()
    table.get_item.return_value = {"Item": {
        "user_id": "user-1", "email": "r@x.com", "email_alerts_enabled": True, "home_routes": []
    }}
    monkeypatch.setattr(handler, "_users", lambda: table)
    resp = handler.lambda_handler(_event("/me", "GET"), MagicMock())
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"])["email_alerts_enabled"] is True


def test_put_me_updates_email_toggle(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "_users", lambda: table)
    resp = handler.lambda_handler(
        _event("/me", "PUT", body={"email_alerts_enabled": True}), MagicMock()
    )
    assert resp["statusCode"] == 200
    kwargs = table.update_item.call_args.kwargs
    assert kwargs["Key"] == {"user_id": "user-1"}
    assert kwargs["ExpressionAttributeValues"][":v"] is True


def test_put_me_rejects_non_bool(monkeypatch):
    # A JSON string "false" must not be coerced to True (silent enable bug).
    table = MagicMock()
    monkeypatch.setattr(handler, "_users", lambda: table)
    resp = handler.lambda_handler(
        _event("/me", "PUT", body={"email_alerts_enabled": "false"}), MagicMock()
    )
    assert resp["statusCode"] == 400
    table.update_item.assert_not_called()


def test_put_me_rejects_missing_key(monkeypatch):
    table = MagicMock()
    monkeypatch.setattr(handler, "_users", lambda: table)
    resp = handler.lambda_handler(_event("/me", "PUT", body={}), MagicMock())
    assert resp["statusCode"] == 400
    table.update_item.assert_not_called()


def test_get_me_falls_back_when_absent(monkeypatch):
    # No row yet (PostConfirmation hasn't run / race): synthesize defaults
    # from the Cognito email claim.
    table = MagicMock()
    table.get_item.return_value = {}  # no "Item"
    monkeypatch.setattr(handler, "_users", lambda: table)
    resp = handler.lambda_handler(_event("/me", "GET", sub="user-9"), MagicMock())
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["user_id"] == "user-9"
    assert body["email"] == "r@x.com"
    assert body["email_alerts_enabled"] is False
    assert body["home_routes"] == []


def test_get_me_backfills_partial_row(monkeypatch):
    # A row that exists but is missing fields (older put_me wrote only the
    # email toggle) must still come back satisfying the full Me contract.
    table = MagicMock()
    table.get_item.return_value = {"Item": {"user_id": "user-1", "email_alerts_enabled": True}}
    monkeypatch.setattr(handler, "_users", lambda: table)
    resp = handler.lambda_handler(_event("/me", "GET"), MagicMock())
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["email_alerts_enabled"] is True   # stored value preserved
    assert body["email"] == "r@x.com"              # backfilled from claim
    assert body["home_routes"] == []               # backfilled default
