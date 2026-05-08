"""Unit tests for the WebSocket connection-manager Lambda."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from lambdas.websocket import handler


def _event(route_key: str, connection_id: str = "abc123", body: str | None = None) -> dict:
    return {
        "requestContext": {"routeKey": route_key, "connectionId": connection_id},
        "body": body,
    }


def test_missing_connection_id_400():
    resp = handler.lambda_handler({"requestContext": {"routeKey": "$connect"}}, None)
    assert resp["statusCode"] == 400


def test_unknown_route_key_400():
    resp = handler.lambda_handler(_event("garbage"), None)
    assert resp["statusCode"] == 400


def test_connect_writes_row(monkeypatch):
    fake_table = MagicMock()
    monkeypatch.setattr(handler, "_get_table", lambda: fake_table)
    resp = handler.lambda_handler(_event("$connect"), None)
    assert resp["statusCode"] == 200
    assert fake_table.put_item.called
    item = fake_table.put_item.call_args.kwargs["Item"]
    assert item["connection_id"] == "abc123"
    assert "connected_at" in item
    assert "ttl_epoch" in item
    # TTL is connected_at + 7200 by default.
    assert item["ttl_epoch"] - item["connected_at"] == handler.TTL_SECONDS


def test_disconnect_deletes_row(monkeypatch):
    fake_table = MagicMock()
    monkeypatch.setattr(handler, "_get_table", lambda: fake_table)
    resp = handler.lambda_handler(_event("$disconnect"), None)
    assert resp["statusCode"] == 200
    fake_table.delete_item.assert_called_once_with(Key={"connection_id": "abc123"})


def test_subscribe_invalid_json_400(monkeypatch):
    fake_table = MagicMock()
    monkeypatch.setattr(handler, "_get_table", lambda: fake_table)
    resp = handler.lambda_handler(_event("subscribe", body="not json"), None)
    assert resp["statusCode"] == 400


def test_subscribe_invalid_bbox_400(monkeypatch):
    fake_table = MagicMock()
    monkeypatch.setattr(handler, "_get_table", lambda: fake_table)
    body = json.dumps({"bbox": {"minLon": "x"}})
    resp = handler.lambda_handler(_event("subscribe", body=body), None)
    assert resp["statusCode"] == 400


def test_subscribe_rejects_too_large_bbox(monkeypatch):
    fake_table = MagicMock()
    monkeypatch.setattr(handler, "_get_table", lambda: fake_table)
    body = json.dumps(
        {
            "bbox": {
                "minLon": -119,
                "minLat": 33,
                "maxLon": -117,
                "maxLat": 35,  # 2°×2° = 4 deg² > 0.5 cap
            }
        }
    )
    resp = handler.lambda_handler(_event("subscribe", body=body), None)
    assert resp["statusCode"] == 400


def test_subscribe_writes_bbox_only(monkeypatch):
    fake_table = MagicMock()
    monkeypatch.setattr(handler, "_get_table", lambda: fake_table)
    body = json.dumps(
        {
            "bbox": {
                "minLon": -118.40, "minLat": 33.95,
                "maxLon": -118.15, "maxLat": 34.15,
            }
        }
    )
    resp = handler.lambda_handler(_event("subscribe", body=body), None)
    assert resp["statusCode"] == 200
    fake_table.update_item.assert_called_once()
    kwargs = fake_table.update_item.call_args.kwargs
    assert "REMOVE subscribed_route_id" in kwargs["UpdateExpression"]
    assert ":route_id" not in kwargs["ExpressionAttributeValues"]
    # bbox values arrive as Decimal so DynamoDB's resource API will accept
    # them — float would be rejected with TypeError at write time.
    assert float(kwargs["ExpressionAttributeValues"][":bbox"]["minLon"]) == -118.40


def test_subscribe_writes_bbox_with_route_filter(monkeypatch):
    fake_table = MagicMock()
    monkeypatch.setattr(handler, "_get_table", lambda: fake_table)
    body = json.dumps(
        {
            "bbox": {
                "minLon": -118.40, "minLat": 33.95,
                "maxLon": -118.15, "maxLat": 34.15,
            },
            "route_id": "720-13196",
        }
    )
    resp = handler.lambda_handler(_event("subscribe", body=body), None)
    assert resp["statusCode"] == 200
    kwargs = fake_table.update_item.call_args.kwargs
    assert "subscribed_route_id = :route_id" in kwargs["UpdateExpression"]
    assert kwargs["ExpressionAttributeValues"][":route_id"] == "720-13196"
