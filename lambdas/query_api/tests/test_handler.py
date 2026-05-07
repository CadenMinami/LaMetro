"""Unit tests for the query API Lambda."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from lambdas.query_api import handler


def test_parse_bbox_happy_path():
    lon_min, lat_min, lon_max, lat_max = handler._parse_bbox("-118.30,34.02,-118.20,34.10")
    assert lon_min == pytest.approx(-118.30)
    assert lat_max == pytest.approx(34.10)


def test_parse_bbox_rejects_inverted():
    with pytest.raises(ValueError):
        handler._parse_bbox("-118.20,34.10,-118.30,34.02")  # max < min


def test_parse_bbox_rejects_too_few_floats():
    with pytest.raises(ValueError):
        handler._parse_bbox("-118.30,34.02,-118.20")


def test_covering_geohashes_returns_at_least_one_cell():
    cells = handler.covering_geohashes(-118.26, 34.04, -118.24, 34.06)
    assert len(cells) >= 1
    # All cells should share the LA-area geohash prefix.
    assert all(c.startswith("9q") for c in cells)


def test_lambda_handler_400_on_missing_bbox():
    resp = handler.lambda_handler({"queryStringParameters": {}}, None)
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["error"] == "missing_bbox"


def test_lambda_handler_400_on_invalid_bbox():
    resp = handler.lambda_handler({"queryStringParameters": {"bbox": "garbage"}}, None)
    assert resp["statusCode"] == 400


def test_lambda_handler_400_on_bbox_too_large():
    resp = handler.lambda_handler(
        {"queryStringParameters": {"bbox": "-119,33,-117,35"}}, None  # ~2°×2° = 4 deg²
    )
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["error"] == "bbox_too_large"


def test_lambda_handler_returns_vehicles_within_bbox():
    fake_table = MagicMock()
    # Each query returns one vehicle.
    fake_table.query.return_value = {
        "Items": [
            {
                "vehicle_id": "5817",
                "route_id": "720",
                "lat": "34.05",
                "lon": "-118.25",
                "last_updated": "2026-05-06T22:52:00Z",
                "delay_seconds": None,
            }
        ]
    }

    with patch.object(handler, "_table_handle", return_value=fake_table):
        resp = handler.lambda_handler(
            {"queryStringParameters": {"bbox": "-118.26,34.04,-118.24,34.06"}}, None
        )

    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["count"] >= 1
    # Should have called query once per covering cell, but at least once.
    assert fake_table.query.called


def test_lambda_handler_filters_by_route():
    fake_table = MagicMock()
    fake_table.query.return_value = {
        "Items": [
            {"vehicle_id": "a", "route_id": "720", "lat": "34.05", "lon": "-118.25", "last_updated": "t"},
            {"vehicle_id": "b", "route_id": "4",   "lat": "34.05", "lon": "-118.25", "last_updated": "t"},
        ]
    }

    with patch.object(handler, "_table_handle", return_value=fake_table):
        resp = handler.lambda_handler(
            {"queryStringParameters": {"bbox": "-118.26,34.04,-118.24,34.06", "route_id": "720"}},
            None,
        )

    body = json.loads(resp["body"])
    assert all(v["route_id"] == "720" for v in body["vehicles"])
