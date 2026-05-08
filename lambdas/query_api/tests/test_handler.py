"""Unit tests for the query API Lambda."""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from lambdas.query_api import handler


def _vehicles_event(qs: dict[str, str]) -> dict:
    return {"resource": "/vehicles", "httpMethod": "GET", "queryStringParameters": qs}


def _aggregates_event(route_id: str, qs: dict[str, str] | None = None) -> dict:
    return {
        "resource": "/routes/{routeId}/aggregates",
        "httpMethod": "GET",
        "pathParameters": {"routeId": route_id},
        "queryStringParameters": qs or {},
    }


def test_parse_bbox_happy_path():
    lon_min, lat_min, lon_max, lat_max = handler._parse_bbox("-118.30,34.02,-118.20,34.10")
    assert lon_min == pytest.approx(-118.30)
    assert lat_max == pytest.approx(34.10)


def test_parse_bbox_rejects_inverted():
    with pytest.raises(ValueError):
        handler._parse_bbox("-118.20,34.10,-118.30,34.02")


def test_parse_bbox_rejects_too_few_floats():
    with pytest.raises(ValueError):
        handler._parse_bbox("-118.30,34.02,-118.20")


def test_covering_geohashes_returns_at_least_one_cell():
    cells = handler.covering_geohashes(-118.26, 34.04, -118.24, 34.06)
    assert len(cells) >= 1
    assert all(c.startswith("9q") for c in cells)


def test_unknown_resource_returns_404():
    resp = handler.lambda_handler({"resource": "/something-else"}, None)
    assert resp["statusCode"] == 404


def test_vehicles_400_on_missing_bbox():
    resp = handler.lambda_handler(_vehicles_event({}), None)
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["error"] == "missing_bbox"


def test_vehicles_400_on_invalid_bbox():
    resp = handler.lambda_handler(_vehicles_event({"bbox": "garbage"}), None)
    assert resp["statusCode"] == 400


def test_vehicles_400_on_bbox_too_large():
    resp = handler.lambda_handler(_vehicles_event({"bbox": "-119,33,-117,35"}), None)
    assert resp["statusCode"] == 400
    assert json.loads(resp["body"])["error"] == "bbox_too_large"


def test_vehicles_returns_within_bbox_with_int_delay():
    fake_table = MagicMock()
    # Decimal incoming from boto3 resource API; we should emit a clean int.
    fake_table.query.return_value = {
        "Items": [
            {
                "vehicle_id": "5817",
                "route_id": "720",
                "lat": "34.05",
                "lon": "-118.25",
                "last_updated": "2026-05-06T22:52:00Z",
                "delay_seconds": Decimal("325"),
            }
        ]
    }

    with patch.object(handler, "_hot", return_value=fake_table):
        resp = handler.lambda_handler(
            _vehicles_event({"bbox": "-118.26,34.04,-118.24,34.06"}), None
        )

    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["count"] >= 1
    v = body["vehicles"][0]
    assert v["delay_seconds"] == 325  # int in JSON, not "325" string
    assert isinstance(v["delay_seconds"], int)


def test_vehicles_filters_by_route():
    fake_table = MagicMock()
    fake_table.query.return_value = {
        "Items": [
            {"vehicle_id": "a", "route_id": "720", "lat": "34.05", "lon": "-118.25", "last_updated": "t"},
            {"vehicle_id": "b", "route_id": "4",   "lat": "34.05", "lon": "-118.25", "last_updated": "t"},
        ]
    }

    with patch.object(handler, "_hot", return_value=fake_table):
        resp = handler.lambda_handler(
            _vehicles_event({"bbox": "-118.26,34.04,-118.24,34.06", "route_id": "720"}), None
        )

    body = json.loads(resp["body"])
    assert all(v["route_id"] == "720" for v in body["vehicles"])


def test_aggregates_400_on_missing_route_id():
    resp = handler.lambda_handler(
        {"resource": "/routes/{routeId}/aggregates", "pathParameters": {}}, None
    )
    assert resp["statusCode"] == 400


def test_aggregates_returns_windows_newest_first():
    fake_table = MagicMock()
    fake_table.query.return_value = {
        "Items": [
            {
                "route_id": "720",
                "window_start_iso": "2026-05-08T01:00:00Z",
                "vehicle_count": Decimal("18"),
                "avg_delay_seconds": Decimal("180"),
                "p95_delay_seconds": Decimal("420"),
                "on_time_pct": "33.3",
            },
            {
                "route_id": "720",
                "window_start_iso": "2026-05-08T00:55:00Z",
                "vehicle_count": Decimal("16"),
                "avg_delay_seconds": Decimal("210"),
                "p95_delay_seconds": Decimal("480"),
                "on_time_pct": "25.0",
            },
        ]
    }

    with patch.object(handler, "_agg", return_value=fake_table):
        resp = handler.lambda_handler(_aggregates_event("720"), None)

    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["route_id"] == "720"
    assert body["count"] == 2
    # Confirm Decimal → int casting and on_time_pct → float
    assert body["windows"][0]["vehicle_count"] == 18
    assert isinstance(body["windows"][0]["vehicle_count"], int)
    assert body["windows"][0]["avg_delay_seconds"] == 180
    assert body["windows"][0]["on_time_pct"] == pytest.approx(33.3)


def test_aggregates_handles_phase_4b_rows_without_delays():
    """Some 4b-era rows have no delay fields; the API shouldn't blow up."""
    fake_table = MagicMock()
    fake_table.query.return_value = {
        "Items": [
            {
                "route_id": "720",
                "window_start_iso": "2026-05-07T22:00:00Z",
                "vehicle_count": Decimal("4"),
                # no avg/p95/on_time_pct
            }
        ]
    }
    with patch.object(handler, "_agg", return_value=fake_table):
        resp = handler.lambda_handler(_aggregates_event("720"), None)
    body = json.loads(resp["body"])
    w = body["windows"][0]
    assert w["avg_delay_seconds"] is None
    assert w["p95_delay_seconds"] is None
    assert w["on_time_pct"] is None
