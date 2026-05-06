"""Unit tests for the ingestion Lambda handler.

Mocks the HTTP fetch so we never hit LA Metro's API in CI.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from google.transit import gtfs_realtime_pb2

from lambdas.ingestion import handler


def _build_feed_with_vehicles(n: int) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_700_000_000
    for i in range(n):
        entity = feed.entity.add()
        entity.id = f"vehicle-{i}"
        entity.vehicle.vehicle.id = f"bus-{i}"
        entity.vehicle.trip.route_id = "720"
        entity.vehicle.position.latitude = 34.05
        entity.vehicle.position.longitude = -118.25
        entity.vehicle.timestamp = 1_700_000_000 + i
    return feed.SerializeToString()


def test_summarize_counts_vehicles_and_picks_first_sample():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(_build_feed_with_vehicles(3))

    summary = handler.summarize(feed)

    assert summary["vehicle_count"] == 3
    assert summary["sample"]["vehicle_id"] == "bus-0"
    assert summary["sample"]["route_id"] == "720"
    assert summary["sample"]["lat"] == pytest.approx(34.05)


def test_summarize_handles_empty_feed():
    feed = gtfs_realtime_pb2.FeedMessage()

    summary = handler.summarize(feed)

    assert summary["vehicle_count"] == 0
    assert summary["sample"] is None


def test_lambda_handler_returns_ok_on_successful_fetch():
    payload = _build_feed_with_vehicles(2)
    with patch.object(handler, "fetch_feed", return_value=payload), \
         patch.object(handler, "get_api_key", return_value="test-key"):
        result = handler.lambda_handler({}, None)

    assert result["ok"] is True
    assert result["vehicle_count"] == 2
    assert "elapsed_ms" in result


def test_lambda_handler_returns_error_on_fetch_failure():
    with patch.object(handler, "fetch_feed", side_effect=RuntimeError("boom")), \
         patch.object(handler, "get_api_key", return_value="test-key"):
        result = handler.lambda_handler({}, None)

    assert result["ok"] is False
    assert "boom" in result["error"]


def test_fetch_feed_sends_authorization_header_when_key_provided():
    captured: dict[str, object] = {}

    class FakeResp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"payload"

    def fake_urlopen(req, timeout):
        captured["headers"] = dict(req.headers)
        return FakeResp()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        handler.fetch_feed("https://example.com/feed", 1.0, api_key="secret")

    assert captured["headers"].get("Authorization") == "secret"


def test_fetch_feed_omits_authorization_header_when_no_key():
    captured: dict[str, object] = {}

    class FakeResp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"payload"

    def fake_urlopen(req, timeout):
        captured["headers"] = dict(req.headers)
        return FakeResp()

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        handler.fetch_feed("https://example.com/feed", 1.0, api_key="")

    assert "Authorization" not in captured["headers"]
