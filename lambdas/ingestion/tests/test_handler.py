"""Unit tests for the ingestion Lambda handler.

Mocks the HTTP fetch and Kinesis client so we never hit LA Metro's API or
real AWS in CI.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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


def test_vehicle_events_yields_one_dict_per_active_vehicle():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(_build_feed_with_vehicles(3))

    events = list(handler.vehicle_events(feed))

    assert len(events) == 3
    assert events[0]["vehicle_id"] == "bus-0"
    assert events[0]["route_id"] == "720"
    assert events[0]["lat"] == pytest.approx(34.05)
    assert events[0]["feed_timestamp"] == 1_700_000_000


def test_vehicle_events_skips_entities_with_empty_vehicle_id():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.timestamp = 1_700_000_000
    entity = feed.entity.add()
    entity.id = "ghost"
    entity.vehicle.vehicle.id = ""  # empty — should be skipped
    entity.vehicle.position.latitude = 34.05
    entity.vehicle.position.longitude = -118.25

    events = list(handler.vehicle_events(feed))

    assert events == []


def test_lambda_handler_emits_kinesis_records_on_successful_fetch():
    payload = _build_feed_with_vehicles(2)
    fake_kinesis = MagicMock()
    fake_kinesis.put_records.return_value = {"FailedRecordCount": 0}

    with patch.object(handler, "fetch_feed", return_value=payload), \
         patch.object(handler, "get_api_key", return_value="test-key"), \
         patch.object(handler, "get_kinesis_client", return_value=fake_kinesis), \
         patch.object(handler, "STREAM_NAME", "test-stream"):
        result = handler.lambda_handler({}, None)

    assert result["ok"] is True
    assert result["vehicle_count"] == 2
    assert result["kinesis_sent"] == 2
    assert result["kinesis_failed"] == 0
    fake_kinesis.put_records.assert_called_once()
    call_kwargs = fake_kinesis.put_records.call_args.kwargs
    assert call_kwargs["StreamName"] == "test-stream"
    assert len(call_kwargs["Records"]) == 2
    # Partition key is vehicle_id for ordered per-vehicle delivery.
    assert call_kwargs["Records"][0]["PartitionKey"] == "bus-0"


def test_lambda_handler_returns_error_on_fetch_failure():
    with patch.object(handler, "fetch_feed", side_effect=RuntimeError("boom")), \
         patch.object(handler, "get_api_key", return_value="test-key"):
        result = handler.lambda_handler({}, None)

    assert result["ok"] is False
    assert "boom" in result["error"]


def test_lambda_handler_errors_when_stream_name_missing():
    payload = _build_feed_with_vehicles(1)
    with patch.object(handler, "fetch_feed", return_value=payload), \
         patch.object(handler, "get_api_key", return_value="test-key"), \
         patch.object(handler, "STREAM_NAME", ""):
        result = handler.lambda_handler({}, None)

    assert result["ok"] is False
    assert "VEHICLE_STREAM_NAME" in result["error"]


def test_lambda_handler_skips_fetch_when_no_active_viewers():
    """Scale-to-zero gate: with the connections table empty, ingestion must
    short-circuit before fetching the feed or writing to Kinesis."""
    fake_ddb = MagicMock()
    fake_ddb.scan.return_value = {"Count": 0}
    fake_fetch = MagicMock()

    with patch.object(handler, "CONNECTIONS_TABLE_NAME", "conns"), \
         patch.object(handler, "get_dynamodb_client", return_value=fake_ddb), \
         patch.object(handler, "fetch_feed", fake_fetch):
        result = handler.lambda_handler({}, None)

    assert result["ok"] is True
    assert result["skipped"] is True
    assert result["vehicle_count"] == 0
    fake_fetch.assert_not_called()


def test_lambda_handler_runs_when_viewers_connected():
    """With at least one connection row, ingestion proceeds as normal."""
    payload = _build_feed_with_vehicles(2)
    fake_ddb = MagicMock()
    fake_ddb.scan.return_value = {"Count": 1}
    fake_kinesis = MagicMock()
    fake_kinesis.put_records.return_value = {"FailedRecordCount": 0}

    with patch.object(handler, "CONNECTIONS_TABLE_NAME", "conns"), \
         patch.object(handler, "get_dynamodb_client", return_value=fake_ddb), \
         patch.object(handler, "fetch_feed", return_value=payload), \
         patch.object(handler, "get_api_key", return_value="test-key"), \
         patch.object(handler, "get_kinesis_client", return_value=fake_kinesis), \
         patch.object(handler, "STREAM_NAME", "test-stream"):
        result = handler.lambda_handler({}, None)

    assert result["ok"] is True
    assert result.get("skipped") is not True
    assert result["vehicle_count"] == 2
    fake_kinesis.put_records.assert_called_once()


def test_has_active_viewers_fails_open_when_table_unset():
    """No table configured → run anyway (don't silently go dark on misconfig)."""
    with patch.object(handler, "CONNECTIONS_TABLE_NAME", ""):
        assert handler.has_active_viewers() is True


def test_has_active_viewers_fails_open_on_scan_error():
    """A transient DynamoDB error must not kill ingestion — fail open."""
    fake_ddb = MagicMock()
    fake_ddb.scan.side_effect = RuntimeError("ddb down")
    with patch.object(handler, "CONNECTIONS_TABLE_NAME", "conns"), \
         patch.object(handler, "get_dynamodb_client", return_value=fake_ddb):
        assert handler.has_active_viewers() is True


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
