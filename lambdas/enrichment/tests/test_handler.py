"""Unit tests for the enrichment Lambda."""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock, patch

import pytest

from lambdas.enrichment import handler


def _kinesis_record(payload: dict) -> dict:
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    return {"kinesis": {"data": encoded}}


def test_encode_geohash_known_downtown_la():
    # Downtown LA at (34.05, -118.25) at precision 6 should be a stable hash.
    h = handler.encode_geohash(34.05, -118.25, precision=6)
    assert len(h) == 6
    assert h.startswith("9q5ct")


def test_encode_geohash_different_locations_differ():
    la = handler.encode_geohash(34.05, -118.25, precision=6)
    nyc = handler.encode_geohash(40.71, -74.00, precision=6)
    assert la != nyc


def test_to_dynamo_item_returns_none_for_missing_coords():
    assert handler.to_dynamo_item({"vehicle_id": "v1", "lat": None, "lon": -118.0}, None) is None
    assert handler.to_dynamo_item({"vehicle_id": "v1", "lat": 34.0, "lon": None}, None) is None
    assert handler.to_dynamo_item({"vehicle_id": "", "lat": 34.0, "lon": -118.0}, None) is None


def test_to_dynamo_item_populates_required_keys():
    event = {
        "vehicle_id": "5817",
        "route_id": "720",
        "trip_id": "trip-1",
        "lat": 34.05,
        "lon": -118.25,
        "bearing": 87.5,
        "speed_mps": 8.3,
        "vehicle_timestamp": 1_700_000_000,
        "feed_timestamp": 1_700_000_000,
    }
    # gtfs=None → no delay computed, key omitted.
    item = handler.to_dynamo_item(event, None)
    assert item is not None
    assert item["vehicle_id"] == "5817"
    assert item["route_id"] == "720"
    assert item["trip_id"] == "trip-1"
    assert len(item["geohash"]) == handler.GEOHASH_PRECISION
    assert item["bearing"] == "87.5"
    assert item["speed_mps"] == "8.3"
    # delay_seconds key is only present when delay is actually computed.
    assert "delay_seconds" not in item
    assert "ttl_epoch" in item


def test_to_dynamo_item_omits_route_id_when_empty():
    event = {
        "vehicle_id": "9404",
        "route_id": "",  # out-of-service vehicle
        "lat": 34.05,
        "lon": -118.25,
        "vehicle_timestamp": 1_700_000_000,
        "feed_timestamp": 1_700_000_000,
    }
    item = handler.to_dynamo_item(event, None)
    assert item is not None
    # route_id MUST NOT be present — the GSI rejects empty strings on key attrs.
    assert "route_id" not in item


def test_to_dynamo_item_includes_delay_when_gtfs_returns_one():
    """When the deviation algorithm returns a number, it should land in the
    item under `delay_seconds`. Mock the deviation call so we don't need real
    geometry here — full algorithm coverage lives in test_deviation.py."""
    event = {
        "vehicle_id": "5817",
        "route_id": "720",
        "trip_id": "trip-1",
        "lat": 34.05,
        "lon": -118.25,
        "vehicle_timestamp": 1_700_000_000,
        "feed_timestamp": 1_700_000_000,
    }
    fake_gtfs = MagicMock()
    with patch.object(handler, "compute_delay_for_event", return_value=120):
        item = handler.to_dynamo_item(event, fake_gtfs)
    assert item is not None
    assert item["delay_seconds"] == 120


def test_to_dynamo_item_fills_route_id_from_static_when_rt_missing():
    """RT sometimes omits route_id between trips. If we know the trip in
    static, prefer that to dropping the row off the GSI."""
    event = {
        "vehicle_id": "5817",
        "route_id": "",  # RT didn't include it
        "trip_id": "trip-1",
        "lat": 34.05,
        "lon": -118.25,
        "vehicle_timestamp": 1_700_000_000,
        "feed_timestamp": 1_700_000_000,
    }
    fake_gtfs = MagicMock()
    fake_gtfs.trip_route = {"trip-1": "720"}
    with patch.object(handler, "compute_delay_for_event", return_value=None):
        item = handler.to_dynamo_item(event, fake_gtfs)
    assert item is not None
    assert item["route_id"] == "720"


def test_seconds_into_service_day_is_local():
    """Epoch 1700000000 = 2023-11-14 22:13:20 UTC = 14:13:20 LA (PST)."""
    s = handler.seconds_into_service_day(1_700_000_000)
    # 14:13:20 = 14*3600 + 13*60 + 20 = 51200
    assert s == 51200


def test_compute_delay_handles_owl_trip_after_midnight():
    """Regression for the late-night service-day bug.

    An owl trip whose schedule is stored as 23:30 → 24:30 (84600 → 88200 in
    GTFS service-day terms) is genuinely active at LA 00:15. The naive
    today's-seconds reading (900) is well below the schedule's start (84600),
    so the deviation algorithm would reject it; but the same wall-clock
    moment, expressed in *yesterday's* service-day, is 87300 — squarely
    inside the trip's window. The wrapper must try both candidates.
    """
    # 2024-03-15 00:15:00 PDT = 2024-03-15 07:15:00 UTC = 1710486900
    rt_epoch = 1_710_486_900
    event = {
        "vehicle_id": "owl-1",
        "route_id": "720",
        "trip_id": "owl-trip",
        "lat": 34.05,
        "lon": -118.25,
        "vehicle_timestamp": rt_epoch,
        "feed_timestamp": rt_epoch,
    }
    fake_gtfs = MagicMock()
    fake_gtfs.shape_for_trip.return_value = "fake-shape"
    fake_gtfs.schedule_for_trip.return_value = ((84600, 0.0), (88200, 1000.0))

    # Deviation returns None for today's seconds (out of window) and a real
    # number for yesterday's-rollover seconds — exercises the both-candidates
    # path in the wrapper.
    call_args: list[int] = []

    def fake_compute(*, seconds_into_service_day, **_):
        call_args.append(seconds_into_service_day)
        return 45 if seconds_into_service_day >= 86400 else None

    with patch.object(handler.deviation, "compute_delay_seconds", side_effect=fake_compute):
        delay = handler.compute_delay_for_event(event, fake_gtfs)

    assert delay == 45
    # Today's candidate was 900s (00:15 LA); yesterday's-rollover was 87300.
    assert call_args == [900, 87300]


def test_lambda_handler_writes_each_valid_record():
    records = [
        _kinesis_record({"vehicle_id": "5817", "route_id": "720", "lat": 34.05, "lon": -118.25, "vehicle_timestamp": 1_700_000_000, "feed_timestamp": 1_700_000_000}),
        _kinesis_record({"vehicle_id": "9404", "route_id": "", "lat": 34.06, "lon": -118.29, "vehicle_timestamp": 1_700_000_000, "feed_timestamp": 1_700_000_000}),
    ]
    fake_batch = MagicMock()
    fake_batch.__enter__.return_value = fake_batch
    fake_batch.__exit__.return_value = False
    fake_table = MagicMock()
    fake_table.batch_writer.return_value = fake_batch

    with patch.object(handler, "get_table", return_value=fake_table), patch.object(
        handler, "get_gtfs", return_value=None
    ):
        result = handler.lambda_handler({"Records": records}, None)

    assert result["written"] == 2
    assert result["skipped"] == 0
    assert result["delays_computed"] == 0
    assert fake_batch.put_item.call_count == 2


def test_lambda_handler_skips_records_missing_coords():
    records = [
        _kinesis_record({"vehicle_id": "ghost", "route_id": "", "lat": None, "lon": None, "vehicle_timestamp": 1_700_000_000, "feed_timestamp": 1_700_000_000}),
    ]
    fake_batch = MagicMock()
    fake_batch.__enter__.return_value = fake_batch
    fake_batch.__exit__.return_value = False
    fake_table = MagicMock()
    fake_table.batch_writer.return_value = fake_batch

    with patch.object(handler, "get_table", return_value=fake_table), patch.object(
        handler, "get_gtfs", return_value=None
    ):
        result = handler.lambda_handler({"Records": records}, None)

    assert result["written"] == 0
    assert result["skipped"] == 1
    fake_batch.put_item.assert_not_called()
