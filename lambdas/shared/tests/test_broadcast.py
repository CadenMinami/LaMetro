"""Unit tests for the WebSocket fan-out helper."""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from lambdas.shared import broadcast
from lambdas.shared.broadcast import Connection


@pytest.fixture(autouse=True)
def reset_cache_between_tests():
    broadcast.reset_cache()
    yield
    broadcast.reset_cache()


def _conn(cid: str, bbox=(-118.40, 33.95, -118.15, 34.15), route_id=None) -> Connection:
    return Connection(connection_id=cid, bbox=bbox, route_id=route_id)


def test_vehicle_matches_inside_bbox():
    v = {"lat": "34.05", "lon": "-118.25", "route_id": "720"}
    assert broadcast._vehicle_matches(v, _conn("c1")) is True


def test_vehicle_outside_bbox_no_match():
    # NYC lat/lon — way outside the LA bbox.
    v = {"lat": "40.71", "lon": "-74.00", "route_id": "720"}
    assert broadcast._vehicle_matches(v, _conn("c1")) is False


def test_vehicle_route_filter_excludes_other_routes():
    v = {"lat": "34.05", "lon": "-118.25", "route_id": "4"}
    assert broadcast._vehicle_matches(v, _conn("c1", route_id="720")) is False
    assert broadcast._vehicle_matches(v, _conn("c1", route_id="4")) is True


def test_vehicle_no_bbox_no_match():
    """A connection without subscribed_bbox shouldn't receive anything."""
    v = {"lat": "34.05", "lon": "-118.25", "route_id": "720"}
    assert broadcast._vehicle_matches(v, _conn("c1", bbox=None)) is False


def test_to_wire_strips_decimal_and_handles_missing_fields():
    v = {
        "vehicle_id": "1234",
        "route_id": "720",
        "trip_id": "t1",
        "lat": "34.05",
        "lon": "-118.25",
        "delay_seconds": Decimal("180"),
        "last_updated": "2026-05-08T00:00:00Z",
        # bearing, speed_mps absent
    }
    out = broadcast._to_wire(v)
    assert out["vehicle_id"] == "1234"
    assert out["lat"] == 34.05
    assert out["delay_seconds"] == 180
    assert isinstance(out["delay_seconds"], int)
    assert out["bearing"] is None
    assert out["speed_mps"] is None


def test_row_to_connection_parses_bbox():
    row = {
        "connection_id": "c1",
        "subscribed_bbox": {
            "minLon": Decimal("-118.40"), "minLat": Decimal("33.95"),
            "maxLon": Decimal("-118.15"), "maxLat": Decimal("34.15"),
        },
        "subscribed_route_id": "720",
    }
    conn = broadcast._row_to_connection(row)
    assert conn is not None
    assert conn.connection_id == "c1"
    assert conn.bbox == (-118.40, 33.95, -118.15, 34.15)
    assert conn.route_id == "720"


def test_row_to_connection_no_bbox_yet():
    """Connections that connected but never sent `subscribe` have no bbox."""
    row = {"connection_id": "c1"}
    conn = broadcast._row_to_connection(row)
    assert conn is not None
    assert conn.bbox is None


def test_get_connections_caches_within_ttl(monkeypatch):
    fake_table = MagicMock()
    fake_table.scan.return_value = {
        "Items": [
            {"connection_id": "c1", "subscribed_bbox": {
                "minLon": -118.4, "minLat": 33.95, "maxLon": -118.15, "maxLat": 34.15,
            }},
        ],
    }
    fake_resource = MagicMock()
    fake_resource.Table.return_value = fake_table
    monkeypatch.setattr(broadcast, "_ddb", lambda: fake_resource)

    # First call → table.scan invoked.
    out1 = broadcast.get_connections("t", now=100.0)
    assert len(out1) == 1
    assert fake_table.scan.call_count == 1

    # Second call within TTL → cached, no extra scan.
    out2 = broadcast.get_connections("t", now=100.0 + 30)
    assert out2 is out1  # same list object (cache)
    assert fake_table.scan.call_count == 1

    # Past TTL → fresh scan.
    broadcast.get_connections("t", now=100.0 + 120)
    assert fake_table.scan.call_count == 2


def test_fan_out_sends_filtered_payload(monkeypatch):
    # Two connections: c1 cares about everything in LA, c2 only route 720.
    monkeypatch.setattr(
        broadcast,
        "get_connections",
        lambda *a, **k: [
            _conn("c1"),
            _conn("c2", route_id="720"),
        ],
    )
    fake_client = MagicMock()
    monkeypatch.setattr(broadcast, "_management_client", lambda url: fake_client)

    vehicles = [
        {"vehicle_id": "a", "route_id": "720", "lat": "34.05", "lon": "-118.25"},
        {"vehicle_id": "b", "route_id": "4",   "lat": "34.05", "lon": "-118.25"},
    ]
    summary = broadcast.fan_out(
        vehicles, callback_url="https://stub", connections_table="t"
    )
    assert summary == {"sent": 2, "stale": 0, "skipped": 0}

    # c1 received both vehicles; c2 only the 720.
    by_conn = {c.kwargs["ConnectionId"]: json.loads(c.kwargs["Data"]) for c in fake_client.post_to_connection.call_args_list}
    assert {v["vehicle_id"] for v in by_conn["c1"]["vehicles"]} == {"a", "b"}
    assert {v["vehicle_id"] for v in by_conn["c2"]["vehicles"]} == {"a"}
    assert by_conn["c1"]["type"] == "positions"


def test_fan_out_skips_connection_with_no_match(monkeypatch):
    monkeypatch.setattr(
        broadcast,
        "get_connections",
        lambda *a, **k: [_conn("c1", route_id="999")],
    )
    fake_client = MagicMock()
    monkeypatch.setattr(broadcast, "_management_client", lambda url: fake_client)
    vehicles = [{"vehicle_id": "a", "route_id": "720", "lat": "34.05", "lon": "-118.25"}]
    summary = broadcast.fan_out(vehicles, callback_url="x", connections_table="t")
    assert summary == {"sent": 0, "stale": 0, "skipped": 1}
    fake_client.post_to_connection.assert_not_called()


def test_fan_out_deletes_gone_connections(monkeypatch):
    monkeypatch.setattr(
        broadcast,
        "get_connections",
        lambda *a, **k: [_conn("c1"), _conn("c2")],
    )
    fake_client = MagicMock()
    # c1 succeeds, c2 returns GoneException.
    def fake_post(ConnectionId, Data):
        if ConnectionId == "c2":
            raise ClientError(
                {"Error": {"Code": "GoneException", "Message": "gone"}}, "PostToConnection"
            )
    fake_client.post_to_connection.side_effect = fake_post
    monkeypatch.setattr(broadcast, "_management_client", lambda url: fake_client)

    fake_table = MagicMock()
    batch = MagicMock()
    fake_table.batch_writer.return_value.__enter__.return_value = batch
    fake_resource = MagicMock()
    fake_resource.Table.return_value = fake_table
    monkeypatch.setattr(broadcast, "_ddb", lambda: fake_resource)

    vehicles = [{"vehicle_id": "a", "route_id": "720", "lat": "34.05", "lon": "-118.25"}]
    summary = broadcast.fan_out(vehicles, callback_url="x", connections_table="t")
    assert summary == {"sent": 1, "stale": 1, "skipped": 0}
    batch.delete_item.assert_called_once_with(Key={"connection_id": "c2"})


def test_fan_out_swallows_other_errors(monkeypatch):
    monkeypatch.setattr(broadcast, "get_connections", lambda *a, **k: [_conn("c1")])
    fake_client = MagicMock()
    fake_client.post_to_connection.side_effect = ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "slow down"}}, "PostToConnection"
    )
    monkeypatch.setattr(broadcast, "_management_client", lambda url: fake_client)
    vehicles = [{"vehicle_id": "a", "route_id": "720", "lat": "34.05", "lon": "-118.25"}]
    # Non-Gone errors don't kill the function or mark connection stale.
    summary = broadcast.fan_out(vehicles, callback_url="x", connections_table="t")
    assert summary == {"sent": 0, "stale": 0, "skipped": 0}
