"""Unit tests for the precompute-predictions Lambda — pure helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from lambdas.precompute_predictions import handler


def test_derive_route_code_is_deterministic_and_bounded():
    a = handler.derive_route_code("720")
    b = handler.derive_route_code("720")
    c = handler.derive_route_code("33")
    assert a == b
    assert a != c
    assert 0 <= a < 1000
    assert 0 <= c < 1000


def test_derive_route_code_matches_crc32_modulo():
    # MUST match 7b's Athena `abs(crc32(to_utf8(route_id))) % 1000`.
    import zlib
    assert handler.derive_route_code("720") == zlib.crc32(b"720") % 1000
    assert handler.derive_route_code("33") == zlib.crc32(b"33") % 1000


def test_pick_recent_lags_returns_3_most_recent_avg_delays():
    rows = [
        {"window_start_iso": "2026-05-27T12:05:00Z", "avg_delay_seconds": Decimal("90")},
        {"window_start_iso": "2026-05-27T12:00:00Z", "avg_delay_seconds": Decimal("60")},
        {"window_start_iso": "2026-05-27T11:55:00Z", "avg_delay_seconds": Decimal("45")},
        {"window_start_iso": "2026-05-27T11:50:00Z", "avg_delay_seconds": Decimal("30")},
    ]
    lags = handler.pick_recent_lags(rows)
    assert lags == [90, 60, 45]


def test_pick_recent_lags_pads_with_zeros_when_insufficient_history():
    rows = [
        {"window_start_iso": "2026-05-27T12:05:00Z", "avg_delay_seconds": Decimal("90")},
    ]
    assert handler.pick_recent_lags(rows) == [90, 0, 0]


def test_pick_recent_lags_skips_null_delay_rows():
    rows = [
        {"window_start_iso": "2026-05-27T12:05:00Z", "avg_delay_seconds": None},
        {"window_start_iso": "2026-05-27T12:00:00Z", "avg_delay_seconds": Decimal("60")},
        {"window_start_iso": "2026-05-27T11:55:00Z", "avg_delay_seconds": Decimal("45")},
        {"window_start_iso": "2026-05-27T11:50:00Z", "avg_delay_seconds": Decimal("30")},
    ]
    assert handler.pick_recent_lags(rows) == [60, 45, 30]


def test_assemble_feature_csv_column_order_matches_training():
    csv = handler.assemble_feature_csv(
        route_code=42, hour_of_day=8, day_of_week=3,
        lags=[120, 90, 60], temp_c=18.5, precip_mm=0.0,
    )
    assert csv == "42,8,3,120,90,60,18.5,0.0"


def test_assemble_feature_csv_handles_null_weather():
    csv = handler.assemble_feature_csv(
        route_code=42, hour_of_day=8, day_of_week=3,
        lags=[120, 90, 60], temp_c=None, precip_mm=None,
    )
    assert csv == "42,8,3,120,90,60,0.0,0.0"


def test_prediction_item_shape():
    item = handler.build_prediction_item(
        route_id="720", predicted=132, current=75, model_version="v=2026-06-15",
        window_start_iso="2026-05-27T12:05:00Z",
        as_of=datetime(2026, 5, 27, 12, 6, 30, tzinfo=timezone.utc),
        ttl_seconds=900,
    )
    assert item["route_id"] == "720"
    assert item["predicted_next_window_avg_delay_seconds"] == 132
    assert item["current_avg_delay_seconds"] == 75
    assert item["model_version"] == "v=2026-06-15"
    assert item["window_start_iso"] == "2026-05-27T12:05:00Z"
    assert item["as_of"] == "2026-05-27T12:06:30Z"
    assert item["ttl_epoch"] > int(datetime(2026, 5, 27, 12, 6, 30, tzinfo=timezone.utc).timestamp())


def _agg_query_response(route_id: str, base_avg: int):
    return {
        "Items": [
            {"route_id": route_id, "window_start_iso": "2026-05-27T12:05:00Z",
             "avg_delay_seconds": Decimal(str(base_avg + 30))},
            {"route_id": route_id, "window_start_iso": "2026-05-27T12:00:00Z",
             "avg_delay_seconds": Decimal(str(base_avg + 15))},
            {"route_id": route_id, "window_start_iso": "2026-05-27T11:55:00Z",
             "avg_delay_seconds": Decimal(str(base_avg))},
        ],
    }


def test_lambda_handler_predicts_each_route_and_writes_to_ddb(monkeypatch):
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_TABLE", "ra")
    monkeypatch.setattr(handler, "ROUTE_PREDICTIONS_TABLE", "rp")
    monkeypatch.setattr(handler, "WEATHER_CACHE_TABLE", "wc")
    monkeypatch.setattr(handler, "MODELS_PREFIX_URI", "s3://bkt/models")
    monkeypatch.setattr(handler, "SAGEMAKER_ENDPOINT_NAME", "la-metro-delay-predictor")

    ra = MagicMock()
    # Two routes have recent data; queried on the base table PK per route.
    ra.query.side_effect = [
        _agg_query_response("720", 60),
        _agg_query_response("33", 20),
    ]
    rp = MagicMock()
    wc = MagicMock()
    wc.get_item.return_value = {"Item": {"temp_c": Decimal("22.4"), "precip_mm": Decimal("0.0")}}
    sm = MagicMock()
    # Endpoint returns a single predicted scalar as text (built-in XGBoost
    # default output format).
    sm.invoke_endpoint.side_effect = [
        {"Body": MagicMock(read=lambda: b"125.7")},
        {"Body": MagicMock(read=lambda: b"45.2")},
    ]
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({"promoted_version": "v=2026-06-15"}).encode())
    }

    monkeypatch.setattr(handler, "_route_aggregates", lambda: ra)
    monkeypatch.setattr(handler, "_route_predictions", lambda: rp)
    monkeypatch.setattr(handler, "_weather_cache", lambda: wc)
    monkeypatch.setattr(handler, "_sagemaker_runtime", lambda: sm)
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    monkeypatch.setattr(handler, "list_active_routes", lambda: ["720", "33"])
    fixed_now = datetime(2026, 5, 27, 12, 6, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(handler, "_utcnow", lambda: fixed_now)

    result = handler.lambda_handler({}, MagicMock())
    assert result["routes_attempted"] == 2
    assert result["predictions_written"] == 2
    assert result["model_version"] == "v=2026-06-15"

    assert sm.invoke_endpoint.call_count == 2
    # Verify endpoint name + ContentType.
    first_call = sm.invoke_endpoint.call_args_list[0].kwargs
    assert first_call["EndpointName"] == "la-metro-delay-predictor"
    assert first_call["ContentType"] == "text/csv"

    # Verify DDB writes.
    assert rp.put_item.call_count == 2
    items_by_route = {
        c.kwargs["Item"]["route_id"]: c.kwargs["Item"]
        for c in rp.put_item.call_args_list
    }
    assert set(items_by_route.keys()) == {"720", "33"}
    assert items_by_route["720"]["predicted_next_window_avg_delay_seconds"] == 126
    # current_avg = the most recent lag (60+30 = 90 for 720).
    assert items_by_route["720"]["current_avg_delay_seconds"] == 90


def test_lambda_handler_per_route_failure_does_not_block_cycle(monkeypatch):
    monkeypatch.setattr(handler, "ROUTE_AGGREGATES_TABLE", "ra")
    monkeypatch.setattr(handler, "ROUTE_PREDICTIONS_TABLE", "rp")
    monkeypatch.setattr(handler, "WEATHER_CACHE_TABLE", "wc")
    monkeypatch.setattr(handler, "MODELS_PREFIX_URI", "s3://bkt/models")
    monkeypatch.setattr(handler, "SAGEMAKER_ENDPOINT_NAME", "la-metro-delay-predictor")

    ra = MagicMock()
    ra.query.side_effect = [
        _agg_query_response("720", 60),
        _agg_query_response("33", 20),
    ]
    rp = MagicMock()
    wc = MagicMock()
    wc.get_item.return_value = {"Item": {"temp_c": Decimal("22.4"), "precip_mm": Decimal("0.0")}}
    sm = MagicMock()
    # First route fails (endpoint throws); second succeeds.
    sm.invoke_endpoint.side_effect = [
        RuntimeError("endpoint cold-start timeout"),
        {"Body": MagicMock(read=lambda: b"45.2")},
    ]
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps({"promoted_version": "v=2026-06-15"}).encode())
    }

    monkeypatch.setattr(handler, "_route_aggregates", lambda: ra)
    monkeypatch.setattr(handler, "_route_predictions", lambda: rp)
    monkeypatch.setattr(handler, "_weather_cache", lambda: wc)
    monkeypatch.setattr(handler, "_sagemaker_runtime", lambda: sm)
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    monkeypatch.setattr(handler, "list_active_routes", lambda: ["720", "33"])
    monkeypatch.setattr(
        handler, "_utcnow",
        lambda: datetime(2026, 5, 27, 12, 6, 30, tzinfo=timezone.utc),
    )

    result = handler.lambda_handler({}, MagicMock())
    assert result["routes_attempted"] == 2
    assert result["predictions_written"] == 1
    assert result["per_route_failures"] == 1
    assert rp.put_item.call_count == 1  # only the successful route


def test_lambda_handler_missing_endpoint_envvar_raises(monkeypatch):
    monkeypatch.setattr(handler, "SAGEMAKER_ENDPOINT_NAME", "")
    import pytest as _pytest
    with _pytest.raises(RuntimeError):
        handler.lambda_handler({}, MagicMock())
