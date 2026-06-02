from ml import backfill_features as bf


def test_iter_json_objects_parses_concatenated_objects():
    raw = b'{"a": 1}{"b": 2}{"c": 3}'
    assert list(bf.iter_json_objects(raw)) == [{"a": 1}, {"b": 2}, {"c": 3}]


def test_iter_json_objects_tolerates_whitespace_and_newlines():
    raw = b'{"a": 1}\n  {"b": 2}\n'
    assert list(bf.iter_json_objects(raw)) == [{"a": 1}, {"b": 2}]


def test_iter_json_objects_stops_cleanly_on_malformed_tail():
    raw = b'{"a": 1}{"b":'  # truncated last object
    assert list(bf.iter_json_objects(raw)) == [{"a": 1}]


def test_is_routed_requires_route_and_trip():
    assert bf.is_routed({"route_id": "70-13196", "trip_id": "t1"}) is True
    assert bf.is_routed({"route_id": "", "trip_id": "t1"}) is False
    assert bf.is_routed({"route_id": "70-13196", "trip_id": ""}) is False
    assert bf.is_routed({}) is False


def test_seconds_into_service_day_la_local():
    # 2026-05-07 12:00:00 America/Los_Angeles (PDT, UTC-7) == 19:00:00 UTC.
    # epoch for 2026-05-07T19:00:00Z = 1778180400.
    # (The plan's draft used 1778518800, which is actually 2026-05-11T17:00Z;
    #  corrected here so the assertion reflects the true noon-LA fact it claims.)
    secs = bf.seconds_into_service_day(1778180400)
    assert secs == 12 * 3600  # noon local


def test_window_start_iso_floors_to_5min_utc():
    # 2026-05-07T19:07:42Z floors to 19:05:00Z. Base epoch 1778180400 = 19:00:00Z.
    assert bf.window_start_iso(1778180400 + 7 * 60 + 42) == "2026-05-07T19:05:00Z"


def test_dedupe_keeps_latest_position_per_vehicle_window():
    recs = [
        {"vehicle_id": "v1", "route_id": "r", "trip_id": "t", "lat": 1.0, "lon": 1.0,
         "vehicle_timestamp": 1778180400},          # 19:00:00Z, window 19:00
        {"vehicle_id": "v1", "route_id": "r", "trip_id": "t", "lat": 2.0, "lon": 2.0,
         "vehicle_timestamp": 1778180430},          # 19:00:30Z, same window — newer
        {"vehicle_id": "v1", "route_id": "r", "trip_id": "t", "lat": 9.0, "lon": 9.0,
         "vehicle_timestamp": 1778180700},          # 19:05:00Z, next window
    ]
    out = bf.dedupe_latest(recs)
    # keyed by (vehicle, window_iso) → latest record
    assert out[("v1", "2026-05-07T19:00:00Z")]["lat"] == 2.0
    assert out[("v1", "2026-05-07T19:05:00Z")]["lat"] == 9.0


class _FakeGTFS:
    """Minimal stand-in for GTFSStatic with the two methods we use."""
    def __init__(self, shape, schedule):
        self._shape, self._schedule = shape, schedule
    def shape_for_trip(self, trip_id):
        return self._shape
    def schedule_for_trip(self, trip_id):
        return self._schedule


def test_delay_for_record_delegates_to_deviation(monkeypatch):
    captured = {}

    def fake_compute(shape, schedule, lat, lon, secs):
        captured.update(shape=shape, schedule=schedule, lat=lat, lon=lon, secs=secs)
        return 42

    monkeypatch.setattr(bf.deviation, "compute_delay_seconds", fake_compute)
    gtfs = _FakeGTFS(shape="SHAPE", schedule=(("s",),))
    # 1778180400 = 2026-05-07T19:00:00Z = noon LA (PDT).
    rec = {"trip_id": "t", "lat": 34.05, "lon": -118.24,
           "vehicle_timestamp": 1778180400}

    assert bf.delay_for_record(rec, gtfs) == 42
    assert captured["shape"] == "SHAPE"
    assert captured["lat"] == 34.05
    assert captured["secs"] == 12 * 3600  # noon LA


def test_delay_for_record_none_when_trip_unknown(monkeypatch):
    gtfs = _FakeGTFS(shape=None, schedule=None)
    rec = {"trip_id": "t", "lat": 1.0, "lon": 1.0, "vehicle_timestamp": 1778180400}
    assert bf.delay_for_record(rec, gtfs) is None


def test_records_for_window_builds_feature_rows():
    # Two vehicles on route "70" in one window; delays already attached.
    deduped = {
        ("v1", "2026-05-07T19:00:00Z"): {"route_id": "70", "delay_seconds": 60},
        ("v2", "2026-05-07T19:00:00Z"): {"route_id": "70", "delay_seconds": 120},
    }
    weather = {"temp_c": 20.0, "precip_mm": 0.0, "observed_at": "2026-05-07T19:00"}
    rows = bf.records_for_window(
        "2026-05-07T19:00:00Z",
        [r for r in deduped.values()],
        weather,
        ingested_at_iso="2026-06-02T00:00:00Z",
    )
    assert len(rows) == 1                      # one route
    row = rows[0]
    assert row["route_id"] == "70"
    assert row["window_start_iso"] == "2026-05-07T19:00:00Z"
    assert row["vehicle_count"] == 2
    assert row["avg_delay_seconds"] == 90
    assert row["temp_c"] == 20.0


def test_parse_archive_weather_indexes_by_hour():
    body = (
        b'{"hourly": {'
        b'"time": ["2026-05-07T19:00", "2026-05-07T20:00"], '
        b'"temperature_2m": [20.5, 21.0], '
        b'"precipitation": [0.0, 0.3]}}'
    )
    idx = bf.parse_archive_weather(body)
    assert idx["2026-05-07T19:00"] == {"temp_c": 20.5, "precip_mm": 0.0,
                                       "observed_at": "2026-05-07T19:00"}
    assert idx["2026-05-07T20:00"]["precip_mm"] == 0.3


def test_weather_for_window_uses_the_window_hour():
    idx = {"2026-05-07T19:00": {"temp_c": 20.5, "precip_mm": 0.0,
                                "observed_at": "2026-05-07T19:00"}}
    # window 19:05 -> hour bucket 19:00
    assert bf.weather_for_window("2026-05-07T19:05:00Z", idx)["temp_c"] == 20.5
    assert bf.weather_for_window("2026-05-07T21:00:00Z", idx) is None
