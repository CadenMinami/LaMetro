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
