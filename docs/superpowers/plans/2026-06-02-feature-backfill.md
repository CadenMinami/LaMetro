# Feature Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A one-time local script (`ml/backfill_features.py`) that turns 26 days of raw GTFS-RT positions in S3 into the same `route_window_features` table the live `feature_snapshot` Lambda produces, using real schedule-deviation delays.

**Architecture:** Pure, unit-tested transform functions (parse → filter → dedupe → delay → aggregate → weather → write) wired by a day-by-day CLI. Reuses `lambdas/shared/deviation`, `lambdas/shared/gtfs_static`, `lambdas/aggregation`, and `lambdas/feature_snapshot` so output matches the live feed exactly. S3 + Open-Meteo are mocked in tests.

**Tech Stack:** Python 3.11+, boto3 (S3), shapely (via shared deviation), stdlib `urllib`/`json`/`gzip`/`zoneinfo`, pytest.

**Key facts (verified):**
- Raw events: `s3://<archive>/raw-events/year=…/month=…/day=…/hour=…/*.gz`, **concatenated JSON** (no newline delimiter), ~6,500 records/file, ~47M total, May 7–Jun 2.
- Record shape: `{"vehicle_id","route_id","trip_id","lat","lon","bearing","speed_mps","vehicle_timestamp","feed_timestamp"}`. ~half have empty `route_id`/`trip_id` (deadheading — skip).
- GTFS static: only 2 versions exist (May 7 & 8, same signup). `current.txt` → May 8 pickle. **One `load_from_s3(bucket)` covers the whole window** — no per-date resolution.
- Archive bucket name: `lametro-storagestack-archivebucket9decbf5d-mg7byceonzyn`.
- Reused signatures:
  - `deviation.compute_delay_seconds(shape, schedule, vehicle_lat, vehicle_lon, seconds_into_service_day) -> int | None`
  - `gtfs_static.load_from_s3(bucket, current_key="gtfs-static/current.txt", shapes=True) -> GTFSStatic`
  - `GTFSStatic.shape_for_trip(trip_id)`, `.schedule_for_trip(trip_id)`
  - `aggregation.handler.aggregate_by_route(vehicles) -> {route_id: {vehicle_count, avg_delay_seconds, p95_delay_seconds, on_time_pct}}` (each vehicle needs `route_id` + `delay_seconds`)
  - `feature_snapshot.handler.build_feature_record(agg_row, weather, ingested_at_iso)` where `agg_row` has `route_id, window_start_iso, avg_delay_seconds, p95_delay_seconds, on_time_pct, vehicle_count`

---

## File Structure

- **Create `ml/backfill_features.py`** — all transform functions + `main()` CLI. One file (mirrors `ml/bootstrap.py`'s single-file CLI shape).
- **Create `ml/tests/test_backfill_features.py`** — unit tests for every pure function; S3/weather mocked.

Imports of lambda code use the same path the tests/CI use: `from lambdas.shared import deviation, gtfs_static`, `from lambdas.aggregation import handler as agg`, `from lambdas.feature_snapshot import handler as fs`.

---

## Task 1: Concatenated-JSON parser

**Files:**
- Create: `ml/backfill_features.py`
- Test: `ml/tests/test_backfill_features.py`

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: FAIL — `AttributeError: module 'ml.backfill_features' has no attribute 'iter_json_objects'`

- [ ] **Step 3: Write minimal implementation**

```python
"""Backfill route_window_features from raw GTFS-RT events in S3.

One-time local script. See docs/superpowers/specs/2026-06-02-feature-backfill-design.md.
"""

from __future__ import annotations

import json
from typing import Any, Iterator

_DECODER = json.JSONDecoder()


def iter_json_objects(raw: bytes) -> Iterator[dict[str, Any]]:
    """Yield each JSON object from a Firehose blob of *concatenated* JSON
    (no delimiter between objects). Stops at the first undecodable tail."""
    text = raw.decode("utf-8")
    i, n = 0, len(text)
    while i < n:
        # Skip inter-object whitespace/newlines.
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        try:
            obj, end = _DECODER.raw_decode(text, i)
        except json.JSONDecodeError:
            break
        yield obj
        i = end
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add ml/backfill_features.py ml/tests/test_backfill_features.py
git commit -m "backfill: concatenated-JSON parser for raw-event blobs"
```

---

## Task 2: Routed-record filter + service-day seconds

**Files:**
- Modify: `ml/backfill_features.py`
- Test: `ml/tests/test_backfill_features.py`

- [ ] **Step 1: Write the failing test**

```python
def test_is_routed_requires_route_and_trip():
    assert bf.is_routed({"route_id": "70-13196", "trip_id": "t1"}) is True
    assert bf.is_routed({"route_id": "", "trip_id": "t1"}) is False
    assert bf.is_routed({"route_id": "70-13196", "trip_id": ""}) is False
    assert bf.is_routed({}) is False


def test_seconds_into_service_day_la_local():
    # 2026-05-07 12:00:00 America/Los_Angeles (PDT, UTC-7) == 19:00:00 UTC.
    # epoch for 2026-05-07T19:00:00Z = 1778518800.
    secs = bf.seconds_into_service_day(1778518800)
    assert secs == 12 * 3600  # noon local
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: FAIL — `AttributeError: ... has no attribute 'is_routed'`

- [ ] **Step 3: Write minimal implementation**

Add near the top imports:

```python
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

LA_TZ = ZoneInfo("America/Los_Angeles")
WINDOW_MINUTES = 5
```

Add functions:

```python
def is_routed(rec: dict[str, Any]) -> bool:
    """True only when the record can be schedule-matched (has route + trip)."""
    return bool(rec.get("route_id")) and bool(rec.get("trip_id"))


def seconds_into_service_day(epoch: int) -> int:
    """Seconds since LA-local midnight for a unix timestamp. Used as the
    service-day clock the GTFS schedule is expressed in. (Owl trips that cross
    midnight fall outside the schedule window and yield a null delay — an
    accepted edge for aggregate features.)"""
    local = datetime.fromtimestamp(int(epoch), tz=LA_TZ)
    return local.hour * 3600 + local.minute * 60 + local.second
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ml/backfill_features.py ml/tests/test_backfill_features.py
git commit -m "backfill: routed-record filter + LA service-day seconds"
```

---

## Task 3: Window flooring + dedupe to latest-per-(vehicle, window)

**Files:**
- Modify: `ml/backfill_features.py`
- Test: `ml/tests/test_backfill_features.py`

- [ ] **Step 1: Write the failing test**

```python
def test_window_start_iso_floors_to_5min_utc():
    # 2026-05-07T19:07:42Z floors to 19:05:00Z.
    assert bf.window_start_iso(1778518800 + 7 * 60 + 42) == "2026-05-07T19:05:00Z"


def test_dedupe_keeps_latest_position_per_vehicle_window():
    recs = [
        {"vehicle_id": "v1", "route_id": "r", "trip_id": "t", "lat": 1.0, "lon": 1.0,
         "vehicle_timestamp": 1778518800},          # 19:00:00Z, window 19:00
        {"vehicle_id": "v1", "route_id": "r", "trip_id": "t", "lat": 2.0, "lon": 2.0,
         "vehicle_timestamp": 1778518830},          # 19:00:30Z, same window — newer
        {"vehicle_id": "v1", "route_id": "r", "trip_id": "t", "lat": 9.0, "lon": 9.0,
         "vehicle_timestamp": 1778519100},          # 19:05:00Z, next window
    ]
    out = bf.dedupe_latest(recs)
    # keyed by (vehicle, window_iso) → latest record
    assert out[("v1", "2026-05-07T19:00:00Z")]["lat"] == 2.0
    assert out[("v1", "2026-05-07T19:05:00Z")]["lat"] == 9.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: FAIL — no attribute `window_start_iso`

- [ ] **Step 3: Write minimal implementation**

```python
def window_start_iso(epoch: int) -> str:
    """Floor a unix timestamp to its 5-min UTC window start, ISO-Z."""
    dt = datetime.fromtimestamp(int(epoch), tz=timezone.utc)
    floored = dt.replace(
        minute=(dt.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
        second=0, microsecond=0,
    )
    return floored.strftime("%Y-%m-%dT%H:%M:%SZ")


def dedupe_latest(records: "Iterable[dict[str, Any]]") -> dict[tuple[str, str], dict[str, Any]]:
    """Keep the newest position per (vehicle_id, window). The perf move: this is
    roughly what the live pipeline scored — one position per vehicle per window."""
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for r in records:
        ts = int(r["vehicle_timestamp"])
        key = (r["vehicle_id"], window_start_iso(ts))
        cur = best.get(key)
        if cur is None or ts > int(cur["vehicle_timestamp"]):
            best[key] = r
    return best
```

Add `Iterable` to the typing import: `from typing import Any, Iterable, Iterator`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ml/backfill_features.py ml/tests/test_backfill_features.py
git commit -m "backfill: 5-min window flooring + latest-per-vehicle dedupe"
```

---

## Task 4: Per-position delay via shared deviation

**Files:**
- Modify: `ml/backfill_features.py`
- Test: `ml/tests/test_backfill_features.py`

- [ ] **Step 1: Write the failing test**

Use a fake GTFS object exposing the two methods the function calls — avoids building a real pickle.

```python
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
    rec = {"trip_id": "t", "lat": 34.05, "lon": -118.24,
           "vehicle_timestamp": 1778518800}

    assert bf.delay_for_record(rec, gtfs) == 42
    assert captured["shape"] == "SHAPE"
    assert captured["lat"] == 34.05
    assert captured["secs"] == 12 * 3600  # noon LA


def test_delay_for_record_none_when_trip_unknown(monkeypatch):
    gtfs = _FakeGTFS(shape=None, schedule=None)
    rec = {"trip_id": "t", "lat": 1.0, "lon": 1.0, "vehicle_timestamp": 1778518800}
    assert bf.delay_for_record(rec, gtfs) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: FAIL — no attribute `delay_for_record` (and `bf.deviation` not imported yet)

- [ ] **Step 3: Write minimal implementation**

Add import near top:

```python
from lambdas.shared import deviation, gtfs_static
```

Add function:

```python
def delay_for_record(rec: dict[str, Any], gtfs: "gtfs_static.GTFSStatic") -> int | None:
    """Schedule deviation (sec) for one position, or None if not computable."""
    trip_id = rec["trip_id"]
    shape = gtfs.shape_for_trip(trip_id)
    schedule = gtfs.schedule_for_trip(trip_id)
    if shape is None or not schedule:
        return None
    return deviation.compute_delay_seconds(
        shape, schedule,
        float(rec["lat"]), float(rec["lon"]),
        seconds_into_service_day(rec["vehicle_timestamp"]),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ml/backfill_features.py ml/tests/test_backfill_features.py
git commit -m "backfill: per-position delay via shared deviation"
```

---

## Task 5: Window → feature records (reuse aggregation + feature_snapshot)

**Files:**
- Modify: `ml/backfill_features.py`
- Test: `ml/tests/test_backfill_features.py`

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: FAIL — no attribute `records_for_window`

- [ ] **Step 3: Write minimal implementation**

Add imports:

```python
from lambdas.aggregation import handler as agg
from lambdas.feature_snapshot import handler as fs
```

Add function:

```python
def records_for_window(
    window_iso: str,
    vehicles: list[dict[str, Any]],
    weather: dict | None,
    ingested_at_iso: str,
) -> list[dict[str, Any]]:
    """Aggregate one window's vehicles into per-route feature records, matching
    the live feature_snapshot schema exactly."""
    by_route = agg.aggregate_by_route(vehicles)   # {route_id: {count, avg, p95, on_time}}
    out: list[dict[str, Any]] = []
    for route_id, a in by_route.items():
        agg_row = {
            "route_id": route_id,
            "window_start_iso": window_iso,
            "avg_delay_seconds": a["avg_delay_seconds"],
            "p95_delay_seconds": a["p95_delay_seconds"],
            "on_time_pct": a["on_time_pct"],
            "vehicle_count": a["vehicle_count"],
        }
        out.append(fs.build_feature_record(agg_row, weather, ingested_at_iso))
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ml/backfill_features.py ml/tests/test_backfill_features.py
git commit -m "backfill: window aggregation -> feature records (reuse agg + feature_snapshot)"
```

---

## Task 6: Historical weather (Open-Meteo Archive) + window lookup

**Files:**
- Modify: `ml/backfill_features.py`
- Test: `ml/tests/test_backfill_features.py`

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: FAIL — no attribute `parse_archive_weather`

- [ ] **Step 3: Write minimal implementation**

```python
import urllib.request

ARCHIVE_URL = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude=34.05&longitude=-118.24"
    "&hourly=temperature_2m,precipitation&timezone=UTC"
    "&start_date={start}&end_date={end}"
)


def parse_archive_weather(body: bytes) -> dict[str, dict[str, Any]]:
    """Index Open-Meteo Archive hourly arrays by their 'YYYY-MM-DDTHH:MM' key."""
    doc = json.loads(body)
    hourly = doc.get("hourly") or {}
    times = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    precs = hourly.get("precipitation") or []
    out: dict[str, dict[str, Any]] = {}
    for t, temp, prec in zip(times, temps, precs):
        if temp is None or prec is None:
            continue
        out[t] = {"temp_c": float(temp), "precip_mm": float(prec), "observed_at": t}
    return out


def weather_for_window(window_iso: str, idx: dict[str, dict[str, Any]]) -> dict | None:
    """Look up the archive hour ('YYYY-MM-DDTHH:00') covering this window."""
    hour_key = window_iso[:13] + ":00"   # '2026-05-07T19:05:00Z' -> '2026-05-07T19:00'
    return idx.get(hour_key)


def fetch_archive_weather(start_date: str, end_date: str) -> dict[str, dict[str, Any]]:
    """Fetch the whole date-range's hourly weather in one call. Returns {} on failure."""
    url = ARCHIVE_URL.format(start=start_date, end=end_date)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:  # noqa: S310 fixed URL
            return parse_archive_weather(resp.read())
    except Exception:
        return {}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ml/backfill_features.py ml/tests/test_backfill_features.py
git commit -m "backfill: historical weather via Open-Meteo Archive + window lookup"
```

---

## Task 7: Deterministic S3 output key + gzip write

**Files:**
- Modify: `ml/backfill_features.py`
- Test: `ml/tests/test_backfill_features.py`

- [ ] **Step 1: Write the failing test**

```python
import gzip
from unittest.mock import MagicMock


def test_backfill_s3_key_is_deterministic():
    k1 = bf.backfill_s3_key("2026-05-07T19:05:00Z")
    k2 = bf.backfill_s3_key("2026-05-07T19:05:00Z")
    assert k1 == k2  # idempotent — no random suffix
    assert k1 == (
        "processed-features/year=2026/month=05/day=07/hour=19/"
        "window=2026-05-07T19:05:00Z-backfill.jsonl.gz"
    )


def test_write_window_records_puts_one_gzip_object():
    s3 = MagicMock()
    rows = [{"route_id": "70", "window_start_iso": "2026-05-07T19:05:00Z"}]
    key = bf.write_window_records(s3, "bkt", "2026-05-07T19:05:00Z", rows)
    s3.put_object.assert_called_once()
    kw = s3.put_object.call_args.kwargs
    assert kw["Bucket"] == "bkt"
    assert kw["Key"] == key
    assert kw["ContentEncoding"] == "gzip"
    assert json.loads(gzip.decompress(kw["Body"]).decode())["route_id"] == "70"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: FAIL — no attribute `backfill_s3_key`

- [ ] **Step 3: Write minimal implementation**

```python
import gzip

PROCESSED_PREFIX = "processed-features"


def backfill_s3_key(window_iso: str) -> str:
    """Deterministic key (no random suffix) so re-running a day overwrites
    rather than duplicating. '-backfill' distinguishes from live feature_snapshot."""
    dt = datetime.strptime(window_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (
        f"{PROCESSED_PREFIX}"
        f"/year={dt:%Y}/month={dt:%m}/day={dt:%d}/hour={dt:%H}"
        f"/window={window_iso}-backfill.jsonl.gz"
    )


def write_window_records(s3, bucket: str, window_iso: str, rows: list[dict[str, Any]]) -> str:
    """Gzip + PUT one JSONL object for a window. Returns the key."""
    body = "\n".join(json.dumps(r) for r in rows).encode("utf-8")
    key = backfill_s3_key(window_iso)
    s3.put_object(
        Bucket=bucket, Key=key, Body=gzip.compress(body),
        ContentType="application/x-ndjson", ContentEncoding="gzip",
    )
    return key
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ml/backfill_features.py ml/tests/test_backfill_features.py
git commit -m "backfill: deterministic S3 key + gzip-JSONL write"
```

---

## Task 8: Day driver (compose the pipeline for one date)

**Files:**
- Modify: `ml/backfill_features.py`
- Test: `ml/tests/test_backfill_features.py`

- [ ] **Step 1: Write the failing test**

`process_day` ties everything together. Inject S3 (mock), GTFS (fake), and weather index so the test stays offline. It reads raw objects for a date, returns `(windows_written, records_written)`.

```python
def test_process_day_writes_features_end_to_end(monkeypatch):
    # One raw object, two positions for route "70" trip "t1" in window 19:00.
    blob = (
        b'{"vehicle_id":"v1","route_id":"70","trip_id":"t1","lat":34.05,'
        b'"lon":-118.24,"vehicle_timestamp":1778518800}'
        b'{"vehicle_id":"v2","route_id":"70","trip_id":"t1","lat":34.06,'
        b'"lon":-118.25,"vehicle_timestamp":1778518810}'
        b'{"vehicle_id":"v3","route_id":"","trip_id":"","lat":0.0,'   # deadhead, skipped
        b'"lon":0.0,"vehicle_timestamp":1778518810}'
    )
    s3 = MagicMock()
    # list_day_keys + read use these two calls:
    monkeypatch.setattr(bf, "list_day_keys", lambda s3c, b, d: ["raw-events/k.gz"])
    monkeypatch.setattr(bf, "read_gz", lambda s3c, b, k: gzip.compress(blob))
    monkeypatch.setattr(bf, "delay_for_record", lambda rec, gtfs: 60)

    gtfs = _FakeGTFS(shape="S", schedule=(("s",),))
    weather_idx = {"2026-05-07T19:00": {"temp_c": 20.0, "precip_mm": 0.0,
                                        "observed_at": "2026-05-07T19:00"}}

    windows, records = bf.process_day(
        s3, "bkt", "2026-05-07", gtfs, weather_idx, ingested_at_iso="2026-06-02T00:00:00Z",
    )
    assert windows == 1
    assert records == 1                       # one route in one window
    written = json.loads(gzip.decompress(s3.put_object.call_args.kwargs["Body"]).decode())
    assert written["route_id"] == "70" and written["vehicle_count"] == 2
    assert written["temp_c"] == 20.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: FAIL — no attribute `process_day` / `list_day_keys` / `read_gz`

- [ ] **Step 3: Write minimal implementation**

```python
from collections import defaultdict


def list_day_keys(s3, bucket: str, date_str: str) -> list[str]:
    """All raw-event object keys for a YYYY-MM-DD date (paginated)."""
    y, m, d = date_str.split("-")
    prefix = f"raw-events/year={y}/month={m}/day={d}/"
    keys: list[str] = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys.extend(o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".gz"))
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return keys


def read_gz(s3, bucket: str, key: str) -> bytes:
    """Raw gzip bytes of an S3 object."""
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def process_day(
    s3, bucket: str, date_str: str, gtfs, weather_idx: dict, ingested_at_iso: str,
) -> tuple[int, int]:
    """Backfill one date. Returns (windows_written, records_written)."""
    # 1. Read + parse + filter to routed positions.
    routed: list[dict[str, Any]] = []
    for key in list_day_keys(s3, bucket, date_str):
        for rec in iter_json_objects(gzip.decompress(read_gz(s3, bucket, key))):
            if is_routed(rec):
                routed.append(rec)

    # 2. Dedupe to latest per (vehicle, window).
    deduped = dedupe_latest(routed)

    # 3. Attach delay, group by window.
    by_window: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (vehicle_id, window_iso), rec in deduped.items():
        delay = delay_for_record(rec, gtfs)
        by_window[window_iso].append({"route_id": rec["route_id"], "delay_seconds": delay})

    # 4. Aggregate + write one object per window.
    windows_written = records_written = 0
    for window_iso, vehicles in by_window.items():
        rows = records_for_window(
            window_iso, vehicles, weather_for_window(window_iso, weather_idx), ingested_at_iso,
        )
        if rows:
            write_window_records(s3, bucket, window_iso, rows)
            windows_written += 1
            records_written += len(rows)
    return windows_written, records_written
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ml/backfill_features.py ml/tests/test_backfill_features.py
git commit -m "backfill: day driver composing the full pipeline"
```

---

## Task 9: CLI (`main`) — iterate dates, load GTFS + weather once

**Files:**
- Modify: `ml/backfill_features.py`
- Test: `ml/tests/test_backfill_features.py`

- [ ] **Step 1: Write the failing test**

```python
def test_daterange_inclusive():
    assert bf.daterange("2026-05-07", "2026-05-09") == [
        "2026-05-07", "2026-05-08", "2026-05-09",
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: FAIL — no attribute `daterange`

- [ ] **Step 3: Write minimal implementation**

```python
import argparse
from datetime import date, timedelta


def daterange(start_date: str, end_date: str) -> list[str]:
    """Inclusive list of YYYY-MM-DD strings from start to end."""
    d0 = date.fromisoformat(start_date)
    d1 = date.fromisoformat(end_date)
    out, cur = [], d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def main(argv: list[str] | None = None) -> int:
    import boto3

    p = argparse.ArgumentParser(description="Backfill route_window_features from raw events.")
    p.add_argument("--bucket", required=True, help="Archive bucket name.")
    p.add_argument("--start", required=True, help="Start date YYYY-MM-DD.")
    p.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive).")
    args = p.parse_args(argv)

    s3 = boto3.client("s3")
    gtfs = gtfs_static.load_from_s3(args.bucket, shapes=True)   # current pointer covers the window
    weather_idx = fetch_archive_weather(args.start, args.end)
    ingested_at_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    total_w = total_r = 0
    for day in daterange(args.start, args.end):
        w, r = process_day(s3, args.bucket, day, gtfs, weather_idx, ingested_at_iso)
        total_w += w
        total_r += r
        print(f"{day}: {w} windows, {r} records")
    print(f"DONE: {total_w} windows, {total_r} feature records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest ml/tests/test_backfill_features.py -q`
Expected: PASS (all tasks' tests)

- [ ] **Step 5: Commit**

```bash
git add ml/backfill_features.py ml/tests/test_backfill_features.py
git commit -m "backfill: CLI iterating dates, single GTFS + weather load"
```

---

## Task 10: One-day live dry run + Athena validation

**Files:** none (operational verification).

- [ ] **Step 1: Run a single day against real S3**

```bash
python -m ml.backfill_features \
  --bucket lametro-storagestack-archivebucket9decbf5d-mg7byceonzyn \
  --start 2026-05-08 --end 2026-05-08
```
Expected: prints `2026-05-08: <N> windows, <M> records` with N, M > 0. If M is suspiciously low (e.g. 0 routes matched), the GTFS trip-match rate is off — investigate before running the full range.

- [ ] **Step 2: Confirm objects landed**

```bash
aws s3 ls s3://lametro-storagestack-archivebucket9decbf5d-mg7byceonzyn/processed-features/year=2026/month=05/day=08/ --recursive | head
```
Expected: `…-backfill.jsonl.gz` objects across hour partitions.

- [ ] **Step 3: Validate via Athena (or local fetch)**

Quick local check (no Athena cost): download one object and confirm schema:
```bash
aws s3 cp "$(aws s3 ls s3://lametro-storagestack-archivebucket9decbf5d-mg7byceonzyn/processed-features/year=2026/month=05/day=08/ --recursive | grep backfill | head -1 | awk '{print $4}' | sed 's#^#s3://lametro-storagestack-archivebucket9decbf5d-mg7byceonzyn/#')" - | gunzip | head -1
```
Expected: a JSON record with `route_id, window_start_iso, avg_delay_seconds, p95_delay_seconds, on_time_pct, vehicle_count, temp_c, precip_mm, ingested_at`.

- [ ] **Step 4: Run the full range**

```bash
python -m ml.backfill_features \
  --bucket lametro-storagestack-archivebucket9decbf5d-mg7byceonzyn \
  --start 2026-05-07 --end 2026-06-02
```
Expected: per-day lines, then `DONE:` totals. Spot-check a non-trivial total of feature records (interview "finding" depends on this being real).

- [ ] **Step 5: Mark backfill task complete**

Update task #3 to completed; note record counts + any caveats (e.g. days with low match rate) for the 7b spec.

---

## Self-Review

**Spec coverage:**
- Local script ✔ (whole plan). Concatenated-JSON parse ✔ T1. Routed filter ✔ T2. Dedupe-for-perf ✔ T3. Real delay via deviation + single GTFS ✔ T4 (+ GTFS load in T9). Window aggregation reusing aggregation+feature_snapshot ✔ T5. Historical weather ✔ T6. Identical path + deterministic/idempotent key ✔ T7. Day-by-day driver ✔ T8. CLI mirroring bootstrap ✔ T9. Athena validation ✔ T10. "No silent sampling" — the driver applies no cap; if one is ever added it must log (none added here).

**Placeholder scan:** No TBD/TODO; every code step has complete code; commands have expected output.

**Type consistency:** `iter_json_objects`→`is_routed`/`dedupe_latest`→`delay_for_record`→`records_for_window`→`write_window_records` all consumed consistently in `process_day`. `weather_for_window`/`fetch_archive_weather`/`parse_archive_weather` agree on the `{hour: {temp_c,precip_mm,observed_at}}` shape. `backfill_s3_key` used by `write_window_records`. GTFS methods (`shape_for_trip`/`schedule_for_trip`) match `gtfs_static.py`.

**Perf note carried:** multiprocessing is *not* in this plan (YAGNI for a first run). The dedupe + day-by-day keeps a single-process run tractable; if the full-range run in T10 is too slow, add `--workers` as a follow-up rather than pre-building it.
