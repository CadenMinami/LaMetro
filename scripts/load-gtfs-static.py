"""Download LA Metro's GTFS static feed, parse it into a compact pickle, and
optionally upload to S3 + run a sanity check against the live hot-vehicles
table.

Phase 4a: this is the *first* step of schedule-deviation work. Before we
write any deviation algorithm, we need to confirm:
  1. We can fetch GTFS static from Swiftly with our existing key.
  2. Static trip_ids overlap with live RT trip_ids (otherwise our trip-id
     join is broken and the whole algorithm falls apart).

Usage:
    python scripts/load-gtfs-static.py --out /tmp/gtfs.pkl
    python scripts/load-gtfs-static.py --out /tmp/gtfs.pkl --sanity-check
    python scripts/load-gtfs-static.py --upload-s3
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import logging
import os
import pickle
import sys
import time
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict
from typing import Any

import boto3

logger = logging.getLogger(__name__)

# LA Metro publishes static GTFS as two separate public zips on GitLab — bus
# and rail. Swiftly only serves real-time, not static, for this agency, so we
# pull from the source. No auth required.
DEFAULT_FEED_URLS = [
    "https://gitlab.com/LACMTA/gtfs_bus/-/raw/master/gtfs_bus.zip",
    "https://gitlab.com/LACMTA/gtfs_rail/-/raw/master/gtfs_rail.zip",
]
DEFAULT_BUCKET = os.environ.get(
    "ARCHIVE_BUCKET",
    "lametro-storagestack-archivebucket9decbf5d-mg7byceonzyn",
)
DEFAULT_HOT_TABLE = os.environ.get("HOT_VEHICLES_TABLE_NAME", "la-metro-hot-vehicles")
DEFAULT_SECRET = os.environ.get("SWIFTLY_SECRET_NAME", "la-metro/swiftly-api-key")


def get_api_key() -> str:
    """Prefer LA_METRO_API_KEY env, fall back to Secrets Manager."""
    key = os.environ.get("LA_METRO_API_KEY", "").strip()
    if key:
        return key
    client = boto3.client("secretsmanager")
    return client.get_secret_value(SecretId=DEFAULT_SECRET)["SecretString"]


def fetch_zip(url: str, timeout: float = 60.0) -> bytes:
    """Fetch a GTFS static zip from a public URL."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "la-metro-reliability/0.4 (gtfs-static-loader)"},
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"GTFS static fetch failed: HTTP {exc.code} from {url}. "
            f"Body (truncated): {exc.read()[:500]!r}"
        ) from exc
    elapsed_ms = int((time.monotonic() - started) * 1000)
    print(f"fetched {len(data):,} bytes from {url} in {elapsed_ms} ms")
    return data


_MERGE_DICT_KEYS = (
    "trips",
    "schedules",
    "shapes",
    "stops",
    "stop_arrivals",
    "trip_service",
    "service_calendar",
    "service_exceptions",
)


def merge_static(parts: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge multiple parsed GTFS dicts (e.g., bus + rail) into one v2 pickle.

    LA Metro uses disjoint trip_ids/shape_ids/stop_ids across bus and rail, so
    simple dict.update() works for those. service_ids occasionally overlap
    (both feeds may define a "WD" weekday calendar) but the entries are
    functionally identical, so last-write-wins is safe; we log collisions
    so a real divergence shows up loud.
    """
    merged: dict[str, Any] = {
        "schema_version": 2,
        "feed_version": "+".join((p.get("feed_version") or "") for p in parts),
        "feed_start_date": next(
            (p.get("feed_start_date") for p in parts if p.get("feed_start_date")),
            None,
        ),
        "feed_end_date": next(
            (p.get("feed_end_date") for p in parts if p.get("feed_end_date")),
            None,
        ),
        **{key: {} for key in _MERGE_DICT_KEYS},
    }
    collisions = {key: 0 for key in _MERGE_DICT_KEYS}
    for part in parts:
        for key in _MERGE_DICT_KEYS:
            existing = merged[key]
            for sub_id in part.get(key, {}):
                if sub_id in existing:
                    collisions[key] += 1
            existing.update(part.get(key, {}))
    nonzero = {k: v for k, v in collisions.items() if v}
    if nonzero:
        print(f"⚠️  id collisions while merging: {nonzero}")
    return merged


def _read_csv(zf: zipfile.ZipFile, name: str) -> list[dict[str, str]]:
    """Read a CSV inside the zip into a list of dicts. Tolerates UTF-8 BOM."""
    with zf.open(name) as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8-sig", newline="")
        return list(csv.DictReader(text))


def parse_static(zip_bytes: bytes) -> dict[str, Any]:
    """Parse GTFS static zip into a slim, algorithm-ready dict (schema v2).

    The Lambdas that consume this need two distinct slices of the data:

      *Phase 4c deviation* (enrichment Lambda):
      - trip_id → (route_id, shape_id) for routing decisions and route fallback.
      - trip_id → schedule tuple of (time_seconds_into_day, dist_along_shape_m)
        — pre-computed here so the Lambda never has to do per-row work.
      - shape_id → tuple of (lat, lon) — projected to LineString lazily on load.

      *Phase 4d arrivals* (query Lambda):
      - stop_id → metadata + lightweight per-stop arrival index pivoted from
        stop_times.txt, so a single dict lookup answers "what trips serve this
        stop?".
      - service_calendar + service_exceptions to filter trips active *today*
        (GTFS calendars are weekday-bitmask + exceptions overlay).
      - trip_id → service_id so each candidate trip can be filtered against
        active services without re-reading trips.txt at runtime.

    A trip is included in `schedules` only when we can produce a non-empty
    (time, distance) sequence — either via shape_dist_traveled in
    stop_times.txt (LA Metro provides it) or by projecting each stop's
    (lat, lon) onto the shape geometry. Trips with neither still appear in
    `stop_arrivals` so the arrivals API can return them with status="scheduled"
    (no live deviation possible without a schedule curve).
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = set(zf.namelist())
        required = {"trips.txt", "stops.txt", "stop_times.txt"}
        missing = required - names
        if missing:
            raise RuntimeError(f"GTFS zip missing required files: {sorted(missing)}")

        # ---- feed_info.txt is optional but contains the version we want.
        feed_version = None
        feed_start = None
        feed_end = None
        if "feed_info.txt" in names:
            rows = _read_csv(zf, "feed_info.txt")
            if rows:
                feed_version = rows[0].get("feed_version") or None
                feed_start = rows[0].get("feed_start_date") or None
                feed_end = rows[0].get("feed_end_date") or None
        if not feed_version:
            feed_version = dt.date.today().isoformat()

        # ---- trips.txt → trip_id -> (route_id, shape_id)
        # We also collect service_id alongside, into trip_service, so the
        # arrivals API can filter to today's active trips without reading
        # trips.txt at runtime.
        trips: dict[str, tuple[str, str]] = {}
        trip_service: dict[str, str] = {}
        trip_route_local: dict[str, str] = {}  # used while building stop_arrivals
        for row in _read_csv(zf, "trips.txt"):
            trip_id = row["trip_id"]
            route_id = row.get("route_id", "")
            shape_id = row.get("shape_id", "")
            service_id = row.get("service_id", "")
            trips[trip_id] = (route_id, shape_id)
            if service_id:
                trip_service[trip_id] = service_id
            if route_id:
                trip_route_local[trip_id] = route_id

        # ---- stops.txt
        # `stops_out` is the public v2 stops table the arrivals API and
        # frontend will consume. `stops_local` is a thin (lat, lon) lookup
        # used inside this function for shape projection — kept separate so
        # we don't pay for the metadata fields during the hot inner loop.
        stops_out: dict[str, dict[str, Any]] = {}
        stops_local: dict[str, tuple[float, float]] = {}
        for row in _read_csv(zf, "stops.txt"):
            try:
                lat = float(row["stop_lat"])
                lon = float(row["stop_lon"])
            except (KeyError, ValueError):
                continue
            stop_id = row["stop_id"]
            stops_local[stop_id] = (lat, lon)
            stops_out[stop_id] = {
                "name": row.get("stop_name", "") or "",
                "lat": lat,
                "lon": lon,
                "code": row.get("stop_code") or None,
                "parent_station": row.get("parent_station") or None,
            }

        # ---- calendar.txt → service_id -> weekday bitmask + date window
        # Optional per the GTFS spec, but LA Metro publishes it. When missing
        # we leave service_calendar empty; the arrivals API treats "no
        # calendar" as "all services active" so the feature still works.
        service_calendar: dict[str, dict[str, Any]] = {}
        if "calendar.txt" in names:
            for row in _read_csv(zf, "calendar.txt"):
                sid = row.get("service_id")
                if not sid:
                    continue
                service_calendar[sid] = {
                    "monday":    row.get("monday")    == "1",
                    "tuesday":   row.get("tuesday")   == "1",
                    "wednesday": row.get("wednesday") == "1",
                    "thursday":  row.get("thursday")  == "1",
                    "friday":    row.get("friday")    == "1",
                    "saturday":  row.get("saturday")  == "1",
                    "sunday":    row.get("sunday")    == "1",
                    "start_date": row.get("start_date") or "",
                    "end_date":   row.get("end_date") or "",
                }

        # ---- calendar_dates.txt → (YYYYMMDD, service_id) -> exception_type
        # exception_type 1 = service added on this date, 2 = removed.
        # Holiday schedules ride on this — e.g. a "weekday" service may be
        # explicitly removed on Christmas Day with type=2.
        service_exceptions: dict[tuple[str, str], int] = {}
        if "calendar_dates.txt" in names:
            for row in _read_csv(zf, "calendar_dates.txt"):
                sid = row.get("service_id")
                date = row.get("date")
                if not sid or not date:
                    continue
                try:
                    etype = int(row.get("exception_type", "0"))
                except ValueError:
                    continue
                if etype in (1, 2):
                    service_exceptions[(date, sid)] = etype

        # ---- stop_times.txt → two indexes from one pass
        # `per_trip` (sorted by sequence) feeds the slim per-trip schedule
        # used by the deviation algorithm. `stop_arrivals_lists` is the
        # by-stop pivot used by the arrivals API; each entry is
        # (trip_id, route_id, arr_s, stop_sequence). One pass over ~2M rows
        # builds both — only a single extra dict insert per row.
        per_trip: dict[str, list[tuple[int, int | None, int | None, float | None, str]]] = (
            defaultdict(list)
        )
        stop_arrivals_lists: dict[str, list[tuple[str, str, int, int]]] = defaultdict(list)
        for row in _read_csv(zf, "stop_times.txt"):
            try:
                trip_id = row["trip_id"]
                seq = int(row["stop_sequence"])
                arr = _hhmmss_to_seconds(row.get("arrival_time", ""))
                dep = _hhmmss_to_seconds(row.get("departure_time", ""))
                dist = _maybe_float(row.get("shape_dist_traveled", ""))
                stop_id = row.get("stop_id", "")
                per_trip[trip_id].append((seq, arr, dep, dist, stop_id))
                # Pivot for the arrivals API. Prefer arrival_time, fall back
                # to departure_time. Skip if neither — can't predict an
                # arrival without a time.
                t_arr = arr if arr is not None else dep
                if stop_id and t_arr is not None:
                    route_id = trip_route_local.get(trip_id, "")
                    stop_arrivals_lists[stop_id].append(
                        (trip_id, route_id, int(t_arr), seq)
                    )
            except (KeyError, ValueError):
                continue
        for stops_seq in per_trip.values():
            stops_seq.sort(key=lambda r: r[0])

        # Sort each stop's arrivals by time so the API can binary-search /
        # short-circuit on the next-N entries past `now`.
        stop_arrivals: dict[str, tuple[tuple[str, str, int, int], ...]] = {
            stop_id: tuple(sorted(rows, key=lambda r: r[2]))
            for stop_id, rows in stop_arrivals_lists.items()
        }

        # ---- shapes.txt → shape_id -> tuple of (lat, lon) sorted by sequence
        shapes_by_id: dict[str, tuple[tuple[float, float], ...]] = {}
        if "shapes.txt" in names:
            tmp: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
            for row in _read_csv(zf, "shapes.txt"):
                try:
                    tmp[row["shape_id"]].append(
                        (
                            int(row["shape_pt_sequence"]),
                            float(row["shape_pt_lat"]),
                            float(row["shape_pt_lon"]),
                        )
                    )
                except (KeyError, ValueError):
                    continue
            for shape_id, pts in tmp.items():
                pts.sort(key=lambda t: t[0])
                shapes_by_id[shape_id] = tuple((lat, lon) for _, lat, lon in pts)

    # Pre-cache each shape's projected xy + cumulative distance ONCE so the
    # 38k-trip loop below only does per-stop projection, not per-trip
    # reprojection of every shape point. With ~700 shapes feeding ~38k
    # trips, this is a 50x speedup over re-projecting inside the trip loop.
    shape_cache: dict[str, tuple[list[float], list[float], list[float]]] = {}
    import math as _math
    for shape_id, pts in shapes_by_id.items():
        if len(pts) < 2:
            continue
        mean_lat = sum(lat for lat, _ in pts) / len(pts)
        mlat = 111_320.0
        mlon = 111_320.0 * _math.cos(_math.radians(mean_lat))
        xs = [lon * mlon for _, lon in pts]
        ys = [lat * mlat for lat, _ in pts]
        cum = [0.0]
        for i in range(1, len(pts)):
            cum.append(
                cum[-1] + _math.hypot(xs[i] - xs[i - 1], ys[i] - ys[i - 1])
            )
        shape_cache[shape_id] = (xs, ys, cum)

    def _project(stop_ll: tuple[float, float], shape_id: str) -> float | None:
        cached = shape_cache.get(shape_id)
        if cached is None:
            return None
        xs, ys, cum = cached
        # Use the same xy projection the shape was cached with. Each shape
        # was projected at *its own* mean lat, so we recover that scale here.
        mean_lat = sum(ys) / (len(ys) * 111_320.0)
        mlon = 111_320.0 * _math.cos(_math.radians(mean_lat))
        px = stop_ll[1] * mlon
        py = stop_ll[0] * 111_320.0
        best_perp_sq = float("inf")
        best_along = 0.0
        for i in range(len(xs) - 1):
            ax, ay = xs[i], ys[i]
            dx = xs[i + 1] - ax
            dy = ys[i + 1] - ay
            seg_len_sq = dx * dx + dy * dy
            if seg_len_sq == 0:
                continue
            t = ((px - ax) * dx + (py - ay) * dy) / seg_len_sq
            if t < 0.0:
                t = 0.0
            elif t > 1.0:
                t = 1.0
            prx = ax + t * dx
            pry = ay + t * dy
            perp_sq = (px - prx) ** 2 + (py - pry) ** 2
            if perp_sq < best_perp_sq:
                best_perp_sq = perp_sq
                best_along = cum[i] + t * _math.sqrt(seg_len_sq)
        return best_along

    schedules: dict[str, tuple[tuple[int, float], ...]] = {}
    skipped_no_shape = 0
    skipped_no_schedule = 0
    for trip_id, stops_seq in per_trip.items():
        trip_meta = trips.get(trip_id)
        if not trip_meta:
            continue
        _, shape_id = trip_meta
        if not shape_id or shape_id not in shape_cache:
            skipped_no_shape += 1
            continue
        sched: list[tuple[int, float]] = []
        for _seq, arr, dep, dist, stop_id in stops_seq:
            t = dep if dep is not None else arr
            if t is None:
                continue
            d = dist
            if d is None:
                stop_ll = stops_local.get(stop_id)
                if stop_ll is None:
                    continue
                d = _project(stop_ll, shape_id)
                if d is None:
                    continue
            sched.append((int(t), float(d)))
        if not sched:
            skipped_no_schedule += 1
            continue
        sched.sort(key=lambda x: x[1])
        schedules[trip_id] = tuple(sched)

    if skipped_no_shape or skipped_no_schedule:
        print(
            f"slim-format trips skipped: no_shape={skipped_no_shape} "
            f"no_schedule={skipped_no_schedule}"
        )

    return {
        "schema_version": 2,
        "feed_version": feed_version,
        "feed_start_date": feed_start,
        "feed_end_date": feed_end,
        # Phase 4c — deviation algorithm
        "trips": trips,             # trip_id -> (route_id, shape_id)
        "schedules": schedules,     # trip_id -> tuple of (time_s, dist_m)
        "shapes": shapes_by_id,     # shape_id -> tuple of (lat, lon)
        # Phase 4d — arrivals API
        "stops": stops_out,                       # stop_id -> {name, lat, lon, code, parent_station}
        "stop_arrivals": stop_arrivals,           # stop_id -> ((trip_id, route_id, arr_s, seq), ...)
        "service_calendar": service_calendar,     # service_id -> weekday bitmask + window
        "service_exceptions": service_exceptions, # (YYYYMMDD, service_id) -> 1|2
        "trip_service": trip_service,             # trip_id -> service_id
    }


def _cum_distances(
    shape_pts: tuple[tuple[float, float], ...]
) -> tuple[float, ...]:
    """Cumulative meters along the shape, starting at 0. Equirectangular at
    the shape's mean latitude — within fractions of a percent for LA."""
    import math as _math
    if not shape_pts:
        return ()
    mean_lat = sum(lat for lat, _ in shape_pts) / len(shape_pts)
    m_per_deg_lat = 111_320.0
    m_per_deg_lon = 111_320.0 * _math.cos(_math.radians(mean_lat))
    out = [0.0]
    for i in range(1, len(shape_pts)):
        dlat = shape_pts[i][0] - shape_pts[i - 1][0]
        dlon = shape_pts[i][1] - shape_pts[i - 1][1]
        seg = _math.hypot(dlat * m_per_deg_lat, dlon * m_per_deg_lon)
        out.append(out[-1] + seg)
    return tuple(out)


def _project_latlon_onto_shape(
    point: tuple[float, float],
    shape_pts: tuple[tuple[float, float], ...],
    cum: tuple[float, ...],
) -> float | None:
    """Project a (lat, lon) onto the shape and return its distance-along.

    Uses the same equirectangular metric as `_cum_distances`. Without a
    dependency on Shapely we still get sub-meter accuracy for LA-scale routes.
    """
    import math as _math
    if len(shape_pts) < 2:
        return None
    mean_lat = sum(lat for lat, _ in shape_pts) / len(shape_pts)
    mlat = 111_320.0
    mlon = 111_320.0 * _math.cos(_math.radians(mean_lat))
    px = point[1] * mlon
    py = point[0] * mlat

    best_perp_sq = float("inf")
    best_along = 0.0
    for i in range(len(shape_pts) - 1):
        ax = shape_pts[i][1] * mlon
        ay = shape_pts[i][0] * mlat
        bx = shape_pts[i + 1][1] * mlon
        by = shape_pts[i + 1][0] * mlat
        dx = bx - ax
        dy = by - ay
        seg_len_sq = dx * dx + dy * dy
        if seg_len_sq == 0:
            continue
        t = ((px - ax) * dx + (py - ay) * dy) / seg_len_sq
        t = max(0.0, min(1.0, t))
        prx = ax + t * dx
        pry = ay + t * dy
        perp_sq = (px - prx) ** 2 + (py - pry) ** 2
        if perp_sq < best_perp_sq:
            best_perp_sq = perp_sq
            best_along = cum[i] + t * _math.sqrt(seg_len_sq)
    return best_along


def _hhmmss_to_seconds(s: str) -> int | None:
    """GTFS times can exceed 24:00:00 (next-day service). Treat as seconds since
    service-day midnight. Returns None on missing/malformed input."""
    s = (s or "").strip()
    if not s:
        return None
    parts = s.split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, sec = (int(p) for p in parts)
    except ValueError:
        return None
    return h * 3600 + m * 60 + sec


def _maybe_float(s: str) -> float | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def summarize(static: dict[str, Any]) -> None:
    print(f"schema_version     = {static.get('schema_version', 1)}")
    print(f"feed_version       = {static['feed_version']}")
    print(f"feed_start_date    = {static.get('feed_start_date')}")
    print(f"feed_end_date      = {static.get('feed_end_date')}")
    print(f"trips              = {len(static.get('trips', {})):,}")
    print(f"schedules          = {len(static.get('schedules', {})):,}")
    sched = static.get("schedules", {})
    if sched:
        print(
            f"  total schedule rows ≈ {sum(len(v) for v in sched.values()):,}"
        )
    print(f"shapes             = {len(static.get('shapes', {})):,}")
    print(f"stops              = {len(static.get('stops', {})):,}")
    arrivals = static.get("stop_arrivals", {})
    print(f"stop_arrivals      = {len(arrivals):,}")
    if arrivals:
        print(
            f"  total arrival rows ≈ {sum(len(v) for v in arrivals.values()):,}"
        )
    print(f"service_calendar   = {len(static.get('service_calendar', {})):,}")
    print(f"service_exceptions = {len(static.get('service_exceptions', {})):,}")
    print(f"trip_service       = {len(static.get('trip_service', {})):,}")


def sanity_check_against_hot_table(static: dict[str, Any], table_name: str) -> None:
    """Pull a sample of trip_ids from the live hot-vehicles table and intersect
    with static trip_ids. The whole point of this script is to discover ahead
    of time whether RT and static trip_ids actually match — if they don't, the
    deviation algorithm needs a translation step."""
    ddb = boto3.client("dynamodb")
    print(f"\n--- Sanity check: trip_id intersection against {table_name} ---")
    trip_ids_seen: set[str] = set()
    paginator = ddb.get_paginator("scan")
    pages = 0
    for page in paginator.paginate(
        TableName=table_name,
        ProjectionExpression="trip_id",
        Limit=1000,
    ):
        for item in page.get("Items", []):
            tid = item.get("trip_id", {}).get("S")
            if tid:
                trip_ids_seen.add(tid)
        pages += 1
        if pages >= 5:  # cap at 5k items — enough to learn the trip_id format
            break

    if not trip_ids_seen:
        print("no trip_ids in hot-vehicles yet — re-run after ingestion fires.")
        return

    static_trips = set(static["trips"].keys())
    matched = trip_ids_seen & static_trips
    missing = trip_ids_seen - static_trips
    pct = (len(matched) / len(trip_ids_seen)) * 100 if trip_ids_seen else 0
    print(f"RT trip_ids sampled        : {len(trip_ids_seen)}")
    print(f"matched in static trips    : {len(matched)} ({pct:.1f}%)")
    print(f"missing (no static match)  : {len(missing)}")
    print("sample RT trip_ids         :", sorted(trip_ids_seen)[:5])
    print("sample static trip_ids     :", sorted(static_trips)[:5])
    if missing and len(missing) > len(matched):
        print(
            "\n⚠️  More RT trip_ids miss static than match. The deviation algo "
            "will need a translation step (e.g., strip a suffix or join via "
            "block_id/service_id). Inspect the samples above."
        )


def upload_to_s3(
    static: dict[str, Any], zip_bytes: bytes, bucket: str, prefix: str = "gtfs-static"
) -> dict[str, str]:
    """Upload both the parsed pickle and the raw zip, plus update a `current`
    pointer. Versioned by feed_version + load timestamp so we can roll back."""
    s3 = boto3.client("s3")
    version = static["feed_version"]
    loaded_at = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = f"{prefix}/v={version}/loaded_at={loaded_at}"
    pickle_key = f"{base}/parsed.pkl"
    zip_key = f"{base}/gtfs.zip"
    current_key = f"{prefix}/current.txt"

    s3.put_object(
        Bucket=bucket,
        Key=pickle_key,
        Body=pickle.dumps(static, protocol=pickle.HIGHEST_PROTOCOL),
        ContentType="application/octet-stream",
    )
    s3.put_object(
        Bucket=bucket, Key=zip_key, Body=zip_bytes, ContentType="application/zip"
    )
    s3.put_object(
        Bucket=bucket,
        Key=current_key,
        Body=pickle_key.encode("utf-8"),
        ContentType="text/plain",
    )
    print(f"uploaded pickle  -> s3://{bucket}/{pickle_key}")
    print(f"uploaded raw zip -> s3://{bucket}/{zip_key}")
    print(f"current pointer  -> s3://{bucket}/{current_key}")
    return {"pickle_key": pickle_key, "zip_key": zip_key, "current_key": current_key}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        action="append",
        default=None,
        help="GTFS static URL. Pass multiple times to merge feeds. "
        "Default: LA Metro bus + rail public GitLab zips.",
    )
    parser.add_argument("--out", default="", help="Local pickle output path")
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help=f"Upload to S3 (bucket: {DEFAULT_BUCKET})",
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--sanity-check",
        action="store_true",
        help="Compare static trip_ids against live hot-vehicles table",
    )
    parser.add_argument("--hot-table", default=DEFAULT_HOT_TABLE)
    args = parser.parse_args(argv)

    urls = args.url or DEFAULT_FEED_URLS
    parts = []
    combined_zip = io.BytesIO()  # placeholder; we keep the first zip for upload
    first_zip_bytes: bytes | None = None
    for u in urls:
        zip_bytes = fetch_zip(u)
        if first_zip_bytes is None:
            first_zip_bytes = zip_bytes
        parts.append(parse_static(zip_bytes))
    static = merge_static(parts) if len(parts) > 1 else parts[0]
    summarize(static)

    if args.out:
        with open(args.out, "wb") as fh:
            pickle.dump(static, fh, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"wrote pickle    -> {args.out}")

    if args.upload_s3:
        # Upload only the first zip alongside the merged pickle. The pickle is
        # the source of truth for the Lambda; the zip is for archival/debug.
        upload_to_s3(static, first_zip_bytes or b"", args.bucket)

    if args.sanity_check:
        sanity_check_against_hot_table(static, args.hot_table)

    return 0


if __name__ == "__main__":
    sys.exit(main())
