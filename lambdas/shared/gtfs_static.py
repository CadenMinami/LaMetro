"""Load a parsed GTFS-static pickle from S3 and turn it into in-memory
structures the deviation algorithm can use.

The pickle layout is whatever `scripts/load-gtfs-static.py` writes:

    {
      "feed_version": str,
      "trips":      {trip_id: {"route_id", "service_id", "shape_id", ...}},
      "stop_times": {trip_id: [{"stop_id", "stop_sequence", "arr_s",
                                "dep_s", "shape_dist_traveled"}, ...]},
      "stops":      {stop_id: {"lat", "lon", "name"}},
      "shapes":     {shape_id: [(lat, lon, dist_traveled), ...]},
    }

We turn that into the algorithm-ready form:

    GTFSStatic.shapes:   {shape_id: shapely.LineString in projected meters}
    GTFSStatic.trip_idx: {trip_id: TripIndex(shape_id, schedule_tuples)}

Sharing LineStrings across trips with the same shape_id is the big memory
win — LA Metro has ~700 distinct shapes feeding ~38k trips, so we collapse
the most expensive per-trip object by ~50×.
"""

from __future__ import annotations

import logging
import pickle
import time
from dataclasses import dataclass
from typing import Any

import boto3
from shapely.geometry import LineString, Point

from .deviation import latlon_to_xy

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TripIndex:
    """Algorithm-ready representation of a single trip."""
    shape_id: str
    # Sorted by distance, increasing. Each element: (time_s_into_day, dist_m).
    schedule: tuple[tuple[int, float], ...]


@dataclass(slots=True)
class GTFSStatic:
    feed_version: str
    shapes: dict[str, LineString]
    trip_idx: dict[str, TripIndex]
    # Carry the route_id so the enrichment Lambda can fall back to the static
    # one when GTFS-RT omits it (it sometimes does between trips).
    trip_route: dict[str, str]

    def shape_for_trip(self, trip_id: str) -> LineString | None:
        ti = self.trip_idx.get(trip_id)
        if ti is None:
            return None
        return self.shapes.get(ti.shape_id)

    def schedule_for_trip(
        self, trip_id: str
    ) -> tuple[tuple[int, float], ...] | None:
        ti = self.trip_idx.get(trip_id)
        return None if ti is None else ti.schedule


def _build_linestring(
    pts: list[tuple[float, float, float | None]],
) -> LineString | None:
    """pts is a list of (lat, lon, dist) sorted by sequence. We only need
    (lat, lon) for the geometry; dist is consumed elsewhere if present."""
    coords: list[tuple[float, float]] = []
    for lat, lon, _dist in pts:
        xy = latlon_to_xy(lat, lon)
        if not coords or coords[-1] != xy:
            coords.append(xy)
    if len(coords) < 2:
        return None
    return LineString(coords)


def _build_schedule_for_trip(
    stop_times: list[dict[str, Any]],
    shape: LineString | None,
    stops_by_id: dict[str, dict[str, Any]],
) -> tuple[tuple[int, float], ...]:
    """Build the (time_s, dist_m) schedule for a trip.

    Prefer GTFS's `shape_dist_traveled` when present (LA Metro provides it).
    Fall back to projecting each stop's (lat, lon) onto the shape — slower
    but works for feeds that don't ship distances.
    """
    out: list[tuple[int, float]] = []
    for st in stop_times:
        # Use departure if present, else arrival. Either is fine for a
        # mid-route position estimate.
        t = st.get("dep_s")
        if t is None:
            t = st.get("arr_s")
        if t is None:
            continue
        dist = st.get("shape_dist_traveled")
        if dist is None and shape is not None:
            stop = stops_by_id.get(st["stop_id"])
            if stop is None:
                continue
            point = Point(*latlon_to_xy(stop["lat"], stop["lon"]))
            dist = shape.project(point)
        if dist is None:
            continue
        out.append((int(t), float(dist)))
    # Sort by distance, not by stop_sequence — matters if a feed has the
    # vehicle backtrack (rare; usually a data error).
    out.sort(key=lambda x: x[1])
    return tuple(out)


def build_static(parsed: dict[str, Any]) -> GTFSStatic:
    """Convert a parsed GTFS dict (from scripts/load-gtfs-static.py) into
    algorithm-ready in-memory structures."""
    started = time.monotonic()

    raw_shapes = parsed.get("shapes", {})
    shapes: dict[str, LineString] = {}
    for shape_id, pts in raw_shapes.items():
        ls = _build_linestring(pts)
        if ls is not None:
            shapes[shape_id] = ls

    stops_by_id = parsed.get("stops", {})
    raw_trips = parsed.get("trips", {})
    raw_stop_times = parsed.get("stop_times", {})

    trip_idx: dict[str, TripIndex] = {}
    trip_route: dict[str, str] = {}
    skipped_no_shape = 0
    skipped_no_schedule = 0

    for trip_id, t in raw_trips.items():
        shape_id = t.get("shape_id", "")
        shape = shapes.get(shape_id) if shape_id else None
        if shape is None:
            skipped_no_shape += 1
            continue
        sts = raw_stop_times.get(trip_id, [])
        schedule = _build_schedule_for_trip(sts, shape, stops_by_id)
        if not schedule:
            skipped_no_schedule += 1
            continue
        trip_idx[trip_id] = TripIndex(shape_id=shape_id, schedule=schedule)
        if t.get("route_id"):
            trip_route[trip_id] = t["route_id"]

    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "gtfs_built shapes=%d trips=%d skipped_no_shape=%d skipped_no_schedule=%d elapsed_ms=%d",
        len(shapes),
        len(trip_idx),
        skipped_no_shape,
        skipped_no_schedule,
        elapsed_ms,
    )

    return GTFSStatic(
        feed_version=parsed.get("feed_version", ""),
        shapes=shapes,
        trip_idx=trip_idx,
        trip_route=trip_route,
    )


_cached_static: GTFSStatic | None = None
_cached_pointer: str | None = None
_s3_client = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def load_from_s3(bucket: str, current_key: str = "gtfs-static/current.txt") -> GTFSStatic:
    """Fetch the parsed pickle pointed at by `current_key`, build, cache.

    Caches by the pointer's value so we re-fetch only when the operator
    rotates the GTFS feed (which writes a new pointer).
    """
    global _cached_static, _cached_pointer
    s3 = _get_s3()
    pointer_obj = s3.get_object(Bucket=bucket, Key=current_key)
    pickle_key = pointer_obj["Body"].read().decode("utf-8").strip()
    if _cached_static is not None and _cached_pointer == pickle_key:
        return _cached_static
    parsed_obj = s3.get_object(Bucket=bucket, Key=pickle_key)
    parsed = pickle.loads(parsed_obj["Body"].read())
    static = build_static(parsed)
    _cached_static = static
    _cached_pointer = pickle_key
    logger.info("gtfs_loaded pointer=%s feed_version=%s", pickle_key, static.feed_version)
    return static
