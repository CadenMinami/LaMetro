"""Load a parsed GTFS-static pickle from S3 and turn it into in-memory
structures the deviation algorithm can use.

The slim pickle layout (post-Phase 4c) is what `scripts/load-gtfs-static.py`
emits — pre-computed, tuple-based, no per-row dicts:

    {
      "feed_version": str,
      "trips":     {trip_id: (route_id, shape_id)},
      "schedules": {trip_id: ((time_s, dist_m), ...)},  # already algorithm-ready
      "shapes":    {shape_id: ((lat, lon), ...)},
    }

The Lambda only needs to wrap each shape's lat/lon tuple in a Shapely
LineString (in projected meters) and copy the `trips`/`schedules` dicts
into the dataclass. Sharing one LineString per shape_id across the trips
that reference it (~700 unique shapes feed ~38k trips) is the big memory
win. The legacy fat-dict layout is no longer accepted — the loader was
updated in lockstep.
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
    pts: tuple[tuple[float, ...], ...] | list[tuple[float, ...]],
) -> LineString | None:
    """pts is a sequence of (lat, lon) tuples sorted by sequence. Tolerates
    legacy (lat, lon, dist) triples from older pickles by ignoring the third
    field — keeps a Lambda with stale cache from crashing during rollover."""
    coords: list[tuple[float, float]] = []
    for pt in pts:
        lat, lon = pt[0], pt[1]
        xy = latlon_to_xy(lat, lon)
        if not coords or coords[-1] != xy:
            coords.append(xy)
    if len(coords) < 2:
        return None
    return LineString(coords)


def build_static(parsed: dict[str, Any]) -> GTFSStatic:
    """Convert the slim parsed GTFS dict into algorithm-ready in-memory
    structures. Wraps each shape's lat/lon tuple in a Shapely LineString and
    copies the pre-computed schedules straight through."""
    started = time.monotonic()

    raw_shapes = parsed.get("shapes", {})
    shapes: dict[str, LineString] = {}
    for shape_id, pts in raw_shapes.items():
        ls = _build_linestring(pts)
        if ls is not None:
            shapes[shape_id] = ls

    raw_trips = parsed.get("trips", {})
    raw_schedules = parsed.get("schedules", {})

    trip_idx: dict[str, TripIndex] = {}
    trip_route: dict[str, str] = {}
    skipped_no_shape = 0
    skipped_no_schedule = 0

    for trip_id, meta in raw_trips.items():
        # `meta` is (route_id, shape_id) in the slim format.
        if isinstance(meta, dict):
            # Legacy fat-dict format — still tolerate it for older pickles
            # that haven't been re-uploaded yet. Will be removed once
            # confident.
            route_id = meta.get("route_id", "")
            shape_id = meta.get("shape_id", "")
        else:
            route_id, shape_id = meta
        shape = shapes.get(shape_id) if shape_id else None
        if shape is None:
            skipped_no_shape += 1
            continue
        schedule = raw_schedules.get(trip_id)
        if not schedule:
            skipped_no_schedule += 1
            continue
        # The pickle's schedule is already a tuple of (int_time, float_dist).
        trip_idx[trip_id] = TripIndex(shape_id=shape_id, schedule=tuple(schedule))
        if route_id:
            trip_route[trip_id] = route_id

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
