"""Load a parsed GTFS-static pickle from S3 and turn it into in-memory
structures the deviation and arrivals features can use.

The pickle layout (schema_version=2) is what `scripts/load-gtfs-static.py`
emits. Two slices share one artifact:

    Phase 4c (deviation):
      "trips":     {trip_id: (route_id, shape_id)}
      "schedules": {trip_id: ((time_s, dist_m), ...)}
      "shapes":    {shape_id: ((lat, lon), ...)}

    Phase 4d (arrivals):
      "stops":              {stop_id: {"name", "lat", "lon", "code", "parent_station"}}
      "stop_arrivals":      {stop_id: ((trip_id, route_id, arr_s, stop_sequence), ...)}
      "service_calendar":   {service_id: {"monday": bool, ..., "start_date", "end_date"}}
      "service_exceptions": {(YYYYMMDD, service_id): 1|2}
      "trip_service":       {trip_id: service_id}

Older v1 pickles (no v2 fields) still load cleanly — every v2 attribute
defaults to an empty dict, so callers that read it get "no arrivals
available" rather than a KeyError. Lambdas that don't need shape geometry
(query API) can pass `shapes=False` to skip the LineString build entirely
— shaves ~3-5s off cold start and lets the Lambda skip the shapely import.
"""

from __future__ import annotations

import datetime as dt
import logging
import pickle
import time
from dataclasses import dataclass, field
from typing import Any

import boto3

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TripIndex:
    """Algorithm-ready representation of a single trip."""
    shape_id: str
    # Sorted by distance, increasing. Each element: (time_s_into_day, dist_m).
    schedule: tuple[tuple[int, float], ...]


@dataclass(frozen=True, slots=True)
class ScheduledArrival:
    """One scheduled stop-time event surfaced by the arrivals API.

    `arr_s` is seconds since the *service-day* midnight (so values can exceed
    86400 — GTFS allows 25:30:00 for owl service that crosses midnight). The
    arrivals API translates this back to wall-clock time using the local
    timezone *and* the resolved service date.
    """
    trip_id: str
    route_id: str
    arr_s: int
    stop_sequence: int
    service_id: str
    # Calendar date this arrival corresponds to (YYYYMMDD). The same trip can
    # surface twice in a 24h window when service crosses midnight — once for
    # yesterday's late owl service, once for today's first run.
    service_date: str


# `LineString` is loaded lazily so the query Lambda (which doesn't need
# geometry) can avoid the shapely import entirely. We only annotate with
# `Any` here; deviation code that actually uses the shape narrows it.
_LineStringType = Any


@dataclass(slots=True)
class GTFSStatic:
    feed_version: str
    shapes: dict[str, _LineStringType]
    trip_idx: dict[str, TripIndex]
    # Carry the route_id so the enrichment Lambda can fall back to the static
    # one when GTFS-RT omits it (it sometimes does between trips).
    trip_route: dict[str, str]
    # Phase 4d additions. All default empty so v1 pickles load without error.
    stops: dict[str, dict[str, Any]] = field(default_factory=dict)
    stop_arrivals: dict[str, tuple[tuple[str, str, int, int], ...]] = field(default_factory=dict)
    service_calendar: dict[str, dict[str, Any]] = field(default_factory=dict)
    service_exceptions: dict[tuple[str, str], int] = field(default_factory=dict)
    trip_service: dict[str, str] = field(default_factory=dict)

    def shape_for_trip(self, trip_id: str) -> _LineStringType | None:
        ti = self.trip_idx.get(trip_id)
        if ti is None:
            return None
        return self.shapes.get(ti.shape_id)

    def schedule_for_trip(
        self, trip_id: str
    ) -> tuple[tuple[int, float], ...] | None:
        ti = self.trip_idx.get(trip_id)
        return None if ti is None else ti.schedule

    # ---------- Phase 4d: arrivals helpers -------------------------------

    def is_service_active(self, service_id: str, date: dt.date) -> bool:
        """Is `service_id` running on `date` (in agency-local time)?

        GTFS rule: weekday bitmask in calendar.txt + an overlay from
        calendar_dates.txt where exception_type=1 forces a service ON for that
        day and exception_type=2 forces it OFF (e.g. holiday schedules).

        When `service_calendar` is empty (older v1 pickle, or feed without
        calendar.txt) we treat *every* service as active — better than
        returning zero arrivals.
        """
        date_str = date.strftime("%Y%m%d")
        exc = self.service_exceptions.get((date_str, service_id))
        if exc == 1:
            return True
        if exc == 2:
            return False
        if not self.service_calendar:
            return True
        cal = self.service_calendar.get(service_id)
        if not cal:
            return False
        weekday_field = (
            "monday", "tuesday", "wednesday", "thursday",
            "friday", "saturday", "sunday",
        )[date.weekday()]
        if not cal.get(weekday_field):
            return False
        start = cal.get("start_date") or ""
        end = cal.get("end_date") or ""
        if start and date_str < start:
            return False
        if end and date_str > end:
            return False
        return True

    def arrivals_for_stop(
        self,
        stop_id: str,
        now_local: dt.datetime,
        horizon_seconds: int,
        lookback_seconds: int = 30,
    ) -> list[ScheduledArrival]:
        """Return scheduled arrivals at `stop_id` whose wall-clock time falls
        in `[now_local - lookback, now_local + horizon]`, filtered to active
        services.

        Two service-dates are considered to handle the midnight boundary:

          - *yesterday's* service-day (in case a 25:00:00 owl trip is still
            active right after midnight local time)
          - *today's* service-day

        For each candidate trip, the absolute wall-clock arrival time is
        reconstructed as `service_date_midnight + arr_s`, where service_date
        is whichever of today/yesterday produces a time inside the window.

        Returns a list, sorted by arr_s — let the caller decide what to do
        with stable sort vs. tie-breaks.
        """
        rows = self.stop_arrivals.get(stop_id)
        if not rows:
            return []

        out: list[ScheduledArrival] = []
        local_tz = now_local.tzinfo  # caller passes a tz-aware datetime
        today = now_local.date()
        yesterday = today - dt.timedelta(days=1)

        # For each candidate service-date, the window is anchored to that
        # date's midnight local time. arr_s is service-day seconds, so we add
        # arr_s to the date's midnight to get a wall-clock datetime.
        for service_date in (today, yesterday):
            midnight = dt.datetime.combine(service_date, dt.time.min, tzinfo=local_tz)
            for trip_id, route_id, arr_s, seq in rows:
                wall = midnight + dt.timedelta(seconds=arr_s)
                delta = (wall - now_local).total_seconds()
                if delta < -lookback_seconds or delta > horizon_seconds:
                    continue
                service_id = self.trip_service.get(trip_id, "")
                # Empty service_id = unknown calendar membership. Allow it
                # rather than dropping; better to render a "scheduled"
                # arrival than to silently swallow it.
                if service_id and not self.is_service_active(service_id, service_date):
                    continue
                out.append(
                    ScheduledArrival(
                        trip_id=trip_id,
                        route_id=route_id,
                        arr_s=arr_s,
                        stop_sequence=seq,
                        service_id=service_id,
                        service_date=service_date.strftime("%Y%m%d"),
                    )
                )

        out.sort(key=lambda a: (a.service_date, a.arr_s))
        return out

    def absolute_arrival_time(
        self, arr: ScheduledArrival, local_tz: dt.tzinfo
    ) -> dt.datetime:
        """Reconstruct the wall-clock arrival time for a `ScheduledArrival`."""
        sd = dt.datetime.strptime(arr.service_date, "%Y%m%d").date()
        midnight = dt.datetime.combine(sd, dt.time.min, tzinfo=local_tz)
        return midnight + dt.timedelta(seconds=arr.arr_s)


def _build_linestring(
    pts: tuple[tuple[float, ...], ...] | list[tuple[float, ...]],
) -> _LineStringType | None:
    """pts is a sequence of (lat, lon) tuples sorted by sequence. Tolerates
    legacy (lat, lon, dist) triples from older pickles by ignoring the third
    field — keeps a Lambda with stale cache from crashing during rollover.

    Imports shapely + the metric projection helper lazily so callers that
    don't need shape geometry (the arrivals API) never pay the import cost.
    """
    from shapely.geometry import LineString  # lazy
    from .deviation import latlon_to_xy

    coords: list[tuple[float, float]] = []
    for pt in pts:
        lat, lon = pt[0], pt[1]
        xy = latlon_to_xy(lat, lon)
        if not coords or coords[-1] != xy:
            coords.append(xy)
    if len(coords) < 2:
        return None
    return LineString(coords)


def build_static(parsed: dict[str, Any], shapes: bool = True) -> GTFSStatic:
    """Convert the parsed GTFS dict into algorithm-ready in-memory structures.

    `shapes=True` (default) wraps each shape's lat/lon tuple in a Shapely
    LineString — required by the deviation algorithm. Pass `shapes=False`
    when only Phase 4d arrivals data is needed; the `shapes` and `trip_idx`
    fields will be left empty, and shapely won't be imported at all (saves
    ~3-5 s of cold start in a fresh Lambda).
    """
    started = time.monotonic()

    raw_shapes = parsed.get("shapes", {}) if shapes else {}
    shape_objects: dict[str, _LineStringType] = {}
    if shapes:
        for shape_id, pts in raw_shapes.items():
            ls = _build_linestring(pts)
            if ls is not None:
                shape_objects[shape_id] = ls

    raw_trips = parsed.get("trips", {})
    raw_schedules = parsed.get("schedules", {})

    trip_idx: dict[str, TripIndex] = {}
    trip_route: dict[str, str] = {}
    skipped_no_shape = 0
    skipped_no_schedule = 0

    # Build trip_idx only when shapes are loaded — without them the deviation
    # algorithm can't run anyway, and the arrivals path doesn't read trip_idx.
    if shapes:
        for trip_id, meta in raw_trips.items():
            if isinstance(meta, dict):
                # Legacy fat-dict format — still tolerated for stale pickles.
                route_id = meta.get("route_id", "")
                shape_id = meta.get("shape_id", "")
            else:
                route_id, shape_id = meta
            shape = shape_objects.get(shape_id) if shape_id else None
            if shape is None:
                skipped_no_shape += 1
                continue
            schedule = raw_schedules.get(trip_id)
            if not schedule:
                skipped_no_schedule += 1
                continue
            trip_idx[trip_id] = TripIndex(shape_id=shape_id, schedule=tuple(schedule))
            if route_id:
                trip_route[trip_id] = route_id
    else:
        # Even without shapes, the enrichment fallback reads trip_route to
        # recover route_id when GTFS-RT omits it. Build the lightweight map.
        for trip_id, meta in raw_trips.items():
            if isinstance(meta, dict):
                route_id = meta.get("route_id", "")
            else:
                route_id = meta[0] if meta else ""
            if route_id:
                trip_route[trip_id] = route_id

    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "gtfs_built shapes=%d trips=%d skipped_no_shape=%d skipped_no_schedule=%d elapsed_ms=%d",
        len(shape_objects),
        len(trip_idx),
        skipped_no_shape,
        skipped_no_schedule,
        elapsed_ms,
    )

    return GTFSStatic(
        feed_version=parsed.get("feed_version", ""),
        shapes=shape_objects,
        trip_idx=trip_idx,
        trip_route=trip_route,
        # v2 fields. `getattr`-style defaults keep older v1 pickles loading.
        stops=parsed.get("stops") or {},
        stop_arrivals=parsed.get("stop_arrivals") or {},
        service_calendar=parsed.get("service_calendar") or {},
        service_exceptions=parsed.get("service_exceptions") or {},
        trip_service=parsed.get("trip_service") or {},
    )


_cached_static: GTFSStatic | None = None
_cached_pointer: str | None = None
_cached_shapes_flag: bool | None = None
_s3_client = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def load_from_s3(
    bucket: str,
    current_key: str = "gtfs-static/current.txt",
    shapes: bool = True,
) -> GTFSStatic:
    """Fetch the parsed pickle pointed at by `current_key`, build, cache.

    Caches by (pointer, shapes-flag) so we re-fetch only when the operator
    rotates the GTFS feed (which writes a new pointer) or a different caller
    asks for the other shape-loading mode. Per-Lambda there's only one mode
    in practice, so cache hits dominate.
    """
    global _cached_static, _cached_pointer, _cached_shapes_flag
    s3 = _get_s3()
    pointer_obj = s3.get_object(Bucket=bucket, Key=current_key)
    pickle_key = pointer_obj["Body"].read().decode("utf-8").strip()
    if (
        _cached_static is not None
        and _cached_pointer == pickle_key
        and _cached_shapes_flag == shapes
    ):
        return _cached_static
    parsed_obj = s3.get_object(Bucket=bucket, Key=pickle_key)
    parsed = pickle.loads(parsed_obj["Body"].read())
    static = build_static(parsed, shapes=shapes)
    _cached_static = static
    _cached_pointer = pickle_key
    _cached_shapes_flag = shapes
    logger.info(
        "gtfs_loaded pointer=%s feed_version=%s shapes=%s",
        pickle_key,
        static.feed_version,
        shapes,
    )
    return static
