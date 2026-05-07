"""Schedule deviation algorithm.

Given a vehicle position and the static schedule for its trip, compute how
many seconds late (positive) or early (negative) the vehicle is. Returns
None when no defensible answer exists — off-route, unknown trip, before/
after the trip's window, or trip missing geometry/schedule.

Design choices:

  - Coordinates are projected to a local equirectangular metric system at LA's
    reference latitude. Within LA County the distortion is < 0.5%, which is
    well inside the 200m off-route tolerance and any meaningful delay budget.
    We avoid pyproj (~30 MB dep) for this — it's a standard simplification
    that interview-grade explanations can defend.
  - Shapely (with bundled GEOS) handles the actual point-on-line projection
    and perpendicular-distance check. ~5 MB packaged, ARM64-compatible
    wheels available, and the interface is the cleanest in the ecosystem.
  - Schedule progress is interpolated linearly between the two flanking
    scheduled stop times in distance space, then converted back to time.
    This matches what most transit on-time-performance tools do and is the
    spec's prescribed approach.
"""

from __future__ import annotations

import bisect
import math
from typing import Sequence

from shapely.geometry import LineString, Point

# LA is roughly 34°N. A single reference latitude is fine — every LA Metro
# vehicle is within ±0.5° of this.
_REF_LAT_DEG = 34.0
_M_PER_DEG_LAT = 111_320.0
_M_PER_DEG_LON = 111_320.0 * math.cos(math.radians(_REF_LAT_DEG))

OFF_ROUTE_THRESHOLD_M = 200.0
# Time slack on either side of the trip's first/last scheduled stop. A vehicle
# that's 5 min before its scheduled start is still meaningfully "on this trip".
TRIP_PRESTART_SLACK_S = 600
TRIP_POSTEND_SLACK_S = 600


def latlon_to_xy(lat: float, lon: float) -> tuple[float, float]:
    """Project (lat, lon) to local x/y meters via equirectangular @ ref lat."""
    return (lon * _M_PER_DEG_LON, lat * _M_PER_DEG_LAT)


def linestring_from_latlon(
    pts: Sequence[tuple[float, float]],
) -> LineString:
    """Build a Shapely LineString in projected meters from a sequence of
    (lat, lon) pairs. Raises ValueError on < 2 distinct points."""
    coords = [latlon_to_xy(lat, lon) for lat, lon in pts]
    # Filter consecutive duplicates — Shapely's LineString rejects them on
    # some operations.
    deduped: list[tuple[float, float]] = []
    for c in coords:
        if not deduped or deduped[-1] != c:
            deduped.append(c)
    if len(deduped) < 2:
        raise ValueError(f"need at least 2 distinct points, got {len(deduped)}")
    return LineString(deduped)


def interpolate_time_at_distance(
    schedule: Sequence[tuple[int, float]], target_dist_m: float
) -> int | None:
    """schedule = [(time_seconds_into_day, distance_along_route_m), ...]
    in increasing distance order. Returns the interpolated time at
    target_dist_m, or None if it falls outside the schedule range.
    """
    if not schedule:
        return None
    if target_dist_m < schedule[0][1] or target_dist_m > schedule[-1][1]:
        return None

    # Binary search the right segment by distance.
    distances = [d for _, d in schedule]
    idx = bisect.bisect_right(distances, target_dist_m)
    # bisect_right returns the index *after* the matching value; clamp to a
    # valid segment [idx-1, idx].
    hi = min(idx, len(schedule) - 1)
    lo = max(0, hi - 1)
    if lo == hi:
        return int(schedule[lo][0])
    t1, d1 = schedule[lo]
    t2, d2 = schedule[hi]
    if d2 == d1:
        return int(t1)
    frac = (target_dist_m - d1) / (d2 - d1)
    return int(t1 + frac * (t2 - t1))


def compute_delay_seconds(
    shape: LineString | None,
    schedule: Sequence[tuple[int, float]] | None,
    vehicle_lat: float,
    vehicle_lon: float,
    seconds_into_service_day: int,
) -> int | None:
    """Compute schedule deviation in seconds.

    Args:
        shape: pre-projected LineString (in equirectangular meters @ ref lat)
            for the vehicle's trip. None → return None.
        schedule: list of (time_seconds_into_service_day, distance_along_shape_m)
            for the trip's scheduled stops, sorted by distance. None/empty → None.
        vehicle_lat, vehicle_lon: WGS84 position of the vehicle.
        seconds_into_service_day: current time in the agency's service day.

    Returns:
        Positive int = late (seconds). 0 = on time. Negative = early.
        None = no defensible answer.
    """
    if shape is None or not schedule:
        return None
    if seconds_into_service_day < schedule[0][0] - TRIP_PRESTART_SLACK_S:
        return None
    if seconds_into_service_day > schedule[-1][0] + TRIP_POSTEND_SLACK_S:
        return None

    point = Point(*latlon_to_xy(vehicle_lat, vehicle_lon))

    # Off-route check: perpendicular distance to the route shape.
    if shape.distance(point) > OFF_ROUTE_THRESHOLD_M:
        return None

    # How far has the vehicle actually progressed along the route?
    actual_dist_m = shape.project(point)
    scheduled_time = interpolate_time_at_distance(schedule, actual_dist_m)
    if scheduled_time is None:
        return None

    return int(seconds_into_service_day - scheduled_time)
