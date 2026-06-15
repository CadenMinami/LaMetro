"""Phase 8 — Transit equity analysis (one-time offline script).

Question: do LA Metro buses serving lower-income neighborhoods run less
reliably than buses serving wealthier ones?

Pipeline (see docs / the approved plan):
  1. Athena  -> per-route on-time % over the analysis window
  2. GTFS    -> one representative LineString per route (its longest shape)
  3. ArcGIS  -> census-tract polygons + median household income (LA County)
  4. geo join-> each route's length-weighted "served income"
  5. stats   -> quartile gap + Pearson/Spearman correlation
  6. output  -> two GeoJSON files (frontend) + finding.json + S3 archive + doc

This module is split into PURE functions (no I/O — unit-tested with synthetic
data) and I/O functions (Athena/ArcGIS/S3/file — exercised only by ``main``).
Run it inside the dedicated venv (ml/.venv-equity); see ml/requirements-equity.txt.
"""

from __future__ import annotations

import json
import math
from typing import Any

# ---------------------------------------------------------------------------
# Tunable constants (documented so the choices are defensible in review).
# ---------------------------------------------------------------------------

# A route seen fewer than this many times over the whole window has a noisy
# on-time %, so we drop it from the finding.
MIN_ROUTE_VEHICLE_COUNT = 500

# A route that only grazes a tract corner shouldn't count as "serving" it.
MIN_SEGMENT_LEN_M = 50.0

# WGS84 (lon/lat) is the lingua franca of GeoJSON / ArcGIS / GTFS.
WGS84 = "EPSG:4326"
# NAD83 / UTM zone 11N — planar metres for LA. Used for all length math.
# NOT Web Mercator (EPSG:3857), whose length error at lat 34 deg is ~1/cos(34)
# ~= 21%, which would systematically bias the length-weighting.
WORKING_CRS = "EPSG:26911"

# ACS "no data" income values seen in Census/Living Atlas layers.
INVALID_INCOME_SENTINELS = (-666666666, -666666666.0, 0, 0.0)

# Polygon/line simplification (in WORKING_CRS metres) + coordinate precision,
# to keep the bundled GeoJSON small without visible change at city zoom.
TRACT_SIMPLIFY_M = 25.0
ROUTE_SIMPLIFY_M = 15.0
COORD_PRECISION = 5  # ~1.1 m at the equator


# ---------------------------------------------------------------------------
# route_id normalization (risk: GTFS-static route_ids may differ from the
# GTFS-RT route_ids that flow through Athena). Verified/adjusted at run time.
# ---------------------------------------------------------------------------

def normalize_route_id(route_id: str) -> str:
    """Best-effort canonical key for joining GTFS-static routes to Athena rows.

    LA Metro's realtime route_ids look like ``"30-13201"`` (line-shapeset);
    static feeds sometimes use the bare line (``"30"``). We key on the part
    before the first hyphen so both spaces line up. If the live data shows the
    two already match, this is a harmless identity-ish transform.
    """
    return route_id.split("-", 1)[0].strip()


# ---------------------------------------------------------------------------
# 1. Reliability rollup (pure). Granularity-agnostic: works on per-window or
#    per-day rows because a vehicle-count-weighted mean composes.
# ---------------------------------------------------------------------------

def aggregate_on_time(rows: list[dict[str, Any]]) -> "Any":
    """Roll per-route rows up to one vehicle-count-weighted on-time % / avg
    delay per route, dropping routes below ``MIN_ROUTE_VEHICLE_COUNT``.

    Each row needs: ``route_id``, ``on_time_pct``, ``avg_delay_seconds``,
    ``vehicle_count``. Returns a DataFrame indexed by position with columns
    ``route_id, on_time_pct, avg_delay_seconds, total_vehicle_count``.
    """
    import pandas as pd

    if not rows:
        return pd.DataFrame(
            columns=["route_id", "on_time_pct", "avg_delay_seconds", "total_vehicle_count"]
        )

    df = pd.DataFrame(rows)
    for col in ("on_time_pct", "avg_delay_seconds", "vehicle_count"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["on_time_pct", "vehicle_count"])
    df = df[df["vehicle_count"] > 0]

    def _roll(g: "pd.DataFrame") -> "pd.Series":
        w = g["vehicle_count"]
        total = w.sum()
        otp = (g["on_time_pct"] * w).sum() / total
        # avg_delay may have NaNs in some windows; weight over the non-null ones.
        d = g.dropna(subset=["avg_delay_seconds"])
        delay = (
            (d["avg_delay_seconds"] * d["vehicle_count"]).sum() / d["vehicle_count"].sum()
            if not d.empty
            else float("nan")
        )
        return pd.Series(
            {"on_time_pct": otp, "avg_delay_seconds": delay, "total_vehicle_count": total}
        )

    out = df.groupby("route_id").apply(_roll, include_groups=False).reset_index()
    out = out[out["total_vehicle_count"] >= MIN_ROUTE_VEHICLE_COUNT]
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 2. Route geometry (pure): one LineString per route = its LONGEST shape.
# ---------------------------------------------------------------------------

def build_route_lines(
    trips: dict[str, tuple[str, str]],
    shapes: dict[str, list[tuple[float, float]]],
) -> "Any":
    """Build one representative WGS84 LineString per route_id.

    ``trips``: {trip_id: (route_id, shape_id)}. ``shapes``: {shape_id:
    [(lat, lon), ...]}. We pick each route's LONGEST shape (its canonical
    full-length pattern) rather than unioning all variants — a union would
    over-count tract coverage and yield a MultiLineString. Returns a
    GeoDataFrame (crs=WGS84) with columns ``route_id, shape_id, geometry``.
    """
    import geopandas as gpd
    from shapely.geometry import LineString

    # route_id -> set of shape_ids that serve it.
    route_shapes: dict[str, set[str]] = {}
    for _trip_id, (route_id, shape_id) in trips.items():
        if route_id and shape_id:
            route_shapes.setdefault(route_id, set()).add(shape_id)

    records: list[dict[str, Any]] = []
    for route_id, shape_ids in route_shapes.items():
        for shape_id in shape_ids:
            pts = shapes.get(shape_id)
            if not pts:
                continue
            # shapes are (lat, lon[, dist]); shapely wants (x=lon, y=lat).
            coords = [(float(p[1]), float(p[0])) for p in pts]
            # de-dup consecutive identical points; need >=2 distinct.
            deduped = [coords[0]] + [c for prev, c in zip(coords, coords[1:]) if c != prev]
            if len(deduped) < 2:
                continue
            records.append(
                {"route_id": route_id, "shape_id": shape_id, "geometry": LineString(deduped)}
            )

    if not records:
        return gpd.GeoDataFrame(
            columns=["route_id", "shape_id", "geometry"], geometry="geometry", crs=WGS84
        )

    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs=WGS84)
    # Length in planar metres to choose the longest shape per route.
    gdf["_len_m"] = gdf.to_crs(WORKING_CRS).length
    idx = gdf.groupby("route_id")["_len_m"].idxmax()
    chosen = gdf.loc[idx, ["route_id", "shape_id", "geometry"]].reset_index(drop=True)
    return gpd.GeoDataFrame(chosen, geometry="geometry", crs=WGS84)


# ---------------------------------------------------------------------------
# 3. Spatial join (pure): each route's length-weighted served income.
# ---------------------------------------------------------------------------

def _drop_invalid_income(tracts_gdf: "Any") -> "Any":
    import pandas as pd

    t = tracts_gdf.copy()
    t["median_income"] = pd.to_numeric(t["median_income"], errors="coerce")
    t = t.dropna(subset=["median_income"])
    return t[~t["median_income"].isin(INVALID_INCOME_SENTINELS)]


def served_income(routes_gdf: "Any", tracts_gdf: "Any") -> "Any":
    """Length-weighted median income of the tracts each route passes through.

    ``routes_gdf``: route_id + line geometry (WGS84). ``tracts_gdf``:
    median_income + polygon geometry (WGS84). Returns a DataFrame with
    ``route_id, served_income``. Routes with no valid overlapping segment are
    dropped.
    """
    import numpy as np
    import geopandas as gpd

    tracts = _drop_invalid_income(tracts_gdf)
    routes_m = routes_gdf.to_crs(WORKING_CRS)
    tracts_m = tracts[["median_income", "geometry"]].to_crs(WORKING_CRS)

    parts = gpd.overlay(
        routes_m[["route_id", "geometry"]], tracts_m, how="intersection", keep_geom_type=True
    )
    if parts.empty:
        import pandas as pd

        return pd.DataFrame(columns=["route_id", "served_income"])

    parts["seg_len_m"] = parts.geometry.length
    parts = parts[parts["seg_len_m"] >= MIN_SEGMENT_LEN_M]

    def _wavg(g: "Any") -> float:
        return float(np.average(g["median_income"], weights=g["seg_len_m"]))

    out = parts.groupby("route_id").apply(_wavg, include_groups=False).reset_index()
    out.columns = ["route_id", "served_income"]
    return out


# ---------------------------------------------------------------------------
# 4. The finding (pure): quartile gap + correlations.
# ---------------------------------------------------------------------------

def equity_finding(joined_df: "Any", n_buckets: int = 4) -> dict[str, Any]:
    """Compute the headline equity finding from a per-route table with columns
    ``route_id, on_time_pct, served_income``.

    Returns a dict with the bottom/top income-bucket mean on-time %, the gap in
    percentage points, Pearson r and Spearman rho (each with p-value), and n.
    """
    import numpy as np
    import pandas as pd
    from scipy import stats

    df = joined_df.dropna(subset=["on_time_pct", "served_income"]).copy()
    n = len(df)
    result: dict[str, Any] = {"n_routes": int(n), "n_buckets": int(n_buckets)}
    if n < n_buckets:
        result["error"] = "not_enough_routes"
        return result

    # Income buckets (Q1 = lowest income). duplicates='drop' guards against
    # tied edges producing fewer bins.
    df["income_bucket"] = pd.qcut(df["served_income"], n_buckets, labels=False, duplicates="drop")
    lo = int(df["income_bucket"].min())
    hi = int(df["income_bucket"].max())
    bottom = df[df["income_bucket"] == lo]
    top = df[df["income_bucket"] == hi]

    bottom_otp = float(bottom["on_time_pct"].mean())
    top_otp = float(top["on_time_pct"].mean())

    pear = stats.pearsonr(df["served_income"], df["on_time_pct"])
    spear = stats.spearmanr(df["served_income"], df["on_time_pct"])

    result.update(
        {
            "bottom_quartile_on_time_pct": round(bottom_otp, 1),
            "top_quartile_on_time_pct": round(top_otp, 1),
            "gap_pct_points": round(top_otp - bottom_otp, 1),
            "bottom_quartile_income": round(float(bottom["served_income"].mean()), 0),
            "top_quartile_income": round(float(top["served_income"].mean()), 0),
            "pearson_r": round(float(pear[0]), 3),
            "pearson_p": float(pear[1]),
            "spearman_rho": round(float(spear[0]), 3),
            "spearman_p": float(spear[1]),
        }
    )
    return result


# ---------------------------------------------------------------------------
# 5. GeoJSON assembly (pure): simplified, precision-reduced FeatureCollections.
# ---------------------------------------------------------------------------

def _round_coords(obj: Any, ndigits: int = COORD_PRECISION) -> Any:
    """Recursively round all numbers in a GeoJSON coordinate structure."""
    if isinstance(obj, float):
        return round(obj, ndigits)
    if isinstance(obj, int):
        return obj
    if isinstance(obj, list):
        return [_round_coords(x, ndigits) for x in obj]
    if isinstance(obj, tuple):
        return [_round_coords(x, ndigits) for x in obj]
    return obj


def _feature(geom_mapping: dict[str, Any], props: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": {
            "type": geom_mapping["type"],
            "coordinates": _round_coords(geom_mapping["coordinates"]),
        },
        "properties": props,
    }


def to_geojson_dicts(
    tracts_gdf: "Any", routes_gdf: "Any"
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build (tracts_fc, routes_fc) GeoJSON FeatureCollections.

    ``tracts_gdf`` needs ``median_income`` (and optionally ``income_quartile``,
    ``geoid``); ``routes_gdf`` needs ``route_id, on_time_pct, served_income``
    (and optionally ``income_bucket``). Geometry is simplified in metres and
    coordinates are rounded to keep the bundled file small.
    """
    import pandas as pd

    # --- tracts ---
    t = tracts_gdf.copy()
    t_m = t.to_crs(WORKING_CRS)
    t_m["geometry"] = t_m.geometry.simplify(TRACT_SIMPLIFY_M, preserve_topology=True)
    t = t_m.to_crs(WGS84)
    tract_features = []
    for _, row in t.iterrows():
        if row.geometry is None or row.geometry.is_empty:
            continue
        props = {"median_income": _num(row.get("median_income"))}
        if "income_quartile" in t.columns:
            props["income_quartile"] = _num(row.get("income_quartile"))
        if "geoid" in t.columns:
            props["geoid"] = None if pd.isna(row.get("geoid")) else str(row.get("geoid"))
        tract_features.append(_feature(row.geometry.__geo_interface__, props))

    # --- routes ---
    r = routes_gdf.copy()
    r_m = r.to_crs(WORKING_CRS)
    r_m["geometry"] = r_m.geometry.simplify(ROUTE_SIMPLIFY_M, preserve_topology=True)
    r = r_m.to_crs(WGS84)
    route_features = []
    for _, row in r.iterrows():
        if row.geometry is None or row.geometry.is_empty:
            continue
        props = {
            "route_id": str(row.get("route_id")),
            "on_time_pct": _num(row.get("on_time_pct")),
            "served_income": _num(row.get("served_income")),
        }
        if "income_bucket" in r.columns:
            props["income_bucket"] = _num(row.get("income_bucket"))
        route_features.append(_feature(row.geometry.__geo_interface__, props))

    return (
        {"type": "FeatureCollection", "features": tract_features},
        {"type": "FeatureCollection", "features": route_features},
    )


def _num(v: Any) -> Any:
    """JSON-safe number: None for NaN/None, int when whole, else rounded float."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f):
        return None
    if f == int(f):
        return int(f)
    return round(f, 3)
