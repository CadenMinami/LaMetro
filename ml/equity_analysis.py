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


# ===========================================================================
# I/O layer (Athena / GTFS-S3 / ArcGIS / file+S3 output). Exercised by main();
# not unit-tested (external dependencies).
# ===========================================================================

import os  # noqa: E402

ATHENA_DATABASE = "la_metro"
ATHENA_TABLE = "route_window_features"
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

# ArcGIS Living Atlas income layer — VERIFY against the real layer at run time
# (field names + the county filter vary between layers). Overridable via env.
ARCGIS_URL = os.environ.get("ARCGIS_URL", "https://www.arcgis.com")
# A feature-layer URL (preferred) OR an item id, pointing at ACS median
# household income by census tract.
INCOME_LAYER_URL = os.environ.get("EQUITY_INCOME_LAYER_URL", "")
INCOME_ITEM_ID = os.environ.get("EQUITY_INCOME_ITEM_ID", "")
INCOME_FIELD = os.environ.get("EQUITY_INCOME_FIELD", "B19013_001E")
GEOID_FIELD = os.environ.get("EQUITY_GEOID_FIELD", "GEOID")
# LA County = state FIPS 06, county FIPS 037.
INCOME_WHERE = os.environ.get("EQUITY_INCOME_WHERE", "STATE = '06' AND COUNTY = '037'")


def run_athena_query(athena, sql: str, output_location: str) -> list[dict[str, Any]]:
    """Run a query to completion and return result rows as dicts (all string
    values; cast downstream). Raises on FAILED/CANCELLED."""
    import time

    start_kwargs: dict[str, Any] = {
        "QueryString": sql,
        "WorkGroup": ATHENA_WORKGROUP,
        "ResultConfiguration": {"OutputLocation": output_location},
    }
    try:
        qid = athena.start_query_execution(**start_kwargs)["QueryExecutionId"]
    except athena.exceptions.ClientError as exc:  # workgroup may enforce output
        if "ResultConfiguration" in str(exc) or "WorkGroup" in str(exc):
            start_kwargs.pop("ResultConfiguration")
            qid = athena.start_query_execution(**start_kwargs)["QueryExecutionId"]
        else:
            raise

    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if state != "SUCCEEDED":
        reason = info["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {state}: {reason}")

    rows: list[dict[str, Any]] = []
    header: list[str] | None = None
    paginator = athena.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            values = [c.get("VarCharValue") for c in r["Data"]]
            if header is None:
                header = values
                continue
            rows.append(dict(zip(header, values)))
    return rows


def reliability_sql(start_yyyymmdd: int, end_yyyymmdd: int) -> str:
    """Per-route-per-day vehicle-count-weighted on-time % + avg delay over the
    window. Day-level keeps the result small (~routes * days); aggregate_on_time
    does the final weighted rollup."""
    return f"""
SELECT route_id,
       SUM(on_time_pct * vehicle_count) / SUM(vehicle_count) AS on_time_pct,
       SUM(avg_delay_seconds * CAST(vehicle_count AS double)) / SUM(vehicle_count) AS avg_delay_seconds,
       SUM(vehicle_count) AS vehicle_count
FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
WHERE (year * 10000 + month * 100 + day) BETWEEN {start_yyyymmdd} AND {end_yyyymmdd}
  AND on_time_pct IS NOT NULL
  AND vehicle_count > 0
GROUP BY route_id, year, month, day
""".strip()


def load_gtfs_raw(s3, bucket: str, current_key: str = "gtfs-static/current.txt") -> dict[str, Any]:
    """Read the raw parsed GTFS pickle (NOT via gtfs_static.build_static, which
    reprojects shapes). Returns the dict with 'trips' and 'shapes'."""
    import pickle

    pointer = s3.get_object(Bucket=bucket, Key=current_key)["Body"].read().decode("utf-8").strip()
    parsed = pickle.loads(s3.get_object(Bucket=bucket, Key=pointer)["Body"].read())
    return parsed


def fetch_income_tracts(gis) -> "Any":
    """Query the ArcGIS Living Atlas income layer for LA County tracts and
    return a GeoDataFrame (WGS84) with columns median_income, geoid, geometry.

    NOTE: INCOME_LAYER_URL/ITEM_ID + INCOME_FIELD + INCOME_WHERE must match the
    actual layer — verify interactively on first run.
    """
    import json
    import geopandas as gpd
    from arcgis.features import FeatureLayer

    if INCOME_LAYER_URL:
        layer = FeatureLayer(INCOME_LAYER_URL, gis=gis)
    elif INCOME_ITEM_ID:
        layer = gis.content.get(INCOME_ITEM_ID).layers[0]
    else:
        raise RuntimeError(
            "Set EQUITY_INCOME_LAYER_URL or EQUITY_INCOME_ITEM_ID to the ACS "
            "median-household-income tract layer."
        )

    fset = layer.query(
        where=INCOME_WHERE,
        out_fields=f"{INCOME_FIELD},{GEOID_FIELD}",
        return_geometry=True,
        out_sr=4326,
    )
    features = json.loads(fset.to_geojson)["features"]
    gdf = gpd.GeoDataFrame.from_features(features, crs=WGS84)
    gdf = gdf.rename(columns={INCOME_FIELD: "median_income", GEOID_FIELD: "geoid"})
    return gdf[["median_income", "geoid", "geometry"]]


def build_finding_doc(finding: dict[str, Any], start: str, end: str) -> str:
    """Render the narrative finding as Markdown for docs/."""
    g = finding
    direction = "less" if g.get("gap_pct_points", 0) > 0 else "more"
    return f"""# Phase 8 — Transit Equity Finding

**Date:** generated by `ml/equity_analysis.py`
**Window:** {start} to {end} (~4 weeks of archived LA Metro vehicle data)
**Routes analyzed:** {g.get('n_routes', 'n/a')}

## Headline

LA Metro routes serving the **lowest-income** neighborhoods were on time
**{g.get('bottom_quartile_on_time_pct', 'n/a')}%** of the time, versus
**{g.get('top_quartile_on_time_pct', 'n/a')}%** for routes serving the
**highest-income** neighborhoods — a gap of
**{abs(g.get('gap_pct_points', 0))} percentage points** ({direction} reliable
for lower-income areas).

- Bottom income quartile mean served income: ${g.get('bottom_quartile_income', 'n/a'):,}
- Top income quartile mean served income: ${g.get('top_quartile_income', 'n/a'):,}
- Pearson r (served income vs on-time %): **{g.get('pearson_r', 'n/a')}** (p = {g.get('pearson_p', 'n/a'):.4g})
- Spearman rho: **{g.get('spearman_rho', 'n/a')}** (p = {g.get('spearman_p', 'n/a'):.4g})

## Method (see the approved plan for detail)

1. On-time % per route from {g.get('n_routes', '?')} routes over the window
   (Athena over `route_window_features`, vehicle-count-weighted; on-time =
   within +/- 60s of schedule; routes with < {MIN_ROUTE_VEHICLE_COUNT} observations dropped).
2. Each route mapped to its longest GTFS shape; intersected with census tracts
   (projected to EPSG:26911 for accurate length).
3. "Served income" = length-weighted mean median household income (ACS, via
   ArcGIS Living Atlas) of the tracts each route traverses.
4. Routes bucketed into income quartiles; quartile gap + correlations computed.

## Caveats

- ~4-week window (not a full year); a snapshot, not a seasonal average.
- Representative-shape choice (longest pattern) approximates multi-variant routes.
- Income is ACS tract-level median household income; tract != exact rider income.
"""


def write_outputs(
    *,
    tracts_fc: dict[str, Any],
    routes_fc: dict[str, Any],
    finding: dict[str, Any],
    finding_doc: str,
    public_dir: str,
    docs_path: str,
    s3=None,
    archive_bucket: str = "",
) -> None:
    """Write the two bundled GeoJSONs + finding.json to the frontend, the
    narrative doc to docs/, and a combined GeoJSON archive to S3."""
    os.makedirs(public_dir, exist_ok=True)
    with open(os.path.join(public_dir, "equity_tracts.geojson"), "w") as f:
        json.dump(tracts_fc, f)
    with open(os.path.join(public_dir, "equity_routes.geojson"), "w") as f:
        json.dump(routes_fc, f)
    with open(os.path.join(public_dir, "equity_finding.json"), "w") as f:
        json.dump(finding, f, indent=2)
    with open(docs_path, "w") as f:
        f.write(finding_doc)

    if s3 and archive_bucket:
        combined = {
            "type": "FeatureCollection",
            "features": [
                {**ft, "properties": {**ft["properties"], "kind": "tract"}}
                for ft in tracts_fc["features"]
            ]
            + [
                {**ft, "properties": {**ft["properties"], "kind": "route"}}
                for ft in routes_fc["features"]
            ],
        }
        s3.put_object(
            Bucket=archive_bucket,
            Key="equity-analysis/equity.geojson",
            Body=json.dumps(combined).encode("utf-8"),
            ContentType="application/geo+json",
        )


def _attach_quartiles(df: "Any", value_col: str, out_col: str, n: int = 4) -> "Any":
    import pandas as pd

    df = df.copy()
    df[out_col] = pd.qcut(df[value_col], n, labels=False, duplicates="drop")
    return df


def main(argv: list[str] | None = None) -> int:
    import argparse

    import boto3
    import pandas as pd

    parser = argparse.ArgumentParser(description="Phase 8 transit equity analysis")
    parser.add_argument("--bucket", required=True, help="archive S3 bucket name")
    parser.add_argument("--start", default="20260507", help="window start YYYYMMDD")
    parser.add_argument("--end", default="20260603", help="window end YYYYMMDD")
    parser.add_argument(
        "--public-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "frontend", "public", "geojson"),
    )
    parser.add_argument(
        "--docs-path",
        default=os.path.join(os.path.dirname(__file__), "..", "docs", "PHASE_8_EQUITY_FINDING.md"),
    )
    args = parser.parse_args(argv)

    from arcgis.gis import GIS

    username = os.environ.get("ARCGIS_USERNAME")
    password = os.environ.get("ARCGIS_PASSWORD")
    if not (username and password):
        raise RuntimeError("Set ARCGIS_USERNAME and ARCGIS_PASSWORD env vars.")

    athena = boto3.client("athena")
    s3 = boto3.client("s3")

    print("[1/6] Athena: per-route on-time %% ...")
    output_loc = f"s3://{args.bucket}/athena-results/"
    rows = run_athena_query(athena, reliability_sql(int(args.start), int(args.end)), output_loc)
    reliability = aggregate_on_time(rows)
    print(f"      {len(reliability)} routes after min-count filter")

    print("[2/6] GTFS: route shapes ...")
    parsed = load_gtfs_raw(s3, args.bucket)
    routes_gdf = build_route_lines(parsed["trips"], parsed["shapes"])
    print(f"      {len(routes_gdf)} route geometries")

    print("[3/6] ArcGIS: census-tract income ...")
    gis = GIS(ARCGIS_URL, username, password)
    tracts_gdf = fetch_income_tracts(gis)
    print(f"      {len(tracts_gdf)} tracts")

    print("[4/6] Spatial join: served income ...")
    served = served_income(routes_gdf, tracts_gdf)

    # Athena and GTFS route_ids are the same GTFS-RT form (verified 108/108
    # exact overlap), so we join on route_id directly.
    merged = reliability.merge(served[["route_id", "served_income"]], on="route_id", how="inner")
    print(f"      {len(merged)} routes matched (reliability x served income)")

    print("[5/6] Finding ...")
    finding = equity_finding(merged[["route_id", "on_time_pct", "served_income"]])
    finding["window_start"] = args.start
    finding["window_end"] = args.end
    print("      ", {k: finding[k] for k in ("gap_pct_points", "pearson_r", "spearman_rho") if k in finding})

    print("[6/6] GeoJSON + outputs ...")
    # tract quartiles for the choropleth class breaks
    tracts_clean = _drop_invalid_income(tracts_gdf)
    tracts_clean = _attach_quartiles(tracts_clean, "median_income", "income_quartile")
    # attach reliability + income bucket to route geometries for the overlay
    route_attrs = merged[["route_id", "on_time_pct", "served_income"]].copy()
    route_attrs = _attach_quartiles(route_attrs, "served_income", "income_bucket")
    routes_out = routes_gdf.merge(route_attrs, on="route_id", how="inner")

    tracts_fc, routes_fc = to_geojson_dicts(tracts_clean, routes_out)
    write_outputs(
        tracts_fc=tracts_fc,
        routes_fc=routes_fc,
        finding=finding,
        finding_doc=build_finding_doc(finding, args.start, args.end),
        public_dir=os.path.abspath(args.public_dir),
        docs_path=os.path.abspath(args.docs_path),
        s3=s3,
        archive_bucket=args.bucket,
    )
    print(f"      tracts={len(tracts_fc['features'])} routes={len(routes_fc['features'])} -> done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
