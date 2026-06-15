"""Unit tests for the Phase 8 equity analysis — pure functions only.

These import geopandas/scipy, which live in the dedicated ml/.venv-equity venv
(not the shared 3.11 test venv). importorskip makes a plain ``pytest ml/`` in
the shared venv skip this file cleanly.
"""

from __future__ import annotations

import pytest

pytest.importorskip("geopandas")
pytest.importorskip("scipy")

import geopandas as gpd  # noqa: E402
from shapely.geometry import LineString, box  # noqa: E402

from ml import equity_analysis as eq  # noqa: E402

M = eq.WORKING_CRS  # metric CRS; building test geoms here makes to_crs a no-op


def test_normalize_route_id():
    assert eq.normalize_route_id("30-13201") == "30"
    assert eq.normalize_route_id("720") == "720"
    assert eq.normalize_route_id("  720-1  ") == "720"


def test_aggregate_on_time_weights_by_vehicle_count():
    rows = [
        {"route_id": "30", "on_time_pct": 60, "avg_delay_seconds": 100, "vehicle_count": 200},
        {"route_id": "30", "on_time_pct": 90, "avg_delay_seconds": 200, "vehicle_count": 600},
    ]
    out = eq.aggregate_on_time(rows)
    assert len(out) == 1
    row = out.iloc[0]
    assert row["route_id"] == "30"
    # (60*200 + 90*600) / 800 = 82.5
    assert row["on_time_pct"] == pytest.approx(82.5)
    # (100*200 + 200*600) / 800 = 175
    assert row["avg_delay_seconds"] == pytest.approx(175.0)
    assert row["total_vehicle_count"] == 800


def test_aggregate_on_time_drops_below_min_count():
    rows = [
        {"route_id": "big", "on_time_pct": 80, "avg_delay_seconds": 50, "vehicle_count": 600},
        {"route_id": "small", "on_time_pct": 50, "avg_delay_seconds": 90, "vehicle_count": 100},
    ]
    out = eq.aggregate_on_time(rows)
    assert set(out["route_id"]) == {"big"}  # 'small' (100 < 500) dropped


def test_build_route_lines_picks_longest_shape():
    trips = {
        "t1": ("r1", "short"),
        "t2": ("r1", "long"),
    }
    shapes = {
        "short": [(34.00, -118.00), (34.001, -118.00)],          # ~110 m
        "long": [(34.00, -118.00), (34.05, -118.00)],            # ~5.5 km
    }
    out = eq.build_route_lines(trips, shapes)
    assert len(out) == 1
    assert out.iloc[0]["route_id"] == "r1"
    assert out.iloc[0]["shape_id"] == "long"


def test_served_income_length_weighted():
    # Two adjacent square tracts in metres; route is 70% in A, 30% in B.
    tracts = gpd.GeoDataFrame(
        {"median_income": [30000, 90000], "geometry": [box(0, 0, 1000, 1000), box(1000, 0, 2000, 1000)]},
        crs=M,
    )
    routes = gpd.GeoDataFrame(
        {"route_id": ["x"], "geometry": [LineString([(300, 500), (1300, 500)])]},  # 700m A, 300m B
        crs=M,
    )
    out = eq.served_income(routes, tracts)
    assert len(out) == 1
    # 0.7*30000 + 0.3*90000 = 48000
    assert out.iloc[0]["served_income"] == pytest.approx(48000.0)


def test_served_income_drops_short_slivers():
    tracts = gpd.GeoDataFrame(
        {"median_income": [30000, 90000], "geometry": [box(0, 0, 1000, 1000), box(1000, 0, 2000, 1000)]},
        crs=M,
    )
    # 500m in A, only 10m in B (< 50m sliver -> dropped) => served == A's income
    routes = gpd.GeoDataFrame(
        {"route_id": ["x"], "geometry": [LineString([(500, 500), (1010, 500)])]},
        crs=M,
    )
    out = eq.served_income(routes, tracts)
    assert out.iloc[0]["served_income"] == pytest.approx(30000.0)


def test_served_income_drops_null_income():
    tracts = gpd.GeoDataFrame(
        {"median_income": [30000, 0], "geometry": [box(0, 0, 1000, 1000), box(1000, 0, 2000, 1000)]},
        crs=M,
    )  # tract B income 0 (sentinel) -> dropped before join
    routes = gpd.GeoDataFrame(
        {"route_id": ["x"], "geometry": [LineString([(300, 500), (1300, 500)])]},
        crs=M,
    )
    out = eq.served_income(routes, tracts)
    assert out.iloc[0]["served_income"] == pytest.approx(30000.0)


def test_equity_finding_quartiles_and_correlation():
    import pandas as pd

    # Monotonic: higher income -> higher on-time%. 8 routes, 4 buckets.
    df = pd.DataFrame(
        {
            "route_id": [f"r{i}" for i in range(8)],
            "served_income": [20, 30, 40, 50, 60, 70, 80, 90],
            "on_time_pct": [60, 64, 68, 72, 76, 80, 84, 88],
        }
    )
    f = eq.equity_finding(df)
    assert f["n_routes"] == 8
    assert f["gap_pct_points"] > 0  # top quartile more reliable
    assert f["spearman_rho"] == pytest.approx(1.0)
    assert f["bottom_quartile_on_time_pct"] < f["top_quartile_on_time_pct"]


def test_equity_finding_handles_too_few_routes():
    import pandas as pd

    df = pd.DataFrame({"route_id": ["a", "b"], "served_income": [10, 20], "on_time_pct": [50, 60]})
    f = eq.equity_finding(df)
    assert f.get("error") == "not_enough_routes"


def test_to_geojson_reduces_precision_and_props():
    tracts = gpd.GeoDataFrame(
        {
            "median_income": [55000],
            "income_quartile": [2],
            "geoid": ["06037123456"],
            "geometry": [box(-118.123456789, 34.0123456789, -118.0, 34.1)],
        },
        crs=eq.WGS84,
    )
    routes = gpd.GeoDataFrame(
        {
            "route_id": ["30"],
            "on_time_pct": [72.4],
            "served_income": [48000.0],
            "income_bucket": [0],
            "geometry": [LineString([(-118.12, 34.01), (-118.05, 34.05)])],
        },
        crs=eq.WGS84,
    )
    tracts_fc, routes_fc = eq.to_geojson_dicts(tracts, routes)

    assert tracts_fc["type"] == "FeatureCollection"
    assert routes_fc["features"][0]["properties"]["route_id"] == "30"
    assert tracts_fc["features"][0]["properties"]["median_income"] == 55000

    # every coordinate rounded to <= COORD_PRECISION decimals
    def _max_decimals(obj):
        if isinstance(obj, float):
            s = repr(obj)
            return len(s.split(".")[1]) if "." in s else 0
        if isinstance(obj, list):
            return max((_max_decimals(x) for x in obj), default=0)
        return 0

    coords = tracts_fc["features"][0]["geometry"]["coordinates"]
    assert _max_decimals(coords) <= eq.COORD_PRECISION
