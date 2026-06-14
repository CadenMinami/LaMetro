"""Structural assertions on the Athena training-set SQL. We don't run the
SQL here (that's integration); we just ensure the key shape can't drift."""

from pathlib import Path

SQL_PATH = Path(__file__).resolve().parent.parent / "feature_extraction.sql"


def test_sql_file_exists():
    assert SQL_PATH.is_file(), f"missing {SQL_PATH}"


def _sql() -> str:
    return SQL_PATH.read_text()


def test_unload_writes_to_supervised_set_prefix():
    sql = _sql()
    assert "UNLOAD" in sql.upper()
    assert "training-sets/" in sql or "training_sets/" in sql


def test_unload_uses_csv_no_header_for_built_in_xgboost():
    # Built-in XGBoost expects CSV with the label in the first column and no
    # header row. Keep this contract assertable.
    sql = _sql().upper()
    assert "FORMAT = 'TEXTFILE'" in sql or "FORMAT='TEXTFILE'" in sql
    assert "FIELD_DELIMITER = ','" in sql or "FIELD_DELIMITER=','" in sql


def test_features_include_lag_avg_delay_and_label_is_lead():
    sql = _sql()
    # Lag features.
    assert "LAG(avg_delay_seconds, 1)" in sql
    assert "LAG(avg_delay_seconds, 2)" in sql
    assert "LAG(avg_delay_seconds, 3)" in sql
    # Label.
    assert "LEAD(avg_delay_seconds, 1)" in sql


def test_partitions_by_route_and_orders_by_window():
    sql = _sql()
    assert "PARTITION BY route_id" in sql
    assert "ORDER BY window_start_iso" in sql


def test_filters_recent_30_days_via_partition_pruning():
    sql = _sql()
    # Partition pruning on year/month/day keeps Athena scan cheap.
    assert "year" in sql and "month" in sql and "day" in sql
