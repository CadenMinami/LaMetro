"""Unit tests for the data-sufficiency check Lambda (Phase 7b).

The gate reads Rows.InputRows of the UNLOAD query from Athena's
GetQueryRuntimeStatistics — the count of feature-store rows the query read.
(OutputRows is ~1 for UNLOAD since it writes files rather than returning rows.)
"""

from __future__ import annotations

from unittest.mock import MagicMock

from lambdas.data_sufficiency_check import handler


def test_input_rows_from_stats_reads_nested_value():
    stats = {"QueryRuntimeStatistics": {"Rows": {"InputRows": 1050}}}
    assert handler.input_rows_from_stats(stats) == 1050


def test_input_rows_from_stats_defaults_to_zero_when_absent():
    assert handler.input_rows_from_stats({}) == 0
    assert handler.input_rows_from_stats({"QueryRuntimeStatistics": {}}) == 0


def test_lambda_handler_sufficient_when_above_threshold(monkeypatch):
    athena = MagicMock()
    athena.get_query_runtime_statistics.return_value = {
        "QueryRuntimeStatistics": {"Rows": {"InputRows": 1500}},
    }
    monkeypatch.setattr(handler, "_athena", lambda: athena)

    event = {"query_execution_id": "qexec-1", "threshold_rows": 1000}
    result = handler.lambda_handler(event, MagicMock())
    assert result == {"sufficient": True, "row_count": 1500, "threshold_rows": 1000}
    athena.get_query_runtime_statistics.assert_called_once_with(QueryExecutionId="qexec-1")


def test_lambda_handler_insufficient_when_below_threshold(monkeypatch):
    athena = MagicMock()
    athena.get_query_runtime_statistics.return_value = {
        "QueryRuntimeStatistics": {"Rows": {"InputRows": 500}},
    }
    monkeypatch.setattr(handler, "_athena", lambda: athena)
    event = {"query_execution_id": "qexec-2", "threshold_rows": 1000}
    result = handler.lambda_handler(event, MagicMock())
    assert result["sufficient"] is False
    assert result["row_count"] == 500


def test_lambda_handler_uses_default_threshold_when_event_omits_it(monkeypatch):
    athena = MagicMock()
    athena.get_query_runtime_statistics.return_value = {
        "QueryRuntimeStatistics": {"Rows": {"InputRows": 2000}},
    }
    monkeypatch.setattr(handler, "_athena", lambda: athena)
    event = {"query_execution_id": "qexec-3"}
    result = handler.lambda_handler(event, MagicMock())
    assert result["threshold_rows"] == handler.DEFAULT_THRESHOLD_ROWS
    assert result["sufficient"] is True
