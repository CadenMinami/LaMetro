"""Unit tests for the data-sufficiency check Lambda (Phase 7b)."""

from __future__ import annotations

from unittest.mock import MagicMock

from lambdas.data_sufficiency_check import handler


def test_parse_unload_manifest_sums_rows_across_files():
    # Athena UNLOAD writes a manifest.csv listing each output object and its
    # row count via S3 Select / object metadata; for our purposes we just sum
    # the rows reported in the manifest.
    raw = b"path,rows\ns3://b/p/a.csv.gz,400\ns3://b/p/b.csv.gz,650\n"
    assert handler.row_count_from_manifest(raw) == 1050


def test_parse_unload_manifest_handles_no_rows_column_with_s3_list_fallback():
    # If the manifest doesn't carry row counts, fall back to a sentinel value
    # so the caller knows to do a direct count.
    raw = b"s3://b/p/a.csv.gz\ns3://b/p/b.csv.gz\n"
    assert handler.row_count_from_manifest(raw) is None


def test_lambda_handler_promotes_when_above_threshold(monkeypatch):
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: b"path,rows\ns3://b/p/a.csv.gz,1500\n"),
    }
    monkeypatch.setattr(handler, "_s3", lambda: s3)

    event = {
        "manifest_uri": "s3://bkt/training-sets/run=R/manifest.csv",
        "threshold_rows": 1000,
    }
    result = handler.lambda_handler(event, MagicMock())
    assert result == {"sufficient": True, "row_count": 1500, "threshold_rows": 1000}


def test_lambda_handler_skips_when_below_threshold(monkeypatch):
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: b"path,rows\ns3://b/p/a.csv.gz,500\n"),
    }
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    event = {
        "manifest_uri": "s3://bkt/training-sets/run=R/manifest.csv",
        "threshold_rows": 1000,
    }
    result = handler.lambda_handler(event, MagicMock())
    assert result["sufficient"] is False
    assert result["row_count"] == 500


def test_lambda_handler_uses_default_threshold_when_event_omits_it(monkeypatch):
    s3 = MagicMock()
    s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: b"path,rows\ns3://b/p/a.csv.gz,2000\n"),
    }
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    event = {"manifest_uri": "s3://bkt/training-sets/run=R/manifest.csv"}
    result = handler.lambda_handler(event, MagicMock())
    assert result["threshold_rows"] == handler.DEFAULT_THRESHOLD_ROWS
