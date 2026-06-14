"""Unit tests for the promote-model Lambda (Phase 7b)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from lambdas.promote_model import handler


def test_version_key_uses_iso_date_in_utc():
    when = datetime(2026, 6, 15, 7, 12, 0, tzinfo=timezone.utc)
    assert handler.version_key(when) == "v=2026-06-15"


def test_lambda_handler_copies_and_writes_metrics(monkeypatch):
    s3 = MagicMock()
    monkeypatch.setattr(handler, "_s3", lambda: s3)
    monkeypatch.setattr(
        handler, "_utcnow",
        lambda: datetime(2026, 6, 15, 7, 12, 0, tzinfo=timezone.utc),
    )

    event = {
        "candidate_model_uri": "s3://bkt/training-jobs/run=R/output/model.tar.gz",
        "models_prefix_uri": "s3://bkt/models",
        "candidate_metric": 87.4,
        "metric_name": "validation:rmse",
    }
    out = handler.lambda_handler(event, MagicMock())

    assert out["promoted_version"] == "v=2026-06-15"
    assert out["promoted_model_uri"].endswith("v=2026-06-15/model.tar.gz")
    assert out["current_model_uri"].endswith("current/model.tar.gz")

    # Three S3 calls: copy to versioned, copy to current, write metrics.json.
    copies = [c for c in s3.copy_object.call_args_list]
    puts = [c for c in s3.put_object.call_args_list]
    assert len(copies) == 2
    assert len(puts) == 1

    # Versioned copy target.
    v_call = copies[0].kwargs
    assert v_call["Bucket"] == "bkt"
    assert v_call["Key"] == "models/v=2026-06-15/model.tar.gz"
    assert v_call["CopySource"]["Bucket"] == "bkt"

    # current/ copy target.
    c_call = copies[1].kwargs
    assert c_call["Key"] == "models/current/model.tar.gz"

    # metrics.json content.
    metrics_call = puts[0].kwargs
    assert metrics_call["Key"] == "models/current/metrics.json"
    body = json.loads(metrics_call["Body"])
    assert body == {
        "validation_metric": 87.4,
        "metric_name": "validation:rmse",
        "promoted_version": "v=2026-06-15",
        "promoted_at": "2026-06-15T07:12:00Z",
    }
