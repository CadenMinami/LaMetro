"""Data-sufficiency check — Phase 7b.

Reads the exact output-row count of the just-run Athena UNLOAD query via
GetQueryRuntimeStatistics (Rows.OutputRows) and decides whether the training
set is large enough to be worth training on. The Step Functions state machine
branches on the returned `sufficient` flag.

Why GetQueryRuntimeStatistics and not the UNLOAD manifest: Athena's UNLOAD
data manifest is a CSV that lists *output file paths only* — it carries no row
counts — and it lives at the query-results location, not the UNLOAD target.

Why Rows.InputRows and not OutputRows: for an UNLOAD, the statement returns no
rows *to the client* (it writes files), so Rows.OutputRows is ~1 — useless as a
size signal. Rows.InputRows is the number of feature-store rows the query read
in the 30-day window, which is exactly the "do we have enough data to train"
signal we want. Requires no extra query and costs nothing.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_THRESHOLD_ROWS = int(os.environ.get("DEFAULT_THRESHOLD_ROWS", "1000"))

_athena_client = None


def _athena():
    global _athena_client
    if _athena_client is None:
        _athena_client = boto3.client("athena")
    return _athena_client


def input_rows_from_stats(stats: dict[str, Any]) -> int:
    """Pull Rows.InputRows from a GetQueryRuntimeStatistics response, or 0 if
    the field is absent (treated as insufficient by the caller). InputRows is
    the rows the UNLOAD read from the feature store — the size signal we want
    (OutputRows is ~1 for UNLOAD; see module docstring)."""
    return int(
        stats.get("QueryRuntimeStatistics", {})
        .get("Rows", {})
        .get("InputRows", 0)
    )


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    query_execution_id = event["query_execution_id"]
    threshold = int(event.get("threshold_rows", DEFAULT_THRESHOLD_ROWS))

    stats = _athena().get_query_runtime_statistics(
        QueryExecutionId=query_execution_id,
    )
    rows = input_rows_from_stats(stats)

    result = {
        "sufficient": rows >= threshold,
        "row_count": rows,
        "threshold_rows": threshold,
    }
    logger.info(str(result))
    return result
