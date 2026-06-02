"""Backfill route_window_features from raw GTFS-RT events in S3.

One-time local script. See docs/superpowers/specs/2026-06-02-feature-backfill-design.md.
"""

from __future__ import annotations

import json
from typing import Any, Iterator

_DECODER = json.JSONDecoder()


def iter_json_objects(raw: bytes) -> Iterator[dict[str, Any]]:
    """Yield each JSON object from a Firehose blob of *concatenated* JSON
    (no delimiter between objects). Stops at the first undecodable tail."""
    text = raw.decode("utf-8")
    i, n = 0, len(text)
    while i < n:
        # Skip inter-object whitespace/newlines.
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break
        try:
            obj, end = _DECODER.raw_decode(text, i)
        except json.JSONDecodeError:
            break
        yield obj
        i = end
