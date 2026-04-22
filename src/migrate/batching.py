"""Shared batching helpers for UC + Hive orchestrators.

Kept as a plain Python module (no ``# Databricks notebook source`` header)
so notebooks in ``src/migrate/`` can import it. Databricks refuses to
import files flagged as notebooks from inside another notebook
(NotebookImportException).
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


# Heavy fields stripped from task-value payloads — Databricks Jobs caps each
# for_each input parameter at 3000 bytes, and a realistic CREATE TABLE DDL
# (expanded with tags / row filters / column masks / comments) can exceed
# that by itself. Workers that need ``create_statement`` re-hydrate the row
# via ``TrackingManager.get_row(object_type, object_name)``.
_STRIPPED_FIELDS = ("create_statement",)

# Upper bound for a single batch's JSON payload in bytes. Databricks Jobs
# rejects any for_each input > 3000 bytes with "Max length of a For each
# input parameter exceeds the limit of 3000 bytes". Stay under with a
# safety margin so we don't race UTF-8 expansion or post-encoding overhead.
MAX_BATCH_BYTES = 2500


def _strip_heavy_fields(objects: list[dict]) -> list[dict]:
    """Return object dicts with heavy fields removed for task-value publishing."""
    return [{k: v for k, v in o.items() if k not in _STRIPPED_FIELDS} for o in objects]


def build_batches(objects: list[dict], batch_size: int) -> list[str]:
    """Split object dicts into JSON-encoded batch strings bounded by both
    ``batch_size`` (count ceiling) AND ``MAX_BATCH_BYTES`` (size ceiling).

    A batch is closed as soon as either ceiling would be exceeded.
    Heavy fields (see ``_STRIPPED_FIELDS``) are removed first so typical
    objects fit comfortably. When a single object's own encoding exceeds
    ``MAX_BATCH_BYTES`` it is still emitted (alone) — dropping it
    silently would be worse than an operator-visible for_each failure
    with the warning pointing at the offending object.
    """
    minimized = _strip_heavy_fields(objects)
    batches: list[str] = []
    current: list[dict] = []
    # Running UTF-8 byte count of the in-progress batch JSON; starts at 2
    # for the enclosing "[]".
    current_bytes = 2

    def _flush() -> None:
        nonlocal current, current_bytes
        if current:
            batches.append(json.dumps(current, default=str))
            current = []
            current_bytes = 2

    for obj in minimized:
        obj_bytes = len(json.dumps(obj, default=str).encode("utf-8"))
        # One comma joins this obj to an existing non-empty batch.
        sep = 1 if current else 0

        if current and (
            len(current) >= batch_size
            or current_bytes + sep + obj_bytes > MAX_BATCH_BYTES
        ):
            _flush()
            sep = 0

        if obj_bytes + 2 > MAX_BATCH_BYTES:
            logger.warning(
                "Single object encoded to %d bytes (> %d cap); for_each "
                "will fail on this item. Trim heavy fields on: %s",
                obj_bytes + 2,
                MAX_BATCH_BYTES,
                obj.get("object_name", "?"),
            )

        current.append(obj)
        current_bytes += sep + obj_bytes

    _flush()
    return batches
