"""
Shape Signature for Schema-Drift Detection
-------------------------------------------
Computes a deterministic shape fingerprint for published JSON feeds so a
diff between today's signatures and yesterday's (via git history) can flag
shape drift that wasn't accompanied by a `CURRENT_SCHEMA_VERSION` bump.

This is the structural complement to `utils/schema_registry.py`:
  - schema_registry says "this is the version" (author-declared).
  - shape_signature says "this is the actual shape we just published"
    (computed). CI compares the two over time so a quiet shape change
    can't ship without the version moving (issue #27, Layer A).

The signature is intentionally lossy at the leaf level: scalar VALUES
don't influence the signature, only their TYPES. We want stable
fingerprints day-over-day for unchanged shapes, even when the actual
prices, temperatures, etc. vary.

File: utils/shape_signature.py
Created: 2026-06-07
Author: Energy Data Hub Project
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

# Loose ISO-8601 timestamp detector. Keys that match this pattern collapse
# in the signature (we record their value-shape, not the timestamp itself)
# so the daily-rolling timestamps don't churn the fingerprint.
_TS_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T')


def _is_timestamp_key(k: Any) -> bool:
    return isinstance(k, str) and bool(_TS_PATTERN.match(k))


def compute_shape_signature(data: Any, _depth: int = 0) -> Union[Dict[str, Any], str]:
    """
    Recursive shape descriptor for any JSON-serialisable value.

    For dicts:
      - If keys are all timestamp-like → returned as `{"_kind": "timestamp_map",
        "value_shape": <signature of one representative value>}`. This
        collapses the day-over-day timestamp churn so the fingerprint
        only changes when the per-record value shape changes.
      - Otherwise → returned as `{"_kind": "dict", "keys": {k: signature(v) for k, v in sorted}}`.

    For lists:
      - Empty → `{"_kind": "list", "value_shape": null}`
      - Non-empty → `{"_kind": "list", "value_shape": <signature of first element>}`

    For scalars: the type tag (`"bool"`, `"int"`, `"float"`, `"str"`, `"null"`).

    Args:
        data: Any JSON-serialisable value
        _depth: Internal recursion guard (default 0, cap 50)

    Returns:
        Dict or scalar tag describing the shape.
    """
    if _depth > 50:
        return "max_depth_exceeded"

    if data is None:
        return "null"
    if isinstance(data, bool):
        return "bool"
    if isinstance(data, int):
        return "int"
    if isinstance(data, float):
        return "float"
    if isinstance(data, str):
        return "str"

    if isinstance(data, dict):
        if not data:
            return {"_kind": "dict", "keys": {}}
        # Timestamp-keyed maps: collapse to one representative value-shape.
        if all(_is_timestamp_key(k) for k in data.keys()):
            sample_value = next(iter(data.values()))
            return {
                "_kind": "timestamp_map",
                "value_shape": compute_shape_signature(sample_value, _depth + 1),
            }
        return {
            "_kind": "dict",
            "keys": {
                k: compute_shape_signature(data[k], _depth + 1)
                for k in sorted(data.keys(), key=str)
            },
        }

    if isinstance(data, list):
        if not data:
            return {"_kind": "list", "value_shape": None}
        return {
            "_kind": "list",
            "value_shape": compute_shape_signature(data[0], _depth + 1),
        }

    # Fallback for any non-JSON-serialisable value
    return f"unknown:{type(data).__name__}"


def signature_hash(signature: Union[Dict[str, Any], str]) -> str:
    """
    Stable SHA-256 hash of a shape signature.

    Used as the compact comparison key for CI drift detection. Two
    structurally identical signatures produce the same hash regardless
    of dict insertion order (we canonicalise via sort_keys).
    """
    canonical = json.dumps(signature, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def signatures_for_published_feeds(
    feed_payloads: Dict[str, Dict[str, Any]],
    schema_version: str,
) -> Dict[str, Any]:
    """
    Build the sidecar payload for `data/_shape_signatures.json`.

    Args:
        feed_payloads: Mapping of feed_name (e.g. 'gas_storage') → the
            unencrypted dict that will be published for that feed (i.e.
            the output of .to_dict() before save_data_file encrypts).
        schema_version: CURRENT_SCHEMA_VERSION at the time of capture.

    Returns:
        Dict suitable for json.dump:

        {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {
                "gas_storage.json": {
                    "shape_signature": {...},
                    "shape_hash": "abc123...",
                    "data_type": "gas_storage",   # extracted for quick CI heuristics
                    "sources": null,              # null for standalone, list for combined
                },
                ...
            }
        }
    """
    out: Dict[str, Any] = {
        "computed_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "schema_version": schema_version,
        "feeds": {},
    }
    for feed_name, payload in feed_payloads.items():
        if payload is None:
            continue
        signature = compute_shape_signature(payload)
        sig_hash = signature_hash(signature)
        meta = payload.get("metadata") if isinstance(payload, dict) else None
        data_type = (meta or {}).get("data_type") if isinstance(meta, dict) else None
        sources: Optional[List[str]] = None
        if isinstance(meta, dict) and meta.get("data_type") == "combined":
            inner = payload.get("data")
            if isinstance(inner, dict):
                sources = sorted(inner.keys())
        out["feeds"][feed_name] = {
            "shape_hash": sig_hash,
            "shape_signature": signature,
            "data_type": data_type,
            "sources": sources,
        }
    return out


def diff_signatures(
    previous: Dict[str, Any],
    current: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Compare two sidecar payloads and report per-feed drift.

    Args:
        previous: Previous commit's `_shape_signatures.json` content.
        current:  Current commit's `_shape_signatures.json` content.

    Returns:
        Dict describing drift:

        {
            "schema_version_changed": True/False,
            "previous_schema_version": "2.2",
            "current_schema_version": "2.3",
            "feeds_added": [...],            # new feeds in current
            "feeds_removed": [...],          # feeds gone from current
            "feeds_changed": [               # shape differs
                {
                    "feed": "gas_storage.json",
                    "previous_hash": "abc",
                    "current_hash": "def",
                    "previous_data_type": "gas_storage",
                    "current_data_type": "gas_storage",
                    "sources_diff": {...},   # only for combined feeds
                },
                ...
            ],
            "feeds_unchanged": [...],
        }
    """
    prev_feeds = previous.get("feeds", {}) if isinstance(previous, dict) else {}
    curr_feeds = current.get("feeds", {}) if isinstance(current, dict) else {}

    prev_names = set(prev_feeds.keys())
    curr_names = set(curr_feeds.keys())

    feeds_added = sorted(curr_names - prev_names)
    feeds_removed = sorted(prev_names - curr_names)
    feeds_changed: List[Dict[str, Any]] = []
    feeds_unchanged: List[str] = []

    for name in sorted(prev_names & curr_names):
        p = prev_feeds[name]
        c = curr_feeds[name]
        if p.get("shape_hash") == c.get("shape_hash"):
            feeds_unchanged.append(name)
            continue
        entry = {
            "feed": name,
            "previous_hash": p.get("shape_hash"),
            "current_hash": c.get("shape_hash"),
            "previous_data_type": p.get("data_type"),
            "current_data_type": c.get("data_type"),
        }
        prev_sources = p.get("sources")
        curr_sources = c.get("sources")
        if prev_sources or curr_sources:
            entry["sources_diff"] = {
                "added": sorted(set(curr_sources or []) - set(prev_sources or [])),
                "removed": sorted(set(prev_sources or []) - set(curr_sources or [])),
            }
        feeds_changed.append(entry)

    prev_ver = previous.get("schema_version") if isinstance(previous, dict) else None
    curr_ver = current.get("schema_version") if isinstance(current, dict) else None

    return {
        "schema_version_changed": prev_ver != curr_ver,
        "previous_schema_version": prev_ver,
        "current_schema_version": curr_ver,
        "feeds_added": feeds_added,
        "feeds_removed": feeds_removed,
        "feeds_changed": feeds_changed,
        "feeds_unchanged": feeds_unchanged,
    }
