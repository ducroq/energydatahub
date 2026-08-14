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

# Loose date/timestamp detector. Matches ISO-8601 date-only keys
# ('2026-06-10') as well as date+time keys ('2026-06-10T13:00:00+02:00',
# '2026-06-10 13:00:00'). Keys that match this pattern collapse in the
# signature (we record their value-shape, not the timestamp itself) so the
# daily-rolling timestamps don't churn the fingerprint. Date-only support
# is required by `market_proxies.gas_ttf.history` and `market_history.*.data`
# which key rolling history windows by 'YYYY-MM-DD' — without the trailing
# `([T ].*|$)` branch, those would enumerate every day individually and
# the fingerprint would churn each time the window advanced.
_TS_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}([T ].*|$)')


def _is_timestamp_key(k: Any) -> bool:
    return isinstance(k, str) and bool(_TS_PATTERN.match(k))


def compute_shape_signature(data: Any, _depth: int = 0) -> Union[Dict[str, Any], str]:
    """
    Recursive shape descriptor for any JSON-serialisable value.

    For dicts:
      - If keys are ALL timestamp-like → returned as `{"_kind": "timestamp_map",
        "value_shape": <signature of one representative value>}`. This
        collapses the day-over-day timestamp churn so the fingerprint
        only changes when the per-record value shape changes.
      - Otherwise → returned as `{"_kind": "dict", "keys": {k: signature(v) for k, v in sorted}}`.

    For lists:
      - Empty → `{"_kind": "list", "value_shape": null}`
      - Non-empty → `{"_kind": "list", "value_shape": <signature of first element>}`

    For scalars: the type tag (`"bool"`, `"int"`, `"float"`, `"str"`, `"null"`).

    KNOWN DESIGN CONSTRAINT — mixed-key dicts lose churn-collapse:

      The timestamp-map collapse fires only when EVERY key matches the
      timestamp pattern. A dict mixing timestamp keys with one or two
      non-timestamp keys (e.g. `{'2026-01-01T...': {...}, 'metadata':
      {...}}`) falls through to the plain `_kind: "dict"` branch and
      records EVERY timestamp key individually. The hash then churns
      day-over-day for that feed even though the structure is stable.

      In practice this is fine because the published feeds use the
      canonical `{metadata, data}` envelope: `data` is a pure
      timestamp-keyed map (collapses), `metadata` is a plain key
      dict (stable). Any future feed that mixes shapes at the same
      nesting level will pay the daily-churn cost.

      Code-reviewer LOW finding on 7c0de64.

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


def _is_dict_signature(sig: Any) -> bool:
    """True for a plain-dict shape node (not a collapsed timestamp map)."""
    return isinstance(sig, dict) and sig.get("_kind") == "dict"


# Envelope keys that appear EXACTLY WHEN something went wrong, and are absent
# on a clean run. `BaseCollector.collect()` attaches
# `metadata['collector_quality_issues']` only when the run raised at least one
# issue, so its presence is a diagnostic signal, not a schema change.
#
# This matters directly for member drift. The same dropout that removes a
# location from `data` now also makes the collector raise a
# `location_completeness` issue — so a degraded run changes `metadata`'s shape
# too, the envelope guard rejects it, and the feed fails the gate for the very
# act of reporting its own degradation. Verified before this was added: the
# member-drift verdict on a realistic degraded buurt payload was None.
DIAGNOSTIC_ENVELOPE_KEYS = frozenset({"collector_quality_issues"})


def _without_diagnostic_keys(sig: Any) -> Any:
    """Strip `DIAGNOSTIC_ENVELOPE_KEYS` from a dict shape node, if present."""
    if not _is_dict_signature(sig):
        return sig
    keys = sig.get("keys")
    if not isinstance(keys, dict):
        return sig
    pruned = {k: v for k, v in keys.items() if k not in DIAGNOSTIC_ENVELOPE_KEYS}
    if len(pruned) == len(keys):
        return sig
    return {"_kind": "dict", "keys": pruned}


def classify_data_member_drift(
    previous_signature: Any,
    current_signature: Any,
    container_key: str = "data",
) -> Optional[Dict[str, List[str]]]:
    """Classify a shape diff as a pure MEMBER-SET change of the data block.

    Member-mapped feeds key their `data` block by a member name — a location
    (`weather_forecast_buurt`: Elsweide_Arnhem_NL / Elderveld_Arnhem_NL,
    `demand_weather_forecast`: eleven population centres). That key set is a
    DATA CATALOG, not schema: when a per-location fetch fails its retries the
    member simply drops out of the payload, and the feed publishes valid data
    for the members that did answer.

    NOT every dict-keyed `data` block is a member map. Several feeds key `data`
    by FIELD name — `grid_imbalance` is `{balance_delta, direction,
    imbalance_price}`, `market_history` is `{carbon_eua, gas_ttf}` — where a
    vanished key is a genuine unversioned schema break, precisely what the
    tripwire exists to catch. Telling the two apart is the whole difficulty,
    and this function alone does NOT do it: callers MUST also gate on a
    declared eligibility set (`MEMBER_MAPPED_FEEDS` in
    `scripts/detect_schema_drift.py`). The homogeneity rule below is defence in
    depth, not the gate — it rejects `grid_imbalance` on structure alone,
    because a `str`-valued map next to two `float`-valued maps is not a map of
    like things, but it cannot reject a field map whose fields happen to share
    a shape.

    The shape signature cannot tell that apart from a real structural break —
    both show up as "the `data` dict's keys changed" — so a single transient
    location dropout failed the drift tripwire and aborted the whole publish
    (2026-08-14: `Elsweide_Arnhem_NL` dropped from BOTH buurt feeds in one run;
    18 healthy feeds went unpublished as a result).

    This is the feed-level catalog-vs-shape split (see `scripts/
    detect_schema_drift.py`) applied one level deeper, and it is deliberately
    narrower than declaring the feed volatile: a volatile feed has its shape
    hash ignored outright, whereas this returns non-None ONLY when the members
    that SURVIVED are structurally identical. A member gaining or losing a
    field, or the envelope changing anywhere outside the data block, still
    reads as a real break and still fails.

    Returns None (→ "not a member-set change, enforce normally") unless ALL of:
      - both signatures are plain-dict envelopes with the same top-level keys;
      - every envelope key other than `container_key` is byte-identical
        (so `metadata` moving is a real break — note that a member dropout
        leaves metadata's SHAPE untouched, since `locations` is a list-of-str
        and `location_count` an int regardless of how many there are);
      - the container is a plain dict on both sides (a timestamp-keyed data
        block collapses to `_kind: timestamp_map` and never matches here);
      - at least one member is retained — a feed emptied completely never
        reaches here: the six Open-Meteo feeds in `PRESENT_EMPTY_GRACE_FEEDS`
        are coerced to absent upstream in `data_fetcher`, and every other feed
        fails `validate_completeness` before publish. Either way an empty
        member map must not be laundered into a warning;
      - ALL members on BOTH sides share one identical signature. This is the
        homogeneity rule: a member catalog is a map of like things, so every
        member — retained, dropped, or newly arrived — has the same shape. It
        subsumes "retained members unchanged" (a survivor that lost a field no
        longer matches its peers), and it validates ADDED members too, closing
        the case where a new key arrives carrying arbitrary structure.

    KNOWN LIMIT — an added key of matching shape is accepted unconditionally.
    A genuinely new, non-member `data` key whose shape happens to match the
    members (say a `_national_aggregate` series added to `solar_forecast`)
    reports as `members recovered: _national_aggregate` and passes. It is
    structurally indistinguishable from a member returning, so this is not
    fixable here — the eligibility registry and code review are what catch it.
    Do not read the narrowness claim above as covering additions: it bounds
    what can CHANGE or VANISH, not what can appear.

    Args:
        previous_signature: `shape_signature` of the feed at the baseline.
        current_signature: `shape_signature` of the feed this run.
        container_key: envelope key holding the member map (default 'data').

    Returns:
        `{"added": [...], "removed": [...], "retained": [...]}` when the diff
        is purely a member-set change, else None.
    """
    if not (_is_dict_signature(previous_signature)
            and _is_dict_signature(current_signature)):
        return None

    prev_keys = previous_signature.get("keys")
    curr_keys = current_signature.get("keys")
    if not isinstance(prev_keys, dict) or not isinstance(curr_keys, dict):
        return None
    if set(prev_keys) != set(curr_keys) or container_key not in prev_keys:
        return None

    # Everything outside the data block must be structurally identical, except
    # that a diagnostic key appearing or disappearing does not count — see
    # DIAGNOSTIC_ENVELOPE_KEYS.
    for key in prev_keys:
        if key == container_key:
            continue
        if _without_diagnostic_keys(prev_keys[key]) != \
                _without_diagnostic_keys(curr_keys[key]):
            return None

    prev_data = prev_keys[container_key]
    curr_data = curr_keys[container_key]
    if not (_is_dict_signature(prev_data) and _is_dict_signature(curr_data)):
        return None
    prev_members = prev_data.get("keys")
    curr_members = curr_data.get("keys")
    if not isinstance(prev_members, dict) or not isinstance(curr_members, dict):
        return None

    retained = set(prev_members) & set(curr_members)
    if not retained:
        return None

    # Magnitude floor: a MAJORITY of the previous members must survive.
    #
    # "At least one retained" is too weak to mean anything. On the real
    # sidecar, `demand_weather_forecast` losing 10 of its 11 locations
    # satisfied it and exited 0 — and nothing downstream catches that either,
    # since one surviving location still supplies ~168 records against an
    # EXPECTED_MIN_POINTS floor of 24, and `location_completeness` is a
    # 'warning', which the workflow quality gate does not fail on.
    #
    # It also ratchets: the baseline advances on the passing run, so the next
    # run compares degraded-to-degraded and reports no drift at all. The
    # directly analogous grace in this project (PRESENT_EMPTY_GRACE_FEEDS, #42)
    # is time-boxed by UPSTREAM_EMPTY_ESCALATION_RUNS for exactly this reason.
    # A majority floor is the cheap half of that guard — it bounds how much can
    # vanish in one step, not how long it may stay vanished. The duration half
    # is deliberately not built here; tracked as issue #47.
    if len(retained) * 2 < len(prev_members):
        return None

    # Homogeneity: every member on both sides must share one signature. A
    # survivor that changed shape no longer matches its peers, so this also
    # covers the "retained members unchanged" requirement, and it validates
    # members that only appear on the current side.
    all_member_sigs = list(prev_members.values()) + list(curr_members.values())
    reference = all_member_sigs[0]
    if any(sig != reference for sig in all_member_sigs):
        return None

    added = sorted(set(curr_members) - set(prev_members))
    removed = sorted(set(prev_members) - set(curr_members))
    if not added and not removed:
        return None

    return {"added": added, "removed": removed, "retained": sorted(retained)}


def signature_hash(signature: Union[Dict[str, Any], str]) -> str:
    """
    Stable SHA-256 hash of a shape signature.

    Used as the compact comparison key for CI drift detection. Two
    structurally identical signatures produce the same hash regardless
    of dict insertion order (we canonicalise via sort_keys).

    Truncated to 32 hex chars (128 bits). 64 bits would already be
    sufficient against a realistic second-preimage MITM attack on the
    tripwire (~2^64 SHA-256 evaluations ≈ centuries on commodity
    hardware), but 128 bits is zero-cost future-proofing — security
    audit on 7c0de64 recommended the extension.
    """
    canonical = json.dumps(signature, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


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


# ---------------------------------------------------------------------------
# Shape observations — the learning record (#43)
# ---------------------------------------------------------------------------
#
# `_shape_signatures.json` plays two roles that pull in opposite directions:
#
#   1. BASELINE — what the drift tripwire diffs against. It must advance only
#      on a run that PASSED, or a genuinely broken shape becomes the new normal
#      and the break goes silent. That is what the 2026-06-10 fail-mode flip
#      exists to prevent.
#   2. HISTORY — what `derive_volatile_feeds()` learns from. It must record
#      EVERY run, including failing ones, or the classifier can never learn
#      about the drift that tripped the gate.
#
# Before #43 the sidecar tried to be both, and role 1 won by accident: the
# tripwire runs before the commit step, so a failing run committed nothing and
# taught the classifier nothing. `ned_production` and `wind_forecast` failed on
# 2026-08-03 and were still unclassified five days later — the classifier could
# only ever learn from its own near-misses.
#
# Splitting the roles fixes it. Observations are append-only, written every run
# regardless of outcome, and carry only what classification needs (feed ->
# shape_hash, plus the schema_version they were seen at). They are NOT a
# baseline and must never be diffed against.

OBSERVATIONS_FILENAME = "_shape_observations.jsonl"

# ~1.3 KB per record, and the whole file is rewritten on every append, so each
# daily commit stores a fresh blob of the entire log. The cap therefore governs
# repo growth (issue #9), not just file size. Keep it a small multiple of the
# default 60-run classification window — records beyond the window are never
# read, so a larger cap is pure dead weight committed daily.
OBSERVATIONS_KEEP_LINES = 150


def observation_from_sidecar(sidecar: Dict[str, Any]) -> Dict[str, Any]:
    """Reduce a full sidecar to the compact record classification needs.

    Drops `shape_signature` (the full nested structure, kilobytes per feed) and
    keeps only the hash — volatility is "did this hash change at a fixed
    schema_version", so the structure itself is never consulted.
    """
    feeds = {}
    raw_feeds = sidecar.get("feeds")
    # Defensive: a malformed sidecar must degrade to an empty record, never
    # raise. This runs after collection but before the quality report, so an
    # exception here would discard a completed run's work.
    if isinstance(raw_feeds, dict):
        for feed, info in raw_feeds.items():
            if isinstance(info, dict) and info.get("shape_hash") is not None:
                feeds[feed] = info["shape_hash"]
    return {
        "observed_at": sidecar.get("computed_at"),
        "schema_version": sidecar.get("schema_version"),
        "feeds": feeds,
    }


def append_shape_observation(
    path: str,
    sidecar: Dict[str, Any],
    keep: int = OBSERVATIONS_KEEP_LINES,
) -> Dict[str, Any]:
    """Append one observation, trimming to the newest `keep` records.

    Never raises on a malformed existing file — a corrupt learning record must
    not take down a collection run. Returns the record written.
    """
    record = observation_from_sidecar(sidecar)
    lines: List[str] = []
    try:
        with open(path) as f:
            lines = [ln for ln in f.read().splitlines() if ln.strip()]
    except (OSError, UnicodeDecodeError):
        lines = []
    lines.append(json.dumps(record, sort_keys=True))
    with open(path, "w") as f:
        f.write("\n".join(lines[-keep:]) + "\n")
    return record


def load_shape_observations(path: str) -> List[Dict[str, Any]]:
    """Read the observation log, skipping unparseable lines."""
    out: List[Dict[str, Any]] = []
    try:
        with open(path) as f:
            raw = f.read().splitlines()
    except (OSError, UnicodeDecodeError):
        return out
    for ln in raw:
        if not ln.strip():
            continue
        try:
            rec = json.loads(ln)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict) and isinstance(rec.get("feeds"), dict):
            out.append(rec)
    return out


def volatile_feeds_from_observations(
    observations: List[Dict[str, Any]],
    window: int = 60,
    exclude_observed_at: Optional[str] = None,
) -> frozenset:
    """Feeds showing >1 shape_hash at the SAME schema_version in `window` runs.

    Same rule as the git-history derivation, applied to the append-only record
    so a run that FAILED the gate still counts. A versioned migration moves the
    schema_version, so its hashes land under different versions and are not
    mistaken for data-driven churn.

    `exclude_observed_at` MUST be set to the current run's `computed_at` by any
    caller that classifies the current run. This is not an optimisation — it is
    the difference between a gate and a rubber stamp. `data_fetcher` appends the
    current record before the tripwire reads the file, so without this filter a
    feed's FIRST EVER break supplies its own second hash, classifies itself as
    volatile, and is downgraded from ::error:: to ::warning::. Reproduced on the
    real log: a fabricated break in `load_forecast` (a CRITICAL_FEED, one hash
    across 75 runs) flipped the verdict to volatile and exit 0. Evidence for
    "this feed churns" has to be strictly PRIOR to the run being judged.
    """
    if exclude_observed_at is not None:
        observations = [
            r for r in observations if r.get("observed_at") != exclude_observed_at
        ]
    seen: Dict[str, Dict[Any, set]] = {}
    for rec in observations[-window:] if window else observations:
        version = rec.get("schema_version")
        for feed, h in (rec.get("feeds") or {}).items():
            if h is None:
                continue
            seen.setdefault(feed, {}).setdefault(version, set()).add(h)
    return frozenset(
        feed for feed, by_version in seen.items()
        if any(len(hashes) > 1 for hashes in by_version.values())
    )
