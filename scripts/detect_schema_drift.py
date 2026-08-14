"""
Schema-Drift Detection (CI tripwire for issue #27 Layer A)
-----------------------------------------------------------
Compares the current `data/_shape_signatures.json` (computed by
`data_fetcher.py` after each run) against the previous git commit's
version of the same file. Surfaces any shape change that wasn't
accompanied by a `CURRENT_SCHEMA_VERSION` bump — the systemic guard
against the silent-shape-break failure mode behind PR #20 and #26.

Exit codes:
  0 — no drift detected, OR drift detected but schema_version was
      bumped (the change is properly versioned), OR --warn-only is set,
      OR only catalog drift (feeds added/removed) without a within-feed
      shape change — see "Catalog vs shape drift" below — OR the only
      within-feed drift was on volatile feeds, OR on declared member-mapped
      feeds whose member set changed but whose shape did not (see "Member
      drift" below).
  1 — within-feed shape drift AND schema_version did NOT change. The
      pipeline shipped a new shape without bumping the version — exactly
      the class of bug this tripwire exists to catch.
  2 — script setup error (missing files, git failure, etc.)

Catalog vs shape drift:
  schema_version captures the envelope/migration shape of existing
  feeds, NOT the feed catalog. A transiently-failing collector
  recovering (feed appears) or a collector being retired (feed
  disappears) is operational and never warrants a version bump. So
  fail-mode reserves exit 1 for `feeds_changed` — within-feed shape
  diffs with no version bump. `feeds_added` and `feeds_removed` always
  surface as warnings, never failures. Without this split, the
  tripwire would fire on every transient collector miss-and-recover.

Member drift (a location dropping out of one feed):
  A member-mapped feed keys its `data` block by location name. When one
  member's fetch exhausts its retries it drops out of the payload, which the
  shape signature reports as a within-feed change indistinguishable from a real
  break. Two gates separate them, and BOTH are required:

    1. the feed is declared in MEMBER_MAPPED_FEEDS — a field-keyed `data` block
       (grid_imbalance, market_history) is not a member catalog, and a key
       vanishing from one IS the break this tripwire exists to catch;
    2. `classify_data_member_drift()` finds the diff is purely a member-set
       change, with every member on both sides sharing one shape.

  Narrower than declaring the feed volatile (which ignores the hash outright),
  so a member gaining or losing a field still fails. Member drift is classified
  BEFORE volatility, and MEMBER_MAPPED_FEEDS are excluded from derived
  volatility, so the blunt rule cannot pre-empt the precise one. CRITICAL_FEEDS
  are exempt and always enforce. Added 2026-08-14 after a single dropped buurt
  location blocked the publish of 18 healthy feeds.

Volatile feeds (within-feed data-driven churn):
  Some feeds legitimately change their within-feed shape day-to-day because
  their data block is keyed by a set that varies (e.g. cross_border_flows'
  per-hour border keys, calendar_features' upcoming_holidays list, the RIVM
  station set). These warn instead of failing. Membership is the UNION of a
  hand-curated seed set (VOLATILE_SHAPE_FEEDS) and a history-derived set
  (`derive_volatile_feeds()`), the latter auto-classifying any feed that has
  wobbled in committed history without a version bump. The derivation makes
  the classification self-maintaining, so a recurring false positive stops
  paging without an allowlist edit.

Usage:
    python scripts/detect_schema_drift.py
    python scripts/detect_schema_drift.py --warn-only        # never fail
    python scripts/detect_schema_drift.py --previous-ref HEAD~7
    python scripts/detect_schema_drift.py --sidecar data/_shape_signatures.json

Designed to be invoked from `.github/workflows/collect-data.yml` after the
publish step, so the previous commit is the most recent successful daily
publish (HEAD~1 typically).

File: scripts/detect_schema_drift.py
Created: 2026-06-07
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure utils/ is importable when invoked from repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.shape_signature import (  # noqa: E402
    classify_data_member_drift,
    diff_signatures,
    load_shape_observations,
    volatile_feeds_from_observations,
    OBSERVATIONS_FILENAME,
)

# The append-only learning record (#43), relative to the repo root. Written
# every run by data_fetcher; read here instead of the sidecar's git history so
# a run that FAILS this tripwire still teaches the volatility classifier.
OBSERVATIONS_PATH = os.path.join("data", OBSERVATIONS_FILENAME)

# Minimum number of PRIOR runs (excluding the one being judged) before the
# observation log is trusted over the git-history fallback. Guards against
# classifying a feed as churn-prone from a sample of one or two, which would
# downgrade a genuine break to a warning on thin history — a fresh clone, the
# first runs after this lands, or a truncated log.
MIN_PRIOR_OBSERVATIONS = 10

# Feeds whose disappearance from the publish set is operationally
# critical — silent retirement is the threat security audit M2 flagged.
# When any of these appear in `feeds_removed`, we upgrade the catalog-
# drift summary to ::error:: even in catalog-only path (the within-feed
# fail-mode trip is unaffected). Keep this curated; flooding the set
# would re-introduce false positives on transient collector blips.
CRITICAL_FEEDS = frozenset({
    'energy_price_forecast.json',     # combined entsoe + energy_zero — Augur primary input
    'load_forecast.json',
    'generation_forecast.json',
    'weather_forecast_multi_location.json',
})

# Feeds whose within-feed shape is OPERATIONALLY VOLATILE — their data
# block is keyed by a set that legitimately varies day-to-day, so the
# shape signature churns without any schema change. For these, within-feed
# drift is downgraded to ::warning:: instead of exit 1 (the same reasoning
# as the catalog-vs-shape split, applied one level deeper — see module
# docstring). This is the structural analogue of CRITICAL_FEEDS.
#
# NOTE: this hand-curated set is no longer the primary mechanism. main()
# UNIONS it with `derive_volatile_feeds()`, which auto-classifies any feed
# that has wobbled in committed history without a version bump. So you do NOT
# need to add a feed here just because it had a recurring false positive —
# history handles that automatically. This set now serves three narrower
# purposes: (1) a documented record of the originally-diagnosed churn sources,
# (2) an explicit override, and (3) a fallback when history is thin (shallow
# clone, first runs after deploy). The entries below could be pruned once
# their volatility is well-established in history; they are kept as the
# documented baseline.
#
#   air_quality_buurt.json: luchtmeetnet maps each requested location to
#   the NEAREST ONLINE RIVM station and includes only the pollutants that
#   station reported. Both the station set and per-station pollutant set
#   are data, not schema — encoded as dict keys, so the hash flips when a
#   station goes offline and returns (the 2026-06-13 false-positive CI
#   failure). Genuine schema changes here are unversioned-but-tolerated;
#   acceptable since this feed is not an Augur primary input.
#
#   cross_border_flows.json: the per-hour `data.flows` map only contains a
#   border key (e.g. NL->GB) for hours where that interconnector reported a
#   flow. The shape signature samples one representative hour, so a border
#   absent in the sampled hour drops from the shape (the 2026-06-14 false
#   positive: NL->GB missing). The border SET is data, not schema.
#
#   calendar_features.json: metadata.upcoming_holidays is an empty list on
#   most days (shape `value_shape: null`) and becomes a list-of-dicts the
#   moment a holiday enters the lookahead window (the other half of the
#   2026-06-14 false positive). Empty<->populated is data, not schema.
#
# ACCEPTED RISK: a feed listed here has its within-feed shape hash IGNORED
# for fail purposes. A genuine *structural* schema change to that feed (e.g.
# its `data` block changing from a per-location dict to a list, or a new
# top-level envelope key) will warn but NEVER fail CI — the tripwire is
# permanently blind to it. This is consciously accepted for secondary,
# non-Augur feeds whose key set is data-driven. Do NOT add a feed that any
# downstream consumer relies on for structural stability; for those, a real
# shape change must go through a CURRENT_SCHEMA_VERSION bump + migration.
#
# Keep this curated and narrow — broadening it blinds the tripwire to real
# shape breaks. Stable buurt feeds (solar_forecast_buurt, weather_forecast_
# buurt) are keyed by fixed configured coords and must NOT be added.
VOLATILE_SHAPE_FEEDS = frozenset({
    'air_quality_buurt.json',
    'cross_border_flows.json',
    'calendar_features.json',
})

# Feeds whose `data` block is keyed by MEMBER NAME (a location), so that a
# vanished key is a per-member fetch failure rather than a schema change. Only
# these are eligible for the member-drift downgrade — see
# `classify_data_member_drift`, which is deliberately not self-gating.
#
# This set exists because "the data dict is keyed by something" is NOT enough.
# 15 of the 20 published feeds have a plain-dict `data` block, and several key
# it by FIELD name, where a vanished key is exactly the unversioned break this
# tripwire exists to catch:
#
#   grid_imbalance.json  {balance_delta, direction, imbalance_price}
#   market_history.json  {carbon_eua, gas_ttf}
#   ned_production.json  {solar, wind_onshore, wind_offshore}
#   wind_forecast.json   {entsoe_wind_generation, offshore_wind}
#
# Without this gate, `grid_imbalance` silently dropping `imbalance_price`
# downgraded to a warning and published — found by the /review-changes battery
# on the first draft of this change, reproduced end-to-end against the live
# sidecar. Keep the set to feeds whose members are genuinely interchangeable
# per-location records; when in doubt, leave a feed out and let it fail loudly.
#
# weather_forecast_multi_location.json is listed for intent even though it is
# also a CRITICAL_FEED and so can never actually downgrade — protected feeds
# short-circuit first in `_partition_member_drift`.
#
# air_quality_buurt.json is deliberately ABSENT despite being location-keyed:
# luchtmeetnet varies the per-station pollutant set, so its members are not
# homogeneous and it is already handled by the declared VOLATILE_SHAPE_FEEDS.
MEMBER_MAPPED_FEEDS = frozenset({
    'weather_forecast_buurt.json',
    'solar_forecast_buurt.json',
    'demand_weather_forecast.json',
    'solar_forecast.json',
    'weather_forecast_multi_location.json',
})

# A feed cannot be both critical (its removal fails CI) and volatile (its
# within-feed shape change is ignored) — those are contradictory signals.
# Enforce the invariant at import time so a future edit to either set can't
# silently violate it.
assert CRITICAL_FEEDS.isdisjoint(VOLATILE_SHAPE_FEEDS), (
    "A feed cannot be both critical and volatile — overlap: "
    f"{sorted(CRITICAL_FEEDS & VOLATILE_SHAPE_FEEDS)}"
)

# Member-mapped and volatile are likewise contradictory: the point of declaring
# a feed member-mapped is that its member-set churn is classified PRECISELY, so
# anything the precise rule rejects must still fail. Declaring it volatile too
# would ignore its hash outright and undo that.
assert MEMBER_MAPPED_FEEDS.isdisjoint(VOLATILE_SHAPE_FEEDS), (
    "A feed cannot be both member-mapped and volatile — overlap: "
    f"{sorted(MEMBER_MAPPED_FEEDS & VOLATILE_SHAPE_FEEDS)}"
)


def _load_current_sidecar(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        print(f"::error::Sidecar file not found: {path}", file=sys.stderr)
        print(
            "Did data_fetcher.py run successfully and write "
            "data/_shape_signatures.json?",
            file=sys.stderr,
        )
        sys.exit(2)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_previous_sidecar(
    repo_path: Path,
    ref: str,
) -> Optional[Dict[str, Any]]:
    """
    Read the previous commit's `data/_shape_signatures.json` via git.

    Returns:
        Parsed dict, or None when the file doesn't exist at that ref
        (the first run after deploying this tripwire — initialise state).
    """
    relative = repo_path.relative_to(REPO_ROOT).as_posix()
    cmd = ["git", "show", f"{ref}:{relative}"]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False, cwd=REPO_ROOT
        )
    except FileNotFoundError:
        print("::error::git not on PATH — cannot diff against previous commit",
              file=sys.stderr)
        sys.exit(2)
    if result.returncode != 0:
        # Most likely: file didn't exist at that ref → first run.
        stderr = (result.stderr or "").lower()
        if "exists on disk, but not in" in stderr or "does not exist" in stderr \
                or "fatal: path" in stderr:
            return None
        # Some other git failure
        print(
            f"::warning::git show {ref}:{relative} failed (exit {result.returncode}): "
            f"{result.stderr.strip()}",
            file=sys.stderr,
        )
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"::warning::Previous sidecar at {ref} is not valid JSON: {exc}",
              file=sys.stderr)
        return None


def derive_volatile_feeds(
    sidecar_relpath: str,
    ref: str = "HEAD",
    window: int = 60,
    repo_root: Path = REPO_ROOT,
    current_observed_at: Optional[str] = None,
) -> frozenset:
    """Classify feeds as shape-volatile from their committed shape history.

    Walks the last `window` commits of `sidecar_relpath` reachable from `ref`
    and records, per feed, the set of shape_hash values seen AT EACH
    schema_version. A feed is volatile if any single schema_version shows more
    than one distinct shape_hash — i.e. its shape changed without a version
    bump, the signature of data-driven churn. A versioned migration moves the
    schema_version too, so its hashes land under different versions and are
    NOT counted (that path is already a clean pass via schema_version_changed).

    This is the self-maintaining counterpart to the hand-curated
    VOLATILE_SHAPE_FEEDS: a feed that has ever wobbled in committed history is
    auto-classified, so a *recurring* data-driven false positive warns instead
    of failing CI with no allowlist edit. main() unions this with the declared
    set, which remains the explicit seed/override and the fallback when history
    is thin (shallow clone, first runs after deploy).

    SOURCE OF HISTORY (#43, 2026-08-08). Prefers the append-only observation
    log `data/_shape_observations.jsonl`, falling back to the sidecar's git
    history when the log is absent or too thin. The distinction is the whole
    point of #43: the sidecar is the tripwire's BASELINE and is committed only
    by a run that passed, so deriving from it means a feed that FAILS the gate
    is never learned from. `ned_production` and `wind_forecast` failed on
    2026-08-03 and were still unclassified five days later for exactly that
    reason. The observation log is written every run, pass or fail.

    The current run is EXCLUDED from its own evidence via `current_observed_at`
    (and CRITICAL_FEEDS are never derived-volatile at all — see main()). Both
    guards exist because `data_fetcher` appends the current record before this
    runs: without them a feed's first ever break supplies its own second hash
    and downgrades itself from ::error:: to ::warning::, which silently undoes
    the 2026-06-10 fail-mode flip. Evidence must be strictly prior.

    KNOWN LIMIT: history can only record variation that was recorded. A feed's
    very first unversioned deviation cannot be distinguished from a real break
    by history alone (it has one prior hash) — that case still relies on the
    declared set or a one-time human call. Derivation eliminates the RECURRING
    failures, which is the actual operational pain. This limit is deliberate:
    it is what makes a genuine first break still fail.

    Returns a frozenset of feed names. Any git/parse failure degrades to an
    empty set (caller falls back to the declared set).
    """
    observations = load_shape_observations(str(repo_root / OBSERVATIONS_PATH))
    if current_observed_at is not None:
        prior = [r for r in observations if r.get("observed_at") != current_observed_at]
    else:
        prior = observations
    # Threshold is on PRIOR runs, not total records, and is deliberately well
    # above 2. Classifying "this feed churns" off one or two prior samples is
    # weaker evidence than the git fallback would have used (which walks up to
    # `window` commits), and it errs in the unsafe direction — a wrong volatile
    # call downgrades a real break to a warning. Below the threshold, defer to
    # the git history path instead.
    if len(prior) >= MIN_PRIOR_OBSERVATIONS:
        return volatile_feeds_from_observations(
            observations, window=window, exclude_observed_at=current_observed_at
        )

    # Fallback: derive from the sidecar's git history. Still correct, just
    # blind to failing runs — this is the pre-#43 behaviour, kept for a thin
    # or missing log (shallow clone, first runs after deploy).
    # feed -> schema_version -> set(shape_hash)
    seen: Dict[str, Dict[Any, set]] = {}
    try:
        log = subprocess.run(
            ["git", "log", f"-n{window}", "--format=%H", ref,
             "--", sidecar_relpath],
            capture_output=True, text=True, check=False, cwd=repo_root,
        )
    except FileNotFoundError:
        return frozenset()
    if log.returncode != 0:
        return frozenset()
    for commit in (c for c in log.stdout.split() if c):
        show = subprocess.run(
            ["git", "show", f"{commit}:{sidecar_relpath}"],
            capture_output=True, text=True, check=False, cwd=repo_root,
        )
        if show.returncode != 0:
            continue
        try:
            payload = json.loads(show.stdout)
        except json.JSONDecodeError:
            continue
        version = payload.get("schema_version")
        for feed, info in (payload.get("feeds") or {}).items():
            if not isinstance(info, dict):
                continue
            h = info.get("shape_hash")
            if h is None:
                continue
            seen.setdefault(feed, {}).setdefault(version, set()).add(h)
    return frozenset(
        feed for feed, by_version in seen.items()
        if any(len(hashes) > 1 for hashes in by_version.values())
    )


def _emit_summary(report: Dict[str, Any], current_path: Path,
                  effective_volatile: frozenset = frozenset(),
                  member_drift_feeds: frozenset = frozenset()) -> None:
    """Human-readable summary for the Actions run log."""
    print(f"Schema-drift report (current sidecar: {current_path})")
    print(f"  previous schema_version: {report['previous_schema_version']!r}")
    print(f"  current  schema_version: {report['current_schema_version']!r}")
    print(f"  schema_version_changed:  {report['schema_version_changed']}")
    print(f"  feeds added:    {report['feeds_added'] or '(none)'}")
    print(f"  feeds removed:  {report['feeds_removed'] or '(none)'}")
    print(f"  feeds unchanged: {len(report['feeds_unchanged'])}")
    if report["feeds_changed"]:
        print("  feeds CHANGED:")
        for c in report["feeds_changed"]:
            # Mark volatile feeds so the CHANGED list reconciles with the
            # downstream ::error:: count (which excludes volatile feeds).
            # Tag from the EFFECTIVE set (declared + derived), not the
            # declared one — otherwise an auto-classified feed prints
            # untagged while being treated as volatile, and the CHANGED
            # list stops reconciling with the ::error:: count, which is
            # the one thing this tag exists to do.
            if c["feed"] in effective_volatile:
                tag = " [volatile]"
            elif c["feed"] in member_drift_feeds:
                tag = " [member-drift]"
            else:
                tag = ""
            print(
                f"    - {c['feed']}{tag}: "
                f"{c['previous_hash']} -> {c['current_hash']}"
            )
            if c.get("sources_diff"):
                sd = c["sources_diff"]
                if sd["added"]:
                    print(f"        + collectors: {sd['added']}")
                if sd["removed"]:
                    print(f"        - collectors: {sd['removed']}")
    else:
        print("  feeds CHANGED:  (none)")


def _partition_within_feed_drift(feeds_changed, volatile_feeds=VOLATILE_SHAPE_FEEDS):
    """Split changed feeds into ``(volatile, enforced)`` by membership in
    ``volatile_feeds``.

    Volatile feeds (VOLATILE_SHAPE_FEEDS) warn but never fail; everything
    else is an enforced shape diff that must be versioned. Extracted as a
    pure helper so the split — including the multi-feed case — is unit-
    testable with an arbitrary feed set, independent of the git/subprocess
    harness in main().
    """
    volatile = [c for c in feeds_changed if c["feed"] in volatile_feeds]
    enforced = [c for c in feeds_changed if c["feed"] not in volatile_feeds]
    return volatile, enforced


def _partition_member_drift(
    feeds_changed,
    previous: Dict[str, Any],
    current: Dict[str, Any],
    protected: frozenset = CRITICAL_FEEDS,
    eligible: frozenset = MEMBER_MAPPED_FEEDS,
):
    """Split changed feeds into ``(member_drift, enforced)``.

    A feed lands in ``member_drift`` only when it is DECLARED member-mapped
    (`eligible`) and the only structural difference is which members its `data`
    block contains — a location that dropped out of (or recovered into) the
    payload, with every member on both sides sharing one shape. See
    `classify_data_member_drift()`.

    The `eligible` gate is what keeps this from applying to field-keyed feeds,
    where a vanished `data` key is a real schema break. `classify_data_member_
    drift` cannot make that call on structure alone and does not try.

    ``protected`` feeds are never downgraded, mirroring the CRITICAL_FEEDS
    carve-out in the derived-volatility path: on the feeds Augur depends on,
    a member vanishing is operationally severe enough to warrant the hard
    stop even though it is not, strictly, a schema change.

    Each downgraded entry carries a ``member_drift`` key with the added /
    removed / retained member names, so the warning can name what moved.
    """
    prev_feeds = (previous.get("feeds") or {}) if isinstance(previous, dict) else {}
    curr_feeds = (current.get("feeds") or {}) if isinstance(current, dict) else {}
    member_drift, enforced = [], []
    for c in feeds_changed:
        name = c["feed"]
        if name in protected or name not in eligible:
            enforced.append(c)
            continue
        verdict = classify_data_member_drift(
            (prev_feeds.get(name) or {}).get("shape_signature"),
            (curr_feeds.get(name) or {}).get("shape_signature"),
        )
        if verdict is None:
            enforced.append(c)
        else:
            member_drift.append({**c, "member_drift": verdict})
    return member_drift, enforced


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sidecar",
        default="data/_shape_signatures.json",
        help="Path to the current sidecar file (relative to repo root)",
    )
    parser.add_argument(
        "--previous-ref",
        default="HEAD~1",
        help="Git ref of the previous sidecar to compare against (default HEAD~1)",
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Print findings but always exit 0. Use during the bedding-in period "
             "before flipping to fail-mode.",
    )
    parser.add_argument(
        "--volatility-window",
        type=int,
        default=60,
        help="How many recent sidecar commits to scan when auto-deriving "
             "shape-volatile feeds from committed history (default 60). "
             "Set 0 to disable derivation and use only the declared set.",
    )
    args = parser.parse_args()

    current_path = REPO_ROOT / args.sidecar
    current = _load_current_sidecar(current_path)
    previous = _load_previous_sidecar(current_path, args.previous_ref)

    if previous is None:
        print(
            f"::notice::No previous shape signatures at {args.previous_ref} — "
            "treating this as initialisation. No drift comparison performed.",
            file=sys.stderr,
        )
        # Still print a summary so the run log has context
        print(f"Schema-drift tripwire: initialising baseline at "
              f"schema_version={current.get('schema_version')!r}, "
              f"{len(current.get('feeds', {}))} feeds.")
        return 0

    report = diff_signatures(previous, current)

    # No change → clean pass
    if not report["feeds_changed"] and not report["feeds_added"] \
            and not report["feeds_removed"]:
        _emit_summary(report, current_path)
        print("::notice::No schema drift detected.")
        return 0

    # Drift accompanied by a version bump → expected, properly versioned
    if report["schema_version_changed"]:
        _emit_summary(report, current_path)
        print(
            f"::notice::Schema drift detected AND schema_version bumped "
            f"({report['previous_schema_version']} -> "
            f"{report['current_schema_version']}). This is a properly "
            "versioned change."
        )
        return 0

    # Drift WITHOUT a version bump. Split by class:
    #   - feeds_changed (within-feed shape diff) → the failure mode this
    #     tripwire catches.
    #   - feeds_added / feeds_removed only → operational catalog drift
    #     (transient collector recovery, retired collector). Surface as
    #     warning even in fail-mode — see "Catalog vs shape drift" in
    #     the module docstring.

    # Catalog-drift summary message — assembled once so we can surface
    # it both in catalog-only mode AND alongside a within-feed
    # alert (opus M4: warn-only with both kinds of drift was losing
    # this summary entirely).
    added = report["feeds_added"]
    removed = report["feeds_removed"]
    critical_removed = sorted(set(removed) & CRITICAL_FEEDS)
    catalog_msg: Optional[str] = None
    if added or removed:
        parts = []
        if added:
            parts.append(f"{len(added)} added ({', '.join(added)})")
        if removed:
            parts.append(f"{len(removed)} removed ({', '.join(removed)})")
        catalog_msg = (
            f"Catalog drift: {'; '.join(parts)}. No within-feed shape "
            "change required for catalog drift — treated as operational "
            "(transient collector recovery / retirement)."
        )

    # Effective volatile set = declared (explicit seed/override + fallback)
    # UNION history-derived (feeds that have wobbled in committed history
    # without a version bump). The derived half makes the classification
    # self-maintaining: a recurring data-driven false positive warns instead
    # of failing CI without anyone editing the allowlist.
    derived_volatile = (
        derive_volatile_feeds(args.sidecar, args.previous_ref,
                              args.volatility_window,
                              current_observed_at=current.get("computed_at"))
        if args.volatility_window > 0 else frozenset()
    )
    # A CRITICAL_FEED is never auto-downgraded. Derivation is a heuristic over
    # observed churn, and on the feeds Augur depends on, a wrong heuristic turns
    # a hard failure into a warning nobody reads. Declaring one volatile stays
    # possible, but it must be a human edit to VOLATILE_SHAPE_FEEDS — which the
    # assert above already forbids, so in practice a critical feed always fails.
    # A MEMBER_MAPPED_FEED is never auto-downgraded to blunt volatility either.
    # Its member-set churn is exactly what the precise classifier below handles,
    # so letting derivation mark it volatile would ignore its shape hash
    # outright and blind the tripwire to the real breaks the precise rule is
    # meant to keep failing. This is not hypothetical: the single transient
    # dropout of 2026-08-14 put two hashes in the observation log for both buurt
    # feeds, and the very next run auto-classified them volatile — the outcome
    # VOLATILE_SHAPE_FEEDS' own comment says must never happen to them.
    derived_volatile = (
        frozenset(derived_volatile) - CRITICAL_FEEDS - MEMBER_MAPPED_FEEDS
    )
    effective_volatile = VOLATILE_SHAPE_FEEDS | derived_volatile
    auto_only = sorted(derived_volatile - VOLATILE_SHAPE_FEEDS)

    # Member drift is classified FIRST, before volatility. Both downgrade to a
    # warning, but member drift is the narrower and more informative verdict —
    # it names what moved and still fails on anything else. Letting the blunt
    # rule win first would discard that signal.
    member_changed, remaining_changed = _partition_member_drift(
        report["feeds_changed"], previous, current
    )

    # Partition what member drift did not explain: volatile feeds (data-driven
    # shape churn) warn but never fail; everything else is an enforced shape
    # diff that must be versioned.
    volatile_changed, enforced_changed = _partition_within_feed_drift(
        remaining_changed, effective_volatile
    )

    # Summary is emitted here, after classification, so each CHANGED line can
    # carry its [volatile] / [member-drift] tag and the list reconciles with
    # the ::error:: count below.
    _emit_summary(report, current_path, effective_volatile,
                  frozenset(c["feed"] for c in member_changed))

    if auto_only:
        print(
            f"::notice::Auto-classified {len(auto_only)} feed(s) as shape-"
            f"volatile from committed history (warn, not fail): {auto_only}"
        )

    for c in member_changed:
        md = c["member_drift"]
        parts = []
        if md["removed"]:
            parts.append(f"members dropped: {', '.join(md['removed'])}")
        if md["added"]:
            parts.append(f"members recovered: {', '.join(md['added'])}")
        print(
            f"::warning::{c['feed']}: {'; '.join(parts)} "
            f"({len(md['retained'])} retained, all structurally unchanged). "
            "Member-set change in the data block — treated as operational "
            "(a per-member fetch failure or recovery), not a schema change."
        )

    if volatile_changed:
        names = ", ".join(c["feed"] for c in volatile_changed)
        print(
            f"::warning::Within-feed shape drift on {len(volatile_changed)} "
            f"volatile feed(s) ({names}) — data-driven shape churn (declared in "
            "VOLATILE_SHAPE_FEEDS or auto-derived from committed history), "
            "treated as operational, not a schema change."
        )

    if enforced_changed:
        changed_count = len(enforced_changed)
        msg = (
            f"Within-feed shape drift on {changed_count} feed(s) "
            f"without a schema_version bump (still "
            f"{report['current_schema_version']}). Either bump "
            "CURRENT_SCHEMA_VERSION + add a SCHEMA_CHANGELOG entry + "
            "a migration function, or revert the shape change."
        )
        if args.warn_only:
            print(f"::warning::{msg} (warn-only mode — not failing)")
            if catalog_msg:
                print(f"::warning::{catalog_msg}")
            if critical_removed:
                print(
                    f"::error::Critical feed(s) removed: {critical_removed}. "
                    "Investigate before next pipeline run."
                )
            return 0
        print(f"::error::{msg}")
        if catalog_msg:
            print(f"::warning::{catalog_msg}")
        if critical_removed:
            print(
                f"::error::Critical feed(s) removed: {critical_removed}. "
                "Investigate before next pipeline run."
            )
        return 1

    # Reached when there is no enforced within-feed drift: catalog-only
    # drift (added/removed), volatile-only within-feed drift, or both.
    # catalog_msg is None when the only drift was volatile within-feed —
    # guard the print so we don't emit "::warning::None".
    # Critical-feed removal upgrades to ::error:: even though catalog
    # drift normally exits 0 (security audit M2).
    if catalog_msg:
        print(f"::warning::{catalog_msg}")
    if critical_removed:
        print(
            f"::error::Critical feed(s) removed: {critical_removed}. "
            "Catalog-drift normally exits 0, but a critical-feed loss is "
            "operationally severe enough to fail CI even without a "
            "schema_version bump."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
