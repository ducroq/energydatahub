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
      shape change — see "Catalog vs shape drift" below.
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

from utils.shape_signature import diff_signatures  # noqa: E402

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

# A feed cannot be both critical (its removal fails CI) and volatile (its
# within-feed shape change is ignored) — those are contradictory signals.
# Enforce the invariant at import time so a future edit to either set can't
# silently violate it.
assert CRITICAL_FEEDS.isdisjoint(VOLATILE_SHAPE_FEEDS), (
    "A feed cannot be both critical and volatile — overlap: "
    f"{sorted(CRITICAL_FEEDS & VOLATILE_SHAPE_FEEDS)}"
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

    KNOWN LIMIT: history can only record variation that was committed. A feed's
    very first unversioned deviation cannot be distinguished from a real break
    by history alone (it has one prior hash) — that case still relies on the
    declared set or a one-time human call. Derivation eliminates the RECURRING
    failures, which is the actual operational pain.

    Returns a frozenset of feed names. Any git/parse failure degrades to an
    empty set (caller falls back to the declared set).
    """
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


def _emit_summary(report: Dict[str, Any], current_path: Path) -> None:
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
            tag = " [volatile]" if c["feed"] in VOLATILE_SHAPE_FEEDS else ""
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
    _emit_summary(report, current_path)

    # No change → clean pass
    if not report["feeds_changed"] and not report["feeds_added"] \
            and not report["feeds_removed"]:
        print("::notice::No schema drift detected.")
        return 0

    # Drift accompanied by a version bump → expected, properly versioned
    if report["schema_version_changed"]:
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
                              args.volatility_window)
        if args.volatility_window > 0 else frozenset()
    )
    effective_volatile = VOLATILE_SHAPE_FEEDS | derived_volatile
    auto_only = sorted(derived_volatile - VOLATILE_SHAPE_FEEDS)
    if auto_only:
        print(
            f"::notice::Auto-classified {len(auto_only)} feed(s) as shape-"
            f"volatile from committed history (warn, not fail): {auto_only}"
        )

    # Partition within-feed drift: volatile feeds (data-driven shape churn)
    # warn but never fail; everything else is an enforced shape diff that must
    # be versioned. See VOLATILE_SHAPE_FEEDS + derive_volatile_feeds().
    volatile_changed, enforced_changed = _partition_within_feed_drift(
        report["feeds_changed"], effective_volatile
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
