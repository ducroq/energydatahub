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
      bumped (the change is properly versioned), OR --warn-only is set.
  1 — drift detected AND schema_version did NOT change. The pipeline
      shipped a new shape without bumping the version — exactly the
      class of bug this tripwire exists to catch.
  2 — script setup error (missing files, git failure, etc.)

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
        print(f"  feeds CHANGED:")
        for c in report["feeds_changed"]:
            print(f"    - {c['feed']}: {c['previous_hash']} -> {c['current_hash']}")
            if c.get("sources_diff"):
                sd = c["sources_diff"]
                if sd["added"]:
                    print(f"        + collectors: {sd['added']}")
                if sd["removed"]:
                    print(f"        - collectors: {sd['removed']}")
    else:
        print(f"  feeds CHANGED:  (none)")


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

    # Drift WITHOUT a version bump → the failure mode this tripwire catches
    feed_count = len(report["feeds_changed"]) + len(report["feeds_added"]) \
        + len(report["feeds_removed"])
    msg = (
        f"Schema drift detected across {feed_count} feed(s) but "
        f"schema_version did not change (still "
        f"{report['current_schema_version']}). Either bump "
        "CURRENT_SCHEMA_VERSION + add a SCHEMA_CHANGELOG entry + a migration "
        "function, or revert the shape change."
    )
    if args.warn_only:
        print(f"::warning::{msg} (warn-only mode — not failing)")
        return 0
    print(f"::error::{msg}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
