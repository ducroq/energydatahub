#!/usr/bin/env python3
"""
CI reporter for span (time-extent) shortfalls — issue #51.

Reads `data/_span_shortfalls.json`, written by `data_fetcher` from
`utils/span_signature`, and reports it to the Actions run.

NEVER A GATE. Always exits 0, whatever it finds. That is not timidity, it is
the measured conclusion from 2026-09-04: the schema-drift tripwire's only
response to a problem is withholding all 20 feeds, five consecutive publishes
were lost that way, and a second blocking gate stacked on top would cost far
more availability than this detection is worth. Detection and blocking are
separate decisions here — the workflow raises a tracking issue instead, which
is visible without being fatal.

Prints a `::warning::` per shortfall and sets a `shortfalls` step output so the
workflow can decide whether to open an alert. Exit 0 also on a missing or
malformed report: a diagnostic that cannot read its input must not fail a
publish, and it says so loudly rather than silently reporting "clean".

Usage:
    python scripts/report_span_shortfall.py
    python scripts/report_span_shortfall.py --report data/_span_shortfalls.json

File: scripts/report_span_shortfall.py
Created: 2026-09-04
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_REPORT = "data/_span_shortfalls.json"


def _set_output(name: str, value: str) -> None:
    """Write a step output when running under Actions; a no-op locally."""
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"{name}={value}\n")
    except OSError as exc:
        print(f"::warning::could not write step output {name}: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", default=DEFAULT_REPORT,
                        help=f"Path to the shortfall report (default {DEFAULT_REPORT})")
    args = parser.parse_args()

    path = REPO_ROOT / args.report

    if not path.is_file():
        # Distinguished from "clean" deliberately. data_fetcher writes this file
        # on every run that reaches the sidecar step, so its absence means the
        # run died earlier or the write failed — not that spans are healthy.
        print(f"::warning::No span report at {args.report} — spans were NOT "
              "checked this run. This is not a clean result.")
        _set_output("shortfalls", "0")
        _set_output("checked", "false")
        return 0

    try:
        with open(path, encoding="utf-8") as fh:
            shortfalls = json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"::warning::Span report at {args.report} is unreadable ({exc}) — "
              "spans were NOT checked this run.")
        _set_output("shortfalls", "0")
        _set_output("checked", "false")
        return 0

    # Accept both the current object form and the bare list written by the very
    # first version of this feature, so the run that straddles the change does
    # not report "unreadable".
    if isinstance(shortfalls, list):
        report = {"shortfalls": shortfalls, "members_checked": None,
                  "members_with_expectation": None}
    elif isinstance(shortfalls, dict):
        report = shortfalls
    else:
        print("::warning::Span report is neither an object nor a list — spans "
              "were NOT checked this run.")
        _set_output("shortfalls", "0")
        _set_output("checked", "false")
        return 0

    rows = report.get("shortfalls") or []
    judged = report.get("members_with_expectation")
    total = report.get("members_checked")
    floor = report.get("min_observations", "the minimum")

    _set_output("shortfalls", str(len(rows)))

    # THREE states, not two. The first run after this feature shipped printed
    # "every member carries its usual number of days" while ZERO members had
    # enough history to be judged — a clean-looking result that had verified
    # nothing. That is this project's own three-incident pattern (GoogleWeather,
    # validate_value_ranges, TenneT metadata), reproduced by the very check
    # written to close a gap of the same kind. `checked` is false whenever
    # nothing was actually judged, so the workflow cannot treat it as a pass.
    if not rows and judged == 0:
        print(f"::warning::Spans were NOT verified: 0 of {total} members have "
              f">={floor} prior observations yet, so no expectation could be "
              "derived and no shortfall was reachable. This is not a clean "
              "result — it becomes one once history accumulates.")
        _set_output("checked", "false")
        return 0

    _set_output("checked", "true")

    if not rows:
        if judged is None:
            print("::notice::No span shortfalls (legacy report format — the "
                  "judged-member count is unavailable).")
        else:
            print(f"::notice::No span shortfalls — {judged} of {total} members "
                  "were judged against their history and all carry their usual "
                  "number of days.")
        return 0

    # Worst first; data_fetcher already sorted by ratio, but do not rely on the
    # producer's ordering for an operator-facing message.
    rows = sorted(rows, key=lambda s: s.get("ratio", 1))
    print(f"::error::{len(rows)} member(s) are short of their usual span. A feed "
          "can lose half its forecast horizon with an IDENTICAL shape hash, so "
          "the drift tripwire cannot see this (#51).")
    for s in rows:
        member = s.get("member") or "(root)"
        print(f"::warning::{s.get('feed')}:{member} carries "
              f"{s.get('observed')} day(s), usually {s.get('expected')} "
              f"({s.get('ratio')} of normal)")

    summary = "; ".join(
        f"{s.get('feed')}:{s.get('member') or '(root)'} "
        f"{s.get('observed')}d/{s.get('expected')}d" for s in rows
    )
    _set_output("summary", summary)
    print(f"\nSpan shortfall summary: {summary}")
    # Exit 0 — see the module docstring. This is an alarm, not a gate.
    return 0


if __name__ == "__main__":
    sys.exit(main())
