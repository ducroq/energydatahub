#!/usr/bin/env python3
"""Verification hook — runs after every Edit/Write/MultiEdit and feeds failures back.

Adopted from agent-ready-projects v1.14.0 ("Verification Hooks"). The point is who
carries the error message: without this, the agent edits, stops, and waits for a human
to run the tests and paste the failure back. With it, the agent edits, sees the failure,
and fixes it while the reasoning that produced the bug is still in context.

**Exit code is the whole mechanism.** On exit 0 this script's stdout goes to a debug log
the agent never reads — that is the "silent hook" failure mode, and it is what you get by
default. Exit 2 writes stderr back to the agent as actionable feedback. So: every failure
path here MUST print to stderr and exit 2.

Scope is deliberately narrow (see the "tightened leash" failure mode): only Python under
the paths that actually run in production, plus a YAML parse for the workflows. Editing
CLAUDE.md or memory/ triggers nothing.

Depth is mapped, not global: an edit to collectors/entsoe_hydro.py runs
tests/unit/test_entsoe_hydro_collector.py (~1s), not the 681-test suite (~10s). The
mapping is derived by glob from the filename, not hand-maintained — this repo's own
"derive classifications from state, don't hand-maintain exception lists" rule.

Two limits of that mapping, both deliberate and neither silent-by-accident:
  - It matches on *filename*, so a test named for a behaviour rather than its subject
    is not reached. Editing collectors/base.py does NOT run test_circuit_breaker.py.
  - A file with no mapped test falls back to the whole tests/unit suite, which may not
    import the edited file at all. A green hook there means "nothing else broke", not
    "this file is covered". utils/secure_data_handler.py was the worst case — the AES/HMAC
    module, verified by 681 tests that never imported it — and got its own test file on
    2026-08-08. Check `mapped_tests(path)` before trusting a green run on a new file.
Exit 0 from this hook is never a coverage claim. Run the full suite before committing.

Verify it still works after changing it:
    echo '{"tool_input":{"file_path":"collectors/base.py"}}' | .claude/hooks/verify_edit.py; echo "exit=$?"
Then break something on purpose and confirm you get exit=2 with the failure on stderr.
A hook you have not seen fail is a hook you cannot trust.
"""

import glob
import json
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
PY = os.path.join(REPO, "venv", "bin", "python")
USING_VENV = os.path.exists(PY)
if not USING_VENV:
    PY = sys.executable

# Only these paths run unattended in production. Everything else is out of scope.
WATCHED_PY = ("collectors/", "utils/", "scripts/", "tests/", "data_fetcher.py")
SKIPPED = ("venv/", "legacy/", "data/", "docs/", "htmlcov/", "__pycache__/", "tests/manual/")

# Budget in seconds. A slow hook burns context on output the agent cannot act on and
# trains you to disable it.
TIMEOUT = 90


def fail(msg):
    """Exit 2 with stderr — the only path that reaches the agent."""
    sys.stderr.write(msg.rstrip() + "\n")
    sys.exit(2)


def run(cmd):
    try:
        p = subprocess.run(
            cmd, cwd=REPO, capture_output=True, text=True, timeout=TIMEOUT
        )
    except subprocess.TimeoutExpired:
        return 1, "", f"timed out after {TIMEOUT}s: {' '.join(cmd)}"
    return p.returncode, p.stdout, p.stderr


def mapped_tests(rel):
    """Find the unit tests covering `rel`, by glob on the filename.

    collectors/base.py            -> tests/unit/test_base_collector.py
    utils/data_types.py           -> tests/unit/test_utils_data_types.py
    collectors/_http_classifier.py-> tests/unit/test_http_classifier.py
    scripts/detect_schema_drift.py-> tests/unit/test_detect_schema_drift_script.py

    All three patterns are unioned — never short-circuited on the first that hits.
    An earlier version stopped at the first matching pattern, which was harmless for
    today's test tree but silently dropped a sibling the moment anyone added the
    natural second file (utils/data_quality.py + test_data_quality_ranges.py would
    have run only test_data_quality.py). Widening here costs a second of runtime;
    narrowing costs a green hook over a failing test.
    """
    stem = os.path.basename(rel)[:-3].lstrip("_")
    seen = []
    for pat in (f"test_{stem}.py", f"test_{stem}_*.py", f"test_*{stem}*.py"):
        for hit in sorted(glob.glob(os.path.join(REPO, "tests", "unit", pat))):
            if hit not in seen:
                seen.append(hit)
    return [os.path.relpath(h, REPO) for h in seen]


def main():
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return  # not a hook invocation we understand; stay silent, exit 0

    # Be defensive about payload shape: a harness change that renames or retypes these
    # keys must not crash us into exit 1, which the harness treats as a non-blocking
    # hook error — i.e. verification stops with no signal at all.
    if not isinstance(payload, dict):
        return
    tool_input = payload.get("tool_input")
    path = tool_input.get("file_path") if isinstance(tool_input, dict) else None
    if not isinstance(path, str) or not path:
        return
    # realpath, not abspath: abspath leaves symlinks unresolved, so reaching this repo
    # through a symlinked home or worktree made `rel` start with ".." and the hook
    # went silently out-of-scope on files it should have checked.
    rel = os.path.relpath(os.path.realpath(path), REPO)
    if rel.startswith("..") or rel.startswith(SKIPPED):
        return

    # GitHub Actions YAML: a workflow that does not parse takes the daily run down
    # silently — CI is the only place it would otherwise surface.
    if rel.startswith(".github/workflows/") and rel.endswith((".yml", ".yaml")):
        code, _, err = run(
            [PY, "-c", f"import yaml,sys;yaml.safe_load(open({rel!r}))"]
        )
        if code != 0:
            # A missing PyYAML and an unparseable workflow both exit non-zero here.
            # Reporting the former as the latter is a lie that blames a valid file —
            # it shipped that way once and made every workflow edit fail.
            if "No module named 'yaml'" in err:
                fail(
                    f"[verify-hook] cannot validate {rel}: PyYAML is not installed in {PY}.\n"
                    "This is a missing dependency, NOT a broken workflow. Install it with:\n"
                    "    uv pip install --python venv/bin/python -r requirements.txt"
                )
            fail(f"[verify-hook] {rel} is not valid YAML — the daily run would fail to start.\n{err}")
        return

    if not rel.endswith(".py") or not rel.startswith(WATCHED_PY):
        return

    code, _, err = run([PY, "-m", "py_compile", rel])
    if code != 0:
        fail(f"[verify-hook] {rel} does not compile.\n{err}")

    tests = mapped_tests(rel)
    if not tests:
        # No mapped test. Fall back to the full unit suite (~10s) rather than nothing:
        # unmapped files here are orchestration (data_fetcher.py), which is where a
        # regression is most expensive and least visible.
        tests = ["tests/unit"]

    code, out, err = run(
        [PY, "-m", "pytest", *tests, "-q", "--no-cov", "-p", "no:cacheprovider", "--tb=short"]
    )
    if code != 0:
        # An interpreter that cannot parse pytest.ini's addopts exits non-zero too.
        # Blaming that on the edit sends the agent hunting a bug in working code —
        # and the obvious "fix" is to edit pytest.ini, which is not even watched here.
        if not USING_VENV and "unrecognized arguments" in (err + out):
            fail(
                f"[verify-hook] cannot run tests: {PY} is not the project venv and does not\n"
                "understand pytest.ini's addopts (--cov / --no-cov). Your edit was NOT verified.\n"
                "This is an environment problem, not a code problem — do not change the code\n"
                "or pytest.ini. Recreate the venv, then re-run:\n"
                f"    {PY} -m pytest {' '.join(tests)} -q"
            )
        fail(
            f"[verify-hook] tests failed after editing {rel} ({' '.join(tests)}):\n"
            f"{out[-4000:]}\n{err[-1000:]}\n"
            "Fix the code. Do not edit the test to make it pass — if the test is wrong, "
            "say so and stop."
        )


if __name__ == "__main__":
    main()
