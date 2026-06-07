"""
End-to-end tests for `scripts/detect_schema_drift.py` (issue #27 Layer A).

Drives the CLI via subprocess against a temp-dir mini-repo so the git-show
mechanism is exercised exactly as it will be in CI.

File: tests/unit/test_detect_schema_drift_script.py
Created: 2026-06-07
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = REPO_ROOT / "scripts" / "detect_schema_drift.py"


def _git(args, cwd):
    """Run a git command in a temp repo, raising on failure."""
    env = os.environ.copy()
    env["GIT_AUTHOR_NAME"] = "test"
    env["GIT_AUTHOR_EMAIL"] = "test@example.com"
    env["GIT_COMMITTER_NAME"] = "test"
    env["GIT_COMMITTER_EMAIL"] = "test@example.com"
    subprocess.run(
        ["git"] + args, cwd=cwd, check=True, env=env,
        capture_output=True, text=True,
    )


def _make_repo_with_two_sidecars(tmp_path: Path, prev: dict, curr: dict) -> Path:
    """Init a git repo with two commits.

    Layout:
      HEAD~1 → commits the `prev` sidecar (CI's "previous run").
      HEAD   → commits the `curr` sidecar (CI's "this run") AND the working
               tree matches HEAD (the script reads the working tree, but
               the script's diff target is `HEAD~1`).
    """
    repo = tmp_path / "repo"
    (repo / "data").mkdir(parents=True)
    # Also need scripts/ and utils/ for the script's imports to resolve.
    import shutil
    shutil.copytree(REPO_ROOT / "scripts", repo / "scripts")
    shutil.copytree(REPO_ROOT / "utils", repo / "utils")

    sidecar = repo / "data" / "_shape_signatures.json"

    _git(["init"], cwd=repo)
    sidecar.write_text(json.dumps(prev))
    _git(["add", "."], cwd=repo)
    _git(["commit", "-m", "previous run"], cwd=repo)

    sidecar.write_text(json.dumps(curr))
    _git(["add", "data/_shape_signatures.json"], cwd=repo)
    # --allow-empty so the helper still works when prev == curr (the
    # no-drift test case has identical sidecars across both commits).
    _git(["commit", "--allow-empty", "-m", "current run"], cwd=repo)

    return repo


def _run_script(repo: Path, *extra_args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(repo / "scripts" / "detect_schema_drift.py"),
         *extra_args],
        cwd=repo, capture_output=True, text=True,
    )


def _feed(hash_val: str, sources=None, data_type="gas_storage"):
    return {
        "shape_hash": hash_val,
        "data_type": data_type,
        "sources": sources,
        "shape_signature": {},
    }


class TestEndToEnd:
    def test_no_drift_exits_0(self, tmp_path):
        sidecar = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {"gas_storage.json": _feed("abc")},
        }
        repo = _make_repo_with_two_sidecars(tmp_path, sidecar, sidecar)
        result = _run_script(repo)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "No schema drift detected" in result.stdout

    def test_drift_without_version_bump_exits_1(self, tmp_path):
        """The danger case CI must catch."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {"gas_storage.json": _feed("old")},
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",  # SAME — but shape changed
            "feeds": {"gas_storage.json": _feed("new")},
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 1, result.stdout + result.stderr
        assert "schema_version did not change" in result.stdout
        assert "::error::" in result.stdout

    def test_drift_with_version_bump_exits_0(self, tmp_path):
        """Shape change accompanied by a proper version bump is expected."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.2",
            "feeds": {"gas_storage.json": _feed("old")},
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",  # BUMPED — properly versioned
            "feeds": {"gas_storage.json": _feed("new")},
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "properly versioned" in result.stdout

    def test_warn_only_never_fails(self, tmp_path):
        """`--warn-only` is the initial bedding-in mode."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {"gas_storage.json": _feed("old")},
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",  # NO bump but shape changed
            "feeds": {"gas_storage.json": _feed("new")},
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo, "--warn-only")
        assert result.returncode == 0, result.stdout + result.stderr
        assert "::warning::" in result.stdout
        assert "warn-only mode" in result.stdout

    def test_first_run_no_previous_sidecar_exits_0(self, tmp_path):
        """Initialisation: no previous sidecar at HEAD~1 → exit 0 cleanly."""
        # Build a repo where the sidecar exists ONLY in the current working tree,
        # not in any commit.
        repo = tmp_path / "repo"
        (repo / "data").mkdir(parents=True)
        import shutil
        shutil.copytree(REPO_ROOT / "scripts", repo / "scripts")
        shutil.copytree(REPO_ROOT / "utils", repo / "utils")
        # Initial commit — no sidecar
        _git(["init"], cwd=repo)
        (repo / "data" / ".keep").write_text("")
        _git(["add", "."], cwd=repo)
        _git(["commit", "-m", "initial empty"], cwd=repo)
        # Now write the sidecar for the first time
        (repo / "data" / "_shape_signatures.json").write_text(json.dumps({
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {"gas_storage.json": _feed("abc")},
        }))
        result = _run_script(repo)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "initialising baseline" in result.stdout

    def test_missing_current_sidecar_exits_2(self, tmp_path):
        """Misconfigured run — sidecar missing entirely."""
        repo = tmp_path / "repo"
        (repo / "data").mkdir(parents=True)
        import shutil
        shutil.copytree(REPO_ROOT / "scripts", repo / "scripts")
        shutil.copytree(REPO_ROOT / "utils", repo / "utils")
        _git(["init"], cwd=repo)
        (repo / "data" / ".keep").write_text("")
        _git(["add", "."], cwd=repo)
        _git(["commit", "-m", "initial"], cwd=repo)
        # No sidecar at all
        result = _run_script(repo)
        assert result.returncode == 2, result.stdout + result.stderr
        assert "::error::Sidecar file not found" in result.stderr

    def test_combined_feed_source_diff_surfaced(self, tmp_path):
        """A combined wrap that loses a per-collector source is visible
        in the summary output."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {
                "energy_price_forecast.json": _feed(
                    "old",
                    sources=["entsoe", "entsoe_de", "energy_zero", "epex"],
                    data_type="combined",
                ),
            },
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {
                "energy_price_forecast.json": _feed(
                    "new",
                    sources=["entsoe", "entsoe_de", "energy_zero"],  # epex dropped
                    data_type="combined",
                ),
            },
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 1, result.stdout + result.stderr
        assert "epex" in result.stdout  # dropped source visible
        assert "- collectors:" in result.stdout
