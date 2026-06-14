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

# Import the script as a module so its pure helpers (e.g.
# _partition_within_feed_drift) and constants can be unit-tested directly,
# alongside the subprocess CLI tests below. scripts/ is not a package, so
# load it by file path.
import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location("detect_schema_drift", SCRIPT)
detect_schema_drift = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(detect_schema_drift)


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


def _sidecar(version: str, feed_hashes: dict) -> dict:
    """Build a minimal sidecar payload mapping feed_name -> shape_hash."""
    return {
        "computed_at": "2026-06-14T16:00:00+00:00",
        "schema_version": version,
        "feeds": {name: _feed(h) for name, h in feed_hashes.items()},
    }


def _make_repo_with_commit_history(
    tmp_path: Path, committed: list, working: dict, copy_code: bool = True
) -> Path:
    """Init a git repo committing each sidecar in `committed` in order, then
    leave `working` as an uncommitted change to the sidecar (the script's
    "current" run). HEAD is the last committed sidecar; `--previous-ref HEAD~1`
    is the second-to-last. Used to exercise history-derived volatility, which
    needs more than two commits of shape history.
    """
    repo = tmp_path / "repo"
    (repo / "data").mkdir(parents=True)
    if copy_code:
        import shutil
        shutil.copytree(REPO_ROOT / "scripts", repo / "scripts")
        shutil.copytree(REPO_ROOT / "utils", repo / "utils")
    sidecar = repo / "data" / "_shape_signatures.json"
    _git(["init"], cwd=repo)
    for i, sc in enumerate(committed):
        sidecar.write_text(json.dumps(sc))
        _git(["add", "." if i == 0 else "data/_shape_signatures.json"], cwd=repo)
        _git(["commit", "--allow-empty", "-m", f"commit {i}"], cwd=repo)
    sidecar.write_text(json.dumps(working))  # uncommitted "current" run
    return repo


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
        assert "without a schema_version bump" in result.stdout
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

    def test_feeds_added_only_exits_0_with_warning(self, tmp_path):
        """Catalog drift (a collector recovers from a transient miss in the
        baseline) is operational, not a schema event. Must NOT fail in
        fail-mode — that would block the 2026-06-21 fail-mode flip on the
        first transiently-recovered collector."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {"gas_storage.json": _feed("abc")},
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",  # NO bump
            "feeds": {
                "gas_storage.json": _feed("abc"),
                "air_quality_buurt.json": _feed("xyz"),  # recovered from baseline miss
            },
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Catalog drift" in result.stdout
        assert "air_quality_buurt.json" in result.stdout
        assert "::warning::" in result.stdout
        assert "::error::" not in result.stdout

    def test_feeds_removed_only_exits_0_with_warning(self, tmp_path):
        """A collector retirement (or persistent failure) shows up as
        feeds_removed. Operational, not schema."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {
                "gas_storage.json": _feed("abc"),
                "retired_feed.json": _feed("def"),
            },
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",  # NO bump
            "feeds": {"gas_storage.json": _feed("abc")},
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Catalog drift" in result.stdout
        assert "retired_feed.json" in result.stdout
        assert "::warning::" in result.stdout
        assert "::error::" not in result.stdout

    def test_within_feed_change_plus_catalog_drift_still_fails(self, tmp_path):
        """Mixed case: a within-feed shape change AND a catalog change.
        The shape change still dominates and fail-mode must trip."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {"gas_storage.json": _feed("old")},
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",  # NO bump
            "feeds": {
                "gas_storage.json": _feed("new"),  # shape changed
                "air_quality_buurt.json": _feed("xyz"),  # also added
            },
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 1, result.stdout + result.stderr
        assert "Within-feed shape drift" in result.stdout
        assert "::error::" in result.stdout

    def test_critical_feed_removal_exits_1_in_catalog_path(self, tmp_path):
        """Security audit M2: a critical feed disappearing must fail CI
        even though catalog-only drift normally exits 0. Prevents silent
        retirement (CWE-693) when the upstream completeness tripwire and
        DATASET_MISSING_SEVERITY ever drift apart."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {
                "gas_storage.json": _feed("abc"),
                "energy_price_forecast.json": _feed("def"),  # CRITICAL
            },
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {"gas_storage.json": _feed("abc")},  # critical removed
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 1, result.stdout + result.stderr
        assert "Critical feed(s) removed" in result.stdout
        assert "energy_price_forecast.json" in result.stdout

    def test_non_critical_feed_removal_exits_0(self, tmp_path):
        """A non-critical removed feed still exits 0 — the upgrade is
        scoped to the curated CRITICAL_FEEDS set only."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {
                "gas_storage.json": _feed("abc"),
                "air_quality_buurt.json": _feed("xyz"),
            },
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {"gas_storage.json": _feed("abc")},
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Catalog drift" in result.stdout
        assert "Critical feed(s) removed" not in result.stdout

    def test_warn_only_keeps_catalog_summary_alongside_shape_alert(self, tmp_path):
        """opus M4: when both within-feed and catalog drift occur in
        warn-only mode, both summaries must surface — previously the
        catalog summary was lost."""
        prev = {
            "computed_at": "2026-06-06T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {"gas_storage.json": _feed("old")},
        }
        curr = {
            "computed_at": "2026-06-07T16:00:00+00:00",
            "schema_version": "2.3",
            "feeds": {
                "gas_storage.json": _feed("new"),         # shape changed
                "air_quality_buurt.json": _feed("xyz"),   # also added
            },
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo, "--warn-only")
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Within-feed shape drift" in result.stdout
        assert "Catalog drift" in result.stdout
        assert "air_quality_buurt.json" in result.stdout

    def test_volatile_feed_within_change_exits_0_with_warning(self, tmp_path):
        """The 2026-06-13 false positive: air_quality_buurt's data block is
        keyed by the RIVM station/pollutant set, which legitimately varies
        day-to-day. A within-feed shape change on a VOLATILE_SHAPE_FEEDS feed
        warns but must NOT fail CI."""
        prev = {
            "computed_at": "2026-06-12T16:00:00+00:00",
            "schema_version": "2.4",
            "feeds": {
                "gas_storage.json": _feed("abc"),
                "air_quality_buurt.json": _feed("b343fc99"),  # 1 station that day
            },
        }
        curr = {
            "computed_at": "2026-06-13T16:00:00+00:00",
            "schema_version": "2.4",  # NO bump — station came back online
            "feeds": {
                "gas_storage.json": _feed("abc"),
                "air_quality_buurt.json": _feed("c30a221a"),  # 2 stations again
            },
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 0, result.stdout + result.stderr
        # Pin the exact volatile-warning message, not loose substrings, and
        # confirm it is the ONLY warning emitted (no stray catalog warning).
        assert (
            "Within-feed shape drift on 1 volatile feed(s) "
            "(air_quality_buurt.json)" in result.stdout
        )
        assert "::error::" not in result.stdout
        assert result.stdout.count("::warning::") == 1
        # Guard the specific "::warning::None" regression (catalog_msg is None
        # on a volatile-only run). Narrower than a blanket "None" check, which
        # the summary can legitimately print for a None hash/data_type.
        assert "::warning::None" not in result.stdout

    def test_volatile_change_plus_enforced_change_still_fails(self, tmp_path):
        """A volatile feed changing does NOT mask a real shape break on a
        non-volatile feed in the same run — the enforced change still trips
        fail-mode."""
        prev = {
            "computed_at": "2026-06-12T16:00:00+00:00",
            "schema_version": "2.4",
            "feeds": {
                "gas_storage.json": _feed("old"),
                "air_quality_buurt.json": _feed("b343fc99"),
            },
        }
        curr = {
            "computed_at": "2026-06-13T16:00:00+00:00",
            "schema_version": "2.4",  # NO bump
            "feeds": {
                "gas_storage.json": _feed("new"),            # enforced shape change
                "air_quality_buurt.json": _feed("c30a221a"),  # volatile churn
            },
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 1, result.stdout + result.stderr
        assert "::error::" in result.stdout
        assert "without a schema_version bump" in result.stdout
        # The volatile feed is still surfaced as a warning alongside the error.
        assert "volatile feed(s)" in result.stdout

    def test_volatile_change_plus_catalog_drift_exits_0_with_both_warnings(self, tmp_path):
        """Volatile within-feed churn co-occurring with catalog drift (a new
        non-critical feed appears) must still exit 0, surfacing BOTH the
        volatile warning and the catalog-drift warning — exercises the
        catalog_msg path when there is no enforced change."""
        prev = {
            "computed_at": "2026-06-12T16:00:00+00:00",
            "schema_version": "2.4",
            "feeds": {
                "gas_storage.json": _feed("abc"),
                "air_quality_buurt.json": _feed("b343fc99"),
            },
        }
        curr = {
            "computed_at": "2026-06-13T16:00:00+00:00",
            "schema_version": "2.4",  # NO bump
            "feeds": {
                "gas_storage.json": _feed("abc"),
                "air_quality_buurt.json": _feed("c30a221a"),  # volatile churn
                "nordic_hydro.json": _feed("new"),            # catalog: feed added
            },
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "volatile feed(s)" in result.stdout
        assert "Catalog drift" in result.stdout
        assert "nordic_hydro.json" in result.stdout
        assert "::error::" not in result.stdout
        assert "::warning::None" not in result.stdout

    def test_volatile_only_under_warn_only_exits_0(self, tmp_path):
        """Volatile-only drift under --warn-only behaves identically to the
        default mode: exit 0 with the volatile warning, no error, no stray
        None."""
        prev = {
            "computed_at": "2026-06-12T16:00:00+00:00",
            "schema_version": "2.4",
            "feeds": {"air_quality_buurt.json": _feed("b343fc99")},
        }
        curr = {
            "computed_at": "2026-06-13T16:00:00+00:00",
            "schema_version": "2.4",
            "feeds": {"air_quality_buurt.json": _feed("c30a221a")},
        }
        repo = _make_repo_with_two_sidecars(tmp_path, prev, curr)
        result = _run_script(repo, "--warn-only")
        assert result.returncode == 0, result.stdout + result.stderr
        assert "volatile feed(s)" in result.stdout
        assert "::error::" not in result.stdout
        assert "::warning::None" not in result.stdout

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


class TestPartitionHelper:
    """Direct unit tests for _partition_within_feed_drift and the set
    invariants. These reach the multi-feed split that the subprocess CLI
    tests can't exercise with only one production volatile feed."""

    @staticmethod
    def _c(name):
        return {"feed": name, "previous_hash": "a", "current_hash": "b"}

    def test_multiple_volatile_feeds_all_partition_to_volatile(self):
        changed = [self._c("air_quality_buurt.json"), self._c("weather_buurt.json")]
        volatile, enforced = detect_schema_drift._partition_within_feed_drift(
            changed,
            volatile_feeds=frozenset(
                {"air_quality_buurt.json", "weather_buurt.json"}
            ),
        )
        assert [c["feed"] for c in volatile] == [
            "air_quality_buurt.json",
            "weather_buurt.json",
        ]
        assert enforced == []

    def test_mixed_volatile_and_enforced_split(self):
        changed = [self._c("air_quality_buurt.json"), self._c("gas_storage.json")]
        volatile, enforced = detect_schema_drift._partition_within_feed_drift(
            changed, volatile_feeds=frozenset({"air_quality_buurt.json"})
        )
        assert [c["feed"] for c in volatile] == ["air_quality_buurt.json"]
        assert [c["feed"] for c in enforced] == ["gas_storage.json"]

    def test_production_volatile_set_is_pinned(self):
        """Pin the exact curated production set so any addition/removal is a
        deliberate, reviewed change. Each entry is a confirmed data-driven
        false-positive source (see VOLATILE_SHAPE_FEEDS comment)."""
        assert detect_schema_drift.VOLATILE_SHAPE_FEEDS == frozenset({
            "air_quality_buurt.json",   # RIVM station/pollutant key churn (06-13)
            "cross_border_flows.json",  # per-hour border key churn (06-14)
            "calendar_features.json",   # upcoming_holidays empty<->populated (06-14)
        })

    def test_default_volatile_set_partitions_all_production_feeds(self):
        """With no explicit set, the helper uses the production
        VOLATILE_SHAPE_FEEDS — every member partitions to volatile, and a
        non-member partitions to enforced."""
        changed = [self._c(f) for f in sorted(detect_schema_drift.VOLATILE_SHAPE_FEEDS)]
        changed.append(self._c("gas_storage.json"))  # non-volatile control
        volatile, enforced = detect_schema_drift._partition_within_feed_drift(changed)
        assert set(c["feed"] for c in volatile) == set(
            detect_schema_drift.VOLATILE_SHAPE_FEEDS
        )
        assert [c["feed"] for c in enforced] == ["gas_storage.json"]

    def test_critical_and_volatile_sets_are_disjoint(self):
        """A feed must never be both critical and volatile (contradictory
        signals). The module also asserts this at import time."""
        assert detect_schema_drift.CRITICAL_FEEDS.isdisjoint(
            detect_schema_drift.VOLATILE_SHAPE_FEEDS
        )


class TestDeriveVolatileFeeds:
    """Unit tests for the history-derived volatility classifier — the
    self-maintaining mechanism that replaces hand-editing the allowlist."""

    def test_feed_with_two_hashes_same_version_is_volatile(self, tmp_path):
        """A feed that shows >1 shape_hash at the SAME schema_version in
        committed history is data-driven churn → volatile. A feed constant
        across history is not."""
        committed = [
            _sidecar("2.4", {"flapper.json": "A", "steady.json": "S"}),
            _sidecar("2.4", {"flapper.json": "B", "steady.json": "S"}),
            _sidecar("2.4", {"flapper.json": "A", "steady.json": "S"}),
        ]
        repo = _make_repo_with_commit_history(
            tmp_path, committed, committed[-1], copy_code=False
        )
        derived = detect_schema_drift.derive_volatile_feeds(
            "data/_shape_signatures.json", "HEAD", 10, repo_root=repo
        )
        assert "flapper.json" in derived
        assert "steady.json" not in derived

    def test_versioned_change_is_not_volatile(self, tmp_path):
        """A feed whose hash changed ACROSS a schema_version bump is a
        legitimate versioned migration, not churn — must NOT be classified
        volatile (else it would mask a later real break)."""
        committed = [
            _sidecar("2.3", {"migrated.json": "old"}),
            _sidecar("2.4", {"migrated.json": "new"}),  # changed WITH bump
        ]
        repo = _make_repo_with_commit_history(
            tmp_path, committed, committed[-1], copy_code=False
        )
        derived = detect_schema_drift.derive_volatile_feeds(
            "data/_shape_signatures.json", "HEAD", 10, repo_root=repo
        )
        assert "migrated.json" not in derived

    def test_git_failure_degrades_to_empty_set(self, tmp_path):
        """A non-repo directory (git failure) returns an empty set rather
        than raising — caller falls back to the declared set."""
        plain = tmp_path / "notarepo"
        plain.mkdir()
        derived = detect_schema_drift.derive_volatile_feeds(
            "data/_shape_signatures.json", "HEAD", 10, repo_root=plain
        )
        assert derived == frozenset()


class TestHistoryDerivedEndToEnd:
    """End-to-end: the script auto-classifies a history-volatile feed via the
    CLI without it being in the declared VOLATILE_SHAPE_FEEDS."""

    def test_history_volatile_undeclared_feed_warns(self, tmp_path):
        """A feed NOT in VOLATILE_SHAPE_FEEDS that wobbled in committed history
        is auto-derived volatile → drift warns, exit 0."""
        base = lambda h: _sidecar("2.4", {"flapper.json": h, "steady.json": "S"})
        # flapper shows both A and B in history (c0,c1) → derived volatile.
        committed = [base("A"), base("B"), base("A"), base("A")]
        working = base("B")  # current run: flapper A->B vs HEAD~1
        repo = _make_repo_with_commit_history(tmp_path, committed, working)
        result = _run_script(repo)  # default --previous-ref HEAD~1
        assert result.returncode == 0, result.stdout + result.stderr
        assert "flapper.json" in result.stdout
        assert "Auto-classified" in result.stdout
        assert "::error::" not in result.stdout

    def test_history_stable_feed_real_break_still_fails(self, tmp_path):
        """A feed constant across all history that suddenly changes is a real
        break, not churn → enforced, exit 1 (derivation must not mask it)."""
        base = lambda h: _sidecar("2.4", {"newcomer.json": h, "steady.json": "S"})
        committed = [base("X"), base("X"), base("X"), base("X")]
        working = base("Y")  # newcomer X->Y, never varied before
        repo = _make_repo_with_commit_history(tmp_path, committed, working)
        result = _run_script(repo)
        assert result.returncode == 1, result.stdout + result.stderr
        assert "newcomer.json" in result.stdout
        assert "::error::" in result.stdout

    def test_volatility_window_zero_disables_derivation(self, tmp_path):
        """--volatility-window 0 turns off history derivation; a feed volatile
        only by history (not declared) then fails as an enforced change."""
        base = lambda h: _sidecar("2.4", {"flapper.json": h, "steady.json": "S"})
        committed = [base("A"), base("B"), base("A"), base("A")]
        working = base("B")
        repo = _make_repo_with_commit_history(tmp_path, committed, working)
        result = _run_script(repo, "--volatility-window", "0")
        assert result.returncode == 1, result.stdout + result.stderr
        assert "::error::" in result.stdout
