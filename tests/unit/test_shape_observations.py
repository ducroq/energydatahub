"""Tests for the shape-observation learning record (#43).

The defect this closes: `derive_volatile_feeds()` learned from the committed
history of `data/_shape_signatures.json`, but the drift tripwire runs BEFORE the
commit step, so a run the tripwire failed committed nothing and taught the
classifier nothing. `ned_production` and `wind_forecast` failed on 2026-08-03
and were still unclassified five days later — the classifier could only ever
learn from its own near-misses.

The fix splits the two roles the sidecar was serving. These tests pin the split,
because the whole thing is worthless if the observation log ever becomes a
baseline or the baseline ever becomes the log.
"""

import json

import pytest

from utils.shape_signature import (
    OBSERVATIONS_KEEP_LINES,
    append_shape_observation,
    load_shape_observations,
    observation_from_sidecar,
    volatile_feeds_from_observations,
)


def sidecar(version="2.4", **feeds):
    return {
        "computed_at": "2026-08-08T16:00:00+02:00",
        "schema_version": version,
        "feeds": {
            name: {"shape_hash": h, "shape_signature": {"_kind": "dict", "keys": {}}}
            for name, h in feeds.items()
        },
    }


class TestObservationRecord:
    def test_keeps_hashes_and_drops_the_bulky_signature(self):
        rec = observation_from_sidecar(sidecar(a="h1", b="h2"))
        assert rec["feeds"] == {"a": "h1", "b": "h2"}
        assert "shape_signature" not in json.dumps(rec), (
            "the full signature is kilobytes per feed and is never consulted "
            "for volatility — storing it would bloat a committed file"
        )

    def test_carries_the_schema_version(self):
        """Volatility is 'changed at a FIXED version', so the version is load-bearing."""
        assert observation_from_sidecar(sidecar(version="2.5", a="h"))["schema_version"] == "2.5"

    def test_feeds_without_a_hash_are_skipped(self):
        s = sidecar(a="h1")
        s["feeds"]["broken"] = {"shape_signature": {}}
        assert observation_from_sidecar(s)["feeds"] == {"a": "h1"}


class TestAppendAndLoad:
    def test_round_trip(self, tmp_path):
        p = str(tmp_path / "obs.jsonl")
        append_shape_observation(p, sidecar(a="h1"))
        append_shape_observation(p, sidecar(a="h2"))
        recs = load_shape_observations(p)
        assert [r["feeds"]["a"] for r in recs] == ["h1", "h2"]

    def test_appends_rather_than_overwrites(self, tmp_path):
        p = str(tmp_path / "obs.jsonl")
        for i in range(5):
            append_shape_observation(p, sidecar(a=f"h{i}"))
        assert len(load_shape_observations(p)) == 5

    def test_trims_to_the_retention_cap(self, tmp_path):
        p = str(tmp_path / "obs.jsonl")
        for i in range(12):
            append_shape_observation(p, sidecar(a=f"h{i}"), keep=10)
        recs = load_shape_observations(p)
        assert len(recs) == 10
        assert recs[-1]["feeds"]["a"] == "h11", "must keep the NEWEST, not the oldest"

    def test_corrupt_lines_are_skipped_not_fatal(self, tmp_path):
        p = tmp_path / "obs.jsonl"
        p.write_text('{"schema_version":"2.4","feeds":{"a":"h1"}}\nNOT JSON\n{"bad":1}\n')
        recs = load_shape_observations(str(p))
        assert len(recs) == 1, "a corrupt learning record must degrade, not crash a run"

    def test_missing_file_loads_empty_and_appends_cleanly(self, tmp_path):
        p = str(tmp_path / "nope.jsonl")
        assert load_shape_observations(p) == []
        append_shape_observation(p, sidecar(a="h1"))
        assert len(load_shape_observations(p)) == 1

    def test_append_survives_a_corrupt_existing_file(self, tmp_path):
        p = tmp_path / "obs.jsonl"
        p.write_text("GARBAGE\n")
        append_shape_observation(str(p), sidecar(a="h1"))
        assert load_shape_observations(str(p))[-1]["feeds"] == {"a": "h1"}

    def test_default_cap_is_bounded(self):
        assert 0 < OBSERVATIONS_KEEP_LINES <= 2000, "committed file must stay bounded"


class TestVolatilityDerivation:
    def test_two_hashes_at_one_version_is_volatile(self):
        obs = [
            observation_from_sidecar(sidecar(a="h1")),
            observation_from_sidecar(sidecar(a="h2")),
        ]
        assert volatile_feeds_from_observations(obs) == frozenset({"a"})

    def test_stable_feed_is_not_volatile(self):
        obs = [observation_from_sidecar(sidecar(a="h1"))] * 5
        assert volatile_feeds_from_observations(obs) == frozenset()

    def test_versioned_migration_is_not_volatile(self):
        """A real migration moves schema_version, so its hashes land under
        different versions and must NOT be mistaken for data-driven churn —
        otherwise a genuine bump would permanently excuse the feed."""
        obs = [
            observation_from_sidecar(sidecar(version="2.4", a="h1")),
            observation_from_sidecar(sidecar(version="2.5", a="h2")),
        ]
        assert volatile_feeds_from_observations(obs) == frozenset()

    def test_window_bounds_the_lookback(self):
        obs = [
            observation_from_sidecar(sidecar(a="old")),
            observation_from_sidecar(sidecar(a="h1")),
            observation_from_sidecar(sidecar(a="h1")),
        ]
        assert volatile_feeds_from_observations(obs, window=2) == frozenset()
        assert volatile_feeds_from_observations(obs, window=3) == frozenset({"a"})

    def test_the_defect_this_closes(self):
        """The 2026-08-03 scenario: a feed drifts on a run that FAILS the gate.

        Pre-#43 that run committed nothing, so the drifted hash never entered
        history and the feed stayed unclassified forever. The observation log is
        written regardless of outcome, so two PRIOR occurrences now classify it.
        """
        obs = [
            observation_from_sidecar(sidecar(ned_production="stable")),
            # this run failed the tripwire — recorded anyway
            observation_from_sidecar(sidecar(ned_production="drifted")),
        ]
        assert "ned_production" in volatile_feeds_from_observations(obs)


class TestCurrentRunExclusion:
    """The classifier must never count the run it is judging as evidence.

    `data_fetcher` appends the current record BEFORE the tripwire reads the
    file, so without exclusion a feed's FIRST EVER break supplies its own
    second hash, self-classifies as volatile, and is downgraded from ::error::
    to ::warning::. Reproduced on the real 75-record log against
    `load_forecast` (a CRITICAL_FEED with one hash across every run) — it
    flipped the verdict to exit 0. This turns the gate into a rubber stamp, so
    these tests guard the difference between a check and a no-op.
    """

    @staticmethod
    def _log(*hashes, current):
        recs = []
        for i, h in enumerate(hashes):
            r = observation_from_sidecar(sidecar(feed=h))
            r["observed_at"] = f"run-{i}"
            recs.append(r)
        cur = observation_from_sidecar(sidecar(feed=current))
        cur["observed_at"] = "run-CURRENT"
        return recs + [cur]

    def test_first_ever_break_does_not_self_classify(self):
        log = self._log("A", "A", "A", current="BREAK")
        assert "feed" in volatile_feeds_from_observations(log), (
            "sanity: without exclusion the break self-classifies"
        )
        assert "feed" not in volatile_feeds_from_observations(
            log, exclude_observed_at="run-CURRENT"
        ), "a feed's first break must NOT excuse itself"

    def test_prior_churn_still_classifies(self):
        """The exclusion must not break the feature it protects."""
        log = self._log("A", "B", "A", current="B")
        assert "feed" in volatile_feeds_from_observations(
            log, exclude_observed_at="run-CURRENT"
        ), "churn evidenced by PRIOR runs must still classify"

    def test_exclusion_is_by_timestamp_not_position(self):
        log = self._log("A", "A", current="BREAK")
        log.append(log[0])  # a duplicate appended after the current record
        assert "feed" not in volatile_feeds_from_observations(
            log, exclude_observed_at="run-CURRENT"
        )

    def test_no_exclusion_argument_preserves_old_behaviour(self):
        """Callers that are not judging a current run (backfills, analysis)
        must still see every record."""
        log = self._log("A", current="B")
        assert "feed" in volatile_feeds_from_observations(log)
