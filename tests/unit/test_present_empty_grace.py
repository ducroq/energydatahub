"""Contract tests for the time-boxed present-but-empty grace (#42).

The guard lives inline in data_fetcher.main, which needs live collectors to
exercise end to end. These tests pin the *decision rule* it implements, using
the same pure helpers the orchestrator calls, so a change to either side of the
grace/escalation boundary fails here rather than in a live run three days later.

The rule: a present-but-empty graced feed is coerced to absent (non-blocking)
for the first UPSTREAM_EMPTY_ESCALATION_RUNS-1 consecutive runs. On the run that
would make the streak reach the threshold, coercion stops and the feed stays
present-but-empty so validate_completeness fails the publish loudly.
"""

import pathlib

import pytest

from utils.data_quality import (
    PRESENT_EMPTY_GRACE_FEEDS,
    UPSTREAM_EMPTY_ESCALATION_RUNS,
    present_empty_feeds,
    update_upstream_empty_streaks,
)
from utils.data_types import EnhancedDataSet


def past_grace(prior_streaks, present_empty_now):
    """The orchestrator's rule, verbatim: this run's streak reaching the
    threshold means the grace is exhausted and the feed must NOT be coerced."""
    return {
        name for name in present_empty_now
        if int(prior_streaks.get(name, 0)) + 1 >= UPSTREAM_EMPTY_ESCALATION_RUNS
    }


class TestGraceBoundary:
    def test_first_empty_run_is_coerced(self):
        empty = {'offshore_wind'}
        assert past_grace({}, empty) == set(), "a single transient must not fail the publish"

    def test_grace_runs_out_exactly_at_the_threshold(self):
        empty = {'offshore_wind'}
        # streak 1 -> 2 : still within grace (threshold is 3)
        assert past_grace({'offshore_wind': 1}, empty) == set()
        # streak 2 -> 3 : reaches the threshold, grace exhausted
        assert past_grace({'offshore_wind': 2}, empty) == empty

    def test_beyond_the_threshold_stays_escalated(self):
        empty = {'offshore_wind'}
        assert past_grace({'offshore_wind': 9}, empty) == empty

    def test_threshold_is_honoured_not_hardcoded(self):
        """Guards against someone changing the constant and leaving a literal 3."""
        prior = {'offshore_wind': UPSTREAM_EMPTY_ESCALATION_RUNS - 2}
        assert past_grace(prior, {'offshore_wind'}) == set()
        prior = {'offshore_wind': UPSTREAM_EMPTY_ESCALATION_RUNS - 1}
        assert past_grace(prior, {'offshore_wind'}) == {'offshore_wind'}


class TestStreakBookkeeping:
    def test_recovery_resets_the_streak(self):
        streaks = update_upstream_empty_streaks(
            {'offshore_wind': 2}, set(), PRESENT_EMPTY_GRACE_FEEDS
        )
        assert streaks['offshore_wind'] == 0, "one healthy run must restore full grace"

    def test_streak_advances_only_for_empty_feeds(self):
        streaks = update_upstream_empty_streaks(
            {'offshore_wind': 1, 'solar_forecast': 1},
            {'offshore_wind'},
            PRESENT_EMPTY_GRACE_FEEDS,
        )
        assert streaks['offshore_wind'] == 2
        assert streaks['solar_forecast'] == 0

    def test_price_feed_keys_do_not_collide(self):
        """#38 and #42 counters share one sidecar; the two key spaces are disjoint."""
        assert not set(PRESENT_EMPTY_GRACE_FEEDS) & {'entsoe', 'entsoe_de'}

    def test_all_graced_feeds_are_tracked(self):
        streaks = update_upstream_empty_streaks({}, set(), PRESENT_EMPTY_GRACE_FEEDS)
        assert set(streaks) == set(PRESENT_EMPTY_GRACE_FEEDS)


class TestRegistryConsistency:
    """The parallel-registry rule: this list is keyed on dataset names that must
    exist in the quality registry, or the grace silently applies to nothing."""

    def test_graced_feeds_are_real_quality_datasets(self):
        from utils.data_quality import EXPECTED_DATA_TYPE
        for feed in PRESENT_EMPTY_GRACE_FEEDS:
            assert feed in EXPECTED_DATA_TYPE, (
                f"{feed} is graced but absent from EXPECTED_DATA_TYPE — either a "
                f"typo or a feed that never reaches the quality gate"
            )

    def test_buurt_feeds_retained_from_the_original_guard(self):
        """2026-07-07 shipped buurt-only; generalising must not drop them."""
        assert 'weather_forecast_buurt' in PRESENT_EMPTY_GRACE_FEEDS
        assert 'solar_forecast_buurt' in PRESENT_EMPTY_GRACE_FEEDS

    def test_ned_production_is_graced(self):
        """Run 34392331572 (2026-09-09): all six NED.nl fetches timed out,
        collectors/ned.py swallowed each one, and the empty envelope failed
        completeness and aborted the publish of 19 healthy feeds."""
        assert 'ned_production' in PRESENT_EMPTY_GRACE_FEEDS

    def test_graced_feeds_have_an_explicit_missing_severity(self):
        """A graced feed spends its grace window absent from the publish, and
        docs/ then re-serves yesterday's file unchanged — so the quality report
        is the ONLY place the degradation is recorded. Falling through to the
        implicit default would make a graced run indistinguishable from a
        healthy one there too."""
        from utils.data_quality import DATASET_MISSING_SEVERITY
        for feed in PRESENT_EMPTY_GRACE_FEEDS:
            assert feed in DATASET_MISSING_SEVERITY, (
                f"{feed} is graced but has no declared missing-severity"
            )


class TestTheEmptinessPredicate:
    """The predicate is the whole grace. Registry membership decides nothing if
    `present_empty_feeds` does not classify the feed's real empty envelope as
    empty — which is exactly how the ned_production registration shipped dead on
    2026-09-10, with the full suite green, because every test asserted the
    wiring and none built an envelope.

    Each case below is the shape the collector ACTUALLY produces on a total
    upstream failure, not a stand-in.
    """

    def _ds(self, data):
        return EnhancedDataSet(metadata={'data_type': 'test'}, data=data)

    def test_ned_total_outage_shape_is_detected(self):
        """collectors/ned.py:255 assigns `parsed[energy_type] = {}` for every
        configured type BEFORE it knows whether anything parsed, so a total
        NED.nl outage yields a TRUTHY dict of empty dicts. Run 34392331572.
        A `not ds.data` predicate returns False here and the grace never fires."""
        ned = self._ds({'solar': {}, 'wind_onshore': {}, 'wind_offshore': {}})
        assert bool(ned.data) is True, "precondition: the envelope is truthy"
        assert present_empty_feeds({'ned_production': ned}) == {'ned_production'}

    def test_openmeteo_total_outage_shape_is_detected(self):
        """The six Open-Meteo collectors gate the per-location assignment on
        `if location_data:`, so they really do collapse to {}. This is the shape
        truthiness happened to handle, which is what hid the flaw above."""
        om = self._ds({})
        assert present_empty_feeds({'solar_forecast': om}) == {'solar_forecast'}

    def test_partial_delivery_is_not_graced(self):
        """One surviving record means the feed is degraded, not empty. Coercing
        it would hide real data from the publish."""
        partial = self._ds({
            'solar': {'actual': {'2026-09-10T00:00:00+02:00': {'volume_kwh': 1}}},
            'wind_onshore': {}, 'wind_offshore': {},
        })
        assert present_empty_feeds({'ned_production': partial}) == set()

    def test_absent_feed_is_not_present_empty(self):
        """None is *absent* — it already routes through the non-blocking missing
        path, and calling it present-empty would advance a streak for a feed
        that never returned an envelope at all."""
        assert present_empty_feeds({'ned_production': None}) == set()

    def test_predicate_matches_the_gate_it_forestalls(self):
        """The guard exists to pre-empt validate_completeness. If the two
        disagree about what 'empty' means, the guard is answering a question
        nobody asked — the 2026-09-10 defect in one sentence."""
        from utils.data_quality import validate_completeness, Severity
        for shape in ({}, {'solar': {}, 'wind_onshore': {}, 'wind_offshore': {}}):
            graced = present_empty_feeds({'ned_production': self._ds(shape)})
            issues = validate_completeness(shape, 'ned_production')
            gate_fails = any(i.severity == Severity.CRITICAL for i in issues)
            assert bool(graced) == gate_fails, (
                f"guard and gate disagree on {shape!r}: "
                f"graced={bool(graced)}, gate_would_fail={gate_fails}"
            )


class TestOrchestratorWiring:
    """The registry says which feeds are graced; the orchestrator's
    `_grace_candidates` dict says which ones the guard can actually see. A feed
    in the first and not the second has a grace that is declared and never
    applied — silent, and the exact shape of the 2026-09-10 defect.

    This reads the dict LITERAL out of the AST rather than grepping the source.
    The first version of this test did grep, and false-passed: `'ned_production':`
    also occurs in the unrelated `quality_datasets` map further down `main()`, so
    deleting the candidate entry left the test green.
    """

    def _candidate_keys(self):
        import ast
        import data_fetcher
        tree = ast.parse(pathlib.Path(data_fetcher.__file__).read_text())
        for node in ast.walk(tree):
            if (isinstance(node, ast.Assign)
                    and any(getattr(t, 'id', None) == '_grace_candidates'
                            for t in node.targets)
                    and isinstance(node.value, ast.Dict)):
                return {k.value for k in node.value.keys}
        raise AssertionError(
            "no `_grace_candidates = {...}` dict literal in data_fetcher — the "
            "guard was renamed or restructured; re-point this test at it"
        )

    def test_candidate_map_matches_the_registry_exactly(self):
        assert self._candidate_keys() == set(PRESENT_EMPTY_GRACE_FEEDS)
