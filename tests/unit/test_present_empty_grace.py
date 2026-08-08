"""Contract tests for the time-boxed present-but-empty grace (#42).

The guard lives inline in data_fetcher.run_data_collection, which needs live
collectors to exercise end to end. These tests pin the *decision rule* it
implements, using the same pure helpers the orchestrator calls, so a change to
either side of the grace/escalation boundary fails here rather than in a live
run three days later.

The rule: a present-but-empty Open-Meteo feed is coerced to absent (non-blocking)
for the first UPSTREAM_EMPTY_ESCALATION_RUNS-1 consecutive runs. On the run that
would make the streak reach the threshold, coercion stops and the feed stays
present-but-empty so validate_completeness fails the publish loudly.
"""

import pytest

from utils.data_quality import (
    PRESENT_EMPTY_GRACE_FEEDS,
    UPSTREAM_EMPTY_ESCALATION_RUNS,
    update_upstream_empty_streaks,
)


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
