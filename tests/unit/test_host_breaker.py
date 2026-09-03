"""
Unit Tests for the shared per-host circuit breaker (#52)
--------------------------------------------------------
Covers `collectors/_host_breaker.py` and its integration into
`BaseCollector._retry_single`.

The defect under test: `data_fetcher` builds eight ENTSO-E collectors against
one host, each with its own per-instance breaker consulted once per
`collect()`, so no breaker ever reaches its threshold during a host-wide
outage. The 2026-08-31 run made ~295 requests into an API returning 503 to
every one. The regression that matters most is therefore
`test_failures_on_one_collector_open_the_breaker_for_its_sibling` — state must
cross instance boundaries, which is the whole point of the module.

File: tests/unit/test_host_breaker.py
Created: 2026-09-02
"""

import pytest

from collectors._host_breaker import (
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_FAILURE_THRESHOLD,
    HostBreaker,
    all_breakers,
    get_host_breaker,
    reset_all,
)
from collectors.base import BaseCollector, NonRetryableError


@pytest.fixture(autouse=True)
def clean_registry():
    """Breaker state is process-wide; keep tests independent of each other."""
    reset_all()
    yield
    reset_all()


class FakeClock:
    """Controllable monotonic source, so cooldowns need no real sleeping."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make_breaker(clock=None, **kwargs):
    return HostBreaker(
        host="example.test",
        time_source=clock or FakeClock(),
        **kwargs,
    )


class TestOpening:
    """Failure accounting and the threshold."""

    def test_starts_closed_and_allows(self):
        breaker = make_breaker()
        assert not breaker.is_open
        assert breaker.allow() is True

    def test_opens_only_at_threshold(self):
        breaker = make_breaker()
        for _ in range(DEFAULT_FAILURE_THRESHOLD - 1):
            breaker.record_failure()
        assert not breaker.is_open, "opened before reaching the threshold"

        breaker.record_failure()
        assert breaker.is_open
        assert breaker.allow() is False

    def test_success_resets_the_counter(self):
        """A partially-healthy host must never trip the breaker."""
        breaker = make_breaker()
        for _ in range(DEFAULT_FAILURE_THRESHOLD - 1):
            breaker.record_failure()
        breaker.record_success()
        for _ in range(DEFAULT_FAILURE_THRESHOLD - 1):
            breaker.record_failure()

        assert not breaker.is_open
        assert breaker.allow() is True

    def test_suppressed_requests_are_counted(self):
        breaker = make_breaker()
        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            breaker.record_failure()

        for _ in range(3):
            breaker.allow()

        assert breaker.snapshot()["suppressed_requests"] == 3
        assert breaker.snapshot()["open_events"] == 1


class TestProbing:
    """The half-open probe — the guarantee that a run can still recover."""

    def test_one_probe_per_cooldown(self):
        clock = FakeClock()
        breaker = make_breaker(clock)
        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            breaker.record_failure()

        assert breaker.allow() is False
        clock.advance(DEFAULT_COOLDOWN_SECONDS + 1)

        assert breaker.allow() is True, "cooldown elapsed but no probe allowed"
        assert breaker.allow() is False, "second concurrent caller got through"
        assert breaker.allow() is False

    def test_probe_success_closes_the_breaker(self):
        clock = FakeClock()
        breaker = make_breaker(clock)
        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            breaker.record_failure()
        clock.advance(DEFAULT_COOLDOWN_SECONDS + 1)
        assert breaker.allow() is True

        breaker.record_success()

        assert not breaker.is_open
        assert breaker.allow() is True
        assert breaker.snapshot()["consecutive_failures"] == 0

    def test_probe_failure_reopens_for_another_cooldown(self):
        clock = FakeClock()
        breaker = make_breaker(clock)
        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            breaker.record_failure()
        clock.advance(DEFAULT_COOLDOWN_SECONDS + 1)
        assert breaker.allow() is True

        breaker.record_failure()

        assert breaker.is_open
        assert breaker.allow() is False
        clock.advance(DEFAULT_COOLDOWN_SECONDS + 1)
        assert breaker.allow() is True, "breaker wedged open after a failed probe"

    def test_retry_rounds_always_get_a_fresh_probe(self):
        """The orchestrator sleeps 300s between rounds; the breaker must not
        outlive that gap, or the load-bearing retry rounds become no-ops."""
        clock = FakeClock()
        breaker = make_breaker(clock)
        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            breaker.record_failure()

        clock.advance(300)

        assert breaker.allow() is True


class TestRegistry:
    def test_same_host_shares_one_instance(self):
        assert get_host_breaker("a.test") is get_host_breaker("a.test")

    def test_different_hosts_are_isolated(self):
        a, b = get_host_breaker("a.test"), get_host_breaker("b.test")
        assert a is not b
        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            a.record_failure()
        assert a.is_open
        assert not b.is_open

    def test_all_breakers_reports_created_hosts(self):
        get_host_breaker("a.test")
        get_host_breaker("b.test")
        assert set(all_breakers()) == {"a.test", "b.test"}


class BreakerCollector(BaseCollector):
    """Minimal collector used to drive `_retry_single` directly."""

    def __init__(self, **kwargs):
        super().__init__(
            name="BreakerCollector",
            data_type="test",
            source="Test API",
            units="test",
            **kwargs,
        )

    async def _fetch_raw_data(self, start_time, end_time, **kwargs):
        return {}

    def _parse_response(self, raw_data, start_time, end_time):
        return {}


class TestRetrySingleIntegration:
    """`_retry_single` is the choke point every ENTSO-E sub-request passes."""

    @pytest.mark.asyncio
    async def test_exhausted_subrequest_counts_once_not_per_attempt(self):
        collector = BreakerCollector(host_breaker_key="example.test")
        calls = []

        def always_fails():
            calls.append(1)
            raise ValueError("boom")

        await collector._retry_single(always_fails, max_attempts=3, initial_delay=0)

        assert len(calls) == 3, "expected all attempts to run"
        breaker = get_host_breaker("example.test")
        assert breaker.snapshot()["consecutive_failures"] == 1, (
            "one exhausted sub-request must count once, not once per attempt"
        )

    @pytest.mark.asyncio
    async def test_open_breaker_short_circuits_without_calling_func(self):
        collector = BreakerCollector(host_breaker_key="example.test")
        breaker = get_host_breaker("example.test")
        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            breaker.record_failure()

        calls = []

        def tracked():
            calls.append(1)
            return "data"

        result = await collector._retry_single(tracked, max_attempts=3, initial_delay=0)

        assert result is None
        assert calls == [], "made a request while the host breaker was open"

    @pytest.mark.asyncio
    async def test_success_records_success(self):
        collector = BreakerCollector(host_breaker_key="example.test")
        breaker = get_host_breaker("example.test")
        breaker.record_failure()
        breaker.record_failure()

        result = await collector._retry_single(lambda: "data", initial_delay=0)

        assert result == "data"
        assert breaker.snapshot()["consecutive_failures"] == 0

    @pytest.mark.asyncio
    async def test_non_retryable_error_is_not_a_host_failure(self):
        """A permanent 4xx for one query says nothing about host health."""
        collector = BreakerCollector(host_breaker_key="example.test")

        def permanent():
            raise NonRetryableError("422 Unprocessable Entity")

        await collector._retry_single(permanent, max_attempts=2, initial_delay=0)

        breaker = get_host_breaker("example.test")
        assert breaker.snapshot()["consecutive_failures"] == 0

    @pytest.mark.asyncio
    async def test_collector_without_key_is_unaffected(self):
        """Opt-in: collectors that do not share a host behave exactly as before."""
        collector = BreakerCollector()
        calls = []

        def always_fails():
            calls.append(1)
            raise ValueError("boom")

        for _ in range(DEFAULT_FAILURE_THRESHOLD + 2):
            await collector._retry_single(always_fails, max_attempts=1, initial_delay=0)

        assert len(calls) == DEFAULT_FAILURE_THRESHOLD + 2
        assert all_breakers() == {}, "created a breaker for an opted-out collector"

    @pytest.mark.asyncio
    async def test_failures_on_one_collector_open_the_breaker_for_its_sibling(self):
        """The #52 regression: eight instances, one host, one breaker.

        Per-instance state cannot see a host-wide outage. This fails against
        the old code, where each collector carried its own breaker.
        """
        first = BreakerCollector(host_breaker_key="shared.test")
        second = BreakerCollector(host_breaker_key="shared.test")

        def always_fails():
            raise ValueError("503 Service Unavailable")

        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            await first._retry_single(always_fails, max_attempts=1, initial_delay=0)

        sibling_calls = []

        def tracked():
            sibling_calls.append(1)
            return "data"

        result = await second._retry_single(tracked, max_attempts=3, initial_delay=0)

        assert result is None
        assert sibling_calls == [], (
            "sibling collector hit the host after a shared breaker opened"
        )


class TestNonHostExceptions:
    """A healthy host with empty windows must never trip the breaker.

    This is the blocker the #52 review caught: entsoe-py's
    `NoMatchingDataError` is a plain `Exception`, so before this it counted as
    a host failure. The NL cable borders are routinely unpublished, so a
    handful of them could open the breaker on a working host and suppress
    every remaining ENTSO-E request in the process.
    """

    @pytest.mark.asyncio
    async def test_empty_window_does_not_count_as_host_failure(self):
        from entsoe.exceptions import NoMatchingDataError

        from collectors._entsoe_shared import ENTSOE_BENIGN_EXCEPTIONS

        collector = BreakerCollector(host_breaker_key="example.test")

        def empty_window():
            raise NoMatchingDataError("no rows for this window")

        for _ in range(DEFAULT_FAILURE_THRESHOLD + 3):
            await collector._retry_single(
                empty_window, max_attempts=3, initial_delay=0,
                non_host_exceptions=ENTSOE_BENIGN_EXCEPTIONS,
            )

        breaker = get_host_breaker("example.test")
        assert breaker.snapshot()["consecutive_failures"] == 0
        assert not breaker.is_open, (
            "a healthy host with empty publication windows opened the breaker"
        )

    @pytest.mark.asyncio
    async def test_empty_window_stops_after_one_attempt(self):
        from entsoe.exceptions import NoMatchingDataError

        from collectors._entsoe_shared import ENTSOE_BENIGN_EXCEPTIONS

        collector = BreakerCollector(host_breaker_key="example.test")
        calls = []

        def empty_window():
            calls.append(1)
            raise NoMatchingDataError("no rows for this window")

        await collector._retry_single(
            empty_window, max_attempts=3, initial_delay=0,
            non_host_exceptions=ENTSOE_BENIGN_EXCEPTIONS,
        )

        assert calls == [1], "burned retries on a window that cannot change"

    @pytest.mark.asyncio
    async def test_failed_probe_is_recorded_through_the_integration(self):
        """`record_failure`'s failed-probe branch must be reachable from
        `_retry_single`, not only by calling the method directly."""
        clock = FakeClock()
        breaker = HostBreaker(host="example.test", time_source=clock)
        import collectors._host_breaker as hb
        hb._BREAKERS["example.test"] = breaker

        collector = BreakerCollector(host_breaker_key="example.test")
        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            breaker.record_failure()
        clock.advance(DEFAULT_COOLDOWN_SECONDS + 1)

        before = breaker.snapshot()["consecutive_failures"]

        def always_fails():
            raise ValueError("503")

        await collector._retry_single(always_fails, max_attempts=3, initial_delay=0)

        assert breaker.snapshot()["consecutive_failures"] == before + 1, (
            "a failed probe recorded no failure — the refusal path returned "
            "before record_failure()"
        )


class TestEntsoeWiring:
    """Every ENTSO-E collector must name the host AND actually consult it."""

    def _collectors(self):
        from collectors.entsoe import EntsoeCollector
        from collectors.entsoe_flows import EntsoeFlowsCollector
        from collectors.entsoe_generation import EntsoeGenerationCollector
        from collectors.entsoe_hydro import EntsoeHydroCollector
        from collectors.entsoe_load import EntsoeLoadCollector
        from collectors.entsoe_wind import EntsoeWindCollector

        return [
            EntsoeCollector(api_key="k"),
            EntsoeFlowsCollector(api_key="k"),
            EntsoeGenerationCollector(api_key="k"),
            EntsoeHydroCollector(api_key="k"),
            EntsoeLoadCollector(api_key="k"),
            EntsoeWindCollector(api_key="k"),
        ]

    def test_all_entsoe_collectors_share_one_host_key(self):
        from collectors._entsoe_shared import ENTSOE_API_HOST

        assert {c.host_breaker_key for c in self._collectors()} == {ENTSOE_API_HOST}

    @pytest.mark.asyncio
    async def test_price_collector_actually_suppresses_when_open(self):
        """The attribute check above is not enough — it was green while
        `EntsoeCollector` never consulted the breaker at all, because it makes
        one request per fetch and so never calls `_retry_single`. That gap hid
        the *critical* feed, the only one that decides whether the publish
        happens. Assert behaviour, not wiring.
        """
        from datetime import datetime, timedelta
        from unittest.mock import patch
        from zoneinfo import ZoneInfo

        from collectors._entsoe_shared import ENTSOE_API_HOST
        from collectors._host_breaker import HostBreakerOpenError
        from collectors.entsoe import EntsoeCollector

        breaker = get_host_breaker(ENTSOE_API_HOST)
        for _ in range(DEFAULT_FAILURE_THRESHOLD):
            breaker.record_failure()
        assert breaker.is_open

        collector = EntsoeCollector(api_key="k")
        tz = ZoneInfo("Europe/Amsterdam")
        start = datetime.now(tz)

        with patch(
            "collectors.entsoe.EntsoePandasClient.query_day_ahead_prices"
        ) as query:
            with pytest.raises(HostBreakerOpenError):
                await collector._fetch_raw_data(
                    start, start + timedelta(days=1), country_code="NL"
                )

        assert query.call_count == 0, (
            "price collector hit the host while the shared breaker was open"
        )
        assert breaker.snapshot()["suppressed_requests"] >= 1
