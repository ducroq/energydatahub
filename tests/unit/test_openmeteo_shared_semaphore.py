"""
Issue #11: All OpenMeteo* collectors must share a single rate-limit budget.

Before this change each collector owned its own ``asyncio.Semaphore(1)``,
so the effective concurrency budget against Open-Meteo's API was
``n_collectors × 1``. That number silently drifted every time a collector
was added — exactly how the 2026-06-05 buurt additions broke the previous
``Semaphore(2)+0.1s`` budget (CI run 27068482501, HTTP 429 storm).

These tests pin the invariant that the semaphore is module-level shared
state, so a 7th OpenMeteo collector cannot regress the budget without
deliberate edits to ``collectors/_openmeteo_shared.py``.

File: tests/unit/test_openmeteo_shared_semaphore.py
Created: 2026-06-06
"""

import asyncio
import pytest

from collectors import _openmeteo_shared
from collectors import openmeteo_weather, openmeteo_solar, openmeteo_offshore_wind


class TestSharedSemaphoreIdentity:
    """The same Semaphore *object* must be referenced from every collector."""

    def test_weather_uses_shared_semaphore(self):
        assert (
            openmeteo_weather.OPENMETEO_SEMAPHORE
            is _openmeteo_shared.OPENMETEO_SEMAPHORE
        )

    def test_solar_uses_shared_semaphore(self):
        assert (
            openmeteo_solar.OPENMETEO_SEMAPHORE
            is _openmeteo_shared.OPENMETEO_SEMAPHORE
        )

    def test_offshore_wind_uses_shared_semaphore(self):
        assert (
            openmeteo_offshore_wind.OPENMETEO_SEMAPHORE
            is _openmeteo_shared.OPENMETEO_SEMAPHORE
        )

    def test_all_three_collectors_share_same_object(self):
        """Belt-and-suspenders: pairwise identity across all three."""
        assert (
            openmeteo_weather.OPENMETEO_SEMAPHORE
            is openmeteo_solar.OPENMETEO_SEMAPHORE
            is openmeteo_offshore_wind.OPENMETEO_SEMAPHORE
        )

    def test_all_three_collectors_share_gap_constant(self):
        assert (
            openmeteo_weather.OPENMETEO_GAP_SECONDS
            == openmeteo_solar.OPENMETEO_GAP_SECONDS
            == openmeteo_offshore_wind.OPENMETEO_GAP_SECONDS
            == _openmeteo_shared.OPENMETEO_GAP_SECONDS
        )


class TestSharedSemaphoreShape:
    """The shared semaphore must have the documented cap."""

    def test_semaphore_is_an_asyncio_semaphore(self):
        assert isinstance(_openmeteo_shared.OPENMETEO_SEMAPHORE, asyncio.Semaphore)

    def test_cap_equals_collector_count(self):
        """6 = matches the 6 OpenMeteo collectors in data_fetcher.py.

        Bumped from 5 → 6 after the 2026-06-07 timeout regression: a cap
        below collector count caused late-scheduled collectors (offshore
        + buurt) to time out on Open-Meteo's CDN per-source cooldown
        window while early strategic collectors monopolised the FIFO
        queue. cap == count restores the pre-#11 per-collector × Semaphore(1)
        effective behavior — parallel sessions instead of one serialised
        queue.
        """
        assert _openmeteo_shared.OPENMETEO_SEMAPHORE_CAP == 6

    def test_gap_is_set_to_documented_safe_budget(self):
        """0.5s = the inter-request gap from PR #10's fix."""
        assert _openmeteo_shared.OPENMETEO_GAP_SECONDS == 0.5


class TestFetchLocationWithRetry:
    """Per-location retry-with-backoff wrapper for OpenMeteo collectors.

    Belt-and-suspenders against transient timeouts/429/5xx — even after
    the cap bump fixed the root cause, a single bad response should
    self-heal without a full collection re-run.
    """

    @pytest.mark.asyncio
    async def test_success_on_first_attempt_no_retry(self):
        import logging
        from collectors._openmeteo_shared import fetch_location_with_retry

        call_count = 0

        async def fetch_fn(session, location):
            nonlocal call_count
            call_count += 1
            return {'name': location['name'], 'data': {'ok': True}, 'error': None}

        result = await fetch_location_with_retry(
            session=None,
            location={'name': 'L1'},
            fetch_fn=fetch_fn,
            logger=logging.getLogger('test'),
            apply_gap=False,
        )
        assert result['data'] == {'ok': True}
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_succeeds_after_transient_failures(self, monkeypatch):
        """Two failures then success → returns success, total 3 calls."""
        import logging
        from collectors import _openmeteo_shared
        # Speed test up: reduce backoff to ~0.
        monkeypatch.setattr(_openmeteo_shared, 'RETRY_INITIAL_DELAY_SECONDS', 0.0)

        call_count = 0

        async def fetch_fn(session, location):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return {'name': location['name'], 'data': None, 'error': 'timeout'}
            return {'name': location['name'], 'data': {'ok': True}, 'error': None}

        result = await _openmeteo_shared.fetch_location_with_retry(
            session=None,
            location={'name': 'L1'},
            fetch_fn=fetch_fn,
            logger=logging.getLogger('test'),
            apply_gap=False,
        )
        assert result['data'] == {'ok': True}
        assert call_count == 3  # used all 3 attempts (2 failed + 1 succeeded)

    @pytest.mark.asyncio
    async def test_returns_last_failure_after_all_retries(self, monkeypatch):
        import logging
        from collectors import _openmeteo_shared
        monkeypatch.setattr(_openmeteo_shared, 'RETRY_INITIAL_DELAY_SECONDS', 0.0)

        call_count = 0

        async def fetch_fn(session, location):
            nonlocal call_count
            call_count += 1
            return {'name': location['name'], 'data': None, 'error': f'attempt-{call_count}-failed'}

        result = await _openmeteo_shared.fetch_location_with_retry(
            session=None,
            location={'name': 'L_dead'},
            fetch_fn=fetch_fn,
            logger=logging.getLogger('test'),
            apply_gap=False,
        )
        assert result['data'] is None
        assert call_count == _openmeteo_shared.MAX_RETRIES
        # Error reflects the LAST attempt, not the first.
        assert 'attempt-3-failed' in result['error']

    @pytest.mark.asyncio
    async def test_exponential_backoff_delays(self, monkeypatch):
        """Backoff sequence: 1s, 2s, 4s, ... — measured by counting
        asyncio.sleep calls between attempts."""
        import logging
        from collectors import _openmeteo_shared

        sleep_durations = []
        original_sleep = asyncio.sleep

        async def fake_sleep(d):
            sleep_durations.append(d)
            await original_sleep(0)

        monkeypatch.setattr(asyncio, 'sleep', fake_sleep)

        async def always_fail(session, location):
            return {'name': location['name'], 'data': None, 'error': 'x'}

        await _openmeteo_shared.fetch_location_with_retry(
            session=None,
            location={'name': 'L1'},
            fetch_fn=always_fail,
            logger=logging.getLogger('test'),
            apply_gap=False,  # don't conflate with retry delays
        )

        # MAX_RETRIES attempts means MAX_RETRIES - 1 inter-attempt sleeps.
        backoff_sleeps = sleep_durations  # since apply_gap=False, all sleeps are retries
        expected = [
            _openmeteo_shared.RETRY_INITIAL_DELAY_SECONDS
            * (_openmeteo_shared.RETRY_BACKOFF_BASE ** i)
            for i in range(_openmeteo_shared.MAX_RETRIES - 1)
        ]
        assert backoff_sleeps == expected


class TestSharedSemaphoreEnforcesConcurrency:
    """Functional check: when ``cap + N`` tasks acquire concurrently, no more
    than ``cap`` may be in-flight at any moment.

    Uses a LOCAL semaphore (not the live module singleton) so a flaky test
    or KeyboardInterrupt can't leave the shared semaphore with a depressed
    counter and poison subsequent test runs (PR #18 review LOW).
    """

    @pytest.mark.asyncio
    async def test_concurrency_is_bounded_by_cap(self):
        # Construct a local semaphore that mirrors the shared one's cap.
        # Verifying the cap value separately (via TestSharedSemaphoreShape)
        # plus this behavior test together pin both "cap is N" and "the
        # semaphore actually enforces N" without poking at `._value`.
        cap = _openmeteo_shared.OPENMETEO_SEMAPHORE_CAP
        local_sem = asyncio.Semaphore(cap)

        in_flight = 0
        peak = 0

        async def worker():
            nonlocal in_flight, peak
            async with local_sem:
                in_flight += 1
                peak = max(peak, in_flight)
                # Yield once so other tasks have a chance to interleave and
                # exceed the cap if the semaphore is broken.
                await asyncio.sleep(0.01)
                in_flight -= 1

        await asyncio.gather(*(worker() for _ in range(cap + 2)))

        assert peak <= cap

    @pytest.mark.asyncio
    async def test_shared_singleton_behaves_as_semaphore(self):
        """Smoke-test the actual shared singleton's behavior without
        risking state pollution: acquire + release in the same task. If
        the singleton was replaced with something not-a-semaphore, this
        fails.
        """
        await _openmeteo_shared.OPENMETEO_SEMAPHORE.acquire()
        _openmeteo_shared.OPENMETEO_SEMAPHORE.release()


class TestSingletonReloadGuard:
    """The reload guard prevents accidental re-import from orphaning
    in-flight acquisitions on the previous semaphore instance.
    """

    def test_reload_preserves_semaphore_identity(self):
        """importlib.reload(_openmeteo_shared) must NOT create a new
        semaphore object (would orphan any waiters on the old one)."""
        import importlib
        original = _openmeteo_shared.OPENMETEO_SEMAPHORE
        importlib.reload(_openmeteo_shared)
        # Identity preserved: guard in module body refused to overwrite.
        assert _openmeteo_shared.OPENMETEO_SEMAPHORE is original


class _FakeClock:
    """Monotonic clock that only moves when the code under test sleeps.

    The shared cooldown compares against ``time.monotonic()``, so a fake
    ``asyncio.sleep`` alone would leave every cooldown permanently active.
    """

    def __init__(self):
        self.now = 0.0
        self.sleeps = []
        self._real_sleep = asyncio.sleep

    def monotonic(self):
        return self.now

    async def sleep(self, d):
        self.sleeps.append(d)
        self.now += max(d, 0)
        await self._real_sleep(0)


@pytest.fixture
def fake_clock(monkeypatch):
    clock = _FakeClock()
    monkeypatch.setattr(_openmeteo_shared.time, 'monotonic', clock.monotonic)
    monkeypatch.setattr(asyncio, 'sleep', clock.sleep)
    monkeypatch.setattr(_openmeteo_shared, 'RATE_LIMIT_JITTER_SECONDS', 0.0)
    _openmeteo_shared.reset_rate_limit_state()
    yield clock
    _openmeteo_shared.reset_rate_limit_state()


def _rate_limited(location):
    return {
        'name': location['name'], 'data': None, 'status': 429,
        'error': '{"error":true,"reason":"Too many concurrent requests"}',
    }


class TestRateLimitRetry:
    """2026-10-04 (run 37226371364): a ~60s Open-Meteo 429 storm outlasted the
    1s + 2s transient backoff, lost 29 per-location fetches and cost the whole
    publish. A
    rate-limited response now takes a longer schedule and arms a shared
    cooldown."""

    def test_is_rate_limited_on_status(self):
        assert _openmeteo_shared.is_rate_limited({'status': 429, 'error': ''})

    def test_is_rate_limited_on_body_marker(self):
        assert _openmeteo_shared.is_rate_limited(
            {'error': '{"reason":"Too many concurrent requests","error":true}'}
        )

    def test_timeout_is_not_rate_limited(self):
        assert not _openmeteo_shared.is_rate_limited({'error': 'timeout', 'status': None})
        assert not _openmeteo_shared.is_rate_limited({'error': 'x', 'status': 500})

    @pytest.mark.asyncio
    async def test_rate_limit_schedule(self, fake_clock):
        import logging

        async def always_429(session, location):
            return _rate_limited(location)

        result = await _openmeteo_shared.fetch_location_with_retry(
            None, {'name': 'L1'}, always_429, logging.getLogger('test'), apply_gap=False,
        )
        assert result['data'] is None
        # The cooldown armed by each 429 has already elapsed when the next
        # attempt starts, so the only sleeps are the backoffs themselves.
        assert fake_clock.sleeps == [5.0, 10.0, 20.0, 30.0, 30.0]

    @pytest.mark.asyncio
    async def test_rate_limit_does_not_consume_transient_budget(self, fake_clock):
        import logging
        calls = 0

        async def fetch_fn(session, location):
            nonlocal calls
            calls += 1
            if calls <= _openmeteo_shared.RATE_LIMIT_MAX_RETRIES:
                return _rate_limited(location)
            if calls < _openmeteo_shared.RATE_LIMIT_MAX_RETRIES + _openmeteo_shared.MAX_RETRIES:
                return {'name': location['name'], 'data': None, 'error': 'timeout'}
            return {'name': location['name'], 'data': {'ok': True}, 'error': None}

        result = await _openmeteo_shared.fetch_location_with_retry(
            None, {'name': 'L1'}, fetch_fn, logging.getLogger('test'), apply_gap=False,
        )
        assert result['data'] == {'ok': True}
        assert calls == _openmeteo_shared.RATE_LIMIT_MAX_RETRIES + _openmeteo_shared.MAX_RETRIES

    @pytest.mark.asyncio
    async def test_cooldown_holds_other_locations(self, fake_clock):
        """One location's 429 delays every other location's next attempt."""
        import logging
        _openmeteo_shared._extend_cooldown(12.0)
        started_at = []

        async def fetch_fn(session, location):
            started_at.append(fake_clock.now)
            return {'name': location['name'], 'data': {'ok': True}, 'error': None}

        await _openmeteo_shared.fetch_location_with_retry(
            None, {'name': 'L2'}, fetch_fn, logging.getLogger('test'), apply_gap=False,
        )
        assert started_at == [12.0]

    @pytest.mark.asyncio
    async def test_no_cooldown_sleep_on_healthy_run(self, fake_clock):
        import logging

        async def ok(session, location):
            return {'name': location['name'], 'data': {'ok': True}, 'error': None}

        await _openmeteo_shared.fetch_location_with_retry(
            None, {'name': 'L1'}, ok, logging.getLogger('test'), apply_gap=False,
        )
        assert fake_clock.sleeps == []

    @pytest.mark.asyncio
    async def test_replay_of_2026_10_04_storm(self, monkeypatch):
        """Every request refused for the first ~61s, then healthy — the 10-04
        profile, on the REAL clock with every delay scaled down 100x.

        Real time, not `fake_clock`: that clock advances by each sleep in turn,
        so 25 concurrent locations would add their sleeps up end to end and
        "outlast" the storm under any retry policy. This test passed against
        the pre-fix code until it was rewritten this way. Under the old
        ~3s (here ~0.03s) transient budget every location is lost.
        """
        import logging
        import time as real_time
        scale = 0.01
        for name in ('RATE_LIMIT_INITIAL_DELAY_SECONDS', 'RATE_LIMIT_MAX_DELAY_SECONDS',
                     'RATE_LIMIT_JITTER_SECONDS', 'RETRY_INITIAL_DELAY_SECONDS',
                     'OPENMETEO_GAP_SECONDS'):
            monkeypatch.setattr(_openmeteo_shared, name,
                                getattr(_openmeteo_shared, name) * scale)
        # A LOCAL semaphore: contending on the module singleton binds it to
        # this test's event loop, and the next contending test then raises
        # "bound to a different event loop" (see TestSharedSemaphoreEnforces-
        # Concurrency for the same rule).
        monkeypatch.setattr(_openmeteo_shared, 'OPENMETEO_SEMAPHORE',
                            asyncio.Semaphore(_openmeteo_shared.OPENMETEO_SEMAPHORE_CAP))
        _openmeteo_shared.reset_rate_limit_state()
        storm_ends = real_time.monotonic() + 61.0 * scale

        async def fetch_fn(session, location):
            if real_time.monotonic() < storm_ends:
                return _rate_limited(location)
            return {'name': location['name'], 'data': {'ok': True}, 'error': None}

        locations = [{'name': f'L{i}'} for i in range(25)]
        try:
            results = await asyncio.gather(*[
                _openmeteo_shared.fetch_location_with_retry(
                    None, loc, fetch_fn, logging.getLogger('test'), apply_gap=(i > 0),
                )
                for i, loc in enumerate(locations)
            ])
        finally:
            _openmeteo_shared.reset_rate_limit_state()
        assert all(r['data'] == {'ok': True} for r in results)


class TestCooldownHonouredUnderContention:
    """Review finding (2026-10-05): with more locations than semaphore slots,
    workers that passed a pre-semaphore cooldown check at t=0 queued on the
    semaphore and then fired into a cooldown armed while they waited — 135 of
    178 attempts in a 38-location simulation. Real clock, delays scaled 100x."""

    @pytest.mark.asyncio
    async def test_no_attempt_starts_during_active_cooldown(self, monkeypatch):
        import logging
        import time as real_time
        scale = 0.01
        for name in ('RATE_LIMIT_INITIAL_DELAY_SECONDS', 'RATE_LIMIT_MAX_DELAY_SECONDS',
                     'RATE_LIMIT_JITTER_SECONDS', 'RETRY_INITIAL_DELAY_SECONDS',
                     'OPENMETEO_GAP_SECONDS'):
            monkeypatch.setattr(_openmeteo_shared, name,
                                getattr(_openmeteo_shared, name) * scale)
        # A LOCAL semaphore: contending on the module singleton binds it to
        # this test's event loop, and the next contending test then raises
        # "bound to a different event loop" (see TestSharedSemaphoreEnforces-
        # Concurrency for the same rule).
        monkeypatch.setattr(_openmeteo_shared, 'OPENMETEO_SEMAPHORE',
                            asyncio.Semaphore(_openmeteo_shared.OPENMETEO_SEMAPHORE_CAP))
        storm_ends = real_time.monotonic() + 30.0 * scale
        attempts = 0
        during_cooldown = 0

        async def fetch_fn(session, location):
            nonlocal attempts, during_cooldown
            attempts += 1
            if _openmeteo_shared._cooldown_remaining() > 0:
                during_cooldown += 1
            await asyncio.sleep(0.2 * scale)  # request latency
            if real_time.monotonic() < storm_ends:
                return _rate_limited(location)
            return {'name': location['name'], 'data': {'ok': True}, 'error': None}

        locations = [{'name': f'L{i}'} for i in range(38)]
        results = await asyncio.gather(*[
            _openmeteo_shared.fetch_location_with_retry(
                None, loc, fetch_fn, logging.getLogger('test'), apply_gap=(i > 0),
            )
            for i, loc in enumerate(locations)
        ])
        assert all(r['data'] == {'ok': True} for r in results)
        assert during_cooldown == 0, f"{during_cooldown} of {attempts} attempts fired into a cooldown"


class _Fake429Response:
    status = 429

    async def text(self):
        return '{"error":true,"reason":"Too many concurrent requests"}'

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Fake429Session:
    def get(self, *args, **kwargs):
        return _Fake429Response()


class TestRealCollectorsReportRateLimit:
    """The retry tests above use a synthetic fetch_fn. This checks the REAL
    `_fetch_location_data` of each collector class produces a response that
    `is_rate_limited` recognises — the link a synthetic fetch_fn cannot prove."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize('cls', [
        openmeteo_weather.OpenMeteoWeatherCollector,
        openmeteo_solar.OpenMeteoSolarCollector,
        openmeteo_offshore_wind.OpenMeteoOffshoreWindCollector,
    ])
    async def test_429_is_classified_rate_limited(self, cls):
        collector = cls(locations=[{'name': 'L1', 'lat': 52.0, 'lon': 5.0}])
        response = await collector._fetch_location_data(
            _Fake429Session(), {'name': 'L1', 'lat': 52.0, 'lon': 5.0},
        )
        assert response['data'] is None
        assert response['status'] == 429
        assert _openmeteo_shared.is_rate_limited(response)
