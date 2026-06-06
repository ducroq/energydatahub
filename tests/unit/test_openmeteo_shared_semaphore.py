"""
Issue #11: All OpenMeteo* collectors must share a single rate-limit budget.

Before this change each collector owned its own `asyncio.Semaphore(1)`, so
the effective concurrency budget against Open-Meteo's API was
`n_collectors × 1`. That number silently drifted every time a collector was
added — exactly how the 2026-06-05 buurt additions broke the previous
`Semaphore(2)+0.1s` budget (CI run 27068482501, HTTP 429 storm).

These tests pin the invariant that the semaphore is module-level shared
state, so a 6th OpenMeteo collector cannot regress the budget without
deliberate edits to `collectors/_openmeteo_shared.py`.

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

    def test_cap_matches_documented_constant(self):
        """Sanity-check: the live semaphore's internal value reflects the cap.

        `asyncio.Semaphore._value` is a private attribute but it's the only
        observable way to verify the cap at construction time. If CPython
        ever renames it this test will fail loudly — pin to the constant
        and update both together.
        """
        # On a fresh semaphore (no pending acquires) _value == cap.
        assert _openmeteo_shared.OPENMETEO_SEMAPHORE._value == _openmeteo_shared.OPENMETEO_SEMAPHORE_CAP

    def test_cap_is_set_to_documented_safe_budget(self):
        """5 = the empirically-safe budget from PR #10's fix."""
        assert _openmeteo_shared.OPENMETEO_SEMAPHORE_CAP == 5

    def test_gap_is_set_to_documented_safe_budget(self):
        """0.5s = the inter-request gap from PR #10's fix."""
        assert _openmeteo_shared.OPENMETEO_GAP_SECONDS == 0.5


class TestSharedSemaphoreEnforcesConcurrency:
    """Functional check: when 7 tasks acquire the semaphore concurrently,
    no more than CAP can be in-flight at any moment.
    """

    @pytest.mark.asyncio
    async def test_concurrency_is_bounded_by_cap(self):
        sem = _openmeteo_shared.OPENMETEO_SEMAPHORE
        cap = _openmeteo_shared.OPENMETEO_SEMAPHORE_CAP

        in_flight = 0
        peak = 0

        async def worker():
            nonlocal in_flight, peak
            async with sem:
                in_flight += 1
                peak = max(peak, in_flight)
                # Yield once so other tasks have a chance to interleave and
                # exceed the cap if the semaphore is broken.
                await asyncio.sleep(0.01)
                in_flight -= 1

        await asyncio.gather(*(worker() for _ in range(cap + 2)))

        assert peak <= cap
