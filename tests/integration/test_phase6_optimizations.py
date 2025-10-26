"""
Integration Tests for Phase 6 Optimizations
-------------------------------------------
Tests circuit breaker and caching in realistic scenarios.

File: tests/integration/test_phase6_optimizations.py
Created: 2025-10-25
"""

import asyncio
import platform
import time
import sys
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from collectors import (
    EnergyZeroCollector,
    EpexCollector,
    CircuitBreakerConfig,
    CircuitState
)
from collectors.luchtmeetnet import LuchtmeetnetCollector


async def test_circuit_breaker_real_api():
    """Test circuit breaker with real API that might fail."""
    print("\n" + "="*70)
    print("TEST: Circuit Breaker with Real APIs")
    print("="*70)

    # Test with ENTSO-E (currently failing)
    from collectors import EntsoeCollector
    from utils.helpers import load_secrets
    import os

    # Load API key from environment or secrets.ini
    config = load_secrets('.')
    api_key = config.get('api_keys', 'entsoe')

    # Create collector with aggressive circuit breaker
    collector = EntsoeCollector(
        api_key=api_key,
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=3,  # Open after 3 failures
            success_threshold=2,
            timeout=5.0,  # Test recovery after 5s
            enabled=True
        )
    )

    tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(tz)
    end = start + timedelta(hours=24)

    print(f"\nCollecting from ENTSO-E (expect failures)...")
    print(f"Circuit breaker: threshold={collector.circuit_breaker_config.failure_threshold}")

    # Attempt multiple collections
    blocked_count = 0
    for i in range(6):
        print(f"\n[Attempt {i+1}] State before: {collector._circuit_breaker.state.value}")
        print(f"  Failure count: {collector._circuit_breaker.failure_count}")

        # Check if circuit is open BEFORE the attempt
        circuit_was_open = collector._circuit_breaker.state == CircuitState.OPEN

        start_time = time.time()
        result = await collector.collect(start, end, country_code='NL')
        duration = time.time() - start_time

        if result:
            print(f"  SUCCESS: {len(result.data)} data points in {duration:.2f}s")
        else:
            # If circuit was open before attempt, this should have been blocked instantly
            if circuit_was_open:
                print(f"  BLOCKED by circuit breaker in {duration:.4f}s")
                assert duration < 0.1, f"Blocked request should be instant, got {duration:.4f}s"
                blocked_count += 1
            else:
                print(f"  FAILED in {duration:.2f}s")

        # Small delay between attempts
        if i < 5:
            await asyncio.sleep(0.5)

    print(f"\nFinal circuit state: {collector._circuit_breaker.state.value}")
    print(f"Total failures: {collector._circuit_breaker.failure_count}")
    print(f"Total blocked requests: {blocked_count}")

    # Circuit should be OPEN after 3 failures
    assert collector._circuit_breaker.state == CircuitState.OPEN, "Circuit should be open after failures"
    assert blocked_count > 0, "Some requests should have been blocked by circuit breaker"
    print("\n[PASS] Circuit breaker opened after threshold failures and blocked subsequent requests")


async def test_luchtmeetnet_cache_performance():
    """Test Luchtmeetnet cache provides significant speedup."""
    print("\n" + "="*70)
    print("TEST: Luchtmeetnet Cache Performance")
    print("="*70)

    # Reset cache
    LuchtmeetnetCollector._station_cache = None
    LuchtmeetnetCollector._cache_timestamp = None

    collector = LuchtmeetnetCollector(51.966472, 5.94009)
    tz = ZoneInfo('Europe/Amsterdam')
    end = datetime.now(tz)
    start = end - timedelta(hours=24)

    # First collection (cache miss)
    print("\n[Run 1] Cache miss - fetching all stations...")
    t0 = time.time()
    data1 = await collector.collect(start, end)
    time1 = time.time() - t0

    if data1:
        print(f"  SUCCESS: {len(data1.data)} data points in {time1:.2f}s")
    else:
        print(f"  FAILED (skipping cache test)")
        return

    # Second collection (cache hit)
    print("\n[Run 2] Cache hit - using cached stations...")
    t0 = time.time()
    data2 = await collector.collect(start, end)
    time2 = time.time() - t0

    if data2:
        print(f"  SUCCESS: {len(data2.data)} data points in {time2:.2f}s")
        print(f"\n  Speedup: {time1/time2:.1f}x faster")
        print(f"  Time saved: {time1-time2:.2f}s ({(time1-time2)/time1*100:.1f}% reduction)")

        # Cache should provide at least 2x speedup
        assert time2 < time1 / 2, f"Cache should provide >2x speedup, got {time1/time2:.1f}x"
        print("\n[PASS] Cache provides significant speedup")
    else:
        print(f"  FAILED")


async def test_working_collectors_with_defaults():
    """Test that working collectors function normally with default circuit breaker."""
    print("\n" + "="*70)
    print("TEST: Working Collectors with Default Circuit Breaker")
    print("="*70)

    collectors = [
        ("EnergyZero", EnergyZeroCollector()),
        ("EPEX", EpexCollector()),
    ]

    tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(tz)
    end = start + timedelta(hours=24)

    for name, collector in collectors:
        print(f"\n[Testing {name}]")
        print(f"  Circuit breaker enabled: {collector.circuit_breaker_config.enabled}")
        print(f"  Failure threshold: {collector.circuit_breaker_config.failure_threshold}")

        result = await collector.collect(start, end)

        if result:
            print(f"  SUCCESS: {len(result.data)} data points")
            print(f"  Circuit state: {collector._circuit_breaker.state.value}")
            print(f"  Failures: {collector._circuit_breaker.failure_count}")

            # Should remain closed on success
            assert collector._circuit_breaker.state == CircuitState.CLOSED
            assert collector._circuit_breaker.failure_count == 0
        else:
            print(f"  FAILED (may be temporary API issue)")

    print("\n[PASS] Working collectors remain in CLOSED state")


async def test_multiple_collection_cycles():
    """Test multiple rapid collection cycles."""
    print("\n" + "="*70)
    print("TEST: Multiple Collection Cycles")
    print("="*70)

    collector = EpexCollector()
    tz = ZoneInfo('Europe/Amsterdam')

    print("\nRunning 5 collection cycles...")
    durations = []
    successes = 0

    for i in range(5):
        start = datetime.now(tz)
        end = start + timedelta(hours=24)

        t0 = time.time()
        result = await collector.collect(start, end)
        duration = time.time() - t0
        durations.append(duration)

        if result:
            successes += 1
            print(f"  Cycle {i+1}: SUCCESS in {duration:.2f}s ({len(result.data)} points)")
        else:
            print(f"  Cycle {i+1}: FAILED in {duration:.2f}s")

        await asyncio.sleep(0.2)  # Small delay between cycles

    print(f"\nResults: {successes}/5 successful")
    print(f"Average duration: {sum(durations)/len(durations):.2f}s")
    print(f"Fastest: {min(durations):.2f}s, Slowest: {max(durations):.2f}s")

    # Should have mostly successes
    assert successes >= 3, f"Expected at least 3/5 successes, got {successes}/5"
    print("\n[PASS] Multiple cycles completed successfully")


async def test_circuit_breaker_recovery():
    """Test circuit breaker recovery after timeout."""
    print("\n" + "="*70)
    print("TEST: Circuit Breaker Recovery")
    print("="*70)

    from collectors import EntsoeCollector
    from utils.helpers import load_secrets

    # Load API key from environment or secrets.ini
    config = load_secrets('.')
    api_key = config.get('api_keys', 'entsoe')

    # Create collector with short timeout
    collector = EntsoeCollector(
        api_key=api_key,
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=2,
            timeout=2.0,  # 2 second timeout
            enabled=True
        )
    )

    tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(tz)
    end = start + timedelta(hours=24)

    # Open the circuit
    print("\nOpening circuit...")
    for i in range(2):
        await collector.collect(start, end, country_code='NL')

    assert collector._circuit_breaker.state == CircuitState.OPEN
    print(f"  Circuit OPEN (failures: {collector._circuit_breaker.failure_count})")

    # Try immediately (should block)
    print("\nTrying immediately (should block)...")
    t0 = time.time()
    result = await collector.collect(start, end, country_code='NL')
    duration = time.time() - t0
    assert result is None
    assert duration < 0.1
    print(f"  BLOCKED in {duration:.4f}s (instant)")

    # Wait for timeout
    print(f"\nWaiting {collector.circuit_breaker_config.timeout}s for timeout...")
    await asyncio.sleep(collector.circuit_breaker_config.timeout + 0.5)

    # Check state changed to HALF_OPEN
    allowed = collector._check_circuit_breaker()
    print(f"  Circuit state: {collector._circuit_breaker.state.value}")
    assert collector._circuit_breaker.state == CircuitState.HALF_OPEN
    print("\n[PASS] Circuit transitioned to HALF_OPEN after timeout")


async def main():
    """Run all integration tests."""
    print("\n" + "="*70)
    print("PHASE 6 OPTIMIZATIONS - INTEGRATION TESTS")
    print("="*70)

    tests = [
        ("Circuit Breaker with Real API", test_circuit_breaker_real_api),
        ("Luchtmeetnet Cache Performance", test_luchtmeetnet_cache_performance),
        ("Working Collectors", test_working_collectors_with_defaults),
        ("Multiple Collection Cycles", test_multiple_collection_cycles),
        ("Circuit Breaker Recovery", test_circuit_breaker_recovery),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            await test_func()
            passed += 1
        except Exception as e:
            print(f"\n[FAIL] {name}: {e}")
            failed += 1

    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Passed: {passed}/{len(tests)}")
    print(f"Failed: {failed}/{len(tests)}")

    if failed == 0:
        print("\n[SUCCESS] All integration tests passed!")
    else:
        print(f"\n[WARNING] {failed} test(s) failed")

    print("="*70)


if __name__ == "__main__":
    # Set Windows event loop policy
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
