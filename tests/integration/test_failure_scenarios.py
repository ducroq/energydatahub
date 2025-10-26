"""
Failure Scenario Test Suite
---------------------------
Tests system behavior under various failure conditions.

File: tests/integration/test_failure_scenarios.py
Created: 2025-10-25
"""

import asyncio
import platform
import sys
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig, CircuitState
from utils.data_types import EnhancedDataSet


class FailingCollector(BaseCollector):
    """Mock collector that fails controllably."""

    def __init__(self, fail_count=0, **kwargs):
        super().__init__(
            name="FailingCollector",
            data_type="test",
            source="Test API",
            units="test",
            **kwargs
        )
        self.fail_count = fail_count
        self.attempts = 0

    async def _fetch_raw_data(self, start_time, end_time, **kwargs):
        self.attempts += 1
        if self.attempts <= self.fail_count:
            raise ValueError(f"Simulated failure {self.attempts}")
        return {"test": "data"}

    def _parse_response(self, raw_data, start_time, end_time):
        return {"2025-10-25T12:00:00+02:00": 42.0}


class IntermittentCollector(BaseCollector):
    """Mock collector that fails intermittently."""

    def __init__(self, fail_pattern=None, **kwargs):
        super().__init__(
            name="IntermittentCollector",
            data_type="test",
            source="Test API",
            units="test",
            **kwargs
        )
        self.fail_pattern = fail_pattern or []
        self.call_index = 0

    async def _fetch_raw_data(self, start_time, end_time, **kwargs):
        should_fail = self.fail_pattern[self.call_index] if self.call_index < len(self.fail_pattern) else False
        self.call_index += 1

        if should_fail:
            raise ValueError(f"Simulated failure at call {self.call_index}")
        return {"test": "data"}

    def _parse_response(self, raw_data, start_time, end_time):
        return {"2025-10-25T12:00:00+02:00": 42.0}


async def test_retry_eventually_succeeds():
    """Test that retry mechanism eventually succeeds."""
    print("\n" + "="*70)
    print("SCENARIO 1: Retry Eventually Succeeds")
    print("="*70)

    # Fail 2 times, succeed on 3rd attempt
    collector = FailingCollector(
        fail_count=2,
        retry_config=RetryConfig(max_attempts=3, initial_delay=0.1)
    )

    tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(tz)
    end = start + timedelta(hours=1)

    print("\nExpecting 2 failures, then success on attempt 3...")
    result = await collector.collect(start, end)

    print(f"  Attempts made: {collector.attempts}")
    print(f"  Result: {'SUCCESS' if result else 'FAILED'}")

    assert result is not None, "Should succeed after retries"
    assert collector.attempts == 3, "Should take 3 attempts"
    print("\n[PASS] Retry mechanism succeeded after 2 failures")


async def test_all_retries_exhausted():
    """Test behavior when all retries are exhausted."""
    print("\n" + "="*70)
    print("SCENARIO 2: All Retries Exhausted")
    print("="*70)

    # Fail all attempts
    collector = FailingCollector(
        fail_count=10,  # More than max_attempts
        retry_config=RetryConfig(max_attempts=3, initial_delay=0.1),
        circuit_breaker_config=CircuitBreakerConfig(failure_threshold=5)
    )

    tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(tz)
    end = start + timedelta(hours=1)

    print("\nExpecting all 3 attempts to fail...")
    result = await collector.collect(start, end)

    print(f"  Attempts made: {collector.attempts}")
    print(f"  Result: {'SUCCESS' if result else 'FAILED'}")
    print(f"  Circuit state: {collector._circuit_breaker.state.value}")
    print(f"  Failure count: {collector._circuit_breaker.failure_count}")

    assert result is None, "Should fail after exhausting retries"
    assert collector.attempts == 3, "Should try exactly max_attempts"
    assert collector._circuit_breaker.failure_count == 1, "Should record one collection failure"
    print("\n[PASS] Failed gracefully after exhausting retries")


async def test_circuit_breaker_prevents_retry_storm():
    """Test circuit breaker prevents excessive retries."""
    print("\n" + "="*70)
    print("SCENARIO 3: Circuit Breaker Prevents Retry Storm")
    print("="*70)

    # Fail consistently
    collector = FailingCollector(
        fail_count=100,
        retry_config=RetryConfig(max_attempts=3, initial_delay=0.1),
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=3,
            enabled=True
        )
    )

    tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(tz)
    end = start + timedelta(hours=1)

    print(f"\nRunning collections until circuit opens (threshold=3)...")
    total_api_calls = 0

    for i in range(6):
        result = await collector.collect(start, end)
        print(f"  Collection {i+1}: {collector._circuit_breaker.state.value}, "
              f"API calls: {collector.attempts - total_api_calls}")
        total_api_calls = collector.attempts

        if collector._circuit_breaker.state == CircuitState.OPEN:
            break

    print(f"\nCircuit opened after {collector._circuit_breaker.failure_count} collection failures")
    print(f"Total API calls made: {total_api_calls}")
    print(f"Without circuit breaker would have made: {6 * 3} calls")

    assert collector._circuit_breaker.state == CircuitState.OPEN
    assert total_api_calls < 12, "Circuit breaker should prevent excessive calls"
    print(f"\n[PASS] Circuit breaker prevented {(6*3) - total_api_calls} unnecessary API calls")


async def test_intermittent_failures_reset_count():
    """Test that intermittent failures don't accumulate."""
    print("\n" + "="*70)
    print("SCENARIO 4: Intermittent Failures Reset Count")
    print("="*70)

    # Fail, succeed, fail, succeed pattern
    pattern = [True, False, True, False, True, False]
    collector = IntermittentCollector(
        fail_pattern=pattern,
        retry_config=RetryConfig(max_attempts=1, initial_delay=0.1),
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=3,
            enabled=True
        )
    )

    tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(tz)
    end = start + timedelta(hours=1)

    print("\nPattern: Fail, Success, Fail, Success, Fail, Success...")
    for i in range(6):
        result = await collector.collect(start, end)
        status = "SUCCESS" if result else "FAILED"
        print(f"  Collection {i+1}: {status}, "
              f"Failure count: {collector._circuit_breaker.failure_count}")

    print(f"\nFinal circuit state: {collector._circuit_breaker.state.value}")
    assert collector._circuit_breaker.state == CircuitState.CLOSED
    assert collector._circuit_breaker.failure_count == 0
    print("\n[PASS] Intermittent successes prevent circuit from opening")


async def test_circuit_recovery():
    """Test circuit breaker recovery after timeout."""
    print("\n" + "="*70)
    print("SCENARIO 5: Circuit Recovery After Timeout")
    print("="*70)

    # Start failing, then start succeeding
    collector = FailingCollector(
        fail_count=2,  # Fail first 2, then succeed
        retry_config=RetryConfig(max_attempts=1, initial_delay=0.1),
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=2,
            success_threshold=2,
            timeout=1.0,
            enabled=True
        )
    )

    tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(tz)
    end = start + timedelta(hours=1)

    # Open the circuit
    print("\nOpening circuit with 2 failures...")
    for i in range(2):
        await collector.collect(start, end)

    print(f"  Circuit state: {collector._circuit_breaker.state.value}")
    print(f"  Attempts so far: {collector.attempts}")
    assert collector._circuit_breaker.state == CircuitState.OPEN

    # Wait for timeout
    print(f"\nWaiting {collector.circuit_breaker_config.timeout}s for timeout...")
    await asyncio.sleep(collector.circuit_breaker_config.timeout + 0.2)

    # Should enter HALF_OPEN and succeed
    print("Testing recovery...")
    result = await collector.collect(start, end)  # Attempt 3 (succeeds, enters HALF_OPEN)
    print(f"  After 1st success: {collector._circuit_breaker.state.value}")
    print(f"  Success count: {collector._circuit_breaker.success_count}")
    assert result is not None, "First recovery attempt should succeed"
    assert collector._circuit_breaker.state == CircuitState.HALF_OPEN

    result = await collector.collect(start, end)  # Attempt 4 (succeeds, closes circuit)
    print(f"  After 2nd success: {collector._circuit_breaker.state.value}")
    assert result is not None, "Second recovery attempt should succeed"

    assert collector._circuit_breaker.state == CircuitState.CLOSED
    print("\n[PASS] Circuit recovered after timeout and success threshold")


async def test_graceful_degradation():
    """Test system continues with partial failures."""
    print("\n" + "="*70)
    print("SCENARIO 6: Graceful Degradation")
    print("="*70)

    collectors = {
        "Working1": FailingCollector(fail_count=0, retry_config=RetryConfig(max_attempts=1)),
        "Failing": FailingCollector(fail_count=10, retry_config=RetryConfig(max_attempts=1)),
        "Working2": FailingCollector(fail_count=0, retry_config=RetryConfig(max_attempts=1)),
    }

    tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(tz)
    end = start + timedelta(hours=1)

    print("\nCollecting from 3 sources (1 failing, 2 working)...")
    results = {}

    for name, collector in collectors.items():
        result = await collector.collect(start, end)
        results[name] = result
        status = "SUCCESS" if result else "FAILED"
        print(f"  {name}: {status}")

    successful = sum(1 for r in results.values() if r is not None)
    print(f"\nSuccessful collections: {successful}/{len(collectors)}")

    assert successful == 2, "Should succeed with working collectors"
    print("\n[PASS] System gracefully degrades with partial failures")


async def main():
    """Run all failure scenario tests."""
    print("\n" + "="*70)
    print("FAILURE SCENARIO TEST SUITE")
    print("="*70)

    tests = [
        ("Retry Eventually Succeeds", test_retry_eventually_succeeds),
        ("All Retries Exhausted", test_all_retries_exhausted),
        ("Circuit Breaker Prevents Retry Storm", test_circuit_breaker_prevents_retry_storm),
        ("Intermittent Failures Reset Count", test_intermittent_failures_reset_count),
        ("Circuit Recovery After Timeout", test_circuit_recovery),
        ("Graceful Degradation", test_graceful_degradation),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            await test_func()
            passed += 1
        except AssertionError as e:
            print(f"\n[FAIL] {name}: {e}")
            failed += 1
        except Exception as e:
            print(f"\n[ERROR] {name}: {e}")
            failed += 1

    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Passed: {passed}/{len(tests)}")
    print(f"Failed: {failed}/{len(tests)}")

    if failed == 0:
        print("\n[SUCCESS] All failure scenario tests passed!")
    else:
        print(f"\n[WARNING] {failed} test(s) failed")

    print("="*70)


if __name__ == "__main__":
    # Set Windows event loop policy
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
