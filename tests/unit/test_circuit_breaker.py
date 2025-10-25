"""
Unit Tests for Circuit Breaker Pattern
---------------------------------------
Tests the circuit breaker functionality in BaseCollector.

File: tests/unit/test_circuit_breaker.py
Created: 2025-10-25
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from collectors.base import (
    BaseCollector,
    CircuitBreakerConfig,
    CircuitState,
    CollectorStatus,
    RetryConfig
)
from utils.data_types import EnhancedDataSet


class MockCollector(BaseCollector):
    """Mock collector for testing circuit breaker."""

    def __init__(self, should_fail=False, **kwargs):
        super().__init__(
            name="MockCollector",
            data_type="test",
            source="Test API",
            units="test",
            **kwargs
        )
        self.should_fail = should_fail
        self.call_count = 0

    async def _fetch_raw_data(self, start_time, end_time, **kwargs):
        """Mock fetch that can be configured to fail."""
        self.call_count += 1
        if self.should_fail:
            raise ValueError("Mock API failure")
        return {"test": "data"}

    def _parse_response(self, raw_data, start_time, end_time):
        """Mock parse that returns simple data."""
        return {"2025-10-25T12:00:00+02:00": 42.0}


class TestCircuitBreakerStates:
    """Test circuit breaker state transitions."""

    @pytest.mark.asyncio
    async def test_initial_state_is_closed(self):
        """Circuit breaker should start in CLOSED state."""
        collector = MockCollector()
        assert collector._circuit_breaker.state == CircuitState.CLOSED
        assert collector._circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_success_keeps_circuit_closed(self):
        """Successful collections should keep circuit CLOSED."""
        collector = MockCollector(should_fail=False)
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Run successful collection
        result = await collector.collect(start, end)

        assert result is not None
        assert collector._circuit_breaker.state == CircuitState.CLOSED
        assert collector._circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_circuit_opens_after_threshold_failures(self):
        """Circuit should OPEN after reaching failure threshold."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),  # Fast failure
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3)
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Cause 3 failures
        for i in range(3):
            result = await collector.collect(start, end)
            assert result is None

            if i < 2:
                assert collector._circuit_breaker.state == CircuitState.CLOSED
            else:
                # After 3rd failure, circuit opens
                assert collector._circuit_breaker.state == CircuitState.OPEN
                assert collector._circuit_breaker.failure_count == 3

    @pytest.mark.asyncio
    async def test_open_circuit_blocks_requests(self):
        """OPEN circuit should block requests immediately."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=2)
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Open the circuit
        await collector.collect(start, end)
        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.OPEN

        # Reset call count
        collector.call_count = 0

        # Next request should be blocked without calling API
        result = await collector.collect(start, end)
        assert result is None
        assert collector.call_count == 0  # API not called

    @pytest.mark.asyncio
    async def test_circuit_enters_half_open_after_timeout(self):
        """Circuit should enter HALF_OPEN after timeout."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=2,
                timeout=0.1  # 100ms timeout for fast test
            )
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Open the circuit
        await collector.collect(start, end)
        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.OPEN

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Check circuit breaker (simulates next collect call)
        allowed = collector._check_circuit_breaker()
        assert allowed is True
        assert collector._circuit_breaker.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_closes_on_success(self):
        """HALF_OPEN circuit should close after success threshold."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=2,
                success_threshold=2,
                timeout=0.1
            )
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Open the circuit
        await collector.collect(start, end)
        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.OPEN

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Fix the API
        collector.should_fail = False

        # First success (still HALF_OPEN)
        result = await collector.collect(start, end)
        assert result is not None
        assert collector._circuit_breaker.state == CircuitState.HALF_OPEN
        assert collector._circuit_breaker.success_count == 1

        # Second success (should CLOSE)
        result = await collector.collect(start, end)
        assert result is not None
        assert collector._circuit_breaker.state == CircuitState.CLOSED
        assert collector._circuit_breaker.failure_count == 0
        assert collector._circuit_breaker.success_count == 0

    @pytest.mark.asyncio
    async def test_half_open_reopens_on_failure(self):
        """HALF_OPEN circuit should reopen on failure."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=2,
                timeout=0.1
            )
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Open the circuit
        await collector.collect(start, end)
        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.OPEN

        # Wait for timeout to enter HALF_OPEN
        await asyncio.sleep(0.15)

        # Fail during HALF_OPEN (should reopen)
        result = await collector.collect(start, end)
        assert result is None
        assert collector._circuit_breaker.state == CircuitState.OPEN
        assert collector._circuit_breaker.success_count == 0


class TestCircuitBreakerConfiguration:
    """Test circuit breaker configuration options."""

    @pytest.mark.asyncio
    async def test_custom_failure_threshold(self):
        """Should respect custom failure threshold."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=5)
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Should take 5 failures to open
        for i in range(4):
            await collector.collect(start, end)
            assert collector._circuit_breaker.state == CircuitState.CLOSED

        # 5th failure opens circuit
        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_custom_success_threshold(self):
        """Should respect custom success threshold."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=1,
                success_threshold=3,
                timeout=0.1
            )
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Open circuit
        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.OPEN

        # Wait and fix API
        await asyncio.sleep(0.15)
        collector.should_fail = False

        # Should need 3 successes to close
        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.HALF_OPEN

        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.HALF_OPEN

        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_disabled_circuit_breaker(self):
        """Disabled circuit breaker should never block requests."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(
                enabled=False,
                failure_threshold=1  # Would normally open after 1 failure
            )
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Even after many failures, circuit stays closed
        for _ in range(10):
            result = await collector.collect(start, end)
            assert result is None
            # Circuit breaker disabled, so state check is meaningless
            # but failures are still recorded
            assert collector.call_count > 0  # API is still being called

    @pytest.mark.asyncio
    async def test_default_configuration(self):
        """Should use sensible defaults when not configured."""
        collector = MockCollector()

        # Check defaults
        assert collector.circuit_breaker_config.failure_threshold == 5
        assert collector.circuit_breaker_config.success_threshold == 2
        assert collector.circuit_breaker_config.timeout == 60.0
        assert collector.circuit_breaker_config.enabled is True


class TestCircuitBreakerMetrics:
    """Test circuit breaker interaction with metrics."""

    @pytest.mark.asyncio
    async def test_metrics_recorded_for_blocked_requests(self):
        """Blocked requests should not appear in metrics history."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=2)
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Open circuit (2 failures recorded)
        await collector.collect(start, end)
        await collector.collect(start, end)
        assert len(collector.get_metrics()) == 2

        # Blocked request (no metric recorded)
        await collector.collect(start, end)
        assert len(collector.get_metrics()) == 2  # Still only 2

    @pytest.mark.asyncio
    async def test_failure_count_resets_on_success(self):
        """Successful collection should reset failure count."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=5)
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # 3 failures
        for _ in range(3):
            await collector.collect(start, end)
        assert collector._circuit_breaker.failure_count == 3

        # 1 success (should reset)
        collector.should_fail = False
        await collector.collect(start, end)
        assert collector._circuit_breaker.failure_count == 0
        assert collector._circuit_breaker.state == CircuitState.CLOSED


class TestCircuitBreakerEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_zero_timeout_immediately_half_opens(self):
        """Zero timeout should immediately allow HALF_OPEN."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=1,
                timeout=0.0
            )
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Open circuit
        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.OPEN

        # Immediately check (no sleep needed)
        allowed = collector._check_circuit_breaker()
        assert allowed is True
        assert collector._circuit_breaker.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_success_threshold_one(self):
        """Success threshold of 1 should close immediately."""
        collector = MockCollector(
            should_fail=True,
            retry_config=RetryConfig(max_attempts=1),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=1,
                success_threshold=1,
                timeout=0.1
            )
        )
        start = datetime.now()
        end = start + timedelta(hours=1)

        # Open circuit
        await collector.collect(start, end)
        await asyncio.sleep(0.15)

        # One success should close
        collector.should_fail = False
        await collector.collect(start, end)
        assert collector._circuit_breaker.state == CircuitState.CLOSED


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
