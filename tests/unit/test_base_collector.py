"""
Unit tests for BaseCollector

Tests the abstract base collector class and retry mechanisms.
"""
import pytest
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock

from collectors.base import (
    BaseCollector,
    RetryConfig,
    CollectorStatus,
    CollectionMetrics
)
from utils.data_types import EnhancedDataSet


# Mock collector implementation for testing
class MockCollector(BaseCollector):
    """Simple mock collector for unit tests."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            name="MockCollector",
            data_type="test_data",
            source="Test API",
            units="test_units",
            *args,
            **kwargs
        )
        self.fetch_called = 0
        self.parse_called = 0

    async def _fetch_raw_data(self, start_time, end_time, **kwargs):
        self.fetch_called += 1
        return {"test": "data"}

    def _parse_response(self, raw_data, start_time, end_time):
        self.parse_called += 1
        # Return simple test data
        return {
            start_time.isoformat(): 100.0,
            (start_time + timedelta(hours=1)).isoformat(): 200.0
        }


class TestBaseCollector:
    """Tests for BaseCollector class."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_successful_collection(self):
        """Test successful data collection."""
        collector = MockCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 10, 25, 12, 0, tzinfo=amsterdam_tz)
        end = start + timedelta(hours=24)

        result = await collector.collect(start, end)

        assert result is not None
        assert isinstance(result, EnhancedDataSet)
        assert collector.fetch_called == 1
        assert collector.parse_called == 1
        assert len(result.data) == 2

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Test retry mechanism on transient failures."""

        class FailingCollector(MockCollector):
            def __init__(self):
                super().__init__(retry_config=RetryConfig(
                    max_attempts=3,
                    initial_delay=0.01,  # Fast for testing
                    exponential_base=2.0
                ))
                self.attempts = 0

            async def _fetch_raw_data(self, start_time, end_time, **kwargs):
                self.attempts += 1
                if self.attempts < 2:
                    raise ConnectionError("Temporary connection error")
                return await super()._fetch_raw_data(start_time, end_time, **kwargs)

        collector = FailingCollector()

        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = start + timedelta(hours=24)

        result = await collector.collect(start, end)

        assert result is not None
        assert collector.attempts == 2  # Failed once, succeeded on retry

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_max_retries_exhausted(self):
        """Test that collection fails after max retries."""

        class AlwaysFailingCollector(MockCollector):
            def __init__(self):
                super().__init__(retry_config=RetryConfig(
                    max_attempts=2,
                    initial_delay=0.01
                ))

            async def _fetch_raw_data(self, start_time, end_time, **kwargs):
                raise ValueError("Always fails")

        collector = AlwaysFailingCollector()

        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = start + timedelta(hours=24)

        result = await collector.collect(start, end)

        assert result is None  # Should fail

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_timestamp_normalization(self):
        """Test that timestamps are normalized to Amsterdam timezone."""

        class UTCCollector(MockCollector):
            def _parse_response(self, raw_data, start_time, end_time):
                # Return UTC timestamps
                utc_time = datetime(2025, 10, 25, 10, 0, tzinfo=ZoneInfo('UTC'))
                return {
                    utc_time.isoformat(): 100.0
                }

        collector = UTCCollector()

        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = start + timedelta(hours=24)

        result = await collector.collect(start, end)

        assert result is not None
        # Check that timestamp was converted to Amsterdam time
        timestamps = list(result.data.keys())
        assert len(timestamps) == 1
        # 10:00 UTC should be 12:00 CEST (+02:00)
        assert '12:00:00+02:00' in timestamps[0] or '12:00:00+01:00' in timestamps[0]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_metadata_generation(self):
        """Test that metadata is correctly generated."""
        collector = MockCollector()

        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = start + timedelta(hours=24)

        result = await collector.collect(start, end)

        assert result is not None
        assert result.metadata['data_type'] == 'test_data'
        assert result.metadata['source'] == 'Test API'
        assert result.metadata['units'] == 'test_units'
        assert result.metadata['collector'] == 'MockCollector'
        assert 'start_time' in result.metadata
        assert 'end_time' in result.metadata

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_metrics_collection(self):
        """Test that collection metrics are recorded."""
        collector = MockCollector()

        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = start + timedelta(hours=24)

        await collector.collect(start, end)

        metrics = collector.get_metrics(limit=1)
        assert len(metrics) == 1

        metric = metrics[0]
        assert metric.collector_name == 'MockCollector'
        assert metric.status == CollectorStatus.SUCCESS
        assert metric.data_points_collected == 2
        assert metric.duration_seconds > 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_success_rate_calculation(self):
        """Test success rate calculation."""
        collector = MockCollector()

        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = start + timedelta(hours=24)

        # First collection should succeed
        await collector.collect(start, end)

        success_rate = collector.get_success_rate()
        assert success_rate == 1.0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_validation_warnings(self):
        """Test that data validation produces warnings for edge cases."""

        class SparseDataCollector(MockCollector):
            def _parse_response(self, raw_data, start_time, end_time):
                # Return only one data point (should trigger warning)
                return {start_time.isoformat(): 100.0}

        collector = SparseDataCollector()

        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = start + timedelta(hours=24)

        result = await collector.collect(start, end)

        assert result is not None

        # Check metrics for warnings
        metrics = collector.get_metrics(limit=1)
        assert len(metrics[0].warnings) > 0

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_empty_data_handling(self):
        """Test handling of empty data responses."""

        class EmptyDataCollector(MockCollector):
            def _parse_response(self, raw_data, start_time, end_time):
                return {}  # No data

        collector = EmptyDataCollector()

        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = start + timedelta(hours=24)

        result = await collector.collect(start, end)

        # Should return dataset even with empty data
        assert result is not None
        assert len(result.data) == 0

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        assert metrics[0].status == CollectorStatus.PARTIAL


class TestRetryConfig:
    """Tests for RetryConfig."""

    @pytest.mark.unit
    def test_default_config(self):
        """Test default retry configuration."""
        config = RetryConfig()

        assert config.max_attempts == 3
        assert config.initial_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True

    @pytest.mark.unit
    def test_custom_config(self):
        """Test custom retry configuration."""
        config = RetryConfig(
            max_attempts=5,
            initial_delay=0.5,
            max_delay=30.0,
            exponential_base=3.0,
            jitter=False
        )

        assert config.max_attempts == 5
        assert config.initial_delay == 0.5
        assert config.max_delay == 30.0
        assert config.exponential_base == 3.0
        assert config.jitter is False


class TestCollectorIntegration:
    """Integration-style tests for collectors."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_multiple_collections(self):
        """Test multiple successive collections."""
        collector = MockCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')

        # Collect data for 3 different time periods
        for i in range(3):
            start = datetime(2025, 10, 25 + i, 12, 0, tzinfo=amsterdam_tz)
            end = start + timedelta(hours=24)

            result = await collector.collect(start, end)
            assert result is not None

        # Check metrics
        metrics = collector.get_metrics(limit=10)
        assert len(metrics) == 3

        # All should be successful
        assert all(m.status == CollectorStatus.SUCCESS for m in metrics)

        # Success rate should be 100%
        assert collector.get_success_rate() == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
