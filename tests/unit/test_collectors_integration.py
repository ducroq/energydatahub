"""
Unit Tests for Individual Collectors
------------------------------------
Tests each collector's unique functionality.

File: tests/unit/test_collectors_integration.py
Created: 2025-10-25
"""

import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock, patch

from collectors import (
    EnergyZeroCollector,
    EpexCollector,
    OpenWeatherCollector,
    CircuitBreakerConfig
)


class TestEnergyZeroCollector:
    """Test EnergyZero collector."""

    def test_initialization(self):
        """Test collector initialization."""
        collector = EnergyZeroCollector()
        assert collector.name == "EnergyZeroCollector"
        assert collector.data_type == "energy_price"
        assert "EnergyZero" in collector.source

    @pytest.mark.asyncio
    async def test_parse_response(self):
        """Test parsing EnergyZero API response."""
        collector = EnergyZeroCollector()

        # Mock API response
        from energyzero import Electricity
        mock_prices = [
            Electricity(
                timestamp=datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam')),
                price=0.25
            ),
            Electricity(
                timestamp=datetime(2025, 10, 25, 13, 0, tzinfo=ZoneInfo('Europe/Amsterdam')),
                price=0.30
            )
        ]

        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = datetime(2025, 10, 25, 14, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

        parsed = collector._parse_response(mock_prices, start, end)

        assert len(parsed) == 2
        assert all(isinstance(v, float) for v in parsed.values())

    def test_metadata(self):
        """Test metadata generation."""
        collector = EnergyZeroCollector()
        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = datetime(2025, 10, 25, 13, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

        metadata = collector._get_metadata(start, end)

        assert metadata['data_type'] == 'energy_price'
        assert 'EnergyZero' in metadata['source']
        assert 'EUR/kWh' in metadata['units']
        assert metadata['collector'] == 'EnergyZeroCollector'


class TestEpexCollector:
    """Test EPEX collector."""

    def test_initialization(self):
        """Test collector initialization."""
        collector = EpexCollector()
        assert collector.name == "EpexCollector"
        assert collector.data_type == "energy_price"
        assert "Awattar" in collector.source

    @pytest.mark.asyncio
    async def test_parse_response(self):
        """Test parsing EPEX/Awattar API response."""
        collector = EpexCollector()

        # Mock API response
        mock_data = {
            'data': [
                {
                    'start_timestamp': 1729850400000,  # Unix timestamp in ms
                    'end_timestamp': 1729854000000,
                    'marketprice': 120.5
                },
                {
                    'start_timestamp': 1729854000000,
                    'end_timestamp': 1729857600000,
                    'marketprice': 115.0
                }
            ]
        }

        start = datetime(2025, 10, 25, 0, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = datetime(2025, 10, 26, 0, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

        parsed = collector._parse_response(mock_data, start, end)

        assert len(parsed) == 2
        assert all(isinstance(v, float) for v in parsed.values())
        # Prices should be converted from EUR/MWh to proper format
        assert all(v > 100 for v in parsed.values())

    def test_metadata(self):
        """Test metadata generation."""
        collector = EpexCollector()
        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = datetime(2025, 10, 25, 13, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

        metadata = collector._get_metadata(start, end)

        assert metadata['data_type'] == 'energy_price'
        assert 'Awattar' in metadata['source']
        assert 'EUR/MWh' in metadata['units']


class TestOpenWeatherCollector:
    """Test OpenWeather collector."""

    def test_initialization(self):
        """Test collector initialization."""
        collector = OpenWeatherCollector(
            api_key="test_key",
            latitude=52.37,
            longitude=4.89
        )
        assert collector.name == "OpenWeatherCollector"
        assert collector.data_type == "weather"
        assert collector.api_key == "test_key"
        assert collector.latitude == 52.37
        assert collector.longitude == 4.89

    @pytest.mark.asyncio
    async def test_parse_response(self):
        """Test parsing OpenWeather API response."""
        collector = OpenWeatherCollector(
            api_key="test_key",
            latitude=52.37,
            longitude=4.89
        )

        # Mock API response
        mock_data = {
            'list': [
                {
                    'dt': 1729850400,  # Unix timestamp
                    'main': {
                        'temp': 15.5,
                        'pressure': 1013,
                        'humidity': 75
                    },
                    'weather': [{'description': 'clear sky'}],
                    'wind': {'speed': 3.5},
                    'clouds': {'all': 20}
                }
            ]
        }

        start = datetime(2025, 10, 25, 0, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = datetime(2025, 10, 26, 0, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

        parsed = collector._parse_response(mock_data, start, end)

        assert len(parsed) >= 1
        # Check that weather data has multiple fields
        for timestamp, data in parsed.items():
            assert isinstance(data, dict)
            assert 'temp' in data or 'temperature' in str(data).lower()

    def test_metadata(self):
        """Test metadata generation."""
        collector = OpenWeatherCollector(
            api_key="test_key",
            latitude=52.37,
            longitude=4.89
        )
        start = datetime(2025, 10, 25, 12, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        end = datetime(2025, 10, 25, 13, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

        metadata = collector._get_metadata(start, end)

        assert metadata['data_type'] == 'weather'
        assert 'OpenWeather' in metadata['source']
        assert metadata['latitude'] == 52.37
        assert metadata['longitude'] == 4.89


class TestCollectorConfiguration:
    """Test collector configuration options."""

    def test_custom_retry_config(self):
        """Test custom retry configuration."""
        from collectors.base import RetryConfig

        retry_config = RetryConfig(
            max_attempts=5,
            initial_delay=2.0,
            max_delay=120.0
        )

        collector = EpexCollector(retry_config=retry_config)

        assert collector.retry_config.max_attempts == 5
        assert collector.retry_config.initial_delay == 2.0
        assert collector.retry_config.max_delay == 120.0

    def test_custom_circuit_breaker_config(self):
        """Test custom circuit breaker configuration."""
        cb_config = CircuitBreakerConfig(
            failure_threshold=10,
            success_threshold=3,
            timeout=120.0,
            enabled=False
        )

        collector = EnergyZeroCollector(circuit_breaker_config=cb_config)

        assert collector.circuit_breaker_config.failure_threshold == 10
        assert collector.circuit_breaker_config.success_threshold == 3
        assert collector.circuit_breaker_config.timeout == 120.0
        assert collector.circuit_breaker_config.enabled is False

    def test_default_configurations(self):
        """Test that defaults are sensible."""
        collector = EpexCollector()

        # Retry defaults
        assert collector.retry_config.max_attempts == 3
        assert collector.retry_config.initial_delay == 1.0
        assert collector.retry_config.jitter is True

        # Circuit breaker defaults
        assert collector.circuit_breaker_config.failure_threshold == 5
        assert collector.circuit_breaker_config.success_threshold == 2
        assert collector.circuit_breaker_config.timeout == 60.0
        assert collector.circuit_breaker_config.enabled is True


class TestCollectorMetrics:
    """Test metrics collection."""

    @pytest.mark.asyncio
    async def test_metrics_recorded_on_success(self):
        """Test that metrics are recorded on successful collection."""
        collector = EpexCollector()

        # Mock successful collection
        with patch.object(collector, '_fetch_raw_data', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = {
                'data': [{
                    'start_timestamp': 1729850400000,
                    'end_timestamp': 1729854000000,
                    'marketprice': 120.5
                }]
            }

            start = datetime.now(ZoneInfo('Europe/Amsterdam'))
            end = start + timedelta(hours=1)

            await collector.collect(start, end)

            # Check metrics were recorded
            metrics = collector.get_metrics(limit=1)
            assert len(metrics) == 1
            assert metrics[0].status.value == 'success'
            assert metrics[0].data_points_collected > 0
            assert metrics[0].duration_seconds > 0

    @pytest.mark.asyncio
    async def test_metrics_recorded_on_failure(self):
        """Test that metrics are recorded on failed collection."""
        collector = EpexCollector()

        # Mock failed collection
        with patch.object(collector, '_fetch_raw_data', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = ValueError("API error")

            start = datetime.now(ZoneInfo('Europe/Amsterdam'))
            end = start + timedelta(hours=1)

            result = await collector.collect(start, end)

            assert result is None

            # Check metrics were recorded
            metrics = collector.get_metrics(limit=1)
            assert len(metrics) == 1
            assert metrics[0].status.value == 'failed'
            assert len(metrics[0].errors) > 0

    def test_metrics_history_limited(self):
        """Test that metrics history is limited."""
        collector = EpexCollector()

        # Manually add metrics to history
        from collectors.base import CollectionMetrics, CollectorStatus
        for i in range(20):
            metrics = CollectionMetrics(
                collection_id=f"test_{i}",
                collector_name="Test",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_seconds=1.0,
                status=CollectorStatus.SUCCESS,
                attempt_count=1,
                data_points_collected=10
            )
            collector._metrics_history.append(metrics)

        # Get limited metrics
        recent_metrics = collector.get_metrics(limit=5)
        assert len(recent_metrics) == 5

        # Should be most recent
        assert recent_metrics[0].collection_id == "test_19"

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        collector = EpexCollector()

        from collectors.base import CollectionMetrics, CollectorStatus

        # Add mixed metrics
        for i in range(10):
            status = CollectorStatus.SUCCESS if i < 7 else CollectorStatus.FAILED
            metrics = CollectionMetrics(
                collection_id=f"test_{i}",
                collector_name="Test",
                start_time=datetime.now(),
                end_time=datetime.now(),
                duration_seconds=1.0,
                status=status,
                attempt_count=1,
                data_points_collected=10 if status == CollectorStatus.SUCCESS else 0
            )
            collector._metrics_history.append(metrics)

        success_rate = collector.get_success_rate()
        assert success_rate == 0.7  # 7 out of 10


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
