"""
Unit tests for NedCollector

Tests the NED.nl (Nationaal Energie Dashboard) collector for Dutch energy production data.
"""
import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from dataclasses import dataclass

from collectors.ned import NedCollector
from collectors.base import (
    RetryConfig,
    CircuitBreakerConfig,
    CollectorStatus,
)
from utils.data_types import EnhancedDataSet


@dataclass
class MockUtilizationRecord:
    """Mock NED.nl API response record."""
    valid_from: str
    capacity: float
    volume: float
    percentage: float
    emission: float = None


def create_sample_ned_forecast_data():
    """Create sample NED.nl forecast data mimicking API response."""
    records = []
    base_time = datetime(2025, 12, 1, 0, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

    for h in range(24):
        time_str = (base_time + timedelta(hours=h)).isoformat()
        records.append(MockUtilizationRecord(
            valid_from=time_str,
            capacity=500.0 + h * 50,  # Increasing capacity
            volume=450.0 + h * 45,
            percentage=90.0 + (h % 10)
        ))

    return records


def create_sample_ned_actual_data():
    """Create sample NED.nl actual/backcast data."""
    records = []
    base_time = datetime(2025, 12, 1, 0, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

    for h in range(24):
        time_str = (base_time + timedelta(hours=h)).isoformat()
        records.append(MockUtilizationRecord(
            valid_from=time_str,
            capacity=480.0 + h * 48,
            volume=430.0 + h * 43,
            percentage=89.0 + (h % 10),
            emission=120.0 + h * 5
        ))

    return records


class TestNedCollector:
    """Tests for NedCollector class."""

    @pytest.mark.unit
    def test_initialization_default(self):
        """Test NED.nl collector initialization with defaults."""
        collector = NedCollector(api_key="test_api_key")

        assert collector.name == "NedCollector"
        assert collector.data_type == "energy_production"
        assert collector.source == "NED.nl (Nationaal Energie Dashboard)"
        assert collector.units == "kW (capacity), kWh (volume)"
        assert collector.api_key == "test_api_key"
        # Default: all three energy types
        assert collector.energy_types == ['solar', 'wind_onshore', 'wind_offshore']
        assert collector.include_forecast is True
        assert collector.include_actual is True
        assert collector.granularity == 'hourly'

    @pytest.mark.unit
    def test_initialization_custom_types(self):
        """Test initialization with custom energy types."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['solar', 'wind_offshore'],
            include_forecast=True,
            include_actual=False
        )

        assert collector.energy_types == ['solar', 'wind_offshore']
        assert collector.include_forecast is True
        assert collector.include_actual is False

    @pytest.mark.unit
    def test_initialization_invalid_energy_type(self):
        """Test initialization with invalid energy type raises error."""
        with pytest.raises(ValueError) as exc_info:
            NedCollector(
                api_key="test_api_key",
                energy_types=['solar', 'invalid_type']
            )

        assert "Unknown energy type" in str(exc_info.value)
        assert "invalid_type" in str(exc_info.value)

    @pytest.mark.unit
    def test_type_ids_mapping(self):
        """Test that energy type IDs are correctly defined."""
        assert NedCollector.TYPE_IDS['solar'] == 2
        assert NedCollector.TYPE_IDS['wind_onshore'] == 1
        assert NedCollector.TYPE_IDS['wind_offshore'] == 17

    @pytest.mark.unit
    def test_point_ids_mapping(self):
        """Test that geographic point IDs are defined."""
        assert NedCollector.POINT_IDS['netherlands'] == 0
        assert NedCollector.POINT_IDS['offshore'] == 14

    @pytest.mark.unit
    def test_classification_ids_mapping(self):
        """Test that classification IDs are defined."""
        assert NedCollector.CLASSIFICATION_IDS['forecast'] == 1
        assert NedCollector.CLASSIFICATION_IDS['current'] == 2
        assert NedCollector.CLASSIFICATION_IDS['backcast'] == 3

    @pytest.mark.unit
    def test_granularity_ids_mapping(self):
        """Test that granularity IDs are defined."""
        assert NedCollector.GRANULARITY_IDS['10min'] == 3
        assert NedCollector.GRANULARITY_IDS['15min'] == 4
        assert NedCollector.GRANULARITY_IDS['hourly'] == 5
        assert NedCollector.GRANULARITY_IDS['daily'] == 6

    @pytest.mark.unit
    def test_parse_response_forecast_data(self):
        """Test parsing forecast response with valid data."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['solar']
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {
            'solar': {
                'forecast': create_sample_ned_forecast_data()
            }
        }

        parsed = collector._parse_response(raw_data, start, end)

        assert 'solar' in parsed
        assert 'forecast' in parsed['solar']
        assert len(parsed['solar']['forecast']) > 0

        # Check first data point has expected fields
        first_timestamp = list(parsed['solar']['forecast'].keys())[0]
        first_data = parsed['solar']['forecast'][first_timestamp]

        assert 'capacity_kw' in first_data
        assert 'volume_kwh' in first_data
        assert 'utilization_pct' in first_data

    @pytest.mark.unit
    def test_parse_response_actual_data(self):
        """Test parsing actual response with CO2 emissions."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['wind_onshore']
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {
            'wind_onshore': {
                'actual': create_sample_ned_actual_data()
            }
        }

        parsed = collector._parse_response(raw_data, start, end)

        assert 'wind_onshore' in parsed
        assert 'actual' in parsed['wind_onshore']
        assert len(parsed['wind_onshore']['actual']) > 0

        # Check CO2 emissions are included
        first_timestamp = list(parsed['wind_onshore']['actual'].keys())[0]
        first_data = parsed['wind_onshore']['actual'][first_timestamp]

        assert 'co2_kg' in first_data

    @pytest.mark.unit
    def test_parse_response_multi_type(self):
        """Test parsing response with multiple energy types."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['solar', 'wind_offshore']
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {
            'solar': {
                'forecast': create_sample_ned_forecast_data(),
                'actual': create_sample_ned_actual_data()
            },
            'wind_offshore': {
                'forecast': create_sample_ned_forecast_data()
            }
        }

        parsed = collector._parse_response(raw_data, start, end)

        assert 'solar' in parsed
        assert 'wind_offshore' in parsed
        assert 'forecast' in parsed['solar']
        assert 'actual' in parsed['solar']
        assert 'forecast' in parsed['wind_offshore']

    @pytest.mark.unit
    def test_parse_response_dict_records(self):
        """Test parsing response with dict records instead of objects."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['solar']
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # Dict-style records
        dict_records = [
            {
                'valid_from': '2025-12-01T00:00:00+01:00',
                'capacity': 500.0,
                'volume': 450.0,
                'percentage': 90.0
            },
            {
                'valid_from': '2025-12-01T01:00:00+01:00',
                'capacity': 550.0,
                'volume': 495.0,
                'percentage': 92.0
            }
        ]

        raw_data = {
            'solar': {
                'forecast': dict_records
            }
        }

        parsed = collector._parse_response(raw_data, start, end)

        assert 'solar' in parsed
        assert 'forecast' in parsed['solar']
        assert len(parsed['solar']['forecast']) == 2

    @pytest.mark.unit
    def test_parse_response_empty(self):
        """Test parsing empty response."""
        collector = NedCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {}

        parsed = collector._parse_response(raw_data, start, end)

        assert parsed == {}

    @pytest.mark.unit
    def test_validate_data_success(self):
        """Test data validation with valid data."""
        collector = NedCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # Create valid data with 24 data points
        valid_data = {
            'solar': {
                'forecast': {
                    f'2025-12-01T{h:02d}:00:00+01:00': {
                        'capacity_kw': 500.0,
                        'volume_kwh': 450.0,
                        'utilization_pct': 90.0
                    }
                    for h in range(24)
                }
            }
        }

        is_valid, warnings = collector._validate_data(valid_data, start, end)

        assert is_valid
        assert len(warnings) == 0

    @pytest.mark.unit
    def test_validate_data_insufficient_points(self):
        """Test data validation with insufficient data points."""
        collector = NedCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # Only 5 data points (should warn)
        sparse_data = {
            'solar': {
                'forecast': {
                    f'2025-12-01T{h:02d}:00:00+01:00': {
                        'capacity_kw': 500.0
                    }
                    for h in range(5)
                }
            }
        }

        is_valid, warnings = collector._validate_data(sparse_data, start, end)

        # Should have warning about insufficient points
        assert len(warnings) > 0
        assert any('5 data points' in w for w in warnings)

    @pytest.mark.unit
    def test_validate_data_empty(self):
        """Test data validation with empty data."""
        collector = NedCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        is_valid, warnings = collector._validate_data({}, start, end)

        assert not is_valid
        assert len(warnings) > 0
        assert any('No data collected' in w for w in warnings)

    @pytest.mark.unit
    def test_validate_data_missing_energy_type(self):
        """Test validation with missing energy type data."""
        collector = NedCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # Energy type present but empty
        data = {
            'solar': {}
        }

        is_valid, warnings = collector._validate_data(data, start, end)

        assert len(warnings) > 0
        assert any('No data collected' in w for w in warnings)

    @pytest.mark.unit
    def test_metadata_includes_custom_fields(self):
        """Test that NED.nl-specific metadata is included."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['solar', 'wind_offshore'],
            granularity='15min'
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        metadata = collector._get_metadata(start, end)

        assert metadata['energy_types'] == ['solar', 'wind_offshore']
        assert metadata['include_forecast'] is True
        assert metadata['include_actual'] is True
        assert metadata['granularity'] == '15min'
        assert metadata['country'] == 'NL'
        assert 'TenneT' in metadata['operators']
        assert 'Gasunie' in metadata['operators']
        assert metadata['api_rate_limit'] == '200 requests per 5 minutes'
        assert 'description' in metadata

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_success(self):
        """Test successful end-to-end collection."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['solar']
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # Mock the fetch method
        async def mock_fetch(*args, **kwargs):
            return {
                'solar': {
                    'forecast': create_sample_ned_forecast_data(),
                    'actual': create_sample_ned_actual_data()
                }
            }

        collector._fetch_raw_data = mock_fetch

        result = await collector.collect(start, end)

        assert result is not None
        assert isinstance(result, EnhancedDataSet)
        assert 'solar' in result.data
        assert 'forecast' in result.data['solar']

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        assert len(metrics) == 1
        assert metrics[0].status in [CollectorStatus.SUCCESS, CollectorStatus.PARTIAL]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_retry_on_failure(self):
        """Test retry mechanism on transient failures."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['solar'],
            retry_config=RetryConfig(
                max_attempts=3,
                initial_delay=0.01,
                exponential_base=2.0
            )
        )

        attempts = 0

        async def mock_fetch_with_failure(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise ConnectionError("Temporary connection error")
            return {
                'solar': {
                    'forecast': create_sample_ned_forecast_data()
                }
            }

        collector._fetch_raw_data = mock_fetch_with_failure

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        result = await collector.collect(start, end)

        assert result is not None
        assert attempts == 2  # Failed once, succeeded on retry

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_failure(self):
        """Test collection failure after all retries exhausted."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['solar'],
            retry_config=RetryConfig(
                max_attempts=2,
                initial_delay=0.01
            )
        )

        async def mock_fetch_always_fails(*args, **kwargs):
            raise ConnectionError("API unavailable")

        collector._fetch_raw_data = mock_fetch_always_fails

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        result = await collector.collect(start, end)

        assert result is None

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        assert len(metrics) == 1
        assert metrics[0].status == CollectorStatus.FAILED

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_circuit_breaker_activation(self):
        """Test circuit breaker opens after consecutive failures."""
        collector = NedCollector(
            api_key="test_api_key",
            energy_types=['solar'],
            retry_config=RetryConfig(max_attempts=1, initial_delay=0.01),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=2,
                enabled=True
            )
        )

        async def mock_fetch_always_fails(*args, **kwargs):
            raise ConnectionError("API unavailable")

        collector._fetch_raw_data = mock_fetch_always_fails

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # First failure
        result1 = await collector.collect(start, end)
        assert result1 is None

        # Second failure - should open circuit
        result2 = await collector.collect(start, end)
        assert result2 is None

        # Third attempt - should be blocked by open circuit
        result3 = await collector.collect(start, end)
        assert result3 is None

        # Circuit breaker should be OPEN
        assert collector._circuit_breaker.state.value == "open"

    @pytest.mark.unit
    def test_normalize_timestamps_passthrough(self):
        """Test that _normalize_timestamps passes through data unchanged."""
        collector = NedCollector(api_key="test_api_key")

        data = {
            'solar': {
                'forecast': {
                    '2025-12-01T00:00:00+01:00': {'capacity_kw': 500.0}
                }
            }
        }

        result = collector._normalize_timestamps(data)

        # Should return same data (timestamps already normalized in _parse_response)
        assert result == data


class TestNedCollectorGranularity:
    """Tests for NedCollector granularity settings."""

    @pytest.mark.unit
    def test_granularity_10min(self):
        """Test initialization with 10-minute granularity."""
        collector = NedCollector(
            api_key="test_api_key",
            granularity='10min'
        )

        assert collector.granularity == '10min'

    @pytest.mark.unit
    def test_granularity_daily(self):
        """Test initialization with daily granularity."""
        collector = NedCollector(
            api_key="test_api_key",
            granularity='daily'
        )

        assert collector.granularity == 'daily'


class TestConvenienceFunction:
    """Tests for the convenience function."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_ned_production(self):
        """Test the convenience function."""
        from collectors.ned import get_ned_production

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # Patch the collector's fetch method
        with patch.object(NedCollector, '_fetch_raw_data') as mock_fetch:
            mock_fetch.return_value = {
                'solar': {
                    'forecast': create_sample_ned_forecast_data()
                }
            }

            result = await get_ned_production(
                api_key="test_api_key",
                start_time=start,
                end_time=end,
                energy_types=['solar']
            )

            assert result is not None
            assert 'solar' in result.data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
