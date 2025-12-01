"""
Unit tests for EntsoeWindCollector

Tests the ENTSO-E wind power generation forecast collector.
"""
import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock, patch, Mock
import pandas as pd
import numpy as np

from collectors.entsoe_wind import EntsoeWindCollector
from collectors.base import (
    RetryConfig,
    CircuitBreakerConfig,
    CollectorStatus,
)
from utils.data_types import EnhancedDataSet


def create_sample_wind_forecast_df():
    """Create sample wind forecast DataFrame mimicking ENTSO-E response."""
    timestamps = pd.date_range(
        start='2025-12-01T00:00:00+00:00',
        periods=24,
        freq='h',
        tz='UTC'
    )

    # ENTSO-E returns data with columns like 'Wind Offshore', 'Wind Onshore', 'Solar'
    data = {
        'Wind Offshore': np.random.uniform(500, 2000, 24),
        'Wind Onshore': np.random.uniform(200, 800, 24),
        'Solar': np.random.uniform(0, 500, 24)  # Not used but often present
    }

    df = pd.DataFrame(data, index=timestamps)
    return df


def create_sample_wind_total_df():
    """Create sample wind DataFrame with only total (no offshore/onshore split)."""
    timestamps = pd.date_range(
        start='2025-12-01T00:00:00+00:00',
        periods=24,
        freq='h',
        tz='UTC'
    )

    data = {
        'Wind': np.random.uniform(700, 2500, 24),
        'Solar': np.random.uniform(0, 500, 24)
    }

    df = pd.DataFrame(data, index=timestamps)
    return df


class TestEntsoeWindCollector:
    """Tests for EntsoeWindCollector class."""

    @pytest.mark.unit
    def test_initialization(self):
        """Test ENTSO-E Wind collector initialization."""
        collector = EntsoeWindCollector(api_key="test_api_key")

        assert collector.name == "EntsoeWindCollector"
        assert collector.data_type == "wind_generation"
        assert collector.source == "ENTSO-E Transparency Platform API v1.3"
        assert collector.units == "MW"
        assert collector.api_key == "test_api_key"
        assert collector.country_codes == ['NL', 'DE_LU']  # Default

    @pytest.mark.unit
    def test_initialization_custom_countries(self):
        """Test initialization with custom country codes."""
        collector = EntsoeWindCollector(
            api_key="test_api_key",
            country_codes=['NL', 'BE', 'DK_1']
        )

        assert collector.country_codes == ['NL', 'BE', 'DK_1']

    @pytest.mark.unit
    def test_supported_countries(self):
        """Test that supported countries are defined."""
        assert 'NL' in EntsoeWindCollector.SUPPORTED_COUNTRIES
        assert 'DE_LU' in EntsoeWindCollector.SUPPORTED_COUNTRIES
        assert 'BE' in EntsoeWindCollector.SUPPORTED_COUNTRIES
        assert 'DK_1' in EntsoeWindCollector.SUPPORTED_COUNTRIES
        assert 'DK_2' in EntsoeWindCollector.SUPPORTED_COUNTRIES

    @pytest.mark.unit
    def test_zone_names(self):
        """Test that zone name mappings exist."""
        assert EntsoeWindCollector.ZONE_NAMES['NL'] == 'Netherlands'
        assert EntsoeWindCollector.ZONE_NAMES['DE_LU'] == 'Germany-Luxembourg'
        assert EntsoeWindCollector.ZONE_NAMES['BE'] == 'Belgium'

    @pytest.mark.unit
    def test_parse_response_with_offshore_onshore(self):
        """Test parsing response with offshore and onshore breakdown."""
        collector = EntsoeWindCollector(api_key="test_api_key", country_codes=['NL'])

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {'NL': create_sample_wind_forecast_df()}

        parsed = collector._parse_response(raw_data, start, end)

        assert 'NL' in parsed
        assert len(parsed['NL']) > 0

        # Check first data point has expected fields
        first_timestamp = list(parsed['NL'].keys())[0]
        first_data = parsed['NL'][first_timestamp]

        assert 'wind_offshore' in first_data
        assert 'wind_onshore' in first_data
        assert 'wind_total' in first_data

        # Total should be sum of offshore + onshore
        assert first_data['wind_total'] == pytest.approx(
            first_data['wind_offshore'] + first_data['wind_onshore'],
            rel=0.01
        )

    @pytest.mark.unit
    def test_parse_response_with_total_only(self):
        """Test parsing response with only total wind (no offshore/onshore split)."""
        collector = EntsoeWindCollector(api_key="test_api_key", country_codes=['BE'])

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {'BE': create_sample_wind_total_df()}

        parsed = collector._parse_response(raw_data, start, end)

        assert 'BE' in parsed
        assert len(parsed['BE']) > 0

        # Check first data point has total
        first_timestamp = list(parsed['BE'].keys())[0]
        first_data = parsed['BE'][first_timestamp]

        assert 'wind_total' in first_data
        # Offshore/onshore may or may not be present depending on data

    @pytest.mark.unit
    def test_parse_response_multi_country(self):
        """Test parsing response with multiple countries."""
        collector = EntsoeWindCollector(
            api_key="test_api_key",
            country_codes=['NL', 'DE_LU']
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {
            'NL': create_sample_wind_forecast_df(),
            'DE_LU': create_sample_wind_total_df()
        }

        parsed = collector._parse_response(raw_data, start, end)

        assert 'NL' in parsed
        assert 'DE_LU' in parsed
        assert len(parsed['NL']) > 0
        assert len(parsed['DE_LU']) > 0

    @pytest.mark.unit
    def test_validate_data_success(self):
        """Test data validation with valid data."""
        collector = EntsoeWindCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # Create valid data with 24 data points
        valid_data = {
            'NL': {
                f'2025-12-01T{h:02d}:00:00+01:00': {
                    'wind_offshore': 1500.0,
                    'wind_onshore': 800.0,
                    'wind_total': 2300.0
                }
                for h in range(24)
            }
        }

        is_valid, warnings = collector._validate_data(valid_data, start, end)

        assert is_valid
        assert len(warnings) == 0

    @pytest.mark.unit
    def test_validate_data_insufficient_points(self):
        """Test data validation with insufficient data points."""
        collector = EntsoeWindCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # Only 5 data points (should warn)
        sparse_data = {
            'NL': {
                f'2025-12-01T{h:02d}:00:00+01:00': {
                    'wind_total': 1500.0
                }
                for h in range(5)
            }
        }

        is_valid, warnings = collector._validate_data(sparse_data, start, end)

        # Should have warning about insufficient points
        assert len(warnings) > 0
        assert any('5 data points' in w for w in warnings)

    @pytest.mark.unit
    def test_validate_data_empty(self):
        """Test data validation with empty data."""
        collector = EntsoeWindCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        is_valid, warnings = collector._validate_data({}, start, end)

        assert not is_valid
        assert len(warnings) > 0

    @pytest.mark.unit
    def test_metadata_includes_custom_fields(self):
        """Test that wind-specific metadata is included."""
        collector = EntsoeWindCollector(
            api_key="test_api_key",
            country_codes=['NL', 'DE_LU', 'BE']
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        metadata = collector._get_metadata(start, end)

        assert metadata['country_codes'] == ['NL', 'DE_LU', 'BE']
        assert 'zones' in metadata
        assert metadata['zones']['NL'] == 'Netherlands'
        assert metadata['forecast_type'] == 'day-ahead'
        assert metadata['resolution'] == 'hourly'
        assert metadata['api_version'] == 'v1.3'
        assert 'description' in metadata

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_success(self):
        """Test successful end-to-end collection."""
        collector = EntsoeWindCollector(
            api_key="test_api_key",
            country_codes=['NL']
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 12, 1, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 12, 2, 0, 0, tzinfo=amsterdam_tz)

        # Mock the fetch method
        async def mock_fetch(*args, **kwargs):
            return {'NL': create_sample_wind_forecast_df()}

        collector._fetch_raw_data = mock_fetch

        result = await collector.collect(start, end)

        assert result is not None
        assert isinstance(result, EnhancedDataSet)
        assert 'NL' in result.data
        assert len(result.data['NL']) > 0

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        assert len(metrics) == 1
        assert metrics[0].status in [CollectorStatus.SUCCESS, CollectorStatus.PARTIAL]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_retry_on_failure(self):
        """Test retry mechanism on transient failures."""
        collector = EntsoeWindCollector(
            api_key="test_api_key",
            country_codes=['NL'],
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
            return {'NL': create_sample_wind_forecast_df()}

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
        collector = EntsoeWindCollector(
            api_key="test_api_key",
            country_codes=['NL'],
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
        collector = EntsoeWindCollector(
            api_key="test_api_key",
            country_codes=['NL'],
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


class TestExtractWindFromWeather:
    """Tests for the _extract_wind_from_weather helper function."""

    @pytest.mark.unit
    def test_extract_wind_from_weather_basic(self):
        """Test basic wind extraction from weather data."""
        from data_fetcher import _extract_wind_from_weather
        from utils.data_types import EnhancedDataSet

        # Create mock weather data
        weather_data = EnhancedDataSet(
            metadata={'data_type': 'weather'},
            data={
                'Borssele_NL': {
                    '2025-12-01T00:00:00+01:00': {
                        'temperature': 8.5,
                        'wind_speed': 12.3,
                        'wind_direction': 270,
                        'humidity': 82
                    },
                    '2025-12-01T01:00:00+01:00': {
                        'temperature': 8.2,
                        'wind_speed': 13.1,
                        'wind_direction': 265,
                        'humidity': 84
                    }
                },
                'Arnhem_NL': {  # Not an offshore location
                    '2025-12-01T00:00:00+01:00': {
                        'temperature': 7.5,
                        'wind_speed': 5.0,
                        'wind_direction': 180,
                        'humidity': 75
                    }
                }
            }
        )

        offshore_locations = [
            {"name": "Borssele_NL", "lat": 51.7, "lon": 3.0}
        ]

        result = _extract_wind_from_weather(weather_data, offshore_locations)

        assert result is not None
        assert 'Borssele_NL' in result
        assert 'Arnhem_NL' not in result  # Not in offshore list

        # Check wind data extracted correctly
        assert 'wind_speed' in result['Borssele_NL']['2025-12-01T00:00:00+01:00']
        assert 'wind_direction' in result['Borssele_NL']['2025-12-01T00:00:00+01:00']
        assert result['Borssele_NL']['2025-12-01T00:00:00+01:00']['wind_speed'] == 12.3

        # Non-wind fields should not be present
        assert 'temperature' not in result['Borssele_NL']['2025-12-01T00:00:00+01:00']
        assert 'humidity' not in result['Borssele_NL']['2025-12-01T00:00:00+01:00']

    @pytest.mark.unit
    def test_extract_wind_from_weather_empty(self):
        """Test extraction with no matching locations."""
        from data_fetcher import _extract_wind_from_weather
        from utils.data_types import EnhancedDataSet

        weather_data = EnhancedDataSet(
            metadata={'data_type': 'weather'},
            data={
                'NonMatchingLocation': {
                    '2025-12-01T00:00:00+01:00': {
                        'wind_speed': 10.0,
                        'wind_direction': 180
                    }
                }
            }
        )

        offshore_locations = [
            {"name": "Borssele_NL", "lat": 51.7, "lon": 3.0}
        ]

        result = _extract_wind_from_weather(weather_data, offshore_locations)

        assert result is None

    @pytest.mark.unit
    def test_extract_wind_from_weather_none_input(self):
        """Test extraction with None input."""
        from data_fetcher import _extract_wind_from_weather

        result = _extract_wind_from_weather(None, [])
        assert result is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
