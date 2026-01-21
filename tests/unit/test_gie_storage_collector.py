"""
Unit tests for GieStorageCollector

Tests the GIE AGSI+ gas storage data collector.
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock, patch

from collectors.gie_storage import GieStorageCollector
from collectors.base import (
    RetryConfig,
    CircuitBreakerConfig,
    CollectorStatus,
)
from utils.data_types import EnhancedDataSet


def create_sample_storage_dataframe():
    """Create sample GIE storage data mimicking gie-py response."""
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    dates = [
        datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz),
        datetime(2025, 1, 16, 0, 0, tzinfo=amsterdam_tz),
        datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz),
    ]

    data = {
        'gasDayStart': dates,
        'full': [72.5, 71.8, 71.2],  # Fill level percentage
        'gasInStorage': [145.2, 143.8, 142.5],  # TWh
        'injection': [0, 0, 0],  # GWh/day
        'withdrawal': [892, 950, 1050],  # GWh/day
        'workingGasVolume': [200.0, 200.0, 200.0],  # TWh reference
    }

    return pd.DataFrame(data)


def create_empty_dataframe():
    """Create empty DataFrame."""
    return pd.DataFrame()


class TestGieStorageCollector:
    """Tests for GieStorageCollector class."""

    @pytest.mark.unit
    def test_initialization_default(self):
        """Test GIE storage collector initialization with defaults."""
        collector = GieStorageCollector(api_key="test_api_key")

        assert collector.name == "GieStorageCollector"
        assert collector.data_type == "gas_storage"
        assert collector.source == "GIE AGSI+"
        assert collector.units == "percent"
        assert collector.api_key == "test_api_key"
        assert collector.country_code == "NL"

    @pytest.mark.unit
    def test_initialization_custom_country(self):
        """Test initialization with custom country code."""
        collector = GieStorageCollector(
            api_key="test_api_key",
            country_code="DE"
        )

        assert collector.country_code == "DE"

    @pytest.mark.unit
    def test_initialization_invalid_country(self):
        """Test initialization with invalid country code raises error."""
        with pytest.raises(ValueError) as exc_info:
            GieStorageCollector(
                api_key="test_api_key",
                country_code="XX"
            )

        assert "Unsupported country code" in str(exc_info.value)
        assert "XX" in str(exc_info.value)

    @pytest.mark.unit
    def test_supported_countries(self):
        """Test that supported countries list is defined."""
        assert 'NL' in GieStorageCollector.SUPPORTED_COUNTRIES
        assert 'DE' in GieStorageCollector.SUPPORTED_COUNTRIES
        assert 'BE' in GieStorageCollector.SUPPORTED_COUNTRIES
        assert 'FR' in GieStorageCollector.SUPPORTED_COUNTRIES
        assert len(GieStorageCollector.SUPPORTED_COUNTRIES) >= 10

    @pytest.mark.unit
    def test_parse_response_valid_data(self):
        """Test parsing response with valid DataFrame."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        df = create_sample_storage_dataframe()
        parsed = collector._parse_response(df, start, end)

        assert len(parsed) == 3

        # Check first data point has expected fields
        first_timestamp = list(parsed.keys())[0]
        first_data = parsed[first_timestamp]

        assert 'fill_level_pct' in first_data
        assert 'working_capacity_twh' in first_data
        assert 'injection_gwh' in first_data
        assert 'withdrawal_gwh' in first_data
        assert 'net_change_gwh' in first_data

    @pytest.mark.unit
    def test_parse_response_fill_level_values(self):
        """Test that fill level values are correctly parsed."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        df = create_sample_storage_dataframe()
        parsed = collector._parse_response(df, start, end)

        # Check fill levels are in expected range
        for ts, values in parsed.items():
            fill_pct = values.get('fill_level_pct')
            assert fill_pct is not None
            assert 0 <= fill_pct <= 100

    @pytest.mark.unit
    def test_parse_response_net_change_calculation(self):
        """Test that net change is calculated correctly."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        df = create_sample_storage_dataframe()
        parsed = collector._parse_response(df, start, end)

        # Check net change calculation (injection - withdrawal)
        for ts, values in parsed.items():
            injection = values.get('injection_gwh', 0)
            withdrawal = values.get('withdrawal_gwh', 0)
            net_change = values.get('net_change_gwh')
            assert net_change == injection - withdrawal

    @pytest.mark.unit
    def test_parse_response_empty_dataframe(self):
        """Test parsing empty DataFrame."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        df = create_empty_dataframe()
        parsed = collector._parse_response(df, start, end)

        assert parsed == {}

    @pytest.mark.unit
    def test_parse_response_none_input(self):
        """Test parsing None input."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        parsed = collector._parse_response(None, start, end)

        assert parsed == {}

    @pytest.mark.unit
    def test_validate_data_success(self):
        """Test data validation with valid data."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        valid_data = {
            '2025-01-15T00:00:00+01:00': {
                'fill_level_pct': 72.5,
                'working_capacity_twh': 145.2,
                'withdrawal_gwh': 892
            },
            '2025-01-16T00:00:00+01:00': {
                'fill_level_pct': 71.8,
                'working_capacity_twh': 143.8,
                'withdrawal_gwh': 950
            }
        }

        is_valid, warnings = collector._validate_data(valid_data, start, end)

        assert is_valid
        assert len(warnings) == 0

    @pytest.mark.unit
    def test_validate_data_empty(self):
        """Test data validation with empty data."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        is_valid, warnings = collector._validate_data({}, start, end)

        assert not is_valid
        assert len(warnings) > 0
        assert any('No gas storage data' in w for w in warnings)

    @pytest.mark.unit
    def test_validate_data_invalid_fill_level(self):
        """Test validation with invalid fill level (>100%)."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        invalid_data = {
            '2025-01-15T00:00:00+01:00': {
                'fill_level_pct': 150.0,  # Invalid
            }
        }

        is_valid, warnings = collector._validate_data(invalid_data, start, end)

        assert len(warnings) > 0
        assert any('Invalid fill level' in w for w in warnings)

    @pytest.mark.unit
    def test_validate_data_no_fill_level(self):
        """Test validation with missing fill level data."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        data_without_fill = {
            '2025-01-15T00:00:00+01:00': {
                'working_capacity_twh': 145.2,
            }
        }

        is_valid, warnings = collector._validate_data(data_without_fill, start, end)

        assert len(warnings) > 0
        assert any('No fill level data' in w for w in warnings)

    @pytest.mark.unit
    def test_metadata_includes_custom_fields(self):
        """Test that GIE-specific metadata is included."""
        collector = GieStorageCollector(
            api_key="test_api_key",
            country_code="DE"
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        metadata = collector._get_metadata(start, end)

        assert metadata['country_code'] == 'DE'
        assert metadata['data_frequency'] == 'daily'
        assert 'description' in metadata
        assert 'usage_notes' in metadata
        assert 'api_documentation' in metadata

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_success(self):
        """Test successful end-to-end collection."""
        collector = GieStorageCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        # Mock the fetch method
        async def mock_fetch(*args, **kwargs):
            return create_sample_storage_dataframe()

        collector._fetch_raw_data = mock_fetch

        result = await collector.collect(start, end)

        assert result is not None
        assert isinstance(result, EnhancedDataSet)
        assert len(result.data) > 0

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        assert len(metrics) == 1
        assert metrics[0].status in [CollectorStatus.SUCCESS, CollectorStatus.PARTIAL]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_no_data(self):
        """Test collection with empty response."""
        collector = GieStorageCollector(
            api_key="test_api_key",
            retry_config=RetryConfig(max_attempts=1, initial_delay=0.01)
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        # Mock the fetch method to return empty DataFrame
        async def mock_fetch(*args, **kwargs):
            return create_empty_dataframe()

        collector._fetch_raw_data = mock_fetch

        result = await collector.collect(start, end)

        # Result may be None or have empty data
        if result is not None:
            assert len(result.data) == 0 or result.data == {}

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_retry_on_failure(self):
        """Test retry mechanism on transient failures."""
        collector = GieStorageCollector(
            api_key="test_api_key",
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
            return create_sample_storage_dataframe()

        collector._fetch_raw_data = mock_fetch_with_failure

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        result = await collector.collect(start, end)

        assert result is not None
        assert attempts == 2  # Failed once, succeeded on retry

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_failure(self):
        """Test collection failure after all retries exhausted."""
        collector = GieStorageCollector(
            api_key="test_api_key",
            retry_config=RetryConfig(
                max_attempts=2,
                initial_delay=0.01
            )
        )

        async def mock_fetch_always_fails(*args, **kwargs):
            raise ConnectionError("API unavailable")

        collector._fetch_raw_data = mock_fetch_always_fails

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

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
        collector = GieStorageCollector(
            api_key="test_api_key",
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
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        # First failure
        result1 = await collector.collect(start, end)
        assert result1 is None

        # Second failure - should open circuit
        result2 = await collector.collect(start, end)
        assert result2 is None

        # Circuit breaker should be OPEN
        assert collector._circuit_breaker.state.value == "open"


class TestConvenienceFunction:
    """Tests for the convenience function."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_gas_storage(self):
        """Test the convenience function."""
        from collectors.gie_storage import get_gas_storage

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 18, 0, 0, tzinfo=amsterdam_tz)

        # Patch the collector's fetch method
        with patch.object(GieStorageCollector, '_fetch_raw_data') as mock_fetch:
            mock_fetch.return_value = create_sample_storage_dataframe()

            result = await get_gas_storage(
                api_key="test_api_key",
                start_time=start,
                end_time=end,
                country_code='NL'
            )

            assert result is not None
            assert len(result.data) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
