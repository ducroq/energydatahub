"""
Unit tests for EntsogFlowsCollector

Tests the ENTSOG gas flow data collector.
"""
import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock, patch
import aiohttp

from collectors.entsog_flows import EntsogFlowsCollector
from collectors.base import (
    RetryConfig,
    CircuitBreakerConfig,
    CollectorStatus,
)
from utils.data_types import EnhancedDataSet


def create_sample_entsog_response():
    """Create sample ENTSOG API response mimicking what _fetch_raw_data returns."""
    # Note: _fetch_raw_data transforms 'operationalData' to 'records'
    return {
        'records': [
            {
                'periodFrom': '2025-01-15',
                'value': 50000000,  # 50 GWh in kWh
                'directionKey': 'entry',
                'pointType': 'Entry Point',
            },
            {
                'periodFrom': '2025-01-15',
                'value': 30000000,  # 30 GWh in kWh
                'directionKey': 'entry',
                'pointType': 'Entry Point',
            },
            {
                'periodFrom': '2025-01-15',
                'value': 40000000,  # 40 GWh in kWh
                'directionKey': 'exit',
                'pointType': 'Exit Point',
            },
            {
                'periodFrom': '2025-01-16',
                'value': 55000000,  # 55 GWh in kWh
                'directionKey': 'entry',
                'pointType': 'Entry Point',
            },
            {
                'periodFrom': '2025-01-16',
                'value': 45000000,  # 45 GWh in kWh
                'directionKey': 'exit',
                'pointType': 'Exit Point',
            },
        ],
        'meta': {
            'total': 5
        }
    }


def create_empty_entsog_response():
    """Create empty ENTSOG response."""
    return {
        'records': [],
        'meta': {'total': 0}
    }


class TestEntsogFlowsCollector:
    """Tests for EntsogFlowsCollector class."""

    @pytest.mark.unit
    def test_initialization_default(self):
        """Test ENTSOG flows collector initialization with defaults."""
        collector = EntsogFlowsCollector()

        assert collector.name == "EntsogFlowsCollector"
        assert collector.data_type == "gas_flows"
        assert collector.source == "ENTSOG Transparency Platform"
        assert collector.units == "kWh/d"
        assert collector.country_code == "NL"

    @pytest.mark.unit
    def test_initialization_custom_country(self):
        """Test initialization with custom country code."""
        collector = EntsogFlowsCollector(country_code="DE")

        assert collector.country_code == "DE"

    @pytest.mark.unit
    def test_initialization_lowercase_country(self):
        """Test initialization with lowercase country code."""
        collector = EntsogFlowsCollector(country_code="nl")

        assert collector.country_code == "NL"

    @pytest.mark.unit
    def test_base_url(self):
        """Test that base URL is defined correctly."""
        assert EntsogFlowsCollector.BASE_URL == "https://transparency.entsog.eu/api/v1"

    @pytest.mark.unit
    def test_country_zones(self):
        """Test that country zones mapping is defined."""
        assert 'NL' in EntsogFlowsCollector.COUNTRY_ZONES
        assert 'DE' in EntsogFlowsCollector.COUNTRY_ZONES
        assert 'BE' in EntsogFlowsCollector.COUNTRY_ZONES

    @pytest.mark.unit
    def test_parse_response_valid_data(self):
        """Test parsing response with valid ENTSOG data."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        raw_data = create_sample_entsog_response()
        parsed = collector._parse_response(raw_data, start, end)

        assert len(parsed) == 2  # Two days of data

        # Check first day has expected fields
        first_timestamp = '2025-01-15T00:00:00+01:00'
        assert first_timestamp in parsed
        first_data = parsed[first_timestamp]

        assert 'entry_total_gwh' in first_data
        assert 'exit_total_gwh' in first_data
        assert 'net_flow_gwh' in first_data
        assert 'entry_points' in first_data
        assert 'exit_points' in first_data

    @pytest.mark.unit
    def test_parse_response_flow_aggregation(self):
        """Test that flows are correctly aggregated by day."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        raw_data = create_sample_entsog_response()
        parsed = collector._parse_response(raw_data, start, end)

        # Day 1: 50 + 30 = 80 GWh entry, 40 GWh exit
        first_day = parsed['2025-01-15T00:00:00+01:00']
        assert first_day['entry_total_gwh'] == 80.0
        assert first_day['exit_total_gwh'] == 40.0
        assert first_day['net_flow_gwh'] == 40.0  # 80 - 40
        assert first_day['entry_points'] == 2
        assert first_day['exit_points'] == 1

        # Day 2: 55 GWh entry, 45 GWh exit
        second_day = parsed['2025-01-16T00:00:00+01:00']
        assert second_day['entry_total_gwh'] == 55.0
        assert second_day['exit_total_gwh'] == 45.0
        assert second_day['net_flow_gwh'] == 10.0  # 55 - 45

    @pytest.mark.unit
    def test_parse_response_empty(self):
        """Test parsing empty response."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        raw_data = create_empty_entsog_response()
        parsed = collector._parse_response(raw_data, start, end)

        assert parsed == {}

    @pytest.mark.unit
    def test_parse_response_missing_records_key(self):
        """Test parsing response with missing records key."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {'meta': {}}  # No operationalData key
        parsed = collector._parse_response(raw_data, start, end)

        assert parsed == {}

    @pytest.mark.unit
    def test_parse_response_invalid_date(self):
        """Test parsing response with invalid date skips that record."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {
            'records': [
                {
                    'periodFrom': 'invalid-date',
                    'value': 50000000,
                    'directionKey': 'entry',
                },
                {
                    'periodFrom': '2025-01-15',
                    'value': 30000000,
                    'directionKey': 'entry',
                },
            ]
        }

        parsed = collector._parse_response(raw_data, start, end)

        # Should still have the valid record
        assert len(parsed) == 1

    @pytest.mark.unit
    def test_parse_response_null_value(self):
        """Test parsing response with null value skips that record."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        raw_data = {
            'records': [
                {
                    'periodFrom': '2025-01-15',
                    'value': None,  # Null value
                    'directionKey': 'entry',
                },
            ]
        }

        parsed = collector._parse_response(raw_data, start, end)

        # Should have timestamp but with zero values
        if '2025-01-15T00:00:00+01:00' in parsed:
            assert parsed['2025-01-15T00:00:00+01:00']['entry_total_gwh'] == 0.0

    @pytest.mark.unit
    def test_validate_data_success(self):
        """Test data validation with valid data."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        valid_data = {
            '2025-01-15T00:00:00+01:00': {
                'entry_total_gwh': 80.0,
                'exit_total_gwh': 40.0,
                'net_flow_gwh': 40.0
            },
            '2025-01-16T00:00:00+01:00': {
                'entry_total_gwh': 55.0,
                'exit_total_gwh': 45.0,
                'net_flow_gwh': 10.0
            }
        }

        is_valid, warnings = collector._validate_data(valid_data, start, end)

        assert is_valid
        assert len(warnings) == 0

    @pytest.mark.unit
    def test_validate_data_empty(self):
        """Test data validation with empty data."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        is_valid, warnings = collector._validate_data({}, start, end)

        assert not is_valid
        assert len(warnings) > 0
        assert any('No gas flow data' in w for w in warnings)

    @pytest.mark.unit
    def test_validate_data_zero_flows(self):
        """Test validation with all zero flows."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        zero_data = {
            '2025-01-15T00:00:00+01:00': {
                'entry_total_gwh': 0.0,
                'exit_total_gwh': 0.0,
                'net_flow_gwh': 0.0
            }
        }

        is_valid, warnings = collector._validate_data(zero_data, start, end)

        assert len(warnings) > 0
        assert any('zero' in w.lower() for w in warnings)

    @pytest.mark.unit
    def test_metadata_includes_custom_fields(self):
        """Test that ENTSOG-specific metadata is included."""
        collector = EntsogFlowsCollector(country_code="DE")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

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
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        # Mock the fetch method
        async def mock_fetch(*args, **kwargs):
            return create_sample_entsog_response()

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
    async def test_collect_api_error(self):
        """Test collection with API error."""
        collector = EntsogFlowsCollector(
            retry_config=RetryConfig(max_attempts=1, initial_delay=0.01)
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        # Mock the fetch method to raise an error
        async def mock_fetch_error(*args, **kwargs):
            raise ValueError("ENTSOG API error 503: Service temporarily unavailable")

        collector._fetch_raw_data = mock_fetch_error

        result = await collector.collect(start, end)

        assert result is None

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        assert len(metrics) == 1
        assert metrics[0].status == CollectorStatus.FAILED

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_retry_on_failure(self):
        """Test retry mechanism on transient failures."""
        collector = EntsogFlowsCollector(
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
            return create_sample_entsog_response()

        collector._fetch_raw_data = mock_fetch_with_failure

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        result = await collector.collect(start, end)

        assert result is not None
        assert attempts == 2  # Failed once, succeeded on retry

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_failure(self):
        """Test collection failure after all retries exhausted."""
        collector = EntsogFlowsCollector(
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
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

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
        collector = EntsogFlowsCollector(
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
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        # First failure
        result1 = await collector.collect(start, end)
        assert result1 is None

        # Second failure - should open circuit
        result2 = await collector.collect(start, end)
        assert result2 is None

        # Circuit breaker should be OPEN
        assert collector._circuit_breaker.state.value == "open"

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_no_data(self):
        """Test collection with empty response."""
        collector = EntsogFlowsCollector(
            retry_config=RetryConfig(max_attempts=1, initial_delay=0.01)
        )

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        # Mock the fetch method to return empty response
        async def mock_fetch(*args, **kwargs):
            return create_empty_entsog_response()

        collector._fetch_raw_data = mock_fetch

        result = await collector.collect(start, end)

        # Result may be None or have empty data
        if result is not None:
            assert len(result.data) == 0 or result.data == {}


class TestConvenienceFunction:
    """Tests for the convenience function."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_gas_flows(self):
        """Test the convenience function."""
        from collectors.entsog_flows import get_gas_flows

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 17, 0, 0, tzinfo=amsterdam_tz)

        # Patch the collector's fetch method
        with patch.object(EntsogFlowsCollector, '_fetch_raw_data') as mock_fetch:
            mock_fetch.return_value = create_sample_entsog_response()

            result = await get_gas_flows(
                start_time=start,
                end_time=end,
                country_code='NL'
            )

            assert result is not None
            assert len(result.data) > 0


class TestKwhToGwhConversion:
    """Tests for kWh to GWh conversion."""

    @pytest.mark.unit
    def test_conversion_factor(self):
        """Test that kWh is correctly converted to GWh."""
        collector = EntsogFlowsCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 1, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 1, 16, 0, 0, tzinfo=amsterdam_tz)

        # 1,000,000 kWh = 1 GWh
        raw_data = {
            'records': [
                {
                    'periodFrom': '2025-01-15',
                    'value': 1000000,  # 1 GWh in kWh
                    'directionKey': 'entry',
                },
            ]
        }

        parsed = collector._parse_response(raw_data, start, end)

        assert parsed['2025-01-15T00:00:00+01:00']['entry_total_gwh'] == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
