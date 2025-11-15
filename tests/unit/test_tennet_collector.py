"""
Unit tests for TennetCollector

Tests the TenneT grid imbalance data collector using the tennet.eu API.
"""
import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock, patch, Mock
import pandas as pd

from collectors.tennet import TennetCollector
from collectors.base import (
    RetryConfig,
    CircuitBreakerConfig,
    CollectorStatus,
)
from utils.data_types import EnhancedDataSet


# Sample DataFrames for testing (mimicking tenneteu-py responses)
def create_sample_settlement_prices_df():
    """Create sample settlement prices DataFrame."""
    data = {
        'datetime': pd.to_datetime([
            '2025-11-15T00:00:00+01:00',
            '2025-11-15T00:15:00+01:00',
            '2025-11-15T00:30:00+01:00',
            '2025-11-15T00:45:00+01:00',
        ]),
        'price': [48.50, 52.30, 45.00, 85.00]
    }
    return pd.DataFrame(data)


def create_sample_balance_delta_df():
    """Create sample balance delta DataFrame."""
    data = {
        'datetime': pd.to_datetime([
            '2025-11-15T00:00:00+01:00',
            '2025-11-15T00:15:00+01:00',
            '2025-11-15T00:30:00+01:00',
            '2025-11-15T00:45:00+01:00',
        ]),
        'igcc': [-45.2, 12.8, -8.5, 150.0]
    }
    return pd.DataFrame(data)


class TestTennetCollector:
    """Tests for TennetCollector class."""

    @pytest.mark.unit
    def test_initialization(self):
        """Test TenneT collector initialization."""
        collector = TennetCollector(api_key="test_api_key")

        assert collector.name == "TennetCollector"
        assert collector.data_type == "grid_imbalance"
        assert collector.source == "TenneT TSO (tennet.eu API)"
        assert collector.units == "MW"
        assert collector.api_key == "test_api_key"

    @pytest.mark.unit
    def test_parse_dataframe_success(self):
        """Test successful DataFrame parsing."""
        collector = TennetCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        raw_data = {
            'settlement_prices': create_sample_settlement_prices_df(),
            'balance_delta': create_sample_balance_delta_df()
        }

        parsed = collector._parse_response(raw_data, start, end)

        assert len(parsed) == 4
        assert '2025-11-15T00:00:00+01:00' in parsed
        assert parsed['2025-11-15T00:00:00+01:00']['imbalance_price'] == 48.50
        assert parsed['2025-11-15T00:00:00+01:00']['balance_delta'] == -45.2
        assert parsed['2025-11-15T00:00:00+01:00']['direction'] == 'long'

    @pytest.mark.unit
    def test_parse_direction_calculation(self):
        """Test that direction is calculated correctly from balance delta sign."""
        collector = TennetCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        raw_data = {
            'settlement_prices': create_sample_settlement_prices_df(),
            'balance_delta': create_sample_balance_delta_df()
        }

        parsed = collector._parse_response(raw_data, start, end)

        # Negative balance delta should be 'long' (oversupply)
        assert parsed['2025-11-15T00:00:00+01:00']['balance_delta'] == -45.2
        assert parsed['2025-11-15T00:00:00+01:00']['direction'] == 'long'

        # Positive balance delta should be 'short' (undersupply)
        assert parsed['2025-11-15T00:15:00+01:00']['balance_delta'] == 12.8
        assert parsed['2025-11-15T00:15:00+01:00']['direction'] == 'short'

    @pytest.mark.unit
    def test_parse_empty_dataframe(self):
        """Test parsing with empty settlement prices DataFrame."""
        collector = TennetCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        raw_data = {
            'settlement_prices': pd.DataFrame(),
            'balance_delta': pd.DataFrame()
        }

        # Should raise ValueError for empty data
        with pytest.raises(ValueError, match="No settlement prices data received"):
            collector._parse_response(raw_data, start, end)

    @pytest.mark.unit
    def test_create_dataset_structure(self):
        """Test that dataset is created with correct structure."""
        collector = TennetCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        # Sample parsed data
        parsed_data = {
            '2025-11-15T00:00:00+01:00': {
                'imbalance_price': 48.50,
                'balance_delta': -45.2,
                'direction': 'long'
            },
            '2025-11-15T00:15:00+01:00': {
                'imbalance_price': 52.30,
                'balance_delta': 12.8,
                'direction': 'short'
            }
        }

        dataset = collector._create_dataset(parsed_data, start, end)

        assert isinstance(dataset, EnhancedDataSet)
        assert dataset.metadata['data_type'] == 'grid_imbalance'
        assert dataset.metadata['source'] == 'TenneT TSO (tennet.eu API)'
        assert dataset.metadata['country'] == 'NL'
        assert dataset.metadata['data_points'] == 2
        assert dataset.metadata['api_version'] == 'tennet.eu v1'

        # Check data structure
        assert 'imbalance_price' in dataset.data
        assert 'balance_delta' in dataset.data
        assert 'direction' in dataset.data

        # Check values
        assert dataset.data['imbalance_price']['2025-11-15T00:00:00+01:00'] == 48.50
        assert dataset.data['balance_delta']['2025-11-15T00:00:00+01:00'] == -45.2
        assert dataset.data['direction']['2025-11-15T00:00:00+01:00'] == 'long'

    @pytest.mark.unit
    def test_normalize_timestamps(self):
        """Test timestamp normalization."""
        collector = TennetCollector(api_key="test_api_key")

        # Data with UTC timestamps
        data = {
            '2025-11-15T00:00:00+00:00': {
                'imbalance_price': 48.50,
                'balance_delta': -45.2,
                'direction': 'long'
            }
        }

        normalized = collector._normalize_timestamps(data)

        # Should be converted to Amsterdam time (UTC+1 or UTC+2)
        timestamps = list(normalized.keys())
        assert len(timestamps) == 1
        # UTC 00:00 should be either 01:00+01:00 or 02:00+02:00 depending on DST
        assert '01:00:00+01:00' in timestamps[0] or '01:00:00+01:00' in timestamps[0]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_fetch_raw_data_with_mocked_client(self):
        """Test that fetch_raw_data calls the correct client methods."""
        collector = TennetCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 16, 23, 59, tzinfo=amsterdam_tz)

        # Mock the TenneTeuClient methods
        settlement_df = create_sample_settlement_prices_df()
        balance_df = create_sample_balance_delta_df()

        collector.client.query_settlement_prices = Mock(return_value=settlement_df)
        collector.client.query_balance_delta = Mock(return_value=balance_df)

        result = await collector._fetch_raw_data(start, end)

        # Verify client methods were called
        collector.client.query_settlement_prices.assert_called_once()
        collector.client.query_balance_delta.assert_called_once()

        # Check result
        assert 'settlement_prices' in result
        assert 'balance_delta' in result
        assert len(result['settlement_prices']) == 4
        assert len(result['balance_delta']) == 4

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_success(self):
        """Test successful end-to-end collection."""
        collector = TennetCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        # Mock the fetch method to return sample DataFrames
        async def mock_fetch(*args, **kwargs):
            return {
                'settlement_prices': create_sample_settlement_prices_df(),
                'balance_delta': create_sample_balance_delta_df()
            }

        collector._fetch_raw_data = mock_fetch

        result = await collector.collect(start, end)

        assert result is not None
        assert isinstance(result, EnhancedDataSet)
        assert result.metadata['data_points'] == 4

        # Check data structure
        assert 'imbalance_price' in result.data
        assert 'balance_delta' in result.data
        assert 'direction' in result.data

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        assert len(metrics) == 1
        assert metrics[0].status == CollectorStatus.SUCCESS
        assert metrics[0].data_points_collected == 4

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_retry_on_failure(self):
        """Test retry mechanism on transient failures."""
        collector = TennetCollector(
            api_key="test_api_key",
            retry_config=RetryConfig(
                max_attempts=3,
                initial_delay=0.01,  # Fast for testing
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
                'settlement_prices': create_sample_settlement_prices_df(),
                'balance_delta': create_sample_balance_delta_df()
            }

        collector._fetch_raw_data = mock_fetch_with_failure

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        result = await collector.collect(start, end)

        assert result is not None
        assert attempts == 2  # Failed once, succeeded on retry

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_failure(self):
        """Test collection failure after all retries exhausted."""
        collector = TennetCollector(
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
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        result = await collector.collect(start, end)

        assert result is None  # Should fail

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        assert len(metrics) == 1
        assert metrics[0].status == CollectorStatus.FAILED

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_circuit_breaker_activation(self):
        """Test circuit breaker opens after consecutive failures."""
        collector = TennetCollector(
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
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

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
    def test_metadata_includes_custom_fields(self):
        """Test that TenneT-specific metadata is included."""
        collector = TennetCollector(api_key="test_api_key")

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        metadata = collector._get_metadata(start, end)

        assert metadata['country_code'] == 'NL'
        assert metadata['market'] == 'transmission'
        assert metadata['resolution'] == 'PTU (15 minutes)'
        assert 'data_fields' in metadata
        assert 'imbalance_price' in metadata['data_fields']
        assert 'balance_delta' in metadata['data_fields']
        assert 'direction' in metadata['data_fields']
        assert metadata['api_version'] == 'tennet.eu v1'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
