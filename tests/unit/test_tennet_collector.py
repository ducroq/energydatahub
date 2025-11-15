"""
Unit tests for TennetCollector

Tests the TenneT grid imbalance data collector.
"""
import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock, patch

from collectors.tennet import TennetCollector
from collectors.base import (
    RetryConfig,
    CircuitBreakerConfig,
    CollectorStatus,
)
from utils.data_types import EnhancedDataSet


# Sample CSV data for testing
SAMPLE_CSV = """DateTime,SystemImbalance_MW,ImbalancePrice_EUR_MWh,Direction
2025-11-15T00:00:00+01:00,-45.2,48.50,long
2025-11-15T01:00:00+01:00,12.8,52.30,short
2025-11-15T02:00:00+01:00,-8.5,45.00,long
2025-11-15T03:00:00+01:00,150.0,85.00,short"""


class TestTennetCollector:
    """Tests for TennetCollector class."""

    @pytest.mark.unit
    def test_initialization(self):
        """Test TenneT collector initialization."""
        collector = TennetCollector()

        assert collector.name == "TennetCollector"
        assert collector.data_type == "grid_imbalance"
        assert collector.source == "TenneT TSO"
        assert collector.units == "MW"

    @pytest.mark.unit
    def test_parse_csv_success(self):
        """Test successful CSV parsing."""
        collector = TennetCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        raw_data = {'csv_content': SAMPLE_CSV}
        parsed = collector._parse_response(raw_data, start, end)

        assert len(parsed) == 4
        assert '2025-11-15T00:00:00+01:00' in parsed
        assert parsed['2025-11-15T00:00:00+01:00']['imbalance_mw'] == -45.2
        assert parsed['2025-11-15T00:00:00+01:00']['price_eur_mwh'] == 48.50
        assert parsed['2025-11-15T00:00:00+01:00']['direction'] == 'long'

    @pytest.mark.unit
    def test_parse_csv_direction_calculation(self):
        """Test that direction is calculated correctly from imbalance sign."""
        collector = TennetCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        raw_data = {'csv_content': SAMPLE_CSV}
        parsed = collector._parse_response(raw_data, start, end)

        # Negative imbalance should be 'long' (oversupply)
        assert parsed['2025-11-15T00:00:00+01:00']['imbalance_mw'] == -45.2
        assert parsed['2025-11-15T00:00:00+01:00']['direction'] == 'long'

        # Positive imbalance should be 'short' (undersupply)
        assert parsed['2025-11-15T01:00:00+01:00']['imbalance_mw'] == 12.8
        assert parsed['2025-11-15T01:00:00+01:00']['direction'] == 'short'

    @pytest.mark.unit
    def test_parse_csv_malformed_row(self):
        """Test CSV parsing with malformed rows."""
        collector = TennetCollector()

        # CSV with one good row and one malformed row
        malformed_csv = """DateTime,SystemImbalance_MW,ImbalancePrice_EUR_MWh,Direction
2025-11-15T00:00:00+01:00,-45.2,48.50,long
2025-11-15T01:00:00+01:00,invalid,52.30,short"""

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        raw_data = {'csv_content': malformed_csv}
        parsed = collector._parse_response(raw_data, start, end)

        # Should only parse the valid row
        assert len(parsed) == 1
        assert '2025-11-15T00:00:00+01:00' in parsed

    @pytest.mark.unit
    def test_parse_csv_empty(self):
        """Test CSV parsing with empty data."""
        collector = TennetCollector()

        # CSV with only header
        empty_csv = """DateTime,SystemImbalance_MW,ImbalancePrice_EUR_MWh,Direction"""

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        raw_data = {'csv_content': empty_csv}

        # Should raise ValueError for empty data
        with pytest.raises(ValueError, match="No valid data points parsed"):
            collector._parse_response(raw_data, start, end)

    @pytest.mark.unit
    def test_create_dataset_structure(self):
        """Test that dataset is created with correct structure."""
        collector = TennetCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        # Sample parsed data
        parsed_data = {
            '2025-11-15T00:00:00+01:00': {
                'imbalance_mw': -45.2,
                'price_eur_mwh': 48.50,
                'direction': 'long'
            },
            '2025-11-15T01:00:00+01:00': {
                'imbalance_mw': 12.8,
                'price_eur_mwh': 52.30,
                'direction': 'short'
            }
        }

        dataset = collector._create_dataset(parsed_data, start, end)

        assert isinstance(dataset, EnhancedDataSet)
        assert dataset.metadata['data_type'] == 'grid_imbalance'
        assert dataset.metadata['source'] == 'TenneT TSO'
        assert dataset.metadata['units'] == 'MW'
        assert dataset.metadata['country'] == 'NL'
        assert dataset.metadata['data_points'] == 2

        # Check data structure
        assert 'imbalance' in dataset.data
        assert 'imbalance_price' in dataset.data
        assert 'direction' in dataset.data

        # Check values
        assert dataset.data['imbalance']['2025-11-15T00:00:00+01:00'] == -45.2
        assert dataset.data['imbalance_price']['2025-11-15T00:00:00+01:00'] == 48.50
        assert dataset.data['direction']['2025-11-15T00:00:00+01:00'] == 'long'

    @pytest.mark.unit
    def test_normalize_timestamps(self):
        """Test timestamp normalization."""
        collector = TennetCollector()

        # Data with UTC timestamps
        data = {
            '2025-11-15T00:00:00+00:00': {
                'imbalance_mw': -45.2,
                'price_eur_mwh': 48.50,
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
    async def test_fetch_raw_data_parameters(self):
        """Test that fetch_raw_data constructs correct API parameters."""
        collector = TennetCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 16, 23, 59, tzinfo=amsterdam_tz)

        # Mock aiohttp session
        mock_response = MagicMock()
        mock_response.text = AsyncMock(return_value=SAMPLE_CSV)
        mock_response.raise_for_status = MagicMock()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await collector._fetch_raw_data(start, end)

            # Verify session.get was called with correct parameters
            call_args = mock_session.get.call_args
            assert call_args is not None

            # Check URL
            assert collector.BASE_URL in call_args[0]

            # Check params
            params = call_args[1]['params']
            assert params['DataType'] == 'SystemImbalance'
            assert params['StartDate'] == '2025-11-15'
            assert params['EndDate'] == '2025-11-16'
            assert params['Output'] == 'csv'

            # Check result
            assert 'csv_content' in result
            assert result['csv_content'] == SAMPLE_CSV

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_collect_success(self):
        """Test successful end-to-end collection."""
        collector = TennetCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        # Mock the fetch method to return sample CSV
        async def mock_fetch(*args, **kwargs):
            return {'csv_content': SAMPLE_CSV}

        collector._fetch_raw_data = mock_fetch

        result = await collector.collect(start, end)

        assert result is not None
        assert isinstance(result, EnhancedDataSet)
        assert result.metadata['data_points'] == 4

        # Check data structure
        assert 'imbalance' in result.data
        assert 'imbalance_price' in result.data
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
            return {'csv_content': SAMPLE_CSV}

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
        collector = TennetCollector()

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

        metadata = collector._get_metadata(start, end)

        assert metadata['country_code'] == 'NL'
        assert metadata['market'] == 'transmission'
        assert metadata['resolution'] == 'hourly'
        assert 'data_fields' in metadata
        assert 'imbalance_mw' in metadata['data_fields']
        assert 'price_eur_mwh' in metadata['data_fields']
        assert 'direction' in metadata['data_fields']


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
