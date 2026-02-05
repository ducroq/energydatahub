"""
Integration test for Nord Pool timezone fix

This test verifies that the timezone handling works correctly with the
ElspotCollector (pynordpool-based) and validates timestamp formats.

Updated: 2026-02-05 - Migrated from legacy nordpool to pynordpool
"""
import pytest
import asyncio
import platform
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pytz

# Mock the pynordpool client since we don't want to hit the real API
from unittest.mock import Mock, patch, MagicMock, AsyncMock

# Fix Windows event loop for aiodns compatibility
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def create_mock_session():
    """Create a properly mocked aiohttp.ClientSession for async context manager usage."""
    mock_session = MagicMock()
    # Make it work as an async context manager
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)
    return mock_session


@pytest.mark.integration
@pytest.mark.asyncio
async def test_elspot_collector_produces_correct_timestamps():
    """Test that ElspotCollector produces correct Amsterdam timezone timestamps"""
    from collectors.elspot import ElspotCollector

    # Create mock delivery data (pynordpool format)
    mock_entry1 = MagicMock()
    mock_entry1.start = datetime(2025, 10, 24, 0, 0, 0, tzinfo=ZoneInfo('UTC'))
    mock_entry1.entry = {'NL': 100.5}

    mock_entry2 = MagicMock()
    mock_entry2.start = datetime(2025, 10, 24, 1, 0, 0, tzinfo=ZoneInfo('UTC'))
    mock_entry2.entry = {'NL': 95.3}

    mock_entry3 = MagicMock()
    mock_entry3.start = datetime(2025, 10, 24, 2, 0, 0, tzinfo=ZoneInfo('UTC'))
    mock_entry3.entry = {'NL': 102.1}

    mock_delivery_data = MagicMock()
    mock_delivery_data.entries = [mock_entry1, mock_entry2, mock_entry3]
    mock_delivery_data.requested_date = datetime(2025, 10, 24).date()
    mock_delivery_data.currency = 'EUR'

    # Mock the pynordpool NordPoolClient
    mock_client_instance = MagicMock()
    mock_client_instance.async_get_delivery_period = AsyncMock(return_value=mock_delivery_data)

    # Create mock session that works as async context manager
    mock_session = create_mock_session()

    with patch('collectors.elspot.NordPoolClient', return_value=mock_client_instance):
        with patch('collectors.elspot.aiohttp.ClientSession', return_value=mock_session):
            collector = ElspotCollector()
            amsterdam_tz = ZoneInfo('Europe/Amsterdam')
            start_time = datetime(2025, 10, 24, 0, 0, 0, tzinfo=amsterdam_tz)
            end_time = datetime(2025, 10, 25, 0, 0, 0, tzinfo=amsterdam_tz)

            result = await collector.collect(start_time, end_time, country_code='NL')

    # Verify the result
    assert result is not None, "Result should not be None"
    assert hasattr(result, 'data'), "Result should have data attribute"
    assert result.data is not None, "Result.data should not be None"
    assert len(result.data) > 0, "Result.data should have entries"

    # Check that all timestamps have correct format
    for timestamp_str in result.data.keys():
        # Should NOT contain malformed offsets
        assert '+00:09' not in timestamp_str, f"Found malformed +00:09 in {timestamp_str}"
        assert '+00:18' not in timestamp_str, f"Found malformed +00:18 in {timestamp_str}"

        # Should contain valid Amsterdam offset
        # October 24 is in CEST period (summer time, +02:00)
        assert '+02:00' in timestamp_str or '+01:00' in timestamp_str, \
            f"Timestamp {timestamp_str} doesn't have valid Amsterdam offset"

    print("[PASS] All timestamps have correct timezone offsets")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_elspot_collector_winter_timestamps():
    """Test ElspotCollector with winter CET timestamps"""
    from collectors.elspot import ElspotCollector

    # Create mock delivery data for winter (January)
    mock_entry1 = MagicMock()
    mock_entry1.start = datetime(2025, 1, 15, 0, 0, 0, tzinfo=ZoneInfo('UTC'))
    mock_entry1.entry = {'NL': 110.5}

    mock_entry2 = MagicMock()
    mock_entry2.start = datetime(2025, 1, 15, 1, 0, 0, tzinfo=ZoneInfo('UTC'))
    mock_entry2.entry = {'NL': 105.3}

    mock_delivery_data = MagicMock()
    mock_delivery_data.entries = [mock_entry1, mock_entry2]
    mock_delivery_data.requested_date = datetime(2025, 1, 15).date()
    mock_delivery_data.currency = 'EUR'

    # Mock the pynordpool NordPoolClient
    mock_client_instance = MagicMock()
    mock_client_instance.async_get_delivery_period = AsyncMock(return_value=mock_delivery_data)

    # Create mock session that works as async context manager
    mock_session = create_mock_session()

    with patch('collectors.elspot.NordPoolClient', return_value=mock_client_instance):
        with patch('collectors.elspot.aiohttp.ClientSession', return_value=mock_session):
            collector = ElspotCollector()
            amsterdam_tz = ZoneInfo('Europe/Amsterdam')
            start_time = datetime(2025, 1, 15, 0, 0, 0, tzinfo=amsterdam_tz)
            end_time = datetime(2025, 1, 16, 0, 0, 0, tzinfo=amsterdam_tz)

            result = await collector.collect(start_time, end_time, country_code='NL')

    # Verify result exists
    assert result is not None, "Result should not be None"
    assert result.data is not None, "Result.data should not be None"

    # Verify winter timestamps have CET offset
    for timestamp_str in result.data.keys():
        # January should have CET offset (+01:00)
        assert '+01:00' in timestamp_str, \
            f"Winter timestamp {timestamp_str} should have +01:00 (CET) offset"

    print("[PASS] Winter timestamps have correct CET offset (+01:00)")


@pytest.mark.integration
def test_validate_combined_dataset():
    """Test that validation works with actual CombinedDataSet structure"""
    from utils.data_types import CombinedDataSet, EnhancedDataSet
    from utils.helpers import validate_data_timestamps
    from datetime import datetime

    # Create a CombinedDataSet like the real data_fetcher.py does
    combined = CombinedDataSet()

    # Add valid Elspot data with correct timestamps
    elspot_data = EnhancedDataSet(
        metadata={
            'data_type': 'energy_price',
            'source': 'Nordpool API',
            'units': 'EUR/MWh'
        },
        data={
            '2025-10-24T00:00:00+02:00': 100.5,
            '2025-10-24T01:00:00+02:00': 95.3,
            '2025-10-24T02:00:00+02:00': 102.1
        }
    )

    combined.add_dataset('elspot', elspot_data)

    # Validate
    data_dict = combined.to_dict()
    is_valid, malformed = validate_data_timestamps(data_dict)

    assert is_valid is True
    assert len(malformed) == 0

    print("[PASS] CombinedDataSet validation passes for correct data")


@pytest.mark.integration
def test_validate_rejects_malformed_combined_dataset():
    """Test that validation catches malformed timestamps in CombinedDataSet"""
    from utils.data_types import CombinedDataSet, EnhancedDataSet
    from utils.helpers import validate_data_timestamps

    # Create a CombinedDataSet with MALFORMED timestamps (simulating the bug)
    combined = CombinedDataSet()

    # Add Elspot data with the BUG (malformed timezone)
    elspot_data = EnhancedDataSet(
        metadata={
            'data_type': 'energy_price',
            'source': 'Nordpool API',
            'units': 'EUR/MWh'
        },
        data={
            '2025-10-24T00:00:00+00:09': 100.5,  # MALFORMED!
            '2025-10-24T01:00:00+00:09': 95.3    # MALFORMED!
        }
    )

    combined.add_dataset('elspot', elspot_data)

    # Validate
    data_dict = combined.to_dict()
    is_valid, malformed = validate_data_timestamps(data_dict)

    assert is_valid is False
    assert len(malformed) == 2
    assert all('elspot' in m for m in malformed)
    assert all('+00:09' in m for m in malformed)

    print("[PASS] Validation correctly rejects malformed timestamps")


@pytest.mark.integration
def test_save_data_file_rejects_malformed():
    """Test that save_data_file rejects malformed timestamps"""
    from utils.data_types import CombinedDataSet, EnhancedDataSet
    from utils.helpers import save_data_file
    import tempfile
    import os

    # Create CombinedDataSet with malformed data
    combined = CombinedDataSet()

    elspot_data = EnhancedDataSet(
        metadata={
            'data_type': 'energy_price',
            'source': 'Nordpool API',
            'units': 'EUR/MWh'
        },
        data={
            '2025-10-24T00:00:00+00:09': 100.5  # MALFORMED!
        }
    )

    combined.add_dataset('elspot', elspot_data)

    # Try to save - should raise ValueError
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        temp_path = f.name

    try:
        with pytest.raises(ValueError, match="malformed timestamps"):
            save_data_file(combined, temp_path, encrypt=False)

        print("[PASS] save_data_file correctly rejects malformed data")
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v", "--tb=short"])
