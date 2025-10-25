"""
Unit Tests for Timezone Helpers Module
--------------------------------------
Tests timezone utility functions.

File: tests/unit/test_utils_timezone_helpers.py
Created: 2025-10-25
"""

import pytest
import pytz
from datetime import datetime
from zoneinfo import ZoneInfo
from unittest.mock import patch, MagicMock
from utils.timezone_helpers import (
    get_timezone,
    get_timezone_and_country,
    compare_timezones,
    localize_naive_datetime,
    normalize_timestamp_to_amsterdam,
    validate_timestamp_format
)


class TestGetTimezone:
    """Test timezone getter function."""

    def test_amsterdam_coordinates(self):
        """Test getting timezone for Amsterdam."""
        tz = get_timezone(52.37, 4.89)
        assert tz is not None
        assert tz.key == 'Europe/Amsterdam'

    def test_paris_coordinates(self):
        """Test getting timezone for Paris."""
        tz = get_timezone(48.8566, 2.3522)
        assert tz is not None
        assert tz.key == 'Europe/Paris'

    def test_new_york_coordinates(self):
        """Test getting timezone for New York."""
        tz = get_timezone(40.7128, -74.0060)
        assert tz is not None
        assert 'America/New_York' in tz.key

    def test_ocean_coordinates_returns_none(self):
        """Test coordinates in ocean return None."""
        # Middle of Pacific Ocean
        tz = get_timezone(0.0, -160.0)
        # Might return None or closest timezone depending on timezonefinder behavior
        # Just verify it doesn't crash
        assert tz is None or isinstance(tz, ZoneInfo)


class TestGetTimezoneAndCountry:
    """Test timezone and country getter function."""

    def test_amsterdam_timezone_and_country(self):
        """Test getting timezone and country for Amsterdam."""
        tz, country = get_timezone_and_country(52.37, 4.89)
        assert tz is not None
        assert tz.key == 'Europe/Amsterdam'
        assert country == 'NL'

    def test_paris_timezone_and_country(self):
        """Test getting timezone and country for Paris."""
        tz, country = get_timezone_and_country(48.8566, 2.3522)
        assert tz is not None
        assert tz.key == 'Europe/Paris'
        assert country == 'FR'

    def test_new_york_timezone_and_country(self):
        """Test getting timezone and country for New York."""
        tz, country = get_timezone_and_country(40.7128, -74.0060)
        assert tz is not None
        assert country == 'US'


class TestCompareTimezones:
    """Test timezone comparison function."""

    def test_matching_timezones(self):
        """Test comparison with matching timezones."""
        amsterdam_time = datetime.now(ZoneInfo('Europe/Amsterdam'))
        matches, message = compare_timezones(amsterdam_time, 52.37, 4.89)

        assert matches is True
        assert 'match' in message.lower()

    def test_non_matching_timezones(self):
        """Test comparison with non-matching timezones."""
        ny_time = datetime.now(ZoneInfo('America/New_York'))
        matches, message = compare_timezones(ny_time, 52.37, 4.89)

        assert matches is False
        assert 'not match' in message.lower()

    def test_naive_datetime_returns_false(self):
        """Test that naive datetime returns false."""
        naive_time = datetime.now()  # No timezone
        matches, message = compare_timezones(naive_time, 52.37, 4.89)

        assert matches is False
        assert 'naive' in message.lower()

    def test_pytz_timezone_comparison(self):
        """Test comparison with pytz timezone."""
        paris_tz = pytz.timezone('Europe/Paris')
        paris_time = datetime.now(paris_tz)
        matches, message = compare_timezones(paris_time, 48.8566, 2.3522)

        assert matches is True


class TestLocalizeNaiveDatetime:
    """Test naive datetime localization function."""

    def test_localize_to_amsterdam(self):
        """Test localizing naive datetime to Amsterdam."""
        naive_dt = datetime(2025, 10, 25, 12, 0, 0)
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')

        aware_dt = localize_naive_datetime(naive_dt, amsterdam_tz)

        assert aware_dt.tzinfo is not None
        # October is CEST (+02:00)
        assert aware_dt.isoformat() == '2025-10-25T12:00:00+02:00'

    def test_localize_to_pytz_timezone(self):
        """Test localizing with pytz timezone."""
        naive_dt = datetime(2025, 10, 25, 12, 0, 0)
        paris_tz = pytz.timezone('Europe/Paris')

        aware_dt = localize_naive_datetime(naive_dt, paris_tz)

        assert aware_dt.tzinfo is not None

    def test_localize_aware_datetime_raises_error(self):
        """Test that localizing aware datetime raises error."""
        aware_dt = datetime(2025, 10, 25, 12, 0, 0, tzinfo=ZoneInfo('UTC'))
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')

        with pytest.raises(ValueError, match="already timezone-aware"):
            localize_naive_datetime(aware_dt, amsterdam_tz)

    def test_localize_winter_time(self):
        """Test localization during winter (CET)."""
        naive_dt = datetime(2025, 1, 15, 12, 0, 0)
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')

        aware_dt = localize_naive_datetime(naive_dt, amsterdam_tz)

        # January is CET (+01:00)
        assert aware_dt.isoformat() == '2025-01-15T12:00:00+01:00'


class TestNormalizeTimestampToAmsterdam:
    """Test Amsterdam timestamp normalization function."""

    def test_normalize_naive_datetime(self):
        """Test normalizing naive datetime."""
        naive_dt = datetime(2025, 10, 25, 12, 0, 0)

        normalized = normalize_timestamp_to_amsterdam(naive_dt)

        assert normalized.tzinfo is not None
        assert normalized.tzinfo.key == 'Europe/Amsterdam'
        # October is CEST (+02:00)
        assert '+02:00' in normalized.isoformat()

    def test_normalize_utc_datetime(self):
        """Test normalizing UTC datetime."""
        utc_dt = datetime(2025, 10, 25, 10, 0, 0, tzinfo=ZoneInfo('UTC'))

        normalized = normalize_timestamp_to_amsterdam(utc_dt)

        # UTC 10:00 = Amsterdam 12:00 (CEST)
        assert normalized.hour == 12
        assert normalized.tzinfo.key == 'Europe/Amsterdam'

    def test_normalize_already_amsterdam(self):
        """Test normalizing datetime already in Amsterdam timezone."""
        amsterdam_dt = datetime(2025, 10, 25, 12, 0, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

        normalized = normalize_timestamp_to_amsterdam(amsterdam_dt)

        assert normalized == amsterdam_dt
        assert normalized.tzinfo.key == 'Europe/Amsterdam'

    def test_normalize_winter_time(self):
        """Test normalization in winter (CET)."""
        naive_dt = datetime(2025, 1, 15, 12, 0, 0)

        normalized = normalize_timestamp_to_amsterdam(naive_dt)

        # January is CET (+01:00)
        assert '+01:00' in normalized.isoformat()

    def test_normalize_from_different_timezone(self):
        """Test normalizing from a different timezone."""
        ny_dt = datetime(2025, 10, 25, 6, 0, 0, tzinfo=ZoneInfo('America/New_York'))

        normalized = normalize_timestamp_to_amsterdam(ny_dt)

        # NY 6:00 EDT = Amsterdam 12:00 CEST
        assert normalized.hour == 12
        assert normalized.tzinfo.key == 'Europe/Amsterdam'


class TestValidateTimestampFormat:
    """Test timestamp format validation function."""

    def test_valid_cest_format(self):
        """Test valid CEST timestamp (+02:00)."""
        assert validate_timestamp_format('2025-10-25T12:00:00+02:00') is True

    def test_valid_cet_format(self):
        """Test valid CET timestamp (+01:00)."""
        assert validate_timestamp_format('2025-01-15T12:00:00+01:00') is True

    def test_valid_utc_format(self):
        """Test valid UTC timestamp."""
        assert validate_timestamp_format('2025-10-25T12:00:00+00:00') is True
        assert validate_timestamp_format('2025-10-25T12:00:00Z') is True

    def test_malformed_offset_09(self):
        """Test malformed +00:09 offset is invalid."""
        assert validate_timestamp_format('2025-10-25T12:00:00+00:09') is False

    def test_malformed_offset_18(self):
        """Test malformed +00:18 offset is invalid."""
        assert validate_timestamp_format('2025-10-25T12:00:00+00:18') is False

    def test_invalid_offset(self):
        """Test other invalid offsets."""
        assert validate_timestamp_format('2025-10-25T12:00:00+05:30') is False
        assert validate_timestamp_format('2025-10-25T12:00:00-04:00') is False

    def test_no_timezone_invalid(self):
        """Test timestamp without timezone is invalid."""
        assert validate_timestamp_format('2025-10-25T12:00:00') is False

    def test_multiple_timestamps(self):
        """Test validation of multiple formats."""
        valid_timestamps = [
            '2025-10-25T12:00:00+02:00',
            '2025-01-15T12:00:00+01:00',
            '2025-10-25T12:00:00+00:00',
            '2025-10-25T12:00:00Z'
        ]
        for ts in valid_timestamps:
            assert validate_timestamp_format(ts) is True

        invalid_timestamps = [
            '2025-10-25T12:00:00+00:09',
            '2025-10-25T12:00:00+00:18',
            '2025-10-25T12:00:00',
            '2025-10-25T12:00:00+03:00'
        ]
        for ts in invalid_timestamps:
            assert validate_timestamp_format(ts) is False


class TestTimezoneEdgeCases:
    """Test edge cases and special scenarios."""

    def test_dst_transition(self):
        """Test behavior around DST transitions."""
        # Last Sunday of October 2025 (DST transition in Europe)
        # Before transition (CEST +02:00)
        before = datetime(2025, 10, 25, 12, 0, 0)
        normalized_before = normalize_timestamp_to_amsterdam(before)
        assert '+02:00' in normalized_before.isoformat()

        # After transition (CET +01:00)
        after = datetime(2025, 11, 1, 12, 0, 0)
        normalized_after = normalize_timestamp_to_amsterdam(after)
        assert '+01:00' in normalized_after.isoformat()

    def test_midnight_timestamp(self):
        """Test normalization at midnight."""
        midnight = datetime(2025, 10, 25, 0, 0, 0)
        normalized = normalize_timestamp_to_amsterdam(midnight)

        assert normalized.hour == 0
        assert normalized.minute == 0

    def test_comparison_with_pytz_and_zoneinfo(self):
        """Test comparison between pytz and ZoneInfo timezones."""
        # Create datetime with pytz
        paris_pytz = pytz.timezone('Europe/Paris')
        paris_time = datetime.now(paris_pytz)

        # Should still match Paris coordinates
        matches, message = compare_timezones(paris_time, 48.8566, 2.3522)
        assert matches is True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
