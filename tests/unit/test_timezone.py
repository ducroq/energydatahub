"""
Unit tests for timezone handling utilities

These tests ensure that the critical timezone bug (+00:09 instead of +02:00)
is fixed and won't regress.
"""
import pytest
from datetime import datetime
from zoneinfo import ZoneInfo
import pytz

from utils.timezone_helpers import (
    localize_naive_datetime,
    normalize_timestamp_to_amsterdam,
    validate_timestamp_format,
    ensure_timezone,
    get_timezone,
    get_timezone_and_country
)


class TestLocalizeNaiveDatetime:
    """Tests for localize_naive_datetime function"""

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_localize_with_zoneinfo_summer(self):
        """Test localization to CEST (summer) timezone using ZoneInfo"""
        naive_dt = datetime(2025, 7, 15, 12, 0, 0)  # July = CEST
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')

        result = localize_naive_datetime(naive_dt, amsterdam_tz)

        assert result.tzinfo is not None
        assert result.isoformat() == '2025-07-15T12:00:00+02:00'

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_localize_with_zoneinfo_winter(self):
        """Test localization to CET (winter) timezone using ZoneInfo"""
        naive_dt = datetime(2025, 1, 15, 12, 0, 0)  # January = CET
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')

        result = localize_naive_datetime(naive_dt, amsterdam_tz)

        assert result.tzinfo is not None
        assert result.isoformat() == '2025-01-15T12:00:00+01:00'

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_localize_with_pytz(self):
        """Test localization using pytz timezone"""
        naive_dt = datetime(2025, 10, 24, 12, 0, 0)
        amsterdam_tz = pytz.timezone('Europe/Amsterdam')

        result = localize_naive_datetime(naive_dt, amsterdam_tz)

        assert result.tzinfo is not None
        # Pytz may return different representation but offset should be correct
        assert '+02:00' in result.isoformat() or '+01:00' in result.isoformat()

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_localize_raises_on_aware_datetime(self):
        """Test that function raises error if datetime is already timezone-aware"""
        aware_dt = datetime(2025, 10, 24, 12, 0, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')

        with pytest.raises(ValueError, match="already timezone-aware"):
            localize_naive_datetime(aware_dt, amsterdam_tz)


class TestNormalizeTimestampToAmsterdam:
    """Tests for normalize_timestamp_to_amsterdam function"""

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_normalize_naive_datetime_summer(self):
        """Test normalizing naive datetime during summer (CEST)"""
        naive_dt = datetime(2025, 7, 15, 12, 0, 0)

        result = normalize_timestamp_to_amsterdam(naive_dt)

        assert result.isoformat() == '2025-07-15T12:00:00+02:00'

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_normalize_naive_datetime_winter(self):
        """Test normalizing naive datetime during winter (CET)"""
        naive_dt = datetime(2025, 1, 15, 12, 0, 0)

        result = normalize_timestamp_to_amsterdam(naive_dt)

        assert result.isoformat() == '2025-01-15T12:00:00+01:00'

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_normalize_utc_to_amsterdam(self):
        """Test converting UTC datetime to Amsterdam"""
        utc_dt = datetime(2025, 10, 24, 10, 0, 0, tzinfo=ZoneInfo('UTC'))

        result = normalize_timestamp_to_amsterdam(utc_dt)

        # 10:00 UTC = 12:00 CEST (UTC+2)
        assert result.hour == 12
        assert '+02:00' in result.isoformat() or '+01:00' in result.isoformat()

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_normalize_new_york_to_amsterdam(self):
        """Test converting New York time to Amsterdam"""
        nyc_dt = datetime(2025, 7, 15, 6, 0, 0, tzinfo=ZoneInfo('America/New_York'))

        result = normalize_timestamp_to_amsterdam(nyc_dt)

        # 6:00 EDT (UTC-4) = 12:00 CEST (UTC+2)
        assert result.hour == 12
        assert '+02:00' in result.isoformat()

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_normalize_already_amsterdam(self):
        """Test that Amsterdam datetime remains unchanged"""
        amsterdam_dt = datetime(2025, 10, 24, 12, 0, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))

        result = normalize_timestamp_to_amsterdam(amsterdam_dt)

        assert result == amsterdam_dt


class TestValidateTimestampFormat:
    """Tests for validate_timestamp_format function"""

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_rejects_malformed_offset_00_09(self):
        """Test rejection of +00:09 malformed offset (THE BUG)"""
        malformed_timestamp = '2025-10-24T12:00:00+00:09'

        result = validate_timestamp_format(malformed_timestamp)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_rejects_malformed_offset_00_18(self):
        """Test rejection of +00:18 malformed offset (historical bug)"""
        malformed_timestamp = '2025-10-24T12:00:00+00:18'

        result = validate_timestamp_format(malformed_timestamp)

        assert result is False

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_accepts_valid_cest_offset(self):
        """Test acceptance of +02:00 (CEST) offset"""
        valid_timestamp = '2025-07-15T12:00:00+02:00'

        result = validate_timestamp_format(valid_timestamp)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_accepts_valid_cet_offset(self):
        """Test acceptance of +01:00 (CET) offset"""
        valid_timestamp = '2025-01-15T12:00:00+01:00'

        result = validate_timestamp_format(valid_timestamp)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_accepts_utc_offset(self):
        """Test acceptance of UTC (+00:00) offset"""
        utc_timestamp = '2025-10-24T12:00:00+00:00'

        result = validate_timestamp_format(utc_timestamp)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_accepts_utc_z_suffix(self):
        """Test acceptance of UTC Z suffix"""
        utc_timestamp = '2025-10-24T12:00:00Z'

        result = validate_timestamp_format(utc_timestamp)

        assert result is True

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_rejects_invalid_offset(self):
        """Test rejection of completely invalid offset"""
        invalid_timestamp = '2025-10-24T12:00:00+05:30'  # Valid for India, but not for Amsterdam

        result = validate_timestamp_format(invalid_timestamp)

        assert result is False


class TestEnsureTimezone:
    """Tests for ensure_timezone function"""

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_ensure_timezone_with_pytz(self):
        """Test ensure_timezone with pytz timezone"""
        amsterdam_tz = pytz.timezone('Europe/Amsterdam')
        start = datetime(2025, 10, 24, 0, 0, 0, tzinfo=amsterdam_tz)
        end = datetime(2025, 10, 25, 0, 0, 0, tzinfo=amsterdam_tz)

        start_result, end_result, tz_result = ensure_timezone(start, end)

        assert start_result.tzinfo is not None
        assert end_result.tzinfo is not None
        assert isinstance(tz_result, pytz.tzinfo.BaseTzInfo)


class TestGetTimezone:
    """Tests for get_timezone function"""

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_get_timezone_amsterdam(self):
        """Test getting timezone for Amsterdam coordinates"""
        lat, lon = 52.3676, 4.9041  # Amsterdam

        result = get_timezone(lat, lon)

        assert result is not None
        # timezonefinder may return Europe/Amsterdam or Europe/Paris (both CET/CEST)
        assert result.key in ['Europe/Amsterdam', 'Europe/Paris']

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_get_timezone_arnhem(self):
        """Test getting timezone for Arnhem coordinates (HAN location)"""
        lat, lon = 51.9851, 5.8987  # Arnhem

        result = get_timezone(lat, lon)

        assert result is not None
        # timezonefinder may return Europe/Amsterdam or Europe/Paris (both CET/CEST)
        assert result.key in ['Europe/Amsterdam', 'Europe/Paris']


class TestGetTimezoneAndCountry:
    """Tests for get_timezone_and_country function"""

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_get_timezone_and_country_netherlands(self):
        """Test getting timezone and country for Netherlands"""
        lat, lon = 52.3676, 4.9041  # Amsterdam

        tz, country = get_timezone_and_country(lat, lon)

        assert tz is not None
        # timezonefinder may return Europe/Amsterdam or Europe/Paris (both CET/CEST)
        assert tz.key in ['Europe/Amsterdam', 'Europe/Paris']
        assert country == 'NL'


class TestTimezoneEdgeCases:
    """Edge case tests for timezone handling"""

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_dst_transition_spring(self):
        """Test timezone handling during spring DST transition"""
        # Last Sunday of March 2025 at 02:00 CET becomes 03:00 CEST
        # Using 01:00 CET (before transition)
        before_dst = datetime(2025, 3, 30, 1, 0, 0)

        result = normalize_timestamp_to_amsterdam(before_dst)

        # Should have CET offset (+01:00) before transition
        assert '+01:00' in result.isoformat()

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_dst_transition_fall(self):
        """Test timezone handling during fall DST transition"""
        # Last Sunday of October 2025 at 03:00 CEST becomes 02:00 CET
        # Using 04:00 CET (after transition)
        after_dst = datetime(2025, 10, 26, 4, 0, 0)

        result = normalize_timestamp_to_amsterdam(after_dst)

        # Should have CET offset (+01:00) after transition
        assert '+01:00' in result.isoformat()

    @pytest.mark.unit
    @pytest.mark.timezone
    def test_midnight_boundary(self):
        """Test timezone handling at midnight"""
        midnight = datetime(2025, 10, 24, 0, 0, 0)

        result = normalize_timestamp_to_amsterdam(midnight)

        assert result.hour == 0
        assert result.isoformat().startswith('2025-10-24T00:00:00')


# Regression tests for the specific bug
class TestElspotBugRegression:
    """Regression tests to ensure Elspot timezone bug doesn't return"""

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_no_00_09_offset(self):
        """Ensure we never generate +00:09 offset"""
        # Simulate various timestamps
        test_dates = [
            datetime(2025, 1, 1, 12, 0),   # Winter
            datetime(2025, 7, 1, 12, 0),   # Summer
            datetime(2025, 3, 30, 12, 0),  # Around DST transition
            datetime(2025, 10, 26, 12, 0), # Around DST transition
        ]

        for dt in test_dates:
            result = normalize_timestamp_to_amsterdam(dt)
            iso_str = result.isoformat()

            # Should NEVER contain +00:09
            assert '+00:09' not in iso_str
            # Should be either CET or CEST
            assert '+02:00' in iso_str or '+01:00' in iso_str

    @pytest.mark.unit
    @pytest.mark.timezone
    @pytest.mark.critical
    def test_no_00_18_offset(self):
        """Ensure we never generate +00:18 offset (historical bug)"""
        test_dates = [
            datetime(2025, 1, 1, 12, 0),
            datetime(2025, 7, 1, 12, 0),
        ]

        for dt in test_dates:
            result = normalize_timestamp_to_amsterdam(dt)
            iso_str = result.isoformat()

            # Should NEVER contain +00:18
            assert '+00:18' not in iso_str
            # Should be either CET or CEST
            assert '+02:00' in iso_str or '+01:00' in iso_str


if __name__ == "__main__":
    # Run tests with: pytest tests/unit/test_timezone.py -v
    pytest.main([__file__, "-v", "--tb=short"])
