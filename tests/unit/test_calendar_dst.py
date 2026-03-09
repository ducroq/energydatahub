"""
Tests for DST features in calendar_features.py
------------------------------------------------
Validates that DST-related features (is_dst, is_dst_transition_day,
dst_utc_offset_hours) are correctly computed for the Europe/Amsterdam timezone.

Key test scenarios:
- Normal CET day (winter) -> is_dst=False, offset=1
- Normal CEST day (summer) -> is_dst=True, offset=2
- Spring forward transition day (last Sunday of March) -> is_dst_transition_day=True
- Fall back transition day (last Sunday of October) -> is_dst_transition_day=True
- Day before/after transitions -> is_dst_transition_day=False
- Integration with existing CalendarFeatures (to_dict, range generation)
"""

import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from utils.calendar_features import (
    get_dst_info,
    get_calendar_features,
    get_calendar_features_dict,
    get_calendar_features_for_range,
)

AMS = ZoneInfo('Europe/Amsterdam')


class TestGetDstInfo:
    """Tests for the get_dst_info helper function."""

    def test_winter_cet(self):
        """January is CET (UTC+1), not DST."""
        dt = datetime(2026, 1, 15, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_dst is False
        assert is_transition is False
        assert offset == 1

    def test_summer_cest(self):
        """July is CEST (UTC+2), DST active."""
        dt = datetime(2026, 7, 15, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_dst is True
        assert is_transition is False
        assert offset == 2

    def test_spring_forward_2026(self):
        """2026 spring forward: last Sunday of March = March 29, 2026."""
        dt = datetime(2026, 3, 29, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_transition is True

    def test_fall_back_2026(self):
        """2026 fall back: last Sunday of October = October 25, 2026."""
        dt = datetime(2026, 10, 25, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_transition is True

    def test_day_before_spring_forward_2026(self):
        """Day before spring forward should NOT be a transition day."""
        dt = datetime(2026, 3, 28, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_transition is False
        assert is_dst is False
        assert offset == 1

    def test_day_after_spring_forward_2026(self):
        """Day after spring forward should NOT be a transition day, but IS in DST."""
        dt = datetime(2026, 3, 30, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_transition is False
        assert is_dst is True
        assert offset == 2

    def test_day_before_fall_back_2026(self):
        """Day before fall back should NOT be a transition day, but IS in DST."""
        dt = datetime(2026, 10, 24, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_transition is False
        assert is_dst is True
        assert offset == 2

    def test_day_after_fall_back_2026(self):
        """Day after fall back should NOT be a transition day, and NOT in DST."""
        dt = datetime(2026, 10, 26, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_transition is False
        assert is_dst is False
        assert offset == 1

    def test_spring_forward_2025(self):
        """2025 spring forward: last Sunday of March = March 30, 2025."""
        dt = datetime(2025, 3, 30, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_transition is True

    def test_fall_back_2025(self):
        """2025 fall back: last Sunday of October = October 26, 2025."""
        dt = datetime(2025, 10, 26, 12, 0, 0, tzinfo=AMS)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_transition is True

    def test_naive_datetime_treated_as_amsterdam(self):
        """Naive datetime should be treated as Amsterdam time."""
        # July -> should be DST
        dt = datetime(2026, 7, 15, 12, 0, 0)
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_dst is True
        assert offset == 2

    def test_utc_datetime_converted_to_amsterdam(self):
        """UTC datetime should be correctly converted to Amsterdam."""
        # January 15 at noon UTC -> 13:00 CET, still CET
        dt = datetime(2026, 1, 15, 12, 0, 0, tzinfo=ZoneInfo('UTC'))
        is_dst, is_transition, offset = get_dst_info(dt)
        assert is_dst is False
        assert offset == 1


class TestCalendarFeaturesDst:
    """Tests that DST features are correctly integrated into CalendarFeatures."""

    def test_winter_features(self):
        """CalendarFeatures in winter should have correct DST fields."""
        dt = datetime(2026, 1, 15, 12, 0, 0, tzinfo=AMS)
        features = get_calendar_features(dt)
        assert features.is_dst is False
        assert features.is_dst_transition_day is False
        assert features.dst_utc_offset_hours == 1

    def test_summer_features(self):
        """CalendarFeatures in summer should have correct DST fields."""
        dt = datetime(2026, 7, 15, 12, 0, 0, tzinfo=AMS)
        features = get_calendar_features(dt)
        assert features.is_dst is True
        assert features.is_dst_transition_day is False
        assert features.dst_utc_offset_hours == 2

    def test_transition_day_features(self):
        """CalendarFeatures on transition day should flag it."""
        dt = datetime(2026, 3, 29, 12, 0, 0, tzinfo=AMS)
        features = get_calendar_features(dt)
        assert features.is_dst_transition_day is True

    def test_to_dict_includes_dst(self):
        """to_dict should include all DST fields."""
        dt = datetime(2026, 7, 15, 12, 0, 0, tzinfo=AMS)
        d = get_calendar_features_dict(dt)
        assert 'is_dst' in d
        assert 'is_dst_transition_day' in d
        assert 'dst_utc_offset_hours' in d
        assert d['is_dst'] is True
        assert d['dst_utc_offset_hours'] == 2

    def test_range_includes_dst(self):
        """get_calendar_features_for_range should include DST in every entry."""
        start = datetime(2026, 7, 15, 0, 0, 0, tzinfo=AMS)
        end = datetime(2026, 7, 15, 3, 0, 0, tzinfo=AMS)
        result = get_calendar_features_for_range(start, end, hourly=True)
        assert len(result) == 3  # hours 0, 1, 2
        for ts, features in result.items():
            assert 'is_dst' in features
            assert features['is_dst'] is True

    def test_range_across_transition(self):
        """Range spanning spring forward should show transition on correct day."""
        # March 28 (CET) -> March 29 (transition) -> March 30 (CEST)
        start = datetime(2026, 3, 28, 12, 0, 0, tzinfo=AMS)
        end = datetime(2026, 3, 31, 12, 0, 0, tzinfo=AMS)
        result = get_calendar_features_for_range(start, end, hourly=False)

        dates_dict = {}
        for ts, features in result.items():
            day = ts[:10]  # YYYY-MM-DD
            dates_dict[day] = features

        assert dates_dict['2026-03-28']['is_dst_transition_day'] is False
        assert dates_dict['2026-03-28']['is_dst'] is False
        assert dates_dict['2026-03-29']['is_dst_transition_day'] is True
        assert dates_dict['2026-03-30']['is_dst_transition_day'] is False
        assert dates_dict['2026-03-30']['is_dst'] is True


class TestExistingFeaturesNotBroken:
    """Regression tests: existing features should still work correctly."""

    def test_christmas_still_holiday(self):
        """Christmas should still be detected as holiday in all countries."""
        dt = datetime(2025, 12, 25, 14, 0, 0, tzinfo=AMS)
        features = get_calendar_features(dt)
        assert features.is_holiday_nl is True
        assert features.is_holiday_de is True
        assert features.holiday_count >= 3

    def test_weekend_detection(self):
        """Weekend detection should still work."""
        # 2026-01-17 is a Saturday
        dt = datetime(2026, 1, 17, 12, 0, 0, tzinfo=AMS)
        features = get_calendar_features(dt)
        assert features.is_weekend is True
        assert features.is_working_day is False

    def test_weekday_detection(self):
        """Weekday detection should still work."""
        # 2026-01-15 is a Thursday
        dt = datetime(2026, 1, 15, 12, 0, 0, tzinfo=AMS)
        features = get_calendar_features(dt)
        assert features.is_weekend is False
        assert features.is_working_day is True

    def test_season_detection(self):
        """Season detection should still work."""
        winter = get_calendar_features(datetime(2026, 1, 15, 12, tzinfo=AMS))
        summer = get_calendar_features(datetime(2026, 7, 15, 12, tzinfo=AMS))
        assert winter.season == 'winter'
        assert winter.is_winter is True
        assert summer.season == 'summer'
        assert summer.is_summer is True

    def test_basic_time_features(self):
        """Basic time features should still be correct."""
        dt = datetime(2026, 3, 15, 14, 0, 0, tzinfo=AMS)
        features = get_calendar_features(dt)
        assert features.year == 2026
        assert features.month == 3
        assert features.day == 15
        assert features.hour == 14
