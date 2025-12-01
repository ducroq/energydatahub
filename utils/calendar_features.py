"""
Calendar Features for Electricity Price Prediction
---------------------------------------------------
Provides calendar-based features that affect electricity demand patterns.

File: utils/calendar_features.py
Created: 2025-12-01
Author: Energy Data Hub Project

Description:
    Calendar features are critical for electricity price prediction:
    - Weekend vs weekday: Industrial demand drops on weekends
    - Holidays: Lower demand, especially simultaneous holidays in NL/DE/BE
    - Season/month: Heating/cooling demand patterns
    - Hour of day: Peak vs off-peak pricing

Usage:
    from utils.calendar_features import get_calendar_features, CalendarFeatures

    # Get features for a specific datetime
    dt = datetime(2025, 12, 25, 14, 0, tzinfo=ZoneInfo('Europe/Amsterdam'))
    features = get_calendar_features(dt)
    print(features.is_holiday_nl)  # True (Christmas)
    print(features.holiday_impact)  # 0.7 (high impact)

    # Get features as dict for JSON serialization
    features_dict = get_calendar_features_dict(dt)
"""

from dataclasses import dataclass, asdict
from datetime import datetime, date
from typing import Dict, Any, Optional, List
import holidays
from zoneinfo import ZoneInfo


# Initialize holiday calendars for relevant countries
NL_HOLIDAYS = holidays.Netherlands()
DE_HOLIDAYS = holidays.Germany()
BE_HOLIDAYS = holidays.Belgium()
FR_HOLIDAYS = holidays.France()

# Country weights for combined holiday impact (based on interconnection and trade)
COUNTRY_WEIGHTS = {
    'NL': 0.5,   # Netherlands (primary market)
    'DE': 0.3,   # Germany (largest trading partner)
    'BE': 0.15,  # Belgium (interconnected)
    'FR': 0.05,  # France (some influence via BE)
}


@dataclass
class CalendarFeatures:
    """Calendar features for a given datetime."""

    # Basic time features
    year: int
    month: int
    day: int
    hour: int
    day_of_week: int      # 0=Monday, 6=Sunday
    day_of_year: int
    week_of_year: int

    # Day type flags
    is_weekend: bool
    is_monday: bool       # Monday effect (post-weekend ramp-up)
    is_friday: bool       # Pre-weekend effect

    # Holiday flags by country
    is_holiday_nl: bool
    is_holiday_de: bool
    is_holiday_be: bool
    is_holiday_fr: bool

    # Combined holiday metrics
    holiday_count: int           # Number of countries with holiday (0-4)
    holiday_impact: float        # Weighted holiday impact (0.0-1.0)
    is_bridge_day: bool          # Day between holiday and weekend

    # Working day indicator
    is_working_day: bool         # Not weekend and not NL holiday
    is_working_day_regional: bool  # Not weekend, not holiday in any country

    # Season features (for demand patterns)
    is_winter: bool              # Dec, Jan, Feb (high heating demand)
    is_summer: bool              # Jun, Jul, Aug (potential cooling)
    season: str                  # 'winter', 'spring', 'summer', 'fall'

    # Holiday names (for logging/debugging)
    holiday_name_nl: Optional[str] = None
    holiday_name_de: Optional[str] = None
    holiday_name_be: Optional[str] = None
    holiday_name_fr: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


def get_season(month: int) -> str:
    """Get season name from month."""
    if month in (12, 1, 2):
        return 'winter'
    elif month in (3, 4, 5):
        return 'spring'
    elif month in (6, 7, 8):
        return 'summer'
    else:
        return 'fall'


def is_bridge_day(dt: datetime) -> bool:
    """
    Check if a date is a bridge day (between holiday and weekend).

    Bridge days often have reduced industrial activity.
    """
    d = dt.date()
    day_of_week = d.weekday()

    # Check if it's a potential bridge day (Thursday or Tuesday)
    if day_of_week == 3:  # Thursday
        # Check if Friday is a holiday
        from datetime import timedelta
        friday = d + timedelta(days=1)
        if friday in NL_HOLIDAYS or friday in DE_HOLIDAYS:
            return True
    elif day_of_week == 1:  # Tuesday
        # Check if Monday was a holiday
        from datetime import timedelta
        monday = d - timedelta(days=1)
        if monday in NL_HOLIDAYS or monday in DE_HOLIDAYS:
            return True

    return False


def get_calendar_features(dt: datetime) -> CalendarFeatures:
    """
    Calculate calendar features for a given datetime.

    Args:
        dt: Datetime to calculate features for (should be timezone-aware)

    Returns:
        CalendarFeatures dataclass with all calendar-based features
    """
    d = dt.date() if isinstance(dt, datetime) else dt

    # Basic time features
    day_of_week = d.weekday()  # 0=Monday, 6=Sunday

    # Holiday detection
    is_holiday_nl = d in NL_HOLIDAYS
    is_holiday_de = d in DE_HOLIDAYS
    is_holiday_be = d in BE_HOLIDAYS
    is_holiday_fr = d in FR_HOLIDAYS

    # Holiday names
    holiday_name_nl = NL_HOLIDAYS.get(d)
    holiday_name_de = DE_HOLIDAYS.get(d)
    holiday_name_be = BE_HOLIDAYS.get(d)
    holiday_name_fr = FR_HOLIDAYS.get(d)

    # Holiday count and weighted impact
    holiday_flags = {
        'NL': is_holiday_nl,
        'DE': is_holiday_de,
        'BE': is_holiday_be,
        'FR': is_holiday_fr,
    }
    holiday_count = sum(holiday_flags.values())
    holiday_impact = sum(
        COUNTRY_WEIGHTS[country] for country, is_hol in holiday_flags.items() if is_hol
    )

    # Weekend and day type
    is_weekend = day_of_week >= 5

    # Working day indicators
    is_working_day = not is_weekend and not is_holiday_nl
    is_working_day_regional = not is_weekend and holiday_count == 0

    # Season
    month = d.month if isinstance(d, date) else dt.month
    season = get_season(month)

    return CalendarFeatures(
        # Basic time
        year=d.year,
        month=d.month,
        day=d.day,
        hour=dt.hour if isinstance(dt, datetime) else 0,
        day_of_week=day_of_week,
        day_of_year=d.timetuple().tm_yday,
        week_of_year=d.isocalendar()[1],

        # Day type flags
        is_weekend=is_weekend,
        is_monday=day_of_week == 0,
        is_friday=day_of_week == 4,

        # Holiday flags
        is_holiday_nl=is_holiday_nl,
        is_holiday_de=is_holiday_de,
        is_holiday_be=is_holiday_be,
        is_holiday_fr=is_holiday_fr,

        # Combined metrics
        holiday_count=holiday_count,
        holiday_impact=round(holiday_impact, 2),
        is_bridge_day=is_bridge_day(dt) if isinstance(dt, datetime) else is_bridge_day(datetime.combine(d, datetime.min.time())),

        # Working day
        is_working_day=is_working_day,
        is_working_day_regional=is_working_day_regional,

        # Season
        is_winter=season == 'winter',
        is_summer=season == 'summer',
        season=season,

        # Holiday names
        holiday_name_nl=holiday_name_nl,
        holiday_name_de=holiday_name_de,
        holiday_name_be=holiday_name_be,
        holiday_name_fr=holiday_name_fr,
    )


def get_calendar_features_dict(dt: datetime) -> Dict[str, Any]:
    """
    Get calendar features as a dictionary.

    Args:
        dt: Datetime to calculate features for

    Returns:
        Dictionary with calendar features
    """
    return get_calendar_features(dt).to_dict()


def get_calendar_features_for_range(
    start: datetime,
    end: datetime,
    hourly: bool = True
) -> Dict[str, Dict[str, Any]]:
    """
    Get calendar features for a time range.

    Args:
        start: Start datetime
        end: End datetime
        hourly: If True, return hourly features; if False, daily

    Returns:
        Dict mapping ISO timestamps to feature dicts
    """
    from datetime import timedelta

    result = {}
    current = start
    delta = timedelta(hours=1) if hourly else timedelta(days=1)

    while current < end:
        features = get_calendar_features(current)
        ts_key = current.isoformat()
        result[ts_key] = features.to_dict()
        current += delta

    return result


def get_upcoming_holidays(
    days_ahead: int = 30,
    countries: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Get list of upcoming holidays.

    Args:
        days_ahead: Number of days to look ahead
        countries: List of country codes (default: ['NL', 'DE', 'BE', 'FR'])

    Returns:
        List of dicts with date, country, and holiday name
    """
    from datetime import timedelta

    if countries is None:
        countries = ['NL', 'DE', 'BE', 'FR']

    holiday_cals = {
        'NL': NL_HOLIDAYS,
        'DE': DE_HOLIDAYS,
        'BE': BE_HOLIDAYS,
        'FR': FR_HOLIDAYS,
    }

    today = date.today()
    end_date = today + timedelta(days=days_ahead)

    upcoming = []
    for country in countries:
        cal = holiday_cals.get(country)
        if cal:
            for d in range((end_date - today).days + 1):
                check_date = today + timedelta(days=d)
                if check_date in cal:
                    upcoming.append({
                        'date': check_date.isoformat(),
                        'country': country,
                        'name': cal.get(check_date),
                        'day_of_week': check_date.strftime('%A'),
                    })

    # Sort by date
    upcoming.sort(key=lambda x: x['date'])
    return upcoming


# Example usage
if __name__ == "__main__":
    from zoneinfo import ZoneInfo

    amsterdam_tz = ZoneInfo('Europe/Amsterdam')

    # Test with Christmas (should be holiday in all countries)
    christmas = datetime(2025, 12, 25, 14, 0, tzinfo=amsterdam_tz)
    features = get_calendar_features(christmas)

    print("Calendar Features for Christmas 2025:")
    print(f"  Day of week: {features.day_of_week} ({'Weekend' if features.is_weekend else 'Weekday'})")
    print(f"  Is holiday NL: {features.is_holiday_nl} ({features.holiday_name_nl})")
    print(f"  Is holiday DE: {features.is_holiday_de} ({features.holiday_name_de})")
    print(f"  Is holiday BE: {features.is_holiday_be} ({features.holiday_name_be})")
    print(f"  Holiday count: {features.holiday_count}")
    print(f"  Holiday impact: {features.holiday_impact}")
    print(f"  Is working day: {features.is_working_day}")
    print(f"  Season: {features.season}")

    print("\nUpcoming holidays (next 30 days):")
    for h in get_upcoming_holidays(30):
        print(f"  {h['date']} ({h['day_of_week']}): {h['country']} - {h['name']}")
