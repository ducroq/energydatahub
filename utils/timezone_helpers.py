from timezonefinder import TimezoneFinder
import reverse_geocoder as rg
from datetime import datetime, tzinfo
import pytz
from zoneinfo import ZoneInfo

# Ensure start and end times are in the specified timezone
def ensure_timezone(start_time: datetime, end_time: datetime) -> tuple[datetime, datetime, ZoneInfo]:
    tz = start_time.tzinfo

    if not isinstance(tz, pytz.BaseTzInfo):
        # If it's not a pytz timezone, try to create one
        try:
            tz = pytz.timezone(str(tz))
        except:
            raise ValueError("Could not create a pytz timezone object")
    start_time = start_time.astimezone(tz)
    end_time = end_time.astimezone(tz)
    return start_time, end_time, tz

def get_timezone(lat:float, lon:float) -> ZoneInfo:
    tf = TimezoneFinder()
    timezone_str = tf.timezone_at(lat=float(lat), lng=float(lon))
    if timezone_str is None:
        return None
    return ZoneInfo(timezone_str)

def get_timezone_and_country(lat, lng):
    tf = TimezoneFinder()
    timezone_str = tf.timezone_at(lat=lat, lng=lng)
    
    # Get country code
    result = rg.search((lat, lng), mode=1)  # mode=1 returns only one result
    country_code = result[0]['cc']
    
    if timezone_str is None:
        return None, country_code
    return ZoneInfo(timezone_str), country_code

def compare_timezones(current_time: datetime, lat: float, lon: float) -> tuple[bool, str]:
    coord_tz = get_timezone(lat, lon)
    if coord_tz is None:
        return False, "Could not determine timezone from coordinates"
    time_tz = current_time.tzinfo

    # Compare the timezones
    if time_tz is None:
        return False, "Current time is naive (no timezone info)"

    # Convert both to ZoneInfo objects for comparison
    if isinstance(time_tz, pytz.tzinfo.BaseTzInfo):
        time_tz = ZoneInfo(time_tz.zone)
    elif isinstance(time_tz, tzinfo):
        # If it's a tzinfo object but not a pytz timezone, try to get its name
        try:
            time_tz = ZoneInfo(time_tz.tzname(None))
        except Exception:
            # If we can't get a name, we'll compare using the original object
            pass

    if isinstance(coord_tz, ZoneInfo) and isinstance(time_tz, ZoneInfo):
        if coord_tz.key == time_tz.key:
            return True, f"Timezones match: {coord_tz}"
        else:
            return False, f"Timezones do not match. From coordinates: {coord_tz}, From current_time: {time_tz}"
    else:
        # If we couldn't convert to ZoneInfo objects, compare directly
        if coord_tz == time_tz:
            return True, f"Timezones match: {coord_tz}"
        else:
            return False, f"Timezones do not match. From coordinates: {coord_tz}, From current_time: {time_tz}"

def localize_naive_datetime(dt: datetime, target_tz: ZoneInfo | pytz.tzinfo.BaseTzInfo) -> datetime:
    """
    Properly localize a naive datetime to a target timezone.

    This function handles the correct way to add timezone information to a naive datetime.
    Unlike replace(tzinfo=...), this properly interprets the datetime as being in the
    target timezone and handles DST transitions correctly.

    Args:
        dt (datetime): A naive datetime (no timezone info)
        target_tz: Target timezone (ZoneInfo or pytz timezone)

    Returns:
        datetime: Timezone-aware datetime in the target timezone

    Raises:
        ValueError: If dt is already timezone-aware

    Examples:
        >>> naive_dt = datetime(2025, 10, 24, 12, 0, 0)
        >>> amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        >>> aware_dt = localize_naive_datetime(naive_dt, amsterdam_tz)
        >>> aware_dt.isoformat()
        '2025-10-24T12:00:00+02:00'  # Correct CEST offset
    """
    if dt.tzinfo is not None:
        raise ValueError("datetime is already timezone-aware")

    # Handle pytz timezones (require localize method)
    if isinstance(target_tz, pytz.tzinfo.BaseTzInfo):
        return target_tz.localize(dt)

    # Handle ZoneInfo (can use replace for naive datetimes)
    return dt.replace(tzinfo=target_tz)

def normalize_timestamp_to_amsterdam(dt: datetime) -> datetime:
    """
    Normalize a datetime to Europe/Amsterdam timezone with proper ISO format.

    This ensures all timestamps use the correct Amsterdam timezone offset:
    - +02:00 during CEST (Central European Summer Time)
    - +01:00 during CET (Central European Time)

    Prevents malformed offsets like +00:09 or +00:18 that can occur from
    incorrect timezone handling.

    Args:
        dt (datetime): A datetime object (naive or aware)

    Returns:
        datetime: Timezone-aware datetime in Europe/Amsterdam timezone

    Examples:
        >>> # Naive datetime (assumed to be Amsterdam time)
        >>> naive_dt = datetime(2025, 10, 24, 12, 0)
        >>> normalized = normalize_timestamp_to_amsterdam(naive_dt)
        >>> normalized.isoformat()
        '2025-10-24T12:00:00+02:00'

        >>> # UTC datetime (converted to Amsterdam)
        >>> utc_dt = datetime(2025, 10, 24, 10, 0, tzinfo=ZoneInfo('UTC'))
        >>> normalized = normalize_timestamp_to_amsterdam(utc_dt)
        >>> normalized.isoformat()
        '2025-10-24T12:00:00+02:00'
    """
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')

    # If naive, assume it's already in Amsterdam time
    if dt.tzinfo is None:
        return localize_naive_datetime(dt, amsterdam_tz)

    # If already timezone-aware, convert to Amsterdam
    return dt.astimezone(amsterdam_tz)

def validate_timestamp_format(timestamp_str: str) -> bool:
    """
    Validate that a timestamp has a proper timezone offset format.

    Checks for:
    - Malformed offsets like +00:09, +00:18 (known bugs)
    - Valid Amsterdam offsets: +02:00 (CEST) or +01:00 (CET)

    Args:
        timestamp_str (str): ISO 8601 timestamp string

    Returns:
        bool: True if valid, False if malformed

    Examples:
        >>> validate_timestamp_format('2025-10-24T12:00:00+00:09')
        False
        >>> validate_timestamp_format('2025-10-24T12:00:00+02:00')
        True
    """
    import re

    # Check for known malformed offsets
    if re.search(r'\+00:(09|18)', timestamp_str):
        return False

    # Check for valid Amsterdam offsets (CET or CEST)
    if re.search(r'\+0[12]:00', timestamp_str):
        return True

    # Allow UTC as well
    if re.search(r'\+00:00|Z$', timestamp_str):
        return True

    return False

if __name__ == "__main__":
    # Example usage
    latitude = 48.8566  # Paris latitude
    longitude = 2.3522  # Paris longitude

    # Example with country code retrieval
    zone_info, country_code = get_timezone_and_country(latitude, longitude)
    print(f"Timezone: {zone_info}, Country code: {country_code}")

    # Example with matching timezone
    current_time_paris = datetime.now(ZoneInfo("Europe/Paris"))
    match, message = compare_timezones(current_time_paris, latitude, longitude)
    print(message)

    # Example with non-matching timezone
    current_time_nyc = datetime.now(ZoneInfo("America/New_York"))
    match, message = compare_timezones(current_time_nyc, latitude, longitude)
    print(message)
