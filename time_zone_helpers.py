from timezonefinder import TimezoneFinder
from datetime import datetime, tzinfo
import pytz
from zoneinfo import ZoneInfo

def get_timezone(lat:float, lon:float) -> ZoneInfo:
    tf = TimezoneFinder()
    timezone_str = tf.timezone_at(lat=float(lat), lng=float(lon))
    if timezone_str is None:
        return None
    return ZoneInfo(timezone_str)

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

if __name__ == "__main__":
    # Example usage
    latitude = 48.8566  # Paris latitude
    longitude = 2.3522  # Paris longitude

    # Example with matching timezone
    current_time_paris = datetime.now(ZoneInfo("Europe/Paris"))
    match, message = compare_timezones(current_time_paris, latitude, longitude)
    print(message)

    # Example with non-matching timezone
    current_time_nyc = datetime.now(ZoneInfo("America/New_York"))
    match, message = compare_timezones(current_time_nyc, latitude, longitude)
    print(message)