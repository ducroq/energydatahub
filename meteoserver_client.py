import asyncio
import logging
import aiohttp
from datetime import datetime, timedelta, timezone
import pytz
from time_zone_helpers import compare_timezones

# TODO: extract liveweer data from MeteoServer API, https://data.meteoserver.nl/api/liveweer_synop.php?lat=52.1052957&long=5.1706729&key=7daf22bed0&select=1
# TODO: extract current sun forecast from response_data['current'][0] and add to sun forecast data

def convert_value(value):
    if value == '-':
        return None  # Using None instead of NaN for JSON compatibility
    elif value.lower() == 'none':
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value  # Keep as string if it's not a number
        
async def get_MeteoServer_sun_forecast(api_key: str, latitude: float, longitude: float, start_time: datetime, end_time: datetime) -> dict:
    """
    Retrieves sun forecast data from MeteoServer API for a specified location and time interval.

    Args:
        api_key (str): The API key for accessing the MeteoServer API.
        latitude (float): The latitude of the location for which to fetch the sun forecast.
        longitude (float): The longitude of the location for which to fetch the sun forecast.
        start_time (datetime): The start of the time range for which to fetch the forecast.
        end_time (datetime): The end of the time range for which to fetch the forecast.

    Returns:
        dict: A dictionary containing the sun forecast data, units, and metadata.
    """
    base_url = 'https://data.meteoserver.nl/api/solar.php'

    if start_time is None:
        raise ValueError("Start time must be provided")
    if end_time is None:
        raise ValueError("End time must be provided")
    
    # Ensure start and end times are in the specified timezone
    tz = start_time.tzinfo
    start_time = start_time.astimezone(tz)
    end_time = end_time.astimezone(tz)
    
    match, message = compare_timezones(start_time, latitude, longitude)
    if not match:
        logging.warning(f"Warning: Timezone mismatch: {message}")

    processed_data = {}
    processed_data['units'] = {
        "temp": "°C",
        "elev (sun altitude at the start of the current hour)": "°",
        "az (sun azimuth at the start of the current hour,  N=0, E=90)": "°",
        "gr (global horizontal radiation intensity)": "J/hr/cm²",
        "gr_w (global horizontal radiation intensity)": "W/m²",
        "sd (number of sunshine minutes in the current hour)": "min",
        "tc (total cloud cover)": "%",
        "lc (low-cloud cover)": "%",
        "mc (intermediate-cloud cover)": "%",
        "hc (high-cloud cover)": "%",
        "vis (visibility)": "m",
        "prec (total precipitation in the current hour)": "mm(/h)"
    }
    processed_data['sun forecast'] = {}
    processed_data['metadata'] = {}

    try:
        async with aiohttp.ClientSession() as session:
            url = f"{base_url}?lat={latitude}&long={longitude}&key={api_key}"

            async with session.get(url) as response:
                if response.status != 200:
                    logging.error(f"Error: Unable to fetch data. Status code: {response.status}")
                    return processed_data

                response_data = await response.json()
                processed_data['metadata'] = {
                    "plaats": response_data['plaatsnaam'][0]['plaats'],
                    "station": response_data['current'][0]['station'], 
                }
                processed_data['sun forecast'] = {}

                for item in response_data['forecast']:
                    naive_item_time = datetime.strptime(item.pop('cet'), '%d-%m-%Y %H:%M')
                    # localized_item_time = tz.localize(naive_item_time)
                    localized_item_time = naive_item_time.astimezone(tz)
                    if localized_item_time >= start_time and localized_item_time <= end_time:
                        processed_item = {key: convert_value(value) for key, value in item.items()}
                        processed_data['sun forecast'][localized_item_time.isoformat()] = processed_item
    except Exception as e:
        logging.error(f"Error fetching sun forecast data: {e}")

    return processed_data    

async def get_MeteoServer_weather_forecast_data(api_key: str, latitude: float, longitude: float, start_time: datetime, end_time: datetime) -> dict:
    """
    Retrieves weather forecast data from MeteoServer API for a specified location and time interval.
    Uses the 'HARMONIE' model by default.

    Args:
        api_key (str): The API key for accessing the MeteoServer API.
        latitude (float): The latitude of the location for which to fetch the forecast.
        longitude (float): The longitude of the location for which to fetch the forecast.
        start_time (datetime): The start of the time range for which to fetch the forecast.
        end_time (datetime): The end of the time range for which to fetch the forecast.

    Returns:
        dict: A dictionary containing the weather forecast data, units, and metadata.
    """
    base_url = 'https://data.meteoserver.nl/api/uurverwachting.php'

    if start_time is None:
        raise ValueError("Start time must be provided")
    if end_time is None:
        raise ValueError("End time must be provided")
    
    # Ensure start and end times are in the specified timezone
    tz = start_time.tzinfo
    start_time = start_time.astimezone(tz)
    end_time = end_time.astimezone(tz)

    match, message = compare_timezones(start_time, latitude, longitude)
    if not match:
        logging.warning(f"Warning: Timezone mismatch: {message}")

    processed_data = {}
    processed_data['units'] = { 
        "temp": "°C",
        "winds (mean wind velocity)": "m/s",
        "windb (mean wind force)": "Beaufort",
        "windknp (mean wind velocity)": "knots",
        "windkmh (mean wind velocity)": "km/h",
        "windr (wind direction)": "°",
        "windrltr (wind direction)": "abbreviation",
        "gust (wind gust, GFS only)": "m/s",
        "gustb (wind gust, GFS only)": "Beaufort",
        "gustkt (wind gust, GFS only)": "knots",
        "gustkmh (wind gust, GFS only)": "km/h",
        "vis (visibility)": "m",
        "neersl (precipitation)": "mm",
        "luchtd (air pressure)": "mbar / hPa",
        "luchtdmmhg (air pressure)": "mm Hg",
        "luchtdinhg (air pressure)": "inch Hg",
        "rv (relative humidity)": "%",
        "gr (global horizontal radiation)": "W/m²",
        "hw (high cloud cover)": "%",
        "mw (medium cloud cover)": "%",
        "lw (low cloud cover)": "%",
        "tw (total cloud cover)": "%",
        "cape (convective available potential energy, GFS only)": "J/kg",
        "cond": "weather condition code",
        "ico": "weather icon code",
        "samenv": "text",
        "icoon": "image name"
    }
    processed_data['weather forecast'] = {}
    processed_data['metadata'] = {}

    try:
        async with aiohttp.ClientSession() as session:
            url = f"{base_url}?lat={latitude}&long={longitude}&key={api_key}"

            async with session.get(url) as response:
                if response.status != 200:
                    logging.error(f"Error: Unable to fetch data. Status code: {response.status}")
                    return processed_data

                response_data = await response.json()
                processed_data['metadata'] = {
                    "plaats": response_data['plaatsnaam'][0]['plaats'],
                    "model": 'HARMONIE'
                }

                for item in response_data['data']:
                    naive_item_time = datetime.strptime(item.pop('tijd_nl'), '%d-%m-%Y %H:%M')
                    localized_item_time = naive_item_time.astimezone(tz)
                    # localized_item_time = tz.localize(naive_item_time)
                    if localized_item_time >= start_time and localized_item_time <= end_time:
                        processed_item = {key: convert_value(value) for key, value in item.items()}
                        processed_data['weather forecast'][localized_item_time.isoformat()] = processed_item                        
    except Exception as e:
        logging.error(f"Error fetching weather forecast data: {e}")

    return processed_data    

async def main():
    import os
    from configparser import ConfigParser

    logging.basicConfig(level=logging.INFO)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, 'secrets.ini')

    configur = ConfigParser() 
    configur.read(secrets_file)
    meteoserver_api_key = configur.get('api_keys', 'meteo')
    latitude = configur.get('location', 'latitude')
    longitude = configur.get('location', 'longitude')

    api_key = meteoserver_api_key
    cest = pytz.timezone('Europe/Amsterdam')
    
    current_time = datetime.now(cest)
    tomorrow = current_time + timedelta(days=1)
    tomorrow_midnight = tomorrow.replace(hour=23, minute=59, second=59, microsecond=999999)

    weather_forecast_data = await get_MeteoServer_weather_forecast_data(api_key, latitude, longitude, current_time, tomorrow_midnight)
    if weather_forecast_data:
        print(f"Weather forecast for {latitude}, {longitude}:")
        print(f"Model: {weather_forecast_data['metadata']['model']}")
        print("\nFirst 3 forecast entries:")
        for timestamp, entry in list(weather_forecast_data['weather forecast'].items())[:3]:
            print(f"Timestamp: {timestamp}, data: {entry}")
        print("\nUnits:")
        for key, value in list(weather_forecast_data['units'].items())[:5]:  # Print first 5 units
            print(f"{key}: {value}")
    else:
        print(f"Failed to retrieve forecast data for {latitude}, {longitude}")

    sun_forecast_data = await get_MeteoServer_sun_forecast(api_key, latitude, longitude, current_time, tomorrow_midnight)
    if sun_forecast_data:
        print(f"Sun forecast for {latitude}, {longitude}:")
        print(sun_forecast_data['metadata'])
        print("\nFirst 3 forecast entries:")
        for timestamp, entry in list(sun_forecast_data['sun forecast'].items())[:3]:
            print(f"Timestamp: {timestamp}, data: {entry}")
        print("\nUnits:")
        for key, value in list(sun_forecast_data['units'].items())[:5]:  # Print first 5 units
            print(f"{key}: {value}")
    else:
        print(f"Failed to retrieve sun forecast data for {latitude}, {longitude}")

if __name__ == "__main__":
    asyncio.run(main())