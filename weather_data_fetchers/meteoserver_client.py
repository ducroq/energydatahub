import asyncio
import logging
import aiohttp
from datetime import datetime, timedelta
from timezone_helpers import ensure_timezone, compare_timezones
from data_types import EnhancedDataSet
import platform

MAX_ATTEMPTS = 10
RETRY_DELAY = 2

async def get_MeteoServer_sun_forecast(api_key: str, latitude: float, longitude: float, start_time: datetime, end_time: datetime) -> EnhancedDataSet:
    """
    Retrieves sun forecast data from MeteoServer API for a specified location and time interval.

    Args:
        api_key (str): The API key for accessing the MeteoServer API.
        latitude (float): The latitude of the location for which to fetch the sun forecast.
        longitude (float): The longitude of the location for which to fetch the sun forecast.
        start_time (datetime): The start of the time range for which to fetch the forecast.
        end_time (datetime): The end of the time range for which to fetch the forecast.

    Returns:
        EnhancedDataSet: An EnhancedDataSet containing the MeteoServer sun data.
    """
    base_url = 'https://data.meteoserver.nl/api/solar.php'

    if start_time is None:
        raise ValueError("Start time must be provided")
    if end_time is None:
        raise ValueError("End time must be provided")

    start_time, end_time, timezone = ensure_timezone(start_time, end_time)

    match, message = compare_timezones(start_time, latitude, longitude)
    if not match:
        logging.warning(f"Timezone mismatch: {message}")

    logging.info(f"Querying Meteo server from {start_time} to {end_time}")

    exclude_fields = ['time', 'cet']
    try:
        url = f"{base_url}?lat={latitude}&long={longitude}&key={api_key}"
        logging.info(f"Fetching sun forecast data from {url}")
        for attempt_nr in range(MAX_ATTEMPTS): # Retry a number of times, since the API returns wrong response sometimes
            async with aiohttp.ClientSession() as session:
                logging.info(f"Attempt nr {attempt_nr+1} to fetch sun forecast data")
                async with session.get(url) as response:
                    if response.status != 200:
                        logging.error(f"Unable to fetch data. Status code: {response.status}")
                        return None
                    response_data = await response.json()
                    if 'forecast' not in response_data:  # Check response data
                        await asyncio.sleep(RETRY_DELAY)  # Delay before retrying
                        continue
                    data = {}
                    for item in response_data['forecast']:
                        timestamp = datetime.fromtimestamp(int(item['time']), tz=timezone)
                        if start_time <= timestamp < end_time:
                            data[timestamp.isoformat()] = {}

                            for key, value in item.items():
                                if key in exclude_fields:
                                    continue
                                if isinstance(value, dict):
                                    for sub_key, sub_value in value.items():
                                        if sub_key in exclude_fields:
                                            continue
                                        data[timestamp.isoformat()][f"{key}_{sub_key}"] = sub_value
                                else:
                                    data[timestamp.isoformat()][key] = value
                    dataset = EnhancedDataSet(
                        metadata = {                
                            'data_type': 'sun',
                            'source': 'MeteoServer API',
                            'city': response_data['plaatsnaam'][0]['plaats'],
                            'station': response_data['current'][0]['station'],
                            'units': {
                                "temp": "°C",
                                "elev (sun altitude at the start of the current hour)": "°",
                                "az (sun azimuth at the start of the current hour, N=0, E=90)": "°",
                                "gr (global horizontal radiation intensity)": "J/hr/cm²",
                                "gr_w (global horizontal radiation intensity)": "W/m²",
                                "sd (number of sunshine minutes in the current hour)": "min",
                                "tc (total cloud cover)": "%",
                                "lc (low-cloud cover)": "%",
                                "mc (intermediate-cloud cover)": "%",
                                "hc (high-cloud cover)": "%",
                                "vis (visibility)": "m",
                                "prec (total precipitation in the current hour)": "mm(/h)"
                            },
                            'start_time': start_time.isoformat(),
                            'end_time': end_time.isoformat()},
                        data = data
                    )

                    if dataset.data:
                        now_hour = list(dataset['data'].keys())[0]
                        next_hour = list(dataset['data'].keys())[1]
                        logging.info(f"OpenWeather forecast from {start_time} to {end_time}\n"
                                    f"Current: {dataset['data'][now_hour]}\n"
                                    f"Next hour: {dataset['data'][next_hour]}")
                    else:
                        logging.warning(f"No data retrieved for the specified time range: {start_time} to {end_time}")
                    return dataset
            raise ValueError(f"Meteo server gives unexpected response during {attempt_nr+1} attempts")
    except Exception as e:
        logging.error(f"Error fetching sun forecast data: {e}")
    return None

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
        EnhancedDataSet: An EnhancedDataSet containing the MeteoServer data.
    """
    base_url = 'https://data.meteoserver.nl/api/uurverwachting.php'

    if start_time is None:
        raise ValueError("Start time must be provided")
    if end_time is None:
        raise ValueError("End time must be provided")

    start_time, end_time, timezone = ensure_timezone(start_time, end_time)

    logging.info(f"Querying MeteoServer from {start_time} to {end_time}")

    match, message = compare_timezones(start_time, latitude, longitude)
    if not match:
        logging.warning(f"Timezone mismatch: {message}")

    exclude_fields = ['tijd', 'tijd_nl', 'loc', 'offset', 'samenv']
    try:
        url = f"{base_url}?lat={latitude}&long={longitude}&key={api_key}"
        logging.info(f"Fetching weather forecast data from {url}")
        for attempt_nr in range(MAX_ATTEMPTS): # Retry a number of times, since the API returns wrong response sometimes
            async with aiohttp.ClientSession() as session:
                logging.info(f"Attempt nr {attempt_nr+1} to fetch weather forecast data")
                async with session.get(url) as response:
                    if response.status != 200:
                        logging.error(f"Unable to fetch weather forecast data. Status code: {response.status}")
                        return None
                    response_data = await response.json()
                    if 'data' not in response_data: # Check response data
                        await asyncio.sleep(RETRY_DELAY)  # Delay before retrying
                        continue
                    data = {}
                    for item in response_data['data']:
                        timestamp = datetime.fromtimestamp(int(item['tijd']), tz=timezone)
                        if start_time <= timestamp < end_time:
                            data[timestamp.isoformat()] = {}

                            for key, value in item.items():
                                if key in exclude_fields:
                                    continue
                                if isinstance(value, dict):
                                    for sub_key, sub_value in value.items():
                                        if sub_key in exclude_fields:
                                            continue
                                        data[timestamp.isoformat()][f"{key}_{sub_key}"] = sub_value
                                else:
                                    data[timestamp.isoformat()][key] = value
                    dataset = EnhancedDataSet(
                        metadata = {
                            'data_type': 'weather',
                            'source': 'MeteoServer API',
                            'model': 'HARMONIE',
                            'city': response_data['plaatsnaam'][0]['plaats'],
                            'units': {
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
                                "icoon": "image name"
                            },
                            'start_time': start_time.isoformat(),
                            'end_time': end_time.isoformat()},
                        data = data
                    )

                    if dataset.data:
                        now_hour = list(dataset['data'].keys())[0]
                        next_hour = list(dataset['data'].keys())[1]
                        logging.info(f"MeteoServer forecast from {start_time} to {end_time}\n"
                                    f"Current: {dataset['data'][now_hour]}\n"
                                    f"Next hour: {dataset['data'][next_hour]}")
                    else:
                        logging.warning(f"No data retrieved for the specified time range: {start_time} to {end_time}")
                    return dataset
            raise ValueError(f"Meteo server gives unexpected response during {attempt_nr+1} attempts")
    except Exception as e:
        logging.error(f"Error fetching weather forecast data: {e}")
        return None

async def main():
    import os
    import pytz
    from configparser import ConfigParser
    from itertools import islice

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

    weather_data = await get_MeteoServer_weather_forecast_data(api_key, latitude, longitude, current_time, tomorrow_midnight)
    if weather_data:
        print(f"Weather data for {weather_data.metadata['city']}:")
        print("\nFirst 3 forecast entries:")
        for key, value in islice(weather_data.data.items(), 3):
            print(f"{key}: {value}")
    else:
        print(f"Failed to retrieve forecast data for {latitude}, {longitude}")

    sun_data = await get_MeteoServer_sun_forecast(api_key, latitude, longitude, current_time, tomorrow_midnight)
    if sun_data:
        print(f"Sun forecast for {sun_data.metadata['city']}:")
        print(sun_data['metadata'])
        print("\nFirst 3 forecast entries:")
        for key, value in islice(sun_data.data.items(), 3):
            print(f"{key}: {value}")        
    else:
        print(f"Failed to retrieve sun forecast data for {latitude}, {longitude}")

if __name__ == "__main__":
    # Set appropriate event loop policy for Windows
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())        

    asyncio.run(main())