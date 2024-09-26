import asyncio
import logging
import aiohttp
from datetime import datetime
from data_types import EnhancedDataSet
from timezone_helpers import ensure_timezone, compare_timezones

async def get_OpenWeather_data(api_key: str, latitude: float, longitude: float, start_time: datetime, end_time: datetime) -> EnhancedDataSet:
    """
    Retrieves weather data from the OpenWeather API based on the configured latitude and longitude.

    Args:
        api_key (str): The API key for accessing the MeteoServer API.
        latitude (float): The latitude of the location for which to fetch the forecast.
        longitude (float): The longitude of the location for which to fetch the forecast.
        start_time (datetime): The start of the time range for which to fetch forecast.
        end_time (datetime): The end of the time range for which to fetch forecast.

    Returns:
        EnhancedDataSet: An EnhancedDataSet containing the OpenWeather data.
    """
    try:
        url = f"https://api.openweathermap.org/data/2.5/forecast?lat={latitude}&lon={longitude}&units=metric&appid={api_key}"

        if start_time is None:
            raise ValueError("Start time must be provided")
        if end_time is None:
            raise ValueError("End time must be provided")
        
        start_time, end_time, tz = ensure_timezone(start_time, end_time)

        logging.info(f"Querying OpenWeather from {start_time} to {end_time}")
        
        match, message = compare_timezones(start_time, latitude, longitude)
        if not match:
            logging.warning(f"Timezone mismatch: {message}")        
        
        exclude_fields = ['dt', 'dt_txt', 'pop', 'sys']

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                logging.info(f"Fetching OpenWeather forecast data from {url}")
                if response.status != 200:
                    logging.error(f"Unable to fetch data. Status code: {response.status}")
                    return None
                                    
                response_data = await response.json()
                data = {}
                for item in response_data['list']:
                    timestamp = datetime.fromtimestamp(item['dt'], tz=tz)
                    if start_time <= timestamp < end_time:
                        data[timestamp.isoformat()] = {}
                        
                        for key, value in item.items():
                            if key in exclude_fields:
                                continue
                            if isinstance(value, list):	
                                value = value[0]  # workaround: for some reason the weather value is a list with one item
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
                        'source': 'OpenWeather API 2.5',
                        'country_code': 'NL',
                        'city': response_data['city']['name'],
                        'id': response_data['city']['id'],
                        'population': response_data['city']['population'],
                        'sunrise': datetime.fromtimestamp(response_data['city']['sunrise'], tz=tz).isoformat(),
                        'sunset': datetime.fromtimestamp(response_data['city']['sunset'], tz=tz).isoformat(),
                        'units': {
                            "temp": "°C",
                            "humidity": "%",
                            "pressure": "hPa",
                            "weather_id": "weather condition code",
                            "weather_description": "text",
                            "wind_speed": "m/s",
                            "wind_direction": "°",
                            "wind gust": "m/s",
                            "visibility": "m",
                            "clouds": "%"
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
    except Exception as e:
        logging.error(f"Error retrieving OpenWeather data: {e}")     
        return None

async def get_OpenWeather_geographical_coordinates_in_NL(api_key: str, plaats: str) -> dict:
    """
    Retrieves the geographical coordinates (latitude and longitude) of a specified location in the Netherlands
    using the OpenWeather API.

    Args:
        api_key (str): The API key for accessing the OpenWeather API.
        plaats (str): The name of the location in the Netherlands.

    Returns:
        dict: A dictionary containing the latitude and longitude of the specified location.
    """
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={plaats},?,NL&limit=1&appid={api_key}"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    latitude = data[0]["lat"]
                    longitude = data[0]["lon"]
                    logging.info(f"OpenWeather geographical coordinates for {plaats}: {latitude}, {longitude}")
                    return {"latitude": latitude, "longitude": longitude}
                else:
                    raise Exception(f"Error retrieving OpenWeather data: {response.status}")
    except Exception as e:
        logging.error(f"Error retrieving OpenWeather data: {e}")     
        return None

async def main():
    import os
    from configparser import ConfigParser
    import pytz
    from datetime import timedelta

    cest = pytz.timezone('Europe/Amsterdam')
    
    current_time = datetime.now(cest)
    tomorrow = current_time + timedelta(days=1)
    tomorrow_midnight = tomorrow.replace(hour=23, minute=59, second=59, microsecond=999999)

    logging.basicConfig(level=logging.INFO)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, 'secrets.ini')

    configur = ConfigParser() 
    configur.read(secrets_file)
    openweather_api_key = configur.get('api_keys', 'openweather')

    api_key = openweather_api_key
    plaats = "Amsterdam"

    # Get geographical coordinates
    coords = await get_OpenWeather_geographical_coordinates_in_NL(api_key, plaats)
    if coords:
        weather_data = await get_OpenWeather_data(api_key, coords["latitude"], coords["longitude"], current_time, tomorrow_midnight)
        if weather_data:
            print(f"Weather data for {weather_data.metadata['city']}:")
            for key, value in weather_data.data.items():
                print(f"{key}: {value}")
        else:
            print(f"Failed to retrieve weather data for {plaats}")
    else:
        print(f"Failed to retrieve coordinates for {plaats}")

if __name__ == "__main__":
    asyncio.run(main())