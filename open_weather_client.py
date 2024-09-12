import asyncio
import logging
from datetime import datetime
import aiohttp

async def get_OpenWeather_data(api_key: str, latitude: str, longitude: str) -> dict:
    """
    Retrieves weather data from the OpenWeather API based on the configured latitude and longitude.

    Args:
        api_key (str): The OpenWeather API key.
        latitude (str): The latitude of the location (-90; 90).
        longitude (str): The longitude of the location (-180; 180).

    Returns:
        dict: A dictionary containing weather data including temperature, humidity, pressure, etc.
    """
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    weather_data = {
                        "temperature": data["main"]["temp"],
                        "humidity": data["main"]["humidity"],
                        "pressure": data["main"]["pressure"],
                        "weather_id": data["weather"][0]["id"],
                        "weather_description": data["weather"][0]["description"],
                        "wind_speed": data["wind"]["speed"],
                        "wind_direction": data["wind"]["deg"],
                        "visibility": data["visibility"],
                        "cloudiness": data["clouds"]["all"]
                    }
                    return weather_data
                else:
                    raise Exception(f"Error retrieving OpenWeather data: {response.status}")
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

    logging.basicConfig(level=logging.INFO)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, 'secrets.ini')

    configur = ConfigParser() 
    configur.read(secrets_file)
    openweather_api_key = configur.get('api_keys', 'openweather')
    meteoserver_api_key = configur.get('api_keys', 'meteo')

    api_key = openweather_api_key
    plaats = "Amsterdam"

    # Get geographical coordinates
    coords = await get_OpenWeather_geographical_coordinates_in_NL(api_key, plaats)
    if coords:
        # Get weather data
        weather_data = await get_OpenWeather_data(api_key, coords["latitude"], coords["longitude"])
        if weather_data:
            print(f"Weather data for {plaats}:")
            for key, value in weather_data.items():
                print(f"{key}: {value}")
        else:
            print(f"Failed to retrieve weather data for {plaats}")
    else:
        print(f"Failed to retrieve coordinates for {plaats}")

if __name__ == "__main__":
    asyncio.run(main())