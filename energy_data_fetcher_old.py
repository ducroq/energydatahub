import os
import shutil
from datetime import datetime, timedelta
import json
import asyncio
import logging
import pytz
from entsoe_client import get_Entsoe_data
from energy_zero_price_fetcher import get_Energy_zero_data
from epex_price_fetcher import get_Epex_data
from open_weather_client import get_OpenWeather_data, get_OpenWeather_geographical_coordinates_in_NL
from meteoserver_client import get_MeteoServer_weather_forecast_data, get_MeteoServer_sun_forecast
from configparser import ConfigParser 

LOGGING_FILE_NAME = 'energyDataScraper.log'

script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(os.getcwd(), 'data')

try:
    if not os.path.exists(output_path):
        os.makedirs(output_path)        
except OSError as e:
    print(f"Error creating folder: {e}")

# file_list = [f for f in os.listdir(output_path) if f.endswith(".json")]
# for f in file_list:
#     os.remove(os.path.join(output_path, f))

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(os.path.join(output_path, LOGGING_FILE_NAME))]
    )

async def main():
    secrets_file = os.path.join(script_dir, 'secrets.ini')
# get the api keys and location from the secrets.ini file
    configur = ConfigParser() 
    configur.read(secrets_file)
    entsoe_api_key = configur.get('api_keys', 'entsoe')
    openweather_api_key = configur.get('api_keys', 'openweather')
    meteoserver_api_key = configur.get('api_keys', 'meteo')
    plaats = configur.get('location', 'plaats')
# get the geographical coordinates of the location
    location = await get_OpenWeather_geographical_coordinates_in_NL(api_key=openweather_api_key, plaats=plaats)
    latitude = location['latitude']
    longitude = location['longitude']
    country_code = configur.get('location', 'country_code')
# get time information
    local_timezone = pytz.timezone(configur.get('location', 'timezone'))
    current_time = datetime.now(local_timezone)
    tomorrow_midnight = (current_time + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
# get the data
    entsoe_data = await get_Entsoe_data(entsoe_api_key, country_code, start_time=current_time, end_time=tomorrow_midnight)
    energy_zero_data = await get_Energy_zero_data(start_time=current_time, end_time=tomorrow_midnight)
    epex_data = await get_Epex_data(start_time=current_time, end_time=tomorrow_midnight)
    weather_data = await get_OpenWeather_data(api_key=openweather_api_key, latitude=latitude, longitude=longitude)
    weather_forecast = await get_MeteoServer_weather_forecast_data(meteoserver_api_key, plaats)
    sun_forecast = await get_MeteoServer_sun_forecast(meteoserver_api_key, plaats)

# write the data to json files
    json_file_name = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_energy_price_forecast.json")
    json_data = {}
    json_data['energy zero price forecast'] = energy_zero_data
    json_data['entsoe price forecast'] = entsoe_data
    json_data['epex price forecast'] = epex_data
    json_data['units'] = {"energy_zero_price": "EUR/kWh (incl. VAT)",
                          "entsoe_price": "EUR/MWh",
                          "epex_price": "EUR/MWh"}
    json_data['metadata'] = {"energy_zero_source": "EnergyZero API v2.1",
                             "entsoe_source": "ENTSO-E Transparency Platform API v1.3",
                             "epex_source": "Awattar API"}
    with open(json_file_name, 'w', encoding='utf-8') as fp:
        json.dump(json_data, fp, indent=4, sort_keys=True, default=str)
    shutil.copy(json_file_name, os.path.join(output_path, "energy_price_forecast.json"))

    json_file_name = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast.json")
    with open(json_file_name, 'w', encoding='utf-8') as fp:
        json.dump(weather_forecast, fp, indent=4, sort_keys=True, default=str)
    shutil.copy(json_file_name, os.path.join(output_path, "weather_forecast.json"))    

    json_file_name = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_sun_forecast.json")
    with open(json_file_name, 'w', encoding='utf-8') as fp:
        json.dump(sun_forecast, fp, indent=4, sort_keys=True, default=str)
    shutil.copy(json_file_name, os.path.join(output_path, "sun_forecast.json"))    

if __name__ == "__main__":
    asyncio.run(main())            
