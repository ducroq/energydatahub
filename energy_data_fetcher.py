import os
import shutil
from datetime import datetime, timedelta
import json
import asyncio
import logging
from typing import Dict, Any
from configparser import ConfigParser

from data_types import CombinedDataSet
from entsoe_client import get_Entsoe_data
from energy_zero_price_fetcher import get_Energy_zero_data
from epex_price_fetcher import get_Epex_data
from open_weather_client import get_OpenWeather_data
from meteoserver_client import get_MeteoServer_weather_forecast_data, get_MeteoServer_sun_forecast
from nordpool_data_fetcher import get_Elspot_data
from luchtmeetnet_data_fetcher import get_luchtmeetnet_data
from timezone_helpers import get_timezone_and_country

# Constants
LOGGING_FILE_NAME = 'energy_data_fetcher.log'
SECRETS_FILE_NAME = 'secrets.ini'
OUTPUT_FOLDER_NAME = 'data'
output_path = os.path.join(os.getcwd(), OUTPUT_FOLDER_NAME)

# TODO: extract intraday price data fro Nordpool API, https://www.nordpoolgroup.com/api/marketdata/page/10?currency=,EUR&endDate=2021-10-01&startDate=2021-09-30&area=SYS&format=json
# TODO: extract liveweer data from MeteoServer API, https://data.meteoserver.nl/api/liveweer_synop.php?lat=52.1052957&long=5.1706729&key=7daf22bed0&select=1
# TODO: extract current sun from response_data['current'][0] and add to sun forecast data
# TODO: extract current ait quality data from Luchtmeetnet API, https://api.luchtmeetnet.nl/open_api/measurements?component=NO2&location_code=NL10204&start=2021-10-01T00:00:00Z&end=2021-10-01T23:59:59Z
# TODO: possibly extract "Copernicus Atmosphere Monitoring Service (CAMS)" data from https://atmosphere.copernicus.eu/catalogue#/

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(), 
        logging.FileHandler(os.path.join(output_path, LOGGING_FILE_NAME))]
    )

def ensure_output_directory(path: str) -> None:
    """Ensure the output directory exists."""
    try:
        os.makedirs(path, exist_ok=True)
        logging.info(f"Output directory ensured: {path}")
    except OSError as e:
        logging.error(f"Error creating folder: {e}")
        raise

def load_config(script_dir: str) -> ConfigParser:
    """Load configuration from the secrets file."""
    config = ConfigParser()
    secrets_file = os.path.join(script_dir, SECRETS_FILE_NAME)
    config.read(secrets_file)
    return config

# async def fetch_forecast_data(config: ConfigParser, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
#     """Fetch data from various APIs."""
#     entsoe_api_key = config.get('api_keys', 'entsoe')
#     openweather_api_key = config.get('api_keys', 'openweather')
#     meteoserver_api_key = config.get('api_keys', 'meteo')
#     latitude = float(config.get('location', 'latitude'))
#     longitude = float(config.get('location', 'longitude'))
#     timezone, country_code = get_timezone_and_country(latitude, longitude)
#     start_time = start_time.astimezone(timezone)
#     end_time = end_time.astimezone(timezone)

#     entsoe_data = await get_Entsoe_data(entsoe_api_key, country_code, start_time, end_time)
#     energy_zero_data = await get_Energy_zero_data(start_time, end_time)
#     epex_data = await get_Epex_data(start_time, end_time)
#     elspot_data = await get_Elspot_data(country_code, start_time, end_time)
#     open_weather_data = await get_OpenWeather_data(openweather_api_key, latitude, longitude)
#     meteo_weather_data = await get_MeteoServer_weather_forecast_data(meteoserver_api_key, latitude, longitude, start_time, end_time)
#     meteo_sun_data = await get_MeteoServer_sun_forecast(meteoserver_api_key, latitude, longitude, start_time, end_time)

#     return {
#         'entsoe': entsoe_data,
#         'energy_zero': energy_zero_data,
#         'epex': epex_data,
#         'elspot': elspot_data,
#         'weather_forecast': meteo_weather_data,
#         'sun_forecast': meteo_sun_data
#     }

async def fetch_reported_data(config: ConfigParser, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """Fetch data from various APIs."""
    latitude = float(config.get('location', 'latitude'))
    longitude = float(config.get('location', 'longitude'))
    timezone, country_code = get_timezone_and_country(latitude, longitude)
    start_time = start_time.astimezone(timezone)
    end_time = end_time.astimezone(timezone)

    luchtmeetnet_data = await get_luchtmeetnet_data(latitude, longitude, start_time, end_time)

    return {
        'air_quality': luchtmeetnet_data,
    }

def write_json_file(data: Dict[str, Any], filename: str, output_path: str) -> None:
    """Write data to a JSON file."""
    full_path = os.path.join(output_path, filename)
    try:
        with open(full_path, 'w', encoding='utf-8') as fp:
            json.dump(data, fp, indent=4, sort_keys=True, default=str)
        logging.info(f"Data written to {full_path}")
    except IOError as e:
        logging.error(f"Error writing to {full_path}: {e}")
        raise

async def main() -> None:
    """Main function to orchestrate the data fetching and writing process."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ensure_output_directory(output_path)

    try:
        config = load_config(script_dir)
        entsoe_api_key = config.get('api_keys', 'entsoe')
        openweather_api_key = config.get('api_keys', 'openweather')
        meteoserver_api_key = config.get('api_keys', 'meteo')
        latitude = float(config.get('location', 'latitude'))
        longitude = float(config.get('location', 'longitude'))
        timezone, country_code = get_timezone_and_country(latitude, longitude)

        current_time = datetime.now()
        tomorrow = (current_time + timedelta(days=1))
        yesterday = current_time - timedelta(days=1)

        start_time = current_time.astimezone(timezone)
        end_time = tomorrow.astimezone(timezone)
        entsoe_data = await get_Entsoe_data(entsoe_api_key, country_code, start_time, end_time)
        energy_zero_data = await get_Energy_zero_data(start_time, end_time)
        epex_data = await get_Epex_data(start_time, end_time)
        elspot_data = await get_Elspot_data(country_code, start_time, end_time)

        combined_data = CombinedDataSet()
        combined_data.add_dataset('entsoe', entsoe_data)
        combined_data.add_dataset('energy_zero', energy_zero_data)
        combined_data.add_dataset('epex', epex_data)
        combined_data.add_dataset('elspot', elspot_data)

        full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_energy_price_forecast.json")
        combined_data.write_to_json(full_path)
        shutil.copy(full_path, os.path.join(output_path, "energy_price_forecast.json"))

        open_weather_data = await get_OpenWeather_data(openweather_api_key, latitude, longitude, start_time, end_time)
        meteo_weather_data = await get_MeteoServer_weather_forecast_data(meteoserver_api_key, latitude, longitude, start_time, end_time)
        combined_data = CombinedDataSet()
        combined_data.add_dataset('OpenWeather', open_weather_data)
        combined_data.add_dataset('MeteoServer', meteo_weather_data)
        full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast.json")
        combined_data.write_to_json(full_path)
        shutil.copy(full_path, os.path.join(output_path, "weather_forecast.json"))

        # meteo_sun_data = await get_MeteoServer_sun_forecast(meteoserver_api_key, latitude, longitude, start_time, end_time)        
        # full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_sun_forecast.json")


        # # Write weather forecast
        # write_json_file(forecast_data['weather_forecast'], f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast.json", output_path)
        # shutil.copy(os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast.json"), 
        #             os.path.join(output_path, "weather_forecast.json"))

        # # Write sun forecast
        # write_json_file(forecast_data['sun_forecast'], f"{datetime.now().strftime('%y%m%d_%H%M%S')}_sun_forecast.json", output_path)
        # shutil.copy(os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_sun_forecast.json"), 
        #             os.path.join(output_path, "sun_forecast.json"))
        
        # # Write air quality report
        # write_json_file(reported_data['air_quality'], f"{datetime.now().strftime('%y%m%d_%H%M%S')}_air_quality.json", output_path)
        # shutil.copy(os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_air_quality.json"), 
        #             os.path.join(output_path, "air_quality.json"))

    except Exception as e:
        logging.error(e)

if __name__ == "__main__":
    asyncio.run(main())