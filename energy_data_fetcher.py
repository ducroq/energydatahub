import os
import shutil
from datetime import datetime, timedelta
import asyncio
import logging
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

        today = current_time.astimezone(timezone)
        tomorrow = tomorrow.astimezone(timezone)
        yesterday = yesterday.astimezone(timezone)

        entsoe_data = await get_Entsoe_data(entsoe_api_key, country_code, today, tomorrow)
        energy_zero_data = await get_Energy_zero_data(today, tomorrow)
        epex_data = await get_Epex_data(today, tomorrow)
        elspot_data = await get_Elspot_data(country_code, today, tomorrow)

        combined_data = CombinedDataSet()
        combined_data.add_dataset('entsoe', entsoe_data)
        combined_data.add_dataset('energy_zero', energy_zero_data)
        combined_data.add_dataset('epex', epex_data)
        combined_data.add_dataset('elspot', elspot_data)
        if combined_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_energy_price_forecast.json")
            combined_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "energy_price_forecast.json"))

        open_weather_data = await get_OpenWeather_data(openweather_api_key, latitude, longitude, today, tomorrow)
        meteo_weather_data = await get_MeteoServer_weather_forecast_data(meteoserver_api_key, latitude, longitude, today, tomorrow)
        combined_data = CombinedDataSet()
        combined_data.add_dataset('OpenWeather', open_weather_data)
        combined_data.add_dataset('MeteoServer', meteo_weather_data)
        if combined_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast.json")
            combined_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "weather_forecast.json"))

        meteo_sun_data = await get_MeteoServer_sun_forecast(meteoserver_api_key, latitude, longitude, today, tomorrow)        
        if meteo_sun_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_sun_forecast.json")
            meteo_sun_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "sun_forecast.json"))

        luchtmeetnet_data = await get_luchtmeetnet_data(latitude, longitude, yesterday, today)
        if luchtmeetnet_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_air_history.json")
            luchtmeetnet_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "air_history.json"))

    except Exception as e:
        logging.error(e)

if __name__ == "__main__":
    asyncio.run(main())