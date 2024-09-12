import os
import shutil
from datetime import datetime, timedelta
import json
import asyncio
import logging
from typing import Dict, Any
import pytz
from configparser import ConfigParser

from entsoe_client import get_Entsoe_data
from energy_zero_price_fetcher import get_Energy_zero_data
from epex_price_fetcher import get_Epex_data
from open_weather_client import get_OpenWeather_data, get_OpenWeather_geographical_coordinates_in_NL
from meteoserver_client import get_MeteoServer_weather_forecast_data, get_MeteoServer_sun_forecast

# Constants
LOGGING_FILE_NAME = 'energy_data_fetcher.log'
SECRETS_FILE_NAME = 'secrets.ini'
OUTPUT_FOLDER_NAME = 'data'
output_path = os.path.join(os.getcwd(), OUTPUT_FOLDER_NAME)

# setup logging
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

async def fetch_data(config: ConfigParser, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """Fetch data from various APIs."""
    entsoe_api_key = config.get('api_keys', 'entsoe')
    openweather_api_key = config.get('api_keys', 'openweather')
    meteoserver_api_key = config.get('api_keys', 'meteo')
    plaats = config.get('location', 'plaats')
    country_code = config.get('location', 'country_code')

    location = await get_OpenWeather_geographical_coordinates_in_NL(api_key=openweather_api_key, plaats=plaats)
    latitude, longitude = location['latitude'], location['longitude']

    tasks = [
        get_Entsoe_data(entsoe_api_key, country_code, start_time=start_time, end_time=end_time),
        get_Energy_zero_data(start_time=start_time, end_time=end_time),
        get_Epex_data(start_time=start_time, end_time=end_time),
        get_OpenWeather_data(api_key=openweather_api_key, latitude=latitude, longitude=longitude),
        get_MeteoServer_weather_forecast_data(meteoserver_api_key, plaats),
        get_MeteoServer_sun_forecast(meteoserver_api_key, plaats)
    ]

    entsoe_data, energy_zero_data, epex_data, open_weather_data, meteo_weather_data, meteo_sun_data = await asyncio.gather(*tasks)

    return {
        'entsoe': entsoe_data,
        'energy_zero': energy_zero_data,
        'epex': epex_data,
        'weather_forecast': meteo_weather_data,
        'sun_forecast': meteo_sun_data
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

    config = load_config(script_dir)
    local_timezone = pytz.timezone(config.get('location', 'timezone'))
    current_time = datetime.now(local_timezone)
    tomorrow_midnight = (current_time + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

    try:
        data = await fetch_data(config, current_time, tomorrow_midnight)

        # Prepare and write energy price forecast
        energy_price_forecast = {
            'energy zero price forecast': data['energy_zero'],
            'entsoe price forecast': data['entsoe'],
            'epex price forecast': data['epex'],
            'price units': {
                "energy zero": "EUR/kWh (incl. VAT)",
                "entsoe": "EUR/MWh",
                "epex": "EUR/MWh"
            },
            'data sources': {
                "energy zero": "EnergyZero API v2.1",
                "entsoe": "ENTSO-E Transparency Platform API v1.3",
                "epex": "Awattar API"
            }
        }
        write_json_file(energy_price_forecast, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_energy_price_forecast.json", output_path)
        shutil.copy(os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_energy_price_forecast.json"), 
                    os.path.join(output_path, "energy_price_forecast.json"))

        # Write weather forecast
        write_json_file(data['weather_forecast'], f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast.json", output_path)
        shutil.copy(os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast.json"), 
                    os.path.join(output_path, "weather_forecast.json"))

        # Write sun forecast
        write_json_file(data['sun_forecast'], f"{datetime.now().strftime('%y%m%d_%H%M%S')}_sun_forecast.json", output_path)
        shutil.copy(os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_sun_forecast.json"), 
                    os.path.join(output_path, "sun_forecast.json"))

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())