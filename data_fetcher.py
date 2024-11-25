import os
import json
import shutil
from datetime import datetime, timedelta
import asyncio
import logging
import base64
import platform

from core.helpers import ensure_output_directory, load_config
from core.data_types import CombinedDataSet
from core.timezone_helpers import get_timezone_and_country
from core.secure_data_handler import SecureDataHandler
from energy_data_fetchers.entsoe_client import get_Entsoe_data
from energy_data_fetchers.energy_zero_price_fetcher import get_Energy_zero_data
from energy_data_fetchers.epex_price_fetcher import get_Epex_data
from energy_data_fetchers.nordpool_data_fetcher import get_Elspot_data
from weather_data_fetchers.open_weather_client import get_OpenWeather_data
from weather_data_fetchers.meteoserver_client import get_MeteoServer_weather_forecast_data, get_MeteoServer_sun_forecast
from weather_data_fetchers.luchtmeetnet_data_fetcher import get_luchtmeetnet_data

# Constants
LOGGING_FILE_NAME = 'energy_data_fetcher.log'
SETTINGS_FILE_NAME = 'settings.ini'
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

async def main() -> None:
    """Main function to orchestrate the data fetching and writing process."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ensure_output_directory(output_path)

    try:
        config = load_config(script_dir, SETTINGS_FILE_NAME)
        latitude = float(config.get('location', 'latitude'))
        longitude = float(config.get('location', 'longitude'))
        timezone, country_code = get_timezone_and_country(latitude, longitude)

        config = load_config(script_dir, SECRETS_FILE_NAME)        
        entsoe_api_key = config.get('api_keys', 'entsoe')
        openweather_api_key = config.get('api_keys', 'openweather')
        meteoserver_api_key = config.get('api_keys', 'meteo')
        encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
        hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))
        handler = SecureDataHandler(encryption_key, hmac_key)

        current_time = datetime.now()
        tomorrow = (current_time + timedelta(days=1))
        yesterday = current_time - timedelta(days=1)

        today = current_time.astimezone(timezone)
        tomorrow = tomorrow.astimezone(timezone)
        yesterday = yesterday.astimezone(timezone)

        tasks = [
            get_Entsoe_data(entsoe_api_key, country_code, today, tomorrow),
            get_Energy_zero_data(today, tomorrow),
            get_Epex_data(today, tomorrow),
            get_OpenWeather_data(openweather_api_key, latitude, longitude, today, tomorrow),
            get_MeteoServer_weather_forecast_data(meteoserver_api_key, latitude, longitude, today, tomorrow),
            get_MeteoServer_sun_forecast(meteoserver_api_key, latitude, longitude, today, tomorrow),
            get_Elspot_data(country_code, today, tomorrow),
            get_luchtmeetnet_data(latitude, longitude, yesterday, today)
        ]

        results = await asyncio.gather(*tasks)
        entsoe_data, energy_zero_data, epex_data, open_weather_data, meteo_weather_data, meteo_sun_data, elspot_data, luchtmeetnet_data = results

        combined_data = CombinedDataSet()
        combined_data.add_dataset('entsoe', entsoe_data)
        combined_data.add_dataset('energy_zero', energy_zero_data)
        combined_data.add_dataset('epex', epex_data)
        combined_data.add_dataset('elspot', elspot_data)
        if combined_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_energy_price_forecast.json")
# todo: make more efficient
            combined_data.write_to_json(full_path)
            data = json.load(open(full_path, 'r'))
            encrypted = handler.encrypt_and_sign(data)
            with open(full_path, 'w') as f:
                json.dump(encrypted, f, indent=2)
            shutil.copy(full_path, os.path.join(output_path, "energy_price_forecast.json"))

        combined_data = CombinedDataSet()
        combined_data.add_dataset('OpenWeather', open_weather_data)
        combined_data.add_dataset('MeteoServer', meteo_weather_data)
        if combined_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast.json")
            encrypted_data = handler.encrypt_and_sign(combined_data.to_dict())
            with open(full_path, 'w') as f:
                f.write(encrypted_data)
            # combined_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "weather_forecast.json"))

        if meteo_sun_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_sun_forecast.json")
            meteo_sun_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "sun_forecast.json"))

        if luchtmeetnet_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_air_history.json")
            luchtmeetnet_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "air_history.json"))

    except Exception as e:
        logging.error(e)

if __name__ == "__main__":
    # Set appropriate event loop policy for Windows
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())        

    asyncio.run(main())