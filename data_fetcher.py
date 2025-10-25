"""
Internet data acquisition for energy applications
--------------------------------------------------
Part of the Energy Data Integration Project at HAN University of Applied Sciences.

File: data_fetcher.py
Created: 2024-10-19
Updated: 2025-10-25

Author: Jeroen Veen
        HAN University of Applied Sciences
        Arnhem, the Netherlands
Contributors:        

Copyright (c) 2024 HAN University of Applied Sciences
All rights reserved.

This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.

Project Contributors:
    - HAN H2 LAB IPKW Development Team
    Initial development and integration with energy conversion systems

Description:
    Main orchestrator for fetching energy and weather data from various external APIs.
    Handles data collection, validation, and storage for day-ahead energy prices,
    weather forecasts, solar data, and air quality measurements.

    Uses the new BaseCollector architecture (Phase 4) with:
    - Automatic retry with exponential backoff
    - Structured logging with correlation IDs
    - Timestamp normalization to Europe/Amsterdam
    - Data validation and quality checks
    - Performance metrics tracking

Dependencies:
    - aiohttp: For async HTTP requests
    - pytz: Timezone handling
    - pandas: Data manipulation
    - cryptography: Data encryption/decryption
    Required local packages:
    - utils.*
    - collectors.* (new architecture)

Usage:
    Can be run directly:
        python data_fetcher.py
    Or imported as a module:
        from data_fetcher import main
        asyncio.run(main())

Notes:
    - Requires configuration in secrets.ini for API keys
    - Implements automated retry mechanisms for API failures
    - Supports both encrypted and unencrypted data storage
    - All timestamps handled in UTC and converted to local timezone
"""
import os
import shutil
from datetime import datetime, timedelta
import asyncio
import logging
import base64
import platform

from utils.helpers import ensure_output_directory, load_settings, load_secrets, save_data_file
from utils.data_types import CombinedDataSet
from utils.timezone_helpers import get_timezone_and_country
from utils.secure_data_handler import SecureDataHandler
# New collector architecture imports
from collectors import (
    EntsoeCollector,
    EnergyZeroCollector,
    EpexCollector,
    ElspotCollector,
    OpenWeatherCollector,
    MeteoServerWeatherCollector,
    MeteoServerSunCollector,
    LuchtmeetnetCollector
)

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
        config = load_settings(script_dir, SETTINGS_FILE_NAME)
        latitude = float(config.get('location', 'latitude'))
        longitude = float(config.get('location', 'longitude'))
        timezone, country_code = get_timezone_and_country(latitude, longitude)
        encryption = bool(config.getint('data', 'encryption'))

        config = load_secrets(script_dir, SECRETS_FILE_NAME)        
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

        # Initialize collectors with new architecture
        entsoe_collector = EntsoeCollector(api_key=entsoe_api_key)
        energy_zero_collector = EnergyZeroCollector()
        epex_collector = EpexCollector()
        elspot_collector = ElspotCollector()
        openweather_collector = OpenWeatherCollector(
            api_key=openweather_api_key,
            latitude=latitude,
            longitude=longitude
        )
        meteoserver_weather_collector = MeteoServerWeatherCollector(
            api_key=meteoserver_api_key,
            latitude=latitude,
            longitude=longitude
        )
        meteoserver_sun_collector = MeteoServerSunCollector(
            api_key=meteoserver_api_key,
            latitude=latitude,
            longitude=longitude
        )
        luchtmeetnet_collector = LuchtmeetnetCollector(
            latitude=latitude,
            longitude=longitude
        )

        # Collect data from all sources
        tasks = [
            entsoe_collector.collect(today, tomorrow, country_code=country_code),
            energy_zero_collector.collect(today, tomorrow),
            epex_collector.collect(today, tomorrow),
            openweather_collector.collect(today, tomorrow),
            meteoserver_weather_collector.collect(today, tomorrow),
            meteoserver_sun_collector.collect(today, tomorrow),
            elspot_collector.collect(today, tomorrow, country_code=country_code),
            luchtmeetnet_collector.collect(yesterday, today)
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
            save_data_file(data=combined_data, file_path=full_path, handler=handler, encrypt=encryption)
            # if encryption:
            #     encrypted_data = handler.encrypt_and_sign(combined_data.to_dict())
            #     with open(full_path, 'w') as f:
            #         f.write(encrypted_data)
            # else:
            #     combined_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "energy_price_forecast.json"))

        combined_data = CombinedDataSet()
        combined_data.add_dataset('OpenWeather', open_weather_data)
        combined_data.add_dataset('MeteoServer', meteo_weather_data)
        if combined_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast.json")
            save_data_file(data=combined_data, file_path=full_path, handler=handler, encrypt=encryption)
            # if encryption:
            #     encrypted_data = handler.encrypt_and_sign(combined_data.to_dict())
            #     with open(full_path, 'w') as f:
            #         f.write(encrypted_data)
            # else:
            #     combined_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "weather_forecast.json"))

        if meteo_sun_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_sun_forecast.json")
            save_data_file(data=meteo_sun_data, file_path=full_path, handler=handler, encrypt=encryption)
            # if encryption:
            #     encrypted_data = handler.encrypt_and_sign(meteo_sun_data.to_dict())
            #     with open(full_path, 'w') as f:
            #         f.write(encrypted_data)
            # else:
            #     meteo_sun_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "sun_forecast.json"))

        if luchtmeetnet_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_air_history.json")
            save_data_file(data=luchtmeetnet_data, file_path=full_path, handler=handler, encrypt=encryption)
            # if encryption:
            #     encrypted_data = handler.encrypt_and_sign(luchtmeetnet_data.to_dict())
            #     with open(full_path, 'w') as f:
            #         f.write(encrypted_data)
            # else:
            #     luchtmeetnet_data.write_to_json(full_path)
            shutil.copy(full_path, os.path.join(output_path, "air_history.json"))

    except Exception as e:
        logging.error(e)

if __name__ == "__main__":
    # Set appropriate event loop policy for Windows
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())        

    asyncio.run(main())