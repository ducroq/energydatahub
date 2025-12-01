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
    EntsoeWindCollector,
    EnergyZeroCollector,
    EpexCollector,
    ElspotCollector,
    OpenWeatherCollector,
    GoogleWeatherCollector,
    MeteoServerWeatherCollector,
    MeteoServerSunCollector,
    LuchtmeetnetCollector,
    TennetCollector
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

def _extract_wind_from_weather(google_weather_data, offshore_locations):
    """
    Extract wind-specific data from Google Weather multi-location forecasts.

    Filters to offshore wind locations and extracts only wind-relevant fields
    (wind_speed, wind_direction) for use in price prediction models.

    Args:
        google_weather_data: EnhancedDataSet from GoogleWeatherCollector
        offshore_locations: List of offshore location dicts with 'name' key

    Returns:
        Dict mapping location names to timestamp -> wind data
    """
    if not google_weather_data or not google_weather_data.data:
        return None

    offshore_names = {loc['name'] for loc in offshore_locations}
    wind_data = {}

    for location_name, location_data in google_weather_data.data.items():
        # Only include offshore wind locations
        if location_name not in offshore_names:
            continue

        if not isinstance(location_data, dict):
            continue

        location_wind = {}
        for timestamp, weather_values in location_data.items():
            if not isinstance(weather_values, dict):
                continue

            # Extract wind fields
            wind_entry = {}
            if 'wind_speed' in weather_values:
                wind_entry['wind_speed'] = weather_values['wind_speed']
            if 'wind_direction' in weather_values:
                wind_entry['wind_direction'] = weather_values['wind_direction']

            if wind_entry:
                location_wind[timestamp] = wind_entry

        if location_wind:
            wind_data[location_name] = location_wind

    return wind_data if wind_data else None


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
        google_weather_api_key = config.get('api_keys', 'google_weather')
        tennet_api_key = config.get('api_keys', 'tennet')
        encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
        hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))
        handler = SecureDataHandler(encryption_key, hmac_key)

        # Strategic locations for pan-European weather (Model A - price prediction)
        # See WEATHER_LOCATION_STRATEGY.md for detailed rationale
        strategic_locations = [
            # Germany - Most important (50%+ of EU renewable capacity)
            {"name": "Hamburg_DE", "lat": 53.5511, "lon": 9.9937},      # North German wind belt
            {"name": "Munich_DE", "lat": 48.1351, "lon": 11.5820},      # South German solar belt

            # Netherlands - Our market
            {"name": "Arnhem_NL", "lat": 51.9851, "lon": 5.8987},       # Local + central NL
            {"name": "IJmuiden_NL", "lat": 52.4608, "lon": 4.6262},     # Offshore wind proxy

            # Belgium - Coupled market
            {"name": "Brussels_BE", "lat": 50.8503, "lon": 4.3517},     # Market coupling

            # Denmark - Wind powerhouse
            {"name": "Esbjerg_DK", "lat": 55.4760, "lon": 8.4516},      # North Sea wind
        ]

        # Offshore wind farm locations for enhanced wind forecasting
        # These locations represent major North Sea offshore wind areas
        offshore_wind_locations = [
            # Dutch offshore wind farms
            {"name": "Borssele_NL", "lat": 51.7000, "lon": 3.0000},     # Borssele wind farm area (1.5 GW)
            {"name": "HollandseKust_NL", "lat": 52.5000, "lon": 4.2000},# Hollandse Kust (3.5 GW planned)
            {"name": "Gemini_NL", "lat": 54.0361, "lon": 5.9625},       # Gemini wind farm (600 MW)
            {"name": "IJmuidenVer_NL", "lat": 52.8500, "lon": 3.5000},  # IJmuiden Ver (4 GW planned)

            # German Bight (major capacity)
            {"name": "HelgolandCluster_DE", "lat": 54.2000, "lon": 7.5000},  # German offshore cluster
            {"name": "BorkumRiffgrund_DE", "lat": 53.9667, "lon": 6.5500},   # Borkum Riffgrund area

            # UK Dogger Bank (world's largest offshore wind farm)
            {"name": "DoggerBank_UK", "lat": 54.7500, "lon": 2.5000},   # Dogger Bank (3.6 GW)

            # Danish North Sea
            {"name": "HornsRev_DK", "lat": 55.4833, "lon": 7.8500},     # Horns Rev wind farms

            # Belgian offshore
            {"name": "NorthSeaBE_BE", "lat": 51.5833, "lon": 2.8000},   # Belgian offshore cluster
        ]

        # Combine strategic + offshore for comprehensive wind coverage
        all_weather_locations = strategic_locations + offshore_wind_locations

        # Calculate day boundaries for proper day-ahead forecasting
        current_time = datetime.now(timezone)

        # Start from beginning of current day
        today = current_time.replace(hour=0, minute=0, second=0, microsecond=0)

        # End at end of tomorrow (23:59:59) - for standard forecasts
        tomorrow = today + timedelta(days=2) - timedelta(seconds=1)

        # Extended horizon for Google Weather (10 days) - for Model A price prediction
        ten_days_ahead = today + timedelta(days=10)

        # Yesterday for historical data (previous 24 hours)
        yesterday = today - timedelta(days=1)

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
        googleweather_collector = GoogleWeatherCollector(
            api_key=google_weather_api_key,
            locations=all_weather_locations,  # Includes offshore wind locations
            hours=240  # 10 days hourly forecast
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
        tennet_collector = TennetCollector(api_key=tennet_api_key)

        # ENTSO-E Wind Generation Forecast collector (for price prediction)
        entsoe_wind_collector = EntsoeWindCollector(
            api_key=entsoe_api_key,
            country_codes=['NL', 'DE_LU', 'BE', 'DK_1']  # Key wind markets affecting NL prices
        )

        # Collect data from all sources
        tasks = [
            entsoe_collector.collect(today, tomorrow, country_code=country_code),
            energy_zero_collector.collect(today, tomorrow),
            epex_collector.collect(today, tomorrow),
            openweather_collector.collect(today, tomorrow),
            googleweather_collector.collect(today, ten_days_ahead),  # 10-day forecast for Model A
            meteoserver_weather_collector.collect(today, tomorrow),
            meteoserver_sun_collector.collect(today, tomorrow),
            elspot_collector.collect(today, tomorrow, country_code=country_code),
            luchtmeetnet_collector.collect(yesterday, today),
            tennet_collector.collect(yesterday, today),  # TenneT data has a delay, use historical data
            entsoe_wind_collector.collect(today, tomorrow)  # Wind generation forecasts
        ]

        results = await asyncio.gather(*tasks)
        entsoe_data, energy_zero_data, epex_data, open_weather_data, google_weather_data, meteo_weather_data, meteo_sun_data, elspot_data, luchtmeetnet_data, tennet_data, entsoe_wind_data = results

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

        # Save Google Weather multi-location data separately for Model A (price prediction)
        if google_weather_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast_multi_location.json")
            save_data_file(data=google_weather_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "weather_forecast_multi_location.json"))
            logging.info(f"Saved multi-location weather forecast for {len(all_weather_locations)} locations (including {len(offshore_wind_locations)} offshore wind sites)")

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

        if tennet_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_grid_imbalance.json")
            save_data_file(data=tennet_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "grid_imbalance.json"))
            logging.info(f"Saved TenneT grid imbalance data with {tennet_data.metadata.get('data_points', 0)} data points")

        # Wind forecast output - combines ENTSO-E generation forecasts with weather-based wind data
        # This dedicated wind file is optimized for price prediction models
        wind_combined_data = CombinedDataSet()

        # Add ENTSO-E wind generation forecasts (MW by country)
        if entsoe_wind_data:
            wind_combined_data.add_dataset('entsoe_wind_generation', entsoe_wind_data)
            logging.info(f"Added ENTSO-E wind generation forecasts for {len(entsoe_wind_data.data)} countries")

        # Extract wind-specific data from Google Weather multi-location forecasts
        if google_weather_data:
            # Create a wind-focused extract from weather data
            wind_weather_data = _extract_wind_from_weather(google_weather_data, offshore_wind_locations)
            if wind_weather_data:
                from utils.data_types import EnhancedDataSet
                wind_weather_dataset = EnhancedDataSet(
                    metadata={
                        'data_type': 'weather_wind',
                        'source': 'Google Weather API (wind extract)',
                        'units': 'm/s (wind_speed), degrees (wind_direction)',
                        'description': 'Wind speed and direction at offshore wind farm locations',
                        'locations': [loc['name'] for loc in offshore_wind_locations]
                    },
                    data=wind_weather_data
                )
                wind_combined_data.add_dataset('weather_wind', wind_weather_dataset)
                logging.info(f"Added weather-based wind data for {len(offshore_wind_locations)} offshore locations")

        # Save combined wind forecast
        if wind_combined_data.datasets:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_wind_forecast.json")
            save_data_file(data=wind_combined_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "wind_forecast.json"))
            logging.info(f"Saved combined wind forecast with {len(wind_combined_data.datasets)} data sources")

    except Exception as e:
        logging.error(e)

if __name__ == "__main__":
    # Set appropriate event loop policy for Windows
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())        

    asyncio.run(main())