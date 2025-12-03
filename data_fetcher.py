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
    Main orchestrator for fetching energy data for electricity price prediction.
    Collects national/regional data including day-ahead prices, wind/solar generation,
    grid balance, and multi-location weather data affecting supply and demand.

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
from utils.data_types import CombinedDataSet, EnhancedDataSet
from utils.timezone_helpers import get_timezone_and_country
from utils.secure_data_handler import SecureDataHandler
from utils.calendar_features import get_calendar_features_for_range, get_upcoming_holidays
# New collector architecture imports
from collectors import (
    EntsoeCollector,
    EntsoeWindCollector,
    EntsoeFlowsCollector,
    EntsoeLoadCollector,
    EntsoeGenerationCollector,
    EnergyZeroCollector,
    EpexCollector,
    ElspotCollector,
    GoogleWeatherCollector,
    TennetCollector,
    NedCollector,
    OpenMeteoSolarCollector,
    OpenMeteoWeatherCollector,
    MarketProxyCollector
)
from collectors.openmeteo_offshore_wind import OpenMeteoOffshoreWindCollector

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
        google_weather_api_key = config.get('api_keys', 'google_weather')
        tennet_api_key = config.get('api_keys', 'tennet')
        # NED.nl API key (optional - only available after registration approval)
        try:
            ned_api_key = config.get('api_keys', 'ned')
        except Exception:
            ned_api_key = None
            logging.info("NED.nl API key not configured - skipping NED.nl collection")

        # Alpha Vantage API key (optional - for carbon/gas price proxies)
        try:
            alpha_vantage_api_key = config.get('api_keys', 'alpha_vantage')
        except Exception:
            alpha_vantage_api_key = None
            logging.info("Alpha Vantage API key not configured - skipping market proxies")
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

        # Offshore wind farm locations - actual open-sea coordinates
        # These are fetched via Open-Meteo (supports offshore) instead of Google Weather (404 errors)
        offshore_wind_locations = [
            # Dutch offshore wind farms
            {"name": "Borssele_NL", "lat": 51.7000, "lon": 3.0000},      # Borssele wind farm area (1.5 GW)
            {"name": "HollandseKust_NL", "lat": 52.5000, "lon": 4.2000}, # Hollandse Kust (3.5 GW planned)
            {"name": "Gemini_NL", "lat": 54.0361, "lon": 5.9625},        # Gemini wind farm (600 MW)
            {"name": "IJmuidenVer_NL", "lat": 52.8500, "lon": 3.5000},   # IJmuiden Ver (4 GW planned)

            # German Bight (major capacity)
            {"name": "HelgolandCluster_DE", "lat": 54.2000, "lon": 7.5000},  # German offshore cluster
            {"name": "BorkumRiffgrund_DE", "lat": 53.9667, "lon": 6.5500},   # Borkum Riffgrund area

            # UK Dogger Bank (world's largest offshore wind farm)
            {"name": "DoggerBank_UK", "lat": 54.7500, "lon": 2.5000},    # Dogger Bank (3.6 GW)

            # Danish North Sea
            {"name": "HornsRev_DK", "lat": 55.4833, "lon": 7.8500},      # Horns Rev wind farms

            # Belgian offshore
            {"name": "NorthSeaBE_BE", "lat": 51.5833, "lon": 2.8000},    # Belgian offshore cluster
        ]

        # Google Weather uses only strategic onshore locations (doesn't support open-sea)
        # Offshore wind data comes from Open-Meteo instead
        all_weather_locations = strategic_locations

        # Solar production locations for supply prediction
        # High solar density areas in NL and neighboring countries
        solar_locations = [
            # Netherlands - high solar density provinces
            {"name": "Rotterdam_NL", "lat": 51.9225, "lon": 4.4792},      # Zuid-Holland (highest density)
            {"name": "Eindhoven_NL", "lat": 51.4416, "lon": 5.4697},      # Noord-Brabant (high density)
            {"name": "Lelystad_NL", "lat": 52.5185, "lon": 5.4714},       # Flevoland (large solar farms)
            {"name": "Groningen_NL", "lat": 53.2194, "lon": 6.5665},      # Northern NL coverage

            # Germany - major solar capacity
            {"name": "Munich_DE", "lat": 48.1351, "lon": 11.5820},        # Bavaria (highest solar)
            {"name": "Stuttgart_DE", "lat": 48.7758, "lon": 9.1829},      # Baden-Württemberg

            # Belgium
            {"name": "Antwerp_BE", "lat": 51.2194, "lon": 4.4025},        # Flanders solar
        ]

        # Population centers for demand prediction (temperature affects heating/cooling demand)
        # Major cities weighted by population for aggregated demand estimation
        population_centers = [
            # Netherlands - major population centers (17.5M total)
            {"name": "Amsterdam_NL", "lat": 52.3676, "lon": 4.9041, "population": 872680},
            {"name": "Rotterdam_NL", "lat": 51.9225, "lon": 4.4792, "population": 651446},
            {"name": "The_Hague_NL", "lat": 52.0705, "lon": 4.3007, "population": 545838},
            {"name": "Utrecht_NL", "lat": 52.0907, "lon": 5.1214, "population": 361924},
            {"name": "Eindhoven_NL", "lat": 51.4416, "lon": 5.4697, "population": 238478},
            {"name": "Groningen_NL", "lat": 53.2194, "lon": 6.5665, "population": 233273},

            # Germany - key population centers in coupled market regions
            {"name": "Dusseldorf_DE", "lat": 51.2277, "lon": 6.7735, "population": 620523},   # Near NL border
            {"name": "Cologne_DE", "lat": 50.9375, "lon": 6.9603, "population": 1083498},    # Rhineland
            {"name": "Hamburg_DE", "lat": 53.5511, "lon": 9.9937, "population": 1906411},    # North Germany

            # Belgium - coupled market
            {"name": "Brussels_BE", "lat": 50.8503, "lon": 4.3517, "population": 1222637},
            {"name": "Antwerp_BE", "lat": 51.2194, "lon": 4.4025, "population": 530630},
        ]

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
        googleweather_collector = GoogleWeatherCollector(
            api_key=google_weather_api_key,
            locations=all_weather_locations,  # Strategic onshore locations only
            hours=240  # 10 days hourly forecast
        )
        tennet_collector = TennetCollector(api_key=tennet_api_key)

        # Open-Meteo Offshore Wind collector (FREE - no API key needed)
        # Uses actual offshore coordinates - Open-Meteo's global models support open-sea locations
        # Unlike Google Weather which returns 404 for offshore coordinates
        openmeteo_offshore_wind_collector = OpenMeteoOffshoreWindCollector(
            locations=offshore_wind_locations,
            forecast_days=10  # 10-day forecast for price prediction
        )

        # ENTSO-E Wind Generation Forecast collector (for price prediction)
        entsoe_wind_collector = EntsoeWindCollector(
            api_key=entsoe_api_key,
            country_codes=['NL', 'DE_LU', 'BE', 'DK_1']  # Key wind markets affecting NL prices
        )

        # NED.nl collector for Dutch energy production (solar, wind onshore/offshore)
        # Only initialize if API key is available (requires registration approval)
        ned_collector = None
        if ned_api_key:
            ned_collector = NedCollector(
                api_key=ned_api_key,
                energy_types=['solar', 'wind_onshore', 'wind_offshore'],
                include_forecast=True,
                include_actual=True
            )

        # Open-Meteo Solar collector for multi-location solar irradiance (FREE - no API key)
        # Solar irradiance affects electricity SUPPLY through solar panel production
        openmeteo_solar_collector = OpenMeteoSolarCollector(
            locations=solar_locations,
            forecast_days=7  # 7-day solar forecast
        )

        # Open-Meteo Weather collector for demand prediction (FREE - no API key)
        # Temperature at population centers affects electricity DEMAND (heating/cooling)
        openmeteo_weather_collector = OpenMeteoWeatherCollector(
            locations=population_centers,
            forecast_days=7  # 7-day weather forecast
        )

        # ENTSO-E Cross-border flows collector (FREE - uses existing API key)
        # Import/export flows directly impact local electricity prices
        entsoe_flows_collector = EntsoeFlowsCollector(api_key=entsoe_api_key)

        # ENTSO-E Load forecast collector (FREE - uses existing API key)
        # Load (demand) is a key driver of electricity prices
        entsoe_load_collector = EntsoeLoadCollector(
            api_key=entsoe_api_key,
            country_codes=['NL', 'DE_LU'],  # NL and Germany
            include_actual=True
        )

        # ENTSO-E Generation collector for French nuclear (FREE - uses existing API key)
        # French nuclear availability significantly impacts European electricity prices
        entsoe_generation_collector = EntsoeGenerationCollector(
            api_key=entsoe_api_key,
            country_codes=['FR'],  # France - largest nuclear fleet in Europe
            generation_types=['nuclear'],
            include_forecast=True,
            include_actual=True
        )

        # Market Proxy collector for carbon and gas prices (requires Alpha Vantage API key)
        # Carbon and gas prices are key drivers of electricity prices
        market_proxy_collector = None
        if alpha_vantage_api_key:
            market_proxy_collector = MarketProxyCollector(
                api_key=alpha_vantage_api_key,
                cache_dir=output_path
            )

        # Collect data from all sources (national/regional for price prediction)
        tasks = [
            entsoe_collector.collect(today, tomorrow, country_code=country_code),
            energy_zero_collector.collect(today, tomorrow),
            epex_collector.collect(today, tomorrow),
            googleweather_collector.collect(today, ten_days_ahead),  # 10-day forecast for price prediction
            elspot_collector.collect(today, tomorrow, country_code=country_code),
            tennet_collector.collect(yesterday, today),  # TenneT data has a delay, use historical data
            entsoe_wind_collector.collect(today, tomorrow),  # Wind generation forecasts
            openmeteo_solar_collector.collect(today, ten_days_ahead),  # Solar irradiance for supply
            openmeteo_weather_collector.collect(today, ten_days_ahead),  # Demand weather (temperature)
            openmeteo_offshore_wind_collector.collect(today, ten_days_ahead),  # Offshore wind at actual locations
            entsoe_flows_collector.collect(yesterday, today),  # Cross-border flows (historical)
            entsoe_load_collector.collect(today, tomorrow),  # Load forecasts
            entsoe_generation_collector.collect(today, tomorrow)  # French nuclear generation
        ]

        # Add NED.nl collection if API key is configured
        if ned_collector:
            tasks.append(ned_collector.collect(today, tomorrow))

        # Add market proxy collection if API key is configured
        if market_proxy_collector:
            tasks.append(market_proxy_collector.collect(today, today))

        results = await asyncio.gather(*tasks)

        # Unpack results - NED.nl and market proxies are optional at the end
        (entsoe_data, energy_zero_data, epex_data, google_weather_data, elspot_data,
         tennet_data, entsoe_wind_data, solar_data, demand_weather_data,
         offshore_wind_data, flows_data, load_data, generation_data) = results[:13]

        # Handle optional collectors (NED.nl and market proxies)
        optional_idx = 13
        ned_data = None
        market_proxy_data = None
        if ned_collector:
            ned_data = results[optional_idx] if len(results) > optional_idx else None
            optional_idx += 1
        if market_proxy_collector:
            market_proxy_data = results[optional_idx] if len(results) > optional_idx else None

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

        # Save Google Weather multi-location data for price prediction (supply/demand factors)
        if google_weather_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_weather_forecast_multi_location.json")
            save_data_file(data=google_weather_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "weather_forecast_multi_location.json"))
            logging.info(f"Saved multi-location weather forecast for {len(all_weather_locations)} strategic onshore locations")

        if tennet_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_grid_imbalance.json")
            save_data_file(data=tennet_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "grid_imbalance.json"))
            logging.info(f"Saved TenneT grid imbalance data with {tennet_data.metadata.get('data_points', 0)} data points")

        # Wind forecast output - combines ENTSO-E generation forecasts with offshore wind weather data
        # This dedicated wind file is optimized for price prediction models
        wind_combined_data = CombinedDataSet()

        # Add ENTSO-E wind generation forecasts (MW by country)
        if entsoe_wind_data:
            wind_combined_data.add_dataset('entsoe_wind_generation', entsoe_wind_data)
            logging.info(f"Added ENTSO-E wind generation forecasts for {len(entsoe_wind_data.data)} countries")

        # Add Open-Meteo offshore wind data (actual offshore coordinates)
        # This provides wind speed at multiple heights (10m, 80m, 120m, 180m) plus air density
        if offshore_wind_data:
            wind_combined_data.add_dataset('offshore_wind', offshore_wind_data)
            logging.info(f"Added Open-Meteo offshore wind data for {len(offshore_wind_data.data)} offshore locations")

        # Save combined wind forecast
        if wind_combined_data.datasets:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_wind_forecast.json")
            save_data_file(data=wind_combined_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "wind_forecast.json"))
            logging.info(f"Saved combined wind forecast with {len(wind_combined_data.datasets)} data sources")

        # Save NED.nl energy production data (solar, wind onshore/offshore - NL actual + forecast)
        if ned_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_ned_production.json")
            save_data_file(data=ned_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "ned_production.json"))
            energy_types = ned_data.metadata.get('energy_types', [])
            logging.info(f"Saved NED.nl production data for {len(energy_types)} energy types: {energy_types}")

        # Save solar irradiance forecast for supply prediction
        if solar_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_solar_forecast.json")
            save_data_file(data=solar_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "solar_forecast.json"))
            logging.info(f"Saved solar irradiance forecast for {len(solar_locations)} locations")

        # Save demand weather forecast for demand prediction (heating/cooling)
        if demand_weather_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_demand_weather_forecast.json")
            save_data_file(data=demand_weather_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "demand_weather_forecast.json"))
            total_pop = sum(loc.get('population', 0) for loc in population_centers)
            logging.info(f"Saved demand weather forecast for {len(population_centers)} population centers ({total_pop:,} total population)")

        # Save cross-border flows data (import/export between NL and neighbors)
        if flows_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_cross_border_flows.json")
            save_data_file(data=flows_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "cross_border_flows.json"))
            summary = flows_data.data.get('summary', {})
            logging.info(f"Saved cross-border flows for {len(summary.get('borders', []))} borders, avg net: {summary.get('avg_net_position', 0)} MW")

        # Save load forecast data (electricity demand predictions)
        if load_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_load_forecast.json")
            save_data_file(data=load_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "load_forecast.json"))
            logging.info(f"Saved load forecast for {len(load_data.data)} countries")

        # Save generation data (French nuclear availability)
        if generation_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_generation_forecast.json")
            save_data_file(data=generation_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "generation_forecast.json"))
            logging.info(f"Saved generation data for {len(generation_data.data)} countries (nuclear availability)")

        # Generate calendar features for the forecast period
        # Calendar features affect electricity demand (holidays, weekends, season)
        calendar_data = get_calendar_features_for_range(today, ten_days_ahead, hourly=True)
        upcoming_holidays = get_upcoming_holidays(days_ahead=30)

        calendar_dataset = EnhancedDataSet(
            metadata={
                'data_type': 'calendar_features',
                'source': 'Python holidays library',
                'description': 'Calendar features affecting electricity demand patterns',
                'countries': ['NL', 'DE', 'BE', 'FR'],
                'upcoming_holidays': upcoming_holidays,
                'start_time': today.isoformat(),
                'end_time': ten_days_ahead.isoformat(),
            },
            data=calendar_data
        )

        full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_calendar_features.json")
        save_data_file(data=calendar_dataset, file_path=full_path, handler=handler, encrypt=encryption)
        shutil.copy(full_path, os.path.join(output_path, "calendar_features.json"))
        logging.info(f"Saved calendar features for {len(calendar_data)} hours, {len(upcoming_holidays)} upcoming holidays")

        # Save market proxy data (carbon and gas prices)
        if market_proxy_data:
            full_path = os.path.join(output_path, f"{datetime.now().strftime('%y%m%d_%H%M%S')}_market_proxies.json")
            save_data_file(data=market_proxy_data, file_path=full_path, handler=handler, encrypt=encryption)
            shutil.copy(full_path, os.path.join(output_path, "market_proxies.json"))
            carbon_price = market_proxy_data.data.get('carbon', {}).get('price', 'N/A')
            gas_price = market_proxy_data.data.get('gas', {}).get('price', 'N/A')
            logging.info(f"Saved market proxies: carbon=${carbon_price}, gas=${gas_price}")

    except Exception as e:
        logging.error(e)

if __name__ == "__main__":
    # Set appropriate event loop policy for Windows
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())        

    asyncio.run(main())