"""
Internet data acquisition for energy applications
--------------------------------------------------
Part of the Energy Data Integration Project at HAN University of Applied Sciences.

File: luchtmeetnet_data_fetcher.py
Created: 2024-11-23
Updated: 2024-12-19

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
    Client for retrieving air quality data from the Dutch National Air Quality
    Monitoring Network (Luchtmeetnet). Provides historical and current air quality
    measurements from the nearest monitoring station, including AQI, NO2, PM10,
    and other pollutant levels.

Dependencies:
    - aiohttp: Async HTTP client
    Required local packages:
    - utils.helpers: Distance calculation and data handling
    - utils.timezone_helpers: Timezone handling utilities
    - utils.data_types: Standardized data structures

Usage:
    async def main():
        data = await get_luchtmeetnet_data(latitude, longitude, start_time, end_time)

Notes:
    - Automatically selects nearest monitoring station
    - All measurements in µg/m³
    - Includes Air Quality Index (AQI) calculations
    - Supports historical data retrieval
    - Implements error handling and data validation
"""
import asyncio
import logging
import aiohttp
from datetime import datetime, timedelta
from utils.timezone_helpers import ensure_timezone, compare_timezones
from utils.helpers import closest
from utils.data_types import EnhancedDataSet

async def get_luchtmeetnet_data(latitude: float, longitude: float, start_time: datetime, end_time: datetime) -> EnhancedDataSet:
    """
    Retrieves air quality data from Luchtmeetnet for a specified location, component, and time range.
    See https://www.luchtmeetnet.nl/informatie/luchtkwaliteit/klassen-luchtkwaliteit

    Args:
        latitude (float): The latitude of the location (-90; 90).
        longitude (float): The longitude of the location (-180; 180).
        start_time (datetime): The start of the time range.
        end_time (datetime): The end of the time range.

    Returns:
        EnhancedDataSet: An EnhancedDataSet containing Luchtmeetnet data.
    """
    base_url = 'https://api.luchtmeetnet.nl/open_api'

    if start_time is None:
        raise ValueError("Start time must be provided")
    if end_time is None:
        raise ValueError("End time must be provided")
    
    start_time, end_time, tz = ensure_timezone(start_time, end_time)

    match, message = compare_timezones(start_time, latitude, longitude)
    if not match:
        logging.warning(f"Timezone mismatch: {message}")

    logging.info(f"Querying Luchtmeetnet server from {start_time} to {end_time}")

    try:
        async with aiohttp.ClientSession() as session:
            logging.info(f"Fetching Luchtmeetnet station list")
            url = f"{base_url}/stations?page=1&order_by=number&organisation_id="
            station_list = []
            async with session.get(url) as response:
                if response.status != 200:
                    logging.error(f"Unable to fetch data. Status code: {response.status}")
                    return None
                response_data = await response.json()
                page_list = list(response_data['pagination']['page_list'])
                
            for page in page_list:
                url = f"{base_url}/stations?page={page}&order_by=number&organisation_id="
                async with session.get(url) as response:
                    if response.status != 200:
                        logging.error(f"Unable to fetch data. Status code: {response.status}")
                        return None
                    response_data = await response.json()
                    station_list.extend(response_data['data'])

            logging.info(f"Fetching Luchtmeetnet station data, pls have patience")
            for station in station_list:
                url = f"{base_url}/stations/{station['number']}/"
                async with session.get(url) as response:
                    if response.status != 200:
                        logging.error(f"Unable to fetch data. Status code: {response.status}")
                        return None
                    response_data = await response.json()
                    if response_data['data']['geometry']['type'] == 'point' and response_data['data']['geometry']['coordinates']:
                        station['latitude'] = response_data['data']['geometry']['coordinates'][1]
                        station['longitude'] = response_data['data']['geometry']['coordinates'][0]
                        station['components'] = response_data['data']['components']
                        station['location'] = response_data['data']['location']
                        station['municipality'] = response_data['data']['municipality']

            logging.info(f"Finding station closest to {latitude}, {longitude}")
            closest_station = closest(station_list, {"latitude": latitude, "longitude": longitude})
          
            logging.info(f"Fetching air quality indicator for station {closest_station['number']}, {closest_station['location']}")
            url = f"{base_url}/lki?station_number={closest_station['number']}&order_by=timestamp_measured&order_direction=desc"
            data = {}
            async with session.get(url) as response:
                if response.status != 200:
                    logging.error(f"Unable to fetch data. Status code: {response.status}")
                    return None
                response_data = await response.json()
                for item in response_data['data']:
                    aware_item_time = datetime.strptime(item.pop('timestamp_measured'), '%Y-%m-%dT%H:%M:%S%z')
                    localized_item_time = aware_item_time.astimezone(tz) # correct timezone
                    if localized_item_time >= start_time and localized_item_time <= end_time:
                        timestamp_key = localized_item_time.isoformat()
                        data.setdefault(timestamp_key, {})['AQI'] = item['value']
           
            logging.info(f"Fetching measurements for station {closest_station['number']}, {closest_station['location']}")
            url = f"{base_url}/stations/{closest_station['number']}/measurements?order_by=timestamp_measured&order_direction=desc"
            async with session.get(url) as response:
                if response.status != 200:
                    logging.error(f"Unable to fetch data. Status code: {response.status}")
                    return None
                response_data = await response.json()
                for item in response_data['data']:
                    aware_item_time = datetime.strptime(item.pop('timestamp_measured'), '%Y-%m-%dT%H:%M:%S%z')
                    localized_item_time = aware_item_time.astimezone(tz) # correct timezone
                    if localized_item_time >= start_time and localized_item_time <= end_time:
                        timestamp_key = localized_item_time.isoformat()
                        data.setdefault(timestamp_key, {})[item['formula']] = item['value']
            dataset = EnhancedDataSet(
                metadata = {        
                    'data_type': 'air',
                    'source': 'Luchtmeetnet API',
                    'city': closest_station['municipality'],
                    'station': closest_station['number'],
                    'latitude': closest_station['latitude'],
                    'longitude': closest_station['longitude'],
                    'components': closest_station['components'],
                    'units': {
                        'all': 'µg/m³'
                    },
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat()},
                data = data
            )

            if dataset.data:
                now_hour = list(dataset['data'].keys())[0]
                logging.info(f"Luchtmeetnet data from {start_time} to {end_time}\n"
                            f"Current: {dataset['data'][now_hour]}\n")
            else:
                logging.warning(f"No data retrieved for the specified time range: {start_time} to {end_time}")
            return dataset 
    except Exception as e:
        logging.error(f"Error fetching luchtmeetnet data: {e}") 
    return None

# Example usage
async def main():
    import os
    from configparser import ConfigParser
    from utils.timezone_helpers import get_timezone_and_country

    logging.basicConfig(level=logging.INFO)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, 'secrets.ini')

    configur = ConfigParser() 
    configur.read(secrets_file)
    latitude = float(configur.get('location', 'latitude'))
    longitude = float(configur.get('location', 'longitude'))
    tz, _ = get_timezone_and_country(latitude, longitude)

    end_time = datetime.now(tz)
    start_time = end_time - timedelta(hours=24)

    luchtmeetnet_data = await get_luchtmeetnet_data(latitude, longitude, start_time, end_time)
    print(luchtmeetnet_data['metadata'])
    print("\nLast 5 data points:")
    for timestamp, value in list(luchtmeetnet_data['data'].items())[:5]:
        print(f"Timestamp: {timestamp}, Value: {value} µg/m³")

if __name__ == "__main__":
    asyncio.run(main())