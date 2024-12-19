"""
Internet data acquisition for energy applications
--------------------------------------------------
Part of the Energy Data Integration Project at HAN University of Applied Sciences.

File: entsoe_client.py
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
    Client for retrieving day-ahead energy price data from the ENTSO-E Transparency
    Platform API. Handles data fetching, validation, and conversion into the project's
    standardized format. Supports asynchronous operations and timezone-aware timestamps.

Dependencies:
    - pandas: Data handling and timestamp management
    - entsoe-py: ENTSO-E API client
    Required local packages:
    - utils.data_types: For standardized data structures
    - utils.timezone_helpers: Timezone handling utilities

Usage:
    async def main():
        data = await get_Entsoe_data(api_key, country_code, start_time, end_time)

Notes:
    - Requires valid ENTSO-E API key in secrets.ini
    - Returns prices in EUR/MWh
    - Implements error handling and logging
    - All timestamps are handled in UTC and converted as needed
"""
import asyncio
from datetime import datetime, timedelta
import pandas as pd
from entsoe import EntsoePandasClient
import logging
from functools import partial
from utils.data_types import EnhancedDataSet

async def get_Entsoe_data(api_key: str, country_code: str, start_time: datetime, end_time: datetime) -> EnhancedDataSet:
    """
    Retrieves day-ahead energy price data from Entsoe API for a specified time range.

    Args:
        api_key (str): The Entsoe API key.
        country_code (str): The country code for which to retrieve data.
        start_time (datetime): The start of the time range.
        end_time (datetime): The end of the time range.

    Returns:
        EnhancedDataSet: An EnhancedDataSet containing the Entsoe data.
    """
    try:
        if start_time is None:
            raise ValueError("Start time must be provided")
        if end_time is None:
            raise ValueError("End time must be provided")
        
        # Convert to pandas Timestamp for consistent handling
        start_timestamp = pd.Timestamp(start_time).tz_convert('UTC')
        end_timestamp = pd.Timestamp(end_time).tz_convert('UTC')

        logging.info(f"Querying Entsoe API for {country_code} from {start_timestamp} to {end_timestamp}")

        client = EntsoePandasClient(api_key=api_key)

        # Use partial to create a function with keyword arguments
        query_func = partial(client.query_day_ahead_prices, 
                             country_code=country_code, 
                             start=start_timestamp, 
                             end=end_timestamp)        

        # EntsoePandasClient is not async, so we run it in a separate thread
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, query_func)

        dataset = EnhancedDataSet(
            metadata={
                'data_type': 'energy_price',
                'source': 'ENTSO-E Transparency Platform API v1.3',                  
                'country_code': 'NL',
                'units': 'EUR/MWh',
                'start_time': start_timestamp.isoformat(),
                'end_time': end_timestamp.isoformat()},
            data = {timestamp.isoformat(): price for timestamp, price in data.items() if start_time <= timestamp < end_time}
        )

        if dataset.data:
            now_hour = list(dataset['data'].keys())[0]
            next_hour = list(dataset['data'].keys())[1]
            logging.info(f"Entsoe day ahead price from: {start_timestamp} to {end_timestamp}\n"
                         f"Current: {dataset['data'][now_hour]} EUR/MWh @ now_hour\n" 
                         f"Next hour: {dataset['data'][next_hour]} EUR/MWh @ next_hour")
        else:
            logging.warning(f"No data retrieved for the specified time range: {start_time} to {end_time}")            

        return dataset

    except Exception as e:
        logging.error(f"Error retrieving Entsoe data: {e}")     
        return None

# Example usage
async def main():
    import os
    from configparser import ConfigParser
    import pytz

    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, 'secrets.ini')

    configur = ConfigParser() 
    configur.read(secrets_file)
    entsoe_api_key = configur.get('api_keys', 'entsoe')   

    cest = pytz.timezone('Europe/Amsterdam')
    country_code = "NL"
    
    current_time = datetime.now(cest)
    tomorrow_midnight = (current_time + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

    entsoe_data = await get_Entsoe_data(entsoe_api_key, country_code, start_time=current_time, end_time=tomorrow_midnight)

    # print(entsoe_data.to_dict())
    print(f"Total data points: {len(entsoe_data.data)}")
    print("\nFirst 5 data points:")
    for timestamp, price in list(entsoe_data.data.items())[:5]:
        print(f"Timestamp (UTC): {timestamp}, Price: {price} EUR/MWh")

if __name__ == "__main__":
    asyncio.run(main())