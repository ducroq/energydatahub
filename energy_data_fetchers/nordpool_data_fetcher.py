"""
Internet data acquisition for energy applications
--------------------------------------------------
Part of the Energy Data Integration Project at HAN University of Applied Sciences.

File: nordpool_data_fetcher.py
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
    Client for retrieving energy price data from the Nord Pool Elspot market.
    Handles day-ahead price data from the Nordic and Baltic power exchange,
    providing context for Dutch energy prices through cross-market comparison.

Dependencies:
    - nordpool: Nord Pool API client
    Required local packages:
    - utils.data_types: For standardized data structures
    - utils.timezone_helpers: Timezone handling utilities

Usage:
    async def main():
        data = await get_Elspot_data(country_code, start_time, end_time)

Notes:
    - Returns prices in EUR/MWh
    - Focuses on day-ahead market prices
    - Implements error handling and logging
    - All timestamps are timezone-aware and standardized
    - Provides broader market context through Nordic/Baltic price data
"""
import asyncio
from datetime import datetime, timedelta
import logging
from nordpool import elspot
from functools import partial
from utils.timezone_helpers import ensure_timezone, localize_naive_datetime, normalize_timestamp_to_amsterdam
from utils.data_types import EnhancedDataSet
import platform

async def get_Elspot_data(country_code: str, start_time: datetime, end_time: datetime) -> EnhancedDataSet:
    """
    Retrieves Elspot price data from Nordpool for a specified country and time range.

    Args:
        country_code (str): The country code for which to fetch prices.
        start_time (datetime): The start of the time range.
        end_time (datetime): The end of the time range.

    Returns:
        EnhancedDataSet: An EnhancedDataSet containing the Elspot data.
    """
    try:
        if start_time is None:
            raise ValueError("Start time must be provided")
        if end_time is None:
            raise ValueError("End time must be provided")
        
        start_time, end_time, timezone = ensure_timezone(start_time, end_time)

        logging.info(f"Querying Nordpool API for {country_code} from {start_time} to {end_time}")

        prices_spot = elspot.Prices()

        loop = asyncio.get_running_loop()
        fetch_func = partial(prices_spot.hourly, areas=[country_code], end_date=end_time.date())
        prices_data = await loop.run_in_executor(None, fetch_func)

        data = {}
        for day_data in prices_data['areas'][country_code]['values']:
            # The Nord Pool API returns naive datetime objects (no timezone info)
            # We need to properly localize them to Europe/Amsterdam timezone
            # Using replace(tzinfo=...) is WRONG - it doesn't convert, just replaces
            # This was causing malformed timezone offsets like +00:09
            naive_timestamp = day_data['start']

            # Properly localize the naive datetime to Amsterdam timezone
            if naive_timestamp.tzinfo is None:
                timestamp = localize_naive_datetime(naive_timestamp, timezone)
            else:
                # If API somehow returns timezone-aware datetime, normalize it
                timestamp = normalize_timestamp_to_amsterdam(naive_timestamp)

            if start_time <= timestamp < end_time:
                # Ensure final timestamp is in Amsterdam timezone with correct offset
                local_timestamp = normalize_timestamp_to_amsterdam(timestamp)
                data[local_timestamp.isoformat()] = day_data['value']

        dataset = EnhancedDataSet(
            metadata={
                'data_type': 'energy_price',
                'source': 'Nordpool API',
                'country_code': 'NL',
                'units': 'EUR/MWh',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()},        
            data = data
        )

        if dataset.data:
            data_keys = list(dataset['data'].keys())
            if len(data_keys) >= 2:
                now_hour = data_keys[0]
                next_hour = data_keys[1]
                logging.info(f"Elspot day ahead price from: {start_time} to {end_time}\n"
                            f"Current: {dataset['data'][now_hour]} EUR/MWh @ {now_hour}\n"
                            f"Next hour: {dataset['data'][next_hour]} EUR/MWh @ {next_hour}")
            else:
                logging.info(f"Elspot data retrieved: {len(data_keys)} data points from {start_time} to {end_time}")
        else:
            logging.warning(f"No data retrieved for the specified time range: {start_time} to {end_time}")   

        return dataset

    except Exception as e:
        logging.error(f"Error retrieving Nordpool data: {e}")
        return None

# Example usage
async def main():
    import pytz

    logging.basicConfig(level=logging.INFO)

    cest = pytz.timezone('Europe/Amsterdam')
    
    current_time = datetime.now(cest)    
    tomorrow_midnight = (current_time + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

    nordpool_data = await get_Elspot_data(country_code='NL', start_time=current_time, end_time=tomorrow_midnight)

    print(f"Total data points: {len(nordpool_data.data)}")
    print("\nFirst 5 data points:")
    for timestamp, price in list(nordpool_data.data.items())[:5]:
        print(f"Timestamp: {timestamp}, Price: {price} EUR/MWh")        

if __name__ == "__main__":
    # Set appropriate event loop policy for Windows
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())        

    asyncio.run(main())