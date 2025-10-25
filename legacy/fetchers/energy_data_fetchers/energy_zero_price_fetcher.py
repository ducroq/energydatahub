"""
Internet data acquisition for energy applications
--------------------------------------------------
Part of the Energy Data Integration Project at HAN University of Applied Sciences.

File: energy_zero_price_fetcher.py
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
    Client for retrieving energy price data from the EnergyZero API. Handles real-time
    and day-ahead price data for the Dutch market, including VAT calculations. 
    Implements async operations for efficient data retrieval.

Dependencies:
    - energyzero: EnergyZero API client
    Required local packages:
    - utils.data_types: For standardized data structures
    - utils.timezone_helpers: Timezone handling utilities

Usage:
    async def main():
        data = await get_Energy_zero_data(start_time, end_time)

Notes:
    - Returns prices in EUR/kWh including VAT
    - Handles automatic timezone conversion
    - Implements error handling and logging
    - Provides real-time and day-ahead price information
"""
import asyncio
from datetime import datetime, timedelta
import logging
from energyzero import EnergyZero, VatOption
from utils.timezone_helpers import ensure_timezone
from utils.data_types import EnhancedDataSet
import platform

async def get_Energy_zero_data(start_time: datetime, end_time: datetime) -> EnhancedDataSet:
    """
    Retrieves energy price data from EnergyZero API for a specified time range.

    Args:
        start_time (datetime): The start of the time range. 
        end_time (datetime): The end of the time range. 

    Returns:
        EnhancedDataSet: An EnhancedDataSet containing the EnergyZero data.
    """
    try:
        if start_time is None:
            raise ValueError("Start time must be provided")
        if end_time is None:
            raise ValueError("End time must be provided")
        
        start_time, end_time, timezone = ensure_timezone(start_time, end_time)

        logging.info(f"Querying EnergyZero API from {start_time} to {end_time}")

        async with EnergyZero(vat=VatOption.INCLUDE) as client:
            data = await client.energy_prices(start_date=start_time.date(), end_date=end_time.date())

            dataset = EnhancedDataSet(
                metadata={
                    'data_type': 'energy_price',
                    'source': 'EnergyZero API v2.1',                  
                    'country_code': 'NL',
                    'units': 'EUR/kWh (incl. VAT)',
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat()},        
                data = {timestamp.astimezone(timezone).isoformat(): price for timestamp, price in data.prices.items() if start_time <= timestamp < end_time}
            )

            if dataset.data:
                now_hour = list(dataset['data'].keys())[0]
                next_hour = list(dataset['data'].keys())[1]
                logging.info(f"EnergyZero day ahead price from: {start_time} to {end_time}\n"
                            f"Current: {dataset['data'][now_hour]} EUR/MWh @ now_hour\n" 
                            f"Next hour: {dataset['data'][next_hour]} EUR/MWh @ next_hour")
            else:
                logging.warning(f"No data retrieved for the specified time range: {start_time} to {end_time}")            

            return dataset
    
    except Exception as e:
        logging.error(f"Error retrieving EnergyZero data: {e}")
        return None

# Example usage
async def main():
    import pytz

    logging.basicConfig(level=logging.INFO)
    
    cest = pytz.timezone('Europe/Amsterdam')
    
    current_time = datetime.now(cest)
    tomorrow_midnight = (current_time + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

    energy_zero_data = await get_Energy_zero_data(start_time=current_time, end_time=tomorrow_midnight)
    print(f"Total data points: {len(energy_zero_data.data)}")
    print("\nFirst 5 data points:")
    for timestamp, price in list(energy_zero_data.data.items())[:5]:
        print(f"Timestamp: {timestamp}, Price: {price} EUR/kWh")

if __name__ == "__main__":
    # Set appropriate event loop policy for Windows
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) 

    asyncio.run(main())