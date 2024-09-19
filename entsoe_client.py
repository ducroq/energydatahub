import asyncio
from datetime import datetime, timedelta
import pandas as pd
from entsoe import EntsoePandasClient
import logging
from functools import partial

async def get_Entsoe_data(api_key: str, country_code: str, start_time: datetime, end_time: datetime) -> dict:
    """
    Retrieves day-ahead energy price data from Entsoe API for a specified time range.

    Args:
        api_key (str): The Entsoe API key.
        country_code (str): The country code for which to retrieve data.
        start_time (datetime): The start of the time range.
        end_time (datetime): The end of the time range.

    Returns:
        dict: A dictionary containing the day-ahead energy price data [EUR/MWh].
              Keys are ISO-formatted timestamps in UTC, values are market prices.
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

        # This would be the synchronous way to query the data
        # ts = client.query_day_ahead_prices(country_code, start=start_timestamp, end=end_timestamp)

        # Use partial to create a function with keyword arguments
        query_func = partial(client.query_day_ahead_prices, 
                             country_code=country_code, 
                             start=start_timestamp, 
                             end=end_timestamp)        

        # EntsoePandasClient is not async, so we run it in a separate thread
        loop = asyncio.get_running_loop()
        ts = await loop.run_in_executor(None, query_func)        

        # Convert the pandas Series to a dictionary
        data = {t.isoformat(): price for t, price in ts.items()}

        # Log some information about the retrieved data
        now_hour = list(data.keys())[0]
        next_hour = list(data.keys())[1]
        logging.info(f"Entsoe day ahead price from: {start_timestamp} to {end_timestamp}\n"
                     f"Current: {data[now_hour]} EUR/MWh @ {now_hour}\n" 
                     f"Next hour: {data[next_hour]} EUR/MWh @ {next_hour}")

        return data

    except Exception as e:
        logging.error(f"Error retrieving Entsoe data: {e}")     
        return {}

# Example usage
async def main():
    import os
    from configparser import ConfigParser
    import pytz

    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, 'secrets.ini')
# get the api keys and location from the secrets.ini file
    configur = ConfigParser() 
    configur.read(secrets_file)
    entsoe_api_key = configur.get('api_keys', 'entsoe')   

    cest = pytz.timezone('Europe/Amsterdam')
    country_code = "NL"
    
    current_time = datetime.now(cest)
    tomorrow_midnight = (current_time + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

    entsoe_data = await get_Entsoe_data(entsoe_api_key, country_code, start_time=current_time, end_time=tomorrow_midnight)
    print(f"Total data points: {len(entsoe_data)}")
    print("\nFirst 5 data points:")
    for timestamp, price in list(entsoe_data.items())[:5]:
        print(f"Timestamp (UTC): {timestamp}, Price: {price} EUR/MWh")

if __name__ == "__main__":
    asyncio.run(main())