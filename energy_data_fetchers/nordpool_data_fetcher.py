import asyncio
from datetime import datetime, timedelta
import logging
from nordpool import elspot
from functools import partial
from core.timezone_helpers import ensure_timezone
from core.data_types import EnhancedDataSet
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
            timestamp = day_data['start'].replace(tzinfo=timezone) # bug in api client?
            if start_time <= timestamp < end_time:
                local_timestamp = timestamp.astimezone(timezone)
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
            now_hour = list(dataset['data'].keys())[0]
            next_hour = list(dataset['data'].keys())[1]
            logging.info(f"EnergyZero day ahead price from: {start_time} to {end_time}\n"
                        f"Current: {dataset['data'][now_hour]} EUR/MWh @ now_hour\n" 
                        f"Next hour: {dataset['data'][next_hour]} EUR/MWh @ next_hour")
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