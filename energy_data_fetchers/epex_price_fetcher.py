import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta
from utils.timezone_helpers import ensure_timezone
from utils.data_types import EnhancedDataSet

async def get_Epex_data(start_time: datetime, end_time: datetime) -> EnhancedDataSet:
    """
    Retrieves Epex energy price data for a specified time range.

    Args:
        start_time (datetime): The start of the time range. 
        end_time (datetime): The end of the time range. 

    Returns:
        EnhancedDataSet: An EnhancedDataSet containing the Epex data.
    """
    base_url = 'https://api.awattar.at/v1/marketdata'

    if start_time is None:
        raise ValueError("Start time must be provided")
    if end_time is None:
        raise ValueError("End time must be provided")

    start_time, end_time, tz = ensure_timezone(start_time, end_time)

    logging.info(f"Querying Epex API from {start_time} to {end_time}")

    params = {
        'start': int(start_time.timestamp() * 1000),
        'end': int(end_time.timestamp() * 1000)
    }

    async with aiohttp.ClientSession() as session:
        url = f"{base_url}?start={params['start']}&end={params['end']}"
        async with session.get(url) as response:
            if response.status != 200:
                logging.error(f"Unable to fetch data. Status code: {response.status}")
                return None

            data = await response.json()            

            dataset = EnhancedDataSet(
                metadata={
                    'data_type': 'energy_price',
                    'source': 'Awattar API',
                    'country_code': 'NL',
                    'units': 'EUR/MWh',
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat()},        
                data = {datetime.fromtimestamp(item['start_timestamp'] / 1000, tz=tz).isoformat(): item['marketprice'] for item in data['data']}
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


# Example usage
async def main():
    import pytz

    cest = pytz.timezone('Europe/Amsterdam')
    
    current_time = datetime.now(cest)
    tomorrow = current_time + timedelta(days=1)
    tomorrow_midnight = tomorrow.replace(hour=23, minute=59, second=59, microsecond=999999)

    spot_data = await get_Epex_data(start_time=current_time, end_time=tomorrow_midnight)

    print(f"Total data points: {len(spot_data.data)}")
    print("\nFirst 5 data points:")
    for timestamp, price in list(spot_data.data.items())[:5]:
        print(f"Timestamp: {timestamp}, Price: {price} EUR/MWh")

if __name__ == "__main__":
    asyncio.run(main())

