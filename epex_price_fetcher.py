import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta, timezone

async def get_Epex_data(start_time: datetime, end_time: datetime) -> dict:
    """
    Retrieves Epex energy price data for a specified time range.

    Args:
        start_time (datetime): The start of the time range. 
        end_time (datetime): The end of the time range. 

    Returns:
        dict: A dictionary containing the day-ahead energy price data [EUR/MWh].
              Keys are ISO-formatted timestamps in UTC, values are market prices.
    """
    base_url = 'https://api.awattar.at/v1/marketdata'

    if start_time is None:
        raise ValueError("Start time must be provided")
    if end_time is None:
        raise ValueError("End time must be provided")

    # # Convert to pandas Timestamp for consistent handling
    # start_timestamp = pd.Timestamp(start_time).tz_convert('UTC')
    # end_timestamp = pd.Timestamp(end_time).tz_convert('UTC')
    # params = {
    #     'start': int(start_timestamp.timestamp() * 1000),
    #     'end': int(end_timestamp.timestamp() * 1000)
    # }

    # Ensure start and end times are in the specified timezone
    tz = start_time.tzinfo or timezone.utc
    start_time = start_time.astimezone(tz)
    end_time = end_time.astimezone(tz)

    params = {
        'start': int(start_time.timestamp() * 1000),
        'end': int(end_time.timestamp() * 1000)
    }

    processed_data = {}

    async with aiohttp.ClientSession() as session:
        url = f"{base_url}?start={params['start']}&end={params['end']}"
        async with session.get(url) as response:
            if response.status != 200:
                logging.error(f"Error: Unable to fetch data. Status code: {response.status}")
                return processed_data

            data = await response.json()
            for item in data['data']:
                item_time = datetime.fromtimestamp(item['start_timestamp'] / 1000, tz=tz)
                processed_data[item_time.isoformat()] = item['marketprice']
    return processed_data

# Example usage
async def main():
    import pytz

    cest = pytz.timezone('Europe/Amsterdam')
    
    current_time = datetime.now(cest)
    tomorrow = current_time + timedelta(days=1)
    tomorrow_midnight = tomorrow.replace(hour=23, minute=59, second=59, microsecond=999999)

    spot_data = await get_Epex_data(start_time=current_time, end_time=tomorrow_midnight)
    print("\nFirst 5 data points:")
    for timestamp, price in list(spot_data.items())[:5]:
        print(f"Timestamp: {timestamp}, Price: {price} EUR/MWh")

if __name__ == "__main__":
    asyncio.run(main())

