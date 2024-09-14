import asyncio
from datetime import datetime, timedelta, timezone
import logging
from nordpool import elspot
from functools import partial

async def get_Elspot_data(country_code: str, start_time: datetime, end_time: datetime) -> dict:
    """
    Retrieves Elspot price data from Nordpool for a specified country and time range.

    Args:
        country_code (str): The country code for which to fetch prices.
        start_time (datetime): The start of the time range.
        end_time (datetime): The end of the time range.

    Returns:
        dict: A dictionary containing the Elspot price data [EUR/MWh].
              Keys are ISO-formatted timestamps, values are electricity prices.
    """
    try:
        if start_time is None:
            raise ValueError("Start time must be provided")
        if end_time is None:
            raise ValueError("End time must be provided")
        
        # Ensure start and end times are in the specified timezone
        tz = start_time.tzinfo or timezone.utc
        start_time = start_time.astimezone(tz)
        end_time = end_time.astimezone(tz)

        logging.info(f"Querying Nordpool API for {country_code} from {start_time} to {end_time}")

        # Initialize Elspot prices fetcher
        prices_spot = elspot.Prices()

        # Fetch Elspot prices
        loop = asyncio.get_running_loop()
        fetch_func = partial(prices_spot.hourly, areas=[country_code], end_date=end_time.date())
        prices_data = await loop.run_in_executor(None, fetch_func)

        # Process the data
        data = {}
        for day_data in prices_data['areas'][country_code]['values']:
            timestamp = day_data['start'].replace(tzinfo=tz) # bug in api client?
            if start_time <= timestamp < end_time:
                local_timestamp = timestamp.astimezone(tz)
                data[local_timestamp.isoformat()] = day_data['value']

        if data:
            now_hour = list(data.keys())[0]
            next_hour = list(data.keys())[1]
            logging.info(f"Nordpool Elspot price for {country_code} from: {start_time} to {end_time}\n"
                         f"Current: {data[now_hour]} EUR/MWh @ {now_hour}\n"
                         f"Next hour: {data[next_hour]} EUR/MWh @ {next_hour}")
        else:
            logging.warning(f"No data retrieved for the specified time range: {start_time} to {end_time}")

        return data

    except Exception as e:
        logging.error(f"Error retrieving Nordpool data: {e}")
        logging.error(f"Error details: country_code={country_code}, start_time={start_time}, end_time={end_time}")
        return {}

# Example usage
async def main():
    import pytz

    logging.basicConfig(level=logging.INFO)

    cest = pytz.timezone('Europe/Amsterdam')
    
    current_time = datetime.now(cest)    
    tomorrow_midnight = (current_time + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

    nordpool_data = await get_Elspot_data(country_code='NL', start_time=current_time, end_time=tomorrow_midnight)
    print(f"Total data points: {len(nordpool_data)}")
    print("\nFirst 5 data points:")
    for timestamp, price in list(nordpool_data.items())[:5]:
        print(f"Timestamp: {timestamp}, Price: {price} EUR/MWh")

if __name__ == "__main__":
    asyncio.run(main())