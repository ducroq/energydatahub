import asyncio
from datetime import datetime, timedelta
import logging
from energyzero import EnergyZero, VatOption
from timezone_helpers import ensure_timezone

async def get_Energy_zero_data(start_time: datetime, end_time: datetime) -> dict:
    """
    Retrieves energy price data from EnergyZero API for a specified time range.

    Args:
        start_time (datetime): The start of the time range. 
        end_time (datetime): The end of the time range. 

    Returns:
        dict: A dictionary containing the energy price data [EUR/kWh].
              Keys are ISO-formatted timestamps in UTC, values are electricity prices.
    """
    try:
        if start_time is None:
            raise ValueError("Start time must be provided")
        if end_time is None:
            raise ValueError("End time must be provided")
        
        # Ensure start and end times are in the specified timezone
        start_time, end_time, tz = ensure_timezone(start_time, end_time)

        logging.info(f"Querying EnergyZero API from {start_time} to {end_time}")

        async with EnergyZero(vat=VatOption.INCLUDE) as client:
            electricity = await client.energy_prices(start_date=start_time.date(), end_date=end_time.date())
            
            # Convert the electricity data to a dictionary
            data = {}
            for timestamp, price in electricity.prices.items():
                if start_time <= timestamp < end_time:
                    local_timestamp = timestamp.astimezone(tz)
                    data[local_timestamp.isoformat()] = price

            if data:
                now_hour = list(data.keys())[0]
                next_hour = list(data.keys())[1]
                logging.info(f"EnergyZero electricity price from: {start_time} to {end_time}\n"
                             f"Current: {data[now_hour]} EUR/kWh @ {now_hour}\n"
                             f"Next hour: {data[next_hour]} EUR/kWh @ {next_hour}\n"
                             f"Max: {electricity.extreme_prices[1]} EUR/kWh @ {electricity.highest_price_time.isoformat()}\n"
                             f"Min: {electricity.extreme_prices[0]} EUR/kWh @ {electricity.lowest_price_time.isoformat()}\n"
                             f"Average: {electricity.average_price} EUR/kWh")
            else:
                logging.warning(f"No data retrieved for the specified time range: {start_time} to {end_time}")

            return data

    except Exception as e:
        logging.error(f"Error retrieving EnergyZero data: {e}")
        return {}

# Example usage
async def main():
    import pytz

    logging.basicConfig(level=logging.INFO)
    
    cest = pytz.timezone('Europe/Amsterdam')
    
    current_time = datetime.now(cest)
    tomorrow_midnight = (current_time + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

    energy_zero_data = await get_Energy_zero_data(start_time=current_time, end_time=tomorrow_midnight)
    print(f"Total data points: {len(energy_zero_data)}")
    print("\nFirst 5 data points:")
    for timestamp, price in list(energy_zero_data.items())[:5]:
        print(f"Timestamp: {timestamp}, Price: {price} EUR/kWh")

if __name__ == "__main__":
    asyncio.run(main())