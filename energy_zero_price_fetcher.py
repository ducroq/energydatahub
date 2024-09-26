import asyncio
from datetime import datetime, timedelta
import logging
from energyzero import EnergyZero, VatOption
from timezone_helpers import ensure_timezone
from data_types import EnhancedDataSet

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
    asyncio.run(main())