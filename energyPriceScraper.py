import os
from datetime import datetime, date, timedelta, timezone
import json
import subprocess
import asyncio
import logging
from energyzero import EnergyZero, VatOption
import pytz

# run this from cron, e.g. hourly, e.g.
#  0 0 * * * /usr/local/bin/python3.12 /home/pi/energyPriceDataHub/energyPriceScraper.py >> /home/pi/tmp/energyPriceScraper.py.log 2>&1

OUTPUT_PATH = '' # r'/home/pi/tmp'
REMOTE_STORAGE_PATH = None # r'gdrive:/data'
LOGGING_FILE_NAME = 'energyPriceScraper.log'

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(os.path.join(OUTPUT_PATH, LOGGING_FILE_NAME))]
)

local_timezone = pytz.timezone("CET")

async def get_energy_zero_today() -> dict:
    async with EnergyZero(vat=VatOption.INCLUDE) as client:
        try:
            today = date.today()

            energy_today = await client.energy_prices(start_date=today, end_date=today)
            next_hour = energy_today.utcnow() + timedelta(hours=1)
            gas_today = await client.gas_prices(start_date=today, end_date=today)

            logging.info(f"Energy zero electricity price, "
                         f"Current: EUR {energy_today.current_price} @ {energy_today.utcnow().astimezone(local_timezone)}, " 
                         f"Next hour: EUR {energy_today.price_at_time(next_hour)}, "  
                         f"Best hours: {energy_today.hours_priced_equal_or_lower}, " 
                         f"Max today: EUR {energy_today.extreme_prices[1]} @ {energy_today.highest_price_time.astimezone(local_timezone)}, " 
                         f"Min today: EUR {energy_today.extreme_prices[0]} @ {energy_today.lowest_price_time.astimezone(local_timezone)}, " 
                         f"Average today: EUR {energy_today.average_price}, " 
                         f"Percentage: {energy_today.pct_of_max_price}%")
            
            return energy_today

        except Exception as e:
            logging.error(f"Error retrieving EnergyZero data: {e}")     
            return None  


if __name__ == "__main__":
    energy_zero_data = asyncio.run(get_energy_zero_today())

    json_file_name = os.path.join(OUTPUT_PATH, f"rss_{datetime.now().strftime(f"%y%m%d_%H%M%S{local_timezone}")}.json")
    json_prices = {key.astimezone(local_timezone).isoformat(): value for key, value in energy_zero_data.prices.items()}
    json_energy_zero = {'energy zero':json_prices}

    with open(json_file_name, 'w', encoding='utf-8') as fp:
        json.dump(json_energy_zero, fp, indent=4, sort_keys=True, default=str)

    if REMOTE_STORAGE_PATH is not None and REMOTE_STORAGE_PATH is not None:
        try:
            subprocess.run(['rclone', 'copy', OUTPUT_PATH, REMOTE_STORAGE_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(str(e))
