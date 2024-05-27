import os
from datetime import datetime, date, timedelta, timezone
import json
import subprocess
import asyncio
import logging
from energyzero import EnergyZero, VatOption
import pytz
from entsoe import EntsoePandasClient
import pandas as pd
from configparser import ConfigParser 

# run this from cron, e.g. hourly, e.g.
# 0 * * * * /homi/pi/EnergyPriceScraper/run_script.sh >> /home/pi/tmp/energyPriceScraper.py.log 2>&1
#
# With a runscript like this:
# #!/bin/bash

# # Path to your virtual environment
# VENV_PATH="/home/pi/energyPriceScraper"

# # Activate the virtual environment
# source "$VENV_PATH/bin/activate"

# # Execute your Python script
# python /home/pi/energyPriceScraper/energyPriceScraper.py

# # Deactivate the virtual environment
# deactivate

OUTPUT_PATH = '' # r'/home/pi/tmp/energyData'
REMOTE_STORAGE_PATH = None # r'gdrive:/data'
LOGGING_FILE_NAME = 'energyPriceScraper.log'

local_timezone = pytz.timezone("CET")

try:
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)        
except OSError as e:
    print(f"Error creating folder: {e}")

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(os.path.join(OUTPUT_PATH, LOGGING_FILE_NAME))]
)

async def get_energy_zero_data() -> dict:
    """
    Retrieves energy price data from EnergyZero API.

    Returns:
        dict: A dictionary containing the energy price data [EUR/kWh].
    """
    async with EnergyZero(vat=VatOption.INCLUDE) as client:
        try:
            today = date.today()
            tomorrow = date.today() + timedelta(days=1)
   
            electricity = await client.energy_prices(start_date=today, end_date=tomorrow)
            next_hour = electricity.utcnow() + timedelta(hours=1)
            gas = await client.gas_prices(start_date=today, end_date=tomorrow)

            logging.info(f"Energy zero electricity price, "
                         f"Current: {electricity.current_price} EUR/kWh @ {electricity.utcnow().astimezone(local_timezone)}, " 
                         f"Next hour: {electricity.price_at_time(next_hour)} EUR/kWh, "  
                         f"Best hours: {electricity.hours_priced_equal_or_lower}, " 
                         f"Max: {electricity.extreme_prices[1]} EUR/kWh @ {electricity.highest_price_time.astimezone(local_timezone)}, " 
                         f"Min: {electricity.extreme_prices[0]} EUR/kWh @ {electricity.lowest_price_time.astimezone(local_timezone)}, " 
                         f"Average: {electricity.average_price} EUR/kWh, " 
                         f"Percentage: {electricity.pct_of_max_price}%")
            
            return electricity
            
        except Exception as e:
            logging.error(f"Error retrieving EnergyZero data: {e}")     
            return None

async def get_Entsoe_data() -> dict:
    """
    Retrieves day-ahead energy price data from Entsoe API.

    Returns:
        dict: A dictionary containing the day-ahead energy price data [EUR/MWh].
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, 'secrets.ini')
    configur = ConfigParser() 
    configur.read(secrets_file)
    my_api_key = configur.get('api_keys', 'entsoe')
    country_code = 'NL'
    try:
        client = EntsoePandasClient(api_key=my_api_key)

        current_time = datetime.now()
        start_timestamp = pd.Timestamp(current_time, tz='Europe/Amsterdam')
        current_start_timestamp = start_timestamp.replace(year=current_time.year, month=current_time.month, day=current_time.day, hour=current_time.hour, minute=0, second=0, microsecond=0)
        tomorrow = current_time  + timedelta(days = 1)
        tomorrow_midnight = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
        end_timestamp = pd.Timestamp(tomorrow_midnight, tz='Europe/Amsterdam')
        ts = client.query_day_ahead_prices(country_code, start=current_start_timestamp, end=end_timestamp)
        data_dict = ts.to_dict() # { ts.inde for ts.index in ts.values}
        formatted_keys = [t.strftime('%Y-%m-%dT%H:%M:%S+02:00') for t in data_dict.keys()]
        data = dict(zip(formatted_keys, data_dict.values()))

        # ts = client.query_wind_and_solar_forecast(country_code, start=today, end=tomorrow)
        # logging.info(f"Entsoe wind and solar forecast: {ts}")

        # Other interesting data from Entsoe API:
        # query_aggregated_bids, query_load, query_load_forecast, query_wind_and_solar_generation_forecast,
        # query_activated_balancing_energy_prices, query_imbalance_prices, query_imbalance_volumes,
        # query_procured_balancing_capacity, query_activated_balancing_energy

        now_hour = list(data.keys())[0]
        next_hour = list(data.keys())[1]
        
        logging.info(f"Entsoe day ahead price from: {start_timestamp} to {end_timestamp}\n"
                     f"Current: {data[now_hour]} EUR/MWh @ {now_hour}\n" 
                     f"Next hour: {data[next_hour]} EUR/MWh @ {next_hour}")

        return data

    except Exception as e:
        logging.error(f"Error retrieving Entsoe data: {e}")     
        return None


if __name__ == "__main__":
    energy_zero_data = asyncio.run(get_energy_zero_data())
    energy_zero_data = {key.astimezone(local_timezone).isoformat(): value for key, value in energy_zero_data.prices.items()}
    current_time = datetime.now(local_timezone)
    current_hour_start = current_time.replace(minute=0, second=0, microsecond=0)
    energy_zero_data = {key: value for key, value in energy_zero_data.items() if datetime.fromisoformat(key) >= current_hour_start}

    entsoe_data = asyncio.run(get_Entsoe_data())

    json_file_name = os.path.join(OUTPUT_PATH, f"data_{datetime.now().strftime('%y%m%d_%H%M%S')}{local_timezone}.json")
    json_data = {'energy zero': energy_zero_data}
    json_data['entsoe'] = entsoe_data

    with open(json_file_name, 'w', encoding='utf-8') as fp:
        json.dump(json_data, fp, indent=4, sort_keys=True, default=str)

    if REMOTE_STORAGE_PATH is not None and REMOTE_STORAGE_PATH is not None:
        try:
            subprocess.run(['rclone', 'copy', OUTPUT_PATH, REMOTE_STORAGE_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(str(e))
