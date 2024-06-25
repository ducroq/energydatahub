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
import requests
import meteoserver as meteo
import shutil

# run this from cron, e.g. hourly, e.g.
# 0 * * * * /home/pi/energyDataScraper/run_script.sh >> /home/pi/tmp/energyDataScraper.py.log 2>&1
# or e.g. every 6 hours:
# 0 */6 * * * /home/pi/energyDataScraper/run_script.sh >> /home/pi/tmp/energyDataScraper.py.log 2>&1
# or daily at 6:00:
# 0 6 * * * /home/pi/energyDataScraper/run_script.sh >> /home/pi/tmp/energyDataScraper.py.log 2>&1
# With a runscript like this:
# #!/bin/bash
# VENV_PATH="/home/pi/energyDataScraper"
# source "$VENV_PATH/bin/activate"
# python /home/pi/energyDataScraper/energyDataScraper.py
# deactivate

OUTPUT_PATH = '' # r'/home/pi/tmp/energyData'
REMOTE_STORAGE_PATH = None # r'gdrive:/data'
LOGGING_FILE_NAME = 'energyDataScraper.log'

local_timezone = pytz.timezone("CET")

try:
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)        
except OSError as e:
    print(f"Error creating folder: {e}")

file_list = [f for f in os.listdir(OUTPUT_PATH) if f.endswith(".json")]
for f in file_list:
    os.remove(os.path.join(OUTPUT_PATH, f))

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

async def get_Entsoe_data(api_key:str) -> dict:
    """
    Retrieves day-ahead energy price data from Entsoe API.

    Args:
        api_key (str): The Entsoe API key.

    Returns:
        dict: A dictionary containing the day-ahead energy price data [EUR/MWh].
    """
    # script_dir = os.path.dirname(os.path.abspath(__file__))
    # secrets_file = os.path.join(script_dir, 'secrets.ini')
    # configur = ConfigParser() 
    # configur.read(secrets_file)
    # api_key = configur.get('api_keys', 'entsoe')
    country_code = 'NL'
    try:
        client = EntsoePandasClient(api_key=api_key)

        current_time = datetime.now()
        start_timestamp = pd.Timestamp(current_time, tz='Europe/Amsterdam')
        current_start_timestamp = start_timestamp.replace(year=current_time.year, month=current_time.month, day=current_time.day, hour=current_time.hour, minute=0, second=0, microsecond=0)
        tomorrow = current_time  + timedelta(days = 1)
        tomorrow_midnight = tomorrow.replace(hour=23, minute=0, second=0, microsecond=0)
        end_timestamp = pd.Timestamp(tomorrow_midnight, tz='Europe/Amsterdam')
        ts = client.query_day_ahead_prices(country_code, start=current_start_timestamp, end=end_timestamp)
        data_dict = ts.to_dict() # { ts.inde for ts.index in ts.values}
        formatted_keys = [t.strftime('%Y-%m-%dT%H:%M:%S+02:00') for t in data_dict.keys()]
        data = dict(zip(formatted_keys, data_dict.values()))

        # Other interesting data from Entsoe API:
        # ts = client.query_wind_and_solar_forecast(country_code, start=today, end=tomorrow)
        # logging.info(f"Entsoe wind and solar forecast: {ts}")

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
    
async def get_OpenWeather_data(api_key:str, latitude:str, longitude:str) -> dict:
    """
    Retrieves weather data from the OpenWeather API based on the configured latitude and longitude.

    Args:
        api_key (str): The OpenWeather API key.
        latitude (str): The latitude of the location (-90; 90).
        longitude (str): The longitude of the location (-180; 180).

    Returns:
        A dictionary containing the following weather data:
        - temperature: The current temperature in Kelvin.
        - humidity: The current humidity percentage.
        - pressure: The current atmospheric pressure in hPa.
        - weather_id: The current weather condition ID.
        - weather_description: A description of the current weather conditions.
        - wind_speed: The current wind speed in meters per second.
        - wind_direction: The current wind direction in degrees.
        - visibility: The current visibility in meters.
        - cloudiness: The current cloudiness percentage.
    """
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # print(json.dumps(data, indent=4))
            weather_data = {
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "weather_id": data["weather"][0]["id"],
                "weather_description": data["weather"][0]["description"],
                "wind_speed": data["wind"]["speed"],
                "wind_direction": data["wind"]["deg"],
                "visibility": data["visibility"],
                "cloudiness": data["clouds"]["all"]
            }
            return weather_data
        else:
            Exception(f"Error retrieving OpenWeather data: {response.status_code}")
    except Exception as e:
        logging.error(f"Error retrieving OpenWeather data: {e}")     
        return None
    
async def get_OpenWeather_geographical_coordinates_in_NL(api_key:str, plaats:str) -> dict:
    """
    Retrieves the geographical coordinates (latitude and longitude) of a specified location in the Netherlands
    using the OpenWeather API.

    Args:
        api_key (str): The API key for accessing the OpenWeather API.
        plaats (str): The name of the location in the Netherlands.

    Returns:
        dict: A dictionary containing the latitude and longitude of the specified location.

    Raises:
        Exception: If there is an error retrieving the OpenWeather data.

    """
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={plaats},?,NL&limit=1&appid={api_key}"
    response = requests.get(url)

    try:
        if response.status_code == 200:
            data = response.json()
            # print(json.dumps(data, indent=4))
            latitude = data[0]["lat"]
            longitude = data[0]["lon"]
            logging.info(f"OpenWeather geographical coordinates for {plaats}: {latitude}, {longitude}")
            return {"latitude": latitude, "longitude": longitude}
        else:
            Exception(f"Error retrieving OpenWeather data: {response.status_code}") 
    except Exception as e:
        logging.error(f"Error retrieving OpenWeather data: {e}")     
        return None

# TODO: get more relevant entsoe data
# TODO: add other price sources


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, 'secrets.ini')
# get the api keys and location from the secrets.ini file
    configur = ConfigParser() 
    configur.read(secrets_file)
    entsoe_api_key = configur.get('api_keys', 'entsoe')
    openweather_api_key = configur.get('api_keys', 'openweather')
    meteoserver_api_key = configur.get('api_keys', 'meteo')
    plaats = configur.get('location', 'plaats')
# get the geographical coordinates of the location
    location = asyncio.run(get_OpenWeather_geographical_coordinates_in_NL(api_key=openweather_api_key, plaats=plaats))
    latitude = location['latitude']
    longitude = location['longitude']
# get the energy price data
    energy_zero_data = asyncio.run(get_energy_zero_data())
    energy_zero_data = {key.astimezone(local_timezone).isoformat(): value for key, value in energy_zero_data.prices.items()}
    current_time = datetime.now(local_timezone)
    current_hour_start = current_time.replace(minute=0, second=0, microsecond=0)
    energy_zero_data = {key: value for key, value in energy_zero_data.items() if datetime.fromisoformat(key) >= current_hour_start}
    entsoe_data = asyncio.run(get_Entsoe_data(api_key=entsoe_api_key))
    weather_data = asyncio.run(get_OpenWeather_data(api_key=openweather_api_key, latitude=latitude, longitude=longitude))
# write the data to a json file
    json_file_name = os.path.join(OUTPUT_PATH, f"{datetime.now().strftime('%y%m%d_%H%M%S')}{local_timezone}_energy_price_forecast.json")
    json_data = {}
    json_data['energy zero price forecast'] = energy_zero_data
    json_data['entsoe price forecast'] = entsoe_data
    # json_data['open weather forecast'] = weather_data
    json_data['units'] = {"energy_price": "EUR/kWh",
                          "entsoe_price": "EUR/MWh"}
                        #   "temperature": "K",
                        #   "pressure": "hPa",
                        #   "wind_speed": "m/s"}
    json_data['metadata'] = {"energy_zero_source": "EnergyZero API v2.1",
                             "entsoe_source": "ENTSO-E Transparency Platform API v1.3"}    
                            #  "weather_source": "OpenWeatherMap API v2.5"}
    with open(json_file_name, 'w', encoding='utf-8') as fp:
        json.dump(json_data, fp, indent=4, sort_keys=True, default=str)
    # # copy the data to a current file to be downloaded by a client
    # shutil.copy(json_file_name, os.path.join(OUTPUT_PATH, "energy_price_forecast.json"))    

# get the weather forecast data and write the data to a json file
    data = meteo.read_json_url_weatherforecast(meteoserver_api_key, plaats, model='HARMONIE')  # Option 1: HARMONIE/HiRLAM
    json_data = {}
    json_data['weather forecast'] = data.to_dict(orient='records')
    json_data['units'] = {
        "temp": "°C",
        "winds (mean wind velocity)": "m/s",
        "windb (mean wind force)": "Beaufort",
        "windknp (mean wind velocity)": "knots",
        "windkmh (mean wind velocity)": "km/h",
        "windr (wind direction)": "°",
        "windrltr (wind direction)": "abbreviation",
        "gust (wind gust, GFS only)": "m/s",
        "gustb (wind gust, GFS only)": "Beaufort",
        "gustkt (wind gust, GFS only)": "knots",
        "gustkmh (wind gust, GFS only)": "km/h",
        "vis (visibility)": "m",
        "neersl (precipitation)": "mm",
        "luchtd (air pressure)": "mbar / hPa",
        "luchtdmmhg (air pressure)": "mm Hg",
        "luchtdinhg (air pressure)": "inch Hg",
        "rv (relative humidity)": "%",
        "gr (global horizontal radiation)": "W/m²",
        "hw (high cloud cover)": "%",
        "mw (medium cloud cover)": "%",
        "lw (low cloud cover)": "%",
        "tw (total cloud cover)": "%",
        "cape (convective available potential energy, GFS only)": "J/kg",
        "cond": "weather condition code",
        "ico": "weather icon code",
        "samenv": "text",
        "icoon": "image name"
    }
    json_data['metadata'] = {"plaats": plaats,
                             "data_timezone": local_timezone,
                             "model": "HARMONIE (Benelux)"}
    json_file_name = os.path.join(OUTPUT_PATH, f"{datetime.now().strftime('%y%m%d_%H%M%S')}{local_timezone}_weather_forecast.json")
    with open(json_file_name, 'w', encoding='utf-8') as fp:
        json.dump(json_data, fp, indent=4, sort_keys=True, default=str)
    # # copy the data to a current file to be downloaded by a client
    # shutil.copy(json_file_name, os.path.join(OUTPUT_PATH, "weather_forecast.json"))    

# get the sun forecast data and write the data to a json file
    current, forecast, location = meteo.read_json_url_sunData(meteoserver_api_key, plaats, loc=True, numeric=False)
    json_data = {}
    json_data['sun forecast'] = forecast.to_dict(orient='records')
    json_data['units'] = {
        "temp": "°C",
        "elev (sun altitude at the start of the current hour)": "°",
        "az (sun azimuth at the start of the current hour,  N=0, E=90)": "°",
        "gr (global horizontal radiation intensity)": "J/hr/cm²",
        "gr_w (global horizontal radiation intensity)": "W/m²",
        "sd (number of sunshine minutes in the current hour)": "min",
        "tc (total cloud cover)": "%",
        "lc (low-cloud cover)": "%",
        "mc (intermediate-cloud cover)": "%",
        "hc (high-cloud cover)": "%",
        "vis (visibility)": "m",
        "prec (total precipitation in the current hour)": "mm(/h)"
    }
    # json_data['current'] = current.to_dict(orient='records')    
    json_data['metadata'] = {"plaats": plaats,
                             "data_timezone": local_timezone}    
    json_file_name = os.path.join(OUTPUT_PATH, f"{datetime.now().strftime('%y%m%d_%H%M%S')}{local_timezone}_sun_forecast.json")
    with open(json_file_name, 'w', encoding='utf-8') as fp:
        json.dump(json_data, fp, indent=4, sort_keys=True, default=str)
    # # copy the data to a current file to be downloaded by a client
    # shutil.copy(json_file_name, os.path.join(OUTPUT_PATH, "sun_forecast.json"))    

# copy the data to remote storage
    if REMOTE_STORAGE_PATH is not None and REMOTE_STORAGE_PATH is not None:
        try:
            subprocess.run(['rclone', 'copy', OUTPUT_PATH, REMOTE_STORAGE_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            logging.error(f"Error copying data to remote storage: {e}")
