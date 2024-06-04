import os
import json
import pytz
import meteoserver as meteo
from configparser import ConfigParser 
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
secrets_file = os.path.join(script_dir, 'secrets.ini')

configur = ConfigParser() 
configur.read(secrets_file)
my_api_key = configur.get('api_keys', 'meteo')
plaats = configur.get('location', 'plaats')

local_timezone = pytz.timezone("CET")

# Weather forecast #################################################################################

# Print some help:
meteo.print_help_weatherforecast()

# Read weather-forecast data from file:
# data = meteo.read_json_file_weatherforecast('WeatherForecast1.json', full=True)  # Option 1: HARMONIE/HiRLAM (48 (42?) hours)
# data = meteo.read_json_file_weatherforecast('WeatherForecast2.json')  # Option 2: GFS (4/10 days), useful columns only, no location
# Option 2, with ALL columns and location; don't convert to numerical format, to allow writing to file later:
# data, location = meteo.read_json_file_weatherforecast('WeatherForecast2.json', full=True, loc=True, numeric=False)

# Get weather-forecast data from server:
data = meteo.read_json_url_weatherforecast(my_api_key, plaats, model='HARMONIE')  # Option 1: HARMONIE/HiRLAM
# data = meteo.read_json_url_weatherforecast(myKey, myLocation)  # Option 2 (default): GFS, useful columns only, no location
# Option 2, with ALL columns and location; don't convert to numerical format, to allow writing to file later:
# data, location = meteo.read_json_url_weatherforecast(myKey, myLocation, full=True, loc=True, numeric=False)

# Print the data:
print(data)

# Write the downloaded data to a json file:
# meteo.write_json_file_weatherforecast('WeatherForecast3.json', plaats, data)
json_data = {}
json_data['plaats'] = plaats
json_data['forecast'] = data.to_dict(orient='records')
json_file_name = os.path.join(f"{datetime.now().strftime('%y%m%d_%H%M%S')}{local_timezone}_weather_forecast.json")
with open(json_file_name, 'w', encoding='utf-8') as fp:
    json.dump(json_data, fp, indent=4, sort_keys=True, default=str)


# Sun forecast #####################################################################################

# Print some help:
meteo.print_help_sunData()

# Read a Meteoserver Sun-data JSON file from disc:
# current, forecast = meteo.read_json_file_sunData('SunData.json')
# Return the location; don't convert to numerical format, to allow writing to file later:
# current, forecast, location = meteo.read_json_file_sunData('SunData.json', loc=True, numeric=False)

# Get Meteoserver Sun data from the server for the given location (and key):
# current, forecast = meteo.read_json_url_sunData(myKey, myLocation)
# Return the location; don't convert to numerical format, to allow writing to file later:
current, forecast, location = meteo.read_json_url_sunData(my_api_key, plaats, loc=True, numeric=False)

# Print the current-weather and forecast dataframes:
print("\nCurrent Sun/weather observation from a nearby station:")
print(current)

print("\nSun/weather forecast for the selected location/region:")
print(forecast)

# Write the downloaded data to a json file:
# meteo.write_json_file_sunData('SunData1.json', location, current, forecast)
json_data = {}
json_data['plaats'] = location
json_data['current'] = current.to_dict(orient='records')
json_data['forecast'] = forecast.to_dict(orient='records')
    
json_file_name = os.path.join(f"{datetime.now().strftime('%y%m%d_%H%M%S')}{local_timezone}_sun_forecast.json")
with open(json_file_name, 'w', encoding='utf-8') as fp:
    json.dump(json_data, fp, indent=4, sort_keys=True, default=str)
