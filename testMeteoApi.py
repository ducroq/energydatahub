"""Example Python script using the Meteoserver module."""

import meteoserver as meteo

myKey = '7daf22bed0pip'    # My Meteoserver API key - put your OWN key here!
myLocation = 'Arnhem'  # My location

# Weather forecast #################################################################################

# Print some help:
meteo.print_help_weatherforecast()

location = 'Arnhem'  # Ensure we always have a location 'name' to write to file.

# Read weather-forecast data from file:
# data = meteo.read_json_file_weatherforecast('WeatherForecast1.json', full=True)  # Option 1: HARMONIE/HiRLAM (48 (42?) hours)
# data = meteo.read_json_file_weatherforecast('WeatherForecast2.json')  # Option 2: GFS (4/10 days), useful columns only, no location
# Option 2, with ALL columns and location; don't convert to numerical format, to allow writing to file later:
# data, location = meteo.read_json_file_weatherforecast('WeatherForecast2.json', full=True, loc=True, numeric=False)

# Get weather-forecast data from server:
# data = meteo.read_json_url_weatherforecast(myKey, myLocation, model='HARMONIE')  # Option 1: HARMONIE/HiRLAM
# data = meteo.read_json_url_weatherforecast(myKey, myLocation)  # Option 2 (default): GFS, useful columns only, no location
# Option 2, with ALL columns and location; don't convert to numerical format, to allow writing to file later:
data, location = meteo.read_json_url_weatherforecast(myKey, myLocation, full=True, loc=True, numeric=False)

# Print the data:
print(data)

# Write the downloaded data to a json file:
meteo.write_json_file_weatherforecast('WeatherForecast3.json', location, data)



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
current, forecast, location = meteo.read_json_url_sunData(myKey, myLocation, loc=True, numeric=False)

# Print the current-weather and forecast dataframes:
print("\nCurrent Sun/weather observation from a nearby station:")
print(current)

print("\nSun/weather forecast for the selected location/region:")
print(forecast)

# Write the downloaded data to a json file:
meteo.write_json_file_sunData('SunData1.json', location, current, forecast)