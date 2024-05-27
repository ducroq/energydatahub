from configparser import ConfigParser 
import requests
import json

configur = ConfigParser() 
configur.read('secrets.ini')
my_api_key = configur.get('api_keys', 'openweather')
lattitude = configur.get('location', 'lattitude')
longitude = configur.get('location', 'longitude')

city = "Arnhem"
country_code = "NL"

url = f"https://api.openweathermap.org/data/2.5/weather?q={city},{country_code}&appid={my_api_key}"
# https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={time}&appid={API key}

# Send GET request
response = requests.get(url)

# Check for successful response
if response.status_code == 200:
  # Parse JSON data
  data = response.json()
  
  # Extract relevant weather information
  # (modify this section to extract desired data)
  temperature = data["main"]["temp"]  # Temperature in Kelvin
  weather_description = data["weather"][0]["description"]
  
  # Print the weather data
  print(f"Current weather in {city}, {country_code}:")
  print(f"  Temperature: {temperature:.2f} Kelvin")
  print(f"  Description: {weather_description}")
else:
  # Handle API errors
  print("Error:", response.status_code)

