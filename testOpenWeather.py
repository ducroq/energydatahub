from configparser import ConfigParser 
import requests
import json

plaats = 'Arnhem'

configur = ConfigParser() 
configur.read('secrets.ini')
my_api_key = configur.get('api_keys', 'openweather')
# latitude = configur.get('location', 'latitude')
# longitude = configur.get('location', 'longitude')

url = f"http://api.openweathermap.org/geo/1.0/direct?q={plaats},?,NL&limit=1&appid={my_api_key}"
response = requests.get(url)

if response.status_code == 200:
  data = response.json()
  print(json.dumps(data, indent=4))
  print(data[0]["lat"], data[0]["lon"])
  latitude = data[0]["lat"]
  longitude = data[0]["lon"]

url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={my_api_key}"
response = requests.get(url)

if response.status_code == 200:
  data = response.json()
  print(json.dumps(data, indent=4))
  data["main"]["temp"]
  data["main"]["humidity"]
  data["main"]["pressure"]
  data["weather"][0]["description"]
  data["wind"]["speed"]
  data["visibility"]
  data["clouds"]["all"]
  
  # # Extract relevant weather information
  # # (modify this section to extract desired data)
  # temperature = data["main"]["temp"]  # Temperature in Kelvin
  # weather_description = data["weather"][0]["description"]
  
  # # Print the weather data
  # print(f"Current weather in {city}, {country_code}:")
  # print(f"  Temperature: {temperature:.2f} Kelvin")
  # print(f"  Description: {weather_description}")
else:
  # Handle API errors
  print("Error:", response.status_code)


# url = f"https://api.openweathermap.org/energy/1.0/solar/data?lat={latitude}&lon={longitude}&date={date}&appid={my_api_key}"
url = f"https://api.openweathermap.org/energy/1.0/solar/data?lat=60.45&lon=-38.67&date=2023-03-30&tz=+03:00&appid={my_api_key}"
response = requests.get(url)

if response.status_code == 200:
  data = response.json()
  print(json.dumps(data, indent=4))
else:
  # Handle API errors
  print("Error:", response.status_code)