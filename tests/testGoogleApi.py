import os
import json
from configparser import ConfigParser 
import urllib.request

# energy_price_forecast: https://drive.google.com/file/d/1fmO7__sddvmrZz_8Na7hMGw3BHQfoqPD/view?usp=drive_link
# weather_forecast: https://drive.google.com/file/d/1qk3B9h6gRiMwcUfFu9bFVc4qMl-GcVX5/view?usp=drive_link
# sun_forecast: https://drive.google.com/file/d/15gB-RGCjf97f4gVIqNTZoXTNpgiAFbCS/view?usp=drive_link

script_dir = os.path.dirname(os.path.abspath(__file__))
secrets_file = os.path.join(os.path.dirname(script_dir), 'secrets.ini')

configur = ConfigParser() 
configur.read(secrets_file)
my_api_key = configur.get('api_keys', 'google')

test_file_ID = "1f_3XnRis36gDBO0h5EBMjMYFYx3tYlGq"
energy_price_forecast_file_ID = "1fmO7__sddvmrZz_8Na7hMGw3BHQfoqPD"
weather_forecast_file_ID = "1qk3B9h6gRiMwcUfFu9bFVc4qMl-GcVX5"
sun_forecast_file_ID = "15gB-RGCjf97f4gVIqNTZoXTNpgiAFbCS"

file_ID = energy_price_forecast_file_ID 

url = "https://www.googleapis.com/drive/v3/files/" + file_ID + "?key=" + my_api_key
contents = urllib.request.urlopen(url).read()
json_contents = json.loads(contents.decode('utf-8'))
print(f"filename = {json_contents['name']}")

url = "https://www.googleapis.com/drive/v3/files/" + file_ID + "?alt=media&key=" + my_api_key
contents = urllib.request.urlopen(url).read()
json_contents = json.loads(contents.decode('utf-8'))
print(f"data = {json_contents}")