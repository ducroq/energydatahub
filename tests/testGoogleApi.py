import os
import json
from configparser import ConfigParser 
import urllib.request

script_dir = os.path.dirname(os.path.abspath(__file__))
secrets_file = os.path.join(os.path.dirname(script_dir), 'secrets.ini')

configur = ConfigParser() 
configur.read(secrets_file)
my_api_key = configur.get('api_keys', 'google')

file_ID = "1f_3XnRis36gDBO0h5EBMjMYFYx3tYlGq"

url = "https://www.googleapis.com/drive/v3/files/" + file_ID + "?key=" + my_api_key
contents = urllib.request.urlopen(url).read()
json_contents = json.loads(contents.decode('utf-8'))
print(f"filename = {json_contents['name']}")

url = "https://www.googleapis.com/drive/v3/files/" + file_ID + "?alt=media&key=" + my_api_key
contents = urllib.request.urlopen(url).read()
json_contents = json.loads(contents.decode('utf-8'))
print(f"data = {json_contents}")