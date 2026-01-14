import os
import json
import base64
from datetime import datetime

from utils.secure_data_handler import SecureDataHandler
from utils.helpers import load_config

SECRETS_FILE_NAME = 'secrets.ini'
SETTINGS_FILE_NAME = 'settings.ini'

file_name = r"c:\Users\scbry\HAN\HAN H2 LAB IPKW - Projects - WebBasedControl\01. Software\energyDataHub\data\energy_price_forecast.json"

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

with open(file_name, 'r') as f:
    data = f.read()

script_dir = os.path.dirname(os.path.abspath(__file__))

config = load_config(script_dir, SECRETS_FILE_NAME)        
encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))
handler = SecureDataHandler(encryption_key, hmac_key)

decrypted = handler.decrypt_and_verify(data)
pretty_json = json.dumps(decrypted, indent=4, default=json_serializer)
# with open(filename, 'w') as f:
#     json.dump(self.to_dict(), f, indent=2, default=json_serializer)

print(pretty_json)


