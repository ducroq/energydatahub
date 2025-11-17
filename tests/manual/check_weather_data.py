"""Quick script to decrypt and check weather_forecast_multi_location.json"""
import os
import sys
import json
import base64

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.helpers import load_secrets
from utils.secure_data_handler import SecureDataHandler

script_dir = os.path.dirname(os.path.abspath(__file__))
config = load_secrets(script_dir, 'secrets.ini')

encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))
handler = SecureDataHandler(encryption_key, hmac_key)

# Read encrypted file
file_path = os.path.join(script_dir, 'data', 'weather_forecast_multi_location.json')
with open(file_path, 'r') as f:
    encrypted_data = f.read()

# Decrypt
data = handler.decrypt_and_verify(encrypted_data)

# Show structure
print("=" * 60)
print("WEATHER FORECAST MULTI-LOCATION DATA STRUCTURE")
print("=" * 60)
print(f"\nTop-level keys: {list(data.keys())}")

print(f"\n\nFull data structure (first 2000 chars):")
print(json.dumps(data, indent=2)[:2000])

if 'data' in data:
    print(f"\n\nData keys: {list(data['data'].keys())}")
    print(f"Number of locations: {len(data['data'])}")

    for location_name, location_data in data['data'].items():
        print(f"\n{location_name}:")
        if isinstance(location_data, dict):
            print(f"  Number of timestamps: {len(location_data)}")
            if location_data:
                first_timestamp = list(location_data.keys())[0]
                print(f"  First timestamp: {first_timestamp}")
                print(f"  Fields: {list(location_data[first_timestamp].keys())}")
        else:
            print(f"  Type: {type(location_data)}")
            print(f"  Value: {location_data}")

if 'metadata' in data:
    print(f"\nMetadata:")
    for key, value in data['metadata'].items():
        print(f"  {key}: {value}")

print("\n" + "=" * 60)
