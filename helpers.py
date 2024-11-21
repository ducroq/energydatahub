import os
import logging
from math import cos, asin, sqrt
from configparser import ConfigParser

def ensure_output_directory(path: str) -> None:
    """Ensure the output directory exists."""
    try:
        os.makedirs(path, exist_ok=True)
        logging.info(f"Output directory ensured: {path}")
    except OSError as e:
        logging.error(f"Error creating folder: {e}")
        raise

def load_config(script_dir: str, filename: str) -> ConfigParser:
    """Load configuration from the secrets file."""
    config = ConfigParser()
    secrets_file = os.path.join(script_dir, filename)
    config.read(secrets_file)
    return config


def convert_value(value):
    if type(value) == int or type(value) == float:
        return value
    if value == '-':
        return None  # Using None instead of NaN for JSON compatibility
    elif value.lower() == 'none':
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value  # Keep as string if it's not a number
        
def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    a = (
        0.5
        - cos((lat2 - lat1) * p) / 2
        + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    )
    distance = 12742 * asin(sqrt(a))
    return distance

def closest(data, v):
    return min(
        data,
        key=lambda p: distance(
            v["latitude"], v["longitude"], p["latitude"], p["longitude"]
        ),
    )        