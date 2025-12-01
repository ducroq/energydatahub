import os
import re
import json
import logging
from math import cos, asin, sqrt
from configparser import ConfigParser
from typing import Any, Dict

def ensure_output_directory(path: str) -> None:
    """Ensure the output directory exists."""
    try:
        os.makedirs(path, exist_ok=True)
        logging.info(f"Output directory ensured: {path}")
    except OSError as e:
        logging.error(f"Error creating folder: {e}")
        raise

def load_settings(script_dir: str, filename: str) -> ConfigParser:
    """Load configuration from the settings file."""
    config = ConfigParser()
    secrets_file = os.path.join(script_dir, filename)
    config.read(secrets_file)
    return config

def load_secrets(script_dir: str, filename: str = 'secrets.ini') -> ConfigParser:
    """
    Load configuration from secrets file and environment variables.
    Follows content-aggregator pattern: file first, then env vars override.
    Never fails - returns empty config if no secrets available.

    Priority:
    1. Load from secrets.ini (shared team secrets)
    2. Override with secrets.local.ini (personal secrets, gitignored)
    3. Override with environment variables (CI/production)

    Args:
        script_dir (str): Directory containing the secrets file
        filename (str): Name of the secrets file (default: 'secrets.ini')

    Returns:
        ConfigParser: Configuration with secrets from file and/or environment
    """
    config = ConfigParser()

    # Initialize all required sections
    config.add_section('security_keys')
    config.add_section('api_keys')
    config.add_section('location')

    # 1. Try to load from secrets.ini (shared team secrets)
    secrets_file = os.path.join(script_dir, filename)
    if os.path.exists(secrets_file):
        logging.info(f"Loading secrets from {filename}")
        config.read(secrets_file)
    else:
        logging.info(f"No {filename} found, using environment variables only")

    # 2. Try to load from secrets.local.ini (personal secrets, NOT in git)
    local_secrets_file = os.path.join(script_dir, 'secrets.local.ini')
    if os.path.exists(local_secrets_file):
        logging.info(f"Loading personal secrets from secrets.local.ini")
        config.read(local_secrets_file)  # Overwrites shared secrets if key exists

    # 3. Override with environment variables (GitHub Actions, production)
    # This allows CI to work without secrets.ini file
    env_mappings = {
        # Security keys
        'ENCRYPTION_KEY': ('security_keys', 'encryption'),
        'HMAC_KEY': ('security_keys', 'hmac'),
        # API keys
        'ENTSOE_API_KEY': ('api_keys', 'entsoe'),
        'OPENWEATHER_API_KEY': ('api_keys', 'openweather'),
        'METEO_API_KEY': ('api_keys', 'meteo'),
        'GOOGLE_API_KEY': ('api_keys', 'google'),
        'GOOGLE_WEATHER_API_KEY': ('api_keys', 'google_weather'),
        'TENNET_API_KEY': ('api_keys', 'tennet'),
        'NED_API_KEY': ('api_keys', 'ned'),
        # Location
        'LATITUDE': ('location', 'latitude'),
        'LONGITUDE': ('location', 'longitude'),
    }

    for env_var, (section, key) in env_mappings.items():
        value = os.getenv(env_var)
        if value:
            config.set(section, key, value)
            logging.debug(f"Overriding {section}.{key} from environment variable {env_var}")

    # Set location defaults if not provided anywhere
    if not config.has_option('location', 'latitude'):
        config.set('location', 'latitude', '51.9851')  # Default to Arnhem
    if not config.has_option('location', 'longitude'):
        config.set('location', 'longitude', '5.8987')

    return config

def load_config(script_dir: str, filename: str = 'secrets.ini') -> ConfigParser:
    """
    Load configuration from secrets file and environment variables.
    Alias for load_secrets() for backward compatibility.
    Follows content-aggregator pattern: file first, then env vars override.

    Args:
        script_dir (str): Directory containing the secrets file
        filename (str): Name of the secrets file (default: 'secrets.ini')

    Returns:
        ConfigParser: Configuration with secrets from file and/or environment
    """
    # Just delegate to load_secrets which now has the correct pattern
    return load_secrets(script_dir, filename)

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

def detect_file_type(content: str) -> str:
    """
    Detect whether file content is JSON or encrypted Base64.
    
    Args:
        content (str): The file content to analyze
        
    Returns:
        str: 'json' or 'encrypted'
    """
    # Try to detect if it's base64 encoded
    base64_pattern = r'^[A-Za-z0-9+/=]+$'
    if re.match(base64_pattern, content.strip()):
        return 'encrypted'
    
    # Try to detect if it's JSON
    try:
        json.loads(content)
        return 'json'
    except json.JSONDecodeError:
        raise ValueError("File content is neither valid JSON nor base64 encoded")
    
def validate_data_timestamps(data: Dict[str, Any]) -> tuple[bool, list]:
    """
    Validate all timestamps in a data structure for correct timezone formatting.

    Handles both flat structures (timestamp -> value) and nested structures
    (country/location -> timestamp -> value) used by multi-region collectors.

    Args:
        data (dict): The data dictionary to validate (CombinedDataSet.to_dict() format)

    Returns:
        tuple: (is_valid, list of malformed timestamps)

    Examples:
        >>> data = {'version': '2.0', 'elspot': {'data': {'2025-10-24T12:00:00+00:09': 50.0}}}
        >>> is_valid, malformed = validate_data_timestamps(data)
        >>> is_valid
        False
        >>> malformed
        ['elspot: 2025-10-24T12:00:00+00:09']
    """
    from utils.timezone_helpers import validate_timestamp_format

    malformed_timestamps = []

    def is_timestamp_like(key: str) -> bool:
        """Check if a key looks like a timestamp (contains date-like pattern)."""
        # Timestamps have format like '2025-12-01T00:00:00+01:00'
        return len(key) > 10 and 'T' in key and ('-' in key or '+' in key)

    def validate_timestamps_recursive(data_dict: dict, source_name: str, prefix: str = ""):
        """Recursively validate timestamps in nested structures."""
        for key, value in data_dict.items():
            full_key = f"{prefix}{key}" if prefix else key

            if is_timestamp_like(key):
                # This looks like a timestamp - validate it
                if not validate_timestamp_format(key):
                    malformed_timestamps.append(f"{source_name}: {full_key}")
            elif isinstance(value, dict):
                # This is a nested structure (e.g., country code -> timestamps)
                # Check if the nested dict contains timestamp-like keys
                if value and any(is_timestamp_like(k) for k in value.keys()):
                    validate_timestamps_recursive(value, source_name, f"{full_key}/")

    for source_name, source_data in data.items():
        # Skip version and other metadata fields
        if source_name == 'version' or not isinstance(source_data, dict):
            continue

        # Check if this source has a 'data' dict with timestamps
        if 'data' not in source_data:
            continue

        # Validate timestamps (handles both flat and nested structures)
        validate_timestamps_recursive(source_data['data'], source_name)

    is_valid = len(malformed_timestamps) == 0
    return is_valid, malformed_timestamps

def save_data_file(
    data: Dict[str, Any],
    file_path: str,
    handler: Any = None,
    encrypt: bool = False
) -> None:
    """
    Save data to a file, optionally encrypting it.

    Validates timestamps before saving to prevent malformed timezone offsets.

    Args:
        data (dict): The data to save
        file_path (str): Path where to save the file
        handler (SecureDataHandler, optional): Handler for encrypting data
        encrypt (bool): Whether to encrypt the data

    Raises:
        ValueError: If data contains malformed timestamps
    """
    try:
        # Validate timestamps before saving
        data_dict = data.to_dict() if hasattr(data, 'to_dict') else data
        is_valid, malformed = validate_data_timestamps(data_dict)

        if not is_valid:
            error_msg = f"Data contains malformed timestamps:\n" + "\n".join(malformed)
            logging.error(error_msg)
            raise ValueError(error_msg)

        logging.info("Timestamp validation passed - all timestamps correctly formatted")

        if encrypt:
            if handler is None:
                raise ValueError("Encryption requested but no handler provided")
            encrypted_data = handler.encrypt_and_sign(data_dict)
            with open(file_path, 'w') as f:
                f.write(encrypted_data)
        else:
            if hasattr(data, 'write_to_json'):
                data.write_to_json(file_path)
            else:
                with open(file_path, 'w') as f:
                    json.dump(data_dict, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving file {file_path}: {e}")
        raise
    
def load_data_file(file_path: str, handler: Any = None) -> Dict[str, Any]:
    """
    Load data from a file, automatically detecting if it's encrypted or plain JSON.
    
    Args:
        file_path (str): Path to the file to load
        handler (SecureDataHandler, optional): Handler for decrypting data
        
    Returns:
        dict: The loaded data
    """
    try:
        with open(file_path, 'r') as f:
            content = f.read().strip()
        
        file_type = detect_file_type(content)
        
        if file_type == 'encrypted':
            if handler is None:
                raise ValueError("Encrypted file found but no handler provided")
            return handler.decrypt_and_verify(content)
        else:
            return json.loads(content)
            
    except Exception as e:
        logging.error(f"Error loading file {file_path}: {e}")
        raise
