import os
import sys
import glob
import base64
from configparser import ConfigParser
import json
import logging

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.secure_data_handler import SecureDataHandler

def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('decrypt_data.log')
        ]
    )

def load_config(script_dir: str, secrets_file: str = 'secrets.ini') -> tuple:
    """
    Load encryption configuration from secrets file.
    
    Args:
        script_dir (str): Directory containing the secrets file
        secrets_file (str): Name of the secrets file
        
    Returns:
        tuple: (encryption_key, hmac_key)
    """
    config = ConfigParser()
    secrets_path = os.path.join(script_dir, secrets_file)
    
    if not os.path.exists(secrets_path):
        raise FileNotFoundError(f"Secrets file not found at {secrets_path}")
        
    config.read(secrets_path)
    
    encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
    hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))
    
    return encryption_key, hmac_key

def create_output_directory(input_dir: str) -> str:
    """
    Create output directory for decrypted files.
    
    Args:
        input_dir (str): Input directory path
        
    Returns:
        str: Path to output directory
    """
    output_dir = input_dir + '_decrypted'
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


if __name__ == "__main__":
    input_folder = r"C:\Users\scbry\HAN\HAN H2 LAB IPKW - Projects - WebBasedControl\01. Software\energyDataHub\data"
    output_folder = r"C:\Users\scbry\HAN\HAN H2 LAB IPKW - Projects - WebBasedControl\05. Data\decrypted_data"

    setup_logging()
    
    try:
        # Get script directory and load configuration (secrets.ini is in parent folder)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        encryption_key, hmac_key = load_config(parent_dir)
        
        # Initialize secure data handler
        handler = SecureDataHandler(encryption_key, hmac_key)
        
        if not os.path.exists(input_folder):
            raise FileNotFoundError(f"Directory not found: {input_folder}")

        # Create output directory if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)

        # Process all JSON files in input directory
        json_files = glob.glob(os.path.join(input_folder, '*.json'))
        
        for file_path in json_files:
            try:
                filename = os.path.basename(file_path)
                output_path = os.path.join(output_folder, filename)
                
                logging.info(f"Processing file: {filename}")

                # Skip if already decrypted
                if os.path.isfile(output_path):
                    continue  

                # Read data
                with open(file_path, 'r') as f:
                    data = f.read()
                    
                # Check if file is empty
                if not data:
                    raise ValueError(f"File {filename} is empty or unreadable")
                
                # Attempt to parse JSON directly 
                try:
                    decrypted_data = json.loads(data)
                except json.JSONDecodeError:
                    # Decrypt data
                    decrypted_data = handler.decrypt_and_verify(data)
                
                # Write decrypted data
                with open(output_path, 'w') as f:
                    json.dump(decrypted_data, f, indent=2, default=str)
                    
                logging.info(f"Successfully decrypted: {filename}")
                
            except Exception as e:
                logging.error(f"Error processing {filename}: {e}")
            
            logging.info("Decryption process completed successfully")
            
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise
