import os
import glob
import base64
from configparser import ConfigParser
from utils.secure_data_handler import SecureDataHandler
import json
from datetime import datetime
import logging

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

def process_files(input_dir: str, handler: SecureDataHandler):
    """
    Process all JSON files in the input directory.
    
    Args:
        input_dir (str): Directory containing encrypted files
        handler (SecureDataHandler): Initialized SecureDataHandler instance
    """
    output_dir = create_output_directory(input_dir)
    
    # Find all JSON files in input directory
    json_files = glob.glob(os.path.join(input_dir, '*.json'))
    
    for file_path in json_files:
        try:
            filename = os.path.basename(file_path)
            output_path = os.path.join(output_dir, filename)
            
            logging.info(f"Processing file: {filename}")
            
            # Read encrypted data
            with open(file_path, 'r') as f:
                encrypted_data = f.read()
            
            # Decrypt data
            decrypted_data = handler.decrypt_and_verify(encrypted_data)
            
            # Write decrypted data
            with open(output_path, 'w') as f:
                json.dump(decrypted_data, f, indent=2, default=str)
                
            logging.info(f"Successfully decrypted: {filename}")
            
        except Exception as e:
            logging.error(f"Error processing {filename}: {e}")

def main(input_dir: str):
    """Main function to orchestrate the decryption process."""
    setup_logging()
    
    try:
        # Get script directory and load configuration
        script_dir = os.path.dirname(os.path.abspath(__file__))
        encryption_key, hmac_key = load_config(script_dir)
        
        # Initialize secure data handler
        handler = SecureDataHandler(encryption_key, hmac_key)
        
        if not os.path.exists(input_dir):
            raise FileNotFoundError(f"Directory not found: {input_dir}")
        
        # Process all files
        process_files(input_dir, handler)
        
        logging.info("Decryption process completed successfully")
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    data_folder = r"..\..\05. Data\encrypted_data_since_2409"
    main(data_folder)