import re
import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import glob
import os
import pytz
import base64

from secure_data_handler import SecureDataHandler
from helpers import load_config

BASE_PATH = r"c:\Users\scbry\HAN\HAN H2 LAB IPKW - Projects - project_nr_WebBasedControl\01. Software\energyDataHub\data"

def load_multiple_files(start_date, end_date):
    timezone = start_date.tzinfo
    
    all_data = []
    json_files = glob.glob(os.path.join(BASE_PATH, '*energy_price_forecast.json'))
    
    for file in json_files:
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            
            for source in ['entsoe', 'energy_zero', 'epex', 'elspot']:
                if source in data:
                    source_data = data[source]['data']
                    for timestamp, price in source_data.items():
                        try:
                            dt = pd.to_datetime(timestamp).tz_convert(timezone)
                            if start_date <= dt <= end_date and price is not None:
                                # Convert Energy Zero kWh to MWh
                                if source == 'energy_zero':
                                    price_float = float(price) * 1000 if isinstance(price, (int, float, str)) else None
                                else:
                                    price_float = float(price) if isinstance(price, (int, float, str)) else None
                                    
                                if price_float is not None:
                                    all_data.append({
                                        'timestamp': dt,
                                        'price': price_float,
                                        'source': source
                                    })
                        except (ValueError, TypeError) as e:
                            print(f"Error processing timestamp {timestamp} or price {price}: {e}")
        except Exception as e:
            print(f"Error processing file {file}: {e}") 

    if not all_data:
        raise ValueError("No valid data found for the specified time range")
    
    df = pd.DataFrame(all_data)
    return df.sort_values('timestamp')


def is_encrypted(json_data):
    if isinstance(json_data, str):
        # Check if it's a single long string with base64 characters
        base64_pattern = r'^[A-Za-z0-9+/=]+$'
        return bool(re.match(base64_pattern, json_data.strip()))
    return False

def plot_prices(df, dark_mode=False):
   color_dict = {
       'entsoe': '#8884d8',
       'energy_zero': '#82ca9d', 
       'epex': '#ffc658',
       'elspot': '#ff7300'
   }

   if dark_mode:
       plt.style.use('dark_background')
       grid_color = '#404040'
   else:
       sns.set_style("whitegrid")
       grid_color = '#cccccc'
   
   plt.figure(figsize=(15, 8))
   
   sns.lineplot(data=df, x='timestamp', y='price', hue='source', marker='o', palette=color_dict)
   
   plt.grid(color=grid_color, linestyle='-', linewidth=1)
   plt.title(f'Energy price forecasts\n{df.timestamp.min().strftime("%Y-%m-%d %H:%M")} to {df.timestamp.max().strftime("%Y-%m-%d %H:%M")}')
   plt.xlabel('Time')
   plt.ylabel('Price (EUR/MWh)')
   plt.xticks(rotation=45)
   plt.tight_layout()
   
   return plt

def load_price_forecast_json_file(json_file, handler):
    timezone = pytz.timezone('Europe/Amsterdam')

    try:
        with open(os.path.join(BASE_PATH, json_file), 'r') as f:
            data = json.load(f)

        if is_encrypted(data):
            print("Data is encrypted, decrypting...")
            data = handler.decrypt_and_verify(data)

        ret_data = []
        for source in ['entsoe', 'energy_zero', 'epex', 'elspot']:
            if source in data:
                source_data = data[source]['data']
                for timestamp, price in source_data.items():
                    dt = pd.to_datetime(timestamp).tz_convert(timezone)

                    if source == 'energy_zero':
                        price_float = float(price) * 1000 if isinstance(price, (int, float, str)) else None
                    else:
                        price_float = float(price) if isinstance(price, (int, float, str)) else None
                    if price_float is not None:
                        ret_data.append({
                            'timestamp': dt,
                            'price': price_float,
                            'source': source
                        })

        df = pd.DataFrame(ret_data)

        return df.sort_values('timestamp')
        
    except Exception as e:
        print(f"Error in main: {e}")


 todo add this to main function
def main_time_interval():
    # Define time interval
    timezone = pytz.timezone('Europe/Amsterdam')
    end_date = datetime.now(timezone)
    start_date = end_date - timedelta(days=100)
    
    print(f"Looking for data between {start_date} and {end_date}")
    
    try:
        df = load_multiple_files(start_date, end_date)
        
        plot = plot_prices(df, dark_mode=False)
        output_file = 'price_comparison_range.png'
        plot.savefig(output_file)
        print(f"Plot saved as {output_file}")
        
        # Print summary statistics
        print("\nPrice statistics by source:")
        stats = df.groupby('source')['price'].agg(['mean', 'min', 'max'])
        print(stats.round(2))
        
    except Exception as e:
        print(f"Error in main: {e}")

if __name__ == "__main__":
    SECRETS_FILE_NAME = 'secrets.ini'
    json_file = 'energy_price_forecast.json'

    script_dir = os.path.dirname(os.path.abspath(__file__))    
    config = load_config(script_dir, SECRETS_FILE_NAME)        
    encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
    hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))
    handler = SecureDataHandler(encryption_key, hmac_key)

    df = load_price_forecast_json_file(json_file, handler)

    plot = plot_prices(df, dark_mode=False)
    output_file = 'price_comparison_range.png'
    plot.savefig(output_file)
    print(f"Plot saved as {output_file}")

    # main_single_file()
