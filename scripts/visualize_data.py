"""
Internet data acquisition for energy applications
--------------------------------------------------
Part of the Energy Data Integration Project at HAN University of Applied Sciences.

File: energy_data_visualiser.py
Created: 2024-11-23
Updated: 2024-12-19

Author: Jeroen Veen
        HAN University of Applied Sciences
        Arnhem, the Netherlands
Contributors:

Copyright (c) 2024 HAN University of Applied Sciences
All rights reserved.

This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.

Project Contributors:
    - HAN H2 LAB IPKW Development Team
    Initial development and integration with energy conversion systems

Description:
    Visualization tool for analyzing and comparing energy price data from multiple
    sources (ENTSO-E, EnergyZero, EPEX, Elspot). Supports both historical data
    analysis and current price comparisons with customizable date ranges and
    visualization options.

Dependencies:
    - pandas: Data manipulation and analysis
    - matplotlib: Base plotting library
    - seaborn: Enhanced plotting and styling
    - pytz: Timezone handling
    Required local packages:
    - utils.secure_data_handler: For encrypted data handling
    - utils.helpers: Configuration and file loading utilities

Usage:
    Run with current data:
        df = load_price_forecast("data/energy_price_forecast.json", handler)
        plot_prices(df, dark_mode=True)
        
    Run with historical data range:
        df = load_price_forecast_range(start_date, end_date, data_folder, handler)
        plot_prices(df, dark_mode=True)

Notes:
    - Supports both encrypted and unencrypted JSON data files
    - Handles timezone-aware timestamps in Europe/Amsterdam timezone
    - Automatically converts EnergyZero prices from EUR/kWh to EUR/MWh
    - Provides dark mode option for visualizations
"""
import re
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import glob
import os
import pytz
import base64

from utils.secure_data_handler import SecureDataHandler
from utils.helpers import load_config, load_data_file

def load_price_forecast_range(start_date, end_date, file_path='data', handler: SecureDataHandler = None):
    timezone = start_date.tzinfo
    
    all_data = []
    json_files = glob.glob(os.path.join(file_path, '*energy_price_forecast.json'))
    
    for file in json_files:
        try:
            data = load_data_file(file, handler)
            
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

def load_price_forecast(json_file: str, handler: SecureDataHandler):
    timezone = pytz.timezone('Europe/Amsterdam')

    try:
        data = load_data_file("data/energy_price_forecast.json", handler)

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

if __name__ == "__main__":
    SECRETS_FILE_NAME = 'secrets.ini'

    script_dir = os.path.dirname(os.path.abspath(__file__))    
    config = load_config(script_dir, SECRETS_FILE_NAME)        
    encryption_key = base64.b64decode(config.get('security_keys', 'encryption'))
    hmac_key = base64.b64decode(config.get('security_keys', 'hmac'))
    handler = SecureDataHandler(encryption_key, hmac_key)

    file_name = r"data\energy_price_forecast.json"
    df = load_price_forecast(file_name, handler)
    # print(df)

    # # Define time interval
    # timezone = pytz.timezone('Europe/Amsterdam')
    # end_date = datetime.now(timezone)
    # start_date = end_date - timedelta(days=100)
    
    # data_folder = r"..\..\05. Data\encrypted_data_since_2409"
    # df = load_price_forecast_range(start_date, end_date, data_folder, handler)

    plot = plot_prices(df, dark_mode=True)

    # output_file = 'price_comparison_range.png'
    # plot.savefig(output_file)

    plt.show()
