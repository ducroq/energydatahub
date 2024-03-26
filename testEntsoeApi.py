from entsoe import EntsoePandasClient
import pandas as pd
from datetime import datetime, timedelta
from configparser import ConfigParser 

# Day-ahead-prices are 'documentType': 'A44', 'EUR/MWh'
configur = ConfigParser() 
configur.read('secrets.ini')
my_api_key = configur.get('api_keys', 'entsoe')
print(my_api_key)

country_code = 'NL'
resolution = '15min'

start = pd.Timestamp(datetime.now() - timedelta(days=1), tz='Europe/Amsterdam')
end = pd.Timestamp(datetime.now() + timedelta(days=1), tz='Europe/Amsterdam')

client = EntsoePandasClient(api_key=my_api_key)

ts = client.query_day_ahead_prices(country_code, start=start, end=end) #, resolution=resolution)

print(ts.head())

# ts.to_csv('outfile.csv')