import knmi 
from datetime import datetime, date, timedelta

today = date.today()
yesterday = date.today() - timedelta(days=1)
tomorrow = date.today() + timedelta(days=1)

df = knmi.get_day_data_dataframe(stations=[260], start=yesterday, end=today) 
print(df)

df = knmi.get_forecast_dataframe()
print(df)
