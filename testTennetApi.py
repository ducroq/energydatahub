from tennet import TenneTClient, DataType, OutputType
import pandas as pd
from datetime import datetime, timedelta

start = pd.Timestamp(datetime.now() - timedelta(days=1))
end = pd.Timestamp(datetime.now())

client = TenneTClient(default_output=OutputType.XML)
df = client.query_df(DataType.settlementprices, d_from=start, d_to=end)
print(df.columns)
print(df.head())