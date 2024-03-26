# ENTSO-E Api

The [Transparency Platform](transparency.entsoe.eu) of the European Network of Transmission System Operators for Electricity (ENTSO-E) provides a central collection and publication of electricity generation, transportation, and consumption data and information for the entire pan-European market.
ENTSO-E deals with the wholesale electricity market, which sets prices for day-ahead and balancing purposes.

## Authorization
To request access to the Restful API, please register on the [Transparency Platform](transparency.entsoe.eu) and send an email to transparency@entsoe.eu with “Restful API access” in the subject line. 
Indicate the email address you entered during registration in the email body. 
Once access has been granted, after logging in on [Transparency Platform](transparency.entsoe.eu), users will find a button to generate their token under 'My Account Settings' on TP.


# Data exploration
See also [user guide](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html),
[help](https://transparency.entsoe.eu/content/static_content/Static%20content/knowledge%20base/knowledge%20base.html),
[glossary](https://docstore.entsoe.eu/data/data-portal/glossary/Pages/home.aspx).

### Data categories

- _Load_: This includes information on electricity demand across different regions.
- _Generation_: Here you'll find data on electricity generation by source (e.g., wind, solar, nuclear).
- _Transmission_: This section deals with information on electricity flows across borders and within the grid. (This might be of particular interest to you since you mentioned entsoe).
- _Balancing_: Data related to balancing electricity supply and demand in real-time.
- _Outages_: Information on outages affecting the power grid.
- _Congestion Management_: Data on how grid congestion is managed.

### Data Access

- _Live_: You can view live updates for most categories, giving you a real-time picture of the European electricity market.
- _Historical_: The platform also offers access to historical data, allowing you to analyze trends and patterns over time. Data is available in hourly, monthly, and yearly formats depending on the category.
- _Actual_: This refers to the real-time measurements of electricity happening on the grid at this very moment. It reflects the actual electricity demand (load) and generation happening across Europe. Often denoted by terms like "Actual," "Measured," or "Real-Time." (e.g., "Total Load - Day Ahead / Actual [6.1.A] & [6.1.B]")
- _Day-ahead_: This is essentially a forecast of what's expected to happen tomorrow. It's based on predictions of electricity demand and generation submitted by market participants a day in advance. Usually indicated by phrases like "Day-Ahead Forecast" or "Expected." (e.g., "Generation Forecast - Day ahead [14.1.C]")

### Time resolution

The smallest time interval for which data is available differs among categories and depends on the type of access. Actual data is collected continuously from meters and sensors across the grid, resulting in high-resolution data, often at hourly intervals. 
Day-ahead forecasts are submitted by market participants at specific points in time, leading to potentially lower resolution.

### Price data

ENTSO-E deals with the pan-European wholesale electricity market, which sets prices for day-ahead and balancing purposes. These aren't directly translated to the final price consumers pay.
The final price you pay for electricity includes factors beyond the wholesale market, like taxes, grid fees, and distribution costs. These vary by location and supplier.

[//]: # (Balancing Costs: The day-ahead forecast only predicts the price for electricity itself. However, the final price also includes any costs incurred for balancing the grid in real-time &#40;prices of activated balancing energy, balancing reserves, etc.&#41;. These costs can fluctuate depending on unexpected changes in demand or generation.)
[//]: # (Transmission Losses: Electricity is lost during transmission over long distances. These losses are factored into the final settlement price, but they're not directly reflected in the day-ahead forecast.)
[//]: # (Taxes and Fees: Depending on the location, additional taxes and fees might be added to the final electricity price.)

The price data that is provided by ENTSO-E includes:
- _Day-ahead prices_: Forecasts of electricity prices for the next day. Market participants submit bids and offers for electricity, and the resulting market clearing process determines the price for each delivery hour. (Found in Transmission data)
- _Price of reserved balancing reserves_: The price paid to generators or other providers for holding reserve capacity that can be called upon to balance the grid in real-time if needed. (Found in Balancing data)
- _Prices of activated balancing energy_: The actual price paid for balancing energy used in real-time. By comparing this with your day-ahead forecast, you can see how much the market price deviated due to imbalances. (Found in Balancing data)
- _Volumes and prices of contracted reserves_: The amount and price of additional balancing capacity that has been pre-contracted by grid operators to ensure sufficient reserves are available for real-time balancing. (Found in Balancing data)
- _Prices of activated balancing energy_: The actual price paid for balancing energy that is activated in real-time to address imbalances between supply and demand. (Found in Balancing data)
- _Settlement prices_ : Final prices determined after the delivery of electricity, taking into account any imbalances and associated costs. Settlement prices might be based on day-ahead prices with adjustments for real-time balancing activities. (Found in FSKAR)

__TODO__: Check if FSKAR settlement prices might be a closer approximation to the actual realized electricity price compared to just the day-ahead prices.
Note FSKAR is a specific settlement mechanism used in some European countries, particularly in Central Western Europe. It might not be applicable to all regions represented on the ENTSO-E platform.
Even with FSKAR, there might still be slight discrepancies between the settlement price and the final delivered price due to factors like taxes and specific grid losses within a particular region.

By combining FSKAR data with additional resources, you can gain a more thorough understanding of how your day-ahead forecasts compare to the actual market outcome.
Additional sources to consider are Market Reports: As mentioned earlier, some countries might offer market reports on the ENTSO-E platform that include historical settlement prices.
External Data Providers: Some companies specialize in energy market data and might offer more comprehensive historical price information, including taxes and specific grid losses (for a fee).


## [Python wrapper](https://pypi.org/project/entsoe-py/)
Python client for the ENTSO-E API  is available

```python
from entsoe import EntsoePandasClient
import pandas as pd

client = EntsoePandasClient(api_key=<YOUR API KEY>)

start = pd.Timestamp('20171201', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'BE'  # Belgium
country_code_from = 'FR'  # France
country_code_to = 'DE_LU' # Germany-Luxembourg
type_marketagreement_type = 'A01'
contract_marketagreement_type = "A01"
process_type = 'A51'
```

__TODO__ Not sure how output relates to Data definitions
