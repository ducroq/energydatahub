# Tennet Api

## [Python wrapper](https://pypi.org/project/tennet-py/)

Wrapper exposes the following data: Measurement data, Imbalance price, Balance delta with prices, Available capacity.

Exact meaning of the terms is unclear.
- Measurement data: This likely refers to real-time or near real-time measurements from the power grid. It could include values like:
  - Electricity flow: The amount of electricity moving through specific points in the grid, measured in Megawatts (MW).
  - Voltage: The electrical pressure at different points in the grid, measured in Kilovolts (kV).
  - Frequency: The rate at which the electricity alternates, typically 50 Hz in Europe.
- Imbalance price: This refers to the price charged or awarded to parties for imbalances between electricity injection and withdrawal from the grid. When there's an imbalance, it can cause instability in the grid's frequency.
- Balance delta with prices: This likely combines information about the imbalance in electricity supply and demand with the associated imbalance price. It might show the amount of imbalance (positive or negative) and the corresponding price for that imbalance.
- Available capacity: This refers to the amount of additional electricity that can be transmitted through a specific part of the grid at a given time. It's crucial for ensuring the grid can handle fluctuations in demand.

However, when I query the wrapper,
```python
client = TenneTClient(default_output=OutputType.XML)
df = client.query_df(DataType.settlementprices, d_from=start, d_to=end)
print(df.columns)
```
we find different columns, namely
```console
['period_from', 'period_until', 'upward_incident_reserve',
       'downward_incident_reserve', 'To regulate up', 'To regulate down',
       'Incentive component', 'Consume', 'Feed', 'Regulation state']
```
It is not immediately obvious what these fields represent, nor how they are related to the fields that are supposed to be exposed. 
This may be because the wrapper seems to be focused on a specific dataset related to balancing the electricity grid, rather than the broader system and transmission data.
An initial guess of the meaning of the terms related to the wrappers definition, this could be an explanation:

- upward_incident_reserve: This could be related to the available capacity, indicating the amount of additional power that can be injected into the grid during this period.
- downward_incident_reserve: This could be the counterpart to the previous field, indicating the amount of additional power that can be withdrawn from the grid during this period.
- To regulate up: This is likely related to the balance delta with prices. It might indicate the amount of power generation required to increase grid frequency (positive imbalance) during this period.
- To regulate down: Similar to the above, this could indicate the amount of power reduction needed to decrease grid frequency (negative imbalance) during this period.
- Incentive component: This is less clear from the field name alone. It might be related to the imbalance price, indicating the financial incentive offered to encourage parties to adjust their electricity consumption or injection to help balance the grid.
- Consume: This could be related to the overall electricity flow, specifically the amount of electricity being withdrawn from the grid during this period.
- Feed: This could also be related to the overall electricity flow, specifically the amount of electricity being injected into the grid during this period.
- Regulation state: This field likely indicates the overall state of the grid's frequency regulation during this period (e.g., Underfrequency, Overfrequency, Balanced).