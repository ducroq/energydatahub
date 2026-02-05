# Carbon and Gas Price Proxies via ETFs and Futures

## Overview

Direct access to EU carbon (EUA) and natural gas (TTF) commodity prices typically requires expensive data subscriptions from exchanges like ICE or EEX. However, we can obtain **free, daily, market-reflective prices** by using Exchange Traded Funds (ETFs) and futures tickers that track these underlying commodities.

This document explains the proxy approach, available tickers, and implementation strategy.

---

## Why Use ETF/Futures Proxies?

### The Problem
- **EU Carbon Price (EUA)**: Official prices from ICE/EEX require paid subscriptions
- **TTF Gas Price**: ICE Dutch TTF Natural Gas futures data is behind a paywall
- **Free alternatives** like EEA or World Bank provide emissions data, not market prices

### The Solution
ETFs and ETCs (Exchange Traded Commodities) are publicly traded securities that track commodity prices. Their prices are available via free APIs like **Alpha Vantage** (primary) or Yahoo Finance (fallback), making them excellent proxies for the underlying commodity prices.

### Benefits
1. **Free daily data** via Alpha Vantage (25 requests/day) or yfinance
2. **Liquid markets** with accurate price discovery
3. **Historical data** available for backtesting
4. **Correlation** with underlying commodity is typically >95%
5. **Simple registration** - free API key from Alpha Vantage

---

## Available Tickers

### EU Carbon Price Proxies

| Ticker | Name | What It Tracks | Best For |
|--------|------|----------------|----------|
| **KEUA** | KraneShares European Carbon Allowance Strategy ETF | S&P Carbon Credit EUA Index (pure EU ETS) | **Primary EU carbon proxy** |
| **KRBN** | KraneShares Global Carbon Strategy ETF | Global carbon credits (~70% EUA, 20% CCA, 10% RGGI) | Diversified carbon exposure |
| **FCO2.L** | HANetf SparkChange Physical Carbon EUA ETC | Physical EUA holdings | London-listed alternative |
| **SGS1.DE** | SG ETC ICE EUA Futures | ICE EUA Futures contracts | German-listed alternative |

#### KEUA - Recommended EU Carbon Proxy

**Why KEUA is ideal:**
- **100% EU ETS exposure**: Directly tracks the S&P Carbon Credit EUA Index
- **Pure EUA exposure**: No dilution from other carbon markets
- **High liquidity**: Traded on NYSE
- **Transparent methodology**: Tracks most traded EUA futures contracts

**Correlation with EUA spot price:** ~98%+

```
KEUA tracks the performance of the most traded EUA futures contracts,
rolling monthly to maintain exposure to the carbon price curve.
```

### EU Natural Gas Price Proxies

| Ticker | Name | What It Tracks | Best For |
|--------|------|----------------|----------|
| **TTF=F** | Dutch TTF Gas Futures (Generic) | Front-month TTF futures | **Primary choice** (yfinance) |
| **UNG** | United States Natural Gas Fund | US Henry Hub gas | Fallback/correlation (Alpha Vantage) |
| **BOIL** | ProShares Ultra Bloomberg Natural Gas | 2x leveraged US gas | Alternative proxy |
| **NG=F** | US Natural Gas Futures | Henry Hub benchmark | US gas comparison |

#### TTF=F - Primary Gas Source (yfinance)

**Why TTF=F is our primary choice:**
- **Actual European benchmark**: TTF is THE price reference for European natural gas
- **EUR/MWh units**: No currency conversion needed for EU electricity price models
- **Direct correlation**: Directly reflects European gas market conditions
- **Reliable via yfinance**: Works consistently for futures data

```
TTF (Title Transfer Facility) is the main European natural gas hub,
located in the Netherlands. It's the benchmark for European gas pricing,
similar to how Henry Hub is the US benchmark.
```

#### UNG - Secondary/Fallback Proxy (Alpha Vantage)

**Why UNG is included:**
- **API availability**: Available on Alpha Vantage with API key
- **Correlation analysis**: US and EU gas prices correlate due to LNG trade
- **Fallback option**: If yfinance is unavailable, UNG provides gas price signals

```
Why gas prices (even US) matter for EU electricity prediction:
1. Gas plants often set the marginal electricity price in Europe
2. When renewable generation is low, gas becomes the price-setter
3. Global gas market integration means US/EU prices move together
4. UNG daily changes are a good proxy for TTF directional moves
```

---

## Why These Proxies Matter for Price Prediction

### Causality: Gas/Carbon → Electricity (Not Circular)

A common concern is whether using gas prices to predict electricity prices is circular. **It is not** - the causality is one-directional:

```
Gas Supply/Demand → TTF Gas Price → Marginal Electricity Cost → Day-Ahead Electricity Price
      ↑
   (LNG imports,
    pipelines,
    storage levels,
    heating demand)
```

**Key points:**
1. **Gas market sets prices first**: TTF gas is traded before the electricity day-ahead auction
2. **Power plants are price-takers**: They buy gas at market price, don't set it
3. **Gas demand is diversified**: Power generation is only ~25% of EU gas demand

### Avoiding Data Leakage (Critical for ML Models)

While causality flows from gas to electricity, **timing matters** for prediction:

| Prediction Task | Valid Input | Invalid (Data Leakage) |
|-----------------|-------------|------------------------|
| Tomorrow's electricity price | Today's gas price | Tomorrow's gas price |
| Hour-ahead electricity | Previous hour's gas | Same hour's gas |
| Week-ahead electricity | Current gas futures | Future spot prices |

**Correct feature engineering:**

```python
# WRONG - Data leakage! Uses information not available at prediction time
features['gas_price'] = same_day_ttf_spot  # Don't do this!

# CORRECT - Use lagged values (available at prediction time)
features['gas_price_lag1'] = previous_day_ttf_spot
features['gas_price_lag2'] = two_days_ago_ttf_spot

# CORRECT - Use forward-looking futures (already priced in)
features['gas_futures_m1'] = ttf_month_ahead_contract
features['gas_futures_q1'] = ttf_quarter_ahead_contract

# ALSO VALID - Use percentage changes (relative movements)
features['gas_price_change_1d'] = (ttf_t0 - ttf_t1) / ttf_t1
```

### Recommended Feature Set for Price Prediction

| Feature | Lag/Type | Why Valid |
|---------|----------|-----------|
| `gas_price_lag1` | T-1 spot | Already known at prediction time |
| `gas_futures_m1` | Month-ahead | Forward-looking, priced in expectations |
| `carbon_price_lag1` | T-1 spot | Already known |
| `gas_storage_level` | Current | Fundamental driver, not price |
| `gas_price_volatility_7d` | Rolling | Historical measure |
| `gas_price_trend_30d` | Rolling | Direction indicator |

---

### Carbon Price Impact on Electricity

```
Electricity Price = Fuel Cost + Carbon Cost + Other Costs

For a gas plant:
  Carbon Cost = (Emissions Factor × Carbon Price) / Efficiency

  Example at €80/tonne EUA:
  - Gas plant: ~0.4 tCO2/MWh at 50% efficiency
  - Carbon cost: 0.4 × €80 = €32/MWh added to electricity price
```

**Key insight**: A €10 change in carbon price adds ~€4/MWh to gas-fired electricity costs.

### Gas Price Impact on Electricity

```
When gas plants are marginal (setting the price):
  Electricity Price ≈ (Gas Price / Efficiency) + Carbon Cost

  Example at €30/MWh gas:
  - Gas plant at 50% efficiency
  - Fuel cost: €30 / 0.50 = €60/MWh
  - Plus carbon: €32/MWh
  - Total: ~€92/MWh electricity price
```

**Key insight**: TTF gas price is the primary driver of electricity prices during low-renewable periods.

---

## Implementation Strategy

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    MarketProxyCollector                      │
├─────────────────────────────────────────────────────────────┤
│  Carbon (KEUA):                                              │
│  - Primary: Alpha Vantage API (25 requests/day free)        │
│  - Fallback: yfinance                                        │
├─────────────────────────────────────────────────────────────┤
│  European Gas (TTF=F):                                       │
│  - Primary: yfinance (actual TTF futures, EUR/MWh)          │
│  - This is the key European gas benchmark!                   │
├─────────────────────────────────────────────────────────────┤
│  US Gas Proxy (UNG):                                         │
│  - Primary: Alpha Vantage API                                │
│  - Included for correlation analysis and as fallback         │
├─────────────────────────────────────────────────────────────┤
│  Last Resort: Cache                                         │
│  - Use previous day's data (up to 48 hours old)             │
├─────────────────────────────────────────────────────────────┤
│  Output: market_proxies.json                                │
│  - carbon (KEUA price + lag features)                       │
│  - gas_ttf (TTF futures price + lag features) ← PRIMARY GAS │
│  - gas (UNG price + lag features) ← US proxy                │
│  - metadata with source, timestamp, ticker info             │
└─────────────────────────────────────────────────────────────┘

API Key Setup:
1. Get free key from https://www.alphavantage.co/support/#api-key
2. Set ALPHA_VANTAGE_API_KEY in secrets.ini or environment
   (Only needed for carbon data; TTF works without API key)
```

### Python Implementation

```python
"""
Market Proxy Collector for Carbon and Gas Prices
------------------------------------------------
Uses Alpha Vantage as primary source (reliable) with yfinance fallback.
See collectors/market_proxies.py for full implementation.
"""

import asyncio
import aiohttp
import os

# Alpha Vantage configuration
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

async def fetch_quote(session, symbol):
    """Fetch current quote from Alpha Vantage."""
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': symbol,
        'apikey': API_KEY
    }
    async with session.get(ALPHA_VANTAGE_URL, params=params) as r:
        data = await r.json()
        quote = data.get('Global Quote', {})
        return {
            'ticker': symbol,
            'price': float(quote.get('05. price', 0)),
            'change_pct': float(quote.get('10. change percent', '0%').rstrip('%'))
        }

async def collect_market_proxies():
    """Collect carbon and gas proxy prices."""
    async with aiohttp.ClientSession() as session:
        carbon = await fetch_quote(session, 'KEUA')  # EU Carbon ETF
        gas = await fetch_quote(session, 'UNG')       # US Gas ETF
        return {'carbon': carbon, 'gas': gas}

# Run: asyncio.run(collect_market_proxies())
```

For the full implementation with lag features, caching, and fallbacks, see `collectors/market_proxies.py`.

### Integration with Data Fetcher

Add to `data_fetcher.py`:

```python
from collectors.market_proxies import MarketProxyCollector

# In main():
market_proxy_collector = MarketProxyCollector()
proxy_data = market_proxy_collector.collect()

# Save to file
if proxy_data.get('carbon') or proxy_data.get('gas'):
    full_path = os.path.join(output_path, "market_proxies.json")
    with open(full_path, 'w') as f:
        json.dump(proxy_data, f, indent=2)
    logging.info(f"Saved market proxy data: carbon={proxy_data.get('carbon', {}).get('price', 'N/A')}, gas={proxy_data.get('gas', {}).get('price', 'N/A')}")
```

---

## Price Conversion Notes

### KEUA to EUA Price

KEUA is priced in USD, but EUA trades in EUR:

```python
# Approximate conversion (assuming ~1:1 correlation)
eua_price_eur = keua_price_usd / usd_eur_rate

# Or use KEUA directly as a relative indicator
# (daily changes track EUA changes closely)
```

**Note**: For price prediction models, the relative change in KEUA often matters more than the absolute price. A 5% increase in KEUA indicates a similar increase in EUA prices.

### TTF Price Units

TTF=F is typically quoted in EUR/MWh, which is the standard European gas market unit.

```python
# No conversion needed - directly usable
ttf_price_eur_mwh = ttf_futures_price
```

---

## Historical Data for Backtesting

### Fetching Historical Data

```python
import yfinance as yf
import pandas as pd

# Get 2 years of historical data
keua = yf.Ticker('KEUA')
keua_hist = keua.history(period='2y')

ttf = yf.Ticker('TTF=F')
ttf_hist = ttf.history(period='2y')

# Create combined dataframe
combined = pd.DataFrame({
    'carbon_proxy': keua_hist['Close'],
    'gas_proxy': ttf_hist['Close']
})

# Save for backtesting
combined.to_csv('market_proxy_history.csv')
```

### Expected Data Availability

| Ticker | Historical Data | Update Frequency |
|--------|-----------------|------------------|
| KEUA | Since 2021 (ETF launch) | Daily (market close) |
| KRBN | Since 2020 | Daily |
| TTF=F | Several years | Daily |

---

## Alternative APIs (Freemium)

### Alpha Vantage

**Website**: [alphavantage.co](https://www.alphavantage.co/)

**Free Tier**:
- 25 API requests/day
- 5 requests/minute
- Stocks, ETFs, forex, crypto, commodities

**Relevant Endpoints**:
```python
import requests

API_KEY = 'your_alpha_vantage_key'

# Get ETF quote (KEUA, KRBN)
url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=KEUA&apikey={API_KEY}'
response = requests.get(url)
data = response.json()
price = float(data['Global Quote']['05. price'])

# Get commodity data (limited)
url = f'https://www.alphavantage.co/query?function=NATURAL_GAS&interval=daily&apikey={API_KEY}'
```

**Pros**: Reliable, good documentation, historical data available
**Cons**: Limited to 25 calls/day on free tier, may not have TTF specifically

---

### Trading Economics

**Website**: [tradingeconomics.com](https://tradingeconomics.com/)

**Free Tier**: Web scraping only (no API on free tier)
**Paid API**: Starts at ~$50/month

**Available Data**:
- EU Natural Gas (TTF): [tradingeconomics.com/commodity/eu-natural-gas](https://tradingeconomics.com/commodity/eu-natural-gas)
- EU Carbon Permits: [tradingeconomics.com/commodity/carbon](https://tradingeconomics.com/commodity/carbon)

**Current Prices** (as of search):
- EU Carbon: ~€83.26/tonne
- TTF Gas: ~€27.82/MWh

**Scraping Approach** (for personal use):
```python
import requests
from bs4 import BeautifulSoup

def scrape_trading_economics_carbon():
    url = 'https://tradingeconomics.com/commodity/carbon'
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    # Extract price from page (structure may change)
    # Note: Check terms of service before scraping
    pass
```

**Note**: Web scraping may violate ToS - use at your own risk for personal projects.

---

### Finnhub

**Website**: [finnhub.io](https://finnhub.io/)

**Free Tier**:
- 60 API calls/minute
- Real-time US stock data
- ETFs including KEUA, KRBN

**Example**:
```python
import finnhub

client = finnhub.Client(api_key="your_key")
quote = client.quote('KEUA')
print(f"KEUA Price: ${quote['c']}")  # Current price
```

**Pros**: Generous free tier, real-time data
**Cons**: May not have European/commodity futures

---

### API Comparison Summary

| API | Free Tier | ETF Data | Futures | Reliability | Best For |
|-----|-----------|----------|---------|-------------|----------|
| **Alpha Vantage** | 25/day | Yes | No | ⭐⭐⭐⭐⭐ | **Primary source** |
| **yfinance** | Unlimited* | Yes | Some | ⭐⭐ (often blocked) | Fallback |
| **Finnhub** | 60/min | Yes | No | ⭐⭐⭐⭐ | Alternative |
| **Trading Economics** | Scrape only | Web | Web | ⭐⭐ | Manual backup |

*yfinance frequently blocked since 2024 due to Yahoo Finance tightening access

---

## Fallback Strategy

When yfinance is unavailable (rate limits, API changes), implement fallbacks:

### 1. Cache Previous Data
```python
# Store last successful fetch
CACHE_FILE = 'market_proxy_cache.json'

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return None

def save_cache(data):
    with open(CACHE_FILE, 'w') as f:
        json.dump(data, f)
```

### 2. Alternative Data Sources

- **Investing.com**: Web scraping (requires parsing HTML)
- **Barchart**: 1 free CSV download per day
- **Google Finance**: Basic price data via URL patterns

### 3. Stale Data Handling

```python
def get_price_with_fallback(commodity):
    """Get price with cache fallback for stale data."""
    fresh_data = fetch_from_api(commodity)

    if fresh_data:
        save_cache(fresh_data)
        return fresh_data

    # Use cached data if less than 24 hours old
    cached = load_cache()
    if cached and is_recent(cached['timestamp'], hours=24):
        logging.warning(f"Using cached {commodity} data from {cached['timestamp']}")
        return cached

    logging.error(f"No fresh or cached data available for {commodity}")
    return None
```

---

## Output Format

### market_proxies.json

The output includes **lagged values** and **rolling statistics** to enable proper ML feature engineering without data leakage:

```json
{
  "metadata": {
    "collected_at": "2025-12-01T18:00:00Z",
    "source": "yfinance (Yahoo Finance)",
    "description": "Market proxy prices for carbon and gas"
  },
  "carbon": {
    "ticker": "KEUA",
    "description": "EU Carbon Allowance (EUA) proxy",
    "units": "USD/share (correlates to EUR/tonne EUA)",

    "// CURRENT VALUES (use with caution - check timing for your prediction task)": "",
    "price": 28.50,
    "date": "2025-12-01T00:00:00-05:00",
    "open": 28.20,
    "high": 28.75,
    "low": 27.90,
    "volume": 125000,
    "change_pct": 1.2,

    "// LAGGED VALUES (SAFE for prediction - no data leakage)": "",
    "price_lag1": 28.15,
    "price_lag2": 27.90,
    "price_lag7": 27.20,

    "// ROLLING STATISTICS (SAFE - based on historical data)": "",
    "volatility_7d": 0.85,
    "mean_7d": 28.10,
    "mean_30d": 27.50,
    "trend_7d": "up",

    "// FULL HISTORY (for custom lag calculations)": "",
    "history": {
      "2025-11-01T00:00:00-05:00": 26.50,
      "2025-11-02T00:00:00-05:00": 26.80,
      "...": "..."
    }
  },
  "gas_ttf": {
    "ticker": "TTF=F",
    "name": "Dutch TTF Natural Gas Futures",
    "description": "European gas benchmark (Title Transfer Facility)",
    "units": "EUR/MWh",
    "currency": "EUR",
    "note": "Primary European natural gas price benchmark, traded on ICE",

    "price": 34.60,
    "date": "2026-02-05",
    "change_pct": 3.28,

    "price_lag1": 33.50,
    "price_lag2": 33.20,
    "price_lag7": 36.00,

    "volatility_7d": 1.85,
    "mean_7d": 36.16,
    "mean_30d": 35.80,
    "trend_7d": "down",

    "history": {"...": "..."}
  },
  "gas": {
    "ticker": "UNG",
    "name": "United States Natural Gas Fund",
    "description": "Natural Gas proxy (US benchmark, correlates with EU)",
    "units": "USD/share",
    "note": "US gas ETF used as fallback when TTF unavailable",

    "price": 15.20,
    "date": "2026-02-05",
    "change_pct": -1.5,

    "price_lag1": 15.43,
    "price_lag2": 15.10,
    "price_lag7": 14.80,

    "volatility_7d": 0.65,
    "mean_7d": 15.10,
    "mean_30d": 14.90,
    "trend_7d": "up",

    "history": {"...": "..."}
  }
}
```

### Using Lagged Values in ML Models

```python
import json

with open('data/market_proxies.json') as f:
    proxies = json.load(f)

# CORRECT: Use lagged values for prediction features
features = {
    # TTF Gas features (PRIMARY - no data leakage)
    'ttf_price_lag1': proxies['gas_ttf']['price_lag1'],
    'ttf_price_lag7': proxies['gas_ttf']['price_lag7'],
    'ttf_volatility_7d': proxies['gas_ttf']['volatility_7d'],
    'ttf_trend': 1 if proxies['gas_ttf']['trend_7d'] == 'up' else 0,

    # US Gas proxy (optional - for correlation analysis)
    'us_gas_price_lag1': proxies['gas']['price_lag1'],

    # Carbon features (no data leakage)
    'carbon_price_lag1': proxies['carbon']['price_lag1'],
    'carbon_mean_7d': proxies['carbon']['mean_7d'],
}

# Use these features to predict tomorrow's electricity price
prediction = model.predict([features])
```

---

## Caveats and Limitations

### 1. ETF Tracking Error
ETFs don't perfectly track the underlying commodity due to:
- Management fees (TER ~0.75-0.89%)
- Futures rolling costs
- Bid-ask spreads

**Impact**: Typically <2% annual deviation from spot prices.

### 2. Market Hours
ETF prices only update during trading hours:
- **KEUA/KRBN**: NYSE hours (9:30-16:00 ET)
- **TTF=F**: ICE hours (varies)

**Impact**: Weekend/holiday data will be stale.

### 3. Currency Considerations
- KEUA trades in USD
- EUA spot trades in EUR
- TTF trades in EUR

**Impact**: For precise modeling, consider EUR/USD exchange rate.

### 4. API Reliability
yfinance depends on Yahoo Finance:
- Rate limits may apply
- API may change without notice
- Some tickers may become unavailable

**Impact**: Implement fallback strategies (see above).

---

## References

- [KraneShares KEUA ETF](https://kraneshares.com/keua/)
- [KraneShares KRBN ETF](https://kraneshares.com/etf/krbn/)
- [Yahoo Finance TTF Futures](https://finance.yahoo.com/quote/TTF=F/)
- [ICE Dutch TTF Futures](https://www.ice.com/products/27996665/Dutch-TTF-Natural-Gas-Futures)
- [EU ETS Overview](https://climate.ec.europa.eu/eu-action/eu-emissions-trading-system-eu-ets_en)

---

*Document created: 2025-12-01*
*Last updated: 2026-02-05 (Added TTF=F as primary European gas source via yfinance)*
