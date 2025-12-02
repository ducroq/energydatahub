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
ETFs and ETCs (Exchange Traded Commodities) are publicly traded securities that track commodity prices. Their prices are available via free APIs like Yahoo Finance (yfinance), making them excellent proxies for the underlying commodity prices.

### Benefits
1. **Free daily data** via yfinance or similar APIs
2. **Liquid markets** with accurate price discovery
3. **Historical data** available for backtesting
4. **Correlation** with underlying commodity is typically >95%
5. **No registration required** for basic historical data

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
| **TTF=F** | Dutch TTF Gas Futures (Generic) | Front-month TTF futures | **Primary gas proxy** |
| **TGF25** | TTF Gas January 2025 | Specific contract month | Specific delivery month |
| **NG=F** | US Natural Gas Futures | Henry Hub benchmark | US gas comparison |

#### TTF=F - Recommended Gas Proxy

**Why TTF=F is ideal:**
- **European benchmark**: TTF is THE reference price for European gas
- **Dutch market**: Title Transfer Facility operated by GTS (Gasunie)
- **Price setter**: TTF price often determines EU electricity prices during gas-fired generation
- **Continuous contract**: Generic ticker rolls to front-month automatically

```
The TTF price is critical for electricity price prediction because:
1. Gas plants often set the marginal electricity price
2. When renewable generation is low, gas becomes the price-setter
3. TTF price volatility directly impacts day-ahead electricity prices
```

---

## Why These Proxies Matter for Price Prediction

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
│  Primary Source: yfinance                                    │
│  - KEUA (EU Carbon)                                         │
│  - TTF=F (EU Gas)                                           │
├─────────────────────────────────────────────────────────────┤
│  Fallback Sources (if yfinance fails):                      │
│  - Investing.com scraping                                   │
│  - Barchart historical                                      │
│  - Cache previous day's data                                │
├─────────────────────────────────────────────────────────────┤
│  Output: market_proxies.json                                │
│  - carbon_price_eur (KEUA-derived)                          │
│  - gas_price_eur_mwh (TTF=F)                                │
│  - metadata with source, timestamp, ticker info             │
└─────────────────────────────────────────────────────────────┘
```

### Python Implementation

```python
"""
Market Proxy Collector for Carbon and Gas Prices
------------------------------------------------
Uses ETF/futures tickers as free proxies for commodity prices.
"""

import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import logging

class MarketProxyCollector:
    """
    Collector for carbon and gas market prices via ETF/futures proxies.

    Primary tickers:
    - KEUA: EU Carbon (EUA) proxy
    - TTF=F: Dutch TTF Natural Gas proxy
    """

    TICKERS = {
        'carbon': {
            'primary': 'KEUA',
            'fallback': ['KRBN', 'FCO2.L'],
            'description': 'EU Carbon Allowance (EUA) proxy',
            'units': 'USD/share (correlates to EUR/tonne EUA)'
        },
        'gas': {
            'primary': 'TTF=F',
            'fallback': ['NG=F'],
            'description': 'Dutch TTF Natural Gas proxy',
            'units': 'EUR/MWh'
        }
    }

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _fetch_ticker(self, ticker: str, period: str = '5d') -> Optional[Dict[str, Any]]:
        """Fetch data for a single ticker."""
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period=period)

            if hist.empty:
                self.logger.warning(f"{ticker}: No data returned")
                return None

            latest = hist.iloc[-1]
            return {
                'ticker': ticker,
                'price': float(latest['Close']),
                'date': hist.index[-1].isoformat(),
                'open': float(latest['Open']),
                'high': float(latest['High']),
                'low': float(latest['Low']),
                'volume': int(latest['Volume']) if 'Volume' in latest else 0,
                'change_pct': float((latest['Close'] - hist.iloc[-2]['Close']) / hist.iloc[-2]['Close'] * 100) if len(hist) > 1 else 0
            }
        except Exception as e:
            self.logger.error(f"{ticker}: Error fetching - {e}")
            return None

    def collect(self) -> Dict[str, Any]:
        """
        Collect carbon and gas proxy prices.

        Returns:
            Dict with structure:
            {
                'carbon': {
                    'ticker': 'KEUA',
                    'price': 28.50,
                    'date': '2025-12-01',
                    ...
                },
                'gas': {
                    'ticker': 'TTF=F',
                    'price': 45.20,
                    ...
                },
                'metadata': {...}
            }
        """
        results = {
            'metadata': {
                'collected_at': datetime.utcnow().isoformat(),
                'source': 'yfinance (Yahoo Finance)',
                'description': 'Market proxy prices for carbon and gas'
            }
        }

        for commodity, config in self.TICKERS.items():
            # Try primary ticker first
            data = self._fetch_ticker(config['primary'])

            # Try fallbacks if primary fails
            if data is None:
                for fallback in config['fallback']:
                    self.logger.info(f"{commodity}: Trying fallback {fallback}")
                    data = self._fetch_ticker(fallback)
                    if data:
                        break

            if data:
                data['description'] = config['description']
                data['units'] = config['units']
                results[commodity] = data
            else:
                self.logger.error(f"{commodity}: All sources failed")
                results[commodity] = {
                    'error': 'Data unavailable',
                    'description': config['description']
                }

        return results


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = MarketProxyCollector()
    data = collector.collect()

    print("Market Proxy Data:")
    for key, value in data.items():
        if key != 'metadata':
            print(f"\n{key.upper()}:")
            if 'price' in value:
                print(f"  Price: {value['price']:.2f}")
                print(f"  Ticker: {value['ticker']}")
                print(f"  Date: {value['date']}")
            else:
                print(f"  Error: {value.get('error', 'Unknown')}")
```

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

| API | Free Tier | ETF Data | Futures | Commodities | Best For |
|-----|-----------|----------|---------|-------------|----------|
| **yfinance** | Unlimited* | Yes | Some | Limited | Primary source |
| **Alpha Vantage** | 25/day | Yes | No | Some | Backup for ETFs |
| **Finnhub** | 60/min | Yes | No | No | Real-time ETF |
| **Trading Economics** | Scrape only | Web | Web | Web | Manual backup |

*yfinance has unofficial rate limits, may fail during high traffic

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

```json
{
  "metadata": {
    "collected_at": "2025-12-01T18:00:00Z",
    "source": "yfinance (Yahoo Finance)",
    "description": "Market proxy prices for carbon and gas"
  },
  "carbon": {
    "ticker": "KEUA",
    "price": 28.50,
    "date": "2025-12-01T00:00:00-05:00",
    "open": 28.20,
    "high": 28.75,
    "low": 27.90,
    "volume": 125000,
    "change_pct": 1.2,
    "description": "EU Carbon Allowance (EUA) proxy",
    "units": "USD/share (correlates to EUR/tonne EUA)"
  },
  "gas": {
    "ticker": "TTF=F",
    "price": 45.20,
    "date": "2025-12-01T00:00:00+00:00",
    "open": 46.10,
    "high": 46.50,
    "low": 44.80,
    "volume": 50000,
    "change_pct": -2.0,
    "description": "Dutch TTF Natural Gas proxy",
    "units": "EUR/MWh"
  }
}
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
*Last updated: 2025-12-01*
