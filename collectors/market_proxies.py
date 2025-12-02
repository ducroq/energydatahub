"""
Market Proxy Collector for Carbon and Gas Prices
-------------------------------------------------
Uses ETF/futures tickers as free proxies for commodity prices.

File: collectors/market_proxies.py
Created: 2025-12-01
Updated: 2025-12-02 - Pivoted to Alpha Vantage as primary source

Description:
    Fetches carbon and gas prices using publicly traded ETF/futures proxies.

    Data Source Priority:
    1. Alpha Vantage (primary) - Reliable, 25 requests/day free tier
    2. yfinance (fallback) - Free but unreliable, often blocked
    3. Cache (last resort) - Use previous day's data

    Primary tickers:
    - KEUA: KraneShares European Carbon Allowance ETF (tracks EUA)
    - TTF is not directly available, use gas-related ETFs as proxy

    See docs/CARBON_GAS_PRICE_PROXIES.md for detailed documentation.

Usage:
    from collectors.market_proxies import MarketProxyCollector

    # Requires ALPHA_VANTAGE_API_KEY in environment or secrets.ini
    collector = MarketProxyCollector(api_key="your_key")
    data = await collector.collect()
"""

import asyncio
import aiohttp
import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig

# Try importing yfinance as fallback
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False


class MarketProxyCollector(BaseCollector):
    """
    Collector for carbon and gas market prices via ETF/futures proxies.

    Uses Alpha Vantage as primary source (reliable, free tier).
    Falls back to yfinance if Alpha Vantage fails.
    """

    # Alpha Vantage API configuration
    ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

    # Ticker configurations
    CARBON_CONFIG = {
        'symbol': 'KEUA',
        'name': 'KraneShares European Carbon Allowance ETF',
        'description': 'EU Carbon Allowance (EUA) proxy',
        'units': 'USD/share (correlates to EUR/tonne EUA)',
        'fallback_symbols': ['KRBN'],  # Global carbon ETF
    }

    GAS_CONFIG = {
        'symbol': 'UNG',  # US Natural Gas Fund - more reliable than TTF=F
        'name': 'United States Natural Gas Fund',
        'description': 'Natural Gas proxy (US benchmark, correlates with EU)',
        'units': 'USD/share',
        'fallback_symbols': ['BOIL', 'FCG'],
        'note': 'TTF futures not available via Alpha Vantage; UNG tracks US gas which correlates with EU prices'
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache_dir: Optional[str] = None,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize Market Proxy collector.

        Args:
            api_key: Alpha Vantage API key (free from alphavantage.co)
                    If not provided, checks ALPHA_VANTAGE_API_KEY env var
            cache_dir: Directory for caching data (fallback when API fails)
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="MarketProxyCollector",
            data_type="market_proxies",
            source="Alpha Vantage API",
            units="Various (see individual commodities)",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )

        # Get API key from parameter, environment, or None
        self.api_key = api_key or os.getenv('ALPHA_VANTAGE_API_KEY')

        if not self.api_key:
            self.logger.warning(
                "No Alpha Vantage API key provided. "
                "Get a free key at: https://www.alphavantage.co/support/#api-key"
            )

        self.cache_dir = cache_dir
        self._cache_file = os.path.join(cache_dir, 'market_proxy_cache.json') if cache_dir else None

    async def _fetch_alpha_vantage_quote(
        self,
        session: aiohttp.ClientSession,
        symbol: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch current quote from Alpha Vantage GLOBAL_QUOTE endpoint.

        Args:
            session: aiohttp session
            symbol: Stock/ETF ticker symbol

        Returns:
            Dict with price data or None if failed
        """
        if not self.api_key:
            return None

        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol,
            'apikey': self.api_key
        }

        try:
            async with session.get(self.ALPHA_VANTAGE_BASE_URL, params=params) as response:
                if response.status != 200:
                    self.logger.debug(f"Alpha Vantage {symbol}: HTTP {response.status}")
                    return None

                data = await response.json()

                # Check for API errors
                if 'Error Message' in data:
                    self.logger.debug(f"Alpha Vantage {symbol}: {data['Error Message']}")
                    return None

                if 'Note' in data:  # Rate limit warning
                    self.logger.warning(f"Alpha Vantage rate limit: {data['Note']}")
                    return None

                quote = data.get('Global Quote', {})
                if not quote:
                    self.logger.debug(f"Alpha Vantage {symbol}: No quote data")
                    return None

                # Parse the response
                price = float(quote.get('05. price', 0))
                if price == 0:
                    return None

                prev_close = float(quote.get('08. previous close', price))
                change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

                return {
                    'ticker': symbol,
                    'price': round(price, 2),
                    'date': quote.get('07. latest trading day', ''),
                    'open': round(float(quote.get('02. open', 0)), 2),
                    'high': round(float(quote.get('03. high', 0)), 2),
                    'low': round(float(quote.get('04. low', 0)), 2),
                    'volume': int(quote.get('06. volume', 0)),
                    'prev_close': round(prev_close, 2),
                    'change': round(float(quote.get('09. change', 0)), 2),
                    'change_pct': round(change_pct, 2),
                    'source': 'alpha_vantage'
                }

        except Exception as e:
            self.logger.debug(f"Alpha Vantage {symbol}: Error - {e}")
            return None

    async def _fetch_alpha_vantage_history(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        days: int = 30
    ) -> Optional[Dict[str, float]]:
        """
        Fetch historical daily prices from Alpha Vantage.

        Args:
            session: aiohttp session
            symbol: Stock/ETF ticker symbol
            days: Number of days of history to fetch

        Returns:
            Dict mapping date strings to closing prices
        """
        if not self.api_key:
            return None

        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'compact'  # Last 100 data points
        }

        try:
            async with session.get(self.ALPHA_VANTAGE_BASE_URL, params=params) as response:
                if response.status != 200:
                    return None

                data = await response.json()

                if 'Error Message' in data or 'Note' in data:
                    return None

                time_series = data.get('Time Series (Daily)', {})
                if not time_series:
                    return None

                # Get last N days
                history = {}
                for date_str, values in sorted(time_series.items(), reverse=True)[:days]:
                    history[date_str] = round(float(values.get('4. close', 0)), 2)

                return history

        except Exception as e:
            self.logger.debug(f"Alpha Vantage history {symbol}: Error - {e}")
            return None

    def _fetch_yfinance_sync(self, symbol: str, period: str = '30d') -> Optional[Dict[str, Any]]:
        """Fallback: Fetch from yfinance (synchronous)."""
        if not YFINANCE_AVAILABLE:
            return None

        try:
            t = yf.Ticker(symbol)
            hist = t.history(period=period)

            if hist.empty:
                return None

            latest = hist.iloc[-1]
            prev_close = hist.iloc[-2]['Close'] if len(hist) > 1 else latest['Close']

            # Build history dict
            history = {
                d.strftime('%Y-%m-%d'): round(float(p), 2)
                for d, p in zip(hist.index, hist['Close'])
            }

            return {
                'ticker': symbol,
                'price': round(float(latest['Close']), 2),
                'date': hist.index[-1].strftime('%Y-%m-%d'),
                'open': round(float(latest['Open']), 2),
                'high': round(float(latest['High']), 2),
                'low': round(float(latest['Low']), 2),
                'volume': int(latest['Volume']) if latest['Volume'] > 0 else None,
                'prev_close': round(float(prev_close), 2),
                'change_pct': round((latest['Close'] - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0,
                'history': history,
                'source': 'yfinance'
            }
        except Exception as e:
            self.logger.debug(f"yfinance {symbol}: Error - {e}")
            return None

    def _calculate_lag_features(self, history: Dict[str, float], current_price: float) -> Dict[str, Any]:
        """
        Calculate lagged features from historical data.

        Args:
            history: Dict of date -> price
            current_price: Current price

        Returns:
            Dict with lag features
        """
        if not history:
            return {}

        # Sort by date descending
        sorted_dates = sorted(history.keys(), reverse=True)
        prices = [history[d] for d in sorted_dates]

        features = {}

        # Lagged values (T-1, T-2, T-7)
        if len(prices) >= 2:
            features['price_lag1'] = prices[1]
        if len(prices) >= 3:
            features['price_lag2'] = prices[2]
        if len(prices) >= 8:
            features['price_lag7'] = prices[7]

        # Rolling statistics
        if len(prices) >= 7:
            last_7 = prices[:7]
            features['mean_7d'] = round(sum(last_7) / len(last_7), 2)
            features['volatility_7d'] = round(
                (sum((p - features['mean_7d'])**2 for p in last_7) / len(last_7))**0.5, 2
            )
            features['trend_7d'] = 'up' if current_price > prices[6] else 'down'

        if len(prices) >= 30:
            last_30 = prices[:30]
            features['mean_30d'] = round(sum(last_30) / len(last_30), 2)

        return features

    async def _fetch_commodity(
        self,
        session: aiohttp.ClientSession,
        name: str,
        config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a commodity using Alpha Vantage with yfinance fallback.

        Args:
            session: aiohttp session
            name: Commodity name (e.g., 'carbon', 'gas')
            config: Ticker configuration dict

        Returns:
            Dict with price data and metadata
        """
        symbol = config['symbol']

        # Try Alpha Vantage first
        self.logger.debug(f"Fetching {name} via Alpha Vantage ({symbol})")
        quote = await self._fetch_alpha_vantage_quote(session, symbol)
        history = None

        if quote:
            # Fetch history for lag features (separate API call)
            history = await self._fetch_alpha_vantage_history(session, symbol)

        # Try fallbacks if primary fails
        if not quote:
            for fallback in config.get('fallback_symbols', []):
                self.logger.info(f"{name}: Trying fallback {fallback}")
                quote = await self._fetch_alpha_vantage_quote(session, fallback)
                if quote:
                    history = await self._fetch_alpha_vantage_history(session, fallback)
                    break

        # Try yfinance as last resort
        if not quote and YFINANCE_AVAILABLE:
            self.logger.info(f"{name}: Trying yfinance fallback")
            loop = asyncio.get_running_loop()
            quote = await loop.run_in_executor(None, self._fetch_yfinance_sync, symbol)
            if quote:
                history = quote.pop('history', None)

        if not quote:
            return None

        # Add metadata
        quote['description'] = config['description']
        quote['units'] = config['units']
        quote['name'] = config['name']
        if 'note' in config:
            quote['note'] = config['note']

        # Calculate lag features
        if history:
            lag_features = self._calculate_lag_features(history, quote['price'])
            quote.update(lag_features)
            quote['history'] = history

        return quote

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch carbon and gas proxy prices.

        Note: start_time/end_time are ignored for this collector
        as we always fetch the latest available market data.

        Returns:
            Dict with 'carbon' and 'gas' price data
        """
        self.logger.debug("Fetching market proxy data via Alpha Vantage")

        results = {}

        async with aiohttp.ClientSession() as session:
            # Fetch carbon and gas
            carbon_data = await self._fetch_commodity(session, 'carbon', self.CARBON_CONFIG)

            # Small delay to respect rate limits
            await asyncio.sleep(0.5)

            gas_data = await self._fetch_commodity(session, 'gas', self.GAS_CONFIG)

        if carbon_data:
            results['carbon'] = carbon_data
            self.logger.info(f"Carbon proxy: {carbon_data['ticker']} = ${carbon_data['price']} ({carbon_data.get('source', 'unknown')})")
        else:
            self.logger.warning("Carbon proxy: No data available")

        if gas_data:
            results['gas'] = gas_data
            self.logger.info(f"Gas proxy: {gas_data['ticker']} = ${gas_data['price']} ({gas_data.get('source', 'unknown')})")
        else:
            self.logger.warning("Gas proxy: No data available")

        # Try to use cache if both failed
        if not results and self._cache_file:
            results = self._load_cache()
            if results:
                self.logger.warning("Using cached market proxy data")
                results['from_cache'] = True

        if not results:
            raise ValueError("No market proxy data available from any source")

        # Save to cache for future fallback
        if self._cache_file and results and not results.get('from_cache'):
            self._save_cache(results)

        return results

    def _parse_response(
        self,
        raw_data: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Parse raw data to standardized format."""
        return raw_data

    def _normalize_timestamps(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Timestamps are already normalized."""
        return data

    def _validate_data(
        self,
        data: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """Validate market proxy data."""
        warnings = []

        if not data:
            warnings.append("No market proxy data available")
            return False, warnings

        if 'carbon' not in data:
            warnings.append("Carbon price proxy unavailable")

        if 'gas' not in data:
            warnings.append("Gas price proxy unavailable")

        return len([w for w in warnings if 'unavailable' in w]) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get metadata for market proxy dataset."""
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'carbon_symbol': self.CARBON_CONFIG['symbol'],
            'gas_symbol': self.GAS_CONFIG['symbol'],
            'primary_source': 'Alpha Vantage API',
            'fallback_source': 'yfinance' if YFINANCE_AVAILABLE else None,
            'api_key_configured': bool(self.api_key),
            'description': 'Market proxy prices for EU carbon (EUA) and natural gas',
            'usage_notes': [
                'KEUA tracks EU ETS carbon allowance prices',
                'UNG tracks US natural gas (correlates with EU gas)',
                'Use lagged values (price_lag1, price_lag7) to avoid data leakage',
                'Prices update daily on market trading days',
                'See docs/CARBON_GAS_PRICE_PROXIES.md for details'
            ]
        })

        return metadata

    def _load_cache(self) -> Optional[Dict[str, Any]]:
        """Load cached data if available and recent."""
        if not self._cache_file or not os.path.exists(self._cache_file):
            return None

        try:
            with open(self._cache_file) as f:
                cached = json.load(f)

            cache_time = datetime.fromisoformat(cached.get('cached_at', '2000-01-01'))
            if (datetime.now() - cache_time).total_seconds() < 48 * 3600:
                return cached.get('data')
        except Exception as e:
            self.logger.debug(f"Cache load error: {e}")

        return None

    def _save_cache(self, data: Dict[str, Any]) -> None:
        """Save data to cache."""
        if not self._cache_file:
            return

        try:
            os.makedirs(os.path.dirname(self._cache_file), exist_ok=True)
            with open(self._cache_file, 'w') as f:
                json.dump({
                    'cached_at': datetime.now().isoformat(),
                    'data': data
                }, f, indent=2)
        except Exception as e:
            self.logger.debug(f"Cache save error: {e}")


# Example usage
async def main():
    """Example usage of MarketProxyCollector."""
    import os
    from zoneinfo import ZoneInfo

    logging.basicConfig(level=logging.INFO)

    # Get API key from environment
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')

    if not api_key:
        print("Set ALPHA_VANTAGE_API_KEY environment variable")
        print("Get a free key at: https://www.alphavantage.co/support/#api-key")
        return

    collector = MarketProxyCollector(api_key=api_key)

    now = datetime.now(ZoneInfo('Europe/Amsterdam'))
    dataset = await collector.collect(now, now)

    if dataset:
        print("\nMarket Proxy Data:")
        print("=" * 60)

        for commodity in ['carbon', 'gas']:
            if commodity in dataset.data:
                d = dataset.data[commodity]
                print(f"\n{commodity.upper()} ({d.get('name', 'Unknown')}):")
                print(f"  Ticker: {d.get('ticker')}")
                print(f"  Price: ${d.get('price')}")
                print(f"  Change: {d.get('change_pct', 0):+.2f}%")
                print(f"  Source: {d.get('source', 'unknown')}")
                if 'price_lag1' in d:
                    print(f"  Lag-1: ${d.get('price_lag1')}")
                if 'trend_7d' in d:
                    print(f"  7d Trend: {d.get('trend_7d')}")
            else:
                print(f"\n{commodity.upper()}: Not available")

        print(f"\nMetadata:")
        print(f"  Collected: {dataset.metadata.get('collection_timestamp')}")
        print(f"  Source: {dataset.metadata.get('primary_source')}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
