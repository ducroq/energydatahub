"""
Market Proxy Collector for Carbon and Gas Prices
-------------------------------------------------
Uses ETF/futures tickers as free proxies for commodity prices.

File: collectors/market_proxies.py
Created: 2025-12-01
Author: Energy Data Hub Project

Description:
    Fetches carbon and gas prices using publicly traded ETF/futures proxies.
    This approach provides free, daily, market-reflective prices without
    requiring expensive exchange data subscriptions.

    Primary tickers:
    - KEUA: KraneShares European Carbon Allowance ETF (tracks EUA)
    - TTF=F: Dutch TTF Natural Gas Futures

    See docs/CARBON_GAS_PRICE_PROXIES.md for detailed documentation.

Usage:
    from collectors.market_proxies import MarketProxyCollector

    collector = MarketProxyCollector()
    data = await collector.collect()

    if data:
        print(f"Carbon: {data.data.get('carbon', {}).get('price')}")
        print(f"Gas: {data.data.get('gas', {}).get('price')}")
"""

import asyncio
import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List
from functools import partial

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig


class MarketProxyCollector(BaseCollector):
    """
    Collector for carbon and gas market prices via ETF/futures proxies.

    Uses yfinance to fetch publicly traded ETF/futures prices that
    track EU carbon (EUA) and natural gas (TTF) commodity prices.
    """

    # Ticker configurations
    CARBON_TICKERS = {
        'primary': 'KEUA',  # KraneShares European Carbon Allowance ETF
        'fallbacks': ['KRBN'],  # Global carbon ETF (70% EUA)
        'description': 'EU Carbon Allowance (EUA) proxy',
        'units': 'USD/share (correlates to EUR/tonne EUA)',
        'correlation_note': 'KEUA tracks S&P Carbon Credit EUA Index, ~98% correlation with EUA spot'
    }

    GAS_TICKERS = {
        'primary': 'TTF=F',  # Dutch TTF Natural Gas Futures
        'fallbacks': ['NG=F'],  # US Natural Gas (Henry Hub) as last resort
        'description': 'Dutch TTF Natural Gas proxy',
        'units': 'EUR/MWh',
        'correlation_note': 'TTF=F is the European benchmark for natural gas'
    }

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize Market Proxy collector.

        Args:
            cache_dir: Directory for caching data (fallback when API fails)
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="MarketProxyCollector",
            data_type="market_proxies",
            source="Yahoo Finance (yfinance)",
            units="Various (see individual commodities)",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )

        self.cache_dir = cache_dir
        self._cache_file = os.path.join(cache_dir, 'market_proxy_cache.json') if cache_dir else None

        if not YFINANCE_AVAILABLE:
            self.logger.warning("yfinance not installed - market proxy collection will fail")

    def _fetch_ticker_sync(self, ticker: str, period: str = '5d') -> Optional[Dict[str, Any]]:
        """
        Fetch data for a single ticker (synchronous).

        Args:
            ticker: Yahoo Finance ticker symbol
            period: Data period (e.g., '5d', '1mo', '1y')

        Returns:
            Dict with price data or None if failed
        """
        if not YFINANCE_AVAILABLE:
            return None

        try:
            t = yf.Ticker(ticker)
            hist = t.history(period=period)

            if hist.empty:
                self.logger.debug(f"{ticker}: No data returned")
                return None

            latest = hist.iloc[-1]
            prev_close = hist.iloc[-2]['Close'] if len(hist) > 1 else latest['Close']

            return {
                'ticker': ticker,
                'price': round(float(latest['Close']), 2),
                'date': hist.index[-1].isoformat(),
                'open': round(float(latest['Open']), 2),
                'high': round(float(latest['High']), 2),
                'low': round(float(latest['Low']), 2),
                'volume': int(latest['Volume']) if 'Volume' in latest and latest['Volume'] > 0 else None,
                'change_pct': round((latest['Close'] - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0,
                'prev_close': round(float(prev_close), 2)
            }
        except Exception as e:
            self.logger.debug(f"{ticker}: Error fetching - {e}")
            return None

    async def _fetch_commodity(
        self,
        name: str,
        config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a commodity using primary ticker with fallbacks.

        Args:
            name: Commodity name (e.g., 'carbon', 'gas')
            config: Ticker configuration dict

        Returns:
            Dict with price data and metadata
        """
        loop = asyncio.get_running_loop()

        # Try primary ticker
        self.logger.debug(f"Fetching {name} via {config['primary']}")
        data = await loop.run_in_executor(
            None,
            partial(self._fetch_ticker_sync, config['primary'])
        )

        if data:
            data['description'] = config['description']
            data['units'] = config['units']
            data['correlation_note'] = config.get('correlation_note', '')
            return data

        # Try fallbacks
        for fallback in config.get('fallbacks', []):
            self.logger.info(f"{name}: Trying fallback {fallback}")
            data = await loop.run_in_executor(
                None,
                partial(self._fetch_ticker_sync, fallback)
            )

            if data:
                data['description'] = config['description']
                data['units'] = config['units']
                data['is_fallback'] = True
                data['fallback_note'] = f"Primary {config['primary']} unavailable, using {fallback}"
                return data

        return None

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
        self.logger.debug("Fetching market proxy data")

        if not YFINANCE_AVAILABLE:
            raise ImportError("yfinance package not installed. Run: pip install yfinance")

        results = {}

        # Fetch carbon and gas in parallel
        carbon_task = self._fetch_commodity('carbon', self.CARBON_TICKERS)
        gas_task = self._fetch_commodity('gas', self.GAS_TICKERS)

        carbon_data, gas_data = await asyncio.gather(carbon_task, gas_task)

        if carbon_data:
            results['carbon'] = carbon_data
            self.logger.info(f"Carbon proxy: {carbon_data['ticker']} = {carbon_data['price']}")
        else:
            self.logger.warning("Carbon proxy: No data available")

        if gas_data:
            results['gas'] = gas_data
            self.logger.info(f"Gas proxy: {gas_data['ticker']} = {gas_data['price']}")
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
        """
        Parse raw data to standardized format.

        For this collector, data is already in the desired format.
        """
        return raw_data

    def _normalize_timestamps(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Timestamps are already normalized from yfinance."""
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

        # Check for stale data
        for commodity in ['carbon', 'gas']:
            if commodity in data and 'date' in data[commodity]:
                try:
                    data_date = datetime.fromisoformat(data[commodity]['date'].replace('Z', '+00:00'))
                    age_hours = (datetime.now(data_date.tzinfo) - data_date).total_seconds() / 3600
                    if age_hours > 72:  # More than 3 days old
                        warnings.append(f"{commodity} data is {age_hours:.0f} hours old")
                except Exception:
                    pass

        return len([w for w in warnings if 'unavailable' in w]) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get metadata for market proxy dataset."""
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'carbon_ticker_primary': self.CARBON_TICKERS['primary'],
            'gas_ticker_primary': self.GAS_TICKERS['primary'],
            'source_api': 'Yahoo Finance (yfinance)',
            'description': 'Market proxy prices for EU carbon (EUA) and natural gas (TTF)',
            'usage_notes': [
                'KEUA tracks EU ETS carbon allowance prices',
                'TTF=F tracks Dutch TTF natural gas futures',
                'Prices update daily on market trading days',
                'Use for relative price changes rather than absolute values',
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

            # Check if cache is less than 48 hours old
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
    from datetime import datetime
    from zoneinfo import ZoneInfo

    logging.basicConfig(level=logging.INFO)

    collector = MarketProxyCollector()

    # Time range doesn't matter for this collector (always gets latest)
    now = datetime.now(ZoneInfo('Europe/Amsterdam'))
    dataset = await collector.collect(now, now)

    if dataset:
        print("\nMarket Proxy Data:")
        print("=" * 50)

        for commodity in ['carbon', 'gas']:
            if commodity in dataset.data:
                d = dataset.data[commodity]
                print(f"\n{commodity.upper()}:")
                print(f"  Ticker: {d.get('ticker')}")
                print(f"  Price: {d.get('price')}")
                print(f"  Date: {d.get('date')}")
                print(f"  Change: {d.get('change_pct', 0):+.2f}%")
                print(f"  Units: {d.get('units')}")
            else:
                print(f"\n{commodity.upper()}: Not available")

        print("\nMetadata:")
        print(f"  Collected: {dataset.metadata.get('collection_timestamp')}")
        print(f"  Source: {dataset.metadata.get('source_api')}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
