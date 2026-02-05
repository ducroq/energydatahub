"""
Market Proxy Collector for Carbon and Gas Prices
-------------------------------------------------
Uses ETF/futures tickers for commodity prices relevant to electricity pricing.

File: collectors/market_proxies.py
Created: 2025-12-01
Updated: 2025-12-02 - Pivoted to Alpha Vantage as primary source
Updated: 2026-02-05 - Added TTF (Dutch gas) via yfinance

Description:
    Fetches carbon and gas prices using publicly traded ETF/futures.

    Data Sources:
    - Carbon (KEUA): Alpha Vantage (primary), yfinance (fallback)
    - TTF gas (TTF=F): yfinance (European gas benchmark, EUR/MWh)
    - US gas proxy (UNG): Alpha Vantage (for correlation analysis)

    Primary tickers:
    - KEUA: KraneShares European Carbon Allowance ETF (tracks EUA)
    - TTF=F: Dutch TTF Natural Gas Futures (European benchmark)
    - UNG: US Natural Gas Fund (fallback/correlation)

    See docs/CARBON_GAS_PRICE_PROXIES.md for detailed documentation.

Usage:
    from collectors.market_proxies import MarketProxyCollector

    # Requires ALPHA_VANTAGE_API_KEY for carbon data
    collector = MarketProxyCollector(api_key="your_key")
    data = await collector.collect()
    # data contains: carbon, gas_ttf, gas (US proxy)
"""

import asyncio
import aiohttp
import hashlib
import hmac
import logging
import json
import os
import stat
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List
from zoneinfo import ZoneInfo

import holidays

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig

# Try importing yfinance as fallback
try:
    import yfinance as yf
    import pandas as pd
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    pd = None  # pandas not available without yfinance


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
        'symbol': 'UNG',  # US Natural Gas Fund - fallback proxy
        'name': 'United States Natural Gas Fund',
        'description': 'Natural Gas proxy (US benchmark, correlates with EU)',
        'units': 'USD/share',
        'fallback_symbols': ['BOIL', 'FCG'],
        'note': 'US gas ETF used as fallback when TTF unavailable'
    }

    # TTF (Title Transfer Facility) - Dutch/European gas benchmark
    TTF_CONFIG = {
        'symbol': 'TTF=F',  # TTF futures on Yahoo Finance
        'name': 'Dutch TTF Natural Gas Futures',
        'description': 'European gas benchmark (Title Transfer Facility)',
        'units': 'EUR/MWh',
        'fallback_symbols': [],  # No direct fallbacks, will use GAS_CONFIG as proxy
        'note': 'Primary European natural gas price benchmark, traded on ICE',
        'source_priority': 'yfinance'  # yfinance is primary for TTF (not on Alpha Vantage)
    }

    # Timeout and rate limiting constants
    HTTP_TIMEOUT_SECONDS = 30.0
    YFINANCE_TIMEOUT_SECONDS = 30.0
    API_RATE_LIMIT_DELAY = 0.5  # Seconds between API calls

    # Cache settings
    CACHE_EXPIRATION_SECONDS = 48 * 3600  # 48 hours
    # Cache integrity key (derived from machine-specific data for portability)
    _CACHE_HMAC_KEY = hashlib.sha256(
        f"market_proxy_cache_{os.getenv('COMPUTERNAME', os.getenv('HOSTNAME', 'default'))}".encode()
    ).digest()

    # TTF price validation thresholds (EUR/MWh)
    TTF_CRISIS_THRESHOLD = 300  # Warn above this (crisis-level pricing)
    TTF_SANITY_THRESHOLD = 1000  # Reject above this (data error)

    # Market holidays cache (Netherlands + common EU market holidays)
    _market_holidays = holidays.Netherlands()

    # Expected API response schemas for validation
    ALPHA_VANTAGE_QUOTE_SCHEMA = {
        'Global Quote': {
            '05. price': (str, float, int),  # Required: price value
        }
    }

    def _validate_api_response(self, data: Any, schema: Dict[str, Any], path: str = '') -> tuple[bool, List[str]]:
        """
        Validate API response matches expected schema structure.

        Args:
            data: The data to validate
            schema: Expected schema (dict with type tuples as values)
            path: Current path in nested structure (for error messages)

        Returns:
            (is_valid, list of error messages)
        """
        errors = []

        if not isinstance(data, dict):
            errors.append(f"{path or 'root'}: Expected dict, got {type(data).__name__}")
            return False, errors

        for key, expected in schema.items():
            full_path = f"{path}.{key}" if path else key

            if key not in data:
                # Check if this is a required field (no default available)
                if isinstance(expected, dict):
                    errors.append(f"{full_path}: Required key missing")
                # Optional fields with type tuples are not flagged as missing
                continue

            value = data[key]

            # Nested dict validation
            if isinstance(expected, dict):
                if not isinstance(value, dict):
                    errors.append(f"{full_path}: Expected dict, got {type(value).__name__}")
                else:
                    _, nested_errors = self._validate_api_response(value, expected, full_path)
                    errors.extend(nested_errors)
            # Type validation (tuple of acceptable types)
            elif isinstance(expected, tuple):
                if not isinstance(value, expected) and value is not None:
                    # Try type coercion for str->float
                    if str in expected and float in expected:
                        try:
                            float(value)
                        except (ValueError, TypeError):
                            errors.append(f"{full_path}: Cannot convert {type(value).__name__} to expected types")
                    else:
                        errors.append(f"{full_path}: Got {type(value).__name__}, expected {expected}")

        return len(errors) == 0, errors

    def _is_near_market_holiday(self, check_date: datetime) -> bool:
        """Check if date is near a market holiday (within 2 days)."""
        for offset in range(1, 3):
            past_date = (check_date - timedelta(days=offset)).date()
            if past_date in self._market_holidays:
                return True
        return False

    def _check_data_freshness(self, date_str: str, symbol: str) -> None:
        """
        Check and log data freshness warnings.

        Args:
            date_str: Date string in YYYY-MM-DD format
            symbol: Ticker symbol for logging
        """
        if not date_str:
            return

        try:
            data_date = datetime.strptime(date_str, '%Y-%m-%d').replace(
                tzinfo=ZoneInfo('Europe/Amsterdam')
            )
            today = datetime.now(ZoneInfo('Europe/Amsterdam'))
            days_old = (today - data_date).days

            threshold = self._get_staleness_threshold(today)

            if days_old > threshold:
                self.logger.warning(f"{symbol}: Data is {days_old} days old (from {date_str})")
            elif days_old > 1:
                self.logger.debug(f"{symbol}: Data is {days_old} days old (weekend/holiday expected)")
        except ValueError as e:
            self.logger.debug(f"{symbol}: Could not parse date: {date_str} - {e}")

    def _get_staleness_threshold(self, today: datetime) -> int:
        """
        Get appropriate staleness threshold based on day of week and holidays.

        Returns number of days before data is considered stale.
        """
        is_weekend_or_monday = today.weekday() in [0, 5, 6]
        is_near_holiday = self._is_near_market_holiday(today)

        if is_near_holiday:
            return 7  # Allow up to 7 days around holidays
        elif is_weekend_or_monday:
            return 5  # Allow up to 5 days for weekends
        else:
            return 3  # Normal weekday threshold

    def _log_fetch_error(
        self,
        source: str,
        symbol: str,
        error: Exception,
        level: str = 'debug'
    ) -> None:
        """Log fetch errors consistently across all methods."""
        error_type = type(error).__name__
        error_msg = str(error) if str(error) else repr(error)
        log_func = getattr(self.logger, level)
        log_func(f"{source} ({symbol}): {error_type}: {error_msg}")

    def _validate_ttf_price(self, price: float) -> tuple[bool, List[str]]:
        """
        Validate TTF price is within reasonable bounds.

        Args:
            price: Price in EUR/MWh

        Returns:
            (is_valid, list of warning/error messages)
        """
        messages = []
        is_valid = True

        if price < 0:
            messages.append(f"Negative price {price} EUR/MWh (rejected)")
            is_valid = False
        elif price <= 0:
            messages.append(f"Zero price {price} (must be positive)")
            is_valid = False
        elif price > self.TTF_SANITY_THRESHOLD:
            messages.append(f"Price {price} EUR/MWh exceeds sanity threshold")
            is_valid = False
        elif price > self.TTF_CRISIS_THRESHOLD:
            messages.append(f"Extremely high price {price} EUR/MWh (crisis level)")
            # Still valid, just a warning

        return is_valid, messages

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

                try:
                    data = await response.json()
                except (aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                    self.logger.debug(f"Alpha Vantage {symbol}: JSON decode error - {e}")
                    return None

                # Check for API errors
                if 'Error Message' in data:
                    self.logger.debug(f"Alpha Vantage {symbol}: {data['Error Message']}")
                    return None

                if 'Note' in data:  # Rate limit warning
                    self.logger.warning(f"Alpha Vantage rate limit: {data['Note']}")
                    return None

                # Validate response structure
                is_valid, errors = self._validate_api_response(data, self.ALPHA_VANTAGE_QUOTE_SCHEMA)
                if not is_valid:
                    self.logger.debug(f"Alpha Vantage {symbol}: Invalid response structure - {errors}")
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
            self._log_fetch_error('Alpha Vantage', symbol, e, level='debug')
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

                try:
                    data = await response.json()
                except (aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                    self.logger.debug(f"Alpha Vantage history {symbol}: JSON decode error - {e}")
                    return None

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
            self._log_fetch_error('Alpha Vantage history', symbol, e, level='debug')
            return None

    def _fetch_yfinance_sync(self, symbol: str, period: str = '30d') -> Optional[Dict[str, Any]]:
        """Fallback: Fetch from yfinance (synchronous)."""
        if not YFINANCE_AVAILABLE:
            return None

        try:
            t = yf.Ticker(symbol)
            hist = t.history(period=period)

            if hist.empty:
                self.logger.debug(f"yfinance {symbol}: Empty history returned")
                return None

            # Validate we have required columns
            required_cols = ['Open', 'High', 'Low', 'Close']
            missing_cols = [c for c in required_cols if c not in hist.columns]
            if missing_cols:
                self.logger.warning(f"yfinance {symbol}: Missing columns {missing_cols}")
                return None

            latest = hist.iloc[-1]

            # Validate Close price is not NaN
            if pd.isna(latest['Close']):
                self.logger.warning(f"yfinance {symbol}: Latest Close price is NaN")
                return None

            prev_close = hist.iloc[-2]['Close'] if len(hist) > 1 else latest['Close']

            # Handle NaN in previous close - fall back to latest (which we know is valid)
            if pd.isna(prev_close):
                self.logger.debug(f"yfinance {symbol}: prev_close is NaN, using latest Close")
                prev_close = latest['Close']

            # Final safety check - both values must be valid for change calculation
            if pd.isna(prev_close) or prev_close <= 0:
                self.logger.warning(f"yfinance {symbol}: Cannot calculate change (prev_close={prev_close})")
                prev_close = latest['Close']  # Use current price, change will be 0%

            # Build history dict, filtering out NaN values
            history = {}
            for d, row in hist.iterrows():
                close_price = row['Close']
                if not pd.isna(close_price):
                    history[d.strftime('%Y-%m-%d')] = round(float(close_price), 2)

            if not history:
                self.logger.warning(f"yfinance {symbol}: All history values are NaN")
                return None

            # Handle volume (can be NaN for some tickers)
            volume = None
            if 'Volume' in hist.columns and not pd.isna(latest['Volume']):
                volume = int(latest['Volume']) if latest['Volume'] > 0 else None

            return {
                'ticker': symbol,
                'price': round(float(latest['Close']), 2),
                'date': hist.index[-1].strftime('%Y-%m-%d'),
                'open': round(float(latest['Open']), 2) if not pd.isna(latest['Open']) else None,
                'high': round(float(latest['High']), 2) if not pd.isna(latest['High']) else None,
                'low': round(float(latest['Low']), 2) if not pd.isna(latest['Low']) else None,
                'volume': volume,
                'prev_close': round(float(prev_close), 2),
                'change_pct': round((latest['Close'] - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0,
                'history': history,
                'source': 'yfinance'
            }
        except KeyError as e:
            self._log_fetch_error('yfinance', symbol, e, level='debug')
            return None
        except (ValueError, TypeError) as e:
            self._log_fetch_error('yfinance', symbol, e, level='debug')
            return None
        except Exception as e:
            self._log_fetch_error('yfinance', symbol, e, level='debug')
            return None

    async def _fetch_ttf_yfinance(self, period: str = '30d', timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """
        Fetch TTF (Dutch gas) prices via yfinance.

        TTF is the European benchmark for natural gas, traded on ICE.
        yfinance provides this data via the TTF=F ticker.

        Args:
            period: History period to fetch (default 30d)
            timeout: Maximum time to wait for yfinance (default 30s)

        Returns:
            Dict with TTF price data or None if failed
        """
        if not YFINANCE_AVAILABLE:
            self.logger.warning("yfinance not available for TTF data")
            return None

        config = self.TTF_CONFIG
        symbol = config['symbol']

        try:
            loop = asyncio.get_running_loop()

            # Wrap yfinance call with timeout to prevent hanging
            try:
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, self._fetch_yfinance_sync, symbol, period),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                self.logger.warning(f"TTF ({symbol}): Timeout after {timeout}s")
                return None

            if not result:
                self.logger.debug(f"TTF ({symbol}): No data from yfinance")
                return None

            # Validate price is reasonable (TTF typically 5-500 EUR/MWh)
            price = result.get('price', 0)
            is_valid, messages = self._validate_ttf_price(price)
            for msg in messages:
                if 'rejected' in msg or 'exceeds' in msg:
                    self.logger.error(f"TTF ({symbol}): {msg}")
                else:
                    self.logger.warning(f"TTF ({symbol}): {msg}")
            if not is_valid:
                return None

            # Check data freshness (warn if older than expected)
            self._check_data_freshness(result.get('date', ''), f"TTF ({symbol})")

            # Extract history before adding metadata
            history = result.pop('history', None)

            # Validate history has enough data for lag features
            if history:
                history_count = len(history)
                if history_count < 7:
                    self.logger.warning(f"TTF ({symbol}): Only {history_count} days of history (need 7 for lag features)")
                elif history_count < 30:
                    self.logger.debug(f"TTF ({symbol}): {history_count} days of history (30 preferred)")

            # Add TTF-specific metadata
            result['description'] = config['description']
            result['units'] = config['units']
            result['name'] = config['name']
            result['note'] = config['note']
            result['currency'] = 'EUR'

            # Calculate lag features
            if history:
                lag_features = self._calculate_lag_features(history, result['price'])
                result.update(lag_features)
                result['history'] = history

            return result

        except asyncio.CancelledError:
            raise  # Re-raise cancellation (framework handles logging)
        except ValueError as e:
            self._log_fetch_error('TTF', symbol, e, level='warning')
            return None
        except Exception as e:
            self._log_fetch_error('TTF', symbol, e, level='warning')
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

        # Try yfinance as last resort (with timeout)
        if not quote and YFINANCE_AVAILABLE:
            self.logger.info(f"{name}: Trying yfinance fallback")
            loop = asyncio.get_running_loop()
            try:
                quote = await asyncio.wait_for(
                    loop.run_in_executor(None, self._fetch_yfinance_sync, symbol),
                    timeout=self.YFINANCE_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                self.logger.warning(f"{name}: yfinance timeout after 30s")
                quote = None
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

        # Set timeout for all HTTP requests
        timeout = aiohttp.ClientTimeout(total=self.HTTP_TIMEOUT_SECONDS, connect=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Fetch carbon via Alpha Vantage
            carbon_data = await self._fetch_commodity(session, 'carbon', self.CARBON_CONFIG)

            # Small delay to respect rate limits
            await asyncio.sleep(self.API_RATE_LIMIT_DELAY)

            # Fetch TTF gas prices via yfinance (primary source for European gas)
            ttf_data = await self._fetch_ttf_yfinance()

            # Fetch US gas proxy as fallback
            await asyncio.sleep(self.API_RATE_LIMIT_DELAY)
            gas_data = await self._fetch_commodity(session, 'gas', self.GAS_CONFIG)

        if carbon_data:
            results['carbon'] = carbon_data
            self.logger.info(f"Carbon proxy: {carbon_data['ticker']} = ${carbon_data['price']} ({carbon_data.get('source', 'unknown')})")
        else:
            self.logger.warning("Carbon proxy: No data available")

        if ttf_data:
            results['gas_ttf'] = ttf_data
            self.logger.info(f"TTF gas: {ttf_data['ticker']} = €{ttf_data['price']}/MWh ({ttf_data.get('source', 'unknown')})")
        else:
            self.logger.warning("TTF gas: No data available")

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

        if 'gas_ttf' not in data:
            warnings.append("TTF gas price unavailable")
        else:
            ttf = data['gas_ttf']

            # Validate TTF price range using shared validation
            ttf_price = ttf.get('price', 0)
            _, price_messages = self._validate_ttf_price(ttf_price)
            warnings.extend([f"TTF: {msg}" for msg in price_messages])

            # Validate currency
            if ttf.get('currency') != 'EUR':
                warnings.append(f"TTF currency should be EUR, got {ttf.get('currency')}")

            # Check for required lag features
            if 'price_lag1' not in ttf:
                warnings.append("TTF missing lag features (insufficient history)")

        if 'gas' not in data:
            warnings.append("US gas proxy unavailable")

        # Consider valid if we have at least TTF or US gas proxy
        has_gas = 'gas_ttf' in data or 'gas' in data
        has_carbon = 'carbon' in data

        # Check for critical validation failures
        critical_failures = [w for w in warnings if 'Invalid' in w or 'No market proxy' in w]

        return has_gas and has_carbon and len(critical_failures) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get metadata for market proxy dataset."""
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'carbon_symbol': self.CARBON_CONFIG['symbol'],
            'gas_ttf_symbol': self.TTF_CONFIG['symbol'],
            'gas_proxy_symbol': self.GAS_CONFIG['symbol'],
            'primary_source': 'Alpha Vantage API (carbon), yfinance (TTF)',
            'fallback_source': 'yfinance' if YFINANCE_AVAILABLE else None,
            'api_key_configured': bool(self.api_key),
            'description': 'Market prices for EU carbon (EUA) and natural gas (TTF + US proxy)',
            'usage_notes': [
                'KEUA tracks EU ETS carbon allowance prices',
                'TTF=F is the Dutch/European natural gas benchmark (EUR/MWh)',
                'UNG is a US gas proxy, included as fallback and for correlation analysis',
                'Use lagged values (price_lag1, price_lag7) to avoid data leakage',
                'Prices update daily on market trading days',
                'See docs/CARBON_GAS_PRICE_PROXIES.md for details'
            ]
        })

        return metadata

    def _compute_cache_signature(self, data_json: str) -> str:
        """Compute HMAC signature for cache data integrity."""
        return hmac.new(
            self._CACHE_HMAC_KEY,
            data_json.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def _load_cache(self) -> Optional[Dict[str, Any]]:
        """Load cached data if available, recent, and integrity verified."""
        if not self._cache_file or not os.path.exists(self._cache_file):
            return None

        try:
            with open(self._cache_file) as f:
                cached = json.load(f)

            # Verify integrity signature if present
            stored_sig = cached.get('signature')
            if stored_sig:
                # Reconstruct the signed data (without the signature itself)
                verify_data = {k: v for k, v in cached.items() if k != 'signature'}
                verify_json = json.dumps(verify_data, sort_keys=True)
                expected_sig = self._compute_cache_signature(verify_json)

                if not hmac.compare_digest(stored_sig, expected_sig):
                    self.logger.warning("Cache integrity check failed - ignoring cache")
                    return None

            # Parse timezone-aware timestamp (fallback to epoch for old caches)
            cached_at_str = cached.get('cached_at', '2000-01-01T00:00:00+00:00')
            cache_time = datetime.fromisoformat(cached_at_str)

            # Ensure timezone-aware comparison
            now = datetime.now(ZoneInfo('Europe/Amsterdam'))
            if cache_time.tzinfo is None:
                cache_time = cache_time.replace(tzinfo=ZoneInfo('Europe/Amsterdam'))

            if (now - cache_time).total_seconds() < self.CACHE_EXPIRATION_SECONDS:
                return cached.get('data')
        except Exception as e:
            self.logger.debug(f"Cache load error: {e}")

        return None

    def _save_cache(self, data: Dict[str, Any]) -> None:
        """Save data to cache with timezone-aware timestamp and integrity signature."""
        if not self._cache_file:
            return

        try:
            os.makedirs(os.path.dirname(self._cache_file), exist_ok=True)

            # Build cache data (without signature for signing)
            cache_data = {
                'cached_at': datetime.now(ZoneInfo('Europe/Amsterdam')).isoformat(),
                'data': data
            }

            # Compute integrity signature
            data_json = json.dumps(cache_data, sort_keys=True)
            cache_data['signature'] = self._compute_cache_signature(data_json)

            # Write atomically via temp file to prevent corruption
            temp_file = self._cache_file + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(cache_data, f, indent=2)

            # Set restrictive permissions (owner read/write only)
            try:
                os.chmod(temp_file, stat.S_IRUSR | stat.S_IWUSR)
            except OSError:
                pass  # Permissions may not be supported on all platforms

            # Atomic rename
            os.replace(temp_file, self._cache_file)

        except Exception as e:
            self.logger.debug(f"Cache save error: {e}")
            # Clean up temp file if it exists
            try:
                temp_file = self._cache_file + '.tmp'
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except OSError:
                pass


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

        for commodity in ['carbon', 'gas_ttf', 'gas']:
            if commodity in dataset.data:
                d = dataset.data[commodity]
                currency = '€' if d.get('currency') == 'EUR' else '$'
                print(f"\n{commodity.upper()} ({d.get('name', 'Unknown')}):")
                print(f"  Ticker: {d.get('ticker')}")
                print(f"  Price: {currency}{d.get('price')} {d.get('units', '')}")
                print(f"  Change: {d.get('change_pct', 0):+.2f}%")
                print(f"  Source: {d.get('source', 'unknown')}")
                if 'price_lag1' in d:
                    print(f"  Lag-1: {currency}{d.get('price_lag1')}")
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
    import platform
    # Fix Windows event loop for aiodns compatibility
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
