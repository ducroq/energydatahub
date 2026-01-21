"""
ENTSO-E Transparency Platform Collector
----------------------------------------
Collects day-ahead energy prices from ENTSO-E Transparency Platform using the
new base collector architecture.

File: collectors/entsoe.py
Created: 2025-10-25
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for ENTSO-E data. Handles day-ahead price data
    from the European Network of Transmission System Operators for Electricity.

Usage:
    from collectors.entsoe import EntsoeCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = EntsoeCollector(api_key="your_api_key")
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=1)

    data = await collector.collect(start, end, country_code='NL')
"""

import asyncio
from datetime import datetime
from typing import Any, Dict
import pandas as pd
from entsoe import EntsoePandasClient
from functools import partial

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class EntsoeCollector(BaseCollector):
    """
    Collector for ENTSO-E Transparency Platform day-ahead energy prices.

    Fetches prices from European electricity markets.
    """

    def __init__(self, api_key: str, retry_config: RetryConfig = None, circuit_breaker_config: CircuitBreakerConfig = None):
        """
        Initialize ENTSO-E collector.

        Args:
            api_key: ENTSO-E API key
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="EntsoeCollector",
            data_type="energy_price",
            source="ENTSO-E Transparency Platform API v1.3",
            units="EUR/MWh",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.api_key = api_key

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        country_code: str = 'NL',
        **kwargs
    ) -> pd.Series:
        """
        Fetch raw data from ENTSO-E Transparency Platform API.

        Args:
            start_time: Start of time range
            end_time: End of time range
            country_code: Country code (default: 'NL')

        Returns:
            Pandas Series with timestamp index and price values

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(f"Fetching ENTSO-E data for {country_code}")

        # Convert to pandas Timestamp and UTC for API
        start_timestamp = pd.Timestamp(start_time).tz_convert('UTC')
        end_timestamp = pd.Timestamp(end_time).tz_convert('UTC')

        self.logger.debug(
            f"Querying ENTSO-E API: {start_timestamp} to {end_timestamp} (UTC)"
        )

        # Create client
        client = EntsoePandasClient(api_key=self.api_key)

        # ENTSO-E API is synchronous, so we run in executor
        loop = asyncio.get_running_loop()
        query_func = partial(
            client.query_day_ahead_prices,
            country_code=country_code,
            start=start_timestamp,
            end=end_timestamp
        )

        # Execute in thread pool to not block event loop
        data = await loop.run_in_executor(None, query_func)

        if data is None or data.empty:
            raise ValueError("No data returned from ENTSO-E API")

        return data

    def _parse_response(
        self,
        raw_data: pd.Series,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Parse ENTSO-E API response to standardized format.

        Args:
            raw_data: Raw pandas Series or DataFrame from API
            start_time: Start of time range (for filtering)
            end_time: End of time range (for filtering)

        Returns:
            Dict mapping ISO timestamp strings to EUR/MWh prices
        """
        data = {}

        # Handle DataFrame with MultiIndex (newer entsoe-py versions may return this)
        if isinstance(raw_data, pd.DataFrame):
            # Flatten to Series if needed
            if len(raw_data.columns) == 1:
                raw_data = raw_data.iloc[:, 0]
            else:
                # Take first column for price data
                raw_data = raw_data.iloc[:, 0]

        # Reset index if it's a MultiIndex with TimeRange objects
        if hasattr(raw_data.index, 'get_level_values'):
            try:
                # Try to get the first level which should be timestamps
                idx = raw_data.index.get_level_values(0)
                raw_data = pd.Series(raw_data.values, index=idx)
            except Exception:
                pass

        for timestamp, price in raw_data.items():
            try:
                # Convert pandas Timestamp to datetime
                if hasattr(timestamp, 'to_pydatetime'):
                    dt = timestamp.to_pydatetime()
                elif hasattr(timestamp, 'start'):
                    # Handle TimeRange objects from newer entsoe-py versions
                    dt = timestamp.start.to_pydatetime() if hasattr(timestamp.start, 'to_pydatetime') else timestamp.start
                elif isinstance(timestamp, datetime):
                    dt = timestamp
                else:
                    # Skip non-timestamp entries
                    self.logger.debug(f"Skipping unknown timestamp type: {type(timestamp)}")
                    continue

                # Filter to requested time range
                if start_time <= dt < end_time:
                    # Normalize to Amsterdam timezone
                    amsterdam_dt = normalize_timestamp_to_amsterdam(dt)
                    data[amsterdam_dt.isoformat()] = float(price)
            except (TypeError, AttributeError) as e:
                self.logger.debug(f"Skipping entry due to type issue: {e}")
                continue

        self.logger.debug(f"Parsed {len(data)} data points from ENTSO-E response")

        return data

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for ENTSO-E dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        # Get base metadata from parent class
        metadata = super()._get_metadata(start_time, end_time)

        # Add ENTSO-E-specific metadata
        metadata.update({
            'country_code': 'NL',  # Could be made configurable
            'market': 'day-ahead',
            'currency': 'EUR',
            'resolution': 'hourly',
            'api_version': 'v1.3'
        })

        return metadata


# Example usage and backward compatibility
async def get_Entsoe_data(
    api_key: str,
    country_code: str,
    start_time: datetime,
    end_time: datetime
):
    """
    Backward-compatible function for existing code.

    This maintains the same interface as the old entsoe_client.py
    but uses the new collector architecture internally.

    Args:
        api_key: ENTSO-E API key
        country_code: Country code (e.g., 'NL')
        start_time: Start of time range
        end_time: End of time range

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = EntsoeCollector(api_key=api_key)
    return await collector.collect(
        start_time=start_time,
        end_time=end_time,
        country_code=country_code
    )


# Example usage
async def main():
    """Example usage of EntsoeCollector."""
    import os
    from configparser import ConfigParser
    from zoneinfo import ZoneInfo
    from datetime import timedelta

    # Load API key
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, '..', 'energy_data_fetchers', 'secrets.ini')

    config = ConfigParser()
    config.read(secrets_file)
    api_key = config.get('api_keys', 'entsoe')

    # Setup time range
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = (start + timedelta(days=1)).replace(hour=23, minute=59, second=59)

    # Create collector and fetch data
    collector = EntsoeCollector(api_key=api_key)
    dataset = await collector.collect(start, end, country_code='NL')

    if dataset:
        print(f"Collected {len(dataset.data)} data points")
        print(f"\nFirst 5 prices:")
        for timestamp, price in list(dataset.data.items())[:5]:
            print(f"  {timestamp}: {price} EUR/MWh")

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        print(f"\nCollection metrics:")
        print(f"  Duration: {metrics[0].duration_seconds:.2f}s")
        print(f"  Status: {metrics[0].status.value}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
