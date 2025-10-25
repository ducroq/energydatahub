"""
EPEX SPOT Market Collector
----------------------------
Collects day-ahead energy prices from EPEX SPOT market via Awattar API using
the new base collector architecture.

File: collectors/epex.py
Created: 2025-10-25
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for EPEX SPOT data. Handles day-ahead market
    prices from the European Power Exchange.

Usage:
    from collectors.epex import EpexCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = EpexCollector()
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=1)

    data = await collector.collect(start, end)
"""

import asyncio
from datetime import datetime
from typing import Any, Dict
import aiohttp

from collectors.base import BaseCollector, RetryConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class EpexCollector(BaseCollector):
    """
    Collector for EPEX SPOT market prices via Awattar API.

    Fetches day-ahead prices from the European Power Exchange.
    """

    def __init__(self, retry_config: RetryConfig = None):
        """
        Initialize EPEX collector.

        Args:
            retry_config: Optional retry configuration
        """
        super().__init__(
            name="EpexCollector",
            data_type="energy_price",
            source="Awattar API (EPEX SPOT)",
            units="EUR/MWh",
            retry_config=retry_config
        )
        self.base_url = 'https://api.awattar.at/v1/marketdata'

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from Awattar API.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Raw API response dictionary

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(f"Fetching EPEX data from Awattar API")

        # Convert to Unix timestamps in milliseconds
        start_ts = int(start_time.timestamp() * 1000)
        end_ts = int(end_time.timestamp() * 1000)

        url = f"{self.base_url}?start={start_ts}&end={end_ts}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise ValueError(
                        f"Awattar API returned status {response.status}"
                    )

                data = await response.json()

        if not data or 'data' not in data:
            raise ValueError("No data returned from Awattar API")

        if not data['data']:
            raise ValueError("Empty data array from Awattar API")

        return data

    def _parse_response(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Parse Awattar API response to standardized format.

        Args:
            raw_data: Raw API response dictionary
            start_time: Start of time range (for context)
            end_time: End of time range (for context)

        Returns:
            Dict mapping ISO timestamp strings to EUR/MWh prices
        """
        data = {}

        for item in raw_data['data']:
            # Convert Unix timestamp (milliseconds) to datetime
            timestamp = datetime.fromtimestamp(
                item['start_timestamp'] / 1000,
                tz=start_time.tzinfo
            )

            # Normalize to Amsterdam timezone
            amsterdam_dt = normalize_timestamp_to_amsterdam(timestamp)

            # Store price
            data[amsterdam_dt.isoformat()] = float(item['marketprice'])

        self.logger.debug(f"Parsed {len(data)} data points from Awattar response")

        return data

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for EPEX dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        # Get base metadata from parent class
        metadata = super()._get_metadata(start_time, end_time)

        # Add EPEX-specific metadata
        metadata.update({
            'country_code': 'NL',
            'market': 'EPEX SPOT day-ahead',
            'currency': 'EUR',
            'resolution': 'hourly',
            'api_provider': 'Awattar'
        })

        return metadata


# Example usage and backward compatibility
async def get_Epex_data(
    start_time: datetime,
    end_time: datetime
):
    """
    Backward-compatible function for existing code.

    This maintains the same interface as the old epex_price_fetcher.py
    but uses the new collector architecture internally.

    Args:
        start_time: Start of time range
        end_time: End of time range

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = EpexCollector()
    return await collector.collect(
        start_time=start_time,
        end_time=end_time
    )


# Example usage
async def main():
    """Example usage of EpexCollector."""
    from zoneinfo import ZoneInfo
    from datetime import timedelta

    # Setup time range
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = (start + timedelta(days=1)).replace(hour=23, minute=59, second=59)

    # Create collector and fetch data
    collector = EpexCollector()
    dataset = await collector.collect(start, end)

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
