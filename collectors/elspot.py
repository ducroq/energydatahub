"""
Nord Pool Elspot Collector
---------------------------
Collects day-ahead energy prices from Nord Pool Elspot market using the new
base collector architecture.

File: collectors/elspot.py
Created: 2025-10-25
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for Nord Pool Elspot data. Handles day-ahead price
    data from the Nordic and Baltic power exchange.

Usage:
    from collectors.elspot import ElspotCollector
    from datetime import datetime
    import pytz

    collector = ElspotCollector()
    amsterdam_tz = pytz.timezone('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=1)

    data = await collector.collect(start, end, country_code='NL')
"""

import asyncio
from datetime import datetime
from typing import Any, Dict
from nordpool import elspot
from functools import partial

from collectors.base import BaseCollector, RetryConfig
from utils.timezone_helpers import localize_naive_datetime


class ElspotCollector(BaseCollector):
    """
    Collector for Nord Pool Elspot day-ahead energy prices.

    Fetches prices from Nordic and Baltic power exchange.
    """

    def __init__(self, retry_config: RetryConfig = None):
        """
        Initialize Elspot collector.

        Args:
            retry_config: Optional retry configuration
        """
        super().__init__(
            name="ElspotCollector",
            data_type="energy_price",
            source="Nord Pool Elspot API",
            units="EUR/MWh",
            retry_config=retry_config
        )

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        country_code: str = 'NL',
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from Nord Pool Elspot API.

        Args:
            start_time: Start of time range
            end_time: End of time range
            country_code: Country code (default: 'NL')

        Returns:
            Raw API response dictionary

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(f"Fetching Elspot data for {country_code}")

        prices_spot = elspot.Prices()

        # Nord Pool API is synchronous, so we run in executor
        loop = asyncio.get_running_loop()
        fetch_func = partial(
            prices_spot.hourly,
            areas=[country_code],
            end_date=end_time.date()
        )

        # Execute in thread pool to not block event loop
        prices_data = await loop.run_in_executor(None, fetch_func)

        if not prices_data:
            raise ValueError("No data returned from Nord Pool API")

        if 'areas' not in prices_data:
            raise ValueError("Invalid response format: missing 'areas' field")

        if country_code not in prices_data['areas']:
            raise ValueError(f"Country {country_code} not found in response")

        return prices_data

    def _parse_response(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Parse Nord Pool API response to standardized format.

        Args:
            raw_data: Raw API response
            start_time: Start of time range (for filtering)
            end_time: End of time range (for filtering)

        Returns:
            Dict mapping ISO timestamp strings to EUR/MWh prices
        """
        # Extract timezone from start_time
        timezone = start_time.tzinfo

        # Get country code from metadata (stored during fetch)
        # For now, we assume NL, but could be made configurable
        country_code = 'NL'

        # Check if we have the data for this country
        if country_code not in raw_data['areas']:
            self.logger.error(f"Country {country_code} not found in Elspot data")
            return {}

        area_data = raw_data['areas'][country_code]

        if 'values' not in area_data:
            self.logger.error(f"No 'values' field in Elspot data for {country_code}")
            return {}

        data = {}

        for day_data in area_data['values']:
            # Nord Pool API returns naive datetime objects
            # We need to properly localize them to Europe/Amsterdam timezone
            naive_timestamp = day_data['start']

            # Properly localize the naive datetime
            if naive_timestamp.tzinfo is None:
                timestamp = localize_naive_datetime(naive_timestamp, timezone)
            else:
                # If somehow it has timezone, convert to target timezone
                timestamp = naive_timestamp.astimezone(timezone)

            # Filter to requested time range
            if start_time <= timestamp < end_time:
                data[timestamp.isoformat()] = day_data['value']

        self.logger.debug(f"Parsed {len(data)} data points from Elspot response")

        return data

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for Elspot dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        # Get base metadata from parent class
        metadata = super()._get_metadata(start_time, end_time)

        # Add Elspot-specific metadata
        metadata.update({
            'country_code': 'NL',  # Could be made configurable
            'market': 'day-ahead',
            'currency': 'EUR',
            'resolution': 'hourly'
        })

        return metadata


# Example usage and backward compatibility
async def get_Elspot_data(
    country_code: str,
    start_time: datetime,
    end_time: datetime
):
    """
    Backward-compatible function for existing code.

    This maintains the same interface as the old nordpool_data_fetcher.py
    but uses the new collector architecture internally.

    Args:
        country_code: Country code (e.g., 'NL')
        start_time: Start of time range
        end_time: End of time range

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = ElspotCollector()
    return await collector.collect(
        start_time=start_time,
        end_time=end_time,
        country_code=country_code
    )
