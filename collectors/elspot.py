"""
Nord Pool Elspot Collector
---------------------------
Collects day-ahead energy prices from Nord Pool Elspot market using the new
base collector architecture.

File: collectors/elspot.py
Created: 2025-10-25
Updated: 2026-02-05 - Migrated to pynordpool (API v2) after v1 deprecation

Description:
    Implements BaseCollector for Nord Pool Elspot data. Handles day-ahead price
    data from the Nordic and Baltic power exchange.

    Note: The original nordpool library used API v1 which was deprecated on
    September 30, 2024. This collector now uses pynordpool which supports
    the new API v2 endpoints.

Usage:
    from collectors.elspot import ElspotCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = ElspotCollector()
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=1)

    data = await collector.collect(start, end, country_code='NL')
"""

import aiohttp
from datetime import datetime, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from pynordpool import NordPoolClient, Currency, DeliveryPeriodData

from collectors.base import BaseCollector, RetryConfig
from utils.timezone_helpers import localize_naive_datetime


# Mapping of country codes to Nord Pool area codes
COUNTRY_TO_AREA = {
    'NL': 'NL',
    'DE': 'DE-LU',  # Germany-Luxembourg bidding zone
    'BE': 'BE',
    'DK': 'DK1',    # Denmark West
    'DK1': 'DK1',
    'DK2': 'DK2',   # Denmark East
    'NO': 'NO1',    # Norway Oslo
    'SE': 'SE3',    # Sweden Stockholm
    'FI': 'FI',
    'EE': 'EE',
    'LV': 'LV',
    'LT': 'LT',
    'AT': 'AT',
    'FR': 'FR',
    'PL': 'PL',
}


class ElspotCollector(BaseCollector):
    """
    Collector for Nord Pool Elspot day-ahead energy prices.

    Fetches prices from Nordic and Baltic power exchange using API v2.
    """

    # HTTP timeout for API requests
    HTTP_TIMEOUT_SECONDS = 30

    def __init__(self, retry_config: RetryConfig = None):
        """
        Initialize Elspot collector.

        Args:
            retry_config: Optional retry configuration
        """
        super().__init__(
            name="ElspotCollector",
            data_type="energy_price",
            source="Nord Pool Elspot API v2",
            units="EUR/MWh",
            retry_config=retry_config
        )

    def _get_area_code(self, country_code: str) -> str:
        """
        Convert country code to Nord Pool area code.

        Args:
            country_code: ISO country code (e.g., 'NL', 'DE')

        Returns:
            Nord Pool area code
        """
        return COUNTRY_TO_AREA.get(country_code.upper(), country_code.upper())

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        country_code: str = 'NL',
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from Nord Pool API v2 via pynordpool.

        Args:
            start_time: Start of time range
            end_time: End of time range
            country_code: Country code (default: 'NL')

        Returns:
            Raw API response dictionary

        Raises:
            Exception: If API call fails
        """
        area_code = self._get_area_code(country_code)
        self.logger.debug(f"Fetching Elspot data for {country_code} (area: {area_code})")

        timeout = aiohttp.ClientTimeout(total=self.HTTP_TIMEOUT_SECONDS)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            client = NordPoolClient(session)

            # Fetch data for today's prices
            # Day-ahead prices for tomorrow are published around 12:00-13:00 CET
            # We request today's date to get the latest available prices
            request_date = datetime.now(ZoneInfo('Europe/Amsterdam'))

            try:
                delivery_data: DeliveryPeriodData = await client.async_get_delivery_period(
                    request_date,
                    Currency.EUR,
                    [area_code]
                )
            except Exception as e:
                self.logger.error(f"Nord Pool API error: {e}")
                raise

        if not delivery_data:
            raise ValueError("No data returned from Nord Pool API")

        # Convert pynordpool response to our internal format
        return {
            'areas': {
                country_code: {
                    'values': self._convert_delivery_data(delivery_data, area_code)
                }
            },
            'raw_response': delivery_data
        }

    def _convert_delivery_data(
        self,
        delivery_data: DeliveryPeriodData,
        area_code: str
    ) -> List[Dict]:
        """
        Convert pynordpool DeliveryPeriodData to our internal format.

        pynordpool returns 15-minute resolution data. We aggregate to hourly
        by taking the first value of each hour (prices are typically the same
        within an hour for day-ahead markets).

        Args:
            delivery_data: Response from pynordpool
            area_code: The area code we requested

        Returns:
            List of dicts with 'start' and 'value' keys (hourly)
        """
        values = []
        seen_hours = set()

        # DeliveryPeriodData has entries attribute with 15-min data
        if hasattr(delivery_data, 'entries') and delivery_data.entries:
            for entry in delivery_data.entries:
                # Each entry has start, end, and entry (dict with area prices)
                if hasattr(entry, 'start') and hasattr(entry, 'entry'):
                    price_dict = entry.entry
                    if isinstance(price_dict, dict):
                        price = price_dict.get(area_code)
                        if price is not None:
                            # Aggregate to hourly - take first value of each hour
                            hour_key = entry.start.replace(minute=0, second=0, microsecond=0)
                            if hour_key not in seen_hours:
                                seen_hours.add(hour_key)
                                values.append({
                                    'start': hour_key,
                                    'value': price
                                })

        self.logger.debug(f"Converted {len(values)} hourly price entries from pynordpool (from {len(delivery_data.entries) if delivery_data.entries else 0} quarter-hourly)")
        return values

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
        timezone = start_time.tzinfo or ZoneInfo('Europe/Amsterdam')

        # Get country code from metadata (stored during fetch)
        country_code = 'NL'

        # Check if we have the data for this country
        if country_code not in raw_data.get('areas', {}):
            self.logger.error(f"Country {country_code} not found in Elspot data")
            return {}

        area_data = raw_data['areas'][country_code]

        if 'values' not in area_data:
            self.logger.error(f"No 'values' field in Elspot data for {country_code}")
            return {}

        data = {}

        for day_data in area_data['values']:
            timestamp = day_data['start']

            # Handle naive datetime objects
            if timestamp.tzinfo is None:
                timestamp = localize_naive_datetime(timestamp, timezone)
            else:
                # Convert to target timezone
                timestamp = timestamp.astimezone(timezone)

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
            'resolution': 'hourly',
            'api_version': 'v2',
            'library': 'pynordpool'
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
