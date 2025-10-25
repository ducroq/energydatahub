"""
EnergyZero API Collector
-------------------------
Collects energy prices from EnergyZero API using the new base collector
architecture.

File: collectors/energyzero.py
Created: 2025-10-25
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for EnergyZero data. Handles real-time and
    day-ahead price data for the Dutch energy market.

    Updated for energyzero 3.0.0 API which removed class-level VAT parameter.

Usage:
    from collectors.energyzero import EnergyZeroCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = EnergyZeroCollector()
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=1)

    data = await collector.collect(start, end)
"""

import asyncio
from datetime import datetime
from typing import Any, Dict
from energyzero import EnergyZero, VatOption

from collectors.base import BaseCollector, RetryConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class EnergyZeroCollector(BaseCollector):
    """
    Collector for EnergyZero API energy prices.

    Fetches prices from the Dutch energy market including VAT.
    Compatible with energyzero 3.0.0+ API.
    """

    def __init__(self, vat_option: VatOption = VatOption.INCLUDE, retry_config: RetryConfig = None):
        """
        Initialize EnergyZero collector.

        Args:
            vat_option: VAT option (INCLUDE or EXCLUDE) - passed to function level
            retry_config: Optional retry configuration
        """
        super().__init__(
            name="EnergyZeroCollector",
            data_type="energy_price",
            source="EnergyZero API v3.0",
            units="EUR/kWh (incl. VAT)" if vat_option == VatOption.INCLUDE else "EUR/kWh (excl. VAT)",
            retry_config=retry_config
        )
        self.vat_option = vat_option

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Any:
        """
        Fetch raw data from EnergyZero API.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Raw API response object (Electricity model)

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(f"Fetching EnergyZero data")

        # EnergyZero API v3.0.0 removed class-level VAT parameter
        # VAT must be specified at function level
        async with EnergyZero() as client:
            data = await client.energy_prices(
                start_date=start_time.date(),
                end_date=end_time.date(),
                interval=4,  # 4 = hourly interval
                vat=self.vat_option
            )

        if not data or not hasattr(data, 'timestamp_prices'):
            raise ValueError("No data returned from EnergyZero API")

        if not data.timestamp_prices:
            raise ValueError("Empty prices dict from EnergyZero API")

        return data

    def _parse_response(
        self,
        raw_data: Any,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Parse EnergyZero API response to standardized format.

        Args:
            raw_data: Raw API response object (Electricity model) with .timestamp_prices attribute
            start_time: Start of time range (for filtering)
            end_time: End of time range (for filtering)

        Returns:
            Dict mapping ISO timestamp strings to EUR/kWh prices
        """
        data = {}

        for timestamp, price in raw_data.timestamp_prices.items():
            # Filter to requested time range
            if start_time <= timestamp < end_time:
                # Normalize to Amsterdam timezone
                amsterdam_dt = normalize_timestamp_to_amsterdam(timestamp)
                data[amsterdam_dt.isoformat()] = float(price)

        self.logger.debug(f"Parsed {len(data)} data points from EnergyZero response")

        return data

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for EnergyZero dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        # Get base metadata from parent class
        metadata = super()._get_metadata(start_time, end_time)

        # Add EnergyZero-specific metadata
        metadata.update({
            'country_code': 'NL',
            'market': 'retail',
            'currency': 'EUR',
            'resolution': 'hourly',
            'vat_included': self.vat_option == VatOption.INCLUDE,
            'api_version': 'v2.1'
        })

        return metadata


# Example usage and backward compatibility
async def get_Energy_zero_data(
    start_time: datetime,
    end_time: datetime
):
    """
    Backward-compatible function for existing code.

    This maintains the same interface as the old energy_zero_price_fetcher.py
    but uses the new collector architecture internally.

    Args:
        start_time: Start of time range
        end_time: End of time range

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = EnergyZeroCollector(vat_option=VatOption.INCLUDE)
    return await collector.collect(
        start_time=start_time,
        end_time=end_time
    )


# Example usage
async def main():
    """Example usage of EnergyZeroCollector."""
    from zoneinfo import ZoneInfo
    from datetime import timedelta
    import platform

    # Set Windows event loop policy if needed
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Setup time range
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = (start + timedelta(days=1)).replace(hour=23, minute=59, second=59)

    # Create collector and fetch data
    collector = EnergyZeroCollector(vat_option=VatOption.INCLUDE)
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected {len(dataset.data)} data points")
        print(f"\nFirst 5 prices:")
        for timestamp, price in list(dataset.data.items())[:5]:
            print(f"  {timestamp}: {price} EUR/kWh")

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        print(f"\nCollection metrics:")
        print(f"  Duration: {metrics[0].duration_seconds:.2f}s")
        print(f"  Status: {metrics[0].status.value}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
