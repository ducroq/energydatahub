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

    Updated for energyzero 5.0.0 API which replaced VatOption with PriceType.

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
from energyzero import EnergyZero, PriceType, Interval

from collectors.base import BaseCollector, RetryConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class EnergyZeroCollector(BaseCollector):
    """
    Collector for EnergyZero API energy prices.

    Fetches prices from the Dutch energy market including VAT.
    Compatible with energyzero 5.0.0+ API.
    """

    def __init__(self, price_type: PriceType = PriceType.ALL_IN, retry_config: RetryConfig = None, circuit_breaker_config=None):
        """
        Initialize EnergyZero collector.

        Args:
            price_type: Price type to fetch:
                - PriceType.MARKET: Wholesale price excl. VAT
                - PriceType.MARKET_WITH_VAT: Market price incl. VAT
                - PriceType.ALL_IN_EXCL_VAT: Market + surcharges excl. VAT
                - PriceType.ALL_IN: Final consumer rate incl. VAT (default)
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        vat_included = price_type in (PriceType.MARKET_WITH_VAT, PriceType.ALL_IN)
        super().__init__(
            name="EnergyZeroCollector",
            data_type="energy_price",
            source="EnergyZero API v5.0",
            units="EUR/kWh (incl. VAT)" if vat_included else "EUR/kWh (excl. VAT)",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.price_type = price_type

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
            Raw API response object (Electricity model) or combined dict

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(f"Fetching EnergyZero data")

        # EnergyZero API v5.0.0 only supports single-day requests
        # If date range spans multiple days, make separate requests
        start_date = start_time.date()
        end_date = end_time.date()

        all_prices = {}

        async with EnergyZero() as client:
            current_date = start_date
            while current_date <= end_date:
                try:
                    data = await client.get_electricity_prices(
                        start_date=current_date,
                        end_date=current_date,  # Same date for single-day request
                        interval=Interval.HOUR,
                        price_type=self.price_type
                    )
                    if data and hasattr(data, 'prices') and data.prices:
                        all_prices.update(data.prices)
                except Exception as e:
                    self.logger.warning(f"Failed to fetch {current_date}: {e}")

                from datetime import timedelta
                current_date += timedelta(days=1)

        if not all_prices:
            raise ValueError("No data returned from EnergyZero API")

        # Return a simple object with prices attribute for compatibility
        class CombinedData:
            def __init__(self, prices):
                self.prices = prices

        return CombinedData(all_prices)

    def _parse_response(
        self,
        raw_data: Any,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Parse EnergyZero API response to standardized format.

        Args:
            raw_data: Raw API response object (Electricity model) with .prices attribute
            start_time: Start of time range (for filtering)
            end_time: End of time range (for filtering)

        Returns:
            Dict mapping ISO timestamp strings to EUR/kWh prices
        """
        data = {}

        for timestamp, price in raw_data.prices.items():
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
        vat_included = self.price_type in (PriceType.MARKET_WITH_VAT, PriceType.ALL_IN)
        metadata.update({
            'country_code': 'NL',
            'market': 'retail',
            'currency': 'EUR',
            'resolution': 'hourly',
            'vat_included': vat_included,
            'price_type': self.price_type.name,
            'api_version': 'v5.0'
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
    collector = EnergyZeroCollector(price_type=PriceType.ALL_IN)
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
    collector = EnergyZeroCollector(price_type=PriceType.ALL_IN)
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
