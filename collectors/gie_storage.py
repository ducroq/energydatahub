"""
GIE AGSI+ Gas Storage Collector
-------------------------------
Fetches European gas storage levels from the GIE AGSI+ platform.

File: collectors/gie_storage.py
Created: 2025-01-19

Description:
    Collects gas storage data including fill levels, working capacity,
    injection and withdrawal rates from the GIE (Gas Infrastructure Europe)
    AGSI+ (Aggregated Gas Storage Inventory) platform.

    Gas storage levels are important for electricity price prediction because:
    - Gas-fired power plants set the marginal price ~40% of the time in NL
    - Low storage levels can indicate supply constraints and higher gas prices
    - Seasonal patterns affect electricity prices (winter heating demand)

    Data Source:
    - GIE AGSI+ API via gie-py library
    - Requires API key from https://agsi.gie.eu/

Usage:
    from collectors.gie_storage import GieStorageCollector

    collector = GieStorageCollector(api_key="your_gie_api_key")
    data = await collector.collect(start_time, end_time)
"""

import asyncio
import logging
from datetime import datetime
from functools import partial
from typing import Dict, Optional, Any, List

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig


class GieStorageCollector(BaseCollector):
    """
    Collector for European gas storage levels from GIE AGSI+.

    Uses gie-py library (synchronous) wrapped for async execution.
    """

    # Country codes supported by AGSI+
    SUPPORTED_COUNTRIES = [
        'AT', 'BE', 'BG', 'CZ', 'DE', 'DK', 'ES', 'FR', 'HR', 'HU',
        'IT', 'LV', 'NL', 'PL', 'PT', 'RO', 'SE', 'SK', 'UA', 'UK'
    ]

    def __init__(
        self,
        api_key: str,
        country_code: str = 'NL',
        retry_config: Optional[RetryConfig] = None,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None
    ):
        """
        Initialize GIE Storage collector.

        Args:
            api_key: GIE AGSI+ API key (get from https://agsi.gie.eu/)
            country_code: ISO country code (default: 'NL')
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="GieStorageCollector",
            data_type="gas_storage",
            source="GIE AGSI+",
            units="percent",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )

        self.api_key = api_key
        self.country_code = country_code.upper()

        if self.country_code not in self.SUPPORTED_COUNTRIES:
            raise ValueError(
                f"Unsupported country code: {self.country_code}. "
                f"Supported: {', '.join(self.SUPPORTED_COUNTRIES)}"
            )

    def _query_storage_sync(
        self,
        start_date: str,
        end_date: str
    ) -> Any:
        """
        Synchronous query to GIE AGSI+ API.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with storage data
        """
        from gie import GiePandasClient

        client = GiePandasClient(api_key=self.api_key)

        # Query country-level aggregated storage data
        df = client.query_gas_country(
            country=self.country_code,
            start=start_date,
            end=end_date
        )

        return df

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Any:
        """
        Fetch gas storage data from GIE AGSI+.

        Uses run_in_executor to run the synchronous gie-py library.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            DataFrame with storage data
        """
        self.logger.debug(f"Fetching GIE storage data for {self.country_code}")

        # Format dates for API
        start_date = start_time.strftime('%Y-%m-%d')
        end_date = end_time.strftime('%Y-%m-%d')

        # Run synchronous query in executor
        loop = asyncio.get_running_loop()
        query_func = partial(self._query_storage_sync, start_date, end_date)
        df = await loop.run_in_executor(None, query_func)

        self.logger.info(
            f"GIE storage: Retrieved {len(df)} records for {self.country_code}"
        )

        return df

    def _parse_response(
        self,
        raw_data: Any,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Parse DataFrame response to standardized format.

        Args:
            raw_data: DataFrame from gie-py
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict mapping ISO timestamp strings to storage data
        """
        if raw_data is None or len(raw_data) == 0:
            self.logger.warning("No GIE storage data in response")
            return {}

        result = {}

        for _, row in raw_data.iterrows():
            # Get timestamp - handle both 'gasDayStart' and index
            if 'gasDayStart' in raw_data.columns:
                timestamp = row['gasDayStart']
            elif raw_data.index.name == 'gasDayStart':
                timestamp = _.isoformat() if hasattr(_, 'isoformat') else str(_)
            else:
                # Try to use index
                timestamp = str(_)

            # Ensure timestamp is a string
            if hasattr(timestamp, 'isoformat'):
                # Add timezone if naive
                if timestamp.tzinfo is None:
                    from zoneinfo import ZoneInfo
                    timestamp = timestamp.replace(tzinfo=ZoneInfo('Europe/Amsterdam'))
                timestamp_str = timestamp.isoformat()
            else:
                timestamp_str = str(timestamp)

            # Extract storage metrics
            data_point = {}

            # Fill level percentage (primary metric)
            if 'full' in raw_data.columns:
                fill_pct = row.get('full')
                if fill_pct is not None:
                    data_point['fill_level_pct'] = float(fill_pct)

            # Working gas volume (TWh)
            if 'gasInStorage' in raw_data.columns:
                gas_twh = row.get('gasInStorage')
                if gas_twh is not None:
                    data_point['working_capacity_twh'] = float(gas_twh)

            # Injection rate (GWh/day)
            if 'injection' in raw_data.columns:
                injection = row.get('injection')
                if injection is not None:
                    data_point['injection_gwh'] = float(injection)

            # Withdrawal rate (GWh/day)
            if 'withdrawal' in raw_data.columns:
                withdrawal = row.get('withdrawal')
                if withdrawal is not None:
                    data_point['withdrawal_gwh'] = float(withdrawal)

            # Net change (injection - withdrawal)
            if 'injection_gwh' in data_point and 'withdrawal_gwh' in data_point:
                data_point['net_change_gwh'] = (
                    data_point['injection_gwh'] - data_point['withdrawal_gwh']
                )

            # Working gas capacity (reference)
            if 'workingGasVolume' in raw_data.columns:
                wgv = row.get('workingGasVolume')
                if wgv is not None:
                    data_point['working_gas_volume_twh'] = float(wgv)

            if data_point:
                result[timestamp_str] = data_point

        return result

    def _validate_data(
        self,
        data: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """
        Validate gas storage data.

        Args:
            data: Parsed data dictionary
            start_time: Expected start time
            end_time: Expected end time

        Returns:
            (is_valid, list of warnings)
        """
        warnings = []

        if not data:
            warnings.append("No gas storage data collected")
            return False, warnings

        # Check for fill level data
        has_fill_level = any(
            'fill_level_pct' in v for v in data.values()
        )
        if not has_fill_level:
            warnings.append("No fill level data in response")

        # Validate fill level ranges
        for ts, values in data.items():
            fill_pct = values.get('fill_level_pct')
            if fill_pct is not None and (fill_pct < 0 or fill_pct > 100):
                warnings.append(f"Invalid fill level at {ts}: {fill_pct}%")

        # Check data point count (daily data, so 1-2 points expected)
        if len(data) < 1:
            warnings.append(f"Only {len(data)} data points collected")

        return len([w for w in warnings if 'No' in w or 'Invalid' in w]) == 0, warnings

    def _get_metadata(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Get metadata for gas storage dataset."""
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'country_code': self.country_code,
            'data_frequency': 'daily',
            'description': (
                f'Gas storage levels for {self.country_code} from GIE AGSI+. '
                'Fill level percentage indicates storage utilization. '
                'Injection/withdrawal rates show daily flow direction.'
            ),
            'usage_notes': [
                'fill_level_pct is the primary metric (0-100%)',
                'Negative net_change_gwh indicates net withdrawal (winter)',
                'Positive net_change_gwh indicates net injection (summer)',
                'Low fill levels may indicate supply constraints',
                'Data is updated daily around 18:00 CET'
            ],
            'api_documentation': 'https://agsi.gie.eu/api-documentation'
        })

        return metadata


async def get_gas_storage(
    api_key: str,
    start_time: datetime,
    end_time: datetime,
    country_code: str = 'NL'
) -> Optional[Any]:
    """
    Convenience function to fetch gas storage data.

    Args:
        api_key: GIE AGSI+ API key
        start_time: Start of time range
        end_time: End of time range
        country_code: ISO country code (default: 'NL')

    Returns:
        EnhancedDataSet with storage data or None if failed
    """
    collector = GieStorageCollector(
        api_key=api_key,
        country_code=country_code
    )
    return await collector.collect(start_time, end_time)


# Example usage
async def main():
    """Example usage of GieStorageCollector."""
    import os
    from zoneinfo import ZoneInfo
    from datetime import timedelta

    logging.basicConfig(level=logging.INFO)

    api_key = os.getenv('GIE_API_KEY')
    if not api_key:
        print("Set GIE_API_KEY environment variable")
        print("Get a key at: https://agsi.gie.eu/")
        return

    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    end = datetime.now(amsterdam_tz)
    start = end - timedelta(days=7)

    collector = GieStorageCollector(api_key=api_key, country_code='NL')
    dataset = await collector.collect(start, end)

    if dataset:
        print("\nGas Storage Data (NL):")
        print("=" * 60)
        for ts, values in sorted(dataset.data.items()):
            print(f"\n{ts}:")
            for k, v in values.items():
                print(f"  {k}: {v}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
