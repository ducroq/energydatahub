"""
NED.nl (Nationaal Energie Dashboard) Collector
-----------------------------------------------
Collects actual and forecasted energy production data from NED.nl.

File: collectors/ned.py
Created: 2025-12-01
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for NED.nl (National Energy Dashboard) data.
    Fetches actual and forecasted production for:
    - Solar power
    - Wind onshore
    - Wind offshore

    NED.nl is operated by TenneT and Gasunie, providing official Dutch
    energy production data with forecasts up to 1 week ahead.

Usage:
    from collectors.ned import NedCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = NedCollector(api_key="your_api_key")
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=1)

    data = await collector.collect(start, end)

API Documentation:
    https://ned.nl/nl/handleiding-api
    Rate limit: 200 requests per 5 minutes
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class NedCollector(BaseCollector):
    """
    Collector for NED.nl (Nationaal Energie Dashboard) energy data.

    Fetches actual and forecasted production data for solar, wind onshore,
    and wind offshore from the official Dutch energy dashboard.
    """

    # Energy type IDs from NED.nl API
    TYPE_IDS = {
        'solar': 2,
        'wind_onshore': 1,
        'wind_offshore': 17,
    }

    # Point IDs (geographic areas)
    POINT_IDS = {
        'netherlands': 0,      # Total Netherlands
        'offshore': 14,        # Offshore total
    }

    # Classification IDs
    CLASSIFICATION_IDS = {
        'forecast': 1,
        'current': 2,          # Near real-time actual
        'backcast': 3,         # Historical actual
    }

    # Activity IDs
    ACTIVITY_IDS = {
        'providing': 1,        # Generation/production
        'consuming': 2,
    }

    # Granularity IDs
    GRANULARITY_IDS = {
        '10min': 3,
        '15min': 4,
        'hourly': 5,
        'daily': 6,
    }

    def __init__(
        self,
        api_key: str,
        energy_types: Optional[List[str]] = None,
        include_forecast: bool = True,
        include_actual: bool = True,
        granularity: str = 'hourly',
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize NED.nl collector.

        Args:
            api_key: NED.nl API key (get from https://ned.nl account)
            energy_types: List of types to fetch: 'solar', 'wind_onshore', 'wind_offshore'
                         Default: all three
            include_forecast: Whether to fetch forecast data
            include_actual: Whether to fetch actual (current/backcast) data
            granularity: Time resolution: '10min', '15min', 'hourly', 'daily'
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="NedCollector",
            data_type="energy_production",
            source="NED.nl (Nationaal Energie Dashboard)",
            units="kW (capacity), kWh (volume)",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.api_key = api_key
        self.energy_types = energy_types or ['solar', 'wind_onshore', 'wind_offshore']
        self.include_forecast = include_forecast
        self.include_actual = include_actual
        self.granularity = granularity

        # Validate energy types
        for et in self.energy_types:
            if et not in self.TYPE_IDS:
                raise ValueError(f"Unknown energy type: {et}. Valid: {list(self.TYPE_IDS.keys())}")

        self.logger.info(
            f"Initialized for types: {self.energy_types}, "
            f"forecast={include_forecast}, actual={include_actual}"
        )

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch energy production data from NED.nl API.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with structure:
            {
                'solar': {'forecast': [...], 'actual': [...]},
                'wind_onshore': {...},
                'wind_offshore': {...}
            }
        """
        try:
            from nednl import NedNL
        except ImportError:
            raise ImportError("nednl package required. Install with: pip install nednl")

        # Format dates for API
        start_str = start_time.strftime('%Y-%m-%d')
        end_str = end_time.strftime('%Y-%m-%d')

        self.logger.debug(f"Fetching NED.nl data from {start_str} to {end_str}")

        results = {}
        granularity_id = self.GRANULARITY_IDS.get(self.granularity, 5)

        async with NedNL(self.api_key) as client:
            for energy_type in self.energy_types:
                type_id = self.TYPE_IDS[energy_type]
                results[energy_type] = {}

                # Determine point_id based on energy type
                if energy_type == 'wind_offshore':
                    point_id = self.POINT_IDS['offshore']
                else:
                    point_id = self.POINT_IDS['netherlands']

                # Fetch forecast data
                if self.include_forecast:
                    try:
                        self.logger.debug(f"Fetching {energy_type} forecast")
                        forecast_data = await client.utilization(
                            point_id=point_id,
                            type_id=type_id,
                            granularity_id=granularity_id,
                            granularity_timezone_id=1,  # CET
                            classification_id=self.CLASSIFICATION_IDS['forecast'],
                            activity_id=self.ACTIVITY_IDS['providing'],
                            start_date=start_str,
                            end_date=end_str,
                        )
                        results[energy_type]['forecast'] = forecast_data
                        self.logger.debug(
                            f"{energy_type} forecast: {len(forecast_data) if forecast_data else 0} records"
                        )
                    except Exception as e:
                        self.logger.warning(f"Failed to fetch {energy_type} forecast: {e}")
                        results[energy_type]['forecast'] = []

                # Fetch actual data (current/backcast)
                if self.include_actual:
                    try:
                        self.logger.debug(f"Fetching {energy_type} actual")
                        actual_data = await client.utilization(
                            point_id=point_id,
                            type_id=type_id,
                            granularity_id=granularity_id,
                            granularity_timezone_id=1,  # CET
                            classification_id=self.CLASSIFICATION_IDS['current'],
                            activity_id=self.ACTIVITY_IDS['providing'],
                            start_date=start_str,
                            end_date=end_str,
                        )
                        results[energy_type]['actual'] = actual_data
                        self.logger.debug(
                            f"{energy_type} actual: {len(actual_data) if actual_data else 0} records"
                        )
                    except Exception as e:
                        self.logger.warning(f"Failed to fetch {energy_type} actual: {e}")
                        results[energy_type]['actual'] = []

        return results

    def _parse_response(
        self,
        raw_data: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse NED.nl API response to standardized format.

        Args:
            raw_data: Dict of energy_type -> {forecast: [...], actual: [...]}
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with structure:
            {
                'solar': {
                    'forecast': {timestamp: {capacity_kw, volume_kwh, ...}},
                    'actual': {timestamp: {...}}
                },
                'wind_onshore': {...},
                'wind_offshore': {...}
            }
        """
        parsed = {}

        for energy_type, data in raw_data.items():
            parsed[energy_type] = {}

            for data_class in ['forecast', 'actual']:
                if data_class not in data or not data[data_class]:
                    continue

                class_data = {}
                records = data[data_class]

                # Handle both list and single object responses
                if not isinstance(records, list):
                    records = [records]

                for record in records:
                    # Extract timestamp - NED.nl uses 'valid_from' or similar
                    timestamp = None
                    if hasattr(record, 'valid_from'):
                        timestamp = record.valid_from
                    elif isinstance(record, dict):
                        timestamp = record.get('valid_from') or record.get('validfrom')

                    if not timestamp:
                        continue

                    # Parse timestamp if it's a string
                    if isinstance(timestamp, str):
                        try:
                            # Try ISO format
                            if 'T' in timestamp:
                                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                            else:
                                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                                dt = dt.replace(tzinfo=ZoneInfo('Europe/Amsterdam'))
                        except ValueError:
                            self.logger.warning(f"Could not parse timestamp: {timestamp}")
                            continue
                    else:
                        dt = timestamp

                    # Normalize to Amsterdam timezone
                    amsterdam_dt = normalize_timestamp_to_amsterdam(dt)
                    ts_key = amsterdam_dt.isoformat()

                    # Extract values
                    values = {}

                    # Capacity (kW)
                    capacity = None
                    if hasattr(record, 'capacity'):
                        capacity = record.capacity
                    elif isinstance(record, dict):
                        capacity = record.get('capacity')
                    if capacity is not None:
                        values['capacity_kw'] = float(capacity)

                    # Volume (kWh)
                    volume = None
                    if hasattr(record, 'volume'):
                        volume = record.volume
                    elif isinstance(record, dict):
                        volume = record.get('volume')
                    if volume is not None:
                        values['volume_kwh'] = float(volume)

                    # Percentage (utilization rate)
                    percentage = None
                    if hasattr(record, 'percentage'):
                        percentage = record.percentage
                    elif isinstance(record, dict):
                        percentage = record.get('percentage')
                    if percentage is not None:
                        values['utilization_pct'] = float(percentage)

                    # CO2 emissions
                    emission = None
                    if hasattr(record, 'emission'):
                        emission = record.emission
                    elif isinstance(record, dict):
                        emission = record.get('emission')
                    if emission is not None:
                        values['co2_kg'] = float(emission)

                    if values:
                        class_data[ts_key] = values

                if class_data:
                    parsed[energy_type][data_class] = class_data
                    self.logger.debug(
                        f"{energy_type} {data_class}: Parsed {len(class_data)} data points"
                    )

        return parsed

    def _normalize_timestamps(
        self,
        data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Override to handle nested energy type structure.
        Timestamps already normalized in _parse_response.
        """
        return data

    def _validate_data(
        self,
        data: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """
        Validate NED.nl data.

        Args:
            data: Dict of energy_type -> {forecast/actual -> timestamp -> values}
            start_time: Expected start time
            end_time: Expected end time

        Returns:
            (is_valid, list of warnings)
        """
        warnings = []

        if not data:
            warnings.append("No data collected from NED.nl")
            return False, warnings

        for energy_type, type_data in data.items():
            if not type_data:
                warnings.append(f"{energy_type}: No data collected")
                continue

            for data_class, class_data in type_data.items():
                if not class_data:
                    warnings.append(f"{energy_type} {data_class}: No data points")
                elif len(class_data) < 6:
                    warnings.append(
                        f"{energy_type} {data_class}: Only {len(class_data)} data points "
                        f"(expected more for the time range)"
                    )

        return len(warnings) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for NED.nl dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'energy_types': self.energy_types,
            'include_forecast': self.include_forecast,
            'include_actual': self.include_actual,
            'granularity': self.granularity,
            'country': 'NL',
            'operators': ['TenneT', 'Gasunie'],
            'api_rate_limit': '200 requests per 5 minutes',
            'description': 'Dutch energy production data from Nationaal Energie Dashboard'
        })

        return metadata


# Convenience function
async def get_ned_production(
    api_key: str,
    start_time: datetime,
    end_time: datetime,
    energy_types: List[str] = None
):
    """
    Fetch NED.nl energy production data.

    Args:
        api_key: NED.nl API key
        start_time: Start of time range
        end_time: End of time range
        energy_types: List of types to fetch (default: all)

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = NedCollector(api_key=api_key, energy_types=energy_types)
    return await collector.collect(start_time=start_time, end_time=end_time)


# Example usage
async def main():
    """Example usage of NedCollector."""
    import os
    from configparser import ConfigParser

    # Load API key
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, '..', 'secrets.ini')

    config = ConfigParser()
    config.read(secrets_file)

    try:
        api_key = config.get('api_keys', 'ned')
    except Exception:
        print("NED.nl API key not found in secrets.ini")
        print("Add [api_keys] ned = YOUR_KEY to secrets.ini")
        return

    # Setup time range
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    # Create collector and fetch data
    collector = NedCollector(
        api_key=api_key,
        energy_types=['solar', 'wind_onshore', 'wind_offshore']
    )
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected NED.nl data")
        for energy_type, data in dataset.data.items():
            print(f"\n{energy_type}:")
            for data_class, class_data in data.items():
                if isinstance(class_data, dict):
                    print(f"  {data_class}: {len(class_data)} timestamps")
                    # Show sample
                    for ts, values in list(class_data.items())[:2]:
                        print(f"    {ts}: {values}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    import platform
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
