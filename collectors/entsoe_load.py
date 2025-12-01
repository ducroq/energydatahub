"""
ENTSO-E Load Forecast Collector
-------------------------------
Collects electricity load (demand) forecasts from ENTSO-E Transparency Platform.

File: collectors/entsoe_load.py
Created: 2025-12-01
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for ENTSO-E load forecasts. Fetches day-ahead
    and week-ahead load forecasts for Netherlands and neighboring countries.
    Load forecasts directly impact electricity price formation.

    Key features:
    - Day-ahead load forecast (MW)
    - Week-ahead load forecast
    - Actual load for comparison
    - Multi-country support

Usage:
    from collectors.entsoe_load import EntsoeLoadCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = EntsoeLoadCollector(api_key="your_api_key")
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=2)

    data = await collector.collect(start, end)

API Documentation:
    https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
    Document types:
    - A65: Total load - day ahead forecast
    - A66: Total load - week ahead forecast
    - A67: Total load - actual
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
import pandas as pd
from entsoe import EntsoePandasClient
from functools import partial

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class EntsoeLoadCollector(BaseCollector):
    """
    Collector for ENTSO-E load forecasts.

    Fetches day-ahead load forecasts from ENTSO-E Transparency Platform.
    Load (demand) is a key driver of electricity prices.
    """

    # Supported country codes
    SUPPORTED_COUNTRIES = ['NL', 'DE_LU', 'BE', 'FR']

    ZONE_NAMES = {
        'NL': 'Netherlands',
        'DE_LU': 'Germany-Luxembourg',
        'BE': 'Belgium',
        'FR': 'France',
    }

    def __init__(
        self,
        api_key: str,
        country_codes: Optional[List[str]] = None,
        include_actual: bool = True,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize ENTSO-E Load collector.

        Args:
            api_key: ENTSO-E API key
            country_codes: List of country codes (default: ['NL', 'DE_LU'])
            include_actual: Also fetch actual load for comparison
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="EntsoeLoadCollector",
            data_type="load_forecast",
            source="ENTSO-E Transparency Platform API v1.3",
            units="MW",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.api_key = api_key
        self.country_codes = country_codes or ['NL', 'DE_LU']
        self.include_actual = include_actual

        self.logger.info(
            f"Initialized for countries: {', '.join(self.country_codes)}, "
            f"include_actual={include_actual}"
        )

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict[str, Dict[str, pd.Series]]:
        """
        Fetch load forecasts and actuals from ENTSO-E.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict mapping country codes to {'forecast': Series, 'actual': Series}

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(f"Fetching load forecasts for: {self.country_codes}")

        # Convert to pandas Timestamp and UTC for API
        start_timestamp = pd.Timestamp(start_time).tz_convert('UTC')
        end_timestamp = pd.Timestamp(end_time).tz_convert('UTC')

        self.logger.debug(f"Query range: {start_timestamp} to {end_timestamp} (UTC)")

        client = EntsoePandasClient(api_key=self.api_key)
        results = {}
        loop = asyncio.get_running_loop()

        for code in self.country_codes:
            country_data = {}

            # Fetch day-ahead forecast
            try:
                self.logger.debug(f"Fetching load forecast for {code}")

                query_func = partial(
                    client.query_load_forecast,
                    country_code=code,
                    start=start_timestamp,
                    end=end_timestamp
                )

                forecast = await loop.run_in_executor(None, query_func)

                if forecast is not None and not forecast.empty:
                    country_data['forecast'] = forecast
                    self.logger.debug(f"{code} forecast: {len(forecast)} points")
                else:
                    self.logger.warning(f"{code}: No forecast data")

            except Exception as e:
                self.logger.warning(f"{code} forecast failed: {e}")

            # Fetch actual load if requested
            if self.include_actual:
                try:
                    self.logger.debug(f"Fetching actual load for {code}")

                    query_func = partial(
                        client.query_load,
                        country_code=code,
                        start=start_timestamp,
                        end=end_timestamp
                    )

                    actual = await loop.run_in_executor(None, query_func)

                    if actual is not None and not actual.empty:
                        country_data['actual'] = actual
                        self.logger.debug(f"{code} actual: {len(actual)} points")
                    else:
                        self.logger.warning(f"{code}: No actual data")

                except Exception as e:
                    self.logger.warning(f"{code} actual failed: {e}")

            if country_data:
                results[code] = country_data

        if not results:
            raise ValueError("No load data returned from any country")

        return results

    def _parse_response(
        self,
        raw_data: Dict[str, Dict[str, pd.Series]],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse ENTSO-E load response to standardized format.

        Args:
            raw_data: Dict of country -> {'forecast': Series, 'actual': Series}
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with structure:
            {
                'NL': {
                    '2025-12-01T00:00:00+01:00': {
                        'load_forecast': 12500.0,
                        'load_actual': 12300.0,
                        'forecast_error': 200.0
                    },
                    ...
                },
                'DE_LU': {...}
            }
        """
        parsed = {}

        for country_code, data_dict in raw_data.items():
            country_data = {}

            forecast = data_dict.get('forecast')
            actual = data_dict.get('actual')

            # Get all timestamps from both series
            all_timestamps = set()
            if forecast is not None:
                all_timestamps.update(forecast.index)
            if actual is not None:
                all_timestamps.update(actual.index)

            for timestamp in sorted(all_timestamps):
                dt = timestamp.to_pydatetime()

                if start_time <= dt < end_time:
                    amsterdam_dt = normalize_timestamp_to_amsterdam(dt)
                    ts_key = amsterdam_dt.isoformat()

                    load_data = {}

                    # Get forecast value
                    if forecast is not None and timestamp in forecast.index:
                        val = forecast.loc[timestamp]
                        if isinstance(val, pd.Series):
                            val = val.iloc[0]
                        if pd.notna(val):
                            load_data['load_forecast'] = float(val)

                    # Get actual value
                    if actual is not None and timestamp in actual.index:
                        val = actual.loc[timestamp]
                        if isinstance(val, pd.Series):
                            val = val.iloc[0]
                        if pd.notna(val):
                            load_data['load_actual'] = float(val)

                    # Calculate forecast error if both available
                    if 'load_forecast' in load_data and 'load_actual' in load_data:
                        load_data['forecast_error'] = round(
                            load_data['load_forecast'] - load_data['load_actual'], 1
                        )

                    if load_data:
                        country_data[ts_key] = load_data

            if country_data:
                parsed[country_code] = country_data
                self.logger.debug(f"{country_code}: Parsed {len(country_data)} load points")

        return parsed

    def _normalize_timestamps(self, data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Timestamps already normalized in _parse_response."""
        return data

    def _validate_data(
        self,
        data: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """Validate load forecast data."""
        warnings = []

        if not data:
            warnings.append("No load data collected")
            return False, warnings

        for country_code, country_data in data.items():
            if not country_data:
                warnings.append(f"{country_code}: No data points")
                continue

            if len(country_data) < 12:
                warnings.append(
                    f"{country_code}: Only {len(country_data)} points (expected at least 12)"
                )

            # Check for forecast values
            missing_forecast = sum(
                1 for v in country_data.values() if 'load_forecast' not in v
            )
            if missing_forecast > len(country_data) * 0.5:
                warnings.append(f"{country_code}: {missing_forecast} points missing forecast")

        return len(warnings) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get metadata for load forecast dataset."""
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'country_codes': self.country_codes,
            'zones': {code: self.ZONE_NAMES.get(code, code) for code in self.country_codes},
            'include_actual': self.include_actual,
            'forecast_type': 'day-ahead',
            'resolution': 'hourly',
            'api_version': 'v1.3',
            'description': 'Electricity load (demand) forecasts from ENTSO-E'
        })

        return metadata


# Example usage
async def main():
    """Example usage of EntsoeLoadCollector."""
    import os
    from configparser import ConfigParser
    from zoneinfo import ZoneInfo
    from datetime import timedelta

    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, '..', 'secrets.ini')

    config = ConfigParser()
    config.read(secrets_file)
    api_key = config.get('api_keys', 'entsoe')

    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    collector = EntsoeLoadCollector(
        api_key=api_key,
        country_codes=['NL', 'DE_LU'],
        include_actual=True
    )
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected load data for {len(dataset.data)} countries")
        for country, data in dataset.data.items():
            print(f"\n{country} ({collector.ZONE_NAMES.get(country, country)}):")
            for ts, values in list(data.items())[:3]:
                print(f"  {ts}: {values}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
