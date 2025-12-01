"""
ENTSO-E Wind Generation Forecast Collector
-------------------------------------------
Collects wind power generation forecasts from ENTSO-E Transparency Platform.

File: collectors/entsoe_wind.py
Created: 2025-12-01
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for ENTSO-E wind generation forecasts. Fetches
    day-ahead wind power generation forecasts for Netherlands and neighboring
    countries relevant to Dutch energy price prediction.

    Key features:
    - Day-ahead wind power generation forecasts (MW)
    - Offshore and onshore wind breakdown where available
    - Multi-country support (NL, DE, BE, DK)
    - Direct correlation to energy price movements

Usage:
    from collectors.entsoe_wind import EntsoeWindCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = EntsoeWindCollector(api_key="your_api_key")
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=1)

    data = await collector.collect(start, end, country_code='NL')

API Documentation:
    https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
    Document types:
    - A69: Wind and solar generation forecasts - day ahead
    - A75: Actual generation per type (for verification)
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
import pandas as pd
from entsoe import EntsoePandasClient
from functools import partial

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class EntsoeWindCollector(BaseCollector):
    """
    Collector for ENTSO-E wind power generation forecasts.

    Fetches day-ahead wind generation forecasts from the ENTSO-E
    Transparency Platform. This data directly impacts electricity
    prices as wind power has near-zero marginal cost.
    """

    # Country codes for wind-relevant markets
    SUPPORTED_COUNTRIES = ['NL', 'DE_LU', 'BE', 'DK_1', 'DK_2']

    # Bidding zone mappings for clearer output
    ZONE_NAMES = {
        'NL': 'Netherlands',
        'DE_LU': 'Germany-Luxembourg',
        'BE': 'Belgium',
        'DK_1': 'Denmark-West',
        'DK_2': 'Denmark-East'
    }

    def __init__(
        self,
        api_key: str,
        country_codes: Optional[List[str]] = None,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize ENTSO-E Wind collector.

        Args:
            api_key: ENTSO-E API key
            country_codes: List of country codes to fetch (default: ['NL', 'DE_LU'])
                          Use SUPPORTED_COUNTRIES for full coverage
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="EntsoeWindCollector",
            data_type="wind_generation",
            source="ENTSO-E Transparency Platform API v1.3",
            units="MW",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.api_key = api_key
        self.country_codes = country_codes or ['NL', 'DE_LU']

        self.logger.info(
            f"Initialized for countries: {', '.join(self.country_codes)}"
        )

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        country_code: Optional[str] = None,
        **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch wind generation forecasts from ENTSO-E.

        Args:
            start_time: Start of time range
            end_time: End of time range
            country_code: Optional single country override

        Returns:
            Dict mapping country codes to pandas DataFrames with wind data

        Raises:
            Exception: If API call fails
        """
        # Determine which countries to fetch
        countries = [country_code] if country_code else self.country_codes

        self.logger.debug(f"Fetching wind forecasts for: {countries}")

        # Convert to pandas Timestamp and UTC for API
        start_timestamp = pd.Timestamp(start_time).tz_convert('UTC')
        end_timestamp = pd.Timestamp(end_time).tz_convert('UTC')

        self.logger.debug(
            f"Query range: {start_timestamp} to {end_timestamp} (UTC)"
        )

        # Create client
        client = EntsoePandasClient(api_key=self.api_key)

        # Fetch data for each country
        results = {}
        loop = asyncio.get_running_loop()

        for code in countries:
            try:
                self.logger.debug(f"Fetching wind forecast for {code}")

                # query_wind_and_solar_forecast returns wind and solar forecasts
                query_func = partial(
                    client.query_wind_and_solar_forecast,
                    country_code=code,
                    start=start_timestamp,
                    end=end_timestamp,
                    psr_type=None  # Get all types (wind onshore, wind offshore, solar)
                )

                # Execute in thread pool to not block event loop
                data = await loop.run_in_executor(None, query_func)

                if data is not None and not data.empty:
                    results[code] = data
                    self.logger.debug(
                        f"{code}: Got {len(data)} data points, columns: {list(data.columns)}"
                    )
                else:
                    self.logger.warning(f"{code}: No wind forecast data returned")

            except Exception as e:
                self.logger.warning(f"{code}: Failed to fetch wind data: {e}")
                # Continue with other countries
                continue

        if not results:
            raise ValueError("No wind forecast data returned from any country")

        return results

    def _parse_response(
        self,
        raw_data: Dict[str, pd.DataFrame],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse ENTSO-E wind forecast response to standardized format.

        Args:
            raw_data: Dict of country code -> DataFrame
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with structure:
            {
                'NL': {
                    '2025-12-01T00:00:00+01:00': {
                        'wind_offshore': 1500.0,
                        'wind_onshore': 800.0,
                        'wind_total': 2300.0
                    },
                    ...
                },
                'DE_LU': {...},
                ...
            }
        """
        parsed = {}

        for country_code, df in raw_data.items():
            country_data = {}

            # ENTSO-E returns different column names depending on availability
            # Common patterns: 'Wind Offshore', 'Wind Onshore', 'Solar'
            # Or combined as 'Wind' for some countries

            wind_offshore_col = None
            wind_onshore_col = None
            wind_total_col = None

            # Find relevant columns (case-insensitive search)
            for col in df.columns:
                col_lower = col.lower() if isinstance(col, str) else str(col).lower()
                if 'wind' in col_lower and 'offshore' in col_lower:
                    wind_offshore_col = col
                elif 'wind' in col_lower and 'onshore' in col_lower:
                    wind_onshore_col = col
                elif 'wind' in col_lower and 'offshore' not in col_lower and 'onshore' not in col_lower:
                    wind_total_col = col

            self.logger.debug(
                f"{country_code} columns found - "
                f"offshore: {wind_offshore_col}, onshore: {wind_onshore_col}, total: {wind_total_col}"
            )

            for timestamp, row in df.iterrows():
                # Convert to datetime
                dt = timestamp.to_pydatetime()

                # Filter to requested time range
                if start_time <= dt < end_time:
                    # Normalize to Amsterdam timezone
                    amsterdam_dt = normalize_timestamp_to_amsterdam(dt)
                    ts_key = amsterdam_dt.isoformat()

                    # Extract wind values
                    wind_data = {}

                    if wind_offshore_col and pd.notna(row.get(wind_offshore_col)):
                        wind_data['wind_offshore'] = float(row[wind_offshore_col])

                    if wind_onshore_col and pd.notna(row.get(wind_onshore_col)):
                        wind_data['wind_onshore'] = float(row[wind_onshore_col])

                    if wind_total_col and pd.notna(row.get(wind_total_col)):
                        wind_data['wind_total'] = float(row[wind_total_col])

                    # Calculate total if we have offshore + onshore but no total
                    if 'wind_offshore' in wind_data and 'wind_onshore' in wind_data and 'wind_total' not in wind_data:
                        wind_data['wind_total'] = wind_data['wind_offshore'] + wind_data['wind_onshore']

                    # If we only have total, use it
                    if 'wind_total' in wind_data and 'wind_offshore' not in wind_data:
                        pass  # Keep just total

                    if wind_data:
                        country_data[ts_key] = wind_data

            if country_data:
                parsed[country_code] = country_data
                self.logger.debug(
                    f"{country_code}: Parsed {len(country_data)} wind forecast points"
                )

        return parsed

    def _normalize_timestamps(
        self,
        data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Override to handle nested country structure.
        Timestamps already normalized in _parse_response.
        """
        # Data is already normalized during parsing
        return data

    def _validate_data(
        self,
        data: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """
        Validate multi-country wind data.

        Args:
            data: Dict of country -> timestamp -> wind_data
            start_time: Expected start time
            end_time: Expected end time

        Returns:
            (is_valid, list of warnings)
        """
        warnings = []

        if not data:
            warnings.append("No wind data collected from any country")
            return False, warnings

        for country_code, country_data in data.items():
            if not country_data:
                warnings.append(f"{country_code}: No data points collected")
                continue

            # Check data point count (expect at least 12 hours for day-ahead)
            if len(country_data) < 12:
                warnings.append(
                    f"{country_code}: Only {len(country_data)} data points "
                    f"(expected at least 12 for day-ahead forecast)"
                )

            # Check for completeness of wind values
            missing_total = 0
            for ts, wind_vals in country_data.items():
                if 'wind_total' not in wind_vals and 'wind_offshore' not in wind_vals and 'wind_onshore' not in wind_vals:
                    missing_total += 1

            if missing_total > 0:
                warnings.append(
                    f"{country_code}: {missing_total} points missing wind values"
                )

        return len(warnings) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for wind generation dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'country_codes': self.country_codes,
            'zones': {code: self.ZONE_NAMES.get(code, code) for code in self.country_codes},
            'forecast_type': 'day-ahead',
            'resolution': 'hourly',
            'api_version': 'v1.3',
            'description': 'Wind power generation forecasts from ENTSO-E Transparency Platform'
        })

        return metadata


# Backward compatibility function
async def get_entsoe_wind_forecast(
    api_key: str,
    country_codes: List[str],
    start_time: datetime,
    end_time: datetime
):
    """
    Fetch ENTSO-E wind generation forecasts.

    Args:
        api_key: ENTSO-E API key
        country_codes: List of country codes
        start_time: Start of time range
        end_time: End of time range

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = EntsoeWindCollector(api_key=api_key, country_codes=country_codes)
    return await collector.collect(start_time=start_time, end_time=end_time)


# Example usage
async def main():
    """Example usage of EntsoeWindCollector."""
    import os
    from configparser import ConfigParser
    from zoneinfo import ZoneInfo
    from datetime import timedelta

    # Load API key
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, '..', 'secrets.ini')

    config = ConfigParser()
    config.read(secrets_file)
    api_key = config.get('api_keys', 'entsoe')

    # Setup time range
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=2)

    # Create collector and fetch data
    collector = EntsoeWindCollector(
        api_key=api_key,
        country_codes=['NL', 'DE_LU', 'BE']
    )
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected wind data for {len(dataset.data)} countries")
        for country, data in dataset.data.items():
            print(f"\n{country} ({collector.ZONE_NAMES.get(country, country)}):")
            for timestamp, values in list(data.items())[:3]:
                print(f"  {timestamp}: {values}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
