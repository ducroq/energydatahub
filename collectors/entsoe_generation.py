"""
ENTSO-E Generation by Type Collector
------------------------------------
Collects electricity generation by type from ENTSO-E Transparency Platform.

File: collectors/entsoe_generation.py
Created: 2025-12-01
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for ENTSO-E generation by type. Fetches
    forecasted and actual generation for specific fuel types (nuclear, fossil gas,
    etc.). French nuclear availability significantly impacts European electricity prices.

    Key features:
    - Generation by fuel type (MW)
    - Nuclear availability (key price driver)
    - Fossil gas generation (marginal price setter)
    - Multi-country support

Usage:
    from collectors.entsoe_generation import EntsoeGenerationCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = EntsoeGenerationCollector(
        api_key="your_api_key",
        generation_types=['nuclear', 'fossil_gas']
    )
    data = await collector.collect(start, end)

API Documentation:
    https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
    Document types:
    - A71: Generation forecast - day ahead
    - A73: Generation forecast - intraday
    - A75: Actual generation per type
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
import pandas as pd
from entsoe import EntsoePandasClient
from functools import partial

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class EntsoeGenerationCollector(BaseCollector):
    """
    Collector for ENTSO-E generation by type.

    Fetches generation data by fuel type, with focus on nuclear and
    gas which are key drivers of electricity prices.
    """

    # PSR (Power System Resource) type codes for ENTSO-E
    # See: https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_psrtype
    PSR_TYPES = {
        'nuclear': 'B14',
        'fossil_gas': 'B04',
        'fossil_hard_coal': 'B05',
        'fossil_lignite': 'B06',
        'hydro_pumped_storage': 'B10',
        'hydro_run_of_river': 'B11',
        'hydro_reservoir': 'B12',
        'wind_onshore': 'B18',
        'wind_offshore': 'B19',
        'solar': 'B16',
    }

    # Display names
    TYPE_NAMES = {
        'nuclear': 'Nuclear',
        'fossil_gas': 'Fossil Gas',
        'fossil_hard_coal': 'Hard Coal',
        'fossil_lignite': 'Lignite',
        'hydro_pumped_storage': 'Pumped Storage',
        'hydro_run_of_river': 'Run-of-River Hydro',
        'hydro_reservoir': 'Reservoir Hydro',
        'wind_onshore': 'Wind Onshore',
        'wind_offshore': 'Wind Offshore',
        'solar': 'Solar',
    }

    ZONE_NAMES = {
        'FR': 'France',
        'DE_LU': 'Germany-Luxembourg',
        'BE': 'Belgium',
        'NL': 'Netherlands',
    }

    def __init__(
        self,
        api_key: str,
        country_codes: Optional[List[str]] = None,
        generation_types: Optional[List[str]] = None,
        include_forecast: bool = True,
        include_actual: bool = True,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize ENTSO-E Generation collector.

        Args:
            api_key: ENTSO-E API key
            country_codes: List of country codes (default: ['FR'] for nuclear focus)
            generation_types: List of generation types to fetch (default: ['nuclear'])
                            Options: nuclear, fossil_gas, fossil_hard_coal, etc.
            include_forecast: Fetch day-ahead generation forecast
            include_actual: Fetch actual generation
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="EntsoeGenerationCollector",
            data_type="generation_by_type",
            source="ENTSO-E Transparency Platform API v1.3",
            units="MW",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.api_key = api_key
        self.country_codes = country_codes or ['FR']
        self.generation_types = generation_types or ['nuclear']
        self.include_forecast = include_forecast
        self.include_actual = include_actual

        # Validate generation types
        invalid_types = [t for t in self.generation_types if t not in self.PSR_TYPES]
        if invalid_types:
            raise ValueError(f"Invalid generation types: {invalid_types}. Valid: {list(self.PSR_TYPES.keys())}")

        self.logger.info(
            f"Initialized for {self.country_codes}, types: {self.generation_types}, "
            f"forecast={include_forecast}, actual={include_actual}"
        )

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch generation data from ENTSO-E.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with country -> {type -> {'forecast': df, 'actual': df}}

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(
            f"Fetching generation for {self.country_codes}, types: {self.generation_types}"
        )

        start_timestamp = pd.Timestamp(start_time).tz_convert('UTC')
        end_timestamp = pd.Timestamp(end_time).tz_convert('UTC')

        self.logger.debug(f"Query range: {start_timestamp} to {end_timestamp} (UTC)")

        client = EntsoePandasClient(api_key=self.api_key)
        results = {}
        loop = asyncio.get_running_loop()

        for code in self.country_codes:
            country_results = {}

            # Fetch actual generation per type (includes all types in one call)
            if self.include_actual:
                try:
                    self.logger.debug(f"Fetching actual generation for {code}")

                    query_func = partial(
                        client.query_generation,
                        country_code=code,
                        start=start_timestamp,
                        end=end_timestamp,
                        psr_type=None  # Get all types
                    )

                    actual_df = await loop.run_in_executor(None, query_func)

                    if actual_df is not None and not actual_df.empty:
                        # Filter to requested types
                        for gen_type in self.generation_types:
                            type_name = self.TYPE_NAMES.get(gen_type, gen_type)
                            # ENTSO-E returns columns like 'Nuclear' or 'Fossil Gas'
                            matching_cols = [
                                c for c in actual_df.columns
                                if type_name.lower() in str(c).lower()
                            ]
                            if matching_cols:
                                if gen_type not in country_results:
                                    country_results[gen_type] = {}
                                country_results[gen_type]['actual'] = actual_df[matching_cols[0]]
                                self.logger.debug(f"{code} {gen_type} actual: {len(actual_df)} points")

                except Exception as e:
                    self.logger.warning(f"{code} actual generation failed: {e}")

            # Fetch generation forecast
            if self.include_forecast:
                try:
                    self.logger.debug(f"Fetching generation forecast for {code}")

                    query_func = partial(
                        client.query_generation_forecast,
                        country_code=code,
                        start=start_timestamp,
                        end=end_timestamp
                    )

                    forecast_df = await loop.run_in_executor(None, query_func)

                    if forecast_df is not None and not forecast_df.empty:
                        for gen_type in self.generation_types:
                            type_name = self.TYPE_NAMES.get(gen_type, gen_type)
                            matching_cols = [
                                c for c in forecast_df.columns
                                if type_name.lower() in str(c).lower()
                            ]
                            if matching_cols:
                                if gen_type not in country_results:
                                    country_results[gen_type] = {}
                                country_results[gen_type]['forecast'] = forecast_df[matching_cols[0]]
                                self.logger.debug(f"{code} {gen_type} forecast: {len(forecast_df)} points")

                except Exception as e:
                    self.logger.warning(f"{code} generation forecast failed: {e}")

            if country_results:
                results[code] = country_results

        if not results:
            raise ValueError("No generation data returned")

        return results

    def _parse_response(
        self,
        raw_data: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse ENTSO-E generation response to standardized format.

        Args:
            raw_data: Dict of country -> {type -> {'forecast': Series, 'actual': Series}}
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with structure:
            {
                'FR': {
                    '2025-12-01T00:00:00+01:00': {
                        'nuclear_forecast': 45000.0,
                        'nuclear_actual': 44500.0,
                        'nuclear_availability': 0.72  # % of installed capacity
                    },
                    ...
                }
            }
        """
        # Installed capacity for availability calculation (approximate GW)
        INSTALLED_CAPACITY = {
            'FR': {'nuclear': 61000},  # 61 GW French nuclear
            'BE': {'nuclear': 5900},   # ~5.9 GW Belgian nuclear
            'DE_LU': {'nuclear': 0},   # Germany phased out
        }

        parsed = {}

        for country_code, type_data in raw_data.items():
            country_parsed = {}

            # Collect all timestamps
            all_timestamps = set()
            for gen_type, data_dict in type_data.items():
                if 'actual' in data_dict:
                    all_timestamps.update(data_dict['actual'].index)
                if 'forecast' in data_dict:
                    all_timestamps.update(data_dict['forecast'].index)

            for timestamp in sorted(all_timestamps):
                dt = timestamp.to_pydatetime()

                if start_time <= dt < end_time:
                    amsterdam_dt = normalize_timestamp_to_amsterdam(dt)
                    ts_key = amsterdam_dt.isoformat()

                    gen_data = {}

                    for gen_type, data_dict in type_data.items():
                        # Get actual
                        if 'actual' in data_dict and timestamp in data_dict['actual'].index:
                            val = data_dict['actual'].loc[timestamp]
                            if pd.notna(val):
                                gen_data[f'{gen_type}_actual'] = float(val)

                        # Get forecast
                        if 'forecast' in data_dict and timestamp in data_dict['forecast'].index:
                            val = data_dict['forecast'].loc[timestamp]
                            if pd.notna(val):
                                gen_data[f'{gen_type}_forecast'] = float(val)

                        # Calculate availability for nuclear
                        if gen_type == 'nuclear':
                            capacity = INSTALLED_CAPACITY.get(country_code, {}).get('nuclear', 0)
                            if capacity > 0:
                                actual = gen_data.get('nuclear_actual', gen_data.get('nuclear_forecast'))
                                if actual:
                                    gen_data['nuclear_availability'] = round(actual / capacity, 3)

                    if gen_data:
                        country_parsed[ts_key] = gen_data

            if country_parsed:
                parsed[country_code] = country_parsed
                self.logger.debug(f"{country_code}: Parsed {len(country_parsed)} generation points")

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
        """Validate generation data."""
        warnings = []

        if not data:
            warnings.append("No generation data collected")
            return False, warnings

        for country_code, country_data in data.items():
            if not country_data:
                warnings.append(f"{country_code}: No data points")
                continue

            if len(country_data) < 12:
                warnings.append(f"{country_code}: Only {len(country_data)} points")

        return len(warnings) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get metadata for generation dataset."""
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'country_codes': self.country_codes,
            'zones': {code: self.ZONE_NAMES.get(code, code) for code in self.country_codes},
            'generation_types': self.generation_types,
            'type_names': {t: self.TYPE_NAMES.get(t, t) for t in self.generation_types},
            'include_forecast': self.include_forecast,
            'include_actual': self.include_actual,
            'resolution': 'hourly',
            'api_version': 'v1.3',
            'description': 'Generation by fuel type from ENTSO-E'
        })

        return metadata


# Example usage
async def main():
    """Example usage of EntsoeGenerationCollector."""
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

    # Focus on French nuclear (key price driver)
    collector = EntsoeGenerationCollector(
        api_key=api_key,
        country_codes=['FR'],
        generation_types=['nuclear'],
        include_actual=True,
        include_forecast=True
    )
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected generation data for {len(dataset.data)} countries")
        for country, data in dataset.data.items():
            print(f"\n{country} ({collector.ZONE_NAMES.get(country, country)}):")
            for ts, values in list(data.items())[:5]:
                print(f"  {ts}:")
                for k, v in values.items():
                    if 'availability' in k:
                        print(f"    {k}: {v*100:.1f}%")
                    else:
                        print(f"    {k}: {v:.0f} MW")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
