"""
Open-Meteo Solar Radiation Collector
------------------------------------
Collects solar irradiance forecast data from Open-Meteo API for multiple locations.

File: collectors/openmeteo_solar.py
Created: 2025-12-01
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for Open-Meteo solar radiation forecasts.
    Fetches Global Horizontal Irradiance (GHI), Direct Normal Irradiance (DNI),
    and Diffuse Horizontal Irradiance (DHI) for price prediction models.

    Solar irradiance affects electricity SUPPLY through solar panel production.
    Multi-location data covers major solar production areas in NL and neighboring countries.

    Key features:
    - FREE API (no key required)
    - Up to 16 days forecast
    - Hourly resolution
    - Multiple solar radiation components (GHI, DNI, DHI)
    - Global coverage

Usage:
    from collectors.openmeteo_solar import OpenMeteoSolarCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    locations = [
        {"name": "Rotterdam_NL", "lat": 51.9225, "lon": 4.4792},
        {"name": "Eindhoven_NL", "lat": 51.4416, "lon": 5.4697},
    ]
    collector = OpenMeteoSolarCollector(locations=locations)

    start = datetime.now(ZoneInfo('Europe/Amsterdam'))
    end = start + timedelta(days=7)

    data = await collector.collect(start, end)

API Documentation:
    https://open-meteo.com/en/docs
    Rate limit: 10,000 requests/day (free tier)
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo
import aiohttp

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class OpenMeteoSolarCollector(BaseCollector):
    """
    Collector for Open-Meteo solar radiation forecasts.

    Fetches solar irradiance data for multiple locations to support
    electricity price prediction (supply side - solar production).
    """

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    # Solar radiation variables to fetch
    SOLAR_VARIABLES = [
        "shortwave_radiation",      # GHI - Global Horizontal Irradiance (W/m²)
        "direct_radiation",         # Direct radiation on horizontal surface (W/m²)
        "diffuse_radiation",        # DHI - Diffuse Horizontal Irradiance (W/m²)
        "direct_normal_irradiance", # DNI - Direct Normal Irradiance (W/m²)
        "cloud_cover",              # Cloud cover percentage (for context)
    ]

    def __init__(
        self,
        locations: List[Dict[str, Any]],
        forecast_days: int = 7,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize Open-Meteo Solar collector.

        Args:
            locations: List of location dicts for multi-location collection
                      Format: [{"name": "Rotterdam_NL", "lat": 51.9225, "lon": 4.4792}, ...]
            forecast_days: Number of forecast days (1-16, default 7)
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="OpenMeteoSolarCollector",
            data_type="solar_irradiance",
            source="Open-Meteo API (free)",
            units="W/m² (irradiance), % (cloud_cover)",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.locations = locations
        self.forecast_days = min(max(forecast_days, 1), 16)  # Clamp to 1-16

        self.logger.info(
            f"Initialized for {len(locations)} locations, {self.forecast_days} day forecast"
        )

    async def _fetch_location_data(
        self,
        session: aiohttp.ClientSession,
        location: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetch solar radiation data for a single location.

        Args:
            session: aiohttp session
            location: Location dict with name, lat, lon

        Returns:
            Dict with location name and hourly data
        """
        params = {
            "latitude": location["lat"],
            "longitude": location["lon"],
            "hourly": ",".join(self.SOLAR_VARIABLES),
            "forecast_days": self.forecast_days,
            "timezone": "Europe/Amsterdam",
        }

        try:
            async with session.get(self.BASE_URL, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    self.logger.warning(
                        f"{location['name']}: HTTP {response.status} - {error_text[:200]}"
                    )
                    return {"name": location["name"], "data": None, "error": error_text}

                data = await response.json()
                return {"name": location["name"], "data": data, "error": None}

        except Exception as e:
            self.logger.warning(f"{location['name']}: Request failed - {e}")
            return {"name": location["name"], "data": None, "error": str(e)}

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch solar radiation data from Open-Meteo API for all locations.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict mapping location names to their hourly data
        """
        self.logger.debug(f"Fetching solar data for {len(self.locations)} locations")

        results = {}
        # Use semaphore to limit concurrent requests and avoid rate limiting (HTTP 429)
        semaphore = asyncio.Semaphore(3)  # Max 3 concurrent requests

        async def fetch_with_rate_limit(session: aiohttp.ClientSession, location: Dict[str, Any]):
            async with semaphore:
                await asyncio.sleep(0.1)  # Small delay between requests
                return await self._fetch_location_data(session, location)

        async with aiohttp.ClientSession() as session:
            tasks = [fetch_with_rate_limit(session, loc) for loc in self.locations]
            responses = await asyncio.gather(*tasks)

            for response in responses:
                location_name = response["name"]
                if response["data"]:
                    results[location_name] = response["data"]
                    self.logger.debug(f"{location_name}: Got solar data")
                else:
                    self.logger.warning(f"{location_name}: No data - {response.get('error', 'unknown')}")

        return results

    def _parse_response(
        self,
        raw_data: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse Open-Meteo API response to standardized format.

        Args:
            raw_data: Dict of location_name -> API response
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with structure:
            {
                'Rotterdam_NL': {
                    '2025-12-01T12:00:00+01:00': {
                        'ghi': 450.0,
                        'dni': 600.0,
                        'dhi': 100.0,
                        'direct': 350.0,
                        'cloud_cover': 25.0
                    },
                    ...
                },
                ...
            }
        """
        parsed = {}
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')

        for location_name, api_data in raw_data.items():
            if not api_data or "hourly" not in api_data:
                continue

            hourly = api_data["hourly"]
            times = hourly.get("time", [])

            location_data = {}
            for i, time_str in enumerate(times):
                # Parse timestamp
                try:
                    # Open-Meteo returns ISO format without timezone
                    dt = datetime.fromisoformat(time_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=amsterdam_tz)
                except ValueError:
                    continue

                # Filter to requested time range
                if dt < start_time or dt > end_time:
                    continue

                # Extract solar values
                values = {}

                # GHI - Global Horizontal Irradiance
                if "shortwave_radiation" in hourly and i < len(hourly["shortwave_radiation"]):
                    val = hourly["shortwave_radiation"][i]
                    if val is not None:
                        values["ghi"] = float(val)

                # Direct radiation
                if "direct_radiation" in hourly and i < len(hourly["direct_radiation"]):
                    val = hourly["direct_radiation"][i]
                    if val is not None:
                        values["direct"] = float(val)

                # DHI - Diffuse Horizontal Irradiance
                if "diffuse_radiation" in hourly and i < len(hourly["diffuse_radiation"]):
                    val = hourly["diffuse_radiation"][i]
                    if val is not None:
                        values["dhi"] = float(val)

                # DNI - Direct Normal Irradiance
                if "direct_normal_irradiance" in hourly and i < len(hourly["direct_normal_irradiance"]):
                    val = hourly["direct_normal_irradiance"][i]
                    if val is not None:
                        values["dni"] = float(val)

                # Cloud cover (for context)
                if "cloud_cover" in hourly and i < len(hourly["cloud_cover"]):
                    val = hourly["cloud_cover"][i]
                    if val is not None:
                        values["cloud_cover"] = float(val)

                if values:
                    ts_key = dt.isoformat()
                    location_data[ts_key] = values

            if location_data:
                parsed[location_name] = location_data
                self.logger.debug(f"{location_name}: Parsed {len(location_data)} timestamps")

        return parsed

    def _normalize_timestamps(
        self,
        data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Override base class method - timestamps already normalized in _parse_response.

        The base class expects a flat dict {timestamp: value}, but we return
        a nested dict {location_name: {timestamp: values}}. Skip normalization
        since timestamps are already in correct format.
        """
        return data

    def _validate_data(
        self,
        data: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """
        Validate solar radiation data.

        Args:
            data: Dict of location_name -> timestamp -> values
            start_time: Expected start time
            end_time: Expected end time

        Returns:
            (is_valid, list of warnings)
        """
        warnings = []

        if not data:
            warnings.append("No solar radiation data collected")
            return False, warnings

        for location_name, location_data in data.items():
            if not location_data:
                warnings.append(f"{location_name}: No data points")
            elif len(location_data) < 12:
                warnings.append(
                    f"{location_name}: Only {len(location_data)} data points "
                    f"(expected more for the time range)"
                )

        return len(warnings) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for solar radiation dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'locations': [loc['name'] for loc in self.locations],
            'location_count': len(self.locations),
            'forecast_days': self.forecast_days,
            'variables': {
                'ghi': 'Global Horizontal Irradiance (W/m²)',
                'dni': 'Direct Normal Irradiance (W/m²)',
                'dhi': 'Diffuse Horizontal Irradiance (W/m²)',
                'direct': 'Direct radiation on horizontal surface (W/m²)',
                'cloud_cover': 'Cloud cover percentage (%)'
            },
            'api_rate_limit': '10,000 requests/day (free tier)',
            'description': 'Solar irradiance forecasts for electricity supply prediction'
        })

        return metadata


# Convenience function
async def get_solar_irradiance(
    locations: List[Dict[str, Any]],
    start_time: datetime,
    end_time: datetime,
    forecast_days: int = 7
):
    """
    Fetch Open-Meteo solar irradiance data.

    Args:
        locations: List of location dicts
        start_time: Start of time range
        end_time: End of time range
        forecast_days: Number of forecast days

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = OpenMeteoSolarCollector(
        locations=locations,
        forecast_days=forecast_days
    )
    return await collector.collect(start_time=start_time, end_time=end_time)


# Example usage
async def main():
    """Example usage of OpenMeteoSolarCollector."""
    from zoneinfo import ZoneInfo

    # Solar production areas in Netherlands
    solar_locations = [
        {"name": "Rotterdam_NL", "lat": 51.9225, "lon": 4.4792},
        {"name": "Eindhoven_NL", "lat": 51.4416, "lon": 5.4697},
        {"name": "Groningen_NL", "lat": 53.2194, "lon": 6.5665},
        {"name": "Lelystad_NL", "lat": 52.5185, "lon": 5.4714},  # Flevoland
    ]

    collector = OpenMeteoSolarCollector(locations=solar_locations)

    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=3)

    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected solar data for {len(dataset.data)} locations")
        for location, data in dataset.data.items():
            print(f"\n{location}: {len(data)} timestamps")
            # Show sample
            for ts, values in list(data.items())[:2]:
                print(f"  {ts}: GHI={values.get('ghi', 'N/A')} W/m², Cloud={values.get('cloud_cover', 'N/A')}%")
    else:
        print("Collection failed")


if __name__ == "__main__":
    import platform
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
