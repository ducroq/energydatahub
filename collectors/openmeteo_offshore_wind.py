"""
Open-Meteo Offshore Wind Collector
----------------------------------
Collects wind forecast data from Open-Meteo API for offshore wind farm locations.

File: collectors/openmeteo_offshore_wind.py
Created: 2025-12-03
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for Open-Meteo wind forecasts at offshore locations.
    Unlike Google Weather API, Open-Meteo's global weather models (ICON, GFS)
    support open-sea coordinates, making it ideal for offshore wind farm forecasting.

    Key features:
    - FREE API (no key required)
    - Works for open-sea coordinates (offshore wind farms)
    - Wind speed at multiple heights (10m, 80m, 120m, 180m)
    - Wind direction and gusts
    - Up to 16 days forecast
    - Hourly resolution

Usage:
    from collectors.openmeteo_offshore_wind import OpenMeteoOffshoreWindCollector
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    offshore_locations = [
        {"name": "Gemini_NL", "lat": 54.0361, "lon": 5.9625},
        {"name": "DoggerBank_UK", "lat": 54.7500, "lon": 2.5000},
    ]
    collector = OpenMeteoOffshoreWindCollector(locations=offshore_locations)

    start = datetime.now(ZoneInfo('Europe/Amsterdam'))
    end = start + timedelta(days=7)

    data = await collector.collect(start, end)

API Documentation:
    https://open-meteo.com/en/docs
    Rate limit: 10,000 requests/day (free tier)
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List
from zoneinfo import ZoneInfo
import aiohttp

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig


class OpenMeteoOffshoreWindCollector(BaseCollector):
    """
    Collector for Open-Meteo wind forecasts at offshore locations.

    Uses Open-Meteo's global weather models which support open-sea coordinates,
    ideal for offshore wind farm forecasting where Google Weather API fails.
    """

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    # Wind variables optimized for offshore wind power prediction
    # Multiple heights for wind shear analysis (turbines typically 80-150m hub height)
    WIND_VARIABLES = [
        "wind_speed_10m",           # Surface wind (m/s)
        "wind_speed_80m",           # Near hub height (m/s)
        "wind_speed_120m",          # Hub height for large turbines (m/s)
        "wind_speed_180m",          # Top of rotor sweep (m/s)
        "wind_direction_10m",       # Wind direction at 10m (degrees)
        "wind_direction_80m",       # Wind direction at 80m (degrees)
        "wind_direction_120m",      # Wind direction at 120m (degrees)
        "wind_direction_180m",      # Wind direction at 180m (degrees)
        "wind_gusts_10m",           # Wind gusts (m/s) - important for turbine safety
        "temperature_2m",           # Air temperature (affects air density -> power)
        "surface_pressure",         # Pressure (affects air density -> power)
    ]

    def __init__(
        self,
        locations: List[Dict[str, Any]],
        forecast_days: int = 10,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize Open-Meteo Offshore Wind collector.

        Args:
            locations: List of offshore location dicts
                      Format: [{"name": "Gemini_NL", "lat": 54.0361, "lon": 5.9625}, ...]
            forecast_days: Number of forecast days (1-16, default 10)
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="OpenMeteoOffshoreWindCollector",
            data_type="offshore_wind",
            source="Open-Meteo API (free) - ICON/GFS global models",
            units="m/s (wind_speed), degrees (wind_direction), hPa (pressure)",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.locations = locations
        self.forecast_days = min(max(forecast_days, 1), 16)  # Clamp to 1-16

        self.logger.info(
            f"Initialized for {len(locations)} offshore locations, {self.forecast_days} day forecast"
        )

    async def _fetch_location_data(
        self,
        session: aiohttp.ClientSession,
        location: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetch wind data for a single offshore location.

        Args:
            session: aiohttp session
            location: Location dict with name, lat, lon

        Returns:
            Dict with location name and hourly data
        """
        params = {
            "latitude": location["lat"],
            "longitude": location["lon"],
            "hourly": ",".join(self.WIND_VARIABLES),
            "forecast_days": self.forecast_days,
            "timezone": "Europe/Amsterdam",
            "wind_speed_unit": "ms",  # meters per second (standard for wind power)
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
        Fetch wind data from Open-Meteo API for all offshore locations.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict mapping location names to their hourly data
        """
        self.logger.debug(f"Fetching offshore wind data for {len(self.locations)} locations")

        results = {}
        # Use semaphore to limit concurrent requests and avoid rate limiting
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
                    self.logger.debug(f"{location_name}: Got offshore wind data")
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
                'Gemini_NL': {
                    '2025-12-01T12:00:00+01:00': {
                        'wind_speed_10m': 12.5,
                        'wind_speed_80m': 15.2,
                        'wind_speed_120m': 16.8,
                        'wind_direction_80m': 270.0,
                        'wind_gusts_10m': 18.5,
                        'temperature': 8.5,
                        'pressure': 1013.25
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
                    dt = datetime.fromisoformat(time_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=amsterdam_tz)
                except ValueError:
                    continue

                # Filter to requested time range
                if dt < start_time or dt > end_time:
                    continue

                # Extract wind values
                values = {}

                # Wind speeds at different heights
                for height in ["10m", "80m", "120m", "180m"]:
                    key = f"wind_speed_{height}"
                    if key in hourly and i < len(hourly[key]):
                        val = hourly[key][i]
                        if val is not None:
                            values[key] = float(val)

                # Wind directions at different heights
                for height in ["10m", "80m", "120m", "180m"]:
                    key = f"wind_direction_{height}"
                    if key in hourly and i < len(hourly[key]):
                        val = hourly[key][i]
                        if val is not None:
                            values[key] = float(val)

                # Wind gusts
                if "wind_gusts_10m" in hourly and i < len(hourly["wind_gusts_10m"]):
                    val = hourly["wind_gusts_10m"][i]
                    if val is not None:
                        values["wind_gusts_10m"] = float(val)

                # Temperature (affects air density)
                if "temperature_2m" in hourly and i < len(hourly["temperature_2m"]):
                    val = hourly["temperature_2m"][i]
                    if val is not None:
                        values["temperature"] = float(val)

                # Surface pressure (affects air density)
                if "surface_pressure" in hourly and i < len(hourly["surface_pressure"]):
                    val = hourly["surface_pressure"][i]
                    if val is not None:
                        values["pressure"] = float(val)

                # Calculate estimated air density if we have temp and pressure
                # Air density = P / (R * T) where R = 287.05 J/(kg·K)
                if "temperature" in values and "pressure" in values:
                    temp_kelvin = values["temperature"] + 273.15
                    pressure_pa = values["pressure"] * 100  # hPa to Pa
                    values["air_density"] = round(pressure_pa / (287.05 * temp_kelvin), 4)

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
        """
        return data

    def _validate_data(
        self,
        data: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """
        Validate offshore wind data.

        Args:
            data: Dict of location_name -> timestamp -> values
            start_time: Expected start time
            end_time: Expected end time

        Returns:
            (is_valid, list of warnings)
        """
        warnings = []

        if not data:
            warnings.append("No offshore wind data collected")
            return False, warnings

        for location_name, location_data in data.items():
            if not location_data:
                warnings.append(f"{location_name}: No data points")
            elif len(location_data) < 24:
                warnings.append(
                    f"{location_name}: Only {len(location_data)} data points "
                    f"(expected at least 24 hours)"
                )

            # Check for critical wind speed data
            missing_wind = 0
            for ts, values in location_data.items():
                if "wind_speed_80m" not in values and "wind_speed_10m" not in values:
                    missing_wind += 1
            if missing_wind > 0:
                warnings.append(f"{location_name}: {missing_wind} points missing wind speed data")

        return len(warnings) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for offshore wind dataset.
        """
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'locations': [loc['name'] for loc in self.locations],
            'location_count': len(self.locations),
            'location_coordinates': {loc['name']: {'lat': loc['lat'], 'lon': loc['lon']} for loc in self.locations},
            'forecast_days': self.forecast_days,
            'variables': {
                'wind_speed_10m': 'Wind speed at 10m (m/s)',
                'wind_speed_80m': 'Wind speed at 80m - near hub height (m/s)',
                'wind_speed_120m': 'Wind speed at 120m - hub height for large turbines (m/s)',
                'wind_speed_180m': 'Wind speed at 180m - top of rotor sweep (m/s)',
                'wind_direction_10m': 'Wind direction at 10m (degrees, 0=N, 90=E)',
                'wind_direction_80m': 'Wind direction at 80m (degrees)',
                'wind_direction_120m': 'Wind direction at 120m (degrees)',
                'wind_direction_180m': 'Wind direction at 180m (degrees)',
                'wind_gusts_10m': 'Wind gusts at 10m (m/s)',
                'temperature': 'Air temperature at 2m (C)',
                'pressure': 'Surface pressure (hPa)',
                'air_density': 'Calculated air density (kg/m3)'
            },
            'api_rate_limit': '10,000 requests/day (free tier)',
            'model_info': 'ICON/GFS global models - supports open-sea coordinates',
            'description': 'Wind forecasts for offshore wind farm locations'
        })

        return metadata


# Example usage
async def main():
    """Example usage of OpenMeteoOffshoreWindCollector."""
    from datetime import timedelta

    # Major North Sea offshore wind farm locations (actual coordinates)
    offshore_locations = [
        # Dutch offshore
        {"name": "Gemini_NL", "lat": 54.0361, "lon": 5.9625},
        {"name": "IJmuidenVer_NL", "lat": 52.8500, "lon": 3.5000},
        {"name": "Borssele_NL", "lat": 51.7000, "lon": 3.0000},
        # German Bight
        {"name": "HelgolandCluster_DE", "lat": 54.2000, "lon": 7.5000},
        {"name": "BorkumRiffgrund_DE", "lat": 53.9667, "lon": 6.5500},
        # UK
        {"name": "DoggerBank_UK", "lat": 54.7500, "lon": 2.5000},
        # Denmark
        {"name": "HornsRev_DK", "lat": 55.4833, "lon": 7.8500},
    ]

    collector = OpenMeteoOffshoreWindCollector(locations=offshore_locations)

    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=3)

    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected offshore wind data for {len(dataset.data)} locations")
        for location, data in dataset.data.items():
            print(f"\n{location}: {len(data)} timestamps")
            # Show sample
            for ts, values in list(data.items())[:2]:
                print(f"  {ts}:")
                print(f"    Wind 80m: {values.get('wind_speed_80m', 'N/A')} m/s")
                print(f"    Wind 120m: {values.get('wind_speed_120m', 'N/A')} m/s")
                print(f"    Direction: {values.get('wind_direction_80m', 'N/A')}°")
                print(f"    Air density: {values.get('air_density', 'N/A')} kg/m³")
    else:
        print("Collection failed")


if __name__ == "__main__":
    import platform
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
