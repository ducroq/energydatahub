"""
Open-Meteo Weather Collector for Demand Prediction
---------------------------------------------------
Collects temperature and weather data from Open-Meteo API for electricity demand prediction.

File: collectors/openmeteo_weather.py
Created: 2025-12-01
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for Open-Meteo weather forecasts focused on DEMAND prediction.
    Temperature drives electricity demand through heating (winter) and cooling (summer).
    Multi-location data covers major population centers in NL and neighboring countries.

    Key features:
    - FREE API (no key required)
    - Up to 16 days forecast
    - Hourly resolution
    - Temperature, humidity, wind chill, apparent temperature
    - Heating/Cooling Degree Days calculation
    - Global coverage

Usage:
    from collectors.openmeteo_weather import OpenMeteoWeatherCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    locations = [
        {"name": "Amsterdam_NL", "lat": 52.3676, "lon": 4.9041, "population": 872680},
        {"name": "Rotterdam_NL", "lat": 51.9225, "lon": 4.4792, "population": 651446},
    ]
    collector = OpenMeteoWeatherCollector(locations=locations)

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


class OpenMeteoWeatherCollector(BaseCollector):
    """
    Collector for Open-Meteo weather forecasts for demand prediction.

    Fetches temperature and related weather data for multiple population centers
    to support electricity price prediction (demand side - heating/cooling).
    """

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    # Weather variables for demand prediction
    WEATHER_VARIABLES = [
        "temperature_2m",           # Air temperature at 2m (°C)
        "apparent_temperature",     # Feels-like temperature (°C)
        "relative_humidity_2m",     # Relative humidity (%)
        "precipitation",            # Precipitation (mm)
        "wind_speed_10m",          # Wind speed at 10m (km/h) - affects perceived temperature
        "cloud_cover",             # Cloud cover (%) - affects solar heating
    ]

    # Base temperature for degree day calculations (°C)
    # Standard European base: 18°C for heating, 24°C for cooling
    HEATING_BASE_TEMP = 18.0
    COOLING_BASE_TEMP = 24.0

    def __init__(
        self,
        locations: List[Dict[str, Any]],
        forecast_days: int = 7,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize Open-Meteo Weather collector for demand prediction.

        Args:
            locations: List of location dicts for multi-location collection
                      Format: [{"name": "Amsterdam_NL", "lat": 52.3676, "lon": 4.9041, "population": 872680}, ...]
                      population field is optional but useful for weighted averages
            forecast_days: Number of forecast days (1-16, default 7)
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="OpenMeteoWeatherCollector",
            data_type="demand_weather",
            source="Open-Meteo API (free)",
            units="°C (temperature), % (humidity, cloud_cover), mm (precipitation), km/h (wind)",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.locations = locations
        self.forecast_days = min(max(forecast_days, 1), 16)  # Clamp to 1-16

        self.logger.info(
            f"Initialized for {len(locations)} population centers, {self.forecast_days} day forecast"
        )

    async def _fetch_location_data(
        self,
        session: aiohttp.ClientSession,
        location: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetch weather data for a single location.

        Args:
            session: aiohttp session
            location: Location dict with name, lat, lon

        Returns:
            Dict with location name and hourly data
        """
        params = {
            "latitude": location["lat"],
            "longitude": location["lon"],
            "hourly": ",".join(self.WEATHER_VARIABLES),
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
        Fetch weather data from Open-Meteo API for all locations.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict mapping location names to their hourly data
        """
        self.logger.debug(f"Fetching demand weather for {len(self.locations)} locations")

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
                    self.logger.debug(f"{location_name}: Got weather data")
                else:
                    self.logger.warning(f"{location_name}: No data - {response.get('error', 'unknown')}")

        return results

    def _calculate_degree_days(self, temp: float) -> Dict[str, float]:
        """
        Calculate Heating Degree Days (HDD) and Cooling Degree Days (CDD).

        HDD = max(0, base_temp - actual_temp)  -> Higher when colder
        CDD = max(0, actual_temp - base_temp)  -> Higher when hotter

        Args:
            temp: Current temperature in °C

        Returns:
            Dict with hdd and cdd values
        """
        hdd = max(0.0, self.HEATING_BASE_TEMP - temp)
        cdd = max(0.0, temp - self.COOLING_BASE_TEMP)
        return {"hdd": round(hdd, 2), "cdd": round(cdd, 2)}

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
                'Amsterdam_NL': {
                    '2025-12-01T12:00:00+01:00': {
                        'temperature': 8.5,
                        'apparent_temperature': 5.2,
                        'humidity': 75.0,
                        'precipitation': 0.0,
                        'wind_speed': 15.0,
                        'cloud_cover': 60.0,
                        'hdd': 9.5,  # Heating Degree Days
                        'cdd': 0.0   # Cooling Degree Days
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

                # Extract weather values
                values = {}

                # Temperature
                temp = None
                if "temperature_2m" in hourly and i < len(hourly["temperature_2m"]):
                    val = hourly["temperature_2m"][i]
                    if val is not None:
                        temp = float(val)
                        values["temperature"] = temp

                # Apparent temperature (feels like)
                if "apparent_temperature" in hourly and i < len(hourly["apparent_temperature"]):
                    val = hourly["apparent_temperature"][i]
                    if val is not None:
                        values["apparent_temperature"] = float(val)

                # Relative humidity
                if "relative_humidity_2m" in hourly and i < len(hourly["relative_humidity_2m"]):
                    val = hourly["relative_humidity_2m"][i]
                    if val is not None:
                        values["humidity"] = float(val)

                # Precipitation
                if "precipitation" in hourly and i < len(hourly["precipitation"]):
                    val = hourly["precipitation"][i]
                    if val is not None:
                        values["precipitation"] = float(val)

                # Wind speed
                if "wind_speed_10m" in hourly and i < len(hourly["wind_speed_10m"]):
                    val = hourly["wind_speed_10m"][i]
                    if val is not None:
                        values["wind_speed"] = float(val)

                # Cloud cover
                if "cloud_cover" in hourly and i < len(hourly["cloud_cover"]):
                    val = hourly["cloud_cover"][i]
                    if val is not None:
                        values["cloud_cover"] = float(val)

                # Calculate degree days if temperature available
                if temp is not None:
                    degree_days = self._calculate_degree_days(temp)
                    values["hdd"] = degree_days["hdd"]
                    values["cdd"] = degree_days["cdd"]

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
        Validate weather data.

        Args:
            data: Dict of location_name -> timestamp -> values
            start_time: Expected start time
            end_time: Expected end time

        Returns:
            (is_valid, list of warnings)
        """
        warnings = []

        if not data:
            warnings.append("No demand weather data collected")
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
        Get metadata for demand weather dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        metadata = super()._get_metadata(start_time, end_time)

        # Calculate total population covered
        total_population = sum(
            loc.get("population", 0) for loc in self.locations
        )

        metadata.update({
            'locations': [loc['name'] for loc in self.locations],
            'location_count': len(self.locations),
            'total_population_covered': total_population,
            'forecast_days': self.forecast_days,
            'variables': {
                'temperature': 'Air temperature at 2m (°C)',
                'apparent_temperature': 'Feels-like temperature (°C)',
                'humidity': 'Relative humidity (%)',
                'precipitation': 'Precipitation (mm)',
                'wind_speed': 'Wind speed at 10m (km/h)',
                'cloud_cover': 'Cloud cover (%)',
                'hdd': f'Heating Degree Days (base {self.HEATING_BASE_TEMP}°C)',
                'cdd': f'Cooling Degree Days (base {self.COOLING_BASE_TEMP}°C)'
            },
            'degree_day_config': {
                'heating_base_temp': self.HEATING_BASE_TEMP,
                'cooling_base_temp': self.COOLING_BASE_TEMP
            },
            'api_rate_limit': '10,000 requests/day (free tier)',
            'description': 'Weather forecasts for electricity demand prediction (heating/cooling)'
        })

        return metadata


# Convenience function
async def get_demand_weather(
    locations: List[Dict[str, Any]],
    start_time: datetime,
    end_time: datetime,
    forecast_days: int = 7
):
    """
    Fetch Open-Meteo weather data for demand prediction.

    Args:
        locations: List of location dicts (with population centers)
        start_time: Start of time range
        end_time: End of time range
        forecast_days: Number of forecast days

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = OpenMeteoWeatherCollector(
        locations=locations,
        forecast_days=forecast_days
    )
    return await collector.collect(start_time=start_time, end_time=end_time)


# Example usage
async def main():
    """Example usage of OpenMeteoWeatherCollector."""
    from zoneinfo import ZoneInfo

    # Major population centers in Netherlands + neighboring regions
    population_centers = [
        {"name": "Amsterdam_NL", "lat": 52.3676, "lon": 4.9041, "population": 872680},
        {"name": "Rotterdam_NL", "lat": 51.9225, "lon": 4.4792, "population": 651446},
        {"name": "The_Hague_NL", "lat": 52.0705, "lon": 4.3007, "population": 545838},
        {"name": "Utrecht_NL", "lat": 52.0907, "lon": 5.1214, "population": 361924},
        {"name": "Eindhoven_NL", "lat": 51.4416, "lon": 5.4697, "population": 238478},
    ]

    collector = OpenMeteoWeatherCollector(locations=population_centers)

    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=3)

    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected demand weather for {len(dataset.data)} locations")
        for location, data in dataset.data.items():
            print(f"\n{location}: {len(data)} timestamps")
            # Show sample
            for ts, values in list(data.items())[:2]:
                print(f"  {ts}: Temp={values.get('temperature', 'N/A')}°C, "
                      f"HDD={values.get('hdd', 'N/A')}, CDD={values.get('cdd', 'N/A')}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    import platform
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
