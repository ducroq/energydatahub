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


# WMO weather interpretation codes — translates Open-Meteo's `weather_code`
# integer into short English text so consumers can render a "condition" string
# without a separate code-to-text lookup.
# Reference: https://open-meteo.com/en/docs (Weather variable documentation)
WMO_CODE_MAP: Dict[int, str] = {
    0:  "Clear sky",
    1:  "Mainly clear",
    2:  "Partly cloudy",
    3:  "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}


class OpenMeteoWeatherCollector(BaseCollector):
    """
    Collector for Open-Meteo weather forecasts (demand + general atmospheric).

    Originally scoped to demand prediction (heating/cooling demand drivers at
    population centers). Field set extended 2026-06-05 — `dew_point`, `pressure`,
    `visibility`, `wind_direction`, `uv_index`, `precipitation_probability`,
    `cape`, `weather_code`, `condition` — to give parity with the (retired)
    GoogleWeatherCollector. The collector is now the single weather source
    used for:
      - strategic price-coupling locations (CWE+DK)
      - population centers (demand side)
      - buurt-level locations (FyE B1 short-horizon forecasting)

    Instantiate once per location pool / forecast horizon. The class is
    location-list agnostic — distinct pools become distinct collector
    instances in the orchestrator.
    """

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    # Weather variables for demand prediction + general atmospheric context.
    # Extended 2026-06-05 to give field parity with the (retired) Google
    # Weather collector so downstream consumers see the same field names.
    WEATHER_VARIABLES = [
        "temperature_2m",            # Air temperature at 2m (°C)
        "apparent_temperature",      # Feels-like temperature (°C)
        "dew_point_2m",              # Dew point at 2m (°C)
        "relative_humidity_2m",      # Relative humidity (%)
        "precipitation",             # Precipitation (mm)
        "precipitation_probability", # Probability of precipitation (%)
        "wind_speed_10m",            # Wind speed at 10m (km/h)
        "wind_direction_10m",        # Wind direction at 10m (degrees)
        "surface_pressure",          # Surface pressure (hPa)
        "visibility",                # Visibility (m)
        "cloud_cover",               # Cloud cover (%)
        "uv_index",                  # UV index (0-11+, daytime only)
        "cape",                      # Convective Available Potential Energy (J/kg)
        "weather_code",              # WMO weather interpretation code (int)
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
        # Use semaphore to limit concurrent requests and avoid rate limiting (HTTP 429).
        # Five Open-Meteo collectors run concurrently in data_fetcher.py (strategic
        # weather + solar, offshore wind, buurt weather + solar — last two added
        # 2026-06-05). At Semaphore(2) per collector that's up to 10 concurrent
        # requests, which crossed Open-Meteo's free-tier limit on 2026-06-06
        # (CI run 27068482501: every buurt + offshore location HTTP 429). Tightened
        # to Semaphore(1) + 500 ms gap: peak ~5 concurrent, ~10 req/s headroom.
        semaphore = asyncio.Semaphore(1)

        async def fetch_with_rate_limit(session: aiohttp.ClientSession, location: Dict[str, Any]):
            async with semaphore:
                await asyncio.sleep(0.5)  # Inter-request gap (was 0.1, raised 2026-06-06)
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
                    # Open-Meteo normally returns naive ISO strings (because we
                    # request timezone=Europe/Amsterdam), but defend against
                    # response-format drift: if a timestamp is naive, attach
                    # Amsterdam tz; if it's aware (offset-suffixed), convert it
                    # to Amsterdam so downstream filter comparisons stay correct.
                    dt = datetime.fromisoformat(time_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=amsterdam_tz)
                    else:
                        dt = dt.astimezone(amsterdam_tz)
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

                # --- Extended fields (2026-06-05) — field parity with retired
                #     Google Weather collector. Output names match Google's
                #     where they exist so downstream consumers see no schema
                #     change between providers.

                # Dew point
                if "dew_point_2m" in hourly and i < len(hourly["dew_point_2m"]):
                    val = hourly["dew_point_2m"][i]
                    if val is not None:
                        values["dew_point"] = float(val)

                # Surface pressure
                if "surface_pressure" in hourly and i < len(hourly["surface_pressure"]):
                    val = hourly["surface_pressure"][i]
                    if val is not None:
                        values["pressure"] = float(val)

                # Visibility (metres)
                if "visibility" in hourly and i < len(hourly["visibility"]):
                    val = hourly["visibility"][i]
                    if val is not None:
                        values["visibility"] = float(val)

                # Wind direction (degrees)
                if "wind_direction_10m" in hourly and i < len(hourly["wind_direction_10m"]):
                    val = hourly["wind_direction_10m"][i]
                    if val is not None:
                        values["wind_direction"] = float(val)

                # UV index
                if "uv_index" in hourly and i < len(hourly["uv_index"]):
                    val = hourly["uv_index"][i]
                    if val is not None:
                        values["uv_index"] = float(val)

                # Precipitation probability (%)
                if "precipitation_probability" in hourly and i < len(hourly["precipitation_probability"]):
                    val = hourly["precipitation_probability"][i]
                    if val is not None:
                        values["precipitation_probability"] = float(val)

                # CAPE — convective available potential energy (J/kg).
                # Proxy for thunderstorm activity; high CAPE + moisture = unstable.
                if "cape" in hourly and i < len(hourly["cape"]):
                    val = hourly["cape"][i]
                    if val is not None:
                        values["cape"] = float(val)

                # Weather code (int) + derived English condition string
                if "weather_code" in hourly and i < len(hourly["weather_code"]):
                    val = hourly["weather_code"][i]
                    if val is not None:
                        wmo = int(val)
                        values["weather_code"] = wmo
                        values["condition"] = WMO_CODE_MAP.get(wmo, "Unknown")

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
                'dew_point': 'Dew point at 2m (°C)',
                'humidity': 'Relative humidity (%)',
                'precipitation': 'Precipitation (mm)',
                'precipitation_probability': 'Probability of precipitation (%)',
                'wind_speed': 'Wind speed at 10m (km/h)',
                'wind_direction': 'Wind direction at 10m (degrees)',
                'pressure': 'Surface pressure (hPa)',
                'visibility': 'Visibility (m)',
                'cloud_cover': 'Cloud cover (%)',
                'uv_index': 'UV index (0-11+, daytime only)',
                'cape': 'Convective Available Potential Energy (J/kg) — thunderstorm proxy',
                'weather_code': 'WMO weather interpretation code (int)',
                'condition': 'WMO code mapped to short English text (see WMO_CODE_MAP)',
                'hdd': f'Heating Degree Days (base {self.HEATING_BASE_TEMP}°C)',
                'cdd': f'Cooling Degree Days (base {self.COOLING_BASE_TEMP}°C)'
            },
            'degree_day_config': {
                'heating_base_temp': self.HEATING_BASE_TEMP,
                'cooling_base_temp': self.COOLING_BASE_TEMP
            },
            'api_rate_limit': '10,000 requests/day (free tier)',
            'description': (
                'Weather forecasts from Open-Meteo (ECMWF + DWD ICON). '
                'Originally scoped for demand-side prediction; field set '
                'extended 2026-06-05 to also serve the strategic-price-coupling '
                'and buurt-level use cases that previously used Google Weather.'
            )
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
