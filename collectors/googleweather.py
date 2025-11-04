"""
Google Weather API Collector
------------------------------
Collects weather forecast data from Google Weather API for multiple locations
using the new base collector architecture.

File: collectors/googleweather.py
Created: 2025-01-04
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for Google Weather API. Supports both single-location
    and multi-location data collection for pan-European weather coverage.

    Key features:
    - Up to 240 hours (10 days) hourly forecasts
    - Multiple location support for price prediction (Model A)
    - Comprehensive weather data (temp, wind, precipitation, cloud cover)
    - Part of Google Maps Platform ecosystem

Usage:
    from collectors.googleweather import GoogleWeatherCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    # Single location
    collector = GoogleWeatherCollector(
        api_key="your_key",
        latitude=52.37,
        longitude=4.89
    )

    # Multiple locations (for Model A - price prediction)
    locations = [
        {"name": "Hamburg_DE", "lat": 53.5511, "lon": 9.9937},
        {"name": "Munich_DE", "lat": 48.1351, "lon": 11.5820},
    ]
    collector = GoogleWeatherCollector(
        api_key="your_key",
        locations=locations
    )

    start = datetime.now(ZoneInfo('Europe/Amsterdam'))
    end = start + timedelta(days=7)

    data = await collector.collect(start, end)

API Documentation:
    https://developers.google.com/maps/documentation/weather

Cost:
    - Free during preview phase
    - After GA: $0.15 per 1000 requests (10,000 free/month)
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
import aiohttp

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class GoogleWeatherCollector(BaseCollector):
    """
    Collector for Google Weather API forecasts.

    Supports both single-location and multi-location collection.
    Fetches up to 240 hours (10 days) of hourly weather forecasts.
    """

    def __init__(
        self,
        api_key: str,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        locations: Optional[List[Dict[str, Any]]] = None,
        hours: int = 240,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize Google Weather collector.

        Args:
            api_key: Google Weather API key
            latitude: Latitude of single location (optional if locations provided)
            longitude: Longitude of single location (optional if locations provided)
            locations: List of location dicts for multi-location collection
                      Format: [{"name": "Hamburg_DE", "lat": 53.5511, "lon": 9.9937}, ...]
            hours: Number of forecast hours (1-240, default 240)
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration

        Note:
            Either (latitude, longitude) OR locations must be provided, not both.
        """
        super().__init__(
            name="GoogleWeatherCollector",
            data_type="weather",
            source="Google Weather API v1",
            units="metric",  # Temperature in °C, wind in m/s, etc.
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )

        self.api_key = api_key
        self.hours = min(240, max(1, hours))  # Clamp to API limits
        self.base_url = "https://weather.googleapis.com/v1/forecast/hours:lookup"

        # Multi-location support
        if locations:
            self.multi_location = True
            self.locations = locations
            self.logger.info(f"Initialized for {len(locations)} locations")
        elif latitude is not None and longitude is not None:
            self.multi_location = False
            self.latitude = latitude
            self.longitude = longitude
            self.logger.info(f"Initialized for single location: {latitude}, {longitude}")
        else:
            raise ValueError(
                "Either (latitude, longitude) or locations must be provided"
            )

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from Google Weather API.

        For multi-location mode, fetches all locations in parallel.

        Args:
            start_time: Start of time range (used for filtering)
            end_time: End of time range (used for filtering)

        Returns:
            Raw API response dictionary
            - Single location: {api response}
            - Multi-location: {"locations": [{"name": "Hamburg_DE", "data": {...}}, ...]}

        Raises:
            Exception: If API call fails
        """
        if self.multi_location:
            return await self._fetch_multi_location(start_time, end_time)
        else:
            return await self._fetch_single_location(
                self.latitude,
                self.longitude,
                start_time,
                end_time
            )

    async def _fetch_single_location(
        self,
        lat: float,
        lon: float,
        start_time: datetime,
        end_time: datetime,
        location_name: Optional[str] = None
    ) -> Dict:
        """
        Fetch weather data for a single location.

        Args:
            lat: Latitude
            lon: Longitude
            start_time: Start of time range
            end_time: End of time range
            location_name: Optional name for logging

        Returns:
            Raw API response dictionary
        """
        loc_str = location_name or f"lat={lat}, lon={lon}"
        self.logger.debug(f"Fetching Google Weather data for {loc_str}")

        params = {
            'key': self.api_key,
            'location.latitude': lat,
            'location.longitude': lon,
            'hours': self.hours
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise ValueError(
                        f"Google Weather API returned status {response.status} "
                        f"for {loc_str}: {error_text}"
                    )

                data = await response.json()

        if not data:
            raise ValueError(f"No data returned from Google Weather API for {loc_str}")

        self.logger.debug(
            f"Successfully fetched {len(data.get('hourlyForecasts', []))} "
            f"hours for {loc_str}"
        )

        return data

    async def _fetch_multi_location(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict:
        """
        Fetch weather data for multiple locations in parallel.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with format: {
                "locations": [
                    {"name": "Hamburg_DE", "lat": 53.5511, "lon": 9.9937, "data": {...}},
                    ...
                ]
            }
        """
        self.logger.info(f"Fetching weather for {len(self.locations)} locations in parallel")

        # Create tasks for parallel fetching
        tasks = []
        for location in self.locations:
            task = self._fetch_single_location(
                lat=location['lat'],
                lon=location['lon'],
                start_time=start_time,
                end_time=end_time,
                location_name=location['name']
            )
            tasks.append(task)

        # Fetch all in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Build response with error handling
        locations_data = []
        for location, result in zip(self.locations, results):
            if isinstance(result, Exception):
                self.logger.error(
                    f"Failed to fetch {location['name']}: {str(result)}"
                )
                # Include location with error marker
                locations_data.append({
                    'name': location['name'],
                    'lat': location['lat'],
                    'lon': location['lon'],
                    'error': str(result),
                    'data': None
                })
            else:
                locations_data.append({
                    'name': location['name'],
                    'lat': location['lat'],
                    'lon': location['lon'],
                    'data': result
                })

        return {'locations': locations_data}

    def _parse_response(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse Google Weather API response to standardized format.

        Args:
            raw_data: Raw API response dictionary
            start_time: Start of time range (for filtering)
            end_time: End of time range (for filtering)

        Returns:
            Dict mapping ISO timestamp strings to weather data dicts

            Single location format:
            {
                '2025-01-05T00:00:00+01:00': {
                    'temperature': 8.5,
                    'feels_like': 6.2,
                    'humidity': 82,
                    'wind_speed': 12.3,
                    ...
                }
            }

            Multi-location format:
            {
                'Hamburg_DE': {
                    '2025-01-05T00:00:00+01:00': {...},
                    ...
                },
                'Munich_DE': {
                    '2025-01-05T00:00:00+01:00': {...},
                    ...
                },
                ...
            }
        """
        if 'locations' in raw_data:
            # Multi-location mode
            return self._parse_multi_location(raw_data, start_time, end_time)
        else:
            # Single location mode
            return self._parse_single_location(raw_data, start_time, end_time)

    def _parse_single_location(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse single location response.

        Args:
            raw_data: Raw API response
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict mapping timestamps to weather data
        """
        data = {}

        hourly_forecasts = raw_data.get('hourlyForecasts', [])

        for forecast in hourly_forecasts:
            # Extract timestamp
            time_str = forecast.get('time')
            if not time_str:
                continue

            # Parse ISO 8601 timestamp
            try:
                timestamp = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                # Normalize to Amsterdam timezone
                timestamp_normalized = normalize_timestamp_to_amsterdam(timestamp)
            except Exception as e:
                self.logger.warning(f"Failed to parse timestamp {time_str}: {e}")
                continue

            # Filter by time range
            if timestamp_normalized < start_time or timestamp_normalized > end_time:
                continue

            # Extract weather fields
            weather_data = self._extract_weather_fields(forecast)

            # Store with ISO timestamp key
            data[timestamp_normalized.isoformat()] = weather_data

        return data

    def _parse_multi_location(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse multi-location response.

        Args:
            raw_data: Raw API response with 'locations' key
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict mapping location names to timestamp dicts
        """
        data = {}

        for location_result in raw_data['locations']:
            location_name = location_result['name']
            location_data = location_result.get('data')

            if location_result.get('error') or not location_data:
                self.logger.warning(
                    f"Skipping {location_name} due to fetch error"
                )
                data[location_name] = {}
                continue

            # Parse this location's data
            location_parsed = self._parse_single_location(
                location_data,
                start_time,
                end_time
            )

            data[location_name] = location_parsed

        return data

    def _extract_weather_fields(self, forecast: Dict) -> Dict[str, Any]:
        """
        Extract standardized weather fields from Google Weather forecast object.

        Args:
            forecast: Single hourly forecast from API response

        Returns:
            Dict with standardized field names
        """
        weather = {}

        # Temperature fields (Google provides in Celsius for metric)
        if 'temperature' in forecast:
            weather['temperature'] = forecast['temperature'].get('value')

        if 'temperatureApparent' in forecast:
            weather['feels_like'] = forecast['temperatureApparent'].get('value')

        if 'dewPoint' in forecast:
            weather['dew_point'] = forecast['dewPoint'].get('value')

        # Atmospheric conditions
        if 'relativeHumidity' in forecast:
            weather['humidity'] = forecast['relativeHumidity'].get('value')

        if 'pressure' in forecast:
            weather['pressure'] = forecast['pressure'].get('value')

        if 'visibility' in forecast:
            weather['visibility'] = forecast['visibility'].get('value')

        if 'cloudCover' in forecast:
            weather['cloud_cover'] = forecast['cloudCover'].get('value')

        # Wind fields
        if 'windSpeed' in forecast:
            weather['wind_speed'] = forecast['windSpeed'].get('value')

        if 'windGust' in forecast:
            weather['wind_gust'] = forecast['windGust'].get('value')

        if 'windDirection' in forecast:
            weather['wind_direction'] = forecast['windDirection'].get('degrees')
            weather['wind_direction_cardinal'] = forecast['windDirection'].get('cardinal')

        # Precipitation
        if 'precipitationProbability' in forecast:
            weather['precipitation_probability'] = forecast['precipitationProbability'].get('value')

        if 'rainAccumulation' in forecast:
            weather['rain_accumulation'] = forecast['rainAccumulation'].get('value')

        if 'snowAccumulation' in forecast:
            weather['snow_accumulation'] = forecast['snowAccumulation'].get('value')

        if 'iceAccumulation' in forecast:
            weather['ice_accumulation'] = forecast['iceAccumulation'].get('value')

        # Additional useful fields
        if 'uvIndex' in forecast:
            weather['uv_index'] = forecast['uvIndex'].get('value')

        if 'thunderstormProbability' in forecast:
            weather['thunderstorm_probability'] = forecast['thunderstormProbability'].get('value')

        # Weather condition description
        if 'condition' in forecast:
            weather['condition'] = forecast['condition']

        # Daytime indicator (useful for solar calculations)
        if 'daytime' in forecast:
            weather['is_daytime'] = forecast['daytime']

        return weather
