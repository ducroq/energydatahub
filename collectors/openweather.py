"""
OpenWeather API Collector
--------------------------
Collects weather forecast data from OpenWeather API using the new base
collector architecture.

File: collectors/openweather.py
Created: 2025-10-25
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for OpenWeather data. Handles weather forecasts
    including temperature, humidity, pressure, wind, and cloud cover.

Usage:
    from collectors.openweather import OpenWeatherCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = OpenWeatherCollector(api_key="your_key", latitude=52.37, longitude=4.89)
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=1)

    data = await collector.collect(start, end)
"""

import asyncio
from datetime import datetime
from typing import Any, Dict
import aiohttp

from collectors.base import BaseCollector, RetryConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class OpenWeatherCollector(BaseCollector):
    """
    Collector for OpenWeather API weather forecasts.

    Fetches detailed weather predictions including temperature, humidity,
    pressure, wind conditions, and cloud cover.
    """

    def __init__(
        self,
        api_key: str,
        latitude: float,
        longitude: float,
        retry_config: RetryConfig = None
    ):
        """
        Initialize OpenWeather collector.

        Args:
            api_key: OpenWeather API key
            latitude: Latitude of location
            longitude: Longitude of location
            retry_config: Optional retry configuration
        """
        super().__init__(
            name="OpenWeatherCollector",
            data_type="weather",
            source="OpenWeather API 2.5",
            units="metric",  # Temperature in °C, wind in m/s, etc.
            retry_config=retry_config
        )
        self.api_key = api_key
        self.latitude = latitude
        self.longitude = longitude
        self.base_url = "https://api.openweathermap.org/data/2.5"

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from OpenWeather API.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Raw API response dictionary

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(
            f"Fetching OpenWeather data for lat={self.latitude}, lon={self.longitude}"
        )

        url = f"{self.base_url}/forecast"
        params = {
            'lat': self.latitude,
            'lon': self.longitude,
            'units': 'metric',
            'appid': self.api_key
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    raise ValueError(
                        f"OpenWeather API returned status {response.status}"
                    )

                data = await response.json()

        if not data or 'list' not in data:
            raise ValueError("No forecast data returned from OpenWeather API")

        if not data['list']:
            raise ValueError("Empty forecast list from OpenWeather API")

        return data

    def _parse_response(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse OpenWeather API response to standardized format.

        Args:
            raw_data: Raw API response dictionary
            start_time: Start of time range (for filtering)
            end_time: End of time range (for filtering)

        Returns:
            Dict mapping ISO timestamp strings to weather data dicts
            Example: {
                '2025-10-25T12:00:00+02:00': {
                    'main_temp': 15.5,
                    'main_humidity': 75,
                    'wind_speed': 5.2,
                    ...
                }
            }
        """
        # Fields to exclude from output
        exclude_fields = ['dt', 'dt_txt', 'pop', 'sys']

        data = {}

        for item in raw_data['list']:
            # Convert Unix timestamp to datetime
            timestamp = datetime.fromtimestamp(
                item['dt'],
                tz=start_time.tzinfo
            )

            # Filter to requested time range
            if start_time <= timestamp < end_time:
                # Normalize to Amsterdam timezone
                amsterdam_dt = normalize_timestamp_to_amsterdam(timestamp)
                timestamp_key = amsterdam_dt.isoformat()

                data[timestamp_key] = {}

                # Flatten nested structure
                for key, value in item.items():
                    if key in exclude_fields:
                        continue

                    if isinstance(value, list):
                        # Weather field is a list with one item
                        value = value[0]

                    if isinstance(value, dict):
                        # Flatten nested dict (e.g., main, wind, clouds)
                        for sub_key, sub_value in value.items():
                            if sub_key in exclude_fields:
                                continue
                            data[timestamp_key][f"{key}_{sub_key}"] = sub_value
                    else:
                        data[timestamp_key][key] = value

        self.logger.debug(f"Parsed {len(data)} data points from OpenWeather response")

        return data

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for OpenWeather dataset.

        Note: City information is added after data fetch since it comes from API.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        # Get base metadata from parent class
        metadata = super()._get_metadata(start_time, end_time)

        # Add OpenWeather-specific metadata
        metadata.update({
            'country_code': 'NL',
            'latitude': self.latitude,
            'longitude': self.longitude,
            'api_version': '2.5',
            'units': {
                "temp": "°C",
                "humidity": "%",
                "pressure": "hPa",
                "weather_id": "weather condition code",
                "weather_description": "text",
                "wind_speed": "m/s",
                "wind_deg": "°",
                "wind_gust": "m/s",
                "visibility": "m",
                "clouds_all": "%"
            }
        })

        return metadata

    async def collect(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ):
        """
        Override collect to add city metadata from API response.

        This is necessary because city info comes from the API response,
        not from our configuration.
        """
        # First, fetch the data
        from utils.data_types import EnhancedDataSet
        import time
        import uuid
        from collectors.base import CollectorStatus, CollectionMetrics

        collection_id = str(uuid.uuid4())[:8]
        start_timestamp = time.time()

        self.logger.info(
            f"[{collection_id}] Starting collection: {start_time} to {end_time}"
        )

        metrics = CollectionMetrics(
            collection_id=collection_id,
            collector_name=self.name,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=0.0,
            status=CollectorStatus.FAILED,
            attempt_count=0,
            data_points_collected=0
        )

        try:
            # Fetch and parse
            raw_data = await self._retry_with_backoff(
                self._fetch_raw_data,
                start_time,
                end_time,
                **kwargs
            )
            parsed_data = self._parse_response(raw_data, start_time, end_time)
            normalized_data = self._normalize_timestamps(parsed_data)
            is_valid, warnings = self._validate_data(normalized_data, start_time, end_time)

            for warning in warnings:
                self.logger.warning(f"[{collection_id}] {warning}")
                metrics.warnings.append(warning)

            # Get metadata and add city info from API
            metadata = self._get_metadata(start_time, end_time)
            if 'city' in raw_data:
                city_info = raw_data['city']
                metadata.update({
                    'city': city_info.get('name'),
                    'city_id': city_info.get('id'),
                    'population': city_info.get('population'),
                    'sunrise': datetime.fromtimestamp(
                        city_info['sunrise'],
                        tz=start_time.tzinfo
                    ).isoformat() if 'sunrise' in city_info else None,
                    'sunset': datetime.fromtimestamp(
                        city_info['sunset'],
                        tz=start_time.tzinfo
                    ).isoformat() if 'sunset' in city_info else None,
                })

            # Create dataset
            dataset = EnhancedDataSet(
                metadata=metadata,
                data=normalized_data
            )

            # Update metrics
            metrics.data_points_collected = len(normalized_data)
            metrics.status = CollectorStatus.PARTIAL if warnings else CollectorStatus.SUCCESS
            metrics.duration_seconds = time.time() - start_timestamp

            self.logger.info(
                f"[{collection_id}] Collection complete: "
                f"{metrics.data_points_collected} data points in "
                f"{metrics.duration_seconds:.2f}s "
                f"(status: {metrics.status.value})"
            )

            self._metrics_history.append(metrics)
            return dataset

        except Exception as e:
            metrics.duration_seconds = time.time() - start_timestamp
            metrics.status = CollectorStatus.FAILED
            metrics.errors.append(f"{type(e).__name__}: {e}")

            self.logger.error(
                f"[{collection_id}] Collection failed after "
                f"{metrics.duration_seconds:.2f}s: {e}"
            )

            self._metrics_history.append(metrics)
            return None


# Backward compatibility functions
async def get_OpenWeather_data(
    api_key: str,
    latitude: float,
    longitude: float,
    start_time: datetime,
    end_time: datetime
):
    """
    Backward-compatible function for existing code.

    Args:
        api_key: OpenWeather API key
        latitude: Latitude of location
        longitude: Longitude of location
        start_time: Start of time range
        end_time: End of time range

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = OpenWeatherCollector(
        api_key=api_key,
        latitude=latitude,
        longitude=longitude
    )
    return await collector.collect(start_time=start_time, end_time=end_time)


async def get_OpenWeather_geographical_coordinates_in_NL(api_key: str, plaats: str) -> dict:
    """
    Helper function to get geographical coordinates for a Dutch city.

    Args:
        api_key: OpenWeather API key
        plaats: Name of the location in the Netherlands

    Returns:
        Dict with 'latitude' and 'longitude' keys, or None if failed
    """
    import logging

    url = f"http://api.openweathermap.org/geo/1.0/direct"
    params = {
        'q': f"{plaats},NL",
        'limit': 1,
        'appid': api_key
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        latitude = data[0]["lat"]
                        longitude = data[0]["lon"]
                        logging.info(
                            f"OpenWeather coordinates for {plaats}: "
                            f"{latitude}, {longitude}"
                        )
                        return {"latitude": latitude, "longitude": longitude}
                    else:
                        raise ValueError(f"No results for {plaats}")
                else:
                    raise ValueError(f"API returned status {response.status}")
    except Exception as e:
        logging.error(f"Error retrieving coordinates: {e}")
        return None


# Example usage
async def main():
    """Example usage of OpenWeatherCollector."""
    import os
    from configparser import ConfigParser
    from zoneinfo import ZoneInfo
    from datetime import timedelta
    import platform

    # Set Windows event loop policy if needed
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Load API key
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, '..', 'weather_data_fetchers', 'secrets.ini')

    config = ConfigParser()
    config.read(secrets_file)
    api_key = config.get('api_keys', 'openweather')

    # Get coordinates for Amsterdam
    plaats = "Amsterdam"
    coords = await get_OpenWeather_geographical_coordinates_in_NL(api_key, plaats)

    if coords:
        # Setup time range
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime.now(amsterdam_tz)
        end = (start + timedelta(days=1)).replace(hour=23, minute=59, second=59)

        # Create collector and fetch data
        collector = OpenWeatherCollector(
            api_key=api_key,
            latitude=coords["latitude"],
            longitude=coords["longitude"]
        )
        dataset = await collector.collect(start, end)

        if dataset:
            print(f"Weather data for {dataset.metadata.get('city', 'Unknown')}:")
            print(f"Collected {len(dataset.data)} data points")
            print(f"\nFirst forecast:")
            first_timestamp = list(dataset.data.keys())[0]
            print(f"  Time: {first_timestamp}")
            for key, value in list(dataset.data[first_timestamp].items())[:5]:
                print(f"    {key}: {value}")

            # Check metrics
            metrics = collector.get_metrics(limit=1)
            print(f"\nCollection metrics:")
            print(f"  Duration: {metrics[0].duration_seconds:.2f}s")
            print(f"  Status: {metrics[0].status.value}")
        else:
            print("Collection failed")
    else:
        print(f"Failed to retrieve coordinates for {plaats}")


if __name__ == "__main__":
    asyncio.run(main())
