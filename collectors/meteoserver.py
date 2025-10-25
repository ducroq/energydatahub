"""
MeteoServer API Collectors
---------------------------
Collects weather and solar radiation forecasts from MeteoServer API using the
new base collector architecture.

File: collectors/meteoserver.py
Created: 2025-10-25
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for MeteoServer data. Provides two collectors:
    - MeteoServerWeatherCollector: General weather forecasts (HARMONIE model)
    - MeteoServerSunCollector: Solar radiation forecasts

Usage:
    from collectors.meteoserver import MeteoServerWeatherCollector, MeteoServerSunCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    weather_collector = MeteoServerWeatherCollector(api_key="your_key", lat=52.37, lon=4.89)
    sun_collector = MeteoServerSunCollector(api_key="your_key", lat=52.37, lon=4.89)

    start = datetime.now(ZoneInfo('Europe/Amsterdam'))
    end = start + timedelta(days=1)

    weather_data = await weather_collector.collect(start, end)
    sun_data = await sun_collector.collect(start, end)
"""

import asyncio
from datetime import datetime
from typing import Any, Dict
import aiohttp

from collectors.base import BaseCollector, RetryConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class MeteoServerWeatherCollector(BaseCollector):
    """
    Collector for MeteoServer weather forecasts.

    Uses HARMONIE weather model for detailed weather predictions.
    """

    def __init__(
        self,
        api_key: str,
        latitude: float,
        longitude: float,
        retry_config: RetryConfig = None
    ):
        """
        Initialize MeteoServer weather collector.

        Args:
            api_key: MeteoServer API key
            latitude: Latitude of location
            longitude: Longitude of location
            retry_config: Optional retry configuration (defaults to 10 attempts)
        """
        # MeteoServer API sometimes returns incomplete data, so use more retries
        if retry_config is None:
            retry_config = RetryConfig(
                max_attempts=10,
                initial_delay=2.0,
                exponential_base=1.0  # Linear backoff, not exponential
            )

        super().__init__(
            name="MeteoServerWeatherCollector",
            data_type="weather",
            source="MeteoServer API (HARMONIE model)",
            units="metric",
            retry_config=retry_config
        )
        self.api_key = api_key
        self.latitude = latitude
        self.longitude = longitude
        self.base_url = 'https://data.meteoserver.nl/api/uurverwachting.php'

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from MeteoServer API.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Raw API response dictionary

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(
            f"Fetching MeteoServer weather for lat={self.latitude}, lon={self.longitude}"
        )

        url = f"{self.base_url}?lat={self.latitude}&long={self.longitude}&key={self.api_key}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise ValueError(
                        f"MeteoServer API returned status {response.status}"
                    )

                data = await response.json()

        if not data or 'data' not in data:
            raise ValueError("No data field in MeteoServer response")

        if not data['data']:
            raise ValueError("Empty data array from MeteoServer API")

        return data

    def _parse_response(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse MeteoServer API response to standardized format.

        Args:
            raw_data: Raw API response dictionary
            start_time: Start of time range (for filtering)
            end_time: End of time range (for filtering)

        Returns:
            Dict mapping ISO timestamp strings to weather data dicts
        """
        # Fields to exclude from output
        exclude_fields = ['tijd', 'tijd_nl', 'loc', 'offset', 'samenv']

        data = {}

        for item in raw_data['data']:
            # Convert Unix timestamp to datetime
            timestamp = datetime.fromtimestamp(
                int(item['tijd']),
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

                    if isinstance(value, dict):
                        # Flatten nested dict
                        for sub_key, sub_value in value.items():
                            if sub_key in exclude_fields:
                                continue
                            data[timestamp_key][f"{key}_{sub_key}"] = sub_value
                    else:
                        data[timestamp_key][key] = value

        self.logger.debug(f"Parsed {len(data)} data points from MeteoServer weather response")

        return data

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for MeteoServer weather dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        # Get base metadata from parent class
        metadata = super()._get_metadata(start_time, end_time)

        # Add MeteoServer-specific metadata
        metadata.update({
            'model': 'HARMONIE',
            'latitude': self.latitude,
            'longitude': self.longitude,
            'units': {
                "temp": "°C",
                "winds": "m/s (mean wind velocity)",
                "windb": "Beaufort (mean wind force)",
                "windknp": "knots (mean wind velocity)",
                "windkmh": "km/h (mean wind velocity)",
                "windr": "° (wind direction)",
                "windrltr": "abbreviation (wind direction)",
                "gust": "m/s (wind gust)",
                "vis": "m (visibility)",
                "neersl": "mm (precipitation)",
                "luchtd": "mbar/hPa (air pressure)",
                "rv": "% (relative humidity)",
                "gr": "W/m² (global horizontal radiation)",
                "hw": "% (high cloud cover)",
                "mw": "% (medium cloud cover)",
                "lw": "% (low cloud cover)",
                "tw": "% (total cloud cover)",
                "cond": "weather condition code",
                "ico": "weather icon code"
            }
        })

        return metadata


class MeteoServerSunCollector(BaseCollector):
    """
    Collector for MeteoServer solar radiation forecasts.

    Provides detailed solar radiation and sun position data.
    """

    def __init__(
        self,
        api_key: str,
        latitude: float,
        longitude: float,
        retry_config: RetryConfig = None
    ):
        """
        Initialize MeteoServer sun collector.

        Args:
            api_key: MeteoServer API key
            latitude: Latitude of location
            longitude: Longitude of location
            retry_config: Optional retry configuration (defaults to 10 attempts)
        """
        # MeteoServer API sometimes returns incomplete data, so use more retries
        if retry_config is None:
            retry_config = RetryConfig(
                max_attempts=10,
                initial_delay=2.0,
                exponential_base=1.0  # Linear backoff
            )

        super().__init__(
            name="MeteoServerSunCollector",
            data_type="sun",
            source="MeteoServer API",
            units="metric",
            retry_config=retry_config
        )
        self.api_key = api_key
        self.latitude = latitude
        self.longitude = longitude
        self.base_url = 'https://data.meteoserver.nl/api/solar.php'

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from MeteoServer solar API.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Raw API response dictionary

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(
            f"Fetching MeteoServer sun forecast for lat={self.latitude}, lon={self.longitude}"
        )

        url = f"{self.base_url}?lat={self.latitude}&long={self.longitude}&key={self.api_key}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise ValueError(
                        f"MeteoServer API returned status {response.status}"
                    )

                data = await response.json()

        if not data or 'forecast' not in data:
            raise ValueError("No forecast field in MeteoServer response")

        if not data['forecast']:
            raise ValueError("Empty forecast array from MeteoServer API")

        return data

    def _parse_response(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse MeteoServer solar API response to standardized format.

        Args:
            raw_data: Raw API response dictionary
            start_time: Start of time range (for filtering)
            end_time: End of time range (for filtering)

        Returns:
            Dict mapping ISO timestamp strings to solar data dicts
        """
        # Fields to exclude from output
        exclude_fields = ['time', 'cet']

        data = {}

        for item in raw_data['forecast']:
            # Convert Unix timestamp to datetime
            timestamp = datetime.fromtimestamp(
                int(item['time']),
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

                    if isinstance(value, dict):
                        # Flatten nested dict
                        for sub_key, sub_value in value.items():
                            if sub_key in exclude_fields:
                                continue
                            data[timestamp_key][f"{key}_{sub_key}"] = sub_value
                    else:
                        data[timestamp_key][key] = value

        self.logger.debug(f"Parsed {len(data)} data points from MeteoServer sun response")

        return data

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for MeteoServer sun dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        # Get base metadata from parent class
        metadata = super()._get_metadata(start_time, end_time)

        # Add MeteoServer-specific metadata
        metadata.update({
            'latitude': self.latitude,
            'longitude': self.longitude,
            'units': {
                "temp": "°C",
                "elev": "° (sun altitude at start of hour)",
                "az": "° (sun azimuth, N=0, E=90)",
                "gr": "J/hr/cm² (global horizontal radiation)",
                "gr_w": "W/m² (global horizontal radiation)",
                "sd": "min (sunshine minutes)",
                "tc": "% (total cloud cover)",
                "lc": "% (low cloud cover)",
                "mc": "% (intermediate cloud cover)",
                "hc": "% (high cloud cover)",
                "vis": "m (visibility)",
                "prec": "mm (precipitation)"
            }
        })

        return metadata


# Backward compatibility functions
async def get_MeteoServer_weather_forecast_data(
    api_key: str,
    latitude: float,
    longitude: float,
    start_time: datetime,
    end_time: datetime
):
    """
    Backward-compatible function for existing code.

    Args:
        api_key: MeteoServer API key
        latitude: Latitude of location
        longitude: Longitude of location
        start_time: Start of time range
        end_time: End of time range

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = MeteoServerWeatherCollector(
        api_key=api_key,
        latitude=latitude,
        longitude=longitude
    )
    return await collector.collect(start_time=start_time, end_time=end_time)


async def get_MeteoServer_sun_forecast(
    api_key: str,
    latitude: float,
    longitude: float,
    start_time: datetime,
    end_time: datetime
):
    """
    Backward-compatible function for existing code.

    Args:
        api_key: MeteoServer API key
        latitude: Latitude of location
        longitude: Longitude of location
        start_time: Start of time range
        end_time: End of time range

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = MeteoServerSunCollector(
        api_key=api_key,
        latitude=latitude,
        longitude=longitude
    )
    return await collector.collect(start_time=start_time, end_time=end_time)


# Example usage
async def main():
    """Example usage of MeteoServer collectors."""
    import os
    from configparser import ConfigParser
    from zoneinfo import ZoneInfo
    from datetime import timedelta
    import platform

    # Set Windows event loop policy if needed
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Load configuration
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, '..', 'secrets.ini')

    config = ConfigParser()
    config.read(secrets_file)
    api_key = config.get('api_keys', 'meteo')

    # Use Amsterdam coordinates
    latitude = 52.3676
    longitude = 4.9041

    # Setup time range
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = (start + timedelta(days=1)).replace(hour=23, minute=59, second=59)

    # Test weather collector
    print("Testing Weather Collector...")
    weather_collector = MeteoServerWeatherCollector(
        api_key=api_key,
        latitude=latitude,
        longitude=longitude
    )
    weather_data = await weather_collector.collect(start, end)

    if weather_data:
        print(f"Weather: {len(weather_data.data)} data points")
        print(f"City: {weather_data.metadata.get('city', 'Unknown')}")

        metrics = weather_collector.get_metrics(limit=1)[0]
        print(f"Duration: {metrics.duration_seconds:.2f}s\n")
    else:
        print("Weather collection failed\n")

    # Test sun collector
    print("Testing Sun Collector...")
    sun_collector = MeteoServerSunCollector(
        api_key=api_key,
        latitude=latitude,
        longitude=longitude
    )
    sun_data = await sun_collector.collect(start, end)

    if sun_data:
        print(f"Sun: {len(sun_data.data)} data points")
        print(f"Station: {sun_data.metadata.get('station', 'Unknown')}")

        metrics = sun_collector.get_metrics(limit=1)[0]
        print(f"Duration: {metrics.duration_seconds:.2f}s")
    else:
        print("Sun collection failed")


if __name__ == "__main__":
    asyncio.run(main())
