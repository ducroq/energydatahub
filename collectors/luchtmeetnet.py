"""
Luchtmeetnet API Collector
---------------------------
Collects air quality data from Dutch National Air Quality Monitoring Network
using the new base collector architecture.

File: collectors/luchtmeetnet.py
Created: 2025-10-25
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for Luchtmeetnet data. Automatically finds the
    nearest monitoring station and retrieves air quality measurements including
    AQI, NO2, PM10, and other pollutants.

Usage:
    from collectors.luchtmeetnet import LuchtmeetnetCollector
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    collector = LuchtmeetnetCollector(latitude=52.37, longitude=4.89)
    end = datetime.now(ZoneInfo('Europe/Amsterdam'))
    start = end - timedelta(hours=24)

    data = await collector.collect(start, end)
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List
import aiohttp

from collectors.base import BaseCollector, RetryConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam
from utils.helpers import closest


class LuchtmeetnetCollector(BaseCollector):
    """
    Collector for Luchtmeetnet air quality data.

    Automatically selects nearest monitoring station and retrieves air quality
    measurements including AQI and various pollutants.
    """

    def __init__(
        self,
        latitude: float,
        longitude: float,
        retry_config: RetryConfig = None
    ):
        """
        Initialize Luchtmeetnet collector.

        Args:
            latitude: Latitude of location
            longitude: Longitude of location
            retry_config: Optional retry configuration
        """
        super().__init__(
            name="LuchtmeetnetCollector",
            data_type="air",
            source="Luchtmeetnet API",
            units="µg/m³",
            retry_config=retry_config
        )
        self.latitude = latitude
        self.longitude = longitude
        self.base_url = 'https://api.luchtmeetnet.nl/open_api'
        self.closest_station = None  # Cache closest station

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from Luchtmeetnet API.

        This is complex: it fetches station list, finds closest, then gets measurements.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dictionary with 'station', 'aqi', and 'measurements' keys

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(
            f"Fetching Luchtmeetnet data for lat={self.latitude}, lon={self.longitude}"
        )

        async with aiohttp.ClientSession() as session:
            # Step 1: Get all stations
            stations = await self._fetch_all_stations(session)

            if not stations:
                raise ValueError("No stations returned from Luchtmeetnet API")

            # Step 2: Find closest station
            self.closest_station = closest(
                stations,
                {"latitude": self.latitude, "longitude": self.longitude}
            )

            self.logger.info(
                f"Using station {self.closest_station['number']} "
                f"({self.closest_station.get('location', 'Unknown')})"
            )

            # Step 3: Fetch AQI data
            aqi_data = await self._fetch_aqi(session, self.closest_station['number'])

            # Step 4: Fetch measurement data
            measurement_data = await self._fetch_measurements(
                session,
                self.closest_station['number']
            )

            return {
                'station': self.closest_station,
                'aqi': aqi_data,
                'measurements': measurement_data
            }

    async def _fetch_all_stations(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch all Luchtmeetnet stations with their details."""
        self.logger.debug("Fetching station list")

        # Get first page to know total pages
        url = f"{self.base_url}/stations?page=1&order_by=number&organisation_id="
        async with session.get(url) as response:
            if response.status != 200:
                raise ValueError(f"API returned status {response.status}")

            data = await response.json()
            page_list = list(data['pagination']['page_list'])

        # Fetch all pages
        station_list = []
        for page in page_list:
            url = f"{self.base_url}/stations?page={page}&order_by=number&organisation_id="
            async with session.get(url) as response:
                if response.status != 200:
                    raise ValueError(f"API returned status {response.status}")

                data = await response.json()
                station_list.extend(data['data'])

        self.logger.debug(f"Found {len(station_list)} stations, fetching details...")

        # Fetch details for each station
        for station in station_list:
            url = f"{self.base_url}/stations/{station['number']}/"
            async with session.get(url) as response:
                if response.status != 200:
                    continue  # Skip stations with errors

                data = await response.json()
                station_data = data['data']

                # Extract coordinates and metadata
                if (station_data['geometry']['type'] == 'point' and
                    station_data['geometry']['coordinates']):
                    station['latitude'] = station_data['geometry']['coordinates'][1]
                    station['longitude'] = station_data['geometry']['coordinates'][0]
                    station['components'] = station_data.get('components', [])
                    station['location'] = station_data.get('location', '')
                    station['municipality'] = station_data.get('municipality', '')

        return station_list

    async def _fetch_aqi(
        self,
        session: aiohttp.ClientSession,
        station_number: str
    ) -> List[Dict]:
        """Fetch Air Quality Index data for a station."""
        self.logger.debug(f"Fetching AQI for station {station_number}")

        url = f"{self.base_url}/lki?station_number={station_number}&order_by=timestamp_measured&order_direction=desc"

        async with session.get(url) as response:
            if response.status != 200:
                raise ValueError(f"API returned status {response.status}")

            data = await response.json()
            return data.get('data', [])

    async def _fetch_measurements(
        self,
        session: aiohttp.ClientSession,
        station_number: str
    ) -> List[Dict]:
        """Fetch pollutant measurements for a station."""
        self.logger.debug(f"Fetching measurements for station {station_number}")

        url = f"{self.base_url}/stations/{station_number}/measurements?order_by=timestamp_measured&order_direction=desc"

        async with session.get(url) as response:
            if response.status != 200:
                raise ValueError(f"API returned status {response.status}")

            data = await response.json()
            return data.get('data', [])

    def _parse_response(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse Luchtmeetnet API response to standardized format.

        Combines AQI and measurement data into one timeline.

        Args:
            raw_data: Raw API response with 'station', 'aqi', 'measurements'
            start_time: Start of time range (for filtering)
            end_time: End of time range (for filtering)

        Returns:
            Dict mapping ISO timestamp strings to air quality data dicts
        """
        data = {}

        # Process AQI data
        for item in raw_data['aqi']:
            # Parse timestamp
            timestamp_str = item['timestamp_measured']
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S%z')

            # Normalize to Amsterdam timezone
            amsterdam_dt = normalize_timestamp_to_amsterdam(timestamp)

            # Filter to requested time range
            if start_time <= amsterdam_dt < end_time:
                timestamp_key = amsterdam_dt.isoformat()
                data.setdefault(timestamp_key, {})['AQI'] = item['value']

        # Process measurement data
        for item in raw_data['measurements']:
            # Parse timestamp
            timestamp_str = item['timestamp_measured']
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S%z')

            # Normalize to Amsterdam timezone
            amsterdam_dt = normalize_timestamp_to_amsterdam(timestamp)

            # Filter to requested time range
            if start_time <= amsterdam_dt < end_time:
                timestamp_key = amsterdam_dt.isoformat()
                # Store measurement by formula (e.g., NO2, PM10, etc.)
                data.setdefault(timestamp_key, {})[item['formula']] = item['value']

        self.logger.debug(f"Parsed {len(data)} data points from Luchtmeetnet response")

        return data

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for Luchtmeetnet dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        # Get base metadata from parent class
        metadata = super()._get_metadata(start_time, end_time)

        # Add Luchtmeetnet-specific metadata
        if self.closest_station:
            metadata.update({
                'city': self.closest_station.get('municipality', 'Unknown'),
                'station': self.closest_station.get('number', 'Unknown'),
                'station_location': self.closest_station.get('location', 'Unknown'),
                'station_latitude': self.closest_station.get('latitude'),
                'station_longitude': self.closest_station.get('longitude'),
                'components': self.closest_station.get('components', [])
            })

        metadata.update({
            'requested_latitude': self.latitude,
            'requested_longitude': self.longitude,
            'units': {'all': 'µg/m³'}
        })

        return metadata


# Backward compatibility function
async def get_luchtmeetnet_data(
    latitude: float,
    longitude: float,
    start_time: datetime,
    end_time: datetime
):
    """
    Backward-compatible function for existing code.

    Args:
        latitude: Latitude of location
        longitude: Longitude of location
        start_time: Start of time range
        end_time: End of time range

    Returns:
        EnhancedDataSet or None if failed
    """
    collector = LuchtmeetnetCollector(
        latitude=latitude,
        longitude=longitude
    )
    return await collector.collect(start_time=start_time, end_time=end_time)


# Example usage
async def main():
    """Example usage of LuchtmeetnetCollector."""
    from zoneinfo import ZoneInfo
    from datetime import timedelta

    # Use Amsterdam coordinates
    latitude = 52.3676
    longitude = 4.9041

    # Setup time range (last 24 hours)
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    end = datetime.now(amsterdam_tz)
    start = end - timedelta(hours=24)

    # Create collector and fetch data
    collector = LuchtmeetnetCollector(
        latitude=latitude,
        longitude=longitude
    )

    print(f"Fetching air quality data from {start} to {end}")
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"\nCollected {len(dataset.data)} data points")
        print(f"Station: {dataset.metadata.get('station')} ({dataset.metadata.get('city')})")
        print(f"Components: {dataset.metadata.get('components')}")

        # Show first few measurements
        print(f"\nFirst 3 measurements:")
        for timestamp, values in list(dataset.data.items())[:3]:
            print(f"  {timestamp}:")
            for pollutant, value in values.items():
                print(f"    {pollutant}: {value} µg/m³")

        # Check metrics
        metrics = collector.get_metrics(limit=1)[0]
        print(f"\nCollection metrics:")
        print(f"  Duration: {metrics.duration_seconds:.2f}s")
        print(f"  Status: {metrics.status.value}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
