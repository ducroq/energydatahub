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
import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import aiohttp

from collectors.base import BaseCollector, RetryConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam
from utils.helpers import closest


# Station-completeness thresholds for the data-quality signal emitted in
# `_get_metadata`. 0–25% filtered = transient upstream noise (no issue);
# 25–50% = warning (broad degradation); >50% = critical (the bbox may no
# longer have a sane closest station — the workflow gate will block publish).
# See issue #12.
STATION_FILTER_WARN_THRESHOLD = 0.25
STATION_FILTER_CRITICAL_THRESHOLD = 0.50

# Strict allowlist for station identifiers before they're interpolated into
# URLs or log messages. RIVM IDs are short alphanumerics (e.g. "NL10497");
# underscores/hyphens accepted because they're URL-safe and used in some
# legacy/test identifiers. Rejects newlines (log-injection vector), path-
# traversal segments, and quoting characters. See security audit on PR #16.
_STATION_NUMBER_PATTERN = re.compile(r'^[A-Za-z0-9_-]{1,32}$')


class LuchtmeetnetCollector(BaseCollector):
    """
    Collector for Luchtmeetnet air quality data.

    Automatically selects nearest monitoring station and retrieves air quality
    measurements including AQI and various pollutants.

    Performance optimization: Caches station list for 24 hours to reduce
    collection time from ~18s to ~2s.
    """

    # Class-level cache for station list (shared across instances)
    _station_cache: Optional[List[Dict]] = None
    _cache_timestamp: Optional[datetime] = None
    _cache_duration = timedelta(hours=24)  # Cache for 24 hours
    # Most-recent filter-stats snapshot taken when the cache was written.
    # Used only to seed `self._last_filter_stats` on cache hits — the
    # per-fetch stats themselves live on the INSTANCE so concurrent buurt
    # collectors can never see each other's stats misattributed (security
    # audit HIGH-1 on PR #16).
    _cache_filter_stats: Optional[Dict[str, int]] = None

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
        # Per-instance filter stats from this collector's most recent station
        # fetch (or copied from class snapshot on cache hit). Instance-scoped
        # so concurrent buurt collectors can't overwrite each other.
        self._last_filter_stats: Optional[Dict[str, int]] = None

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from Luchtmeetnet API.

        This is complex: it fetches station list, finds closest, then gets measurements.
        Uses cached station list if available (24h cache).

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
            # Step 1: Get all stations (with caching)
            stations = await self._get_stations_cached(session)

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

    async def _get_stations_cached(self, session: aiohttp.ClientSession) -> List[Dict]:
        """
        Get station list with caching.

        Checks if cached data is available and fresh (< 24 hours old).
        If not, fetches new data and updates cache.

        Args:
            session: aiohttp session for making requests

        Returns:
            List of station dictionaries with coordinates and metadata
        """
        now = datetime.now()

        # Check if cache is valid
        if (LuchtmeetnetCollector._station_cache is not None and
            LuchtmeetnetCollector._cache_timestamp is not None):
            cache_age = now - LuchtmeetnetCollector._cache_timestamp

            if cache_age < LuchtmeetnetCollector._cache_duration:
                self.logger.info(
                    f"Using cached station list (age: {cache_age.total_seconds()/3600:.1f}h)"
                )
                # Inherit the snapshot's stats so _get_metadata can emit
                # the station_completeness signal even on cache hits.
                self._last_filter_stats = LuchtmeetnetCollector._cache_filter_stats
                return LuchtmeetnetCollector._station_cache

        # Cache miss or expired - fetch new data
        self.logger.info("Station cache miss or expired, fetching fresh data")
        stations = await self._fetch_all_stations(session)

        # Refuse to cache empty results — a one-off upstream outage would
        # otherwise lock collection out for 24h (issue #13). The caller's
        # existing `if not stations` check will fail this run; the next run
        # gets a fresh retry instead of inheriting a poisoned cache.
        if stations:
            LuchtmeetnetCollector._station_cache = stations
            LuchtmeetnetCollector._cache_filter_stats = self._last_filter_stats
            LuchtmeetnetCollector._cache_timestamp = now
            self.logger.info(f"Cached {len(stations)} stations")
        else:
            self.logger.warning(
                "Refusing to cache empty station list — upstream likely "
                "degraded; next run will retry"
            )

        return stations

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
            # Reject station numbers that don't match the expected pattern
            # before interpolating them into URLs or log messages — defends
            # against log-injection (newlines in number) and path-traversal
            # in the URL segment (security audit MEDIUM-2 on PR #16).
            raw_number = station.get('number')
            if not isinstance(raw_number, str) or not _STATION_NUMBER_PATTERN.match(raw_number):
                self.logger.warning(
                    f"Skipping station with malformed number (len={len(raw_number) if isinstance(raw_number, str) else 'n/a'})"
                )
                continue
            url = f"{self.base_url}/stations/{raw_number}/"
            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        continue  # Skip stations with errors

                    data = await response.json()
                    station_data = data['data']

                    # Extract coordinates and metadata. Accept both 'point' (current
                    # Luchtmeetnet behavior) and 'Point' (GeoJSON RFC 7946 standard)
                    # so an upstream-spec correction doesn't silently break us.
                    geom = (station_data or {}).get('geometry') or {}
                    if (geom.get('type') in ('point', 'Point')
                            and geom.get('coordinates')):
                        station['latitude'] = geom['coordinates'][1]
                        station['longitude'] = geom['coordinates'][0]
                        station['components'] = station_data.get('components', [])
                        station['location'] = station_data.get('location', '')
                        station['municipality'] = station_data.get('municipality', '')
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                json.JSONDecodeError,
                ValueError,
                KeyError,
                TypeError,
            ) as e:
                # Single-station network/parse failures must not poison the cache.
                # Before this guard a single bad detail-fetch left the station in
                # the list without lat/lon, causing the cached list to crash
                # `closest()` for 24h (issue #14). Except list is narrow on
                # purpose: AttributeError/NameError from a future refactor
                # mistake should propagate. `raw_number` is pre-validated above
                # so it's safe to interpolate into the log.
                self.logger.warning(
                    f"Skipping station {raw_number}: detail-fetch failed ({type(e).__name__})"
                )

        # Drop any station that didn't get coordinates assigned. `closest()`
        # iterates p['latitude'] over every member; a single missing entry
        # raises KeyError and kills the whole collection.
        clean = [s for s in station_list if 'latitude' in s and 'longitude' in s]
        filtered = len(station_list) - len(clean)
        if filtered:
            self.logger.info(
                f"Filtered {filtered} stations without coordinates "
                f"({len(clean)}/{len(station_list)} usable)"
            )
        # INSTANCE-scoped stats — concurrent buurt collectors cannot overwrite
        # each other's snapshot (security audit HIGH-1 on PR #16). The class-
        # level _cache_filter_stats slot is updated in _get_stations_cached
        # alongside _station_cache so cache hits can inherit.
        self._last_filter_stats = {
            'total': len(station_list),
            'filtered': filtered,
        }
        return clean

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

        # Allowlist of RIVM measurement formulas we accept into the output.
        # Defensive against an upstream schema change or a MITM-altered response
        # injecting unexpected `formula` keys with arbitrary content.
        # Source: https://api.luchtmeetnet.nl/open_api/components
        ALLOWED_FORMULAS = {
            'NO',    # Nitric oxide
            'NO2',   # Nitrogen dioxide
            'NOX',   # Sum of nitrogen oxides
            'O3',    # Ozone
            'SO2',   # Sulfur dioxide
            'CO',    # Carbon monoxide
            'PM10',  # Particulate matter < 10 µm
            'PM25',  # Particulate matter < 2.5 µm
            'PM1',   # Particulate matter < 1 µm (some stations only)
            'BC',    # Black carbon
            'C6H6',  # Benzene
            'NH3',   # Ammonia
            'H2S',   # Hydrogen sulfide
        }

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
                # Store measurement by formula (e.g., NO2, PM10, etc.) — allowlist-guarded
                formula = item.get('formula')
                if formula in ALLOWED_FORMULAS:
                    data.setdefault(timestamp_key, {})[formula] = item['value']
                elif formula:
                    self.logger.debug(f"Skipping unknown formula key from API: {formula!r}")

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

        # Surface station-completeness as a data-quality signal so the
        # workflow's `data_quality_report.overall_status == critical` gate
        # can block publish when station selection is broadly degraded
        # (issue #12). Stats come from this instance's most recent fetch,
        # or were copied from the class snapshot on a cache hit. Reading
        # from instance state means concurrent buurt collectors cannot
        # misattribute each other's stats (security audit HIGH-1).
        stats = self._last_filter_stats
        if stats and stats['total'] > 0:
            filtered = stats['filtered']
            total = stats['total']
            ratio = filtered / total
            metadata['stations_total'] = total
            metadata['stations_filtered'] = filtered
            metadata['stations_filtered_pct'] = round(ratio * 100, 1)
            if ratio > STATION_FILTER_WARN_THRESHOLD:
                severity = (
                    'critical' if ratio > STATION_FILTER_CRITICAL_THRESHOLD
                    else 'warning'
                )
                metadata.setdefault('collector_quality_issues', []).append({
                    'check_name': 'station_completeness',
                    'severity': severity,
                    'message': (
                        f"{filtered}/{total} Luchtmeetnet stations filtered "
                        f"({ratio * 100:.0f}%) — selection may have shifted "
                        f"to a less-representative station"
                    ),
                    'details': {'filtered': filtered, 'total': total},
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
