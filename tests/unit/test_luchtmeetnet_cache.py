"""
Unit Tests for Luchtmeetnet Station Caching
-------------------------------------------
Tests the 24-hour caching mechanism for station list.

File: tests/unit/test_luchtmeetnet_cache.py
Created: 2025-10-25
"""

import pytest
import asyncio
import platform
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
from collectors.luchtmeetnet import LuchtmeetnetCollector

# Fix Windows event loop for aiodns compatibility
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class TestLuchtmeetnetCacheBasics:
    """Test basic caching functionality."""

    def test_cache_starts_empty(self):
        """Cache should be None initially."""
        # Reset class-level cache
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)
        assert LuchtmeetnetCollector._station_cache is None
        assert LuchtmeetnetCollector._cache_timestamp is None

    @pytest.mark.asyncio
    async def test_first_collection_populates_cache(self):
        """First collection should populate the cache."""
        # Reset cache
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)

        # Mock the API responses
        mock_stations = [
            {'number': 'NL001', 'latitude': 52.37, 'longitude': 4.89}
        ]

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            with patch.object(collector, '_fetch_aqi', new_callable=AsyncMock) as mock_aqi:
                with patch.object(collector, '_fetch_measurements', new_callable=AsyncMock) as mock_meas:
                    mock_fetch.return_value = mock_stations
                    mock_aqi.return_value = []
                    mock_meas.return_value = []

                    start = datetime.now()
                    end = start + timedelta(hours=1)

                    # Create mock session
                    mock_session = AsyncMock()

                    # Directly call _get_stations_cached
                    stations = await collector._get_stations_cached(mock_session)

                    # Cache should be populated
                    assert LuchtmeetnetCollector._station_cache is not None
                    assert LuchtmeetnetCollector._cache_timestamp is not None
                    assert stations == mock_stations
                    mock_fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_second_collection_uses_cache(self):
        """Second collection should use cached data."""
        # Populate cache
        mock_stations = [
            {'number': 'NL001', 'latitude': 52.37, 'longitude': 4.89}
        ]
        LuchtmeetnetCollector._station_cache = mock_stations
        LuchtmeetnetCollector._cache_timestamp = datetime.now()

        collector = LuchtmeetnetCollector(52.37, 4.89)

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_session = AsyncMock()
            stations = await collector._get_stations_cached(mock_session)

            # Should use cache, not call API
            assert stations == mock_stations
            mock_fetch.assert_not_called()


class TestLuchtmeetnetCacheExpiry:
    """Test cache expiration logic."""

    @pytest.mark.asyncio
    async def test_cache_expires_after_24_hours(self):
        """Cache should expire after 24 hours."""
        # Set cache with old timestamp
        mock_stations_old = [
            {'number': 'NL001', 'latitude': 52.0, 'longitude': 4.0}
        ]
        LuchtmeetnetCollector._station_cache = mock_stations_old
        LuchtmeetnetCollector._cache_timestamp = datetime.now() - timedelta(hours=25)

        collector = LuchtmeetnetCollector(52.37, 4.89)

        # New data from API
        mock_stations_new = [
            {'number': 'NL002', 'latitude': 52.37, 'longitude': 4.89}
        ]

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_stations_new
            mock_session = AsyncMock()

            stations = await collector._get_stations_cached(mock_session)

            # Should fetch new data
            assert stations == mock_stations_new
            mock_fetch.assert_called_once()

            # Cache should be updated
            assert LuchtmeetnetCollector._station_cache == mock_stations_new

    @pytest.mark.asyncio
    async def test_cache_valid_within_24_hours(self):
        """Cache should be valid within 24 hours."""
        mock_stations = [
            {'number': 'NL001', 'latitude': 52.37, 'longitude': 4.89}
        ]
        # Set cache with recent timestamp (1 hour ago)
        LuchtmeetnetCollector._station_cache = mock_stations
        LuchtmeetnetCollector._cache_timestamp = datetime.now() - timedelta(hours=1)

        collector = LuchtmeetnetCollector(52.37, 4.89)

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_session = AsyncMock()
            stations = await collector._get_stations_cached(mock_session)

            # Should use cache
            assert stations == mock_stations
            mock_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_exactly_at_boundary(self):
        """Test cache behavior exactly at 24-hour boundary."""
        mock_stations = [
            {'number': 'NL001', 'latitude': 52.37, 'longitude': 4.89}
        ]
        # Set cache exactly 24 hours ago (should expire)
        LuchtmeetnetCollector._station_cache = mock_stations
        LuchtmeetnetCollector._cache_timestamp = datetime.now() - timedelta(hours=24, seconds=1)

        collector = LuchtmeetnetCollector(52.37, 4.89)

        mock_stations_new = [
            {'number': 'NL002', 'latitude': 52.37, 'longitude': 4.89}
        ]

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_stations_new
            mock_session = AsyncMock()

            stations = await collector._get_stations_cached(mock_session)

            # Should fetch new (cache expired)
            assert stations == mock_stations_new
            mock_fetch.assert_called_once()


class TestLuchtmeetnetCacheSharing:
    """Test cache sharing across collector instances."""

    @pytest.mark.asyncio
    async def test_cache_shared_across_instances(self):
        """Cache should be shared across all collector instances."""
        # Reset cache
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        # Create first collector
        collector1 = LuchtmeetnetCollector(52.37, 4.89)

        mock_stations = [
            {'number': 'NL001', 'latitude': 52.37, 'longitude': 4.89}
        ]

        # First collector populates cache
        with patch.object(collector1, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_stations
            mock_session = AsyncMock()
            await collector1._get_stations_cached(mock_session)

        # Create second collector (different location)
        collector2 = LuchtmeetnetCollector(53.0, 5.0)

        # Second collector should use same cache
        with patch.object(collector2, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch2:
            mock_session2 = AsyncMock()
            stations2 = await collector2._get_stations_cached(mock_session2)

            # Should use cache, not fetch
            assert stations2 == mock_stations
            mock_fetch2.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_updates_affect_all_instances(self):
        """Cache updates should affect all collector instances."""
        # Start with populated cache
        mock_stations_old = [
            {'number': 'NL001', 'latitude': 52.0, 'longitude': 4.0}
        ]
        LuchtmeetnetCollector._station_cache = mock_stations_old
        LuchtmeetnetCollector._cache_timestamp = datetime.now() - timedelta(hours=25)

        collector1 = LuchtmeetnetCollector(52.37, 4.89)
        collector2 = LuchtmeetnetCollector(53.0, 5.0)

        mock_stations_new = [
            {'number': 'NL002', 'latitude': 52.37, 'longitude': 4.89}
        ]

        # Collector1 triggers cache refresh
        with patch.object(collector1, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_stations_new
            mock_session = AsyncMock()
            await collector1._get_stations_cached(mock_session)

        # Collector2 should see updated cache
        with patch.object(collector2, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch2:
            mock_session2 = AsyncMock()
            stations2 = await collector2._get_stations_cached(mock_session2)

            # Should use updated cache
            assert stations2 == mock_stations_new
            mock_fetch2.assert_not_called()


class TestLuchtmeetnetCachePerformance:
    """Test cache performance characteristics."""

    @pytest.mark.asyncio
    async def test_cache_hit_is_faster(self):
        """Cache hit should be significantly faster than cache miss."""
        import time

        # Populate cache
        mock_stations = [
            {'number': f'NL{i:03d}', 'latitude': 52.0 + i*0.01, 'longitude': 4.0 + i*0.01}
            for i in range(101)  # 101 stations like real API
        ]
        LuchtmeetnetCollector._station_cache = mock_stations
        LuchtmeetnetCollector._cache_timestamp = datetime.now()

        collector = LuchtmeetnetCollector(52.37, 4.89)

        # Mock slow API call
        async def slow_fetch(session):
            await asyncio.sleep(0.1)  # Simulate API latency
            return mock_stations

        with patch.object(collector, '_fetch_all_stations', side_effect=slow_fetch):
            mock_session = AsyncMock()

            # Cache hit (should be fast)
            start = time.time()
            stations = await collector._get_stations_cached(mock_session)
            cache_hit_time = time.time() - start

            # Should be very fast (<10ms)
            assert cache_hit_time < 0.01  # 10ms
            assert stations == mock_stations

    @pytest.mark.asyncio
    async def test_cache_miss_calls_api(self):
        """Cache miss should call API exactly once."""
        # Empty cache
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)

        mock_stations = [{'number': 'NL001'}]
        call_count = 0

        async def counting_fetch(session):
            nonlocal call_count
            call_count += 1
            return mock_stations

        with patch.object(collector, '_fetch_all_stations', side_effect=counting_fetch):
            mock_session = AsyncMock()
            await collector._get_stations_cached(mock_session)

            # Should call API exactly once
            assert call_count == 1


class TestLuchtmeetnetCacheEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_empty_station_list_cached(self):
        """Empty station list should still be cached."""
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)

        empty_stations = []

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = empty_stations
            mock_session = AsyncMock()

            stations = await collector._get_stations_cached(mock_session)

            # Empty list should be cached
            assert LuchtmeetnetCollector._station_cache == []
            assert LuchtmeetnetCollector._cache_timestamp is not None
            assert stations == []

    @pytest.mark.asyncio
    async def test_none_timestamp_triggers_fetch(self):
        """None timestamp should trigger fetch even if cache has data."""
        # Cache with data but no timestamp
        LuchtmeetnetCollector._station_cache = [{'number': 'NL001'}]
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)

        mock_stations_new = [{'number': 'NL002'}]

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_stations_new
            mock_session = AsyncMock()

            stations = await collector._get_stations_cached(mock_session)

            # Should fetch (no timestamp)
            assert stations == mock_stations_new
            mock_fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_none_cache_triggers_fetch(self):
        """None cache should trigger fetch even if timestamp exists."""
        # Timestamp but no cache
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = datetime.now()

        collector = LuchtmeetnetCollector(52.37, 4.89)

        mock_stations = [{'number': 'NL001'}]

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_stations
            mock_session = AsyncMock()

            stations = await collector._get_stations_cached(mock_session)

            # Should fetch (no cache)
            assert stations == mock_stations
            mock_fetch.assert_called_once()


class TestLuchtmeetnetCacheIntegration:
    """Test cache integration with full collection workflow."""

    @pytest.mark.asyncio
    async def test_cache_used_in_collect_method(self):
        """Collect method should use cached stations."""
        # Populate cache
        mock_stations = [
            {
                'number': 'NL001',
                'latitude': 52.37,
                'longitude': 4.89,
                'location': 'Test Location',
                'components': ['NO2', 'PM10']
            }
        ]
        LuchtmeetnetCollector._station_cache = mock_stations
        LuchtmeetnetCollector._cache_timestamp = datetime.now()

        collector = LuchtmeetnetCollector(52.37, 4.89)

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch_stations:
            with patch.object(collector, '_fetch_aqi', new_callable=AsyncMock) as mock_aqi:
                with patch.object(collector, '_fetch_measurements', new_callable=AsyncMock) as mock_meas:
                    mock_aqi.return_value = []
                    mock_meas.return_value = []

                    start = datetime.now()
                    end = start + timedelta(hours=1)

                    # Collect should use cache
                    result = await collector.collect(start, end)

                    # Stations fetch should not be called
                    mock_fetch_stations.assert_not_called()

                    # But AQI and measurements should still be fetched
                    mock_aqi.assert_called_once()
                    mock_meas.assert_called_once()


class TestLuchtmeetnetStationFilter:
    """Regression: stations missing lat/lon must not poison the cache.

    Before 2026-06-06 a failing per-station detail-fetch (HTTP non-200 or
    network error) left the station in `_fetch_all_stations`'s result
    *without* `latitude`/`longitude`. `closest()` then iterated
    `p['latitude']` over the cached list and raised `KeyError: 'latitude'`
    for 24h — see CI run 27068482501.

    These tests assert the post-fetch filter drops incomplete entries.
    """

    @pytest.mark.asyncio
    async def test_filter_drops_stations_without_coordinates(self):
        """Stations without lat/lon (e.g. detail-fetch HTTP 500) are dropped."""
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)

        # First page: three stations
        page_response = AsyncMock()
        page_response.status = 200
        page_response.json = AsyncMock(return_value={
            "pagination": {"page_list": [1]},
            "data": [
                {"number": "NL_OK1"},
                {"number": "NL_BAD"},
                {"number": "NL_OK2"},
            ],
        })

        # Per-station detail responses: NL_OK1 + NL_OK2 succeed, NL_BAD HTTP 500
        ok1_response = AsyncMock()
        ok1_response.status = 200
        ok1_response.json = AsyncMock(return_value={"data": {
            "geometry": {"type": "point", "coordinates": [4.0, 52.0]},
            "components": [], "location": "OK1", "municipality": "Foo",
        }})
        bad_response = AsyncMock()
        bad_response.status = 500
        ok2_response = AsyncMock()
        ok2_response.status = 200
        ok2_response.json = AsyncMock(return_value={"data": {
            "geometry": {"type": "Point", "coordinates": [5.0, 53.0]},  # capital P also OK
            "components": [], "location": "OK2", "municipality": "Bar",
        }})

        responses = iter([page_response, page_response, ok1_response, bad_response, ok2_response])

        def get_side_effect(url):
            cm = AsyncMock()
            cm.__aenter__ = AsyncMock(return_value=next(responses))
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        mock_session = Mock()
        mock_session.get = Mock(side_effect=get_side_effect)

        result = await collector._fetch_all_stations(mock_session)

        # NL_BAD must be filtered out; only stations with coords survive.
        numbers = [s["number"] for s in result]
        assert "NL_BAD" not in numbers
        assert set(numbers) == {"NL_OK1", "NL_OK2"}
        for s in result:
            assert "latitude" in s and "longitude" in s

    @pytest.mark.asyncio
    async def test_closest_does_not_raise_on_filtered_list(self):
        """`closest()` over the filtered list never raises KeyError."""
        from utils.helpers import closest
        stations = [
            {"number": "A", "latitude": 52.0, "longitude": 4.0},
            {"number": "B", "latitude": 53.0, "longitude": 5.0},
        ]
        # Should pick A (closer to query)
        result = closest(stations, {"latitude": 52.1, "longitude": 4.1})
        assert result["number"] == "A"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
