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
    async def test_empty_station_list_not_cached(self):
        """Empty station list must NOT be cached (issue #13).

        Caching an empty list would lock collection out for the full 24h
        TTL — the caller's `if not stations: raise ValueError` check would
        fire every run until the cache expires. Inverted from the original
        `test_empty_station_list_cached` which documented the bug as
        intended behavior.
        """
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = []
            mock_session = AsyncMock()

            stations = await collector._get_stations_cached(mock_session)

            # Empty result is returned (caller will raise) but NOT persisted.
            assert stations == []
            assert LuchtmeetnetCollector._station_cache is None
            assert LuchtmeetnetCollector._cache_timestamp is None

    @pytest.mark.asyncio
    async def test_empty_fetch_does_not_overwrite_existing_cache(self):
        """A subsequent empty fetch must not wipe a previously-good cache (#13).

        Scenario: yesterday's run cached a healthy station list, the cache
        has now expired, today's fetch returns empty (upstream outage).
        Before the fix the empty list overwrote the cached good list. After
        the fix the cache slot is preserved so a third call within the
        original 24h window could still hit a sane snapshot.
        """
        good_stations = [
            {'number': 'NL001', 'latitude': 52.37, 'longitude': 4.89}
        ]
        LuchtmeetnetCollector._station_cache = good_stations
        original_ts = datetime.now() - timedelta(hours=25)  # expired
        LuchtmeetnetCollector._cache_timestamp = original_ts

        collector = LuchtmeetnetCollector(52.37, 4.89)

        with patch.object(collector, '_fetch_all_stations', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = []
            mock_session = AsyncMock()

            result = await collector._get_stations_cached(mock_session)

            # Empty result is returned, but the previously-good cache stays put.
            assert result == []
            assert LuchtmeetnetCollector._station_cache == good_stations
            assert LuchtmeetnetCollector._cache_timestamp == original_ts

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

    @pytest.mark.asyncio
    async def test_filter_drops_station_on_network_exception(self):
        """A per-station detail-fetch that raises (e.g. ClientError, timeout) is dropped, not propagated.

        This exercises the broad `try: ... except Exception` branch added 2026-06-06.
        Before the guard, an aiohttp transient on one station's detail-fetch killed
        the whole collection — that path was the *reason* for the try/except, but
        the original regression test only covered the HTTP-non-200 path (which goes
        through `continue`, not `except`).
        """
        from aiohttp import ClientConnectionError

        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)

        # First page response (used for both pagination discovery + page-1 fetch)
        page_response = AsyncMock()
        page_response.status = 200
        page_response.json = AsyncMock(return_value={
            "pagination": {"page_list": [1]},
            "data": [
                {"number": "NL_OK"},
                {"number": "NL_NETERR"},
            ],
        })

        ok_response = AsyncMock()
        ok_response.status = 200
        ok_response.json = AsyncMock(return_value={"data": {
            "geometry": {"type": "point", "coordinates": [4.0, 52.0]},
            "components": [], "location": "OK", "municipality": "Foo",
        }})

        call_count = [0]

        def get_side_effect(url):
            call_count[0] += 1
            cm = AsyncMock()
            cm.__aexit__ = AsyncMock(return_value=None)
            if call_count[0] in (1, 2):
                # Page-list discovery + page-1 fetch
                cm.__aenter__ = AsyncMock(return_value=page_response)
            elif call_count[0] == 3:
                # NL_OK detail
                cm.__aenter__ = AsyncMock(return_value=ok_response)
            else:
                # NL_NETERR detail — network error on context-manager entry
                cm.__aenter__ = AsyncMock(side_effect=ClientConnectionError("connection reset"))
            return cm

        mock_session = Mock()
        mock_session.get = Mock(side_effect=get_side_effect)

        # Must NOT raise — the try/except in _fetch_all_stations catches it,
        # the filter drops the un-coord'd station, and the good station comes back.
        result = await collector._fetch_all_stations(mock_session)

        numbers = [s["number"] for s in result]
        assert "NL_NETERR" not in numbers
        assert numbers == ["NL_OK"]


class TestLuchtmeetnetNarrowExcept:
    """Issue #14: per-station detail-fetch must only swallow realistic
    upstream-failure modes (aiohttp / asyncio.timeout / JSON / Key / Type /
    Value). Programmer errors like AttributeError must propagate so future
    refactor mistakes surface immediately rather than vanishing into the
    'station was filtered' path.
    """

    @pytest.mark.asyncio
    async def test_attribute_error_propagates_through_narrow_except(self):
        """AttributeError from inside the loop body is NOT swallowed."""
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)

        page_response = AsyncMock()
        page_response.status = 200
        page_response.json = AsyncMock(return_value={
            "pagination": {"page_list": [1]},
            "data": [{"number": "NL_ATTR"}],
        })

        # Detail-fetch returns a response whose .json() coroutine raises
        # AttributeError. That's the shape a real programmer error (e.g.
        # `data.dat['x']` typo) would take, and the narrow except clause
        # must not catch it.
        bad_detail = AsyncMock()
        bad_detail.status = 200
        bad_detail.json = AsyncMock(side_effect=AttributeError("simulated typo"))

        responses = iter([page_response, page_response, bad_detail])

        def get_side_effect(url):
            cm = AsyncMock()
            cm.__aenter__ = AsyncMock(return_value=next(responses))
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        mock_session = Mock()
        mock_session.get = Mock(side_effect=get_side_effect)

        with pytest.raises(AttributeError, match="simulated typo"):
            await collector._fetch_all_stations(mock_session)


class TestLuchtmeetnetStationCompletenessQuality:
    """Issue #12: when >N% of stations are filtered, the collector must
    emit a `station_completeness` quality issue in metadata so the
    workflow's data_quality_report gate can act on it.
    """

    def _reset_class_state(self):
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None
        LuchtmeetnetCollector._cache_filter_stats = None

    def test_no_issue_when_no_stations_filtered(self):
        """0% filtered → no quality issue (clean upstream)."""
        self._reset_class_state()

        collector = LuchtmeetnetCollector(52.37, 4.89)
        collector._last_filter_stats = {'total': 100, 'filtered': 0}
        start = datetime.now()
        end = start + timedelta(hours=1)
        metadata = collector._get_metadata(start, end)

        assert metadata['stations_filtered'] == 0
        assert metadata['stations_filtered_pct'] == 0.0
        assert 'collector_quality_issues' not in metadata

    def test_no_issue_below_warning_threshold(self):
        """20% filtered → still below 25% threshold → no issue."""
        self._reset_class_state()

        collector = LuchtmeetnetCollector(52.37, 4.89)
        collector._last_filter_stats = {'total': 100, 'filtered': 20}
        start = datetime.now()
        end = start + timedelta(hours=1)
        metadata = collector._get_metadata(start, end)

        assert metadata['stations_filtered_pct'] == 20.0
        assert 'collector_quality_issues' not in metadata

    def test_warning_issue_above_warning_threshold(self):
        """30% filtered → warning severity. The BaseCollector hook now
        owns the metadata injection (refactoring H1 collapsed the two
        prior dialects); `_get_metadata` only POPULATES via
        `_add_quality_issue`. The injection happens in `collect()`."""
        self._reset_class_state()

        collector = LuchtmeetnetCollector(52.37, 4.89)
        collector._last_filter_stats = {'total': 100, 'filtered': 30}
        start = datetime.now()
        end = start + timedelta(hours=1)
        collector._get_metadata(start, end)

        issues = collector._collector_quality_issues
        assert len(issues) == 1
        assert issues[0]['check_name'] == 'station_completeness'
        assert issues[0]['severity'] == 'warning'
        assert issues[0]['details'] == {'filtered': 30, 'total': 100}

    def test_critical_issue_above_critical_threshold(self):
        """60% filtered → critical severity (workflow gate aborts publish)."""
        self._reset_class_state()

        collector = LuchtmeetnetCollector(52.37, 4.89)
        collector._last_filter_stats = {'total': 100, 'filtered': 60}
        start = datetime.now()
        end = start + timedelta(hours=1)
        collector._get_metadata(start, end)

        issues = collector._collector_quality_issues
        assert len(issues) == 1
        assert issues[0]['severity'] == 'critical'

    @pytest.mark.asyncio
    async def test_filter_stats_populated_on_instance_after_fetch(self):
        """_fetch_all_stations must populate self._last_filter_stats (INSTANCE-
        scoped per the PR #16 review HIGH-1 fix)."""
        self._reset_class_state()

        collector = LuchtmeetnetCollector(52.37, 4.89)

        page_response = AsyncMock()
        page_response.status = 200
        page_response.json = AsyncMock(return_value={
            "pagination": {"page_list": [1]},
            "data": [{"number": "OK1"}, {"number": "BAD"}, {"number": "OK2"}],
        })
        ok_response = AsyncMock()
        ok_response.status = 200
        ok_response.json = AsyncMock(return_value={"data": {
            "geometry": {"type": "point", "coordinates": [4.0, 52.0]},
            "components": [], "location": "ok", "municipality": "x",
        }})
        bad_response = AsyncMock()
        bad_response.status = 500

        responses = iter([
            page_response, page_response,  # page-list discovery + page-1 fetch
            ok_response, bad_response, ok_response,  # 3 detail fetches
        ])

        def get_side_effect(url):
            cm = AsyncMock()
            cm.__aenter__ = AsyncMock(return_value=next(responses))
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        mock_session = Mock()
        mock_session.get = Mock(side_effect=get_side_effect)

        await collector._fetch_all_stations(mock_session)

        assert collector._last_filter_stats == {'total': 3, 'filtered': 1}

    def test_concurrent_collectors_do_not_share_filter_stats(self):
        """HIGH-1 regression: two concurrent buurt collectors must have
        independent `_last_filter_stats`. Before the fix this was a
        class-level slot — one buurt's stats could overwrite another's
        between fetch and `_get_metadata`, misattributing the
        station_completeness signal to the wrong buurt.
        """
        self._reset_class_state()

        c1 = LuchtmeetnetCollector(52.37, 4.89)
        c2 = LuchtmeetnetCollector(51.99, 5.90)

        c1._last_filter_stats = {'total': 100, 'filtered': 60}  # critical
        c2._last_filter_stats = {'total': 100, 'filtered': 5}   # clean

        m1 = c1._get_metadata(datetime.now(), datetime.now() + timedelta(hours=1))
        m2 = c2._get_metadata(datetime.now(), datetime.now() + timedelta(hours=1))

        # c1 emits critical, c2 emits no issue — neither sees the other's stats.
        # Quality signals live on the populator (instance state) per
        # refactoring H1; metadata-side injection happens in collect().
        assert c1._collector_quality_issues[0]['severity'] == 'critical'
        assert c2._collector_quality_issues == []
        assert m1['stations_filtered'] == 60
        assert m2['stations_filtered'] == 5


class TestDataQualityCollectorIssuesIntegration:
    """data_quality.validate_dataset must surface collector_quality_issues
    from metadata into the per-dataset QualityIssue list (#12 plumbing).
    """

    def test_collector_critical_issue_flips_dataset_status(self):
        from utils.data_quality import validate_dataset, Severity
        from utils.data_types import EnhancedDataSet
        from zoneinfo import ZoneInfo

        ams = ZoneInfo('Europe/Amsterdam')
        now = datetime.now(ams)

        # Minimal dataset with a critical collector-emitted issue.
        dataset = EnhancedDataSet(
            metadata={
                'data_type': 'air',
                'source': 'Luchtmeetnet API',
                'units': 'µg/m³',
                'start_time': now.isoformat(),
                'end_time': now.isoformat(),
                'collector': 'LuchtmeetnetCollector',
                'collector_quality_issues': [{
                    'check_name': 'station_completeness',
                    'severity': 'critical',
                    'message': '60/100 stations filtered (60%)',
                    'details': {'filtered': 60, 'total': 100},
                }],
            },
            # Enough data points to satisfy the completeness/staleness checks
            # so we know `critical` came from the collector issue, not them.
            data={
                (now - timedelta(hours=i)).isoformat(): {'NO2': 20.0}
                for i in range(24)
            },
        )

        report = validate_dataset(dataset, 'air_quality_buurt')

        station_issues = [
            i for i in report.issues if i.check_name == 'station_completeness'
        ]
        assert len(station_issues) == 1
        assert station_issues[0].severity == Severity.CRITICAL
        # Status should be critical (this is what the workflow gate checks)
        assert report.status == 'critical'

    def test_info_severity_does_not_inflate_checks_failed(self):
        """PR #16 review MEDIUM-1: INFO-severity collector issues are
        informational only and must NOT increment `checks_failed`.
        """
        from utils.data_quality import validate_dataset
        from utils.data_types import EnhancedDataSet
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo('Europe/Amsterdam'))
        dataset = EnhancedDataSet(
            metadata={
                'data_type': 'air',
                'source': 'Luchtmeetnet API',
                'units': 'µg/m³',
                'collector': 'LuchtmeetnetCollector',
                'collector_quality_issues': [{
                    'check_name': 'station_completeness',
                    'severity': 'info',
                    'message': 'station selection nominal',
                    'details': {'filtered': 1, 'total': 100},
                }],
            },
            data={
                (now - timedelta(hours=i)).isoformat(): {'NO2': 20.0}
                for i in range(24)
            },
        )

        report = validate_dataset(dataset, 'air_quality_buurt')

        # The info issue is recorded but not counted as a failed check.
        info_issues = [i for i in report.issues if i.severity.value == 'info']
        assert len(info_issues) == 1
        # checks_failed should reflect only ≥WARNING issues. Generic checks
        # all pass on this dataset, so checks_failed must be 0.
        assert report.checks_failed == 0
        # Status stays at the lowest severity (info → 'info').
        assert report.status == 'info'

    def test_unknown_severity_downgrades_to_warning_and_counts(self):
        """Malformed severity string → conservative WARNING + counted as
        failed (so the bad emission doesn't silently look benign)."""
        from utils.data_quality import validate_dataset, Severity
        from utils.data_types import EnhancedDataSet
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo('Europe/Amsterdam'))
        dataset = EnhancedDataSet(
            metadata={
                'data_type': 'air',
                'source': 'Luchtmeetnet API',
                'units': 'µg/m³',
                'collector': 'LuchtmeetnetCollector',
                'collector_quality_issues': [{
                    'check_name': 'mystery',
                    'severity': 'NOT_A_REAL_LEVEL',
                    'message': 'malformed emission',
                }],
            },
            data={
                (now - timedelta(hours=i)).isoformat(): {'NO2': 20.0}
                for i in range(24)
            },
        )

        report = validate_dataset(dataset, 'air_quality_buurt')

        mystery = [i for i in report.issues if i.check_name == 'mystery']
        assert len(mystery) == 1
        assert mystery[0].severity == Severity.WARNING
        assert report.checks_failed >= 1


class TestLuchtmeetnetStationNumberValidation:
    """PR #16 security audit MEDIUM-2: station['number'] from upstream is
    interpolated into URLs and log messages. Without validation, an attacker
    controlling the upstream API can inject newlines (log injection) or
    path-traversal segments.
    """

    @pytest.mark.asyncio
    async def test_malformed_station_number_with_newline_is_skipped(self):
        """A station number containing a newline (log injection) is skipped
        without ever appearing in a log or URL."""
        LuchtmeetnetCollector._station_cache = None
        LuchtmeetnetCollector._cache_timestamp = None

        collector = LuchtmeetnetCollector(52.37, 4.89)

        page_response = AsyncMock()
        page_response.status = 200
        page_response.json = AsyncMock(return_value={
            "pagination": {"page_list": [1]},
            "data": [
                {"number": "NL001"},                           # valid
                {"number": "NL\n[ERROR] injected fake log"},   # log-injection
                {"number": "../etc/passwd"},                   # path-traversal
                {"number": ""},                                # empty
                {"number": 12345},                             # wrong type
            ],
        })
        ok_response = AsyncMock()
        ok_response.status = 200
        ok_response.json = AsyncMock(return_value={"data": {
            "geometry": {"type": "point", "coordinates": [4.0, 52.0]},
            "components": [], "location": "ok", "municipality": "x",
        }})

        # Only valid NL001 should reach the detail-fetch.
        responses = iter([page_response, page_response, ok_response])

        def get_side_effect(url):
            # URL must never contain raw injected content.
            assert '\n' not in url
            assert '..' not in url
            cm = AsyncMock()
            cm.__aenter__ = AsyncMock(return_value=next(responses))
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        mock_session = Mock()
        mock_session.get = Mock(side_effect=get_side_effect)

        result = await collector._fetch_all_stations(mock_session)

        numbers = [s.get("number") for s in result]
        assert numbers == ["NL001"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
