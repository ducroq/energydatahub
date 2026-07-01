"""
Unit tests for EntsoeCollector (day-ahead prices)

Focus: upstream-gap handling. When ENTSO-E responds successfully
but publishes no day-ahead prices for the window, entsoe-py raises
NoMatchingDataError. The collector must translate that into an
UpstreamNoDataError so the run fast-fails without tripping the circuit
breaker, sets last_run_no_upstream_data, and reports CollectorStatus.NO_DATA
— letting the orchestrator keep publishing the healthy feeds.
"""
import pytest
from datetime import datetime
from zoneinfo import ZoneInfo
from unittest.mock import MagicMock, patch

from entsoe.exceptions import NoMatchingDataError

from collectors.entsoe import EntsoeCollector
from collectors.base import (
    RetryConfig,
    CircuitBreakerConfig,
    CollectorStatus,
    UpstreamNoDataError,
    CircuitState,
)

AMS = ZoneInfo('Europe/Amsterdam')
START = datetime(2026, 6, 30, 0, 0, tzinfo=AMS)
END = datetime(2026, 7, 1, 0, 0, tzinfo=AMS)


class TestEntsoeUpstreamGap:
    """NoMatchingDataError → UpstreamNoDataError → graceful degradation."""

    @pytest.mark.asyncio
    async def test_no_matching_data_translated_to_upstream_no_data(self):
        """_fetch_raw_data turns entsoe-py's NoMatchingDataError into
        UpstreamNoDataError (a NonRetryableError, so no retry burn)."""
        collector = EntsoeCollector(api_key="test")
        mock_client = MagicMock()
        mock_client.query_day_ahead_prices.side_effect = NoMatchingDataError()

        with patch('collectors.entsoe.EntsoePandasClient', return_value=mock_client):
            with pytest.raises(UpstreamNoDataError):
                await collector._fetch_raw_data(START, END, country_code='NL')

    @pytest.mark.asyncio
    async def test_collect_reports_no_data_and_sets_flag(self):
        """collect() returns None, flags upstream-empty, records NO_DATA."""
        collector = EntsoeCollector(
            api_key="test",
            retry_config=RetryConfig(max_attempts=3, initial_delay=0.01),
        )
        mock_client = MagicMock()
        mock_client.query_day_ahead_prices.side_effect = NoMatchingDataError()

        with patch('collectors.entsoe.EntsoePandasClient', return_value=mock_client):
            result = await collector.collect(START, END, country_code='NL')

        assert result is None
        assert collector.last_run_no_upstream_data is True
        metrics = collector.get_metrics(limit=1)
        assert metrics[0].status == CollectorStatus.NO_DATA
        # Fast-fail: NonRetryableError stops after the first attempt.
        assert mock_client.query_day_ahead_prices.call_count == 1

    @pytest.mark.asyncio
    async def test_upstream_gap_does_not_trip_circuit_breaker(self):
        """An upstream gap is not a service failure — the breaker stays CLOSED
        so a later genuine request isn't blocked."""
        collector = EntsoeCollector(
            api_key="test",
            retry_config=RetryConfig(max_attempts=1, initial_delay=0.01),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=1, enabled=True
            ),
        )
        mock_client = MagicMock()
        mock_client.query_day_ahead_prices.side_effect = NoMatchingDataError()

        with patch('collectors.entsoe.EntsoePandasClient', return_value=mock_client):
            await collector.collect(START, END, country_code='NL')
            await collector.collect(START, END, country_code='NL')

        assert collector._circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_genuine_failure_is_not_flagged_upstream_empty(self):
        """A real error (network) → FAILED, flag stays False, breaker records
        the failure — the opposite of the upstream-gap path."""
        collector = EntsoeCollector(
            api_key="test",
            retry_config=RetryConfig(max_attempts=1, initial_delay=0.01),
        )
        mock_client = MagicMock()
        mock_client.query_day_ahead_prices.side_effect = ConnectionError("boom")

        with patch('collectors.entsoe.EntsoePandasClient', return_value=mock_client):
            result = await collector.collect(START, END, country_code='NL')

        assert result is None
        assert collector.last_run_no_upstream_data is False
        assert collector.get_metrics(limit=1)[0].status == CollectorStatus.FAILED

    @pytest.mark.asyncio
    async def test_flag_resets_on_next_successful_collect(self):
        """last_run_no_upstream_data must not leak across runs."""
        import pandas as pd

        collector = EntsoeCollector(
            api_key="test",
            retry_config=RetryConfig(max_attempts=1, initial_delay=0.01),
        )
        mock_client = MagicMock()

        # First: upstream gap → flag True
        mock_client.query_day_ahead_prices.side_effect = NoMatchingDataError()
        with patch('collectors.entsoe.EntsoePandasClient', return_value=mock_client):
            await collector.collect(START, END, country_code='NL')
        assert collector.last_run_no_upstream_data is True

        # Then: a good response → flag resets to False
        idx = pd.date_range('2026-06-30T00:00', periods=24, freq='h', tz=AMS)
        good = pd.Series([50.0] * 24, index=idx)
        mock_client.query_day_ahead_prices.side_effect = None
        mock_client.query_day_ahead_prices.return_value = good
        with patch('collectors.entsoe.EntsoePandasClient', return_value=mock_client):
            result = await collector.collect(START, END, country_code='NL')

        assert result is not None
        assert collector.last_run_no_upstream_data is False


class TestEntsoeParseGuard:
    """#38 review follow-up — if the API returns rows but none fall inside the
    requested window after parsing (a window/timezone bug), that must surface
    as a genuine failure, NOT a silently-published empty price series and NOT
    an upstream gap."""

    def test_all_rows_out_of_window_raises(self):
        import pandas as pd
        collector = EntsoeCollector(api_key="test")
        # Series entirely BEFORE the requested window → parse yields zero points.
        idx = pd.date_range('2026-06-01T00:00', periods=24, freq='h', tz=AMS)
        series = pd.Series([50.0] * 24, index=idx)
        with pytest.raises(ValueError, match="none fell within"):
            collector._parse_response(series, START, END)

    def test_in_window_rows_parse_normally(self):
        import pandas as pd
        collector = EntsoeCollector(api_key="test")
        idx = pd.date_range('2026-06-30T00:00', periods=24, freq='h', tz=AMS)
        series = pd.Series([50.0] * 24, index=idx)
        parsed = collector._parse_response(series, START, END)
        assert len(parsed) == 24
