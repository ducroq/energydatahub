"""
Unit tests for EntsoeHydroCollector (issue #3).

Verified via mocks against the entsoe-py API surface. Real-API smoke
test before wiring into data_fetcher.py is the user's call — these
tests pin the parsing + classification behavior.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from collectors.base import NonRetryableError, RetryConfig
from collectors.entsoe_hydro import EntsoeHydroCollector

AMS = ZoneInfo("Europe/Amsterdam")


def _make_weekly_series(country_code: str, n_weeks: int = 4) -> pd.Series:
    """Build a realistic weekly reservoir series (MWh)."""
    start = pd.Timestamp("2026-01-05T00:00:00+00:00")  # Monday of ISO week 2
    timestamps = [start + pd.Timedelta(weeks=w) for w in range(n_weeks)]
    # Reservoir levels drop through winter — typical pattern.
    base_mwh = {"NO": 8.0e7, "SE": 3.5e7, "FI": 1.0e7}.get(country_code, 5e7)
    values = [base_mwh * (1 - 0.05 * w) for w in range(n_weeks)]
    return pd.Series(values, index=pd.DatetimeIndex(timestamps))


class TestEntsoeHydroCollectorInit:
    def test_default_countries(self):
        collector = EntsoeHydroCollector(api_key="test_key")
        assert collector.country_codes == ["NO", "SE"]
        assert collector.data_type == "hydro_reservoir"
        assert collector.units == "MWh"

    def test_custom_countries(self):
        collector = EntsoeHydroCollector(api_key="test_key", country_codes=["NO", "FI"])
        assert collector.country_codes == ["NO", "FI"]

    def test_metadata_includes_zone_metadata(self):
        collector = EntsoeHydroCollector(api_key="test_key")
        start = datetime(2026, 1, 1, tzinfo=AMS)
        end = start + timedelta(weeks=4)
        meta = collector._get_metadata(start, end)
        assert meta["country_codes"] == ["NO", "SE"]
        assert meta["country_names"] == ["Norway", "Sweden"]
        assert meta["resolution"] == "weekly"
        assert "A72" in meta["document_type"]


class TestEntsoeHydroParseResponse:
    """The parsing layer is fully testable without any API."""

    def _collector(self) -> EntsoeHydroCollector:
        return EntsoeHydroCollector(api_key="test_key")

    def test_parse_basic_two_country_response(self):
        collector = self._collector()
        raw = {
            "NO": _make_weekly_series("NO"),
            "SE": _make_weekly_series("SE"),
        }
        # Range wide enough to cover the entire mock series.
        start = datetime(2026, 1, 1, tzinfo=AMS)
        end = datetime(2026, 3, 1, tzinfo=AMS)
        parsed = collector._parse_response(raw, start, end)

        assert "NO" in parsed
        assert "SE" in parsed
        # Each country has 4 weekly points
        assert len(parsed["NO"]) == 4
        assert len(parsed["SE"]) == 4
        # Each entry has the canonical sub-keys
        first_ts = next(iter(parsed["NO"]))
        entry = parsed["NO"][first_ts]
        assert "reservoir_mwh" in entry
        assert "iso_week" in entry
        assert "iso_year" in entry
        assert isinstance(entry["reservoir_mwh"], float)
        assert entry["iso_year"] == 2026

    def test_parse_filters_to_requested_window(self):
        """Only points within [start, end) survive."""
        collector = self._collector()
        raw = {"NO": _make_weekly_series("NO", n_weeks=4)}
        # Window covers only the 2nd point.
        start = datetime(2026, 1, 12, tzinfo=AMS)
        end = datetime(2026, 1, 19, tzinfo=AMS)
        parsed = collector._parse_response(raw, start, end)
        assert len(parsed["NO"]) == 1

    def test_parse_skips_nan_values(self):
        collector = self._collector()
        series = _make_weekly_series("NO", n_weeks=4)
        series.iloc[1] = float("nan")
        raw = {"NO": series}
        start = datetime(2026, 1, 1, tzinfo=AMS)
        end = datetime(2026, 3, 1, tzinfo=AMS)
        parsed = collector._parse_response(raw, start, end)
        assert len(parsed["NO"]) == 3  # 1 NaN dropped

    def test_parse_raises_when_no_points_in_window(self):
        collector = self._collector()
        raw = {"NO": _make_weekly_series("NO", n_weeks=4)}
        # Window after the data ends → nothing in range
        start = datetime(2026, 6, 1, tzinfo=AMS)
        end = datetime(2026, 7, 1, tzinfo=AMS)
        with pytest.raises(ValueError, match="No reservoir data points"):
            collector._parse_response(raw, start, end)


class TestEntsoeHydroValidate:
    def test_in_range_data_passes(self):
        collector = EntsoeHydroCollector(api_key="test_key")
        data = {
            "NO": {
                "2026-01-19T01:00:00+01:00": {
                    "reservoir_mwh": 8.0e7,
                    "iso_week": 4,
                    "iso_year": 2026,
                },
            },
        }
        ok, warnings = collector._validate_data(
            data, datetime(2026, 1, 1, tzinfo=AMS), datetime(2026, 3, 1, tzinfo=AMS)
        )
        assert ok is True
        assert warnings == []

    def test_implausibly_large_value_flagged(self):
        collector = EntsoeHydroCollector(api_key="test_key")
        data = {
            "NO": {
                "2026-01-19T01:00:00+01:00": {
                    "reservoir_mwh": 5.0e9,  # 5 PWh — physically impossible
                    "iso_week": 4,
                    "iso_year": 2026,
                },
            },
        }
        ok, warnings = collector._validate_data(
            data, datetime(2026, 1, 1, tzinfo=AMS), datetime(2026, 3, 1, tzinfo=AMS)
        )
        assert ok is False
        assert any("out of plausible range" in w for w in warnings)

    def test_norway_peak_within_bound(self):
        """Norway's physical maximum (~85 TWh = 8.5e7 MWh) must pass."""
        collector = EntsoeHydroCollector(api_key="test_key")
        data = {
            "NO": {
                "2026-01-19T01:00:00+01:00": {
                    "reservoir_mwh": 8.5e7,
                    "iso_week": 4,
                    "iso_year": 2026,
                },
            },
        }
        ok, warnings = collector._validate_data(
            data, datetime(2026, 1, 1, tzinfo=AMS), datetime(2026, 3, 1, tzinfo=AMS)
        )
        assert ok is True
        assert warnings == []

    def test_2x_unit_error_caught_after_tightening(self):
        """A 2x unit-scaling error (e.g. GWh mislabeled MWh on NO at peak)
        produces 1.7e8 — must be flagged by the post-review bound 1.2e8.
        Regression for dataset-qa finding on 935c483."""
        collector = EntsoeHydroCollector(api_key="test_key")
        data = {
            "NO": {
                "2026-01-19T01:00:00+01:00": {
                    "reservoir_mwh": 1.7e8,  # 2× Norway peak
                    "iso_week": 4,
                    "iso_year": 2026,
                },
            },
        }
        ok, warnings = collector._validate_data(
            data, datetime(2026, 1, 1, tzinfo=AMS), datetime(2026, 3, 1, tzinfo=AMS)
        )
        assert ok is False
        assert any("out of plausible range" in w for w in warnings)

    def test_empty_data_fails(self):
        collector = EntsoeHydroCollector(api_key="test_key")
        ok, warnings = collector._validate_data(
            {}, datetime(2026, 1, 1, tzinfo=AMS), datetime(2026, 3, 1, tzinfo=AMS)
        )
        assert ok is False


class TestEntsoeHydroFetchClassifier:
    """The fetch layer's NonRetryableError surfacing is essential because
    it inherits the #25 bail-out pattern from the BaseCollector."""

    @pytest.mark.asyncio
    async def test_fetch_raises_non_retryable_when_all_zones_return_none(self):
        """If every zone returns None after retries, raise NonRetryableError
        so the BaseCollector outer loop bails out cleanly (reviewer BLOCKER
        on 935c483: previously raised ValueError which got caught by the
        outer Exception handler and triggered max_attempts retries)."""
        collector = EntsoeHydroCollector(api_key="test_key")

        # Patch _retry_single to always return None (mimics two zones both failing)
        async def always_none(*args, **kwargs):
            return None

        with patch.object(collector, "_retry_single", side_effect=always_none):
            with pytest.raises(NonRetryableError, match="No reservoir data"):
                await collector._fetch_raw_data(
                    datetime(2026, 1, 1, tzinfo=AMS),
                    datetime(2026, 2, 1, tzinfo=AMS),
                )

    @pytest.mark.asyncio
    async def test_all_zones_empty_makes_only_one_attempt(self):
        """End-to-end via collect(): all-zones-empty bails out on first try.

        Regression for the reviewer BLOCKER: if this raises ValueError
        instead of NonRetryableError, the outer retry loop would run
        max_attempts (default 3) times, each calling _retry_single twice
        (once per zone). Counting the per-zone calls confirms the
        bail-out fires after the first attempt.
        """
        collector = EntsoeHydroCollector(
            api_key="test_key",
            country_codes=["NO", "SE"],
            retry_config=RetryConfig(max_attempts=3, initial_delay=0.01),
        )

        call_count = 0

        async def returns_none(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return None

        with patch.object(collector, "_retry_single", side_effect=returns_none):
            start = datetime(2026, 1, 1, tzinfo=AMS)
            end = datetime(2026, 2, 1, tzinfo=AMS)
            result = await collector.collect(start, end)

        assert result is None
        # Only 2 calls (one per zone, single outer attempt). Without the
        # bail-out, would be 6 (2 zones × 3 outer retries).
        assert call_count == 2, (
            f"Expected 2 calls (1 outer attempt × 2 zones), got {call_count}. "
            "The retry loop is still burning attempts on a permanent state."
        )

    @pytest.mark.asyncio
    async def test_fetch_propagates_non_retryable_error(self):
        """A NonRetryableError from one zone reaches BaseCollector's retry
        loop so it bails out without burning further attempts (issue #25)."""
        collector = EntsoeHydroCollector(api_key="test_key", country_codes=["NO"])

        async def raise_non_retryable(*args, **kwargs):
            raise NonRetryableError("simulated permanent error from API")

        with patch.object(collector, "_retry_single", side_effect=raise_non_retryable):
            with pytest.raises(NonRetryableError):
                await collector._fetch_raw_data(
                    datetime(2026, 1, 1, tzinfo=AMS),
                    datetime(2026, 2, 1, tzinfo=AMS),
                )

    @pytest.mark.asyncio
    async def test_fetch_dataframe_response_collapses_to_series(self):
        """Some entsoe-py versions return a DataFrame instead of a Series.
        Verify the collapse logic handles that."""
        collector = EntsoeHydroCollector(api_key="test_key", country_codes=["NO"])

        # Build a DataFrame mimicking the alternate response shape
        df = _make_weekly_series("NO").to_frame(name="reservoir")

        async def returns_df(*args, **kwargs):
            return df

        with patch.object(collector, "_retry_single", side_effect=returns_df):
            result = await collector._fetch_raw_data(
                datetime(2026, 1, 1, tzinfo=AMS),
                datetime(2026, 2, 1, tzinfo=AMS),
            )

        assert "NO" in result
        assert isinstance(result["NO"], pd.Series)
        assert len(result["NO"]) == 4


class TestEntsoeHydroEndToEndCollect:
    """End-to-end through `collect()`: mocked API → populated EnhancedDataSet."""

    @pytest.mark.asyncio
    async def test_collect_success_path(self):
        collector = EntsoeHydroCollector(
            api_key="test_key",
            country_codes=["NO", "SE"],
            retry_config=RetryConfig(max_attempts=2, initial_delay=0.01),
        )

        async def fake_per_country(query_func, *args, max_attempts=2, **kwargs):
            # Inspect which country was requested via the partial
            country_code = query_func.keywords.get("country_code")
            return _make_weekly_series(country_code, n_weeks=4)

        with patch.object(collector, "_retry_single", side_effect=fake_per_country):
            start = datetime(2026, 1, 1, tzinfo=AMS)
            end = datetime(2026, 3, 1, tzinfo=AMS)
            dataset = await collector.collect(start, end)

        assert dataset is not None
        assert dataset.metadata["data_type"] == "hydro_reservoir"
        assert dataset.metadata["resolution"] == "weekly"
        assert "NO" in dataset.data
        assert "SE" in dataset.data
        # Each country has weekly points
        first_no_ts = next(iter(dataset.data["NO"]))
        no_entry = dataset.data["NO"][first_no_ts]
        assert "reservoir_mwh" in no_entry
        assert no_entry["reservoir_mwh"] > 0
