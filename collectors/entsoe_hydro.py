"""
ENTSO-E Nordic Hydro Reservoir Collector (#3)
----------------------------------------------
Collects weekly hydro reservoir storage levels for Norway and Sweden from
the ENTSO-E Transparency Platform (document type A72 — Reservoir filling
information). Norway generates ~95% of electricity from hydro; reservoir
levels set Nordic price floors and feed through to NL via the NorNed
cable (700 MW). A slow-moving but powerful leading indicator for
multi-day price forecasts.

File: collectors/entsoe_hydro.py
Created: 2026-06-07
Author: Energy Data Hub Project

API Documentation:
    https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
    Document type: A72 (Reservoir filling information)
    Resolution:    weekly
    Unit:          MWh (stored energy equivalent of current reservoir volume)

Status:
    Wired into `data_fetcher.py` (#3 closed). Publishes to
    `nordic_hydro.json` daily; field-range validated via
    `FIELD_RANGES_BY_TYPE['hydro_reservoir']` in `utils/data_quality.py`.

Usage:
    from collectors.entsoe_hydro import EntsoeHydroCollector
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    collector = EntsoeHydroCollector(
        api_key="your_api_key",
        country_codes=['NO', 'SE'],
    )
    end = datetime.now(ZoneInfo('Europe/Amsterdam'))
    start = end - timedelta(weeks=52)  # one year of weekly data
    data = await collector.collect(start, end)
"""

import asyncio
from datetime import datetime
from functools import partial
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

try:
    from entsoe import EntsoePandasClient
except ImportError:
    # Defer the import error to instantiation so the module can still be
    # imported in environments without entsoe-py (e.g. running a subset
    # of tests). The collector itself fails clearly when invoked.
    EntsoePandasClient = None  # type: ignore

from collectors.base import (
    BaseCollector,
    CircuitBreakerConfig,
    NonRetryableError,
    RetryConfig,
)
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class EntsoeHydroCollector(BaseCollector):
    """
    Collector for ENTSO-E aggregate water reservoir + hydro storage data.

    Each per-country payload is a weekly time series of stored energy in
    MWh (the energy that could be generated if the current reservoir
    volume were fully drawn down). The collector publishes these as
    nested per-country dicts so downstream ML can join against the same
    timeline.
    """

    # Country zones with significant hydro reservoirs relevant to NL prices.
    # NO: ~95% hydro generation, primary NorNed cable source.
    # SE: large hydro fleet, indirectly influences NL via DE-DK-SE corridor.
    DEFAULT_COUNTRY_CODES: List[str] = ['NO', 'SE']

    ZONE_NAMES: Dict[str, str] = {
        'NO': 'Norway',
        'SE': 'Sweden',
        'FI': 'Finland',  # not enabled by default but supported
    }

    # Per-zone completeness floor (#29). The aggregate
    # EXPECTED_MIN_POINTS['nordic_hydro']=12 (6 fresh weeks × 2 zones)
    # catches total collapse but not single-zone half-dark (e.g. NO=10,
    # SE=2 still aggregates to 12). Six fresh weekly points per zone is
    # the same definition applied per-zone.
    EXPECTED_POINTS_PER_ZONE: int = 6

    def __init__(
        self,
        api_key: str,
        country_codes: Optional[List[str]] = None,
        retry_config: Optional[RetryConfig] = None,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
    ):
        """
        Initialise the collector.

        Args:
            api_key: ENTSO-E Transparency Platform API key
            country_codes: ENTSO-E country/bidding-zone codes to fetch
                (default: ['NO', 'SE'])
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="EntsoeHydroCollector",
            data_type="hydro_reservoir",
            source="ENTSO-E Transparency Platform (A72)",
            units="MWh",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config,
        )
        self.api_key = api_key
        self.country_codes = country_codes or list(self.DEFAULT_COUNTRY_CODES)
        # Per-run quality signals use the BaseCollector hook
        # (`self._add_quality_issue` + auto-reset in `collect()` + auto-
        # inject in `_get_metadata`). No collector-specific plumbing needed
        # here — see `collectors/base.py`. Refactoring-guide H1 collapsed
        # the prior dialect into the base hook (was two divergent
        # implementations across Luchtmeetnet + EntsoeHydro).

        if EntsoePandasClient is None:
            raise ImportError(
                "entsoe-py is not installed. Run `pip install entsoe-py` "
                "to use EntsoeHydroCollector."
            )

        self.logger.info(
            f"Initialised hydro collector for zones: {self.country_codes}"
        )

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> Dict[str, pd.Series]:
        """
        Fetch weekly reservoir data per country from ENTSO-E.

        Args:
            start_time: Start of time range (any timezone)
            end_time:   End of time range

        Returns:
            Dict mapping country_code → pandas Series indexed by week
            timestamp with MWh values.

        Raises:
            NonRetryableError: If the API returns a permanent client error
                (e.g. 400/401/403/404) — bail-out behaviour mirrors the
                TenneT collector's classifier (issue #25).
            Exception: For transient errors, propagated to BaseCollector's
                retry loop.
        """
        start_ts = pd.Timestamp(start_time).tz_convert('UTC')
        end_ts = pd.Timestamp(end_time).tz_convert('UTC')
        self.logger.debug(
            f"Fetching hydro reservoirs for {self.country_codes} from "
            f"{start_ts.isoformat()} to {end_ts.isoformat()}"
        )

        client = EntsoePandasClient(api_key=self.api_key)

        # Per-country: use `_retry_single` so one failed country doesn't
        # poison the whole collection. The BaseCollector-level retry still
        # wraps the outer call; this is the inner per-source backoff.
        # NonRetryableError raised by `_retry_single` (e.g. via the HTTP
        # classifier) propagates naturally out of this loop and up to
        # BaseCollector's bail-out path — no explicit catch+raise needed
        # (opus N2 dead-code removal).
        results: Dict[str, pd.Series] = {}
        for code in self.country_codes:
            query_func = partial(
                client.query_aggregate_water_reservoirs_and_hydro_storage,
                country_code=code,
                start=start_ts,
                end=end_ts,
            )
            series = await self._retry_single(query_func, max_attempts=2)
            if series is None:
                self.logger.warning(
                    f"{code}: query_aggregate_water_reservoirs returned None "
                    "after retries — skipping this zone for this run"
                )
                continue
            if isinstance(series, pd.DataFrame):
                # Some entsoe-py versions return a DataFrame; collapse to
                # the first numeric column (the reservoir level itself).
                numeric_cols = [c for c in series.columns
                                if pd.api.types.is_numeric_dtype(series[c])]
                if not numeric_cols:
                    self.logger.warning(
                        f"{code}: response has no numeric column — skipping"
                    )
                    continue
                series = series[numeric_cols[0]]
            if series.empty:
                self.logger.warning(f"{code}: empty result — skipping")
                continue
            results[code] = series
            self.logger.info(
                f"{code}: {len(series)} weekly reservoir points"
            )

        if not results:
            # All zones returned None or empty. Retrying the outer loop
            # will hit the same state — bail out via NonRetryableError so
            # BaseCollector's `_retry_with_backoff` doesn't burn additional
            # attempts (reviewer BLOCKER on 935c483: raising ValueError
            # here was caught by the outer Exception handler and got
            # retried up to max_attempts times, defeating the per-zone
            # bail-out intent).
            raise NonRetryableError(
                "No reservoir data returned for any requested country"
            )
        return results

    def _parse_response(
        self,
        raw_data: Dict[str, pd.Series],
        start_time: datetime,
        end_time: datetime,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse per-country reservoir series into normalised dict.

        Args:
            raw_data:   Dict[country_code, pd.Series] from _fetch_raw_data
            start_time: Range start (used for in-range filtering)
            end_time:   Range end

        Returns:
            Nested dict, keyed first by country then by ISO timestamp:

            {
                'NO': {
                    '2026-01-19T00:00:00+01:00': {
                        'reservoir_mwh': 8.42e7,
                        'iso_week': 4,
                        'iso_year': 2026,
                    },
                    ...
                },
                'SE': { ... }
            }

            The ISO week + year fields make it cheap for downstream ML to
            join against seasonal medians or build week-of-year features.
        """
        parsed: Dict[str, Dict[str, Any]] = {}
        for country_code, series in raw_data.items():
            country_parsed: Dict[str, Any] = {}
            for ts, value in series.items():
                if pd.isna(value):
                    continue
                dt = ts.to_pydatetime() if hasattr(ts, 'to_pydatetime') else ts
                # Defensive guard (sonnet): some entsoe-py versions have
                # been observed returning a tz-naive Timestamp index for
                # certain series. Comparing naive `dt` against tz-aware
                # `start_time`/`end_time` raises TypeError. Assume UTC
                # for naive timestamps — the A72 endpoint documents UTC
                # cadence — then let the downstream Amsterdam normalisation
                # handle the conversion.
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo('UTC'))
                if not (start_time <= dt < end_time):
                    continue
                amsterdam_dt = normalize_timestamp_to_amsterdam(dt)
                iso_year, iso_week, _ = amsterdam_dt.isocalendar()
                country_parsed[amsterdam_dt.isoformat()] = {
                    'reservoir_mwh': float(value),
                    'iso_week': int(iso_week),
                    'iso_year': int(iso_year),
                }
            if country_parsed:
                parsed[country_code] = country_parsed
                self.logger.debug(
                    f"{country_code}: parsed {len(country_parsed)} weekly points "
                    f"in range [{start_time.date()}, {end_time.date()})"
                )

        if not parsed:
            raise ValueError(
                "No reservoir data points fell within the requested window"
            )
        return parsed

    def _normalize_timestamps(
        self,
        data: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """No-op: timestamps already normalised in `_parse_response`."""
        return data

    def _validate_data(
        self,
        data: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[bool, List[str]]:
        """Validate the nested per-country structure."""
        warnings: List[str] = []
        # Reset per-run structured signals explicitly so callers that
        # exercise `_validate_data` directly (unit tests, manual scripts)
        # see clean state. `collect()` also resets at its top, so under
        # the production flow this is a defensive double-reset rather
        # than load-bearing.
        self._reset_quality_issues()
        if not data:
            return False, ["No hydro reservoir data collected"]
        # Iterate `data.items()` rather than `self.country_codes`: if a
        # future entsoe-py version leaks an unexpected zone through
        # `_fetch_raw_data`'s per-code filter, the per-zone completeness
        # signal still fires (tagged with the unexpected zone code).
        # Default-to-defensive — a library regression that silently adds
        # a zone should surface as a quality warning, not vanish.
        # Pinned by `test_unexpected_zone_in_data_does_not_crash` (#31).
        for country_code, country_data in data.items():
            n_points = len(country_data) if country_data else 0
            # Per-zone completeness signal (#29). The structured signal
            # below covers both the empty-zone and the half-dark cases
            # uniformly; emit the legacy free-text warning only for the
            # empty case so an operator scanning the run log still sees
            # the zone name (the structured signal lands in the report).
            if not country_data:
                warnings.append(f"{country_code}: no data points")
            if n_points < self.EXPECTED_POINTS_PER_ZONE:
                self._add_quality_issue(
                    check_name='completeness_per_zone',
                    severity='warning',
                    message=(
                        f"{country_code} has {n_points} weekly points "
                        f"(expected >= {self.EXPECTED_POINTS_PER_ZONE})"
                    ),
                    details={
                        'zone': country_code,
                        'observed': n_points,
                        'expected': self.EXPECTED_POINTS_PER_ZONE,
                    },
                )
            if not country_data:
                continue
            for ts, point in country_data.items():
                mwh = point.get('reservoir_mwh')
                # Physical bounds derived from NVE / Statnett public data:
                # NO peak usable reservoir capacity ~85 TWh = 8.5e7 MWh,
                # SE ~33 TWh = 3.3e7 MWh, FI ~5 TWh = 5e6 MWh. 1.2e8 gives
                # ~40% headroom over Norway's peak — tight enough to catch
                # a 2x unit-scaling error (e.g. GWh reported as MWh) and a
                # 10x error (kWh reported as MWh) is caught at ~8.5e8 too.
                # ENTSO-E A72 returns non-negative MWh; zero is a legal
                # dead-storage edge case.
                if mwh is None or mwh < 0 or mwh > 1.2e8:
                    warnings.append(
                        f"{country_code} {ts}: reservoir_mwh={mwh} out of "
                        "plausible range [0, 1.2e8]"
                    )
        return not warnings, warnings

    def _get_metadata(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> Dict[str, Any]:
        """Augment base metadata with hydro-specific fields."""
        metadata = super()._get_metadata(start_time, end_time)
        metadata.update({
            'country_codes': list(self.country_codes),
            'country_names': [
                self.ZONE_NAMES.get(c, c) for c in self.country_codes
            ],
            'resolution': 'weekly',
            'document_type': 'A72 (Reservoir filling)',
        })
        # `collector_quality_issues` injection happens automatically in
        # `BaseCollector.collect()` after this method returns — no
        # per-collector plumbing needed.
        return metadata
