"""
Unit tests for per-zone delivery tracking on the ENTSO-E collectors.

Regression cover for the 2026-08-29 incident: ENTSO-E served sustained
HTTP 503s and NL dropped out of `load_forecast.json`, `generation_mix.json`
and `wind_forecast.json`. The envelope produced that run still claimed
`country_codes: ['NL', 'DE_LU']` for load. Nothing downstream could tell —
the schema-drift tripwire was the only thing that noticed, and it said so by
aborting before the publish step, taking the 13 unchanged feeds down with it.

These pin the three behaviours that fix requires:
  1. metadata publishes the DELIVERED zone set, not the configured one;
  2. a dropped zone raises a `zone_completeness` quality issue that routes
     through the existing DQ gate into the committed quality report;
  3. a zone that publishes no actual load is named, per zone, when actuals
     were requested — `include_actual` itself stays a statement about the
     request and is deliberately left alone.

None of it adds a metadata key: a new envelope key is a published-schema
change, and `details` on the quality issue already carries requested /
delivered / missing. `test_healthy_run_envelope_is_unchanged` pins that a
healthy run publishes exactly what it published before.
"""

from datetime import datetime, timedelta
from functools import partial
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from collectors._entsoe_shared import (
    ZONE_COMPLETENESS_CHECK,
    published_zones,
    record_zone_delivery,
    record_zone_request,
)
from collectors.entsoe_generation import EntsoeGenerationCollector
from collectors.entsoe_hydro import EntsoeHydroCollector
from collectors.entsoe_load import EntsoeLoadCollector
from collectors.entsoe_wind import EntsoeWindCollector

AMS = ZoneInfo("Europe/Amsterdam")
START = datetime(2026, 8, 29, 0, 0, tzinfo=AMS)
END = START + timedelta(days=2)


def _hourly_series(n: int = 48, value: float = 12000.0) -> pd.Series:
    idx = pd.date_range("2026-08-29T00:00:00+02:00", periods=n, freq="h")
    return pd.Series([value] * n, index=idx)


class TestRecordZoneDelivery:
    """The shared helper, exercised directly."""

    def _collector(self):
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        record_zone_request(c)
        return c

    def test_full_delivery_emits_no_issue(self):
        c = self._collector()
        record_zone_delivery(c, {"NL": {}, "DE_LU": {}})
        assert c._collector_quality_issues == []
        assert published_zones(c) == ["NL", "DE_LU"]

    def test_dropped_zone_emits_warning_with_details(self):
        c = self._collector()
        record_zone_delivery(c, {"DE_LU": {}})

        assert published_zones(c) == ["DE_LU"]

        issues = c._collector_quality_issues
        assert len(issues) == 1
        issue = issues[0]
        assert issue["check_name"] == ZONE_COMPLETENESS_CHECK
        assert issue["severity"] == "warning"
        assert "NL" in issue["message"]
        assert issue["details"] == {
            "requested": ["NL", "DE_LU"],
            "delivered": ["DE_LU"],
            "missing": ["NL"],
        }

    def test_delivered_order_follows_configured_order(self):
        """Not insertion order of the results dict — the configured order is
        what metadata consumers diff against."""
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        record_zone_request(c)
        record_zone_delivery(c, {"DE_LU": {}, "NL": {}})
        assert published_zones(c) == ["NL", "DE_LU"]

    def test_repeated_calls_do_not_stack_duplicate_issues(self):
        """`_validate_data` is reachable directly from tests and scripts, and
        `_reset_quality_issues` fires only once per `collect()`. A plain
        append would stack duplicates."""
        c = self._collector()
        record_zone_delivery(c, {"DE_LU": {}})
        record_zone_delivery(c, {"DE_LU": {}})
        assert len(c._collector_quality_issues) == 1

    def test_later_attempt_supersedes_earlier_one(self):
        """The last attempt is the one that produced the published data."""
        c = self._collector()
        record_zone_delivery(c, {"DE_LU": {}})
        record_zone_delivery(c, {"NL": {}, "DE_LU": {}})
        assert c._collector_quality_issues == []
        assert published_zones(c) == ["NL", "DE_LU"]

    def test_unrelated_issues_survive_the_idempotency_filter(self):
        c = self._collector()
        c._add_quality_issue("something_else", "warning", "keep me")
        record_zone_delivery(c, {"DE_LU": {}})
        names = [i["check_name"] for i in c._collector_quality_issues]
        assert names == ["something_else", ZONE_COMPLETENESS_CHECK]

    def test_requested_override_narrows_the_expectation(self):
        """EntsoeWindCollector accepts a single-country override; a run that
        asked for one zone and got it is complete, not degraded."""
        c = EntsoeWindCollector(api_key="k", country_codes=["NL", "DE_LU", "BE"])
        record_zone_request(c, requested=["NL"])
        record_zone_delivery(c, {"NL": {}})
        assert c._collector_quality_issues == []
        assert published_zones(c) == ["NL"]

    def test_helpers_fall_back_to_configured_before_any_fetch(self):
        """`_get_metadata` is reachable without a fetch (direct calls,
        tests) — the configured list is the correct answer there."""
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        assert published_zones(c) == ["NL", "DE_LU"]

    def test_severity_is_overridable(self):
        c = self._collector()
        record_zone_delivery(c, {"DE_LU": {}}, severity="critical")
        assert c._collector_quality_issues[0]["severity"] == "critical"


class TestLoadCollectorZoneDelivery:
    """The `load_forecast.json` half of the 2026-08-29 incident."""

    def _patch_retry(self, collector, *, forecast_zones, actual_zones):
        """Patch `_retry_single` to answer per (query, zone), mimicking a
        partial ENTSO-E outage."""

        async def fake(query_func: partial, *args, **kwargs):
            zone = query_func.keywords["country_code"]
            name = query_func.func.__name__
            if name == "query_load_forecast":
                return _hourly_series() if zone in forecast_zones else None
            return _hourly_series(value=11800.0) if zone in actual_zones else None

        return patch.object(collector, "_retry_single", side_effect=fake)

    @pytest.mark.asyncio
    async def test_metadata_publishes_delivered_zones_not_configured(self):
        """The regression itself: NL 503s, envelope must not claim NL."""
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with self._patch_retry(c, forecast_zones={"DE_LU"}, actual_zones={"DE_LU"}):
            dataset = await c.collect(START, END)

        assert dataset is not None
        assert set(dataset.data.keys()) == {"DE_LU"}
        assert dataset.metadata["country_codes"] == ["DE_LU"]

        issues = dataset.metadata["collector_quality_issues"]
        zone_issues = [i for i in issues if i["check_name"] == ZONE_COMPLETENESS_CHECK]
        assert len(zone_issues) == 1
        assert zone_issues[0]["details"]["missing"] == ["NL"]

    @pytest.mark.asyncio
    async def test_healthy_run_publishes_both_zones_and_no_issue(self):
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with self._patch_retry(
            c, forecast_zones={"NL", "DE_LU"}, actual_zones={"NL", "DE_LU"}
        ):
            dataset = await c.collect(START, END)

        assert dataset.metadata["country_codes"] == ["NL", "DE_LU"]
        assert "collector_quality_issues" not in dataset.metadata

    @pytest.mark.asyncio
    async def test_healthy_run_envelope_is_unchanged(self):
        """A healthy run must publish exactly the keys it published before
        this change. Any NEW envelope key is a published-schema change
        (CURRENT_SCHEMA_VERSION bump + migration + SCHEMA_CHANGELOG entry),
        and it would also flip the shape hash and fail the drift tripwire —
        which is the failure mode this whole change exists to stop causing.
        """
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with self._patch_retry(
            c, forecast_zones={"NL", "DE_LU"}, actual_zones={"NL", "DE_LU"}
        ):
            dataset = await c.collect(START, END)

        assert set(dataset.metadata) == {
            "data_type",
            "source",
            "units",
            "start_time",
            "end_time",
            "collector",
            "country_codes",
            "zones",
            "include_actual",
            "forecast_type",
            "resolution",
            "api_version",
            "description",
            # Stamped by utils.schema_registry.stamp_metadata.
            "schema_version",
            "schema_changelog_entry",
        }

    @pytest.mark.asyncio
    async def test_actuals_outage_is_not_advertised_as_delivered(self):
        """Observed in a live probe on 2026-08-30: the actual-load endpoint 503'd for
        every zone, so `load_actual` and `forecast_error` were absent while
        `include_actual: True` still claimed them."""
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with self._patch_retry(
            c, forecast_zones={"NL", "DE_LU"}, actual_zones=set()
        ):
            dataset = await c.collect(START, END)

        assert dataset.metadata["include_actual"] is True

        issues = dataset.metadata["collector_quality_issues"]
        field_issues = [i for i in issues if i["check_name"] == "field_completeness"]
        assert len(field_issues) == 1
        assert field_issues[0]["details"]["missing_fields"] == [
            "load_actual",
            "forecast_error",
        ]

        # And the payload really is forecast-only, which is what makes the
        # claim false rather than merely imprecise.
        sample = next(iter(dataset.data["NL"].values()))
        assert set(sample.keys()) == {"load_forecast"}

    @pytest.mark.asyncio
    async def test_zone_and_field_outage_report_independently(self):
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with self._patch_retry(c, forecast_zones={"DE_LU"}, actual_zones=set()):
            dataset = await c.collect(START, END)

        names = sorted(
            i["check_name"] for i in dataset.metadata["collector_quality_issues"]
        )
        assert names == ["field_completeness", ZONE_COMPLETENESS_CHECK]


class TestGenerationCollectorZoneDelivery:
    """The `generation_mix.json` half of the 2026-08-29 incident."""

    def _patch_retry(self, collector, *, zones):
        async def fake(query_func: partial, *args, **kwargs):
            zone = query_func.keywords["country_code"]
            if zone not in zones:
                return None
            return pd.DataFrame(
                {"Fossil Gas": [4200.0] * 48, "Nuclear": [480.0] * 48},
                index=pd.date_range(
                    "2026-08-29T00:00:00+02:00", periods=48, freq="h"
                ),
            )

        return patch.object(collector, "_retry_single", side_effect=fake)

    @pytest.mark.asyncio
    async def test_dropped_zone_narrows_metadata_and_raises_issue(self):
        c = EntsoeGenerationCollector(
            api_key="k",
            country_codes=["NL", "DE_LU", "BE"],
            generation_types=["nuclear", "fossil_gas"],
            include_forecast=False,
            include_actual=True,
        )
        with self._patch_retry(c, zones={"DE_LU", "BE"}):
            dataset = await c.collect(START, END)

        assert dataset is not None
        assert dataset.metadata["country_codes"] == ["DE_LU", "BE"]

        zone_issues = [
            i
            for i in dataset.metadata["collector_quality_issues"]
            if i["check_name"] == ZONE_COMPLETENESS_CHECK
        ]
        assert zone_issues[0]["details"]["missing"] == ["NL"]

    @pytest.mark.asyncio
    async def test_zones_lookup_stays_full_width(self):
        """`zones` is a constant name table, not a delivery claim. Narrowing
        a dict-keyed metadata field would add fresh shape churn for the
        drift tripwire — the very thing this change exists to stop causing.
        """
        c = EntsoeGenerationCollector(
            api_key="k",
            country_codes=["NL", "DE_LU", "BE"],
            generation_types=["nuclear"],
            include_forecast=False,
            include_actual=True,
        )
        with self._patch_retry(c, zones={"DE_LU"}):
            dataset = await c.collect(START, END)

        assert set(dataset.metadata["zones"]) == {"NL", "DE_LU", "BE"}


class TestHydroCollectorZoneDelivery:
    """`entsoe_hydro` already had a per-zone signal, but `_validate_data`
    iterates `data.items()` — so it covered a HALF-DARK zone and was silent
    on one that vanished entirely."""

    @pytest.mark.asyncio
    async def test_vanished_zone_now_reported(self):
        c = EntsoeHydroCollector(api_key="k", country_codes=["NO", "SE"])

        async def fake(query_func, *args, **kwargs):
            if query_func.keywords["country_code"] != "NO":
                return None
            idx = pd.DatetimeIndex(
                [pd.Timestamp("2026-01-05T00:00:00+00:00") + pd.Timedelta(weeks=w)
                 for w in range(8)]
            )
            return pd.Series([8.0e7] * 8, index=idx)

        with patch.object(c, "_retry_single", side_effect=fake):
            dataset = await c.collect(
                datetime(2026, 1, 1, tzinfo=AMS),
                datetime(2026, 3, 1, tzinfo=AMS),
            )

        assert dataset is not None
        assert dataset.metadata["country_codes"] == ["NO"]
        assert dataset.metadata["country_names"] == ["Norway"]

        zone_issues = [
            i
            for i in dataset.metadata["collector_quality_issues"]
            if i["check_name"] == ZONE_COMPLETENESS_CHECK
        ]
        assert len(zone_issues) == 1
        assert zone_issues[0]["details"]["missing"] == ["SE"]


class TestParseStageZoneLoss:
    """Review blocker R1: delivery must be measured against the PARSED data.

    An earlier draft recorded delivery at the end of `_fetch_raw_data`. But all
    four `_parse_response` implementations end `if country_data: parsed[...]`,
    so a zone can fetch successfully and still vanish from `data`. That draft
    recorded it as delivered — reproducing the exact 2026-08-29 signature the
    module exists to eliminate, with a green run and no quality issue.
    """

    @pytest.mark.asyncio
    async def test_wind_zone_with_no_wind_columns_is_not_counted_delivered(self):
        """`query_wind_and_solar_forecast(psr_type=None)` can return a
        Solar-only frame for a zone. The wind matcher finds no column, every
        row parses empty, and the zone drops out of `data`."""
        idx = pd.date_range("2026-08-29T00:00:00+02:00", periods=48, freq="h")

        async def fake(query_func: partial, *args, **kwargs):
            zone = query_func.keywords["country_code"]
            if zone == "NL":
                return pd.DataFrame({"Solar": [100.0] * 48}, index=idx)
            return pd.DataFrame(
                {"Wind Onshore": [700.0] * 48, "Wind Offshore": [800.0] * 48},
                index=idx,
            )

        c = EntsoeWindCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with patch.object(c, "_retry_single", side_effect=fake):
            dataset = await c.collect(START, END)

        assert "NL" not in dataset.data
        assert dataset.metadata["country_codes"] == ["DE_LU"]
        zone_issues = [
            i for i in dataset.metadata["collector_quality_issues"]
            if i["check_name"] == ZONE_COMPLETENESS_CHECK
        ]
        assert len(zone_issues) == 1
        assert zone_issues[0]["details"]["missing"] == ["NL"]

    @pytest.mark.asyncio
    async def test_load_zone_with_all_nan_series_is_not_counted_delivered(self):
        """A non-empty but all-NaN publication clears the `.empty` check in
        `_fetch_raw_data` and then parses to nothing (`pd.notna` guards)."""
        idx = pd.date_range("2026-08-29T00:00:00+02:00", periods=48, freq="h")

        async def fake(query_func: partial, *args, **kwargs):
            zone = query_func.keywords["country_code"]
            if zone == "NL":
                return pd.Series([float("nan")] * 48, index=idx)
            return pd.Series([12000.0] * 48, index=idx)

        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with patch.object(c, "_retry_single", side_effect=fake):
            dataset = await c.collect(START, END)

        assert "NL" not in dataset.data
        assert dataset.metadata["country_codes"] == ["DE_LU"]
        zone_issues = [
            i for i in dataset.metadata["collector_quality_issues"]
            if i["check_name"] == ZONE_COMPLETENESS_CHECK
        ]
        assert zone_issues[0]["details"]["missing"] == ["NL"]


class TestPerZoneActualCompleteness:
    """Review blocker R3: the earlier draft used `any()` across the whole feed,
    so it reported nothing when only SOME zones lost their actuals — while the
    shape hash still flipped and still blocked the publish. Signal-free
    failure, and the more probable outage of the two."""

    def _patch_retry(self, collector, *, actual_zones):
        async def fake(query_func: partial, *args, **kwargs):
            zone = query_func.keywords["country_code"]
            if query_func.func.__name__ == "query_load_forecast":
                return _hourly_series()
            return _hourly_series(value=11800.0) if zone in actual_zones else None

        return patch.object(collector, "_retry_single", side_effect=fake)

    @pytest.mark.asyncio
    async def test_single_zone_losing_actuals_is_reported(self):
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with self._patch_retry(c, actual_zones={"NL"}):
            dataset = await c.collect(START, END)

        # Both zones publish, so this is NOT a zone dropout.
        assert set(dataset.data) == {"NL", "DE_LU"}
        assert dataset.metadata["country_codes"] == ["NL", "DE_LU"]

        field_issues = [
            i for i in dataset.metadata["collector_quality_issues"]
            if i["check_name"] == "field_completeness"
        ]
        assert len(field_issues) == 1
        assert field_issues[0]["details"]["zones_without_actual"] == ["DE_LU"]

        # And the payload really is asymmetric — which is what makes the
        # per-zone report the accurate one.
        assert "load_actual" in next(iter(dataset.data["NL"].values()))
        assert set(next(iter(dataset.data["DE_LU"].values()))) == {"load_forecast"}

    @pytest.mark.asyncio
    async def test_all_zones_losing_actuals_names_all_of_them(self):
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with self._patch_retry(c, actual_zones=set()):
            dataset = await c.collect(START, END)

        field_issues = [
            i for i in dataset.metadata["collector_quality_issues"]
            if i["check_name"] == "field_completeness"
        ]
        assert field_issues[0]["details"]["zones_without_actual"] == ["NL", "DE_LU"]

    @pytest.mark.asyncio
    async def test_no_field_issue_when_actuals_not_requested(self):
        c = EntsoeLoadCollector(
            api_key="k", country_codes=["NL", "DE_LU"], include_actual=False
        )
        with self._patch_retry(c, actual_zones=set()):
            dataset = await c.collect(START, END)

        assert "collector_quality_issues" not in dataset.metadata


class TestZoneRequestResetsStaleDelivery:
    """`_delivered_zones` outlives the run that produced it, and data_fetcher
    reuses collector instances across runs.

    An earlier version of this docstring claimed the reset prevents an early
    return from publishing a stale zone list. Review traced every path in
    `collect()` and refuted it: circuit-breaker-open, UpstreamNoDataError and
    the generic except all `return None` WITHOUT calling `_get_metadata`, so a
    stale list can never reach an envelope. The reset is defence in depth, not
    a live fix. What these tests actually pin is that consecutive runs on one
    instance each publish their own delivered set."""

    @pytest.mark.asyncio
    async def test_second_run_does_not_inherit_first_runs_zones(self):
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])

        async def both(query_func: partial, *args, **kwargs):
            return _hourly_series()

        with patch.object(c, "_retry_single", side_effect=both):
            await c.collect(START, END)
        assert published_zones(c) == ["NL", "DE_LU"]

        async def de_only(query_func: partial, *args, **kwargs):
            if query_func.keywords["country_code"] == "NL":
                return None
            return _hourly_series()

        with patch.object(c, "_retry_single", side_effect=de_only):
            dataset = await c.collect(START, END)
        assert dataset.metadata["country_codes"] == ["DE_LU"]

    def test_request_clears_prior_delivery(self):
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        record_zone_request(c)
        record_zone_delivery(c, {"DE_LU": {}})
        assert published_zones(c) == ["DE_LU"]
        record_zone_request(c)
        assert published_zones(c) == ["NL", "DE_LU"]


class TestAllZonesLostAtParseStage:
    """The single-zone variant is covered by TestParseStageZoneLoss; this is
    the total-loss case, which review found untested and which contradicts an
    invariant the module docstring used to assert."""

    @pytest.mark.asyncio
    async def test_every_zone_lost_yields_empty_country_codes(self):
        """`collect()` never reads `_validate_data`'s `is_valid`, and load's
        `_parse_response` returns {} without raising, so this state is
        reachable. `country_codes: []` has a DIFFERENT shape signature from a
        non-empty list — pinned here so the 'list of str either way' reasoning
        is not read as unconditional."""
        idx = pd.date_range("2026-08-29T00:00:00+02:00", periods=48, freq="h")

        async def all_nan(query_func: partial, *args, **kwargs):
            return pd.Series([float("nan")] * 48, index=idx)

        c = EntsoeLoadCollector(api_key="k", country_codes=["NL", "DE_LU"])
        with patch.object(c, "_retry_single", side_effect=all_nan):
            dataset = await c.collect(START, END)

        assert dataset.data == {}
        assert dataset.metadata["country_codes"] == []

        zone_issues = [
            i for i in dataset.metadata["collector_quality_issues"]
            if i["check_name"] == ZONE_COMPLETENESS_CHECK
        ]
        assert zone_issues[0]["details"]["missing"] == ["NL", "DE_LU"]

        # And no field_completeness noise: zone_completeness already reports
        # the total loss in full.
        assert not [
            i for i in dataset.metadata["collector_quality_issues"]
            if i["check_name"] == "field_completeness"
        ]


class TestUnexpectedZoneIsPublished:
    """Review finding: `published_zones` filtered `data` down to the requested
    set, so a zone entsoe-py leaked would be present in `data` but absent from
    `country_codes` — breaking the module's own consumer contract, which says
    `country_codes` is authoritative. entsoe_hydro's #31 comment and
    test_entsoe_hydro_collector.py both anticipate exactly that leak."""

    def test_leaked_zone_appears_in_country_codes(self):
        c = EntsoeLoadCollector(api_key="k", country_codes=["NL"])
        record_zone_request(c)
        record_zone_delivery(c, {"NL": {}, "FI": {}})
        assert published_zones(c) == ["NL", "FI"]
        assert c._collector_quality_issues == []
