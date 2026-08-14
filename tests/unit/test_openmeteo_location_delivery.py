"""
Per-location dropout must be visible downstream (2026-08-14 follow-up).

Before this, a location whose fetch exhausted its retries was simply absent
from the results dict, while `_get_metadata` still built `locations` and
`location_count` from the CONFIGURED list. A degraded feed therefore published
an envelope asserting a location that `data` did not contain, no quality issue
was raised, and `validate_completeness` passed comfortably — so nothing but the
schema-drift tripwire noticed, and it noticed by failing the whole publish.

File: tests/unit/test_openmeteo_location_delivery.py
Created: 2026-08-14
"""

import pytest

from collectors._openmeteo_shared import (
    record_location_delivery,
    published_locations,
)
from collectors.openmeteo_weather import OpenMeteoWeatherCollector
from collectors.openmeteo_solar import OpenMeteoSolarCollector

LOCS = [
    {"name": "Elsweide_Arnhem_NL", "lat": 51.98955, "lon": 5.95470},
    {"name": "Elderveld_Arnhem_NL", "lat": 51.96069, "lon": 5.86010},
]


@pytest.fixture(params=[OpenMeteoWeatherCollector, OpenMeteoSolarCollector])
def collector(request):
    """Both collectors share the helper; both must behave identically."""
    return request.param(locations=list(LOCS), forecast_days=16)


class TestRecordLocationDelivery:
    def test_full_delivery_raises_no_issue(self, collector):
        record_location_delivery(
            collector, {"Elsweide_Arnhem_NL": {}, "Elderveld_Arnhem_NL": {}}
        )
        assert collector._collector_quality_issues == []
        assert published_locations(collector) == [
            "Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"
        ]

    def test_dropout_raises_warning_naming_the_location(self, collector):
        record_location_delivery(collector, {"Elderveld_Arnhem_NL": {}})
        issues = collector._collector_quality_issues
        assert len(issues) == 1
        issue = issues[0]
        assert issue["check_name"] == "location_completeness"
        assert issue["severity"] == "warning"
        assert "Elsweide_Arnhem_NL" in issue["message"]
        assert issue["details"]["missing"] == ["Elsweide_Arnhem_NL"]
        assert issue["details"]["delivered"] == ["Elderveld_Arnhem_NL"]
        assert issue["details"]["requested"] == [
            "Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"
        ]

    def test_metadata_reports_delivered_not_configured(self, collector):
        """The core defect: the published envelope must not claim a location
        that `data` does not carry."""
        record_location_delivery(collector, {"Elderveld_Arnhem_NL": {}})
        assert published_locations(collector) == ["Elderveld_Arnhem_NL"]

    def test_configured_order_is_preserved(self, collector):
        """Delivery arrives out of order (asyncio.gather); the published list
        must still be deterministic, or the metadata churns run to run."""
        record_location_delivery(
            collector,
            {"Elderveld_Arnhem_NL": {}, "Elsweide_Arnhem_NL": {}},
        )
        assert published_locations(collector) == [
            "Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"
        ]

    def test_falls_back_to_configured_before_any_fetch(self, collector):
        """`_get_metadata` is reachable without a fetch (direct calls, tests);
        the configured list is the right answer there."""
        assert published_locations(collector) == [
            "Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"
        ]

    def test_total_dropout_reports_every_location(self, collector):
        record_location_delivery(collector, {})
        assert published_locations(collector) == []
        issue = collector._collector_quality_issues[0]
        assert issue["details"]["missing"] == [
            "Elsweide_Arnhem_NL", "Elderveld_Arnhem_NL"
        ]


class TestMetadataIntegration:
    def test_get_metadata_publishes_delivered_set(self, collector):
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        start = datetime.now(ZoneInfo("Europe/Amsterdam"))
        record_location_delivery(collector, {"Elderveld_Arnhem_NL": {}})
        meta = collector._get_metadata(start, start + timedelta(days=16))
        assert meta["locations"] == ["Elderveld_Arnhem_NL"]
        assert meta["location_count"] == 1
