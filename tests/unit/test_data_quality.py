"""
Tests for Data Quality Validation Framework
---------------------------------------------
Validates all FMEA-based quality checks work correctly.

Test scenarios cover:
- Value range validation (in-range, out-of-range, nested data)
- Completeness checks (full, partial, empty)
- Null ratio detection
- Staleness detection
- Duplicate timestamp detection
- Per-dataset validation
- Pipeline-level validation with missing datasets
- Severity escalation logic
"""

import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from utils.data_quality import (
    Severity,
    QualityIssue,
    DatasetQualityReport,
    PipelineQualityReport,
    validate_value_ranges,
    validate_completeness,
    validate_null_ratio,
    validate_staleness,
    validate_duplicate_timestamps,
    validate_dataset,
    validate_pipeline,
    get_dataset_validation_config,
    EXPECTED_DATA_TYPE,
)
from utils.data_types import EnhancedDataSet

AMS = ZoneInfo('Europe/Amsterdam')


def _make_price_data(n_hours=24, base_price=50.0, start=None):
    """Generate n_hours of realistic price data."""
    if start is None:
        start = datetime.now(AMS).replace(hour=0, minute=0, second=0, microsecond=0)
    data = {}
    for h in range(n_hours):
        ts = (start + timedelta(hours=h)).isoformat()
        data[ts] = base_price + (h % 12) * 5  # Simple pattern
    return data


def _make_dataset(data, data_type='energy_price', source='test'):
    """Helper to create an EnhancedDataSet."""
    return EnhancedDataSet(
        metadata={'data_type': data_type, 'source': source, 'units': 'EUR/MWh'},
        data=data,
    )


class TestValueRanges:
    """Tests for validate_value_ranges."""

    def test_all_values_in_range(self):
        """Normal prices should pass without issues."""
        data = _make_price_data(24, base_price=50.0)
        issues = validate_value_ranges(data, 'energy_price', 'test')
        assert len(issues) == 0

    def test_negative_prices_within_range(self):
        """Slightly negative prices are valid (oversupply)."""
        data = {'2026-01-15T00:00:00+01:00': -10.0}
        issues = validate_value_ranges(data, 'energy_price', 'test')
        assert len(issues) == 0

    def test_extreme_negative_price(self):
        """Price below -500 EUR/MWh should trigger warning."""
        data = {'2026-01-15T00:00:00+01:00': -600.0}
        issues = validate_value_ranges(data, 'energy_price', 'test')
        assert len(issues) == 1
        assert issues[0].severity in (Severity.WARNING, Severity.ERROR)

    def test_extreme_high_price(self):
        """Price above 10,000 EUR/MWh should trigger warning."""
        data = {'2026-01-15T00:00:00+01:00': 15000.0}
        issues = validate_value_ranges(data, 'energy_price', 'test')
        assert len(issues) == 1

    def test_many_out_of_range_escalates_to_error(self):
        """More than 3 out-of-range values should be ERROR severity."""
        data = {f'2026-01-15T{h:02d}:00:00+01:00': 99999.0 for h in range(5)}
        issues = validate_value_ranges(data, 'energy_price', 'test')
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR

    def test_negative_wind_speed(self):
        """Negative wind speed is physically impossible."""
        data = {'2026-01-15T00:00:00+01:00': -5.0}
        issues = validate_value_ranges(data, 'wind_speed', 'test')
        assert len(issues) == 1

    def test_nested_data_validation(self):
        """Nested dict data (multi-location) should be checked."""
        data = {
            '2026-01-15T00:00:00+01:00': {'value': 15.0},
            '2026-01-15T01:00:00+01:00': {'value': 999.0},  # out of range
        }
        issues = validate_value_ranges(data, 'temperature', 'test')
        assert len(issues) == 1
        assert '1 values outside range' in issues[0].message

    def test_none_values_ignored(self):
        """None values should be skipped, not flagged as out-of-range."""
        data = {'2026-01-15T00:00:00+01:00': None, '2026-01-15T01:00:00+01:00': 50.0}
        issues = validate_value_ranges(data, 'energy_price', 'test')
        assert len(issues) == 0

    def test_unknown_data_type_no_check(self):
        """Unknown data types should return no issues (no range defined)."""
        data = {'2026-01-15T00:00:00+01:00': 999999.0}
        issues = validate_value_ranges(data, 'unknown_type', 'test')
        assert len(issues) == 0

    def test_issue_includes_examples(self):
        """Out-of-range issues should include example values."""
        data = {'2026-01-15T00:00:00+01:00': 50000.0}
        issues = validate_value_ranges(data, 'energy_price', 'test')
        assert issues[0].details is not None
        assert len(issues[0].details['examples']) > 0


class TestWeatherFieldRanges:
    """Tests for per-field weather validation (the main backtest fix)."""

    def test_weather_humidity_not_flagged_as_temperature(self):
        """Humidity (0-100%) should NOT be flagged when checking weather data."""
        data = {
            '2026-01-15T00:00:00+01:00': {
                'main_temp': 17.0,
                'main_humidity': 62,
                'main_pressure': 1021,
                'wind_speed': 5.0,
            },
        }
        issues = validate_value_ranges(data, 'weather', 'test')
        assert len(issues) == 0

    def test_weather_pressure_not_flagged(self):
        """Pressure (1021 hPa) should NOT be flagged as out-of-range temperature."""
        data = {
            '2026-01-15T00:00:00+01:00': {
                'main_pressure': 1021,
                'main_sea_level': 1021,
            },
        }
        issues = validate_value_ranges(data, 'weather', 'test')
        assert len(issues) == 0

    def test_weather_visibility_not_flagged(self):
        """Visibility (10000m) should NOT be flagged."""
        data = {
            '2026-01-15T00:00:00+01:00': {
                'visibility': 10000,
                'vis': 50000,
            },
        }
        issues = validate_value_ranges(data, 'weather', 'test')
        assert len(issues) == 0

    def test_weather_actual_bad_temperature(self):
        """A genuinely impossible temperature SHOULD be flagged."""
        data = {
            '2026-01-15T00:00:00+01:00': {
                'main_temp': 999.0,  # Impossible
                'main_humidity': 50,
            },
        }
        issues = validate_value_ranges(data, 'weather', 'test')
        assert len(issues) == 1
        assert 'main_temp=999.0' in issues[0].details['examples'][0]

    def test_weather_bad_pressure(self):
        """Impossible pressure (e.g., 0) should be flagged."""
        data = {
            '2026-01-15T00:00:00+01:00': {
                'main_pressure': 0.0,  # Impossible on Earth
            },
        }
        issues = validate_value_ranges(data, 'weather', 'test')
        assert len(issues) == 1

    def test_weather_unknown_fields_ignored(self):
        """Fields not in WEATHER_FIELD_RANGES should be silently skipped."""
        data = {
            '2026-01-15T00:00:00+01:00': {
                'weather_main': 'Clouds',      # String, not numeric
                'weather_id': 804,              # Unknown field
                'cond': 3,                      # Unknown field
                'ico': 3,                       # Unknown field
            },
        }
        issues = validate_value_ranges(data, 'weather', 'test')
        assert len(issues) == 0

    def test_full_openweather_sample(self):
        """A real OpenWeather sample should pass all field checks."""
        data = {
            '2026-01-15T00:00:00+01:00': {
                'main_temp': 17.93,
                'main_feels_like': 17.4,
                'main_temp_min': 16.11,
                'main_temp_max': 17.93,
                'main_pressure': 1021,
                'main_sea_level': 1021,
                'main_grnd_level': 1018,
                'main_humidity': 62,
                'main_temp_kf': 1.82,
                'clouds_all': 87,
                'wind_speed': 1.21,
                'wind_deg': 85,
                'wind_gust': 1.26,
                'visibility': 10000,
            },
        }
        issues = validate_value_ranges(data, 'weather', 'test')
        assert len(issues) == 0

    def test_full_meteoserver_sample(self):
        """A real MeteoServer sample should pass all field checks."""
        data = {
            '2026-01-15T00:00:00+01:00': {
                'temp': 17,
                'winds': 1,
                'windb': 1,
                'windknp': 2,
                'windkmh': 3.6,
                'windr': 53,
                'vis': 50000,
                'neersl': 0,
                'luchtd': 1021.9,
                'luchtdmmhg': 766.5,
                'luchtdinhg': 30.18,
                'hw': 52,
                'mw': 0,
                'lw': 0,
                'tw': 52,
                'rv': 66,
                'gr': 0,
                'gr_w': 0,
                'snd': 0,
                'snv': 0,
            },
        }
        issues = validate_value_ranges(data, 'weather', 'test')
        assert len(issues) == 0


class TestDatasetValidationConfig:
    """Reviewer tier-2 finding #8: single lookup point for per-dataset config."""

    def test_gas_storage_returns_both_field_ranges_and_staleness(self):
        cfg = get_dataset_validation_config('gas_storage', 'gas_storage')
        assert cfg['field_ranges'] is not None
        assert 'fill_level_pct' in cfg['field_ranges']
        assert cfg['staleness_hours'] == 96

    def test_weather_returns_field_ranges_but_default_staleness(self):
        cfg = get_dataset_validation_config('weather_forecast_buurt', 'weather')
        assert cfg['field_ranges'] is not None
        assert 'main_temp' in cfg['field_ranges']
        assert cfg['staleness_hours'] == 48  # default

    def test_unknown_dataset_returns_safe_defaults(self):
        cfg = get_dataset_validation_config('unknown', 'unknown')
        assert cfg['field_ranges'] is None
        assert cfg['staleness_hours'] == 48


class TestExpectedDataTypeDefense:
    """Review finding #6 (MEDIUM, CWE-20): MITM-injected metadata.data_type
    cannot steer validation to the wrong field-range registry. When the
    pipeline knows the expected data_type for a dataset_name, that wins.
    """

    def test_mismatched_data_type_uses_expected_for_routing(self, caplog):
        """gas_storage dataset with metadata.data_type='weather' (injected)
        must still route through gas_storage validation rules."""
        # Realistic gas_storage payload but with weather data_type spoofed
        # in the envelope metadata. If the defense fails, validate_value_ranges
        # would use WEATHER_FIELD_RANGES and skip the gas_storage range checks.
        dataset = EnhancedDataSet(
            metadata={
                'data_type': 'weather',  # MITM-spoofed; real should be gas_storage
                'source': 'GIE AGSI+',
                'units': 'mixed',
            },
            data={
                (datetime.now(AMS) - timedelta(hours=12)).isoformat(): {
                    'fill_level_pct': 15.24,
                    'gas_in_storage_twh': 21.9185,
                    'injection_gwh': 385.08,
                },
            },
        )

        import logging
        with caplog.at_level(logging.WARNING, logger='utils.data_quality'):
            report = validate_dataset(dataset, 'gas_storage')

        # The report's data_type field reflects the expected value (defended).
        assert report.data_type == 'gas_storage'
        # A warning was logged about the mismatch.
        assert any(
            'gas_storage' in rec.message and 'weather' in rec.message
            for rec in caplog.records
        )

    def test_expected_data_type_registry_covers_named_feeds(self):
        """Sanity check: the headline datasets the pipeline publishes have
        their expected data_type pinned, so a future MITM cannot silently
        steer validation for the most important feeds."""
        # Feeds the ML downstream most relies on:
        assert 'gas_storage' in EXPECTED_DATA_TYPE
        assert 'grid_imbalance' in EXPECTED_DATA_TYPE
        assert 'load_forecast' in EXPECTED_DATA_TYPE
        assert 'generation_forecast' in EXPECTED_DATA_TYPE

    def test_price_feeds_pinned_post_review(self):
        """Reviewer security-auditor MEDIUM finding (CWE-20): the five
        per-source energy-price feeds were the highest-value MITM targets
        (an attacker could spoof data_type='weather' to route EUR/MWh
        values through temperature/pressure bounds and silently pass
        nonsense). All five must now be pinned to 'energy_price'."""
        for feed in ('entsoe', 'entsoe_de', 'energy_zero', 'epex', 'elspot'):
            assert feed in EXPECTED_DATA_TYPE, f"Price feed {feed!r} not pinned"
            assert EXPECTED_DATA_TYPE[feed] == 'energy_price'

    def test_published_feeds_all_pinned(self):
        """Every dataset name that appears in data_fetcher.py's
        `quality_datasets` block should have a pin. Drift between the
        two registries means a newly-added feed would silently lack the
        defense — this guards against that.
        """
        # Mirrors the quality_datasets dict in data_fetcher.py:946-970.
        # Update both together when a new feed is added.
        published_feeds = {
            'entsoe', 'entsoe_de', 'energy_zero', 'epex', 'elspot',
            'weather_forecast_multi_location', 'weather_forecast_buurt',
            'solar_forecast_buurt', 'air_quality_buurt', 'grid_imbalance',
            'wind_forecast', 'solar_forecast', 'demand_weather_forecast',
            'offshore_wind', 'cross_border_flows', 'load_forecast',
            'generation_forecast', 'generation_mix', 'ned_production',
            'market_proxies', 'market_history', 'gas_storage', 'gas_flows',
        }
        missing = published_feeds - set(EXPECTED_DATA_TYPE.keys())
        assert missing == set(), (
            f"These published feeds lack an EXPECTED_DATA_TYPE pin and are "
            f"vulnerable to data_type-spoof MITM (CWE-20): {sorted(missing)}"
        )


class TestGasStorageFieldRanges:
    """Issue #24: gas_storage's nested {fill_level_pct, *_twh, *_gwh} dict
    must validate per-field, not against a single 0-100% range that flags
    every TWh value daily.
    """

    def _sample(self, **overrides):
        # Realistic NL gas storage payload (post field-rename hotfix).
        base = {
            'fill_level_pct': 15.24,
            'gas_in_storage_twh': 21.9185,    # variable (6-56 TWh observed range)
            'injection_gwh': 385.08,
            'withdrawal_gwh': 7.9,
            'net_change_gwh': 377.18,
            'working_gas_volume_twh': 143.7945,  # fixed infrastructure capacity
        }
        base.update(overrides)
        return base

    def test_realistic_sample_passes(self):
        """The exact payload shape from issue #24 must produce zero issues."""
        data = {'2026-05-29T00:00:00+02:00': self._sample()}
        issues = validate_value_ranges(data, 'gas_storage', 'gas_storage')
        assert issues == []

    def test_impossible_fill_level_flagged(self):
        """fill_level_pct > 100 is physically impossible and must flag."""
        data = {'2026-05-29T00:00:00+02:00': self._sample(fill_level_pct=150.0)}
        issues = validate_value_ranges(data, 'gas_storage', 'gas_storage')
        assert len(issues) == 1
        assert 'fill_level_pct=150.0' in issues[0].details['examples'][0]

    def test_negative_stored_gas_flagged(self):
        """Negative TWh values are impossible — must flag."""
        data = {'2026-05-29T00:00:00+02:00': self._sample(gas_in_storage_twh=-5.0)}
        issues = validate_value_ranges(data, 'gas_storage', 'gas_storage')
        assert len(issues) == 1

    def test_net_change_allows_negative(self):
        """net_change_gwh is signed (withdrawal > injection → negative)."""
        data = {'2026-05-29T00:00:00+02:00': self._sample(net_change_gwh=-1500.0)}
        issues = validate_value_ranges(data, 'gas_storage', 'gas_storage')
        assert issues == []

    def test_validate_dataset_staleness_override(self):
        """gas_storage gets 96h staleness threshold (GIE AGSI+ publishes
        with 2-3 day lag); 60h old data must NOT trigger staleness."""
        sixty_h_old = datetime.now(AMS) - timedelta(hours=60)
        dataset = EnhancedDataSet(
            metadata={
                'data_type': 'gas_storage',
                'source': 'GIE AGSI+',
                'units': 'mixed',
            },
            data={sixty_h_old.isoformat(): self._sample()},
        )
        report = validate_dataset(dataset, 'gas_storage')
        staleness = [i for i in report.issues if i.check_name == 'staleness']
        assert staleness == []

    def test_validate_dataset_staleness_still_fires_past_96h(self):
        """Beyond the 96h override, gas_storage staleness must still flag."""
        five_days_old = datetime.now(AMS) - timedelta(hours=120)
        dataset = EnhancedDataSet(
            metadata={
                'data_type': 'gas_storage',
                'source': 'GIE AGSI+',
                'units': 'mixed',
            },
            data={five_days_old.isoformat(): self._sample()},
        )
        report = validate_dataset(dataset, 'gas_storage')
        staleness = [i for i in report.issues if i.check_name == 'staleness']
        assert len(staleness) == 1
        assert staleness[0].details['threshold_hours'] == 96

    def test_validate_dataset_clean_run_status(self):
        """Realistic gas_storage data → status info (no value_range error,
        no staleness error). This is the regression for the daily noise."""
        fresh = datetime.now(AMS) - timedelta(hours=24)
        dataset = EnhancedDataSet(
            metadata={
                'data_type': 'gas_storage',
                'source': 'GIE AGSI+',
                'units': 'mixed',
            },
            data={fresh.isoformat(): self._sample()},
        )
        report = validate_dataset(dataset, 'gas_storage')
        # No range or staleness issues; status should not be error/critical
        assert report.status not in ('error', 'critical')


class TestDataTypeRangeMapInvariants:
    """Refactoring-guide H2 — DATA_TYPE_RANGE_MAP entries that route to
    FIELD_RANGES_BY_TYPE must actually exist there; otherwise
    `validate_value_ranges` silently no-ops. The module-load assert in
    utils/data_quality.py catches missing entries at import; these tests
    pin the behavior so a deletion can't slip in unnoticed."""

    def test_every_self_mapped_data_type_has_coverage(self):
        """Every key→key entry in DATA_TYPE_RANGE_MAP must resolve to
        either FIELD_RANGES_BY_TYPE or VALUE_RANGES coverage."""
        from utils.data_quality import (
            DATA_TYPE_RANGE_MAP, FIELD_RANGES_BY_TYPE, VALUE_RANGES,
        )
        self_mapped = {k for k, v in DATA_TYPE_RANGE_MAP.items() if k == v}
        coverage = set(FIELD_RANGES_BY_TYPE.keys()) | set(VALUE_RANGES.keys())
        missing = self_mapped - coverage
        assert not missing, f"Uncovered self-mapped data_types: {missing}"

    def test_solar_irradiance_alias_routes_to_solar(self):
        """opus M2: alias defends a non-EXPECTED_DATA_TYPE-pinned dataset
        whose collector emits the canonical 'solar_irradiance' name."""
        from utils.data_quality import DATA_TYPE_RANGE_MAP, FIELD_RANGES_BY_TYPE
        assert DATA_TYPE_RANGE_MAP['solar_irradiance'] == 'solar'
        assert 'solar' in FIELD_RANGES_BY_TYPE

    def test_load_forecast_alias_routes_to_load(self):
        """opus M2: same protection for collector-emitted 'load_forecast'."""
        from utils.data_quality import DATA_TYPE_RANGE_MAP, FIELD_RANGES_BY_TYPE
        assert DATA_TYPE_RANGE_MAP['load_forecast'] == 'load'
        assert 'load' in FIELD_RANGES_BY_TYPE

    def test_validate_value_ranges_via_solar_irradiance_alias(self):
        """End-to-end: a solar value passed via 'solar_irradiance' alias
        routes through the per-field SOLAR_FIELD_RANGES rather than the
        old blanket VALUE_RANGES['solar_irradiance'] = [0,1400] (which
        would re-introduce the #28 dawn/dusk false-positive)."""
        from utils.data_quality import DATA_TYPE_RANGE_MAP, validate_value_ranges
        range_type = DATA_TYPE_RANGE_MAP['solar_irradiance']
        # dhi=-200 must pass through SOLAR_FIELD_RANGES (floor -300)
        data = {'2026-05-29T05:00:00+02:00': {
            'ghi': 0.0, 'dni': 0.0, 'dhi': -200.0,
            'direct': 0.0, 'cloud_cover': 100.0,
        }}
        issues = validate_value_ranges(data, range_type, 'solar_forecast')
        assert issues == []


class TestSolarFieldRanges:
    """Issue #28: OpenMeteo solar feeds emit (ghi, dni, dhi, direct,
    cloud_cover) — the prior blanket [0, 1400] route via `solar_irradiance`
    fired on every dawn/dusk numerical-noise negative (observed DHI down
    to -266 W/m²).
    """

    def _sample(self, **overrides):
        # Realistic Open-Meteo solar payload, mid-day clear-sky conditions.
        base = {
            'ghi': 450.0,
            'dni': 700.0,
            'dhi': 100.0,
            'direct': 380.0,
            'cloud_cover': 35.0,
        }
        base.update(overrides)
        return base

    def test_realistic_sample_passes(self):
        """Mid-day clear-sky values must produce zero issues."""
        data = {'2026-05-29T12:00:00+02:00': self._sample()}
        issues = validate_value_ranges(data, 'solar', 'solar_forecast')
        assert issues == []

    def test_dawn_dusk_negative_noise_passes(self):
        """The whole point of #28: small negative dawn/dusk values are
        Open-Meteo numerical noise, not data errors. Must not flag."""
        data = {
            '2026-05-29T05:00:00+02:00': self._sample(
                ghi=-0.5, direct=-0.2, dhi=-12.0,
            ),
        }
        issues = validate_value_ranges(data, 'solar', 'solar_forecast')
        assert issues == []

    def test_observed_dhi_extreme_passes(self):
        """DHI floor (-300) chosen to absorb the worst observed Open-Meteo
        dawn artifact (-266 W/m²). Anything within observed range must pass."""
        data = {'2026-05-29T06:00:00+02:00': self._sample(dhi=-260.0)}
        issues = validate_value_ranges(data, 'solar', 'solar_forecast')
        assert issues == []

    def test_poison_dhi_flagged(self):
        """A massive DHI value (10x physical max) is a poison signal that
        the bound is still tight enough to catch real attacks/regressions."""
        data = {'2026-05-29T12:00:00+02:00': self._sample(dhi=9999.0)}
        issues = validate_value_ranges(data, 'solar', 'solar_forecast')
        assert len(issues) == 1
        assert 'dhi' in issues[0].details['examples'][0]

    def test_poison_ghi_flagged(self):
        """GHI ceiling (1400 W/m²) — beyond the solar constant is impossible."""
        data = {'2026-05-29T12:00:00+02:00': self._sample(ghi=2500.0)}
        issues = validate_value_ranges(data, 'solar', 'solar_forecast')
        assert len(issues) == 1

    def test_negative_cloud_cover_flagged(self):
        """cloud_cover is a percentage — negative is impossible."""
        data = {'2026-05-29T12:00:00+02:00': self._sample(cloud_cover=-5.0)}
        issues = validate_value_ranges(data, 'solar', 'solar_forecast')
        assert len(issues) == 1

    def test_two_level_zone_nested_payload(self):
        """Solar feeds publish as {location: {ts: {fields}}} — verify the
        flatten helper routes per-field validation through correctly."""
        data = {
            'Rotterdam_NL': {
                '2026-05-29T12:00:00+02:00': self._sample(),
            },
            'Berlin_DE': {
                '2026-05-29T12:00:00+02:00': self._sample(ghi=2500.0),  # poison
            },
        }
        issues = validate_value_ranges(data, 'solar', 'solar_forecast')
        assert len(issues) == 1  # Berlin's poison ghi only


class TestLoadFieldRanges:
    """Issue #28: ENTSO-E load feeds emit (load_forecast, load_actual,
    forecast_error) — forecast_error is signed (negative when we
    over-predicted demand) and the prior blanket [0, 100000] flagged it.
    """

    def _sample(self, **overrides):
        # Realistic NL load values, evening peak.
        base = {
            'load_forecast': 14500.0,
            'load_actual': 14200.0,
            'forecast_error': 300.0,
        }
        base.update(overrides)
        return base

    def test_realistic_sample_passes(self):
        data = {'2026-05-29T19:00:00+02:00': self._sample()}
        issues = validate_value_ranges(data, 'load', 'load_forecast')
        assert issues == []

    def test_negative_forecast_error_passes(self):
        """The whole point of #28 for load: forecast_error legitimately
        goes negative when we over-predicted demand."""
        data = {'2026-05-29T19:00:00+02:00': self._sample(forecast_error=-2500.0)}
        issues = validate_value_ranges(data, 'load', 'load_forecast')
        assert issues == []

    def test_negative_load_forecast_flagged(self):
        """load_forecast (actual demand) cannot be negative — must flag."""
        data = {'2026-05-29T19:00:00+02:00': self._sample(load_forecast=-100.0)}
        issues = validate_value_ranges(data, 'load', 'load_forecast')
        assert len(issues) == 1

    def test_poison_forecast_error_flagged(self):
        """A forecast_error of -99k MW is bigger than the entire grid —
        poison signal must still fire."""
        data = {'2026-05-29T19:00:00+02:00': self._sample(forecast_error=-99000.0)}
        issues = validate_value_ranges(data, 'load', 'load_forecast')
        assert len(issues) == 1

    def test_poison_load_actual_flagged(self):
        """load_actual ceiling — load_actual > 100GW is implausible for any
        single country."""
        data = {'2026-05-29T19:00:00+02:00': self._sample(load_actual=200000.0)}
        issues = validate_value_ranges(data, 'load', 'load_forecast')
        assert len(issues) == 1

    def test_two_level_country_nested_payload(self):
        """ENTSO-E load publishes as {country: {ts: {fields}}}."""
        data = {
            'NL': {'2026-05-29T19:00:00+02:00': self._sample()},
            'DE_LU': {
                '2026-05-29T19:00:00+02:00': self._sample(load_forecast=-50.0),
            },
        }
        issues = validate_value_ranges(data, 'load', 'load_forecast')
        assert len(issues) == 1


class TestLoadCrossFieldConsistency:
    """Issue #30: defence-in-depth check on the load triple
    (load_forecast, load_actual, forecast_error). After #28's per-field
    bounds, a MITM that flips load_actual + forecast_error independently
    can still produce a triple inside all per-field bounds. This check
    flags |forecast_error|/load_actual > 40%."""

    def _sample(self, **overrides):
        # Realistic NL evening peak, same as TestLoadFieldRanges.
        base = {
            'load_forecast': 14500.0,
            'load_actual': 14200.0,
            'forecast_error': 300.0,
        }
        base.update(overrides)
        return base

    def test_realistic_triple_passes(self):
        """Acceptance from #30: the realistic record (ratio ~0.02) passes."""
        from utils.data_quality import validate_load_cross_field_consistency
        data = {'2026-05-29T19:00:00+02:00': self._sample()}
        issues = validate_load_cross_field_consistency(data, 'load_forecast')
        assert issues == []

    def test_mitm_triple_flags_warning(self):
        """Acceptance from #30: tampered triple
        (load_forecast=14500, load_actual=50000, forecast_error=20000) —
        each value passes #28's per-field bounds but ratio is 0.40 →
        WARNING."""
        from utils.data_quality import (
            validate_load_cross_field_consistency,
            Severity,
        )
        data = {
            '2026-05-29T19:00:00+02:00': self._sample(
                load_actual=50000.0,
                forecast_error=20000.0,
            )
        }
        issues = validate_load_cross_field_consistency(data, 'load_forecast')
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING
        assert issues[0].check_name == 'load_cross_field_consistency'
        assert issues[0].details['count'] == 1

    def test_max_observed_real_world_ratio_passes(self):
        """The worst observed Mar-Jun 2026 ratio was 18914/70295 = 0.269.
        The 0.40 threshold must give comfortable headroom over this."""
        from utils.data_quality import validate_load_cross_field_consistency
        data = {
            '2026-02-15T08:00:00+01:00': self._sample(
                load_forecast=70000.0,
                load_actual=70295.0,
                forecast_error=18914.0,
            )
        }
        issues = validate_load_cross_field_consistency(data, 'load_forecast')
        assert issues == []

    def test_negative_forecast_error_uses_abs_in_ratio(self):
        """Forecast error is signed; the consistency check uses |error|
        so a large over-prediction triggers the same way as an under-
        prediction."""
        from utils.data_quality import validate_load_cross_field_consistency
        data = {
            '2026-05-29T19:00:00+02:00': self._sample(
                load_actual=50000.0,
                forecast_error=-25000.0,  # ratio 0.50
            )
        }
        issues = validate_load_cross_field_consistency(data, 'load_forecast')
        assert len(issues) == 1

    def test_near_zero_load_actual_uses_floor(self):
        """When load_actual is near zero the denominator floor (1000 MW)
        kicks in. A small absolute |forecast_error| should not false-flag."""
        from utils.data_quality import validate_load_cross_field_consistency
        # 100 MW error against ~0 actual → ratio 0.10 with floor (passes)
        data = {
            '2026-05-29T03:00:00+02:00': self._sample(
                load_actual=0.0,
                forecast_error=100.0,
            )
        }
        issues = validate_load_cross_field_consistency(data, 'load_forecast')
        assert issues == []

    def test_near_zero_actual_with_large_error_still_flags(self):
        """The floor stops false positives at small absolute errors; a
        genuinely large absolute error still trips the check."""
        from utils.data_quality import validate_load_cross_field_consistency
        # 5000 MW error against ~0 actual → ratio 5.0 with floor → flags
        data = {
            '2026-05-29T03:00:00+02:00': self._sample(
                load_actual=0.0,
                forecast_error=5000.0,
            )
        }
        issues = validate_load_cross_field_consistency(data, 'load_forecast')
        assert len(issues) == 1

    def test_two_level_country_nested_payload(self):
        """ENTSO-E load publishes as {country: {ts: {fields}}} — the
        flatten helper must reach the inner records."""
        from utils.data_quality import validate_load_cross_field_consistency
        data = {
            'NL': {'2026-05-29T19:00:00+02:00': self._sample()},
            'DE_LU': {
                '2026-05-29T19:00:00+02:00': self._sample(
                    load_actual=50000.0,
                    forecast_error=20001.0,  # just over 0.40 threshold
                ),
            },
        }
        issues = validate_load_cross_field_consistency(data, 'load_forecast')
        assert len(issues) == 1
        assert issues[0].details['count'] == 1  # DE_LU only

    def test_missing_fields_skipped(self):
        """Records that lack load_actual or forecast_error are skipped
        (this check doesn't double up on completeness)."""
        from utils.data_quality import validate_load_cross_field_consistency
        data = {
            '2026-05-29T19:00:00+02:00': {'load_forecast': 14500.0},
        }
        issues = validate_load_cross_field_consistency(data, 'load_forecast')
        assert issues == []


class TestCompleteness:
    """Tests for validate_completeness."""

    def test_complete_data(self):
        """24+ data points for hourly data should pass."""
        data = _make_price_data(24)
        issues = validate_completeness(data, 'energy_price_forecast')
        assert len(issues) == 0

    def test_empty_data(self):
        """Empty data should be CRITICAL."""
        issues = validate_completeness({}, 'energy_price_forecast')
        assert len(issues) == 1
        assert issues[0].severity == Severity.CRITICAL

    def test_partial_data_warning(self):
        """Half the expected data should be WARNING."""
        data = _make_price_data(15)
        issues = validate_completeness(data, 'energy_price_forecast')
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_very_incomplete_data_error(self):
        """Less than half expected should be ERROR."""
        data = _make_price_data(5)
        issues = validate_completeness(data, 'energy_price_forecast')
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR

    def test_unknown_dataset_uses_default(self):
        """Unknown dataset names should use default expected count."""
        data = _make_price_data(24)
        issues = validate_completeness(data, 'unknown_dataset', expected_hours=24)
        assert len(issues) == 0

    def test_market_proxies_single_point(self):
        """Market proxies only need 1 data point."""
        data = {'2026-01-15': {'carbon': 80.0}}
        issues = validate_completeness(data, 'market_proxies')
        assert len(issues) == 0

    def test_epex_23_points_ok(self):
        """EPEX naturally returns 23 points — should not be flagged."""
        data = _make_price_data(23)
        issues = validate_completeness(data, 'epex')
        assert len(issues) == 0

    def test_elspot_19_points_ok(self):
        """Elspot NL zone returns 19 points — should not be flagged."""
        data = _make_price_data(19)
        issues = validate_completeness(data, 'elspot')
        assert len(issues) == 0

    def test_ned_production_3level_shape_counted(self):
        """Issue #32: ned_production data is 3 levels deep
        ({energy_type: {forecast|actual: {ts: {fields}}}}). Pre-fix
        `_count_data_points` returned 6 (energy_types × classes),
        triggering false completeness/error every day."""
        ts = _make_price_data(24)  # {ts: scalar} — 24 entries
        # Map scalars to fields-dicts so it mirrors real ned shape:
        ts_dict = {k: {'capacity_kw': v} for k, v in ts.items()}
        data = {
            'solar':         {'forecast': ts_dict, 'actual': ts_dict},
            'wind_onshore':  {'forecast': ts_dict, 'actual': ts_dict},
            'wind_offshore': {'forecast': ts_dict, 'actual': ts_dict},
        }
        # 3 kinds × 2 classes × 24 timestamps = 144
        issues = validate_completeness(data, 'ned_production')
        assert issues == [], f"Expected no completeness issues, got {issues}"

    def test_market_history_3level_with_metadata_counted(self):
        """Issue #32: market_history data is
        {series: {metadata: {...}, data: {date: price}}}. Pre-fix
        returned 4 (2 series × 2 sub-keys metadata/data). Now counts
        the date-keyed inner dict."""
        ts = _make_price_data(50)  # 50 daily entries
        data = {
            'gas_ttf':    {'metadata': {'units': 'EUR/MWh'}, 'data': ts},
            'carbon_eua': {'metadata': {'units': 'USD/share'}, 'data': ts},
        }
        # 2 series × 50 dates = 100 (+ 2 metadata sub-dicts counted as 1
        # each = 102). Well above the 24 floor.
        issues = validate_completeness(data, 'market_history')
        assert issues == []

    def test_market_proxies_snapshot_passes(self):
        """Issue #32: market_proxies is a snapshot
        ({commodity: {field: val}}). Each commodity counts as one
        record; with EXPECTED_MIN_POINTS=1 the dataset passes."""
        data = {
            'carbon':  {'price': 80.0, 'units': 'USD/share', 'date': '2026-06-08'},
            'gas_ttf': {'price': 45.0, 'units': 'EUR/MWh',   'date': '2026-06-08'},
            'gas':     {'price': 12.0, 'units': 'USD',       'date': '2026-06-08'},
        }
        issues = validate_completeness(data, 'market_proxies')
        assert issues == []


class TestNullRatio:
    """Tests for validate_null_ratio."""

    def test_no_nulls(self):
        """Data with no nulls should pass."""
        data = _make_price_data(24)
        issues = validate_null_ratio(data)
        assert len(issues) == 0

    def test_few_nulls_ok(self):
        """A few nulls (< 20%) should pass."""
        data = _make_price_data(24)
        # Set 2 out of 24 to None (8.3%)
        keys = list(data.keys())
        data[keys[0]] = None
        data[keys[1]] = None
        issues = validate_null_ratio(data)
        assert len(issues) == 0

    def test_many_nulls_warning(self):
        """More than 20% nulls should trigger WARNING."""
        data = _make_price_data(10)
        keys = list(data.keys())
        for k in keys[:3]:  # 30% null
            data[k] = None
        issues = validate_null_ratio(data)
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_mostly_nulls_error(self):
        """More than 50% nulls should trigger ERROR."""
        data = _make_price_data(10)
        keys = list(data.keys())
        for k in keys[:6]:  # 60% null
            data[k] = None
        issues = validate_null_ratio(data)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR

    def test_nested_nulls(self):
        """Null values in nested dicts should be counted."""
        data = {
            '2026-01-15T00:00:00+01:00': {'temp': None, 'wind': None, 'humidity': None},
            '2026-01-15T01:00:00+01:00': {'temp': 10.0, 'wind': None, 'humidity': 50},
        }
        # 4 out of 6 are None (67%) -> ERROR
        issues = validate_null_ratio(data)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR

    def test_empty_data_no_issue(self):
        """Empty data should not crash or produce null ratio issues."""
        issues = validate_null_ratio({})
        assert len(issues) == 0


class TestStaleness:
    """Tests for validate_staleness."""

    def test_fresh_data(self):
        """Data from now should pass."""
        now = datetime.now(AMS)
        data = {now.isoformat(): 50.0}
        issues = validate_staleness(data)
        assert len(issues) == 0

    def test_stale_data(self):
        """Data from 3 days ago should trigger ERROR (default 48h threshold)."""
        old = (datetime.now(AMS) - timedelta(days=3))
        data = {old.isoformat(): 50.0}
        issues = validate_staleness(data)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR

    def test_custom_threshold(self):
        """Custom staleness threshold should be respected."""
        old = (datetime.now(AMS) - timedelta(hours=10))
        data = {old.isoformat(): 50.0}
        # 10 hours old with 6h threshold -> stale
        issues = validate_staleness(data, max_age_hours=6)
        assert len(issues) == 1
        # 10 hours old with 24h threshold -> fine
        issues = validate_staleness(data, max_age_hours=24)
        assert len(issues) == 0

    def test_unparseable_timestamps(self):
        """Non-ISO timestamps should produce a warning."""
        data = {'not-a-timestamp': 50.0}
        issues = validate_staleness(data)
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_empty_data(self):
        """Empty data should not crash."""
        issues = validate_staleness({})
        assert len(issues) == 0

    def test_mixed_timestamps_uses_newest(self):
        """Should check freshness of the NEWEST data point."""
        old = (datetime.now(AMS) - timedelta(days=5))
        fresh = datetime.now(AMS)
        data = {
            old.isoformat(): 50.0,
            fresh.isoformat(): 55.0,
        }
        issues = validate_staleness(data)
        assert len(issues) == 0  # Newest is fresh

    def test_3level_shape_walks_deeper_for_timestamps(self):
        """Issue #32: ned_production has timestamps 3 levels deep.
        Pre-fix `_extract_timestamp_keys` only walked 1 level deep and
        returned [] → fired the misleading 'could not parse' warning
        every run."""
        fresh = datetime.now(AMS)
        ts_dict = {fresh.isoformat(): {'capacity_kw': 100.0}}
        data = {
            'solar':         {'forecast': ts_dict, 'actual': ts_dict},
            'wind_onshore':  {'forecast': ts_dict, 'actual': ts_dict},
        }
        issues = validate_staleness(data)
        assert issues == [], (
            f"Expected staleness to find the timestamps 3 levels deep, "
            f"got {issues}"
        )

    def test_market_history_data_subkey_dates_found(self):
        """market_history nests dates under {series: {data: {date: price}}}.
        Walker must reach the date keys."""
        fresh_date = datetime.now(AMS).date().isoformat()
        data = {
            'gas_ttf': {
                'metadata': {'units': 'EUR/MWh'},
                'data': {fresh_date: 45.0},
            },
        }
        issues = validate_staleness(data)
        assert issues == []

    def test_snapshot_falls_back_to_metadata_anchor(self):
        """Issue #32: market_proxies is a snapshot — no timestamp-keyed
        dict in data. With metadata_anchor_iso, staleness uses that as
        the freshness anchor instead of emitting the misleading
        'could not parse' warning."""
        fresh = datetime.now(AMS).isoformat()
        snapshot = {
            'carbon':  {'price': 80.0, 'date': fresh},
            'gas_ttf': {'price': 45.0, 'date': fresh},
        }
        issues = validate_staleness(snapshot, metadata_anchor_iso=fresh)
        assert issues == []

    def test_snapshot_without_anchor_still_warns(self):
        """If no metadata_anchor_iso is supplied AND the data has no
        timestamp-keyed dict, the original 'could not parse' warning
        still fires (pre-#32 behaviour preserved when caller doesn't
        opt into the fallback)."""
        snapshot = {
            'carbon':  {'price': 80.0},
            'gas_ttf': {'price': 45.0},
        }
        issues = validate_staleness(snapshot)
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARNING

    def test_stale_metadata_anchor_flags(self):
        """If the fallback anchor itself is stale, the check still fires."""
        old = (datetime.now(AMS) - timedelta(days=7)).isoformat()
        snapshot = {'carbon': {'price': 80.0}}
        issues = validate_staleness(snapshot, metadata_anchor_iso=old)
        assert len(issues) == 1
        assert issues[0].check_name == 'staleness'


class TestDuplicateTimestamps:
    """Tests for validate_duplicate_timestamps."""

    def test_normal_hourly_data(self):
        """Normal hourly data should have no duplicates."""
        data = _make_price_data(24)
        issues = validate_duplicate_timestamps(data)
        assert len(issues) == 0

    def test_single_point_no_issue(self):
        """Single data point should not trigger check."""
        data = {'2026-01-15T00:00:00+01:00': 50.0}
        issues = validate_duplicate_timestamps(data)
        assert len(issues) == 0

    def test_15min_resolution_not_flagged(self):
        """15-minute resolution data (4 per hour) should NOT be flagged."""
        start = datetime(2026, 1, 15, 0, 0, 0, tzinfo=AMS)
        data = {}
        for i in range(96):  # 24h * 4 per hour
            ts = (start + timedelta(minutes=15 * i)).isoformat()
            data[ts] = 50.0 + i
        issues = validate_duplicate_timestamps(data)
        assert len(issues) == 0

    def test_entsoe_96_points_not_flagged(self):
        """96 ENTSO-E points (24h at 15-min) should not be flagged."""
        start = datetime(2026, 1, 15, 0, 0, 0, tzinfo=AMS)
        data = {}
        for i in range(96):
            ts = (start + timedelta(minutes=15 * i)).isoformat()
            data[ts] = 50.0
        issues = validate_duplicate_timestamps(data)
        assert len(issues) == 0


class TestValidateDataset:
    """Tests for the full validate_dataset function."""

    def test_healthy_dataset(self):
        """A healthy dataset should pass all checks."""
        data = _make_price_data(24)
        dataset = _make_dataset(data)
        report = validate_dataset(dataset, 'entsoe')
        assert report.status == 'info'
        assert report.checks_failed == 0
        assert report.checks_passed > 0

    def test_empty_dataset(self):
        """An empty dataset should have critical issues."""
        dataset = _make_dataset({})
        report = validate_dataset(dataset, 'entsoe')
        assert report.status == 'critical'
        assert report.checks_failed > 0

    def test_dataset_with_out_of_range(self):
        """Dataset with extreme values should be flagged."""
        data = _make_price_data(24)
        key = list(data.keys())[0]
        data[key] = 50000.0  # Extreme price
        dataset = _make_dataset(data)
        report = validate_dataset(dataset, 'entsoe')
        assert any(i.check_name == 'value_range' for i in report.issues)

    def test_report_to_dict(self):
        """Report should serialize to dict correctly."""
        data = _make_price_data(24)
        dataset = _make_dataset(data)
        report = validate_dataset(dataset, 'entsoe')
        d = report.to_dict()
        assert 'dataset_name' in d
        assert 'status' in d
        assert 'issues' in d
        assert isinstance(d['issues'], list)


class TestValidatePipeline:
    """Tests for pipeline-level validation."""

    def test_full_pipeline(self):
        """Pipeline with all required datasets should pass."""
        datasets = {
            'entsoe': _make_dataset(_make_price_data(24)),
            'energy_zero': _make_dataset(_make_price_data(24)),
            'epex': _make_dataset(_make_price_data(24)),
        }
        report = validate_pipeline(datasets)
        assert 'entsoe' not in report.missing_datasets
        assert 'energy_zero' not in report.missing_datasets
        assert len(report.dataset_reports) == 3

    def test_missing_required_dataset(self):
        """Missing required dataset should be flagged."""
        datasets = {
            'epex': _make_dataset(_make_price_data(24)),
        }
        report = validate_pipeline(datasets)
        assert 'entsoe' in report.missing_datasets
        assert 'energy_zero' in report.missing_datasets
        assert report.status == 'critical'

    def test_none_dataset_treated_as_missing(self):
        """Dataset that is None (failed collection) should be treated as missing."""
        datasets = {
            'entsoe': None,
            'energy_zero': _make_dataset(_make_price_data(24)),
        }
        report = validate_pipeline(datasets)
        assert 'entsoe' in report.missing_datasets

    def test_pipeline_report_to_dict(self):
        """Pipeline report should serialize completely."""
        datasets = {
            'entsoe': _make_dataset(_make_price_data(24)),
            'energy_zero': _make_dataset(_make_price_data(24)),
        }
        report = validate_pipeline(datasets)
        d = report.to_dict()
        assert 'overall_status' in d
        assert 'total_issues' in d
        assert 'datasets_collected' in d
        assert 'datasets_missing' in d
        assert 'dataset_reports' in d

    def test_pipeline_with_mixed_quality(self):
        """Pipeline with some good and some bad datasets."""
        datasets = {
            'entsoe': _make_dataset(_make_price_data(24)),
            'energy_zero': _make_dataset({}),  # Empty = critical
        }
        report = validate_pipeline(datasets)
        assert report.status == 'critical'
        assert report.total_issues > 0


class TestDatasetMissingSeverityRegistry:
    """Reviewer tier-2 finding (refactoring-guide OVER-ENGINEERED):
    the three parallel registries collapsed into a single
    DATASET_MISSING_SEVERITY dict. The back-compat shims must still
    derive correctly from the dict so any importer keeps working.
    """

    def test_required_datasets_derived_from_critical(self):
        from utils.data_quality import DATASET_MISSING_SEVERITY, REQUIRED_DATASETS
        for name in REQUIRED_DATASETS:
            assert DATASET_MISSING_SEVERITY.get(name) == 'critical'
        # And the reverse — every critical entry shows up in REQUIRED_DATASETS.
        for name, sev in DATASET_MISSING_SEVERITY.items():
            if sev == 'critical':
                assert name in REQUIRED_DATASETS

    def test_warning_datasets_derived(self):
        from utils.data_quality import DATASET_MISSING_SEVERITY, WARNING_IF_MISSING_DATASETS
        for name in WARNING_IF_MISSING_DATASETS:
            assert DATASET_MISSING_SEVERITY.get(name) == 'warning'

    def test_expected_datasets_includes_critical_and_info(self):
        """EXPECTED_DATASETS historically included REQUIRED_DATASETS as a
        superset (it duplicates the critical entries plus info-flake
        ones). The back-compat shim preserves this — both critical and
        info entries appear."""
        from utils.data_quality import DATASET_MISSING_SEVERITY, EXPECTED_DATASETS
        for name in EXPECTED_DATASETS:
            sev = DATASET_MISSING_SEVERITY.get(name)
            assert sev in ('critical', 'info')

    def test_grid_imbalance_is_warning_severity(self):
        """The #25 soft-gate is preserved through the refactor."""
        from utils.data_quality import DATASET_MISSING_SEVERITY
        assert DATASET_MISSING_SEVERITY['grid_imbalance'] == 'warning'

    def test_critical_datasets_are_entsoe_and_energy_zero(self):
        """Stable contract: the two critical datasets are the day-ahead
        price feeds that Augur depends on."""
        from utils.data_quality import DATASET_MISSING_SEVERITY
        critical = {
            name for name, sev in DATASET_MISSING_SEVERITY.items()
            if sev == 'critical'
        }
        assert critical == {'entsoe', 'energy_zero'}


class TestSoftGateForGridImbalance:
    """Issue #25: missing grid_imbalance promotes status to 'warning',
    not 'critical' — TenneT failure is operational, not stop-everything.
    """

    def test_missing_grid_imbalance_is_tracked(self):
        """grid_imbalance absence is recorded in missing_datasets."""
        datasets = {
            'entsoe': _make_dataset(_make_price_data(24)),
            'energy_zero': _make_dataset(_make_price_data(24)),
            'grid_imbalance': None,  # TenneT 422 cascade
        }
        report = validate_pipeline(datasets)
        assert 'grid_imbalance' in report.missing_datasets

    def test_missing_grid_imbalance_promotes_to_warning(self):
        """grid_imbalance missing → overall_status='warning' (not critical)."""
        datasets = {
            'entsoe': _make_dataset(_make_price_data(24)),
            'energy_zero': _make_dataset(_make_price_data(24)),
            'grid_imbalance': None,
        }
        report = validate_pipeline(datasets)
        # Not critical — TenneT failure doesn't block publish
        assert report.status != 'critical'
        # But visible as warning so anyone reading overall_status sees it
        assert report.status == 'warning'

    def test_grid_imbalance_present_no_warning(self):
        """When grid_imbalance is present, no soft-gate warning is added."""
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        ams = ZoneInfo('Europe/Amsterdam')
        now = datetime.now(ams)
        grid_data = {
            (now - timedelta(hours=i)).isoformat(): 50.0 for i in range(24)
        }
        datasets = {
            'entsoe': _make_dataset(_make_price_data(24)),
            'energy_zero': _make_dataset(_make_price_data(24)),
            'grid_imbalance': _make_dataset(grid_data, data_type='grid_imbalance'),
        }
        report = validate_pipeline(datasets)
        assert 'grid_imbalance' not in report.missing_datasets
        # No soft-gate-driven warning; status reflects only dataset reports
        assert report.status in ('info', 'warning')  # warning possible from other checks

    def test_missing_required_still_wins_over_soft_gate(self):
        """A required-missing dataset → critical, even when grid_imbalance
        is also missing. Critical wins over warning."""
        datasets = {
            'entsoe': None,             # REQUIRED missing → critical
            'energy_zero': _make_dataset(_make_price_data(24)),
            'grid_imbalance': None,     # Soft-gated → would be warning alone
        }
        report = validate_pipeline(datasets)
        assert report.status == 'critical'


class TestQualityIssue:
    """Tests for QualityIssue dataclass."""

    def test_to_dict(self):
        """QualityIssue should serialize correctly."""
        issue = QualityIssue(
            check_name='test',
            severity=Severity.WARNING,
            message='test message',
            details={'key': 'value'},
        )
        d = issue.to_dict()
        assert d['severity'] == 'warning'
        assert d['check_name'] == 'test'
        assert d['details'] == {'key': 'value'}

    def test_to_dict_without_details(self):
        """QualityIssue without details should not include details key."""
        issue = QualityIssue(
            check_name='test',
            severity=Severity.INFO,
            message='all good',
        )
        d = issue.to_dict()
        assert 'details' not in d


class TestDatasetQualityReportStatus:
    """Tests for status property severity escalation."""

    def test_no_issues_is_info(self):
        report = DatasetQualityReport(
            dataset_name='test', data_type='test', source='test', data_points=24
        )
        assert report.status == 'info'

    def test_warning_only(self):
        report = DatasetQualityReport(
            dataset_name='test', data_type='test', source='test', data_points=24,
            issues=[QualityIssue('x', Severity.WARNING, 'w')]
        )
        assert report.status == 'warning'

    def test_error_overrides_warning(self):
        report = DatasetQualityReport(
            dataset_name='test', data_type='test', source='test', data_points=24,
            issues=[
                QualityIssue('x', Severity.WARNING, 'w'),
                QualityIssue('y', Severity.ERROR, 'e'),
            ]
        )
        assert report.status == 'error'

    def test_critical_overrides_all(self):
        report = DatasetQualityReport(
            dataset_name='test', data_type='test', source='test', data_points=24,
            issues=[
                QualityIssue('x', Severity.ERROR, 'e'),
                QualityIssue('y', Severity.CRITICAL, 'c'),
            ]
        )
        assert report.status == 'critical'
