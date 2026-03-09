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
