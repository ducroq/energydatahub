"""
Data Quality Validation Framework (FMEA-based)
-----------------------------------------------
Systematic data quality checks for the energyDataHub pipeline.

Based on Failure Mode and Effects Analysis (FMEA) of the data collection pipeline,
this module provides validation rules that catch common failure modes:

1. Value range violations (e.g., negative wind speed, extreme prices)
2. Completeness checks (expected vs actual data points)
3. Staleness detection (data older than expected)
4. Schema consistency (expected fields present)
5. Cross-source consistency (multiple sources for same data type)

File: utils/data_quality.py
Created: 2026-03-09
Author: Energy Data Hub Project
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional

from utils.data_types import EnhancedDataSet, CombinedDataSet

logger = logging.getLogger(__name__)


class Severity(Enum):
    """Severity of a quality issue."""
    INFO = "info"           # Informational, no action needed
    WARNING = "warning"     # Degraded quality, data still usable
    ERROR = "error"         # Significant issue, data may be unreliable
    CRITICAL = "critical"   # Data should not be used


@dataclass
class QualityIssue:
    """A single data quality issue found during validation."""
    check_name: str
    severity: Severity
    message: str
    details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            'check_name': self.check_name,
            'severity': self.severity.value,
            'message': self.message,
        }
        if self.details:
            result['details'] = self.details
        return result


@dataclass
class DatasetQualityReport:
    """Quality report for a single dataset."""
    dataset_name: str
    data_type: str
    source: str
    data_points: int
    issues: List[QualityIssue] = field(default_factory=list)
    checks_passed: int = 0
    checks_failed: int = 0

    @property
    def status(self) -> str:
        max_severity = Severity.INFO
        for issue in self.issues:
            if issue.severity == Severity.CRITICAL:
                return "critical"
            if issue.severity == Severity.ERROR:
                max_severity = Severity.ERROR
            elif issue.severity == Severity.WARNING and max_severity != Severity.ERROR:
                max_severity = Severity.WARNING
        return max_severity.value

    def to_dict(self) -> Dict[str, Any]:
        return {
            'dataset_name': self.dataset_name,
            'data_type': self.data_type,
            'source': self.source,
            'status': self.status,
            'data_points': self.data_points,
            'checks_passed': self.checks_passed,
            'checks_failed': self.checks_failed,
            'issues': [i.to_dict() for i in self.issues],
        }


@dataclass
class PipelineQualityReport:
    """Quality report for the entire pipeline run."""
    timestamp: str
    dataset_reports: List[DatasetQualityReport] = field(default_factory=list)
    missing_datasets: List[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.missing_datasets:
            has_critical_missing = any(
                name in ('entsoe', 'energy_zero')
                for name in self.missing_datasets
            )
            if has_critical_missing:
                return "critical"
        statuses = [r.status for r in self.dataset_reports]
        if "critical" in statuses:
            return "critical"
        if "error" in statuses:
            return "error"
        if "warning" in statuses:
            return "warning"
        return "info"

    @property
    def total_issues(self) -> int:
        return sum(len(r.issues) for r in self.dataset_reports)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp,
            'overall_status': self.status,
            'total_issues': self.total_issues,
            'datasets_collected': len(self.dataset_reports),
            'datasets_missing': self.missing_datasets,
            'dataset_reports': [r.to_dict() for r in self.dataset_reports],
        }


# --- Value range definitions per data type ---
# These are physically reasonable bounds, not statistical outliers.
# Exceeding these means something is fundamentally wrong with the data.

VALUE_RANGES = {
    'energy_price': {
        'min': -500.0,    # Negative prices happen (oversupply), but not below -500 EUR/MWh
        'max': 10000.0,   # 10,000 EUR/MWh is extreme but has occurred in crises
        'unit': 'EUR/MWh',
    },
    'wind_speed': {
        'min': 0.0,
        'max': 75.0,      # Hurricane force ~65 m/s, turbines cut out ~25 m/s
        'unit': 'm/s',
    },
    'wind_direction': {
        'min': 0.0,
        'max': 360.0,
        'unit': 'degrees',
    },
    'temperature': {
        'min': -50.0,     # Extreme cold in NW Europe
        'max': 50.0,      # Extreme heat
        'unit': '°C',
    },
    'solar_irradiance': {
        'min': 0.0,
        'max': 1400.0,    # Solar constant ~1361 W/m², ground max ~1200 W/m²
        'unit': 'W/m²',
    },
    'load_mw': {
        'min': 0.0,
        'max': 100000.0,  # NL peak ~18 GW, DE peak ~80 GW
        'unit': 'MW',
    },
    'generation_mw': {
        'min': 0.0,
        'max': 100000.0,
        'unit': 'MW',
    },
    'flow_mw': {
        'min': -15000.0,  # Cross-border flows can be negative (export)
        'max': 15000.0,
        'unit': 'MW',
    },
    'gas_storage_pct': {
        'min': 0.0,
        'max': 100.0,
        'unit': '%',
    },
}

# Per-field ranges for weather data (nested dict with multiple fields)
# Used instead of a single blanket range when data_type is 'weather'
WEATHER_FIELD_RANGES = {
    # Temperature fields (°C)
    'temp': (-50.0, 50.0),
    'temperature': (-50.0, 50.0),
    'main_temp': (-50.0, 50.0),
    'main_feels_like': (-60.0, 60.0),
    'main_temp_min': (-50.0, 50.0),
    'main_temp_max': (-50.0, 50.0),
    'main_temp_kf': (-20.0, 20.0),
    'apparent_temperature': (-60.0, 60.0),
    'temperature_2m': (-50.0, 50.0),
    # Pressure fields (hPa)
    'main_pressure': (850.0, 1100.0),
    'main_sea_level': (850.0, 1100.0),
    'main_grnd_level': (700.0, 1100.0),
    'luchtd': (850.0, 1100.0),
    'pressure_msl': (850.0, 1100.0),
    # Pressure in other units
    'luchtdmmhg': (630.0, 830.0),
    'luchtdinhg': (25.0, 33.0),
    # Humidity fields (%)
    'main_humidity': (0.0, 100.0),
    'rv': (0.0, 100.0),
    'humidity': (0.0, 100.0),
    'relative_humidity_2m': (0.0, 100.0),
    # Wind fields
    'wind_speed': (0.0, 75.0),
    'wind_gust': (0.0, 100.0),
    'wind_deg': (0.0, 360.0),
    'wind_direction': (0.0, 360.0),
    'winds': (0.0, 12.0),        # Beaufort scale
    'windb': (0.0, 12.0),
    'windknp': (0.0, 200.0),     # Knots
    'windkmh': (0.0, 400.0),     # km/h
    'windr': (0.0, 360.0),       # Wind direction degrees
    'wind_speed_10m': (0.0, 75.0),
    'wind_speed_80m': (0.0, 100.0),
    'wind_speed_120m': (0.0, 120.0),
    'wind_speed_180m': (0.0, 130.0),
    'wind_direction_10m': (0.0, 360.0),
    'wind_direction_80m': (0.0, 360.0),
    'wind_direction_120m': (0.0, 360.0),
    'wind_direction_180m': (0.0, 360.0),
    # Visibility (meters)
    'vis': (0.0, 100000.0),
    'visibility': (0.0, 100000.0),
    # Precipitation (mm)
    'neersl': (0.0, 500.0),
    'precipitation': (0.0, 500.0),
    # Cloud cover (%)
    'clouds_all': (0.0, 100.0),
    'hw': (0.0, 100.0),          # High clouds
    'mw': (0.0, 100.0),          # Mid clouds
    'lw': (0.0, 100.0),          # Low clouds
    'tw': (0.0, 100.0),          # Total clouds
    'cloud_cover': (0.0, 100.0),
    # Solar radiation (W/m²)
    'gr': (0.0, 1400.0),
    'gr_w': (0.0, 1400.0),
    'shortwave_radiation': (0.0, 1400.0),
    'direct_radiation': (0.0, 1400.0),
    'diffuse_radiation': (0.0, 800.0),
    'direct_normal_irradiance': (0.0, 1400.0),
    'global_tilted_irradiance': (0.0, 1400.0),
    # CAPE (J/kg)
    'cape': (0.0, 6000.0),
    # Snow
    'snd': (0.0, 300.0),         # Snow depth (cm)
    'snv': (0.0, 100.0),         # Snow coverage (%)
    # Air density (kg/m³)
    'air_density': (0.5, 2.0),
    # HDD/CDD (degree days)
    'hdd': (0.0, 50.0),
    'cdd': (0.0, 40.0),
}

# Expected minimum data points per dataset/source for a standard daily collection.
# Keys can be dataset names (from pipeline) or source-specific collector names.
EXPECTED_MIN_POINTS = {
    # Per-source minimums (inside CombinedDataSet files)
    'entsoe': 24,                    # 24h hourly or 96 at 15-min (both OK)
    'entsoe_de': 24,                 # German prices, same as NL
    'energy_zero': 24,               # 24-48h of hourly prices
    'epex': 23,                      # EPEX returns 23 prices (trading window offset)
    'elspot': 19,                    # Nord Pool NL zone: 19-24 prices typical
    # Pipeline-level file names
    'weather_forecast_multi_location': 24,
    'wind_forecast': 24,
    'solar_forecast': 24,
    'demand_weather_forecast': 24,
    'grid_imbalance': 48,            # 15-min resolution = 96 per day, but partial is OK
    'cross_border_flows': 12,
    'load_forecast': 24,
    'generation_forecast': 24,
    'calendar_features': 24,
    'market_proxies': 1,             # Daily values
    'gas_storage': 1,                # Daily values
    'gas_flows': 1,                  # Daily values
    'ned_production': 24,
    # Old collector names (for backtest compatibility)
    'OpenWeather': 8,                # OpenWeather free tier: limited forecast points
    'MeteoServer': 24,
}

# Required datasets (pipeline should warn if these are missing)
REQUIRED_DATASETS = ['entsoe', 'energy_zero']
EXPECTED_DATASETS = [
    'entsoe', 'entsoe_de', 'energy_zero', 'epex', 'elspot',
]


def _count_data_points(data: Any) -> int:
    """Count data points, handling nested structures."""
    if not isinstance(data, dict):
        return 0
    # Check if values are nested dicts (multi-location data)
    first_value = next(iter(data.values()), None)
    if isinstance(first_value, dict):
        # Could be location -> {timestamp -> value} or timestamp -> {fields}
        # Count leaf values
        return sum(
            len(v) if isinstance(v, dict) else 1
            for v in data.values()
        )
    return len(data)


def _check_field_range(
    field_name: str,
    value: float,
    field_ranges: Dict[str, tuple],
) -> bool:
    """Check if a named field's value is within its specific range.

    Returns True if out of range, False if OK or field is unknown.
    """
    if field_name not in field_ranges:
        return False  # Unknown field — don't flag
    min_val, max_val = field_ranges[field_name]
    return value < min_val or value > max_val


def validate_value_ranges(
    data: Dict[str, Any],
    data_type: str,
    dataset_name: str,
) -> List[QualityIssue]:
    """
    Check that all scalar values fall within physically reasonable ranges.

    For weather data, uses per-field ranges (temperature, pressure, humidity, etc.)
    instead of a single blanket range. For other data types, uses a single range.

    Args:
        data: The data dictionary from an EnhancedDataSet
        data_type: The data type (e.g., 'energy_price', 'weather')
        dataset_name: Name for reporting

    Returns:
        List of QualityIssue objects
    """
    issues = []
    use_field_ranges = data_type == 'weather'
    range_spec = VALUE_RANGES.get(data_type) if not use_field_ranges else None

    if not use_field_ranges and not range_spec:
        return issues

    out_of_range_count = 0
    out_of_range_examples = []

    for timestamp, value in data.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if not isinstance(sub_value, (int, float)) or sub_value is None:
                    continue
                if use_field_ranges:
                    if _check_field_range(sub_key, sub_value, WEATHER_FIELD_RANGES):
                        out_of_range_count += 1
                        if len(out_of_range_examples) < 5:
                            field_range = WEATHER_FIELD_RANGES.get(sub_key, ('?', '?'))
                            out_of_range_examples.append(
                                f"{sub_key}={sub_value} (range {field_range[0]}-{field_range[1]})"
                            )
                elif sub_value < range_spec['min'] or sub_value > range_spec['max']:
                    out_of_range_count += 1
                    if len(out_of_range_examples) < 5:
                        out_of_range_examples.append(
                            f"{timestamp}/{sub_key}: {sub_value}"
                        )
        elif isinstance(value, (int, float)) and value is not None:
            if range_spec and (value < range_spec['min'] or value > range_spec['max']):
                out_of_range_count += 1
                if len(out_of_range_examples) < 5:
                    out_of_range_examples.append(f"{timestamp}: {value}")

    if out_of_range_count > 0:
        severity = Severity.ERROR if out_of_range_count > 3 else Severity.WARNING
        if use_field_ranges:
            msg = f"{out_of_range_count} weather field values outside their specific ranges"
        else:
            msg = (f"{out_of_range_count} values outside range "
                   f"[{range_spec['min']}, {range_spec['max']}] {range_spec['unit']}")
        issues.append(QualityIssue(
            check_name='value_range',
            severity=severity,
            message=msg,
            details={
                'count': out_of_range_count,
                'examples': out_of_range_examples,
            },
        ))

    return issues


def _is_dst_transition_data(data: Dict[str, Any]) -> bool:
    """Check if data spans a DST transition day (23 or 25 hour day)."""
    for key in data.keys():
        try:
            dt = datetime.fromisoformat(key.replace('Z', '+00:00'))
            # Check if this date is a DST transition
            from utils.calendar_features import get_dst_info
            _, is_transition, _ = get_dst_info(dt)
            if is_transition:
                return True
        except (ValueError, TypeError):
            continue
    return False


def validate_completeness(
    data: Dict[str, Any],
    dataset_name: str,
    expected_hours: int = 24,
) -> List[QualityIssue]:
    """
    Check that we have a reasonable number of data points.

    DST-aware: on transition days, adjusts expectations (23 or 25 hours
    instead of 24) to avoid false positives.

    Args:
        data: The data dictionary
        dataset_name: Name for looking up expected counts
        expected_hours: Expected number of hourly data points

    Returns:
        List of QualityIssue objects
    """
    issues = []
    actual_points = _count_data_points(data)
    min_expected = EXPECTED_MIN_POINTS.get(dataset_name, expected_hours)

    # On DST transition days, allow 1 fewer point (23h spring-forward)
    if min_expected > 1 and _is_dst_transition_data(data):
        min_expected = max(1, min_expected - 1)

    if actual_points == 0:
        issues.append(QualityIssue(
            check_name='completeness',
            severity=Severity.CRITICAL,
            message=f"No data points collected (expected >= {min_expected})",
            details={'actual': 0, 'expected_min': min_expected},
        ))
    elif actual_points < min_expected:
        # Calculate how incomplete
        ratio = actual_points / min_expected
        severity = Severity.ERROR if ratio < 0.5 else Severity.WARNING
        issues.append(QualityIssue(
            check_name='completeness',
            severity=severity,
            message=f"Only {actual_points} data points (expected >= {min_expected}, "
                    f"{ratio:.0%} complete)",
            details={
                'actual': actual_points,
                'expected_min': min_expected,
                'completeness_ratio': round(ratio, 2),
            },
        ))

    return issues


def validate_null_ratio(
    data: Dict[str, Any],
    max_null_ratio: float = 0.2,
) -> List[QualityIssue]:
    """
    Check that the ratio of null/None values is acceptable.

    Args:
        data: The data dictionary
        max_null_ratio: Maximum acceptable ratio of None values (default 20%)

    Returns:
        List of QualityIssue objects
    """
    issues = []
    total = 0
    null_count = 0

    for value in data.values():
        if isinstance(value, dict):
            for sub_value in value.values():
                total += 1
                if sub_value is None:
                    null_count += 1
        else:
            total += 1
            if value is None:
                null_count += 1

    if total == 0:
        return issues

    null_ratio = null_count / total
    if null_ratio > max_null_ratio:
        severity = Severity.ERROR if null_ratio > 0.5 else Severity.WARNING
        issues.append(QualityIssue(
            check_name='null_ratio',
            severity=severity,
            message=f"{null_count}/{total} values are null ({null_ratio:.0%}), "
                    f"threshold is {max_null_ratio:.0%}",
            details={
                'null_count': null_count,
                'total_count': total,
                'null_ratio': round(null_ratio, 3),
                'threshold': max_null_ratio,
            },
        ))

    return issues


def validate_staleness(
    data: Dict[str, Any],
    max_age_hours: int = 48,
) -> List[QualityIssue]:
    """
    Check that data is not stale (all timestamps too old).

    Args:
        data: The data dictionary with ISO timestamp keys
        max_age_hours: Maximum acceptable age of newest data point

    Returns:
        List of QualityIssue objects
    """
    issues = []
    if not data:
        return issues

    # Find the most recent timestamp
    now = datetime.now().astimezone()
    newest_ts = None

    for key in data.keys():
        try:
            dt = datetime.fromisoformat(key.replace('Z', '+00:00'))
            if newest_ts is None or dt > newest_ts:
                newest_ts = dt
        except (ValueError, TypeError):
            continue

    if newest_ts is None:
        issues.append(QualityIssue(
            check_name='staleness',
            severity=Severity.WARNING,
            message="Could not parse any timestamps to check staleness",
        ))
        return issues

    age = now - newest_ts
    age_hours = age.total_seconds() / 3600

    if age_hours > max_age_hours:
        issues.append(QualityIssue(
            check_name='staleness',
            severity=Severity.ERROR,
            message=f"Newest data is {age_hours:.1f}h old (threshold: {max_age_hours}h)",
            details={
                'newest_timestamp': newest_ts.isoformat(),
                'age_hours': round(age_hours, 1),
                'threshold_hours': max_age_hours,
            },
        ))

    return issues


def validate_duplicate_timestamps(
    data: Dict[str, Any],
) -> List[QualityIssue]:
    """
    Check for suspicious timestamp patterns that suggest data issues.

    Dict keys are unique by definition, so true duplicates can't exist.
    Instead, this checks for:
    - Multiple points resolving to the same minute (normalization issue)
    - Very low ratio of unique timestamps to data points

    Note: sub-hourly data (e.g., 15-min ENTSO-E since Oct 2025) legitimately
    has 4 points per hour, so we check at minute granularity, not hour.
    """
    issues = []
    if not data or len(data) < 2:
        return issues

    # Check at minute granularity (catches true duplicates without
    # false-flagging 15-min resolution data)
    minutes_seen = set()
    parseable = 0
    for key in data.keys():
        try:
            dt = datetime.fromisoformat(key.replace('Z', '+00:00'))
            minute_key = dt.strftime('%Y-%m-%d %H:%M')
            minutes_seen.add(minute_key)
            parseable += 1
        except (ValueError, TypeError):
            continue

    if parseable > 0 and len(minutes_seen) < parseable * 0.5:
        issues.append(QualityIssue(
            check_name='duplicate_timestamps',
            severity=Severity.WARNING,
            message=f"Only {len(minutes_seen)} unique timestamps from {parseable} data points "
                    f"(possible normalization issue)",
            details={
                'unique_timestamps': len(minutes_seen),
                'total_points': parseable,
            },
        ))

    return issues


# --- Data type to range mapping for automatic validation ---
# Maps data_type from metadata to the VALUE_RANGES key to use.
# 'weather' is special: uses per-field WEATHER_FIELD_RANGES instead.
DATA_TYPE_RANGE_MAP = {
    'energy_price': 'energy_price',
    'wind_generation': 'generation_mw',
    'wind_weather': 'wind_speed',
    'solar': 'solar_irradiance',
    'weather': 'weather',            # Special: uses WEATHER_FIELD_RANGES per-field
    'load': 'load_mw',
    'generation': 'generation_mw',
    'grid_imbalance': 'energy_price',  # Imbalance prices in EUR/MWh
    'cross_border_flows': 'flow_mw',
    'gas_storage': 'gas_storage_pct',
}


def validate_dataset(
    dataset: EnhancedDataSet,
    dataset_name: str,
) -> DatasetQualityReport:
    """
    Run all quality checks on a single dataset.

    Args:
        dataset: The EnhancedDataSet to validate
        dataset_name: Name for the report

    Returns:
        DatasetQualityReport with all findings
    """
    data_type = dataset.metadata.get('data_type', 'unknown')
    source = dataset.metadata.get('source', 'unknown')
    data_points = _count_data_points(dataset.data)

    report = DatasetQualityReport(
        dataset_name=dataset_name,
        data_type=data_type,
        source=source,
        data_points=data_points,
    )

    checks_run = 0
    checks_failed = 0

    # 1. Completeness check
    checks_run += 1
    completeness_issues = validate_completeness(dataset.data, dataset_name)
    if completeness_issues:
        checks_failed += 1
        report.issues.extend(completeness_issues)

    # 2. Null ratio check
    checks_run += 1
    null_issues = validate_null_ratio(dataset.data)
    if null_issues:
        checks_failed += 1
        report.issues.extend(null_issues)

    # 3. Value range check (if we know the data type)
    range_type = DATA_TYPE_RANGE_MAP.get(data_type)
    if range_type:
        checks_run += 1
        range_issues = validate_value_ranges(dataset.data, range_type, dataset_name)
        if range_issues:
            checks_failed += 1
            report.issues.extend(range_issues)

    # 4. Staleness check
    checks_run += 1
    staleness_issues = validate_staleness(dataset.data)
    if staleness_issues:
        checks_failed += 1
        report.issues.extend(staleness_issues)

    # 5. Duplicate timestamp check
    checks_run += 1
    dup_issues = validate_duplicate_timestamps(dataset.data)
    if dup_issues:
        checks_failed += 1
        report.issues.extend(dup_issues)

    report.checks_passed = checks_run - checks_failed
    report.checks_failed = checks_failed

    # Log summary
    if report.issues:
        for issue in report.issues:
            log_func = {
                Severity.INFO: logger.info,
                Severity.WARNING: logger.warning,
                Severity.ERROR: logger.error,
                Severity.CRITICAL: logger.critical,
            }.get(issue.severity, logger.warning)
            log_func(f"[DQ:{dataset_name}] {issue.check_name}: {issue.message}")
    else:
        logger.info(f"[DQ:{dataset_name}] All {checks_run} quality checks passed")

    return report


def validate_pipeline(
    datasets: Dict[str, Optional[EnhancedDataSet]],
) -> PipelineQualityReport:
    """
    Run quality checks on all datasets from a pipeline run.

    Args:
        datasets: Dict mapping dataset names to EnhancedDataSet (or None if failed)

    Returns:
        PipelineQualityReport with all findings
    """
    report = PipelineQualityReport(
        timestamp=datetime.now().astimezone().isoformat(),
    )

    # Check for missing required datasets
    for name in REQUIRED_DATASETS:
        if name not in datasets or datasets[name] is None:
            report.missing_datasets.append(name)
            logger.warning(f"[DQ:pipeline] Required dataset '{name}' is missing")

    # Check for missing expected datasets
    for name in EXPECTED_DATASETS:
        if name not in datasets or datasets[name] is None:
            if name not in report.missing_datasets:
                report.missing_datasets.append(name)
                logger.info(f"[DQ:pipeline] Expected dataset '{name}' is missing")

    # Validate each collected dataset
    for name, dataset in datasets.items():
        if dataset is None:
            continue
        dataset_report = validate_dataset(dataset, name)
        report.dataset_reports.append(dataset_report)

    # Log overall summary
    status = report.status
    total_issues = report.total_issues
    collected = len(report.dataset_reports)
    missing = len(report.missing_datasets)

    logger.info(
        f"[DQ:pipeline] Quality report: status={status}, "
        f"datasets={collected} collected / {missing} missing, "
        f"issues={total_issues}"
    )

    return report
