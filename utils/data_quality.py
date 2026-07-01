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
    # Subset of missing_datasets whose absence is an upstream data gap (the
    # source responded OK but published nothing), not a collector failure.
    # A critical dataset that is merely upstream-empty is downgraded to a
    # 'warning' contribution so a temporary source outage doesn't block
    # publishing the healthy feeds. See collectors.base.UpstreamNoDataError.
    upstream_empty_datasets: List[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        statuses = [r.status for r in self.dataset_reports]
        # Per-name severity contribution from missing datasets — single
        # registry consulted instead of three parallel lists (tier-2
        # reviewer finding on 7c0de64). An upstream-empty critical dataset
        # contributes only a 'warning' (source healthy, no data yet).
        _upstream_empty = set(self.upstream_empty_datasets)
        missing_severities = set()
        for name in self.missing_datasets:
            sev = DATASET_MISSING_SEVERITY.get(name, 'info')
            if name in _upstream_empty and sev == 'critical':
                sev = 'warning'
            missing_severities.add(sev)

        # Worst-case wins ladder: critical > error > warning > info.
        if 'critical' in missing_severities or "critical" in statuses:
            return "critical"
        if "error" in statuses:
            return "error"
        if 'warning' in missing_severities or "warning" in statuses:
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
            'datasets_upstream_empty': self.upstream_empty_datasets,
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

# Per-field ranges for GIE gas-storage data. The collector publishes a nested
# dict per timestamp mixing percent and energy fields; a single 0-100% range
# (the prior behaviour) false-flags every TWh/GWh value daily (issue #24).
# Bounds derived from 133 observed NL files (1062 records, Jan-May 2026):
#   gas_in_storage_twh observed 6-56 TWh (NL is country_code='NL' per
#     data_fetcher.py:545 — NOT EU aggregate). 200 TWh ceiling = 3.5x peak.
#   working_gas_volume_twh ~144 TWh (fixed NL infrastructure capacity).
#   injection peak ~422 GWh/day; withdrawal peak ~1287 GWh/day (Jan 2026 cold).
#   net_change is asymmetric: winter withdrawal dominates so lower bound is wider.
# If country_code is ever switched to EU aggregate, all bounds need to be
# re-derived — EU working capacity is ~1100 TWh and peak withdrawal can
# exceed 10,000 GWh/day.
GAS_STORAGE_FIELD_RANGES = {
    'fill_level_pct':         (0.0,    100.0),
    'gas_in_storage_twh':     (0.0,    200.0),
    'working_gas_volume_twh': (0.0,    200.0),
    'injection_gwh':          (0.0,   1500.0),
    'withdrawal_gwh':         (0.0,   3000.0),
    'net_change_gwh':         (-3000.0, 1500.0),
}

# Per-field ranges for ENTSO-E A72 Nordic hydro reservoirs (#3 security review).
# Without this entry the pipeline range check silently no-ops on hydro_reservoir,
# leaving only the collector's in-band bounds — which a plausible-value MITM
# (e.g. flipping NO from 8.4e7 to 8.4e6, a credible 10x drawdown) would defeat.
# Upper bound matches the collector's _validate_data ceiling of 1.2e8 MWh.
HYDRO_RESERVOIR_FIELD_RANGES = {
    'reservoir_mwh': (0.0, 1.2e8),
}

# Per-field ranges for OpenMeteo solar feeds (#28). The collector emits
# 3-letter aliases (ghi/dni/dhi/direct) plus cloud_cover — NOT the
# OpenMeteo source names that WEATHER_FIELD_RANGES carries. The prior
# blanket [0, 1400] route via `solar_irradiance` fired on every
# dawn/dusk numerical-noise negative (DHI observed as low as -266 W/m²
# from Open-Meteo). Bounds derived from 107 solar_forecast + 8
# solar_forecast_buurt files Mar-Jun 2026 (215k records):
#   ghi   observed [-1, 912];    bound [-10, 1400]  (physical ceiling)
#   dni   observed [0, 933];     bound [0, 1400]    (always non-negative)
#   dhi   observed [-266, 683];  bound [-300, 800]  (dawn artifact + 1.2x)
#   direct observed [-1, 785];   bound [-10, 1200]
#   cloud_cover observed [0,100]; bound [0, 100]    (% bounded)
SOLAR_FIELD_RANGES = {
    'ghi':         (-10.0, 1400.0),
    'dni':         (   0.0, 1400.0),
    'dhi':         (-300.0,  800.0),
    'direct':      ( -10.0, 1200.0),
    'cloud_cover': (   0.0,  100.0),
}

# Per-field ranges for ENTSO-E load forecast feeds (#28). The collector
# emits load_forecast + load_actual + forecast_error per timestamp per
# country. The prior blanket [0, 100000] route via `load_mw` fired on
# every signed forecast_error (observed -6779 to +18914 MW) which is
# legitimately negative when the forecast over-predicted demand.
# Bounds derived from 104 files Mar-Jun 2026 (65k records):
#   load_forecast  observed [5163, 70295];   bound [0, 100000] (NL+DE peak ~75 GW)
#   load_actual    observed [0,    71296];   bound [0, 100000]
#   forecast_error observed [-6779, 18914];  bound [-20000, 25000]
#
# forecast_error asymmetry (opus L5): -20000/-6779 = 2.95x, +25000/+18914 = 1.32x.
# The asymmetric headroom is intentional, not a derivation artefact:
# we observed only Mar-Jun 2026 — winter peak demand months
# (Dec/Jan/Feb) push positive forecast_error higher because cold-snap
# heating spikes are routinely under-predicted, while over-prediction
# (negative error) is less seasonal. Tightening the positive bound to
# match the negative would risk dawn-cold-snap false positives next
# winter. Re-derive after a full winter cycle if either bound feels off.
LOAD_FIELD_RANGES = {
    'load_forecast':  (    0.0, 100000.0),
    'load_actual':    (    0.0, 100000.0),
    'forecast_error': (-20000.0,  25000.0),
}

# data_type → per-field range registry. When a data_type is registered here,
# `validate_value_ranges` walks each nested-dict sub-key against its own
# range instead of applying a single blanket range. Same pattern weather
# has used since 2026-03; gas_storage joins it via issue #24; hydro_reservoir
# joins it via #3 security review; solar + load join it via #28.
FIELD_RANGES_BY_TYPE = {
    'weather': WEATHER_FIELD_RANGES,
    'gas_storage': GAS_STORAGE_FIELD_RANGES,
    'hydro_reservoir': HYDRO_RESERVOIR_FIELD_RANGES,
    'solar': SOLAR_FIELD_RANGES,
    'load': LOAD_FIELD_RANGES,
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
    # Nordic hydro (#3): weekly cadence × 2 zones (NO/SE) over a 12-week
    # window yields ~20 points after the 2-3 week publication lag. 12 = 6
    # fresh weeks × 2 zones — flags total collection collapse. The
    # per-zone completeness signal (#29) lives inside
    # EntsoeHydroCollector._validate_data and surfaces a half-dark zone
    # (e.g. NO=10 SE=2 = 12 aggregate, passes here) via
    # `collector_quality_issues`, so this aggregate floor stays additive.
    'nordic_hydro': 12,
    # Old collector names (for backtest compatibility)
    'OpenWeather': 8,                # OpenWeather free tier: limited forecast points
    'MeteoServer': 24,
}

# Severity when each dataset is absent from the pipeline. Single source of
# truth — what was previously three parallel registries (REQUIRED_DATASETS /
# EXPECTED_DATASETS / WARNING_IF_MISSING_DATASETS) with overlapping membership
# and implicit precedence is now one dict. Reviewer tier-2 finding on 7c0de64.
#
#   'critical' — pipeline status promotes to 'critical' if this dataset is
#                missing. Downstream forecasts (Augur) materially degrade.
#   'warning'  — known operational issue (e.g. TenneT 422 per #25); the
#                absence promotes status to 'warning' but doesn't block
#                publish.
#   'info'     — expected but routinely flaky upstream sources; absence is
#                noted in missing_datasets but doesn't promote status.
#
# Datasets NOT in this dict trigger no status promotion when missing
# (silent absence) — they're either optional or aren't tracked here yet.
DATASET_MISSING_SEVERITY: Dict[str, str] = {
    'entsoe':         'critical',
    'energy_zero':    'critical',
    'entsoe_de':      'info',
    'epex':           'info',
    'elspot':         'info',
    'grid_imbalance': 'warning',  # TenneT 422 cascade per #25
    'nordic_hydro':   'info',     # #3 — leading indicator, not gate-critical
}

# Back-compat shims: derive the three named lists from the dict so any
# code (tests, other modules) that imported these names continues to work.
# These are read-only — mutate DATASET_MISSING_SEVERITY instead.
REQUIRED_DATASETS = [
    name for name, sev in DATASET_MISSING_SEVERITY.items() if sev == 'critical'
]
EXPECTED_DATASETS = [
    name for name, sev in DATASET_MISSING_SEVERITY.items() if sev in ('critical', 'info')
]
WARNING_IF_MISSING_DATASETS = [
    name for name, sev in DATASET_MISSING_SEVERITY.items() if sev == 'warning'
]


# Consecutive-upstream-empty escalation (#38 review follow-up). An upstream
# data gap is downgraded critical->warning so the pipeline keeps publishing —
# but a *sustained* gap must not degrade silently forever. After this many
# consecutive runs with no upstream data for a feed, it escalates back to a
# hard failure (loud CI alert) instead of a warning.
UPSTREAM_EMPTY_ESCALATION_RUNS = 3


def update_upstream_empty_streaks(
    prior_streaks: Dict[str, int],
    upstream_empty_now: set,
    tracked_feeds,
) -> Dict[str, int]:
    """Advance the consecutive-upstream-empty counters for a run.

    For each tracked feed: increment its streak if it was upstream-empty this
    run, otherwise reset to 0. Pure — the caller handles persistence. Feeds
    that genuinely failed (vs upstream-empty) reset to 0; that case already
    fails the run through the normal missing-critical path.
    """
    new_streaks: Dict[str, int] = {}
    for feed in tracked_feeds:
        if feed in upstream_empty_now:
            new_streaks[feed] = int(prior_streaks.get(feed, 0)) + 1
        else:
            new_streaks[feed] = 0
    return new_streaks


def escalated_upstream_feeds(
    streaks: Dict[str, int],
    threshold: int = UPSTREAM_EMPTY_ESCALATION_RUNS,
) -> set:
    """Feeds whose upstream-empty streak has reached the escalation threshold —
    a sustained gap that should fail the run loudly rather than warn."""
    return {feed for feed, count in streaks.items() if count >= threshold}


def _is_timestamp_str(s: Any) -> bool:
    """True if `s` is a string that parses as ISO date or datetime."""
    try:
        datetime.fromisoformat(str(s).replace('Z', '+00:00'))
        return True
    except (ValueError, TypeError):
        return False


def _count_data_points(data: Any, _max_depth: int = 5) -> int:
    """Count records in a possibly nested data dict.

    Heuristic (timestamp-aware, depth-walking — issue #32):
      - If a dict's keys parse as ISO timestamps, it IS the record
        collection — return its length (each entry is one record,
        whether the value is a scalar or a fields-dict).
      - Otherwise recurse into dict children (up to `_max_depth`) and
        sum their counts. Non-dict children contribute 0.
      - A dict with neither timestamp keys nor dict children is a
        snapshot leaf — count as 1.

    Shapes handled:
      flat:     {ts: scalar}                       -> N (top level)
      flat:     {ts: {field: val}}                 -> N (top level)
      2-level:  {loc: {ts: {field: val}}}          -> sum per loc
      3-level:  {kind: {class: {ts: {field}}}}     -> deeper sum (#32)
      mixed:    {series: {metadata, data: {ts}}}   -> recursed; metadata
                sub-dict adds +1 noise per series, swamped by N >> 1 in
                practice (market_history)
      snapshot: {commodity: {field: val}}          -> one record per
                commodity (market_proxies)
    """
    if not isinstance(data, dict) or _max_depth <= 0 or not data:
        return 0
    keys_sample = list(data.keys())[:5]
    if all(_is_timestamp_str(k) for k in keys_sample):
        return len(data)
    dict_children = [v for v in data.values() if isinstance(v, dict)]
    if not dict_children:
        # Non-empty dict with no dict children and no timestamp keys —
        # a snapshot leaf (e.g. one commodity's fields in market_proxies).
        return 1
    return sum(_count_data_points(child, _max_depth - 1) for child in dict_children)


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


def _flatten_to_timestamp_records(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten a 2-level location/zone-keyed structure into a 1-level
    timestamp-keyed dict so `validate_value_ranges` walks all records.

    Pipeline shapes seen in the wild:
      - 1-level (gas_storage):  {iso_ts: {field: value, ...}, ...}
      - 2-level (weather, nordic_hydro):
            {location_or_zone: {iso_ts: {field: value, ...}, ...}, ...}

    Before this helper, the validator only iterated the outer dict and
    skipped 2-level structures (the inner value was a dict, not numeric),
    silently no-op'ing range checks on weather and the new hydro feed.
    Detected via the #3 security review.

    Heuristic for "is this already flat?": if any of the first few
    top-level keys parses as an ISO timestamp, treat as 1-level and
    return unchanged. Otherwise descend one level.
    """
    for key in list(data.keys())[:3]:
        try:
            datetime.fromisoformat(str(key).replace('Z', '+00:00'))
            return data
        except (ValueError, TypeError):
            continue

    flat: Dict[str, Any] = {}
    for outer_key, inner in data.items():
        if not isinstance(inner, dict):
            continue
        for ts, fields in inner.items():
            flat[f"{outer_key}/{ts}"] = fields
    return flat


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
    field_ranges = FIELD_RANGES_BY_TYPE.get(data_type)
    range_spec = VALUE_RANGES.get(data_type) if field_ranges is None else None

    if field_ranges is None and not range_spec:
        return issues

    out_of_range_count = 0
    out_of_range_examples = []

    for timestamp, value in _flatten_to_timestamp_records(data).items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if not isinstance(sub_value, (int, float)) or sub_value is None:
                    continue
                if field_ranges is not None:
                    if _check_field_range(sub_key, sub_value, field_ranges):
                        out_of_range_count += 1
                        if len(out_of_range_examples) < 5:
                            fr = field_ranges.get(sub_key, ('?', '?'))
                            out_of_range_examples.append(
                                f"{sub_key}={sub_value} (range {fr[0]}-{fr[1]})"
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
        if field_ranges is not None:
            msg = (f"{out_of_range_count} {data_type} field values outside "
                   f"their specific ranges")
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


# Threshold for the load cross-field consistency check (#30). A MITM that
# tampers `load_actual` and `forecast_error` independently can produce a
# triple whose per-field bounds pass (#28) but whose internal arithmetic
# is broken — e.g. (load_forecast=70000, load_actual=50000, forecast_error=20000)
# implies a forecast error of 40% of actual load, which is physically
# absurd. Derivation: max observed |forecast_error|/load_actual over the
# Mar-Jun 2026 sample = 18914/70295 = 0.27. 0.40 gives ~48% headroom over
# the worst observed. Re-derive after a full winter cycle if cold-snap
# under-prediction stretches the legit ceiling closer to 0.40.
LOAD_CROSS_FIELD_RATIO_THRESHOLD = 0.4

# Denominator floor (MW) for the load consistency ratio. Prevents
# division blow-up when load_actual is near zero (e.g. early-AM minimum,
# a missing record, or a tampered zero). Below this floor we still
# evaluate the check but use 1000 MW so a small absolute |forecast_error|
# doesn't false-flag.
LOAD_CROSS_FIELD_ACTUAL_FLOOR = 1000.0


def validate_load_cross_field_consistency(
    data: Dict[str, Any],
    dataset_name: str,
) -> List[QualityIssue]:
    """
    Cross-field consistency check for the load triple (#30).

    `forecast_error` should be a small fraction of `load_actual`. If
    |forecast_error| / max(|load_actual|, floor) exceeds
    LOAD_CROSS_FIELD_RATIO_THRESHOLD, the record is internally inconsistent
    — either upstream arithmetic is broken or the values were tampered
    independently. Defends against the MITM scenario where the attacker
    flips multiple fields to a triple whose per-field bounds (#28) all pass
    but whose ratio gives the game away.

    Args:
        data:         The load data (1- or 2-level nested by country/timestamp)
        dataset_name: For reporting

    Returns:
        List of QualityIssue. Empty when all records are internally consistent.
    """
    issues: List[QualityIssue] = []
    inconsistent_count = 0
    examples: List[str] = []

    for ts_key, record in _flatten_to_timestamp_records(data).items():
        if not isinstance(record, dict):
            continue
        load_actual = record.get('load_actual')
        forecast_error = record.get('forecast_error')
        if not isinstance(load_actual, (int, float)):
            continue
        if not isinstance(forecast_error, (int, float)):
            continue
        denom = max(abs(load_actual), LOAD_CROSS_FIELD_ACTUAL_FLOOR)
        ratio = abs(forecast_error) / denom
        # Inclusive boundary: the acceptance triple
        # (load_actual=50000, forecast_error=20000) is exactly 0.40 and
        # must flag (#30). The threshold reads as "40% or more is bad".
        if ratio >= LOAD_CROSS_FIELD_RATIO_THRESHOLD:
            inconsistent_count += 1
            if len(examples) < 5:
                examples.append(
                    f"{ts_key}: forecast_error={forecast_error}, "
                    f"load_actual={load_actual}, ratio={ratio:.2f}"
                )

    if inconsistent_count > 0:
        issues.append(QualityIssue(
            check_name='load_cross_field_consistency',
            severity=Severity.WARNING,
            message=(
                f"{inconsistent_count} load records have "
                f"|forecast_error|/load_actual > "
                f"{LOAD_CROSS_FIELD_RATIO_THRESHOLD:.0%} — possible "
                "field-tampered values or broken upstream arithmetic"
            ),
            details={
                'count': inconsistent_count,
                'examples': examples,
                'threshold': LOAD_CROSS_FIELD_RATIO_THRESHOLD,
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


def _extract_timestamp_keys(data: Dict[str, Any], _max_depth: int = 5) -> List[str]:
    """
    Extract timestamp-like keys from data, walking deeper when needed.

    Many datasets nest timestamps under country/location keys, e.g.:
    {'NL': {'2026-03-30T00:00:00+02:00': {...}}, 'DE_LU': {...}}

    Some go deeper (issue #32): ned_production has
    {energy_type: {forecast|actual: {ts: {fields}}}} and market_history
    has {series: {metadata, data: {date: price}}}. Walking only 1 level
    deep missed those.

    Returns:
        List of string keys that look like ISO timestamps. The first
        sub-tree containing timestamps is returned (breadth-first), so
        staleness only inspects one consistent layer of the data.
    """
    if not isinstance(data, dict) or _max_depth <= 0:
        return []
    keys = [k for k in data.keys() if _is_timestamp_str(k)]
    if keys:
        return keys
    for v in data.values():
        if isinstance(v, dict):
            child_keys = _extract_timestamp_keys(v, _max_depth - 1)
            if child_keys:
                return child_keys
    return []


def validate_staleness(
    data: Dict[str, Any],
    max_age_hours: int = 48,
    metadata_anchor_iso: Optional[str] = None,
) -> List[QualityIssue]:
    """
    Check that data is not stale (all timestamps too old).

    Args:
        data: The data dictionary with ISO timestamp keys
        max_age_hours: Maximum acceptable age of newest data point
        metadata_anchor_iso: Optional ISO timestamp from dataset metadata
            (e.g. metadata['end_time']) used as fallback when the data
            shape carries no timestamp-keyed dict — typical for snapshot
            datasets like market_proxies. Without this, snapshots fire
            a misleading "Could not parse any timestamps" warning every
            run (issue #32).

    Returns:
        List of QualityIssue objects
    """
    issues = []
    if not data:
        return issues

    # Find the most recent timestamp
    now = datetime.now().astimezone()
    newest_ts = None

    for key in _extract_timestamp_keys(data):
        try:
            dt = datetime.fromisoformat(str(key).replace('Z', '+00:00'))
            # Date-only ISO (e.g. '2026-06-09', used by market_history)
            # parses as naive — assume Amsterdam wall-clock so the
            # subsequent `now - dt` doesn't raise TypeError. Same anchor
            # convention as `_extract_timestamp_keys`.
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=now.tzinfo)
            if newest_ts is None or dt > newest_ts:
                newest_ts = dt
        except (ValueError, TypeError):
            continue

    if newest_ts is None and metadata_anchor_iso:
        # Snapshot-shape datasets (no timestamp-keyed dict in data) fall
        # back to the metadata anchor — usually metadata['end_time'].
        try:
            newest_ts = datetime.fromisoformat(
                str(metadata_anchor_iso).replace('Z', '+00:00')
            )
            if newest_ts.tzinfo is None:
                newest_ts = newest_ts.replace(tzinfo=now.tzinfo)
        except (ValueError, TypeError):
            newest_ts = None

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
    for key in _extract_timestamp_keys(data):
        try:
            dt = datetime.fromisoformat(str(key).replace('Z', '+00:00'))
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
#
# NOTE: 'weather' and 'gas_storage' are SENTINEL keys (their values match
# themselves, not VALUE_RANGES entries). validate_value_ranges checks
# FIELD_RANGES_BY_TYPE first and routes those types to per-field validation
# instead of a blanket scalar range. Adding a `VALUE_RANGES['gas_storage']`
# entry would be unreachable — the sentinel is what activates the per-field
# path. Same for 'weather'.
DATA_TYPE_RANGE_MAP = {
    'energy_price': 'energy_price',
    'wind_generation': 'generation_mw',
    'wind_weather': 'wind_speed',
    'solar': 'solar',                    # routed via FIELD_RANGES_BY_TYPE (#28)
    'weather': 'weather',                # routed via FIELD_RANGES_BY_TYPE
    'load': 'load',                      # routed via FIELD_RANGES_BY_TYPE (#28)
    'generation': 'generation_mw',
    'grid_imbalance': 'energy_price',    # Imbalance prices in EUR/MWh
    'cross_border_flows': 'flow_mw',
    'gas_storage': 'gas_storage',        # routed via FIELD_RANGES_BY_TYPE
    'hydro_reservoir': 'hydro_reservoir',  # routed via FIELD_RANGES_BY_TYPE
    # Aliases for collector-emission data_types that aren't pinned by
    # EXPECTED_DATA_TYPE (opus M2 belt-and-suspenders). EXPECTED_DATA_TYPE
    # already rewrites solar_forecast/load_forecast to canonical 'solar'/
    # 'load' for the pipeline, but a future-added solar/load dataset
    # NOT in EXPECTED_DATA_TYPE would otherwise get zero range validation
    # because DATA_TYPE_RANGE_MAP.get('solar_irradiance') was None.
    'solar_irradiance': 'solar',
    'load_forecast':    'load',
}

# Invariant (refactoring-guide H2): every DATA_TYPE_RANGE_MAP entry whose
# value matches a key in FIELD_RANGES_BY_TYPE must actually exist there.
# Without this, a typo or stale entry silently no-ops `validate_value_ranges`
# on that data_type at production time. Module-load assert means a missing
# entry is caught at import (test-collection), not at first poisoned value.
_FIELD_RANGE_TARGETS = {
    v for v in DATA_TYPE_RANGE_MAP.values() if v in FIELD_RANGES_BY_TYPE
}
_DECLARED_FIELD_RANGE_TYPES = set(FIELD_RANGES_BY_TYPE.keys())
_routed = {v for v in DATA_TYPE_RANGE_MAP.values()
           if v not in VALUE_RANGES and v in FIELD_RANGES_BY_TYPE}
# Every routed-to-FIELD_RANGES target must have an entry. Conversely, any
# self-mapping data_type ('solar' -> 'solar') that's NOT in either registry
# would silently skip validation — also catch that here.
_self_mapped = {k for k, v in DATA_TYPE_RANGE_MAP.items() if k == v}
_self_mapped_missing = _self_mapped - _DECLARED_FIELD_RANGE_TYPES - set(VALUE_RANGES.keys())
assert not _self_mapped_missing, (
    f"DATA_TYPE_RANGE_MAP self-mapped entries lack FIELD_RANGES_BY_TYPE/"
    f"VALUE_RANGES coverage: {_self_mapped_missing}. Either add the field "
    "ranges or change the mapping target."
)
del _FIELD_RANGE_TARGETS, _DECLARED_FIELD_RANGE_TYPES, _routed
del _self_mapped, _self_mapped_missing

# Per-dataset staleness threshold overrides (hours). The default 48h applies
# to most feeds; GIE AGSI+ publishes with a documented 2-3 day lag, so 96h
# is the right floor before flagging (issue #24).
#
# Note the parallel-registry pattern: STALENESS_OVERRIDES is keyed by
# dataset_name (pipeline truth) while FIELD_RANGES_BY_TYPE is keyed by
# data_type (semantic). For datasets where these coincide (gas_storage)
# this is invisible; for the future case where they don't, look up via
# `get_dataset_validation_config()` below — that function is the single
# point of truth for "what does the pipeline know about this dataset?".
STALENESS_OVERRIDES = {
    'gas_storage': 96,
    # ENTSO-E A72 Nordic hydro reservoirs publish weekly with a 2-3 week
    # publication lag, so the default 48h threshold would always trip.
    # 28 days (672h) flags only genuinely stale data while accommodating
    # normal publication delays and bank-holiday weeks.
    'nordic_hydro': 672,
}


# Expected data_type per pipeline dataset_name. When the pipeline's dataset
# name is one of these keys, the expected data_type is the source of truth
# for validation routing — even if metadata.data_type from the upstream API
# says otherwise. Defends against MITM-controlled metadata steering
# validation to a more lenient field-range registry (security audit MEDIUM,
# CWE-20). Datasets whose name is not in this map fall back to trusting
# metadata.data_type (no expected-type pin).
EXPECTED_DATA_TYPE = {
    # Energy-price feeds (highest-value MITM target — spoofing data_type
    # could route EUR/MWh values through weather field-range validation
    # and silently pass nonsense). Security audit MEDIUM CWE-20 on
    # 7c0de64. The five price feeds share the same data_type because
    # they're separate sub-sources of `energy_price_forecast`.
    'entsoe':                          'energy_price',
    'entsoe_de':                       'energy_price',
    'energy_zero':                     'energy_price',
    'epex':                            'energy_price',
    'elspot':                          'energy_price',
    # Existing entries (a74f662). Note: some legacy entries below pin to
    # validation-routing aliases ('weather' / 'solar' / 'load' / 'generation')
    # rather than the actual collector emission ('demand_weather' /
    # 'solar_irradiance' / 'load_forecast' / 'generation_by_type'). The
    # aliases keep range validation routed correctly through
    # DATA_TYPE_RANGE_MAP at the cost of a warning per run. Separate cleanup
    # to align the collectors with the validation registry is a follow-up.
    'gas_storage':                     'gas_storage',
    'weather_forecast_multi_location': 'weather',
    'weather_forecast_buurt':          'weather',
    'demand_weather_forecast':         'weather',
    'solar_forecast':                  'solar',
    'solar_forecast_buurt':            'solar',
    'grid_imbalance':                  'grid_imbalance',
    'cross_border_flows':              'cross_border_flows',
    'load_forecast':                   'load',
    'generation_forecast':             'generation',
    'generation_mix':                  'generation',
    # Newly pinned feeds (reviewer follow-up to 7c0de64). These match the
    # actual collector emissions so no mismatch warning fires; they pin
    # the data_type so a MITM-spoofed metadata.data_type cannot redirect
    # validation. Where the legit type has no DATA_TYPE_RANGE_MAP entry
    # (offshore_wind, energy_production, etc.), pinning still defends
    # against the "spoof to weather for lenient validation" attack vector.
    'wind_forecast':                   'wind_generation',
    'offshore_wind':                   'offshore_wind',
    'ned_production':                  'energy_production',
    'market_proxies':                  'market_proxies',
    'market_history':                  'market_history',
    'gas_flows':                       'gas_flows',
    'air_quality_buurt':               'air',
    'nordic_hydro':                    'hydro_reservoir',
}


def get_dataset_validation_config(
    dataset_name: str,
    data_type: str,
) -> Dict[str, Any]:
    """
    Single lookup point for per-dataset validation config.

    Internally consults FIELD_RANGES_BY_TYPE (keyed by data_type, since
    field semantics are per-type) and STALENESS_OVERRIDES (keyed by
    dataset_name, since publish-cadence overrides are per-dataset). When
    debugging "what does the pipeline know about <dataset>?", this is the
    single function to call (refactoring review tier-2 finding #8).

    Args:
        dataset_name: Pipeline identifier (e.g. 'gas_storage', 'weather_forecast_buurt')
        data_type:    Semantic type from metadata (e.g. 'gas_storage', 'weather')

    Returns:
        Dict with keys:
          field_ranges    - per-field (min, max) dict or None if no per-field config
          staleness_hours - max age in hours (default 48)
    """
    return {
        'field_ranges': FIELD_RANGES_BY_TYPE.get(data_type),
        'staleness_hours': STALENESS_OVERRIDES.get(dataset_name, 48),
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
    raw_data_type = dataset.metadata.get('data_type', 'unknown')
    source = dataset.metadata.get('source', 'unknown')
    data_points = _count_data_points(dataset.data)

    # Security defense (CWE-20): if the pipeline registers an expected
    # data_type for this dataset name, prefer it over the upstream-provided
    # metadata.data_type. Prevents an attacker MITMing the source API and
    # setting metadata.data_type='weather' on a non-weather payload to
    # steer the validator to a more lenient field-range registry. We log
    # a warning so the mismatch is visible in the run log.
    expected = EXPECTED_DATA_TYPE.get(dataset_name)
    if expected is not None and raw_data_type != expected:
        logger.warning(
            f"[DQ:{dataset_name}] metadata.data_type='{raw_data_type}' "
            f"does not match registered expected '{expected}' — using "
            f"expected for validation routing"
        )
        data_type = expected
    else:
        data_type = raw_data_type

    # Single point of truth for per-dataset config (#8): one call covers
    # field_ranges + staleness so future per-dataset config additions
    # have one place to land.
    config = get_dataset_validation_config(dataset_name, data_type)

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

    # 3b. Load cross-field consistency (#30). Defence-in-depth beyond
    # the per-field bounds (#28): a MITM can stay inside per-field
    # bounds while tampering load_actual + forecast_error to an
    # internally-inconsistent triple.
    if data_type == 'load':
        checks_run += 1
        cross_issues = validate_load_cross_field_consistency(
            dataset.data, dataset_name
        )
        if cross_issues:
            checks_failed += 1
            report.issues.extend(cross_issues)

    # 4. Staleness check. Snapshot datasets (e.g. market_proxies) have
    # no timestamp-keyed dict; fall back to metadata's end/start_time so
    # they don't fire a misleading "could not parse" warning (#32).
    checks_run += 1
    staleness_anchor = (
        dataset.metadata.get('end_time') or dataset.metadata.get('start_time')
    )
    staleness_issues = validate_staleness(
        dataset.data,
        max_age_hours=config['staleness_hours'],
        metadata_anchor_iso=staleness_anchor,
    )
    if staleness_issues:
        checks_failed += 1
        report.issues.extend(staleness_issues)

    # 5. Duplicate timestamp check
    checks_run += 1
    dup_issues = validate_duplicate_timestamps(dataset.data)
    if dup_issues:
        checks_failed += 1
        report.issues.extend(dup_issues)

    # 6. Collector-emitted quality issues — surface anything the collector
    # itself flagged in metadata['collector_quality_issues'] so domain-specific
    # signals (e.g. Luchtmeetnet station_completeness, issue #12) flow into the
    # same pipeline-level gate as the generic checks above.
    for issue_dict in dataset.metadata.get('collector_quality_issues', []) or []:
        checks_run += 1
        try:
            severity = Severity(issue_dict['severity'])
        except (KeyError, ValueError):
            # Unknown / missing severity → conservative downgrade to WARNING
            # so a malformed collector emission doesn't silently look benign.
            severity = Severity.WARNING
        # Only count toward checks_failed when severity warrants attention.
        # INFO-severity collector signals are informational only and must
        # not inflate the failed-checks count (PR #16 review MEDIUM-1).
        if severity in (Severity.WARNING, Severity.ERROR, Severity.CRITICAL):
            checks_failed += 1
        report.issues.append(QualityIssue(
            check_name=issue_dict.get('check_name', 'collector_issue'),
            severity=severity,
            message=issue_dict.get('message', ''),
            details=issue_dict.get('details'),
        ))

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
    upstream_empty: Optional[set] = None,
) -> PipelineQualityReport:
    """
    Run quality checks on all datasets from a pipeline run.

    Args:
        datasets: Dict mapping dataset names to EnhancedDataSet (or None if failed)
        upstream_empty: Optional set of dataset names that are missing because
            the upstream source published no data for the window (as opposed to
            a collector/API failure). A critical dataset that is merely
            upstream-empty is downgraded to a 'warning' so a temporary source
            outage doesn't block publishing the healthy feeds. See
            collectors.base.UpstreamNoDataError.

    Returns:
        PipelineQualityReport with all findings
    """
    upstream_empty = set(upstream_empty or ())
    report = PipelineQualityReport(
        timestamp=datetime.now().astimezone().isoformat(),
    )

    # Single pass over the consolidated severity registry. Log level
    # tracks severity so an absent critical dataset is flagged as a
    # warning in the operator log, while an info-severity flake is
    # quietly noted. Upstream-empty datasets are logged distinctly so the
    # operator sees "source gap" rather than "collector broke".
    _missing_log = {
        'critical': logger.warning,
        'warning':  logger.warning,
        'info':     logger.info,
    }
    for name, severity in DATASET_MISSING_SEVERITY.items():
        if name in datasets and datasets[name] is not None:
            continue
        report.missing_datasets.append(name)
        if name in upstream_empty:
            report.upstream_empty_datasets.append(name)
            logger.warning(
                f"[DQ:pipeline] Dataset '{name}' is missing — upstream "
                f"published no data for the window (severity={severity}, "
                f"downgraded to warning; healthy feeds still publish)"
            )
            continue
        log_fn = _missing_log.get(severity, logger.info)
        log_fn(
            f"[DQ:pipeline] Dataset '{name}' is missing "
            f"(severity={severity})"
        )

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
