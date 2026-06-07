"""
Schema Registry and Migration Framework
-----------------------------------------
Tracks schema versions for energyDataHub output files and provides
backward-compatible reading of historical data.

The students' main frustration was that format changes broke their parsers.
This module ensures:
1. Every output file embeds its schema version
2. A changelog documents what changed between versions
3. Old files can be read and normalized to the current schema

File: utils/schema_registry.py
Created: 2026-03-09
Author: Energy Data Hub Project

Schema Version History:
    1.0 - Original format (pre-Oct 2025). Inconsistent structures,
          no metadata, mixed timestamp formats.
    2.0 - Standardized format (Oct 2025). Added metadata dict,
          CombinedDataSet with version field, normalized timestamps.
    2.1 - Added DST features to calendar_features, added data_quality_report.json,
          added schema_version to all metadata. (Mar 2026)
    2.2 - Homogenised strategic-feed envelope: energy_price_forecast and
          wind_forecast wrapped in canonical {metadata, data} (was flat
          {version, src1, src2, ...}). Sibling to PR #20 buurt-air. (Jun 2026)
    2.3 - Renamed gas_storage field `working_capacity_twh` → `gas_in_storage_twh`
          for semantic accuracy (it always held the current stored volume,
          not the working capacity). Tightened gas_storage range validators
          based on observed NL distributions. (Jun 2026)
"""

import copy
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

# Current schema version
CURRENT_SCHEMA_VERSION = "2.3"

# Schema changelog - documents what changed in each version
SCHEMA_CHANGELOG = {
    "1.0": {
        "date": "2024-10-19",
        "description": "Original format",
        "changes": [
            "Initial data collection format",
            "No standardized metadata",
            "Mixed timestamp formats (some UTC, some local)",
            "Inconsistent field naming across collectors",
        ],
    },
    "2.0": {
        "date": "2025-10-25",
        "description": "Standardized format with BaseCollector architecture",
        "changes": [
            "Added metadata dict to all datasets (data_type, source, units)",
            "CombinedDataSet wrapper with version field",
            "All timestamps normalized to Europe/Amsterdam",
            "Data validation with convert_value() for NaN/Infinity/nulls",
            "Structured as {version, dataset_name: {metadata, data}}",
        ],
    },
    "2.1": {
        "date": "2026-03-09",
        "description": "DST features, data quality, schema versioning",
        "changes": [
            "Added schema_version to all dataset metadata",
            "Added DST features to calendar_features (is_dst, is_dst_transition_day, dst_utc_offset_hours)",
            "Added data_quality_report.json output",
            "Added schema_registry for backward-compatible reading",
        ],
    },
    "2.2": {
        "date": "2026-06-07",
        "description": "Strategic-feed envelope homogenisation (issue #26)",
        "changes": [
            "CombinedDataSet.to_dict() now wraps in canonical {metadata, data}",
            "energy_price_forecast.json moved per-collector keys under top-level `data`",
            "wind_forecast.json moved per-collector keys under top-level `data`",
            "Sibling to PR #20 buurt-air envelope homogenisation",
            "Breaking for consumers reading payload['entsoe'] etc. — use payload['data']['entsoe']",
        ],
    },
    "2.3": {
        "date": "2026-06-07",
        "description": "gas_storage field rename + range tightening (reviewer hotfix)",
        "changes": [
            "gas_storage field `working_capacity_twh` renamed to `gas_in_storage_twh`",
            "  (semantic fix: the field always held current stored volume, not capacity)",
            "GAS_STORAGE_FIELD_RANGES tightened to NL-realistic bounds (was ~10-26x loose)",
            "data_quality.py comment corrected from EU-aggregate to NL-only scope",
            "Auto-migrates historical v2.x gas_storage files via _migrate_2_2_to_2_3",
        ],
    },
}


def get_current_version() -> str:
    """Get the current schema version string."""
    return CURRENT_SCHEMA_VERSION


def get_changelog() -> Dict[str, Any]:
    """Get the full schema changelog."""
    return SCHEMA_CHANGELOG


def stamp_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add schema_version + the corresponding changelog entry to a metadata dict.

    Layer B of the schema-drift defense (#27): every published file carries
    not just the version string but the human-readable changelog slice for
    that version. Downstream consumers can compare `metadata.schema_version`
    against their last-seen version and, when it bumps, read
    `metadata.schema_changelog_entry` for guidance — no need to fetch the
    repo CHANGELOG separately.

    Idempotent: won't overwrite existing fields. If a caller supplies a
    pre-stamped `schema_version`, the changelog entry is looked up for
    *that* version (not CURRENT) so migrations preserve the lineage.

    Args:
        metadata: The metadata dict to stamp

    Returns:
        The same dict, mutated in place, with `schema_version` and (when
        a changelog entry exists for that version) `schema_changelog_entry`.
    """
    if 'schema_version' not in metadata:
        metadata['schema_version'] = CURRENT_SCHEMA_VERSION
    if 'schema_changelog_entry' not in metadata:
        version = metadata['schema_version']
        entry = SCHEMA_CHANGELOG.get(version)
        if entry is not None:
            # Deep copy so a downstream mutation of the metadata's slice
            # (e.g. appending to the inner `changes` list) can't poison
            # the module-level changelog.
            metadata['schema_changelog_entry'] = copy.deepcopy(entry)
    return metadata


def detect_version(data: Dict[str, Any]) -> str:
    """
    Detect the schema version of a loaded JSON file.

    Detection logic:
    - Has 'version' key at top level -> v2.0+
    - Has nested dataset with metadata containing schema_version -> use that
    - Has nested dataset with metadata but no schema_version -> v2.0
    - No metadata structure -> v1.0

    Args:
        data: The parsed JSON data

    Returns:
        Detected version string
    """
    # Check for explicit version field (CombinedDataSet format)
    top_version = data.get('version')

    # Look for schema_version in any nested dataset metadata
    for key, value in data.items():
        if key in ('version', 'metadata', 'data'):
            continue
        if isinstance(value, dict) and 'metadata' in value:
            meta = value['metadata']
            if isinstance(meta, dict) and 'schema_version' in meta:
                return meta['schema_version']

    # Has version field but no schema_version in metadata -> v2.0
    if top_version == '2.0':
        return '2.0'

    # Check for v2.0 structure: either has top-level 'version' or nested datasets
    # with 'metadata'+'data' structure (but not a standalone metadata/data pair
    # which is also valid v2.0)
    if 'metadata' in data and 'data' in data and isinstance(data.get('metadata'), dict):
        # Standalone EnhancedDataSet format (v2.0)
        meta = data['metadata']
        if 'schema_version' in meta:
            return meta['schema_version']
        if 'data_type' in meta or 'source' in meta:
            return '2.0'

    for key, value in data.items():
        if key in ('version', 'metadata', 'data'):
            continue
        if isinstance(value, dict) and 'metadata' in value:
            return '2.0'

    # No recognizable structure -> v1.0
    return '1.0'


def _migrate_1_to_2(data: Dict[str, Any], filename: str = '') -> Dict[str, Any]:
    """
    Migrate v1.0 data to v2.0 format.

    v1.0 files were raw key-value pairs without metadata wrapper.
    We wrap them in the v2.0 structure with inferred metadata.

    Args:
        data: Raw v1.0 data
        filename: Original filename (used to infer data type)

    Returns:
        Data in v2.0 format
    """
    # Infer data type from filename
    data_type = 'unknown'
    if 'energy_price' in filename:
        data_type = 'energy_price'
    elif 'weather' in filename:
        data_type = 'weather'
    elif 'sun' in filename:
        data_type = 'sun'
    elif 'air' in filename:
        data_type = 'air'
    elif 'wind' in filename:
        data_type = 'wind_weather'

    return {
        'version': '2.0',
        'migrated_data': {
            'metadata': {
                'data_type': data_type,
                'source': 'unknown (migrated from v1.0)',
                'units': 'unknown',
                'migrated_from': '1.0',
                'original_filename': filename,
            },
            'data': data,
        },
    }


def _migrate_2_to_2_1(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migrate v2.0 data to v2.1 format.

    Changes: adds schema_version to metadata of each dataset.
    Calendar features datasets will be missing DST fields — we add defaults.

    Args:
        data: v2.0 format data

    Returns:
        Data in v2.1 format
    """
    # Update top-level version
    data['version'] = '2.1'

    # Stamp all dataset metadata with schema_version
    for key, value in data.items():
        if key == 'version':
            continue
        if isinstance(value, dict) and 'metadata' in value:
            value['metadata']['schema_version'] = '2.1'
            value['metadata'].setdefault('migrated_from', '2.0')

            # Add default DST fields to calendar features
            if value['metadata'].get('data_type') == 'calendar_features':
                if 'data' in value and isinstance(value['data'], dict):
                    for ts, features in value['data'].items():
                        if isinstance(features, dict):
                            features.setdefault('is_dst', None)
                            features.setdefault('is_dst_transition_day', None)
                            features.setdefault('dst_utc_offset_hours', None)

    return data


def _migrate_2_1_to_2_2(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migrate v2.1 data to v2.2 format.

    v2.1 CombinedDataSet files were `{version, src1: {metadata, data}, ...}`.
    v2.2 wraps them as `{metadata, data: {src1: {metadata, data}, ...}}`.
    Standalone EnhancedDataSet files (already `{metadata, data}`) just need
    their `schema_version` bumped.

    Args:
        data: v2.1 format data

    Returns:
        Data in v2.2 format
    """
    # Standalone EnhancedDataSet: already canonical, just bump the stamp.
    if (isinstance(data.get('metadata'), dict) and 'data' in data
            and 'version' not in data):
        data['metadata']['schema_version'] = '2.2'
        data['metadata'].setdefault('migrated_from', '2.1')
        return data

    # Legacy flat CombinedDataSet: extract per-source datasets, wrap in
    # canonical envelope. The top-level `version` field carried the
    # CombinedDataSet format version (typically "2.0"); preserve it inside
    # the new metadata for downstream parity with freshly-produced files.
    if 'version' in data:
        version_val = data.pop('version', '2.0')
        sources = {
            k: v for k, v in data.items()
            if isinstance(v, dict) and 'metadata' in v and 'data' in v
        }
        # Bump each per-collector metadata to v2.2 too, so consumers walking
        # `data['<source>']['metadata']['schema_version']` see the same
        # version as the envelope. Mirrors what stamp_metadata produces for
        # freshly-stamped CombinedDataSet output.
        for sub in sources.values():
            sub['metadata']['schema_version'] = '2.2'
            sub['metadata'].setdefault('migrated_from', '2.1')
        return {
            'metadata': {
                'schema_version': '2.2',
                'version': version_val,
                'source': 'aggregated',
                'data_type': 'combined',
                'units': 'mixed',
                'migrated_from': '2.1',
            },
            'data': sources,
        }

    # Unrecognised shape — no-op, return as-is.
    return data


def _migrate_2_2_to_2_3(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migrate v2.2 data to v2.3 format.

    Renames the gas_storage field `working_capacity_twh` to `gas_in_storage_twh`
    inside the nested per-timestamp payloads. Other dataset types are unaffected
    (the rename is scoped to gas_storage by checking data_type in metadata).

    Args:
        data: v2.2 format data

    Returns:
        Data in v2.3 format (renamed in place + version bumped)
    """
    def _rename_in_section(section: Dict[str, Any]) -> None:
        """Rename the gas_storage field inside one {metadata, data} section."""
        meta = section.get('metadata') if isinstance(section, dict) else None
        if not isinstance(meta, dict):
            return
        if meta.get('data_type') != 'gas_storage':
            return
        inner = section.get('data')
        if not isinstance(inner, dict):
            return
        for ts_key, point in inner.items():
            if isinstance(point, dict) and 'working_capacity_twh' in point:
                point['gas_in_storage_twh'] = point.pop('working_capacity_twh')
        meta['schema_version'] = '2.3'
        meta.setdefault('migrated_from', '2.2')

    # Canonical envelope: {metadata, data: {src: {metadata, data}}}
    if (isinstance(data.get('metadata'), dict)
            and isinstance(data.get('data'), dict)):
        # Standalone EnhancedDataSet (gas_storage published this way)
        _rename_in_section(data)
        # Multi-collector wrap: walk each per-collector section too
        for sub in data['data'].values():
            if isinstance(sub, dict) and 'metadata' in sub and 'data' in sub:
                _rename_in_section(sub)
        # Bump envelope stamp regardless of whether any rename hit
        data['metadata']['schema_version'] = '2.3'
        return data

    # Legacy flat (shouldn't appear at v2.2, but be defensive)
    for key, section in data.items():
        if isinstance(section, dict) and 'metadata' in section and 'data' in section:
            _rename_in_section(section)
    return data


# Migration path: ordered list of (from_version, to_version, migration_func)
MIGRATIONS = [
    ('1.0', '2.0', _migrate_1_to_2),
    ('2.0', '2.1', _migrate_2_to_2_1),
    ('2.1', '2.2', _migrate_2_1_to_2_2),
    ('2.2', '2.3', _migrate_2_2_to_2_3),
]


def migrate_to_current(
    data: Dict[str, Any],
    filename: str = '',
) -> Dict[str, Any]:
    """
    Migrate data from any detected version to the current schema.

    Applies migrations sequentially: 1.0 -> 2.0 -> 2.1 -> ...

    Contract: the caller's input dict is treated as immutable. The current
    version pass-through returns the original (no work needed); all other
    paths return a freshly deep-copied object. Individual migration
    functions may mutate freely because they only see the local copy.
    This makes the mutation semantics uniform across the legacy-flat /
    standalone / wrapped branches of `_migrate_2_1_to_2_2` and friends
    (reviewer finding HIGH on 3dfc7fb).

    Args:
        data: The parsed JSON data (any version)
        filename: Original filename (for v1.0 migration context)

    Returns:
        Data migrated to current schema version
    """
    version = detect_version(data)

    if version == CURRENT_SCHEMA_VERSION:
        return data

    logger.info(f"Migrating data from v{version} to v{CURRENT_SCHEMA_VERSION}")

    # Deep-copy so individual migration functions can mutate freely without
    # corrupting the caller's reference. Each migration func returns a dict;
    # the chain reassigns, but the original input remains pristine for any
    # caller that wants to retain a pre-migration baseline (e.g. for diff logs).
    data = copy.deepcopy(data)

    for from_ver, to_ver, migrate_func in MIGRATIONS:
        if version <= from_ver:
            if from_ver == '1.0':
                data = migrate_func(data, filename)
            else:
                data = migrate_func(data)
            logger.debug(f"Applied migration v{from_ver} -> v{to_ver}")

    return data


def read_json_file(
    filepath: str,
    migrate: bool = True,
) -> Dict[str, Any]:
    """
    Read a JSON data file with automatic version detection and migration.

    This is the recommended way to read historical energyDataHub files.
    It handles all schema versions transparently.

    Args:
        filepath: Path to the JSON file
        migrate: If True, migrate to current schema (default True)

    Returns:
        Parsed and optionally migrated data dict

    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
    """
    with open(filepath, 'r') as f:
        data = json.load(f)

    if not migrate:
        return data

    filename = filepath.rsplit('/', 1)[-1].rsplit('\\', 1)[-1]
    return migrate_to_current(data, filename=filename)
