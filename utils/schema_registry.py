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
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

# Current schema version
CURRENT_SCHEMA_VERSION = "2.1"

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
}


def get_current_version() -> str:
    """Get the current schema version string."""
    return CURRENT_SCHEMA_VERSION


def get_changelog() -> Dict[str, Any]:
    """Get the full schema changelog."""
    return SCHEMA_CHANGELOG


def stamp_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add schema_version to metadata dict.

    This should be called when creating any EnhancedDataSet to ensure
    the version is recorded. Idempotent — won't overwrite existing version.

    Args:
        metadata: The metadata dict to stamp

    Returns:
        The metadata dict with schema_version added
    """
    if 'schema_version' not in metadata:
        metadata['schema_version'] = CURRENT_SCHEMA_VERSION
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


# Migration path: ordered list of (from_version, to_version, migration_func)
MIGRATIONS = [
    ('1.0', '2.0', _migrate_1_to_2),
    ('2.0', '2.1', _migrate_2_to_2_1),
]


def migrate_to_current(
    data: Dict[str, Any],
    filename: str = '',
) -> Dict[str, Any]:
    """
    Migrate data from any detected version to the current schema.

    Applies migrations sequentially: 1.0 -> 2.0 -> 2.1 -> ...

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
