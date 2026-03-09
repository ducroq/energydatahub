"""
Tests for Schema Registry and Migration Framework
---------------------------------------------------
Validates version detection, migration paths, and backward compatibility.

Test scenarios:
- Version detection for v1.0, v2.0, v2.1 data
- Migration v1.0 -> v2.1 (full path)
- Migration v2.0 -> v2.1
- No migration needed for current version
- stamp_metadata auto-populates schema_version
- EnhancedDataSet auto-stamps metadata
- Calendar features migration adds DST defaults
- read_json_file with migrate flag
"""

import pytest
import json
import os
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

from utils.schema_registry import (
    CURRENT_SCHEMA_VERSION,
    detect_version,
    stamp_metadata,
    migrate_to_current,
    read_json_file,
    get_current_version,
    get_changelog,
    _migrate_1_to_2,
    _migrate_2_to_2_1,
)
from utils.data_types import EnhancedDataSet, CombinedDataSet


class TestDetectVersion:
    """Tests for detect_version."""

    def test_detect_v1_raw_data(self):
        """v1.0 data has no metadata or version field."""
        data = {
            '2024-09-15T00:00:00': 50.5,
            '2024-09-15T01:00:00': 55.2,
        }
        assert detect_version(data) == '1.0'

    def test_detect_v2_combined_dataset(self):
        """v2.0 data has version field and metadata but no schema_version."""
        data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {
                    'data_type': 'energy_price',
                    'source': 'ENTSO-E',
                    'units': 'EUR/MWh',
                },
                'data': {'2025-01-15T00:00:00+01:00': 50.5},
            },
        }
        assert detect_version(data) == '2.0'

    def test_detect_v2_1_with_schema_version(self):
        """v2.1 data has schema_version in metadata."""
        data = {
            'version': '2.1',
            'entsoe': {
                'metadata': {
                    'data_type': 'energy_price',
                    'source': 'ENTSO-E',
                    'units': 'EUR/MWh',
                    'schema_version': '2.1',
                },
                'data': {'2026-01-15T00:00:00+01:00': 50.5},
            },
        }
        assert detect_version(data) == '2.1'

    def test_detect_v2_standalone_dataset(self):
        """v2.0 standalone dataset (not combined) with metadata."""
        data = {
            'metadata': {
                'data_type': 'weather',
                'source': 'Open-Meteo',
            },
            'data': {'2025-01-15T00:00:00+01:00': {'temp': 5.0}},
        }
        # This wraps itself as having metadata -> v2.0
        assert detect_version(data) == '2.0'


class TestStampMetadata:
    """Tests for stamp_metadata."""

    def test_adds_schema_version(self):
        """Should add schema_version to metadata."""
        meta = {'data_type': 'energy_price', 'source': 'test'}
        result = stamp_metadata(meta)
        assert result['schema_version'] == CURRENT_SCHEMA_VERSION
        # Original fields preserved
        assert result['data_type'] == 'energy_price'

    def test_does_not_overwrite_existing(self):
        """Should not overwrite existing schema_version."""
        meta = {'data_type': 'test', 'schema_version': '1.0'}
        result = stamp_metadata(meta)
        assert result['schema_version'] == '1.0'

    def test_modifies_in_place(self):
        """stamp_metadata modifies the dict in place AND returns it."""
        meta = {'data_type': 'test'}
        result = stamp_metadata(meta)
        assert result is meta
        assert meta['schema_version'] == CURRENT_SCHEMA_VERSION


class TestMigrate1To2:
    """Tests for v1.0 -> v2.0 migration."""

    def test_wraps_raw_data(self):
        """v1.0 raw data should be wrapped in v2.0 structure."""
        data = {'2024-09-15T00:00:00': 50.5}
        result = _migrate_1_to_2(data, 'energy_price_forecast.json')
        assert result['version'] == '2.0'
        assert 'migrated_data' in result
        assert result['migrated_data']['metadata']['data_type'] == 'energy_price'
        assert result['migrated_data']['metadata']['migrated_from'] == '1.0'
        assert result['migrated_data']['data'] == data

    def test_infers_weather_type(self):
        """Should infer data_type from filename."""
        data = {'2024-09-15T00:00:00': {'temp': 5.0}}
        result = _migrate_1_to_2(data, 'weather_forecast.json')
        assert result['migrated_data']['metadata']['data_type'] == 'weather'

    def test_infers_wind_type(self):
        data = {'2024-09-15T00:00:00': 5.0}
        result = _migrate_1_to_2(data, 'wind_forecast.json')
        assert result['migrated_data']['metadata']['data_type'] == 'wind_weather'

    def test_unknown_filename(self):
        """Unknown filename should result in 'unknown' data_type."""
        data = {'key': 'value'}
        result = _migrate_1_to_2(data, 'mysterious_data.json')
        assert result['migrated_data']['metadata']['data_type'] == 'unknown'


class TestMigrate2To21:
    """Tests for v2.0 -> v2.1 migration."""

    def test_adds_schema_version_to_metadata(self):
        """Should add schema_version to each dataset's metadata."""
        data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'data_type': 'energy_price', 'source': 'ENTSO-E'},
                'data': {'2025-01-15T00:00:00+01:00': 50.5},
            },
        }
        result = _migrate_2_to_2_1(data)
        assert result['version'] == '2.1'
        assert result['entsoe']['metadata']['schema_version'] == '2.1'
        assert result['entsoe']['metadata']['migrated_from'] == '2.0'

    def test_adds_dst_defaults_to_calendar(self):
        """Calendar features should get DST field defaults."""
        data = {
            'version': '2.0',
            'calendar': {
                'metadata': {'data_type': 'calendar_features'},
                'data': {
                    '2025-07-15T00:00:00+02:00': {
                        'is_weekend': False,
                        'is_holiday_nl': False,
                        # No DST fields
                    },
                },
            },
        }
        result = _migrate_2_to_2_1(data)
        features = result['calendar']['data']['2025-07-15T00:00:00+02:00']
        assert 'is_dst' in features
        assert 'is_dst_transition_day' in features
        assert 'dst_utc_offset_hours' in features
        # Values are None because we can't retroactively compute them
        assert features['is_dst'] is None

    def test_non_calendar_unchanged(self):
        """Non-calendar datasets should not get DST fields added to their data."""
        data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'data_type': 'energy_price'},
                'data': {'2025-01-15T00:00:00+01:00': 50.5},
            },
        }
        result = _migrate_2_to_2_1(data)
        # Energy price data values are scalars, not dicts — should remain unchanged
        assert result['entsoe']['data']['2025-01-15T00:00:00+01:00'] == 50.5


class TestMigrateToCurrentFull:
    """Tests for full migration path."""

    def test_v1_to_current(self):
        """v1.0 data should migrate all the way to current."""
        data = {'2024-09-15T00:00:00': 50.5}
        result = migrate_to_current(data, filename='energy_price_forecast.json')
        assert result['version'] == '2.1'
        assert 'migrated_data' in result
        assert result['migrated_data']['metadata']['schema_version'] == '2.1'

    def test_v2_to_current(self):
        """v2.0 data should migrate to current."""
        data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'data_type': 'energy_price'},
                'data': {'2025-01-15T00:00:00+01:00': 50.5},
            },
        }
        result = migrate_to_current(data)
        assert result['version'] == '2.1'
        assert result['entsoe']['metadata']['schema_version'] == '2.1'

    def test_current_version_no_migration(self):
        """Current version data should pass through unchanged."""
        data = {
            'version': '2.1',
            'entsoe': {
                'metadata': {'data_type': 'energy_price', 'schema_version': '2.1'},
                'data': {'2026-01-15T00:00:00+01:00': 50.5},
            },
        }
        result = migrate_to_current(data)
        assert result is data  # Same object, not modified


class TestReadJsonFile:
    """Tests for read_json_file with actual files."""

    def test_read_v2_file(self):
        """Should read a v2.0 file and migrate to current."""
        data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'data_type': 'energy_price'},
                'data': {'2025-01-15T00:00:00+01:00': 50.5},
            },
        }
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            json.dump(data, f)
            tmppath = f.name

        try:
            result = read_json_file(tmppath)
            assert result['version'] == '2.1'
            assert result['entsoe']['metadata']['schema_version'] == '2.1'
        finally:
            os.unlink(tmppath)

    def test_read_without_migration(self):
        """Should read raw data when migrate=False."""
        data = {'version': '2.0', 'test': {'metadata': {}, 'data': {}}}
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            json.dump(data, f)
            tmppath = f.name

        try:
            result = read_json_file(tmppath, migrate=False)
            assert result['version'] == '2.0'  # Not migrated
        finally:
            os.unlink(tmppath)

    def test_read_nonexistent_file(self):
        """Should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            read_json_file('/nonexistent/path/data.json')

    def test_read_invalid_json(self):
        """Should raise JSONDecodeError."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False
        ) as f:
            f.write('not valid json {{{')
            tmppath = f.name

        try:
            with pytest.raises(json.JSONDecodeError):
                read_json_file(tmppath)
        finally:
            os.unlink(tmppath)


class TestEnhancedDataSetIntegration:
    """Tests that EnhancedDataSet now auto-stamps schema_version."""

    def test_auto_stamps_version(self):
        """New EnhancedDataSet should have schema_version in metadata."""
        ds = EnhancedDataSet(
            metadata={'data_type': 'energy_price', 'source': 'test', 'units': 'EUR/MWh'},
            data={'2026-01-15T00:00:00+01:00': 50.5},
        )
        assert ds.metadata['schema_version'] == CURRENT_SCHEMA_VERSION

    def test_preserves_existing_version(self):
        """Should not overwrite existing schema_version."""
        ds = EnhancedDataSet(
            metadata={'data_type': 'energy_price', 'source': 'test',
                      'units': 'EUR/MWh', 'schema_version': '2.0'},
            data={'2026-01-15T00:00:00+01:00': 50.5},
        )
        assert ds.metadata['schema_version'] == '2.0'

    def test_to_dict_includes_version(self):
        """to_dict output should include schema_version."""
        ds = EnhancedDataSet(
            metadata={'data_type': 'energy_price', 'source': 'test', 'units': 'EUR/MWh'},
            data={'2026-01-15T00:00:00+01:00': 50.5},
        )
        d = ds.to_dict()
        assert d['metadata']['schema_version'] == CURRENT_SCHEMA_VERSION


class TestCombinedDataSetIntegration:
    """Tests that CombinedDataSet works with versioned datasets."""

    def test_combined_preserves_versions(self):
        """Datasets added to CombinedDataSet should keep their schema_version."""
        combined = CombinedDataSet()
        ds = EnhancedDataSet(
            metadata={'data_type': 'energy_price', 'source': 'test', 'units': 'EUR/MWh'},
            data={'2026-01-15T00:00:00+01:00': 50.5},
        )
        combined.add_dataset('test', ds)
        d = combined.to_dict()
        assert d['test']['metadata']['schema_version'] == CURRENT_SCHEMA_VERSION


class TestGetters:
    """Tests for utility functions."""

    def test_get_current_version(self):
        assert get_current_version() == CURRENT_SCHEMA_VERSION

    def test_get_changelog(self):
        changelog = get_changelog()
        assert '1.0' in changelog
        assert '2.0' in changelog
        assert '2.1' in changelog
        assert 'changes' in changelog['2.1']
