"""
Unit Tests for Utils Helpers Module
------------------------------------
Tests utility functions in utils/helpers.py.

File: tests/unit/test_utils_helpers.py
Created: 2025-10-25
"""

import pytest
import os
import json
import tempfile
from configparser import ConfigParser
from unittest.mock import patch, MagicMock, mock_open
from utils.helpers import (
    ensure_output_directory,
    load_settings,
    load_secrets,
    load_config,
    convert_value,
    distance,
    closest,
    detect_file_type,
    validate_data_timestamps,
    save_data_file,
    load_data_file
)


class TestEnsureOutputDirectory:
    """Test directory creation function."""

    def test_creates_directory(self, tmp_path):
        """Test directory is created successfully."""
        test_dir = tmp_path / "test_output"
        assert not test_dir.exists()

        ensure_output_directory(str(test_dir))

        assert test_dir.exists()
        assert test_dir.is_dir()

    def test_creates_nested_directory(self, tmp_path):
        """Test nested directory creation."""
        test_dir = tmp_path / "level1" / "level2" / "level3"

        ensure_output_directory(str(test_dir))

        assert test_dir.exists()
        assert test_dir.is_dir()

    def test_existing_directory_no_error(self, tmp_path):
        """Test no error when directory already exists."""
        test_dir = tmp_path / "existing"
        test_dir.mkdir()

        # Should not raise error
        ensure_output_directory(str(test_dir))

        assert test_dir.exists()


class TestLoadSettings:
    """Test settings loading function."""

    def test_load_valid_settings(self, tmp_path):
        """Test loading valid settings file."""
        config_file = tmp_path / "settings.ini"
        config_file.write_text("[section1]\nkey1 = value1\nkey2 = value2\n")

        config = load_settings(str(tmp_path), "settings.ini")

        assert config.get('section1', 'key1') == 'value1'
        assert config.get('section1', 'key2') == 'value2'

    def test_load_nonexistent_file(self, tmp_path):
        """Test loading non-existent file returns empty config."""
        config = load_settings(str(tmp_path), "nonexistent.ini")

        # Should return empty ConfigParser
        assert len(config.sections()) == 0


class TestLoadSecrets:
    """Test secrets loading function."""

    def test_load_from_environment_variables(self):
        """Test loading from environment variables."""
        env_vars = {
            'ENCRYPTION_KEY': 'test_enc_key',
            'HMAC_KEY': 'test_hmac_key',
            'ENTSOE_API_KEY': 'test_entsoe',
            'OPENWEATHER_API_KEY': 'test_weather',
            'METEO_API_KEY': 'test_meteo',
            'GOOGLE_API_KEY': 'test_google'
        }

        with patch.dict(os.environ, env_vars):
            config = load_secrets('/fake/path')

            assert config.get('security_keys', 'encryption') == 'test_enc_key'
            assert config.get('security_keys', 'hmac') == 'test_hmac_key'
            assert config.get('api_keys', 'entsoe') == 'test_entsoe'
            assert config.get('api_keys', 'openweather') == 'test_weather'

    def test_load_from_secrets_file(self, tmp_path):
        """Test loading from secrets file."""
        secrets_file = tmp_path / "secrets.ini"
        secrets_file.write_text(
            "[security_keys]\n"
            "encryption = file_enc_key\n"
            "hmac = file_hmac_key\n"
            "[api_keys]\n"
            "entsoe = file_entsoe\n"
        )

        config = load_secrets(str(tmp_path), 'secrets.ini')

        assert config.get('security_keys', 'encryption') == 'file_enc_key'
        assert config.get('api_keys', 'entsoe') == 'file_entsoe'

    def test_missing_config_raises_error(self, tmp_path):
        """Test error when no config available."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(RuntimeError, match="No configuration found"):
                load_secrets(str(tmp_path), 'nonexistent.ini')


class TestLoadConfig:
    """Test config loading function."""

    def test_load_from_environment_variables(self):
        """Test loading config from environment."""
        env_vars = {
            'ENTSOE_API_KEY': 'test_entsoe',
            'OPENWEATHER_API_KEY': 'test_weather',
            'METEO_API_KEY': 'test_meteo'
        }

        with patch.dict(os.environ, env_vars):
            config = load_config('/fake/path')

            assert config['api_keys']['entsoe'] == 'test_entsoe'
            assert config['api_keys']['openweather'] == 'test_weather'

    def test_load_from_config_file(self, tmp_path):
        """Test loading from config file."""
        config_file = tmp_path / "secrets.ini"
        config_file.write_text(
            "[api_keys]\n"
            "entsoe = file_entsoe\n"
            "openweather = file_weather\n"
            "meteo = file_meteo\n"
        )

        config = load_config(str(tmp_path), 'secrets.ini')

        assert config.get('api_keys', 'entsoe') == 'file_entsoe'


class TestConvertValue:
    """Test value conversion function."""

    def test_convert_int(self):
        """Test integer conversion."""
        assert convert_value(42) == 42
        assert convert_value("42") == 42

    def test_convert_float(self):
        """Test float conversion."""
        assert convert_value(42.5) == 42.5
        assert convert_value("42.5") == 42.5

    def test_convert_dash_to_none(self):
        """Test dash converts to None."""
        assert convert_value('-') is None

    def test_convert_none_string(self):
        """Test 'none' string converts to None."""
        assert convert_value('none') is None
        assert convert_value('None') is None
        assert convert_value('NONE') is None

    def test_convert_invalid_keeps_string(self):
        """Test invalid values remain as strings."""
        assert convert_value('invalid') == 'invalid'

    def test_convert_existing_types(self):
        """Test values that are already correct type."""
        assert convert_value(42) == 42
        assert convert_value(3.14) == 3.14


class TestDistance:
    """Test distance calculation function."""

    def test_distance_same_point(self):
        """Test distance between same point is zero."""
        dist = distance(52.37, 4.89, 52.37, 4.89)
        assert dist < 0.001  # Should be very close to 0

    def test_distance_amsterdam_rotterdam(self):
        """Test distance between Amsterdam and Rotterdam."""
        # Amsterdam: 52.37, 4.89
        # Rotterdam: 51.92, 4.48
        dist = distance(52.37, 4.89, 51.92, 4.48)
        # Distance should be approximately 57 km
        assert 50 < dist < 65

    def test_distance_returns_float(self):
        """Test distance returns a float."""
        dist = distance(52.0, 4.0, 53.0, 5.0)
        assert isinstance(dist, float)


class TestClosest:
    """Test closest point finder function."""

    def test_find_closest_station(self):
        """Test finding closest station to location."""
        stations = [
            {"latitude": 52.0, "longitude": 4.0, "name": "Station A"},
            {"latitude": 52.5, "longitude": 4.5, "name": "Station B"},
            {"latitude": 53.0, "longitude": 5.0, "name": "Station C"}
        ]
        target = {"latitude": 52.4, "longitude": 4.4}

        result = closest(stations, target)

        assert result["name"] == "Station B"

    def test_find_closest_single_station(self):
        """Test with only one station."""
        stations = [
            {"latitude": 52.0, "longitude": 4.0, "name": "Only Station"}
        ]
        target = {"latitude": 53.0, "longitude": 5.0}

        result = closest(stations, target)

        assert result["name"] == "Only Station"


class TestDetectFileType:
    """Test file type detection function."""

    def test_detect_json(self):
        """Test detecting JSON content."""
        json_content = '{"key": "value", "number": 42}'
        assert detect_file_type(json_content) == 'json'

    def test_detect_encrypted_base64(self):
        """Test detecting base64 encrypted content."""
        base64_content = "SGVsbG8gV29ybGQh"  # Valid base64
        assert detect_file_type(base64_content) == 'encrypted'

    def test_invalid_content_raises_error(self):
        """Test invalid content raises ValueError."""
        invalid_content = "This is neither JSON nor base64!"
        with pytest.raises(ValueError, match="neither valid JSON nor base64"):
            detect_file_type(invalid_content)

    def test_detect_json_with_whitespace(self):
        """Test JSON detection with whitespace."""
        json_content = '  {"key": "value"}  '
        assert detect_file_type(json_content) == 'json'


class TestValidateDataTimestamps:
    """Test timestamp validation function."""

    def test_valid_timestamps(self):
        """Test validation with valid timestamps."""
        data = {
            'version': '2.0',
            'energy': {
                'data': {
                    '2025-10-25T12:00:00+02:00': 100.5,
                    '2025-10-25T13:00:00+01:00': 105.0
                }
            }
        }

        is_valid, malformed = validate_data_timestamps(data)

        assert is_valid is True
        assert len(malformed) == 0

    def test_malformed_timestamps(self):
        """Test detection of malformed timestamps."""
        data = {
            'version': '2.0',
            'energy': {
                'data': {
                    '2025-10-25T12:00:00+00:09': 100.5,  # Malformed
                    '2025-10-25T13:00:00+02:00': 105.0
                }
            }
        }

        is_valid, malformed = validate_data_timestamps(data)

        assert is_valid is False
        assert len(malformed) == 1
        assert 'energy' in malformed[0]

    def test_skip_version_field(self):
        """Test that version field is skipped."""
        data = {
            'version': '2.0'
        }

        is_valid, malformed = validate_data_timestamps(data)

        assert is_valid is True


class TestSaveDataFile:
    """Test data file saving function."""

    def test_save_unencrypted_json(self, tmp_path):
        """Test saving unencrypted JSON file."""
        from utils.data_types import CombinedDataSet, EnhancedDataSet

        combined = CombinedDataSet()
        dataset = EnhancedDataSet(
            metadata={'data_type': 'test'},
            data={'2025-10-25T12:00:00+02:00': 42.0}
        )
        combined.add_dataset('test', dataset)

        file_path = tmp_path / "output.json"

        save_data_file(combined, str(file_path), encrypt=False)

        assert file_path.exists()
        with open(file_path) as f:
            loaded = json.load(f)
        assert 'version' in loaded

    def test_save_with_malformed_timestamps_raises_error(self, tmp_path):
        """Test that malformed timestamps prevent saving."""
        from utils.data_types import CombinedDataSet, EnhancedDataSet

        # Create CombinedDataSet (which validates through to_dict())
        combined = CombinedDataSet()
        dataset = EnhancedDataSet(
            metadata={'data_type': 'test'},
            data={'2025-10-25T12:00:00+00:09': 42.0}  # Malformed
        )
        combined.add_dataset('test', dataset)

        file_path = tmp_path / "output.json"

        with pytest.raises(ValueError, match="malformed timestamps"):
            save_data_file(combined, str(file_path), encrypt=False)


class TestLoadDataFile:
    """Test data file loading function."""

    def test_load_json_file(self, tmp_path):
        """Test loading plain JSON file."""
        data = {'key': 'value', 'number': 42}
        file_path = tmp_path / "test.json"

        with open(file_path, 'w') as f:
            json.dump(data, f)

        loaded = load_data_file(str(file_path))

        assert loaded == data

    def test_load_encrypted_without_handler_raises_error(self, tmp_path):
        """Test loading encrypted file without handler raises error."""
        file_path = tmp_path / "encrypted.txt"
        file_path.write_text("SGVsbG8gV29ybGQh")  # Base64 content

        with pytest.raises(ValueError, match="no handler provided"):
            load_data_file(str(file_path), handler=None)

    def test_load_nonexistent_file_raises_error(self):
        """Test loading non-existent file raises error."""
        with pytest.raises(Exception):
            load_data_file('/nonexistent/file.json')


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
