"""
Unit tests for data validation utilities

Tests the validation functions that prevent malformed data from being saved.
"""
import pytest
from utils.helpers import validate_data_timestamps


class TestValidateDataTimestamps:
    """Tests for validate_data_timestamps function"""

    @pytest.mark.unit
    @pytest.mark.critical
    def test_valid_data_passes(self):
        """Test that valid data with correct timestamps passes validation"""
        valid_data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {
                    '2025-10-24T12:00:00+02:00': 100.5,
                    '2025-10-24T13:00:00+02:00': 95.3
                }
            },
            'elspot': {
                'metadata': {'source': 'Nord Pool'},
                'data': {
                    '2025-10-24T12:00:00+02:00': 102.1,
                    '2025-10-24T13:00:00+02:00': 98.7
                }
            }
        }

        is_valid, malformed = validate_data_timestamps(valid_data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    @pytest.mark.critical
    def test_detects_malformed_elspot_timestamps(self):
        """Test detection of malformed +00:09 timestamps in elspot data"""
        malformed_data = {
            'version': '2.0',
            'elspot': {
                'metadata': {'source': 'Nord Pool'},
                'data': {
                    '2025-10-24T12:00:00+00:09': 100.5,  # MALFORMED
                    '2025-10-24T13:00:00+00:09': 95.3    # MALFORMED
                }
            }
        }

        is_valid, malformed = validate_data_timestamps(malformed_data)

        assert is_valid is False
        assert len(malformed) == 2
        assert all('elspot' in ts for ts in malformed)
        assert all('+00:09' in ts for ts in malformed)

    @pytest.mark.unit
    @pytest.mark.critical
    def test_detects_mixed_valid_and_malformed(self):
        """Test detection when some sources are valid and others malformed"""
        mixed_data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {
                    '2025-10-24T12:00:00+02:00': 100.5,  # VALID
                    '2025-10-24T13:00:00+02:00': 95.3    # VALID
                }
            },
            'elspot': {
                'metadata': {'source': 'Nord Pool'},
                'data': {
                    '2025-10-24T12:00:00+00:09': 102.1,  # MALFORMED
                    '2025-10-24T13:00:00+02:00': 98.7    # VALID
                }
            }
        }

        is_valid, malformed = validate_data_timestamps(mixed_data)

        assert is_valid is False
        assert len(malformed) == 1
        assert 'elspot' in malformed[0]
        assert '+00:09' in malformed[0]

    @pytest.mark.unit
    def test_ignores_version_field(self):
        """Test that version field is ignored during validation"""
        data = {
            'version': '2.0',  # Should be ignored
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {
                    '2025-10-24T12:00:00+02:00': 100.5
                }
            }
        }

        is_valid, malformed = validate_data_timestamps(data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    def test_ignores_sources_without_data_field(self):
        """Test that sources without 'data' field are ignored"""
        data = {
            'version': '2.0',
            'metadata_only': {
                'source': 'Some Source'
                # No 'data' field
            },
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {
                    '2025-10-24T12:00:00+02:00': 100.5
                }
            }
        }

        is_valid, malformed = validate_data_timestamps(data)

        assert is_valid is True

    @pytest.mark.unit
    @pytest.mark.critical
    def test_accepts_utc_timestamps(self):
        """Test that UTC timestamps (+00:00) are accepted"""
        utc_data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {
                    '2025-10-24T10:00:00+00:00': 100.5,
                    '2025-10-24T11:00:00Z': 95.3  # Z suffix is also valid UTC
                }
            }
        }

        is_valid, malformed = validate_data_timestamps(utc_data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    @pytest.mark.critical
    def test_accepts_winter_cet_offset(self):
        """Test that winter CET offset (+01:00) is accepted"""
        winter_data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {
                    '2025-01-15T12:00:00+01:00': 100.5,
                    '2025-01-15T13:00:00+01:00': 95.3
                }
            }
        }

        is_valid, malformed = validate_data_timestamps(winter_data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    def test_empty_data_is_valid(self):
        """Test that empty data dict is considered valid"""
        empty_data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {}
            }
        }

        is_valid, malformed = validate_data_timestamps(empty_data)

        assert is_valid is True
        assert len(malformed) == 0


class TestMultipleSourceValidation:
    """Tests for validation across multiple energy sources"""

    @pytest.mark.unit
    @pytest.mark.critical
    def test_all_four_sources_valid(self):
        """Test validation with all four energy sources (entsoe, elspot, epex, energy_zero)"""
        all_sources_data = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {'2025-10-24T12:00:00+02:00': 100.5}
            },
            'elspot': {
                'metadata': {'source': 'Nord Pool'},
                'data': {'2025-10-24T12:00:00+02:00': 102.1}
            },
            'epex': {
                'metadata': {'source': 'EPEX SPOT'},
                'data': {'2025-10-24T12:00:00+02:00': 95.8}
            },
            'energy_zero': {
                'metadata': {'source': 'EnergyZero'},
                'data': {'2025-10-24T12:00:00+02:00': 0.25}
            }
        }

        is_valid, malformed = validate_data_timestamps(all_sources_data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    @pytest.mark.critical
    def test_identifies_specific_malformed_source(self):
        """Test that validation correctly identifies which source has malformed timestamps"""
        data_with_one_bad_source = {
            'version': '2.0',
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {'2025-10-24T12:00:00+02:00': 100.5}  # VALID
            },
            'elspot': {
                'metadata': {'source': 'Nord Pool'},
                'data': {'2025-10-24T12:00:00+00:09': 102.1}  # MALFORMED
            },
            'epex': {
                'metadata': {'source': 'EPEX SPOT'},
                'data': {'2025-10-24T12:00:00+02:00': 95.8}   # VALID
            }
        }

        is_valid, malformed = validate_data_timestamps(data_with_one_bad_source)

        assert is_valid is False
        assert len(malformed) == 1
        assert 'elspot' in malformed[0]
        assert 'entsoe' not in malformed[0]
        assert 'epex' not in malformed[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
