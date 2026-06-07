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
    def _wrap(self, sub_datasets):
        """Wrap per-collector sub-datasets in the canonical v2.x envelope.

        Helper for these tests: the validator now requires `{metadata, data}`
        with `data_type='combined'` to route to the multi-collector branch.
        """
        return {
            'metadata': {'data_type': 'combined', 'source': 'aggregated'},
            'data': sub_datasets,
        }

    def test_valid_data_passes(self):
        """Test that valid data with correct timestamps passes validation"""
        valid_data = self._wrap({
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
        })

        is_valid, malformed = validate_data_timestamps(valid_data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    @pytest.mark.critical
    def test_detects_malformed_elspot_timestamps(self):
        """Test detection of malformed +00:09 timestamps in elspot data"""
        malformed_data = self._wrap({
            'elspot': {
                'metadata': {'source': 'Nord Pool'},
                'data': {
                    '2025-10-24T12:00:00+00:09': 100.5,  # MALFORMED
                    '2025-10-24T13:00:00+00:09': 95.3    # MALFORMED
                }
            }
        })

        is_valid, malformed = validate_data_timestamps(malformed_data)

        assert is_valid is False
        assert len(malformed) == 2
        assert all('elspot' in ts for ts in malformed)
        assert all('+00:09' in ts for ts in malformed)

    @pytest.mark.unit
    @pytest.mark.critical
    def test_detects_mixed_valid_and_malformed(self):
        """Test detection when some sources are valid and others malformed"""
        mixed_data = self._wrap({
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
        })

        is_valid, malformed = validate_data_timestamps(mixed_data)

        assert is_valid is False
        assert len(malformed) == 1
        assert 'elspot' in malformed[0]
        assert '+00:09' in malformed[0]

    @pytest.mark.unit
    def test_envelope_metadata_does_not_pollute_results(self):
        """The envelope's `metadata` block is not iterated as a sub-source."""
        data = self._wrap({
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {
                    '2025-10-24T12:00:00+02:00': 100.5
                }
            }
        })

        is_valid, malformed = validate_data_timestamps(data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    def test_ignores_sources_without_data_field(self):
        """Test that sources without 'data' field are ignored"""
        data = self._wrap({
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
        })

        is_valid, malformed = validate_data_timestamps(data)

        assert is_valid is True

    @pytest.mark.unit
    @pytest.mark.critical
    def test_accepts_utc_timestamps(self):
        """Test that UTC timestamps (+00:00) are accepted"""
        utc_data = self._wrap({
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {
                    '2025-10-24T10:00:00+00:00': 100.5,
                    '2025-10-24T11:00:00Z': 95.3  # Z suffix is also valid UTC
                }
            }
        })

        is_valid, malformed = validate_data_timestamps(utc_data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    @pytest.mark.critical
    def test_accepts_winter_cet_offset(self):
        """Test that winter CET offset (+01:00) is accepted"""
        winter_data = self._wrap({
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {
                    '2025-01-15T12:00:00+01:00': 100.5,
                    '2025-01-15T13:00:00+01:00': 95.3
                }
            }
        })

        is_valid, malformed = validate_data_timestamps(winter_data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    def test_empty_data_is_valid(self):
        """Test that empty data dict is considered valid"""
        empty_data = self._wrap({
            'entsoe': {
                'metadata': {'source': 'ENTSO-E'},
                'data': {}
            }
        })

        is_valid, malformed = validate_data_timestamps(empty_data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    def test_empty_combined_wrap_returns_true(self):
        """Empty combined wrap (all collectors failed) returns (True, []).

        Regression for reviewer finding #5: under the old discriminator,
        empty `data: {}` was routed to the standalone branch via duck-typing
        and silently returned (True, []). Under the deterministic
        `data_type=='combined'` discriminator, the routing is correct (it
        goes to the combined branch), but the result is the same — and
        that's right: no timestamps means none to invalidate. Completeness
        is a separate concern handled by validate_completeness.
        """
        empty_combined = {
            'metadata': {'data_type': 'combined', 'source': 'aggregated'},
            'data': {},
        }

        is_valid, malformed = validate_data_timestamps(empty_combined)

        assert is_valid is True
        assert malformed == []


class TestMultipleSourceValidation:
    """Tests for validation across multiple energy sources"""

    def _wrap(self, sub_datasets):
        return {
            'metadata': {'data_type': 'combined', 'source': 'aggregated'},
            'data': sub_datasets,
        }

    @pytest.mark.unit
    @pytest.mark.critical
    def test_all_four_sources_valid(self):
        """Test validation with all four energy sources (entsoe, elspot, epex, energy_zero)"""
        all_sources_data = self._wrap({
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
        })

        is_valid, malformed = validate_data_timestamps(all_sources_data)

        assert is_valid is True
        assert len(malformed) == 0

    @pytest.mark.unit
    @pytest.mark.critical
    def test_identifies_specific_malformed_source(self):
        """Test that validation correctly identifies which source has malformed timestamps"""
        data_with_one_bad_source = self._wrap({
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
        })

        is_valid, malformed = validate_data_timestamps(data_with_one_bad_source)

        assert is_valid is False
        assert len(malformed) == 1
        assert 'elspot' in malformed[0]
        assert 'entsoe' not in malformed[0]
        assert 'epex' not in malformed[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
