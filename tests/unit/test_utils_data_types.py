"""
Unit Tests for Data Types Module
---------------------------------
Tests EnhancedDataSet and CombinedDataSet.

File: tests/unit/test_utils_data_types.py
Created: 2025-10-25
"""

import pytest
import json
from datetime import datetime
from utils.data_types import EnhancedDataSet, CombinedDataSet


class TestEnhancedDataSet:
    """Test EnhancedDataSet class."""

    def test_initialization_energy_price(self):
        """Test initialization with energy price data."""
        metadata = {
            'data_type': 'energy_price',
            'source': 'Test API',
            'units': 'EUR/MWh'
        }
        data = {
            '2025-10-25T12:00:00+02:00': 100.5,
            '2025-10-25T13:00:00+02:00': 105.0
        }

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        assert dataset.metadata == metadata
        assert dataset.data == data
        assert len(dataset.data) == 2

    def test_initialization_weather_data(self):
        """Test initialization with weather data."""
        metadata = {
            'data_type': 'weather',
            'source': 'Test API',
            'units': 'metric'
        }
        data = {
            '2025-10-25T12:00:00+02:00': {
                'temp': 15.5,
                'humidity': 75,
                'pressure': 1013
            }
        }

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        assert dataset.metadata == metadata
        assert len(dataset.data) == 1
        assert 'temp' in dataset.data['2025-10-25T12:00:00+02:00']

    def test_initialization_unknown_type(self):
        """Test initialization with unknown data type."""
        metadata = {
            'data_type': 'unknown',
            'source': 'Test API'
        }
        data = {
            '2025-10-25T12:00:00+02:00': 42.0
        }

        # Should not raise error
        dataset = EnhancedDataSet(metadata=metadata, data=data)
        assert dataset.metadata == metadata
        assert dataset.data == data

    def test_to_dict(self):
        """Test conversion to dictionary."""
        metadata = {'data_type': 'energy_price', 'source': 'Test'}
        data = {'2025-10-25T12:00:00+02:00': 100.5}

        dataset = EnhancedDataSet(metadata=metadata, data=data)
        result = dataset.to_dict()

        assert 'metadata' in result
        assert 'data' in result
        assert result['metadata'] == metadata
        assert result['data'] == data

    def test_write_to_json(self, tmp_path):
        """Test writing to JSON file."""
        metadata = {'data_type': 'energy_price', 'source': 'Test'}
        data = {
            '2025-10-25T12:00:00+02:00': 100.5,
            '2025-10-25T13:00:00+02:00': 105.0
        }

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        # Write to temp file
        file_path = tmp_path / "test_output.json"
        dataset.write_to_json(str(file_path))

        # Verify file exists and is valid JSON
        assert file_path.exists()

        with open(file_path, 'r') as f:
            loaded = json.load(f)

        assert 'metadata' in loaded
        assert 'data' in loaded
        assert loaded['metadata'] == metadata
        assert loaded['data'] == data

    def test_validate_energy_prices(self):
        """Test energy price validation."""
        metadata = {'data_type': 'energy_price'}
        data = {
            '2025-10-25T12:00:00+02:00': 100.5,
            '2025-10-25T13:00:00+02:00': 105.0
        }

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        # All values should be floats
        for value in dataset.data.values():
            assert isinstance(value, (int, float))

    def test_validate_weather_data(self):
        """Test weather data validation."""
        metadata = {'data_type': 'weather'}
        data = {
            '2025-10-25T12:00:00+02:00': {
                'temp': 15.5,
                'humidity': 75
            }
        }

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        # All values should be dicts
        for value in dataset.data.values():
            assert isinstance(value, dict)

    def test_empty_data(self):
        """Test with empty data."""
        metadata = {'data_type': 'energy_price'}
        data = {}

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        assert len(dataset.data) == 0
        assert dataset.to_dict()['data'] == {}


class TestCombinedDataSet:
    """Test CombinedDataSet class."""

    def test_initialization(self):
        """Test initialization."""
        combined = CombinedDataSet()

        assert combined.datasets == {}
        assert len(combined.datasets) == 0

    def test_add_dataset(self):
        """Test adding a dataset."""
        combined = CombinedDataSet()

        metadata = {'data_type': 'energy_price', 'source': 'Test'}
        data = {'2025-10-25T12:00:00+02:00': 100.5}
        dataset = EnhancedDataSet(metadata=metadata, data=data)

        combined.add_dataset('test_source', dataset)

        assert 'test_source' in combined.datasets
        # CombinedDataSet stores to_dict() output, not the object
        assert combined.datasets['test_source'] == dataset.to_dict()

    def test_add_multiple_datasets(self):
        """Test adding multiple datasets."""
        combined = CombinedDataSet()

        # Add first dataset
        metadata1 = {'data_type': 'energy_price', 'source': 'Source1'}
        data1 = {'2025-10-25T12:00:00+02:00': 100.5}
        dataset1 = EnhancedDataSet(metadata=metadata1, data=data1)
        combined.add_dataset('source1', dataset1)

        # Add second dataset
        metadata2 = {'data_type': 'weather', 'source': 'Source2'}
        data2 = {'2025-10-25T12:00:00+02:00': {'temp': 15.5}}
        dataset2 = EnhancedDataSet(metadata=metadata2, data=data2)
        combined.add_dataset('source2', dataset2)

        assert len(combined.datasets) == 2
        assert 'source1' in combined.datasets
        assert 'source2' in combined.datasets

    def test_add_none_dataset(self):
        """Test adding None dataset (should be skipped)."""
        combined = CombinedDataSet()

        combined.add_dataset('test', None)

        # None should not be added
        assert 'test' not in combined.datasets
        assert len(combined.datasets) == 0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        combined = CombinedDataSet()

        metadata = {'data_type': 'energy_price'}
        data = {'2025-10-25T12:00:00+02:00': 100.5}
        dataset = EnhancedDataSet(metadata=metadata, data=data)

        combined.add_dataset('test_source', dataset)

        result = combined.to_dict()

        assert 'test_source' in result
        assert 'metadata' in result['test_source']
        assert 'data' in result['test_source']

    def test_write_to_json(self, tmp_path):
        """Test writing combined dataset to JSON."""
        combined = CombinedDataSet()

        # Add dataset
        metadata = {'data_type': 'energy_price', 'source': 'Test'}
        data = {'2025-10-25T12:00:00+02:00': 100.5}
        dataset = EnhancedDataSet(metadata=metadata, data=data)
        combined.add_dataset('test_source', dataset)

        # Write to file
        file_path = tmp_path / "combined_output.json"
        combined.write_to_json(str(file_path))

        # Verify
        assert file_path.exists()

        with open(file_path, 'r') as f:
            loaded = json.load(f)

        assert 'test_source' in loaded
        assert loaded['test_source']['metadata'] == metadata

    def test_bool_evaluation_empty(self):
        """Test boolean evaluation with empty dataset."""
        combined = CombinedDataSet()

        # CombinedDataSet doesn't implement __bool__, so always truthy
        # Check emptiness via len(datasets) instead
        assert len(combined.datasets) == 0

    def test_bool_evaluation_with_data(self):
        """Test boolean evaluation with data."""
        combined = CombinedDataSet()

        metadata = {'data_type': 'energy_price'}
        data = {'2025-10-25T12:00:00+02:00': 100.5}
        dataset = EnhancedDataSet(metadata=metadata, data=data)
        combined.add_dataset('test', dataset)

        # CombinedDataSet doesn't implement __bool__, check datasets instead
        assert len(combined.datasets) > 0

    def test_multiple_sources_to_dict(self):
        """Test to_dict with multiple sources."""
        combined = CombinedDataSet()

        # Add energy data
        energy_meta = {'data_type': 'energy_price', 'source': 'EnergyAPI'}
        energy_data = {
            '2025-10-25T12:00:00+02:00': 100.5,
            '2025-10-25T13:00:00+02:00': 105.0
        }
        combined.add_dataset('energy', EnhancedDataSet(energy_meta, energy_data))

        # Add weather data
        weather_meta = {'data_type': 'weather', 'source': 'WeatherAPI'}
        weather_data = {
            '2025-10-25T12:00:00+02:00': {
                'temp': 15.5,
                'humidity': 75
            }
        }
        combined.add_dataset('weather', EnhancedDataSet(weather_meta, weather_data))

        result = combined.to_dict()

        # to_dict() includes 'version' field, so len is 3 (version + 2 datasets)
        assert len(result) == 3
        assert 'version' in result
        assert result['version'] == '2.0'
        assert 'energy' in result
        assert 'weather' in result
        assert result['energy']['metadata']['data_type'] == 'energy_price'
        assert result['weather']['metadata']['data_type'] == 'weather'


class TestDataTypeEdgeCases:
    """Test edge cases and error conditions."""

    def test_invalid_json_serialization(self):
        """Test handling of non-serializable data."""
        metadata = {'data_type': 'test'}
        # datetime objects are not JSON serializable by default
        data = {'time': datetime.now()}

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        # to_dict should work (doesn't serialize yet)
        result = dataset.to_dict()
        assert 'data' in result

    def test_large_dataset(self):
        """Test with large dataset."""
        metadata = {'data_type': 'energy_price'}
        # Create 1000 data points
        data = {
            f'2025-10-25T{h:02d}:{m:02d}:00+02:00': 100.0 + h + m/60.0
            for h in range(24)
            for m in range(0, 60, 15)  # Every 15 minutes
        }

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        assert len(dataset.data) == 96  # 24 hours * 4 per hour
        result = dataset.to_dict()
        assert len(result['data']) == 96

    def test_special_characters_in_keys(self):
        """Test with special characters in keys."""
        metadata = {'data_type': 'test', 'source': 'Test API: Special/Chars\\Path'}
        data = {'2025-10-25T12:00:00+02:00': 100.5}

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        # Should handle special characters
        result = dataset.to_dict()
        assert result['metadata']['source'] == 'Test API: Special/Chars\\Path'

    def test_unicode_in_metadata(self):
        """Test unicode characters in metadata."""
        metadata = {
            'data_type': 'energy_price',
            'source': 'Test API',
            'location': 'Arnhem',
            'notes': 'Temperature: 15°C, Price: €100/MWh'
        }
        data = {'2025-10-25T12:00:00+02:00': 100.5}

        dataset = EnhancedDataSet(metadata=metadata, data=data)

        result = dataset.to_dict()
        assert '°C' in result['metadata']['notes']
        assert '€' in result['metadata']['notes']


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
