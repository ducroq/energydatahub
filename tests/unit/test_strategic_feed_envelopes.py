"""
Issue #26: pins the canonical {metadata, data} envelope shape on the
CombinedDataSet-backed strategic feeds (`energy_price_forecast.json`,
`wind_forecast.json`) so they match the 14 already-homogenised feeds.

Before this fix `CombinedDataSet.to_dict()` produced `{version, src_a: ...,
src_b: ...}` — flat at the root, no `data` wrap. Downstream consumers had
to special-case the two feeds. Sibling regression to PR #20's buurt-air fix.

File: tests/unit/test_strategic_feed_envelopes.py
Created: 2026-06-07
"""

import pytest

from utils.data_types import CombinedDataSet, EnhancedDataSet


def _eds(data_type, source, data):
    return EnhancedDataSet(
        metadata={'data_type': data_type, 'source': source, 'units': 'x'},
        data=data,
    )


def _energy_price_combined():
    """Mirror data_fetcher.py:666 — energy_price_forecast composition."""
    cds = CombinedDataSet()
    cds.add_dataset('entsoe',      _eds('energy_price', 'ENTSO-E NL', {'2026-01-01T00:00:00+01:00': 50.0}))
    cds.add_dataset('entsoe_de',   _eds('energy_price', 'ENTSO-E DE', {'2026-01-01T00:00:00+01:00': 60.0}))
    cds.add_dataset('energy_zero', _eds('energy_price', 'EnergyZero', {'2026-01-01T00:00:00+01:00': 55.0}))
    cds.add_dataset('epex',        _eds('energy_price', 'EPEX',       {'2026-01-01T00:00:00+01:00': 58.0}))
    cds.add_dataset('elspot',      _eds('energy_price', 'Nord Pool',  {'2026-01-01T00:00:00+01:00': 45.0}))
    return cds


def _wind_combined():
    """Mirror data_fetcher.py:703 — wind_forecast composition."""
    cds = CombinedDataSet()
    cds.add_dataset(
        'entsoe_wind_generation',
        _eds('wind_generation', 'ENTSO-E', {'NL': {'2026-01-01T00:00:00+01:00': 1200.0}}),
    )
    cds.add_dataset(
        'offshore_wind',
        _eds('wind_weather', 'Open-Meteo', {'borssele': {'2026-01-01T00:00:00+01:00': {'wind_speed_10m': 8.5}}}),
    )
    return cds


class TestStrategicFeedEnvelopeShape:
    """All CombinedDataSet-backed strategic feeds must publish the canonical
    `{metadata, data}` envelope. New feeds added later are caught by the
    parametrised loop below.
    """

    @pytest.mark.parametrize("feed_name,build", [
        ('energy_price_forecast', _energy_price_combined),
        ('wind_forecast',         _wind_combined),
    ])
    def test_top_level_is_metadata_and_data(self, feed_name, build):
        payload = build().to_dict()
        assert set(payload.keys()) == {'metadata', 'data'}, (
            f"{feed_name} must expose only top-level metadata and data; "
            f"got {sorted(payload.keys())}"
        )

    @pytest.mark.parametrize("feed_name,build", [
        ('energy_price_forecast', _energy_price_combined),
        ('wind_forecast',         _wind_combined),
    ])
    def test_envelope_metadata_carries_version_and_schema(self, feed_name, build):
        payload = build().to_dict()
        meta = payload['metadata']
        assert 'version' in meta, f"{feed_name} metadata missing CombinedDataSet version"
        assert 'schema_version' in meta, f"{feed_name} metadata missing schema_version"
        assert meta['source'] == 'aggregated'

    @pytest.mark.parametrize("feed_name,expected_sources,build", [
        ('energy_price_forecast',
         {'entsoe', 'entsoe_de', 'energy_zero', 'epex', 'elspot'},
         _energy_price_combined),
        ('wind_forecast',
         {'entsoe_wind_generation', 'offshore_wind'},
         _wind_combined),
    ])
    def test_per_collector_subdatasets_under_data(self, feed_name, expected_sources, build):
        payload = build().to_dict()
        assert set(payload['data'].keys()) == expected_sources
        # Each per-collector sub-payload retains its own {metadata, data} wrap
        # so consumers can fetch source/units/etc. per collector.
        for src, sub in payload['data'].items():
            assert 'metadata' in sub, f"{feed_name}.{src} missing metadata"
            assert 'data' in sub, f"{feed_name}.{src} missing data"

    def test_flat_root_keys_are_gone(self):
        """Regression guard: a per-collector key must NEVER appear at the
        top level of the envelope (the pre-#26 flat shape)."""
        payload = _energy_price_combined().to_dict()
        for forbidden in ('entsoe', 'entsoe_de', 'energy_zero', 'epex', 'elspot', 'version'):
            assert forbidden not in payload, (
                f"top-level key '{forbidden}' indicates the pre-#26 flat shape leaked back"
            )


class TestTimestampValidatorHandlesWrap:
    """The malformed-timestamp guard in `save_data_file` walks the published
    envelope. With the new wrap it must reach per-collector timestamps via
    `payload['data'][src]['data']` instead of skipping them.
    """

    def test_malformed_timestamp_inside_wrap_is_detected(self):
        from utils.helpers import validate_data_timestamps

        cds = CombinedDataSet()
        # Malformed offset `+00:09` is the canonical example from helpers.py docstring.
        cds.add_dataset(
            'entsoe',
            _eds('energy_price', 'ENTSO-E', {'2025-10-24T12:00:00+00:09': 50.0}),
        )
        is_valid, malformed = validate_data_timestamps(cds.to_dict())

        assert is_valid is False
        assert any('entsoe' in m for m in malformed), malformed

    def test_clean_payload_passes(self):
        from utils.helpers import validate_data_timestamps

        is_valid, malformed = validate_data_timestamps(_energy_price_combined().to_dict())
        assert is_valid is True, malformed
