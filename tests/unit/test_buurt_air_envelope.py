"""
Issue #17: pins the published shape of `air_quality_buurt.json` to match
`weather_forecast_buurt.json` / `solar_forecast_buurt.json` so consumers
can use one envelope-traversal code path for all three buurt feeds.

Before this fix the air-quality file was a CombinedDataSet, serialising to
{version, loc1: {...}, loc2: {...}} — location keys at the root, no
top-level metadata/data wrapper. FyE silently saw `available: []` after
PR #10's cron ran.

File: tests/unit/test_buurt_air_envelope.py
Created: 2026-06-06
"""

import pytest

from data_fetcher import assemble_buurt_air_envelope
from utils.data_types import EnhancedDataSet


def _ds(station, ts_to_pollutants, extra_meta=None, components=None):
    """Build a per-location Luchtmeetnet-shaped EnhancedDataSet.

    Matches the actual key set produced by `LuchtmeetnetCollector._get_metadata`
    (including `components`) so envelope tests don't pass on incomplete
    fixtures.
    """
    metadata = {
        'data_type': 'air',
        'source': 'Luchtmeetnet API',
        'units': 'µg/m³',
        'station': station,
        'station_location': f'{station}_loc',
        'station_latitude': 52.0,
        'station_longitude': 4.0,
        'requested_latitude': 51.9,
        'requested_longitude': 3.9,
        'city': 'Arnhem',
        'components': components if components is not None else ['NO2', 'PM10'],
    }
    if extra_meta:
        metadata.update(extra_meta)
    return EnhancedDataSet(metadata=metadata, data=ts_to_pollutants)


class TestEnvelopeShape:
    """The envelope must match the {metadata, data: {loc: {...}}} shape used
    by the weather/solar buurt feeds. This is the core invariant the issue
    fixes — every test in this class would have failed against the old
    CombinedDataSet-based assembler.
    """

    def test_returns_enhanced_dataset_not_combined(self):
        locs = [{'name': 'L1', 'lat': 52.0, 'lon': 4.0}]
        ds = _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}})
        result = assemble_buurt_air_envelope(locs, [ds])
        assert isinstance(result, EnhancedDataSet)
        # CombinedDataSet would have `.datasets`; EnhancedDataSet has `.data`.
        assert hasattr(result, 'data')
        assert not hasattr(result, 'datasets')

    def test_payload_has_metadata_and_data_at_top_level(self):
        locs = [{'name': 'L1', 'lat': 52.0, 'lon': 4.0}]
        ds = _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}})
        payload = assemble_buurt_air_envelope(locs, [ds]).to_dict()
        assert set(payload.keys()) == {'metadata', 'data'}

    def test_data_block_keyed_by_location_name(self):
        locs = [
            {'name': 'Elsweide_Arnhem_NL', 'lat': 52.0, 'lon': 4.0},
            {'name': 'Elderveld_Arnhem_NL', 'lat': 52.1, 'lon': 4.1},
        ]
        datasets = [
            _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}}),
            _ds('NL002', {'2026-06-06T12:00:00+02:00': {'PM10': 15.0}}),
        ]
        payload = assemble_buurt_air_envelope(locs, datasets).to_dict()
        assert set(payload['data'].keys()) == {'Elsweide_Arnhem_NL', 'Elderveld_Arnhem_NL'}

    def test_no_version_key_at_root(self):
        """Regression: CombinedDataSet wrote `version` at root; new envelope
        must not. If a future refactor reintroduces it, this test fails."""
        locs = [{'name': 'L1', 'lat': 52.0, 'lon': 4.0}]
        ds = _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}})
        payload = assemble_buurt_air_envelope(locs, [ds]).to_dict()
        assert 'version' not in payload


class TestStationMetadataPreservation:
    """Per-location station info must survive the flatten (closest RIVM
    station + coordinates per buurt) — moved into metadata['stations']."""

    def test_stations_dict_keyed_by_location_name(self):
        locs = [
            {'name': 'L1', 'lat': 52.0, 'lon': 4.0},
            {'name': 'L2', 'lat': 53.0, 'lon': 5.0},
        ]
        datasets = [
            _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}}),
            _ds('NL002', {'2026-06-06T12:00:00+02:00': {'PM10': 15.0}}),
        ]
        payload = assemble_buurt_air_envelope(locs, datasets).to_dict()
        assert set(payload['metadata']['stations'].keys()) == {'L1', 'L2'}
        assert payload['metadata']['stations']['L1']['station'] == 'NL001'
        assert payload['metadata']['stations']['L2']['station'] == 'NL002'

    def test_components_field_preserved_per_station(self):
        """PR #20 review HIGH/MEDIUM: `components` (the list of pollutants
        the station actually measures) must be preserved. Without it,
        downstream consumers cannot validate that a snapped station
        actually reports the pollutant they're consuming.
        """
        locs = [
            {'name': 'L1', 'lat': 52.0, 'lon': 4.0},
            {'name': 'L2', 'lat': 53.0, 'lon': 5.0},
        ]
        datasets = [
            _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}},
                components=['NO', 'NO2', 'NOX', 'PM10', 'PM25']),
            _ds('NL002', {'2026-06-06T12:00:00+02:00': {'PM10': 15.0}},
                components=['PM10', 'PM25']),
        ]
        payload = assemble_buurt_air_envelope(locs, datasets).to_dict()
        assert payload['metadata']['stations']['L1']['components'] == [
            'NO', 'NO2', 'NOX', 'PM10', 'PM25'
        ]
        assert payload['metadata']['stations']['L2']['components'] == ['PM10', 'PM25']


class TestMutationIsolation:
    """PR #20 review HIGH+MEDIUM: deep copies must isolate the published
    envelope from the source collector's state. Any mutation on either
    side after assembly must NOT leak to the other.
    """

    def test_issue_details_location_tag_does_not_mutate_source(self):
        """Adding `details.location` during aggregation must not mutate
        the original issue dict on the collector's metadata. Before the
        fix this was a shallow `dict(issue)` copy, so the inner `details`
        dict was shared.
        """
        locs = [{'name': 'L_BUURT_X', 'lat': 52.0, 'lon': 4.0}]
        original_details = {'filtered': 60, 'total': 100}
        original_issue = {
            'check_name': 'station_completeness',
            'severity': 'critical',
            'message': 'degraded',
            'details': original_details,
        }
        ds = _ds(
            'NL001',
            {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}},
            extra_meta={'collector_quality_issues': [original_issue]},
        )

        assemble_buurt_air_envelope(locs, [ds])

        # The collector's original issue dict must NOT have gained `location`.
        assert 'location' not in original_issue['details']
        assert original_details == {'filtered': 60, 'total': 100}

    def test_published_data_isolated_from_source(self):
        """PR #20 review MEDIUM: the envelope's `data[<loc>]` must be a
        copy, not a reference to `aq_ds.data`. Otherwise any downstream
        mutation of either side poisons the other.
        """
        locs = [{'name': 'L1', 'lat': 52.0, 'lon': 4.0}]
        source_data = {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}}
        ds = _ds('NL001', source_data)

        envelope = assemble_buurt_air_envelope(locs, [ds])

        # Mutate the source AFTER assembly.
        ds.data['2026-06-06T12:00:00+02:00']['NO2'] = 999.0
        ds.data['2026-06-06T13:00:00+02:00'] = {'NO2': 1000.0}

        # Envelope must be unchanged.
        assert envelope.data['L1']['2026-06-06T12:00:00+02:00']['NO2'] == 20.0
        assert '2026-06-06T13:00:00+02:00' not in envelope.data['L1']


class TestInputValidation:
    """PR #20 review MINOR: silent zip-truncation on mismatched input
    lengths would drop the tail of buurt_aq_data without notice.
    """

    def test_length_mismatch_raises(self):
        locs = [
            {'name': 'L1', 'lat': 52.0, 'lon': 4.0},
            {'name': 'L2', 'lat': 52.1, 'lon': 4.1},
        ]
        ds = _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}})
        with pytest.raises(ValueError, match="length mismatch"):
            assemble_buurt_air_envelope(locs, [ds])  # 2 locs vs 1 result


class TestQualityIssueAggregation:
    """Per-location collector_quality_issues must aggregate to top-level
    so the pipeline data-quality gate still sees them after the flatten."""

    def test_aggregates_quality_issues_with_location_tag(self):
        locs = [
            {'name': 'L1', 'lat': 52.0, 'lon': 4.0},
            {'name': 'L2', 'lat': 53.0, 'lon': 5.0},
        ]
        ds_with_issue = _ds(
            'NL002',
            {'2026-06-06T12:00:00+02:00': {'PM10': 15.0}},
            extra_meta={
                'collector_quality_issues': [{
                    'check_name': 'station_completeness',
                    'severity': 'critical',
                    'message': '60/100 stations filtered',
                    'details': {'filtered': 60, 'total': 100},
                }],
            },
        )
        ds_clean = _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}})

        payload = assemble_buurt_air_envelope(locs, [ds_clean, ds_with_issue]).to_dict()

        issues = payload['metadata'].get('collector_quality_issues', [])
        assert len(issues) == 1
        assert issues[0]['check_name'] == 'station_completeness'
        # Location tag was added so downstream consumers can identify which
        # buurt the issue came from.
        assert issues[0]['details']['location'] == 'L2'
        # Original detail fields preserved.
        assert issues[0]['details']['filtered'] == 60

    def test_no_issues_key_when_all_locations_clean(self):
        locs = [{'name': 'L1', 'lat': 52.0, 'lon': 4.0}]
        ds = _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}})
        payload = assemble_buurt_air_envelope(locs, [ds]).to_dict()
        assert 'collector_quality_issues' not in payload['metadata']


class TestEmptyInputs:
    """Defensive: when all per-location collectors returned None (total
    outage), the assembler must return None so the caller skips the save
    block instead of writing a half-empty envelope.
    """

    def test_all_none_returns_none(self):
        locs = [{'name': 'L1', 'lat': 52.0, 'lon': 4.0}]
        assert assemble_buurt_air_envelope(locs, [None]) is None

    def test_empty_lists_returns_none(self):
        assert assemble_buurt_air_envelope([], []) is None

    def test_partial_success_includes_only_succeeded_locations(self):
        locs = [
            {'name': 'L1', 'lat': 52.0, 'lon': 4.0},
            {'name': 'L2', 'lat': 53.0, 'lon': 5.0},
        ]
        ds = _ds('NL001', {'2026-06-06T12:00:00+02:00': {'NO2': 20.0}})
        payload = assemble_buurt_air_envelope(locs, [ds, None]).to_dict()
        assert list(payload['data'].keys()) == ['L1']
        assert payload['metadata']['locations'] == ['L1']
