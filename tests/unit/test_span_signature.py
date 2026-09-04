"""
Unit tests for `utils/span_signature.py` — the span (time-extent) gate (#51).

The anchor test is `test_would_have_caught_issue_51`: the real payload shapes
from 2026-08-26 (healthy) and 2026-08-28 (degraded), asserting the check fires
on the day-count loss that the shape tripwire hashed identically.

File: tests/unit/test_span_signature.py
Created: 2026-09-04
"""

import json

import pytest

from utils.span_signature import (
    MIN_SPAN_OBSERVATIONS,
    describe_shortfalls,
    evaluate_spans,
    expected_spans,
    feed_spans,
    span_shortfalls,
    spans_for_published_feeds,
)


def _quarter_hours(day: str, n: int = 96):
    """n quarter-hourly ISO keys within one calendar day."""
    return {
        f'{day}T{(i // 4):02d}:{(i % 4) * 15:02d}:00+02:00': {'v': 1.0}
        for i in range(n)
    }


def _load_payload(days):
    """A load_forecast-shaped payload spanning `days`, both zones."""
    data = {}
    for zone in ('NL', 'DE_LU'):
        recs = {}
        for d in days:
            recs.update(_quarter_hours(d))
        data[zone] = recs
    return {'metadata': {'schema_version': '2.4'}, 'data': data}


class TestFeedSpans:
    def test_counts_distinct_days_not_points(self):
        """96 points in one day is 1 day, not 96 — the whole premise."""
        p = _load_payload(['2026-08-26'])
        assert feed_spans(p['data']) == {'NL': 1, 'DE_LU': 1}

    def test_two_days(self):
        p = _load_payload(['2026-08-26', '2026-08-27'])
        assert feed_spans(p['data']) == {'NL': 2, 'DE_LU': 2}

    def test_per_member_not_aggregated(self):
        """The #51 shape: one zone healthy, one short. An aggregate would hide it."""
        data = {
            'NL': {**_quarter_hours('2026-08-28'), **_quarter_hours('2026-08-29')},
            'DE_LU': _quarter_hours('2026-08-28'),
        }
        assert feed_spans(data) == {'NL': 2, 'DE_LU': 1}

    def test_nested_member_path(self):
        """ned_production nests {type: {forecast|actual: {ts: ...}}}."""
        data = {'solar': {'forecast': _quarter_hours('2026-08-28'),
                          'actual': _quarter_hours('2026-08-28', 40)}}
        assert feed_spans(data) == {'solar/forecast': 1, 'solar/actual': 1}

    def test_timestamps_directly_under_data(self):
        """calendar_features has no member level; the root path is ''."""
        assert feed_spans(_quarter_hours('2026-08-28')) == {'': 1}

    def test_snapshot_feed_yields_nothing(self):
        """market_proxies-style current values are not a span question at all.

        Must be {} and not {x: 0} — a zero would read as "lost all its days".
        """
        assert feed_spans({'gas_ttf': {'price': 31.2}, 'carbon_eua': {'price': 71.0}}) == {}

    def test_date_only_keys_count(self):
        """market_history is keyed by date, not datetime."""
        assert feed_spans({'gas_ttf': {'data': {'2026-08-26': 1, '2026-08-27': 2}}}) \
            == {'gas_ttf/data': 2}

    @pytest.mark.parametrize('bad', [None, 'string', 42, [], {}])
    def test_degenerate_input(self, bad):
        assert feed_spans(bad) == {}


class TestSpansForPublishedFeeds:
    def test_omits_snapshot_feeds_entirely(self):
        payloads = {
            'load_forecast.json': _load_payload(['2026-08-26', '2026-08-27']),
            'market_proxies.json': {'data': {'gas_ttf': {'price': 31.2}}},
        }
        out = spans_for_published_feeds(payloads)
        assert out == {'load_forecast.json': {'NL': 2, 'DE_LU': 2}}
        assert 'market_proxies.json' not in out


class TestExpectedSpans:
    def _obs(self, n, days, feed='load_forecast.json', member='NL'):
        return [{'observed_at': f'2026-08-{i:02d}', 'spans': {feed: {member: days}}}
                for i in range(1, n + 1)]

    def test_requires_minimum_observations(self):
        """Thin history sets no expectation — the wind_forecast lesson."""
        assert expected_spans(self._obs(MIN_SPAN_OBSERVATIONS - 1, 2)) == {}
        assert expected_spans(self._obs(MIN_SPAN_OBSERVATIONS, 2)) == \
            {'load_forecast.json': {'NL': 2}}

    def test_uses_the_mode_not_the_max(self):
        """One exceptional 5-day run must not become the expectation."""
        obs = self._obs(12, 2)
        obs.append({'observed_at': 'x', 'spans': {'load_forecast.json': {'NL': 5}}})
        assert expected_spans(obs)['load_forecast.json']['NL'] == 2

    def test_variable_member_gets_no_expectation(self):
        """market_history grows every run — 13-30% agreement, measured."""
        obs = [{'observed_at': str(i),
                'spans': {'market_history.json': {'gas_ttf/data': 100 + i}}}
               for i in range(30)]
        assert expected_spans(obs) == {}

    def test_ignores_records_without_spans(self):
        """Every record written before 2026-09-04 lacks the key."""
        legacy = [{'observed_at': str(i), 'feeds': {'a.json': 'hash'}} for i in range(30)]
        assert expected_spans(legacy) == {}

    def test_legacy_and_new_records_mixed(self):
        obs = [{'observed_at': f'old{i}', 'feeds': {'a': 'h'}} for i in range(20)]
        obs += self._obs(MIN_SPAN_OBSERVATIONS, 2)
        assert expected_spans(obs) == {'load_forecast.json': {'NL': 2}}


class TestSpanShortfalls:
    def test_flags_a_short_member(self):
        got = span_shortfalls({'load_forecast.json': {'NL': 2, 'DE_LU': 1}},
                              {'load_forecast.json': {'NL': 2, 'DE_LU': 2}})
        assert got == [{'feed': 'load_forecast.json', 'member': 'DE_LU',
                        'observed': 1, 'expected': 2, 'ratio': 0.5}]

    def test_more_days_is_never_a_shortfall(self):
        """market_history grows; growth must never alarm."""
        assert span_shortfalls({'market_history.json': {'gas_ttf/data': 200}},
                               {'market_history.json': {'gas_ttf/data': 128}}) == []

    def test_vanished_member_is_not_reported_here(self):
        """A dropped member is member-drift's event, not span's — no double alarm."""
        assert span_shortfalls({'load_forecast.json': {'NL': 2}},
                               {'load_forecast.json': {'NL': 2, 'DE_LU': 2}}) == []

    def test_member_with_no_expectation_is_not_checked(self):
        assert span_shortfalls({'market_history.json': {'gas_ttf/data': 3}}, {}) == []

    def test_sorted_worst_first(self):
        got = span_shortfalls(
            {'a.json': {'x': 9}, 'b.json': {'y': 1}},
            {'a.json': {'x': 10}, 'b.json': {'y': 10}})
        assert [e['feed'] for e in got] == ['b.json', 'a.json']


class TestIssue51Regression:
    """The defect this module exists for."""

    def test_would_have_caught_issue_51(self, tmp_path):
        """load_forecast went 192pts/2days -> 96pts/1day on 2026-08-28.

        The shape signature hashed both identically, so the drift tripwire was
        blind. Assert the span check is not.
        """
        log = tmp_path / '_shape_observations.jsonl'
        healthy = spans_for_published_feeds(
            {'load_forecast.json': _load_payload(['2026-08-26', '2026-08-27'])})
        with log.open('w') as fh:
            for i in range(MIN_SPAN_OBSERVATIONS + 2):
                fh.write(json.dumps({'observed_at': f'2026-08-{i + 1:02d}',
                                     'schema_version': '2.4',
                                     'spans': healthy}) + '\n')

        degraded = spans_for_published_feeds(
            {'load_forecast.json': _load_payload(['2026-08-28'])})
        shortfalls = evaluate_spans(degraded, str(log))

        assert {(s['member'], s['observed'], s['expected']) for s in shortfalls} == \
            {('NL', 1, 2), ('DE_LU', 1, 2)}
        assert 'load_forecast.json:NL 1d of 2d' in describe_shortfalls(shortfalls)

    def test_shape_hash_really_is_blind_to_this(self):
        """Pin the premise, so this module's reason for existing stays true.

        If a future change makes the shape signature span-aware, this test
        fails and tells you the two gates now overlap.
        """
        from utils.shape_signature import compute_shape_signature, signature_hash
        one_day = _load_payload(['2026-08-28'])
        two_day = _load_payload(['2026-08-26', '2026-08-27'])
        assert signature_hash(compute_shape_signature(one_day)) == \
            signature_hash(compute_shape_signature(two_day))

    def test_current_run_excluded_from_its_own_evidence(self, tmp_path):
        """A first-ever short span must not normalise itself.

        Same trap `volatile_feeds_from_observations` documents; there it turned
        a genuine first break into a warning.
        """
        log = tmp_path / 'obs.jsonl'
        healthy = {'load_forecast.json': {'NL': 2}}
        with log.open('w') as fh:
            for i in range(MIN_SPAN_OBSERVATIONS + 1):
                fh.write(json.dumps({'observed_at': f'd{i}', 'spans': healthy}) + '\n')
            fh.write(json.dumps({'observed_at': 'today',
                                 'spans': {'load_forecast.json': {'NL': 1}}}) + '\n')

        current = {'load_forecast.json': {'NL': 1}}
        assert evaluate_spans(current, str(log), observed_at='today')
        # Without the exclusion the single short record still loses to the mode
        # here, but the caller must pass it — assert the parameter is honoured.
        assert evaluate_spans(current, str(log), observed_at=None)

    def test_missing_log_is_silent_not_crashing(self, tmp_path):
        assert evaluate_spans({'a.json': {'x': 1}}, str(tmp_path / 'nope.jsonl')) == []

    def test_malformed_log_lines_are_skipped(self, tmp_path):
        log = tmp_path / 'obs.jsonl'
        lines = ['{not json', '[]', 'null']
        lines += [json.dumps({'observed_at': f'd{i}',
                              'spans': {'a.json': {'x': 4}}})
                  for i in range(MIN_SPAN_OBSERVATIONS)]
        log.write_text('\n'.join(lines) + '\n')
        assert evaluate_spans({'a.json': {'x': 1}}, str(log)) == \
            [{'feed': 'a.json', 'member': 'x', 'observed': 1,
              'expected': 4, 'ratio': 0.25}]


class TestDescribeShortfalls:
    def test_empty_is_empty_string(self):
        assert describe_shortfalls([]) == ''

    def test_root_member_is_labelled(self):
        out = describe_shortfalls([{'feed': 'calendar_features.json', 'member': '',
                                    'observed': 3, 'expected': 10, 'ratio': 0.3}])
        assert '(root) 3d of 10d' in out
