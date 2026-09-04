"""
Span (time-extent) signatures — the missing fourth gate.

A gate covers exactly one of {presence, structure, span, values}. This project
had three of the four and the gap cost four publishes:

  presence   completeness tripwire + DATASET_MISSING_SEVERITY  — is the feed there?
  structure  shape_signature + detect_schema_drift             — is its shape right?
  span       THIS MODULE                                       — does it cover the window?
  values     validate_value_ranges                            — are the numbers sane?

Issue #51: `load_forecast` shipped 96 points / 1 day instead of 192 / 2 days for
four consecutive publishes from 2026-08-28, and nothing objected. A shape
signature cannot see it — 96 records and 192 records hash **identically**,
because the signature describes the shape of a record, not how many days of them
there are. `metadata.end_time` kept declaring +2 days throughout, so the envelope
over-declared its own coverage. The only thing that noticed was the downstream
consumer's t0 hold-back, a day later.

Three design decisions, each forced by measuring real payloads rather than
reasoning about them:

DAYS, NOT POINTS. `ned_production` carries `forecast` at 95 points and `actual`
at 39 within the same single day, because ENTSO-E actuals lag intraday. Point
counts are therefore noisy by construction and a point-based check would cry
every afternoon. Distinct calendar days are stable, and #51 was a day-count
change (2 → 1). An intra-day gap is a COMPLETENESS defect and belongs to
`validate_completeness`, not here — keeping that boundary is the whole point of
the taxonomy above.

PER MEMBER, NOT PER FEED. `load_forecast` keys `data` by bidding zone and the
#51 outage hit the zones unevenly — on 2026-09-04 at 07:59 UTC, NL had 2 days
and DE_LU had 1. Any per-feed roll-up (a sum, a max, a min over members) hides
one member going short behind its healthy siblings. So spans are recorded at the
member path that actually holds the timestamps.

SHORTFALL, NOT MISMATCH. `market_history` grows: gas_ttf was at 128 distinct
days and climbs every run. `nordic_hydro` and `gas_storage` are similar. A check
for "the span CHANGED" fires on all of them forever; a check for "the span is
BELOW what this member normally carries" fires only on a real loss.

Deliberately NOT a gate. It emits quality issues and feeds an alert; it never
fails a run. The measured reason is in `memory/gotcha-log.md` (2026-09-04): the
drift tripwire's only response to a problem is withholding all 20 feeds, and
adding a second blocking gate on top of that would cost far more availability
than the detection is worth. Detection and blocking are separate decisions here.

File: utils/span_signature.py
Created: 2026-09-04
"""

from __future__ import annotations

import json
import os
from collections import Counter
from typing import Any, Dict, List, Optional

# A timestamp key starts with an ISO date. Deliberately a prefix match and not
# a full parse: `_is_timestamp_str` in data_quality accepts date-only keys too
# (market_history is keyed by date), and the only thing needed here is the
# leading YYYY-MM-DD to bucket by day.
_DATE_LEN = 10


def _is_ts_key(k: Any) -> bool:
    """True for a string key beginning with an ISO date (YYYY-MM-DD)."""
    if not isinstance(k, str) or len(k) < _DATE_LEN:
        return False
    d = k[:_DATE_LEN]
    return (d[4] == '-' and d[7] == '-'
            and d[:4].isdigit() and d[5:7].isdigit() and d[8:10].isdigit())


# How deep to walk looking for a timestamp map. The deepest real case is
# `ned_production` at {energy_type: {forecast|actual: {ts: ...}}} = 3, and
# `market_history` at {series: {data: {date: ...}}} = 2. Four leaves headroom
# without letting a pathological payload walk forever.
_MAX_DEPTH = 4

# Separator for a nested member path. `/` matches how the observation log and
# the operator-facing messages already read (`ned_production: solar/actual`).
_SEP = '/'


def feed_spans(payload: Any, _depth: int = 0, _path: str = '') -> Dict[str, int]:
    """Map member path → number of distinct calendar days, for one feed payload.

    Walks the `data` block until it finds timestamp-keyed maps and records the
    day count at each. A feed whose `data` holds no timestamps at all — a pure
    snapshot like `market_proxies`' current prices — yields `{}`, and callers
    must treat that as "span is not a meaningful question here", never as zero.

    The empty path `''` means the timestamps sat directly under `data` with no
    member level (e.g. `calendar_features`).
    """
    if not isinstance(payload, dict) or _depth > _MAX_DEPTH or not payload:
        return {}

    ts_keys = [k for k in payload if _is_ts_key(k)]
    if ts_keys:
        # This IS the record collection. Stop here — descending further would
        # walk into individual records' fields.
        return {_path: len({k[:_DATE_LEN] for k in ts_keys})}

    out: Dict[str, int] = {}
    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        child = f'{_path}{_SEP}{key}' if _path else str(key)
        out.update(feed_spans(value, _depth + 1, child))
    return out


def spans_for_published_feeds(
    feed_payloads: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, int]]:
    """Build the span record for a run: feed name → {member path → days}.

    Takes the same `{feed_name: published_dict}` mapping that
    `signatures_for_published_feeds` takes, so both are computed from one
    in-memory copy of the payloads at the same point in `data_fetcher`.

    Feeds contributing no timestamp maps are omitted entirely rather than
    recorded as empty, so a snapshot feed never looks like a feed that lost
    all its days.
    """
    out: Dict[str, Dict[str, int]] = {}
    for name, payload in (feed_payloads or {}).items():
        if not isinstance(payload, dict):
            continue
        spans = feed_spans(payload.get('data'))
        if spans:
            out[name] = spans
    return out


# How many PRIOR observations of a given member are needed before its expected
# span is trusted. Mirrors MIN_PRIOR_OBSERVATIONS in detect_schema_drift, and
# exists for a reason measured on 2026-09-04: `derive_volatile_feeds` classifies
# on `>1 distinct value`, and that let `wind_forecast` be permanently
# reclassified off two excursions in sixty runs. A derived expectation with no
# evidence floor learns from noise. Ten keeps the check inert on a fresh clone
# and for the first ten runs after this lands, which is the safe direction.
MIN_SPAN_OBSERVATIONS = 10

# Fraction of a member's history that must agree on the modal day count before
# it is treated as that member's expected span. A member whose span genuinely
# varies run to run — a growing historical series — has no single expected
# value, and guessing one would produce a permanent false alarm.
#
# 0.6 measured across 30 vintages of every published feed (2026-09-04), and it
# sits in a WIDE EMPTY BAND rather than near anything:
#
#   13%, 30%   market_history carbon_eua / gas_ttf — genuinely growing series
#   --- nothing between 30% and 77% ---
#   77-87%     the day-ahead-dependent members: entsoe/entsoe_de 77%,
#              load_forecast NL+DE_LU 80%, nordic_hydro 80/87%,
#              market_proxies 83%, solar_forecast_buurt 85/86%
#   93-100%    everything else
#
# An earlier draft of this comment claimed the stable members sit at 93-100%.
# They do not — a third of them sit at 77-87%, because a member whose span
# depends on the day-ahead auction reads 1 day on any run collected before it
# clears. That is ~20% of historical runs and they are mostly early-hour manual
# dispatches. Those runs SHOULD report a shortfall: a pre-auction publish
# really is short, and the downstream consumer rejects it on size anyway.
MIN_SPAN_AGREEMENT = 0.6


def expected_spans(
    observations: List[Dict[str, Any]],
    min_observations: int = MIN_SPAN_OBSERVATIONS,
    min_agreement: float = MIN_SPAN_AGREEMENT,
) -> Dict[str, Dict[str, int]]:
    """Derive each member's expected span from history: feed → {member → days}.

    The expectation is the MODAL day count, not the mean or the max:

      - the mean is not a day count and a shortfall against 1.7 days is
        meaningless;
      - the max is set by the single best run ever seen, so any feed whose
        window legitimately moves reports a permanent shortfall.

    A member is included only when it has at least `min_observations` prior
    records AND at least `min_agreement` of them agree on the mode. Everything
    else is omitted, which means "no expectation" and therefore no check —
    silence here is deliberate and must not be read as "span is fine".

    Records with no `spans` key are ignored, so this is safe to run against a
    log written before spans were recorded (every record before 2026-09-04).
    """
    per_member: Dict[str, Dict[str, List[int]]] = {}
    for rec in observations:
        spans = rec.get('spans')
        if not isinstance(spans, dict):
            continue
        for feed, members in spans.items():
            if not isinstance(members, dict):
                continue
            for member, days in members.items():
                if isinstance(days, int) and days > 0:
                    per_member.setdefault(feed, {}).setdefault(member, []).append(days)

    out: Dict[str, Dict[str, int]] = {}
    for feed, members in per_member.items():
        for member, values in members.items():
            if len(values) < min_observations:
                continue
            mode, count = Counter(values).most_common(1)[0]
            if count / len(values) < min_agreement:
                continue
            out.setdefault(feed, {})[member] = mode
    return out


def span_shortfalls(
    current: Dict[str, Dict[str, int]],
    expected: Dict[str, Dict[str, int]],
) -> List[Dict[str, Any]]:
    """Members carrying FEWER days than their expected span.

    Returns one entry per shortfall, sorted worst-first by the shortfall ratio
    so an operator reading a truncated list sees the biggest loss:

        {'feed': 'load_forecast.json', 'member': 'DE_LU',
         'observed': 1, 'expected': 2, 'ratio': 0.5}

    Only reports members present in BOTH mappings. A member that vanished
    entirely is a MEMBER-set change and belongs to the drift tripwire's member
    -drift path (`classify_data_member_drift`); reporting it here as well would
    double-alarm one event through two instruments. A member carrying MORE days
    than expected is never a shortfall — `market_history` grows every run.
    """
    out: List[Dict[str, Any]] = []
    for feed, members in (expected or {}).items():
        observed_members = (current or {}).get(feed)
        if not isinstance(observed_members, dict):
            continue
        for member, exp_days in members.items():
            obs_days = observed_members.get(member)
            if not isinstance(obs_days, int) or obs_days >= exp_days:
                continue
            out.append({
                'feed': feed,
                'member': member,
                'observed': obs_days,
                'expected': exp_days,
                'ratio': round(obs_days / exp_days, 3),
            })
    out.sort(key=lambda e: (e['ratio'], e['feed'], e['member']))
    return out


def describe_shortfalls(shortfalls: List[Dict[str, Any]]) -> str:
    """One-line operator summary, worst first. Empty string when there are none."""
    if not shortfalls:
        return ''
    parts = [
        f"{s['feed']}:{s['member'] or '(root)'} {s['observed']}d of {s['expected']}d"
        for s in shortfalls
    ]
    return f"{len(shortfalls)} member(s) short of their usual span — " + '; '.join(parts)


def load_span_observations(path: str) -> List[Dict[str, Any]]:
    """Read the shared observation log, tolerating malformed lines.

    Deliberately reuses `data/_shape_observations.jsonl` rather than adding a
    second log: it is already appended every run (including runs that fail the
    drift gate, which is issue #43's whole point) and already committed by its
    own workflow step before the gate. A parallel log would be a second thing
    to wire, commit and keep in step — and this project's own gotcha log has
    two incidents of parallel registries drifting apart.
    """
    if not os.path.isfile(path):
        return []
    records: List[Dict[str, Any]] = []
    try:
        with open(path, encoding='utf-8') as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                if isinstance(rec, dict):
                    records.append(rec)
    except OSError:
        return []
    return records


def evaluate_spans(
    current: Dict[str, Dict[str, int]],
    observations_path: str,
    observed_at: Optional[str] = None,
    min_observations: int = MIN_SPAN_OBSERVATIONS,
) -> Dict[str, Any]:
    """Load history, derive expectations, and report BOTH the shortfalls and
    how many members could be judged at all.

    Returns:
        {"members_checked": int,           # members in the current run
         "members_with_expectation": int,  # of those, how many have history
         "shortfalls": [...]}              # see span_shortfalls

    The denominator is not decoration. On the first run after this shipped the
    caller reported "No span shortfalls — every member carries its usual number
    of days" while `members_with_expectation` was ZERO: one observation existed,
    the minimum is ten, so no member could be judged and no shortfall was
    reachable. A clean result that cannot distinguish "all verified" from
    "nothing verifiable" is the failure this project has logged three times
    (GoogleWeather, validate_value_ranges, TenneT metadata) and it was
    reproduced here within an hour of shipping the check. Callers MUST surface
    the denominator; see `scripts/report_span_shortfall.py`.

    `observed_at` MUST be the current run's own `observed_at` when the current
    record has already been appended to the log, so a member's first-ever short
    span cannot supply its own evidence and normalise itself. This is the same
    trap `volatile_feeds_from_observations` documents at length — there it was
    reproduced turning a genuine first break into a warning.
    """
    records = load_span_observations(observations_path)
    if observed_at is not None:
        records = [r for r in records if r.get('observed_at') != observed_at]
    expected = expected_spans(records, min_observations=min_observations)

    members_checked = sum(len(m) for m in (current or {}).values()
                          if isinstance(m, dict))
    members_with_expectation = sum(
        1
        for feed, members in expected.items()
        for member in members
        if isinstance((current or {}).get(feed), dict)
        and member in current[feed]
    )
    return {
        'members_checked': members_checked,
        'members_with_expectation': members_with_expectation,
        'min_observations': min_observations,
        'shortfalls': span_shortfalls(current, expected),
    }
