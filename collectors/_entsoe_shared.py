"""
Shared per-zone delivery tracking for the ENTSO-E country-keyed collectors.

Why this exists
---------------
`EntsoeLoadCollector`, `EntsoeGenerationCollector`, `EntsoeWindCollector` and
`EntsoeHydroCollector` all fetch one bidding zone at a time and build a
`{zone: ...}` dict. A zone can fall out at either of two stages — its query
exhausts `_retry_single` (absent from `_fetch_raw_data`'s results), or it
returns data that parses to nothing (all four gate the per-zone assignment on
a truthiness check of the parsed records: `if country_data:` in load and wind,
`if country_parsed:` in generation and hydro). Either way it is visible only
as a `logger.warning`. Meanwhile
`_get_metadata` built `country_codes` / `zones` from `self.country_codes`, the
CONFIGURED list, so a degraded feed published an envelope asserting a zone that
was not in `data`. Nothing downstream could tell: `validate_completeness` counts
points across all zones and passes comfortably when one of two survives (the
floor is 24 aggregate points; DE_LU alone yielded 192 on the incident run), and `_validate_data`
iterates `data.items()` so a vanished zone contributes no warning at all.

This is the same defect `_openmeteo_shared.record_location_delivery` was
written for on 2026-08-14, one feed family over. It surfaced on 2026-08-29,
when ENTSO-E served sustained HTTP 503s and NL dropped out of
`load_forecast.json`, `generation_mix.json` and `wind_forecast.json`. The
envelope produced that run still claimed `country_codes: ['NL', 'DE_LU']` for
load (generation_mix is configured `['NL', 'DE_LU', 'BE']`). Nothing was
published — the schema-drift tripwire, which fingerprints the envelope and
knows nothing about electricity, was the only thing in the pipeline that
noticed, and it said so by aborting before the publish step, taking the 13
unchanged feeds down with it.

MEASURED AGAINST THE PARSED DATA, NOT THE FETCH. `record_zone_delivery` is
called from `_validate_data`, which receives the final normalised `data` — the
exact mapping that will be published — and runs once, after the retry loop and
before `_get_metadata`. An earlier draft recorded delivery at the end of
`_fetch_raw_data` and was refuted in review: a zone that fetches but parses to
nothing (a Solar-only DataFrame with no Wind columns; an all-NaN placeholder
publication; hydro points falling outside the requested window) was recorded
as delivered while absent from `data` — reproducing the very signature this
module exists to eliminate. The Open-Meteo precedent has no lossy parse stage
behind it; these four do. `record_zone_request` runs at the top of
`_fetch_raw_data` instead, solely to capture what this run asked for.

Deliberately NOT in scope
-------------------------
This makes a zone dropout honest and attributable; it does not decide what the
pipeline should do about it, and for most of these feeds it does NOT stop a
degraded run from failing the schema-drift tripwire. Severity stays 'warning'
(matching the Open-Meteo precedent), and on a degraded run the envelope
additionally gains `metadata.collector_quality_issues`, which is itself a
shape change. Per feed, as of 2026-08-30:

  load_forecast.json        aborts the publish. Unavoidable — CRITICAL_FEEDS
  generation_forecast.json  are excluded from every downgrade path.
  nordic_hydro.json         aborts. Eligible for MEMBER_MAPPED_FEEDS but
                            deliberately not registered yet; see the comment
                            in scripts/detect_schema_drift.py.
  generation_mix.json       warns TODAY, but only because the 2026-08-29
  wind_forecast.json        outage put a second shape hash in the observation
                            log and `derive_volatile_feeds` picked it up. That
                            is one outlier record inside a 60-observation
                            window — it slides out around late October 2026
                            and both revert to aborting. Do not read the
                            current warn as a property of these feeds.

So what this buys is not a working publish: it is that the envelope and the
quality report say WHICH zone went missing, instead of the shape hash being
the only witness. And even that reaches the committed quality report only for
a feed whose run survives the tripwire — on a CRITICAL_FEED the job dies
before the report is committed, leaving only the Actions run log. Whether a
degraded critical feed should still block the healthy ones is a separate
policy decision needing an Augur-side contract. The root fix for the
diagnostic-key half is hypothesis-log H7. See H6/H7.

Note on metadata shape: this adds NO new envelope key. New keys are a
published-schema change (CURRENT_SCHEMA_VERSION bump + migration +
SCHEMA_CHANGELOG entry, via the user-typed `/release`), and everything a
consumer needs is already in the `zone_completeness` issue's `details`:
`requested`, `delivered`, `missing`. What changes is that `country_codes`
narrows to the delivered zones — a list of str either way, so the shape
signature is unaffected — while the `zones` name-lookup table stays
full-width, since narrowing a dict-keyed field WOULD add fresh shape churn.
On a healthy run the published envelope is byte-identical to before.

CONSUMER CONTRACT: for the load / generation / wind family, on a degraded run
`zones` therefore lists a zone that `country_codes` and `data` do not.
`country_codes` is the authoritative delivered set and always equals `data`'s
key set; `zones` is a static display-name lookup, not a delivery claim.
Iterate `country_codes` (or `data`), never `zones`. `entsoe_hydro` has no
`zones` dict — its `country_names` is a list and narrows in step with
`country_codes`, so the two never disagree there.

File: collectors/_entsoe_shared.py
Created: 2026-08-30
"""

from __future__ import annotations

from typing import Any, Iterable, List, Mapping, Optional

# Check name for the structured signal. A constant because two call sites
# filter on it to stay idempotent — a bare literal there would silently stop
# matching if this were ever renamed.
ZONE_COMPLETENESS_CHECK = 'zone_completeness'

# Distinct from EntsoeHydroCollector's pre-existing 'completeness_per_zone',
# which flags a zone that IS present but under-populated (half-dark). This one
# flags a zone that is absent outright. Both can appear in one hydro report and
# they mean different things; neither subsumes the other.


def drop_issues(collector, check_name: str) -> None:
    """Remove this run's entries for one check, so re-running is idempotent.

    Under `collect()` both callers now run from `_validate_data`, which
    executes exactly once, so this is not load-bearing there — the earlier
    draft called them from the retried `_fetch_raw_data`, where it was. It is
    still needed because `_validate_data` is reachable directly from tests and
    scripts, which `_reset_quality_issues` (fired once at the top of
    `collect()`) does not cover.
    """
    collector._collector_quality_issues = [
        issue for issue in collector._collector_quality_issues
        if issue.get('check_name') != check_name
    ]


def record_zone_request(
    collector,
    requested: Optional[Iterable[str]] = None,
) -> None:
    """Record the zone list this run asked for, and clear any prior delivery.

    Call at the TOP of `_fetch_raw_data`. Clearing `_delivered_zones` here is
    what keeps `published_zones`' fallback honest across repeated `collect()`
    calls on one instance (data_fetcher reuses collector instances): without
    it, a run that returned early would publish the PREVIOUS run's zone list.

    Args:
        collector: the ENTSO-E collector instance.
        requested: optional override of the requested zone list, for
            collectors that accept a single-country override at call time
            (EntsoeWindCollector's `country_code` kwarg). A run that asked for
            one zone and got it is complete, not degraded.
    """
    collector._requested_zones = (
        list(requested) if requested is not None else list(collector.country_codes)
    )
    collector._delivered_zones = None


def record_zone_delivery(
    collector,
    data: Mapping[str, Any],
    severity: str = 'warning',
) -> None:
    """Record which requested zones survived into the published `data`.

    Call from `_validate_data`, which receives the final normalised mapping —
    see "MEASURED AGAINST THE PARSED DATA" above for why this is not
    `_fetch_raw_data`.

    Sets `collector._delivered_zones` for `_get_metadata` to consume, and
    raises a `zone_completeness` quality issue when anything is missing.

    Args:
        collector: the ENTSO-E collector instance (needs `.country_codes` and
            BaseCollector's `_add_quality_issue`).
        data: the parsed, normalised zone mapping. Only its keys are read.
        severity: 'info' | 'warning' | 'error' | 'critical'. Defaults to
            'warning' — see "Deliberately NOT in scope" above.
    """
    requested = requested_zones(collector)
    # Requested-and-present first (stable, config-ordered), then anything the
    # API leaked that we did not ask for. Filtering the unexpected zone out
    # would make `country_codes` disagree with `data`, breaking the consumer
    # contract in this module's header — and `entsoe_hydro._validate_data`
    # (#31) explicitly distrusts the "a zone in data was requested" invariant,
    # with a test pinning the leak case.
    unexpected = [z for z in data if z not in requested]
    collector._delivered_zones = (
        [z for z in requested if z in data] + unexpected
    )
    missing = [z for z in requested if z not in data]

    drop_issues(collector, ZONE_COMPLETENESS_CHECK)
    if not missing:
        return

    collector._add_quality_issue(
        check_name=ZONE_COMPLETENESS_CHECK,
        severity=severity,
        message=(
            f"{len(missing)} of {len(requested)} ENTSO-E zone(s) absent from "
            f"the published data: {', '.join(missing)}"
        ),
        details={
            'requested': requested,
            'delivered': list(collector._delivered_zones),
            'missing': missing,
        },
    )


def requested_zones(collector) -> List[str]:
    """The zone codes this run asked for.

    Falls back to the configured list when `record_zone_request` has not run —
    `_validate_data` and `_get_metadata` are both reachable without a fetch
    (direct calls, tests), and the configured list is correct there.
    """
    asked = getattr(collector, '_requested_zones', None)
    if asked is None:
        return list(collector.country_codes)
    return list(asked)


def published_zones(collector) -> List[str]:
    """The zone codes to publish in metadata: DELIVERED, not configured.

    Always equals `data`'s key set once `record_zone_delivery` has run, so a
    consumer can iterate it and index into `data` safely. Falls back to the
    requested list before any delivery has been recorded.

    CAN return an empty list. `collect()` does not read `_validate_data`'s
    `is_valid`, and three of the four collectors return `{}` from
    `_parse_response` without raising when every timestamp falls outside the
    requested window (only `entsoe_hydro` raises). `country_codes: []` has
    signature `{_kind: list, value_shape: null}` rather than `value_shape:
    str`, so that run drifts on this key too — harmless in practice, because
    an empty `data` block already drifts and `validate_completeness` fails the
    quality gate at 0 points before the tripwire is reached, but do not read
    the "list of str either way" note above as unconditional.
    """
    delivered = getattr(collector, '_delivered_zones', None)
    if delivered is None:
        return requested_zones(collector)
    return list(delivered)
