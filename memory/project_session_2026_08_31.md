# Session 2026-08-31 — ENTSO-E zone delivery, two review rounds, Actions audit

## What happened

**Triage of the 2026-08-29 CI failure.** Run `33269881393` failed the schema-drift
tripwire. Root cause was upstream: ENTSO-E served sustained HTTP 503s and NL dropped
out of `load_forecast`, `generation_mix` and `wind_forecast`. Verified by decrypting
the committed publishes rather than reasoning from point counts. Nothing was
published — the tripwire exits before the docs-prepare and commit steps, so 13
unchanged feeds went down with it. Self-healed on 2026-08-30 (`33329553703`), both
feeds back to their exact pre-incident hashes.

**Fix shipped: `collectors/_entsoe_shared.py`** (`9ecf4f8`, branch
`entsoe-zone-delivery`, NOT pushed). Per-zone delivery tracking for the four
country-keyed ENTSO-E collectors — the zone-level analogue of
`_openmeteo_shared.record_location_delivery`, which fixed the identical bug for
locations 16 days earlier. Metadata publishes the delivered zone set; a
`zone_completeness` issue names what went missing. Shape-neutral on healthy runs
(verified against the committed sidecar), so no schema bump.

## Two review rounds, and what they cost

Round 1 (4 lenses) REFUTED the first draft: delivery was recorded at the end of
`_fetch_raw_data`, but all four `_parse_response` bodies gate on a truthiness check,
so a zone that fetches and then parses to nothing was recorded as delivered —
reproducing the exact defect being fixed. Moved to `_validate_data`, which measures
the parsed data.

Round 2 (5 lenses, on the actual commit — the reworked diff was NOT the reviewed
diff) found a blocker of its own: registering `nordic_hydro.json` in
`MEMBER_MAPPED_FEEDS` would ALSO bar it from `derive_volatile_feeds()`, and the
likelier drift there is `collector_quality_issues` arriving with both zones present,
which classifies `None` and hard-fails. Unregistered it fails once then self-classifies
volatile; registered it would abort every run forever. Registration dropped;
`scripts/detect_schema_drift.py` is now comment-only. The empirical evaluation of all
five zone-keyed feeds is kept there.

Round 2 also caught three factual errors I had introduced in the rework, including an
incident narrative for an actuals outage that never happened on 2026-08-29 (it was my
own live probe on 08-30, carried back onto the wrong date).

## GitHub Actions audit (estate-wide, cross-session)

Account hit 90% of 3,000 included minutes. **energydatahub is public, so its Actions
are free and contributed nothing.** Spend was `NexusMind` (~1,264 billed min, but
~82% in a burst that ended mid-month) and `dsp-workshop` (~1,031 billed min, still
running hot at ~100 min/day).

Six wrong calls between three sessions, every one plausible at the aggregate and wrong
one level down: my toolchain cache (93% of the build is render, not setup), their
concurrency (79% of supersedable time sits on `main`, which must not be cancelled),
their pip cache (shipped, measured at +41s/run, reverted — only 11s of 133s was
download), and three successive cache hit/miss greps each blind to a wording the
others caught. `dsp-workshop` shipped `paths-ignore` plus a `_freeze/` cache:
build 641s → 191s.

Trigger worth knowing estate-wide: `dsp-workshop`'s version-controlled agent-memory
layer was firing full site rebuilds on `/curate` commits. energydatahub already guards
against this via `test.yml` path filters; other repos running this framework may not.

## Open at session end

- `9ecf4f8` unpushed on `entsoe-zone-delivery`; `main` untouched.
- 2026-08-31 collect run had not fired by ~19:00 UTC (cron 16:00). Would answer #51.
- **H7 is the precondition** for the `nordic_hydro` registration and the root cause of
  the `generation_mix` / `wind_forecast` volatility fuse expiring ~late October.
- Horizon-coverage check (#51's class) — third instance of request-vs-response, and
  the only one with no equivalent guard. Diagnostics-only: Augur derives coverage from
  data, not metadata.
