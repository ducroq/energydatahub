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

## Continuation, 2026-09-01

**Landed.** Rebased onto `origin/main` (which had moved 2 commits), merged fast-forward,
pushed. 833 tests green on the rebased tree. Feature branch deleted. `main` = `3a6a255`.

**Smoke test, and it is the strongest evidence for the change.** Decrypted the real
published `load_forecast.json`, rebuilt the pandas series the collector would have
received *from that payload*, and replayed it through the real collector. The healthy
replay reproduces `fac9d3f0e1b94e116f2ddb4b52978908` — the exact hash of the artifact
live on Pages. The degraded replay (NL dropped) narrows `country_codes` to `['DE_LU']`,
keeps `zones` full-width, and raises `zone_completeness` naming NL. Unit tests use
synthetic frames; this used production data through production code.

Still uncovered: the live API path. That needs the first successful production run and
is blocked on the outage.

**2026-08-31 run failed too**, differently from 08-29: ~295 requests to ENTSO-E all 503,
zero `NoMatchingDataError`, so `entsoe`/`entsoe_de` were missing and the critical gate
aborted at the Collect step. Still 503 at 06:13 UTC on 09-01. The gate behaved correctly
— `energy_zero`/`epex`/`elspot` all collected, so it *could* have published a price feed
with the ENTSO-E half silently absent, and refused.

**Why that gate must not be relaxed** (measured downstream, recorded in MEMORY.md and
committed as `3a6a255`): Augur's merge order is `("elspot","epex","entsoe")` with
later-writes-win, so without entsoe the *worst* source becomes their training target.
epex runs mean +16.13 EUR/MWh against entsoe (MAE 21.16, corr 0.859) versus elspot's
+0.86 (MAE 9.02, corr 0.954), offset flat across lags so it is level bias not
misalignment. The epex path is **already live at 1.16% of training hours** — not
hypothetical, as first reported — and the gate bounds it rather than preventing it.

**Corrections I had to make, both mine.** I claimed the scheduling delay made Augur's
window "false on three of five runs"; verified against 106 publishes it is zero after
their 20:31 timeout, and the drift runs *earlier* month over month (18:04 → 17:11 →
16:38 UTC). I had sampled fire times, two from runs that published nothing, and reasoned
about publish times from them. Same wrong-denominator error as the Actions levers, inside
24 hours of writing that lesson down.

**Issues.** #45 updated (H7 confirmed; fix is larger than assumed — nested envelopes),
#44 (H6 trigger fired; registration would have been a net regression), #51 (root cause,
explicitly not settled at n=1), #50 (two more instances, cost model changed), #9
(`storage/gdrive.py` exists and is unwired). Filed #52 (shared per-host circuit breaker)
and #53 (delivered-vs-requested time span).

## Next session

1. **#45 / H7** — precondition for everything else in the drift tripwire.
2. **#52** — shared per-host breaker.
3. **#53** — horizon guard, diagnostics only.
4. First successful production run is the outstanding live validation of `23d47bc`.
