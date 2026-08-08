# Present-but-empty guard: roll out beyond buurt (#42)

## What & Why

A collector that times out on *every* location returns a truthy `EnhancedDataSet` with
`data={}`. That empty envelope gets saved, then `validate_completeness` hard-fails it
(`CRITICAL` on 0 points — `utils/data_quality.py:786`, `if actual_points == 0` at :813, message at :816), which promotes
`overall_status=critical` and aborts the entire daily publish — including the feeds that
collected fine.

`ad008df` (2026-07-07) patched this for the two buurt feeds only, by coercing them to
`None` in `data_fetcher.py` so they route through the non-blocking `'info'` missing path.
The same failure is untreated for the other feeds in the shared Open-Meteo request wave:
`demand_weather_forecast`, `weather_forecast_multi_location`, `solar_forecast`,
`offshore_wind`. Any one of them timing out on all locations aborts publish exactly as
buurt did. Buurt was patched because buurt is what happened to fail.

## Current Status

**Not started.** Issue #42 is open with three options written up and no decision taken.
Nothing in the tree has changed since `ad008df`.

Next concrete step: decide between the three options below, then implement. Option 3 is
the standing recommendation in the issue body.

## Decisions

- **2026-07-07 — patch buurt only, silent `'info'`, no fail-after-3.** Maintainer chose
  the narrow fix over #38-style escalation, so a *sustained* buurt outage is log-only.
  Deliberate: Augur does not consume the buurt feeds. This is the decision #42 exists to
  revisit for the feeds Augur *does* consume.

Still to decide — the three options from #42:

1. **Extend the empty→None guard** to the four feeds in `data_fetcher.py`. Simplest, but
   per-feed whack-a-mole, and the gotcha log's `derive_volatile_feeds` lesson explicitly
   warns against reactive allowlists.
2. **Root fix in `base.collect()`** — return `None` when `_validate_data` fails *and* the
   data is empty. Cleanest, but the blast radius covers every collector and its tests.
3. **Root fix in `validate_completeness`** — read the 0-points severity per-feed from
   `DATASET_MISSING_SEVERITY` (default `critical`) instead of hardcoding `CRITICAL`.
   Aligns present-but-empty with the missing-dataset path, which is the actual
   inconsistency, and preserves the important distinction: strategic and demand weather
   stay critical-when-empty, buurt stays `'info'`.

## Open Questions

- Options 2 and 3 both change behaviour for feeds that have never failed this way. What
  is the evidence that they *can*? The 2026-06-07 late-wave timeout regression is the
  documented precedent, but it has only ever been observed hitting buurt.
- Option 3 makes `DATASET_MISSING_SEVERITY` govern two different conditions (absent, and
  present-but-empty). Is that one concept or two? If a feed should be `'info'` when
  missing but `critical` when empty, one dict cannot say so.
- The empty path is currently exercised by **unit test only** — smoke run `28846743856`
  was green but buurt collected normally that run. Whatever ships needs a way to observe
  the empty path on a real run, or it inherits the "registered but never surfaced"
  pattern this repo has hit three times.

## Outcome

<!-- Fill in when the work lands or is abandoned. What shipped, what the durable lesson
     is, and where that lesson now lives (ADR / gotcha log / CLAUDE.md). Then update the
     pointer in memory/MEMORY.md and delete or archive this file. -->

**Landed 2026-08-08 in commit `7ff9623` — option 2 (extend the coercion), with the
time-boxing that resolves the objection to it.**

What shipped: `PRESENT_EMPTY_GRACE_FEEDS` in `utils/data_quality.py` lists the six
Open-Meteo-backed feeds; `data_fetcher` coerces any of them from present-but-empty to
`None` for the first two consecutive runs, then **stops coercing** on the run that would
reach `UPSTREAM_EMPTY_ESCALATION_RUNS`, letting the completeness gate fail the publish
loudly. Streaks share `data/_upstream_empty_streak.json` with the #38 counters (disjoint
keys). 10 contract tests in `tests/unit/test_present_empty_grace.py`; suite 704 → 714.

**How the open questions resolved:**
- *"What is the evidence these feeds can fail this way?"* — Run `30838120578` (2026-08-03)
  showed **every** offshore location timing out in a single wave (`Gemini_NL`,
  `HollandseKust_NL`, `HelgolandCluster_DE`, `IJmuidenVer_NL`, `Borssele_NL`) plus a NED
  timeout. The precedent was no longer buurt-only, which is what unblocked the decision.
- *"Is `DATASET_MISSING_SEVERITY` one concept or two?"* — Sidestepped. Option 3 was not
  taken, so that dict still governs only absence. Time-boxing gave the severity
  distinction a different axis (duration, not feed), which avoids overloading the registry.
- *"The empty path is unit-test-only."* — **Still true and still the weakest point.** The
  escalation branch has never run in production. It is now at least observable: the run
  log prints `N/3 before escalation` on every coerced feed, so the counter is visible in
  the operator log before it ever fires.

**Durable lesson** (now in `memory/gotcha-log.md` and CLAUDE.md): a grace that is not
time-boxed is indistinguishable from a suppressed error. The fix for "this transient
aborts the publish" is not "ignore it" but "ignore it for a bounded number of runs, then
stop ignoring it".

**Known interaction, found after the commit — read before pushing.** `offshore_wind` is not
a standalone published file: it is merged into `wind_forecast.json` through a
`CombinedDataSet` (`data_fetcher.py:~825`). So coercing `offshore_wind_data` to `None`
drops that sub-dataset from the envelope entirely, where previously an empty-but-present
block was added. Both states differ from the healthy shape, so `wind_forecast.json`'s
fingerprint churns on an offshore timeout **either way** — this change does not create the
problem, but it does change which wrong shape you get.

That matters because `wind_forecast.json` is one of the two feeds that failed the
schema-drift tripwire on 2026-08-03, and #43 means the volatility classifier cannot learn
it (a failing run commits no sidecar). The candidate mitigation is to add
`wind_forecast.json` and `ned_production.json` to the declared `VOLATILE_SHAPE_FEEDS` seed
in `scripts/detect_schema_drift.py`. **Deliberately not done here**: that loosens a CI gate
from fail to warn, which is a decision with its own review bar, and #43 is the principled
fix rather than another hand-maintained allowlist entry — the exact whack-a-mole
`derive_volatile_feeds()` was built to end.

This file can be deleted once `7ff9623` is pushed and one production run has exercised
the coercion path.
