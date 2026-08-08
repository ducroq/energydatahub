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

_Not yet complete._
