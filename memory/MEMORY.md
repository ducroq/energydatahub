# Memory

<!-- Loaded every session. Keep lean — use as index, deep knowledge in topic files.

     END-OF-SESSION CURATION (~5 min):
     1. Review gotcha-log for recurring patterns — promote them here or to topic files
     2. Check if any entries below are stale — retire them
     3. Update "Current State" to reflect what shipped or changed
     Monthly: audit everything. Prune as much as you add. -->

## Topic Files

| File | When to load | Key insight |
|------|-------------|-------------|
| `memory/gotcha-log.md` | Stuck or debugging | Problem-fix archive |

## Current State

- **Issues completed (2026-03-27)**: #5 market history accumulation, #6 NED publish (already done), #7 generation mix collector
- **Open issues**: #2 JAO interconnector, #3 Nordic hydro, #4 gap detection/backfill
- **Active collectors**: 15+ sources, daily automated via GitHub Actions at 16:00 UTC
- **Schema version**: 2.1 (v1.0 -> v2.0 -> v2.1 migration supported)

## Recently Promoted

<!-- Gotchas promoted from gotcha-log. Retire once they appear in their destination. -->

## Key File Paths

- `data_fetcher.py` — all collector wiring, save logic, quality reporting
- `collectors/market_proxies.py` — carbon/gas with yfinance fallback and history
- `collectors/entsoe_generation.py` — reusable for both nuclear-only and full generation mix

## Active Decisions

- Market history stored as separate `market_history.json` (not in `market_proxies.json`) to avoid breaking existing consumers
- Generation mix uses same `EntsoeGenerationCollector` class with expanded params, separate from French nuclear `generation_forecast.json`
- In-repo memory (this file) over auto-memory, per agent-ready-projects ADR-001
