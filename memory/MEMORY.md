# Memory

<!-- Loaded every session. Keep lean — navigational index only.
     Deep knowledge lives in topic files (linked below). Reference paths
     and architecture live in CLAUDE.md, not here.

     END-OF-SESSION CURATION (~5 min):
     1. Review gotcha-log for recurring patterns — promote them here or to topic files
     2. Check if any entries below are stale — retire them
     3. Update "Current State" to reflect what shipped or changed
     Monthly: audit everything. Prune as much as you add. -->

## Topic Files

| File | When to load | Key insight |
|------|-------------|-------------|
| `memory/gotcha-log.md` | Stuck or debugging | Problem-fix archive |
| `memory/project_data_backfill_gaps.md` | Outage recovery, prioritising manual runs | Which collectors can backfill vs lose data permanently |
| `memory/project_actions_optimization.md` | Adding workflows or scheduled triggers | Account-wide 3k min/month budget, what we cut to fit |
| `memory/project_entsoe_old_files.md` | Touching pre-Oct-2025 historical files | 26 files have malformed timestamps; skip them unless fixing root cause |

## Current State

<!-- verify: gh run list --workflow=collect-data.yml --limit 1 — should show recent success
     verify: gh pr list --state merged --limit 10 — confirms recent shipping pace
     verify: gh issue list --state open — confirms open work -->

- **Pipeline**: Active development. 7 PRs merged 2026-06-06/07 (#10, #16, #18, #19, #20, #22, #23). Daily 16:00 UTC cron + workflow_dispatch on demand. Latest manual run published cleanly.
- **Active collectors**: 22 datasets reaching the published file set. `grid_imbalance` silently absent due to TenneT 422→429 (tracked in #25).
- **Schema version**: 2.1 (unchanged; `air_quality_buurt.json` envelope shape changed in PR #20 but stays within v2.1 — see #24 / `utils/schema_registry.py` if revisiting).
- **Resilience layers in place**:
  - `entsoe`/`entsoe_de` critical retry: up to 3 rounds × 5 min on failure; workflow exits non-zero if still missing
  - `BaseCollector._retry_single()` for sub-requests (per-border, per-country)
  - OpenMeteo: shared `Semaphore(6)` + per-location exponential backoff (1s → 2s → 4s) via `collectors/_openmeteo_shared.py`
  - Luchtmeetnet: 24h station cache, refuses to cache empty results, station-number regex validation, instance-scoped filter stats
  - Quality gate: pipeline-level `overall_status == 'critical'` blocks publish; `collector_quality_issues` from any collector metadata feeds into the gate
- **Known degraded datasets (out of scope for current PR cycle)**:
  - `gas_storage` — data_quality validator mis-applies the 0-100% range to TWh/GWh fields (#24)
  - `ned_production`, `market_history` — completeness < 50%
  - `market_proxies` — staleness timestamp format issue
  - `grid_imbalance` (TenneT) — 422→429 cascade (#25)
- **Open issues**: 9 total. Recent: #24 (gas_storage validator), #25 (TenneT cascade). Older: #2/#3/#4 (collector additions), #9 (storage migration).

## Active Decisions

- Market history stored as separate `market_history.json` (not in `market_proxies.json`) to avoid breaking existing consumers
- Generation mix uses same `EntsoeGenerationCollector` class with expanded params, separate from French nuclear `generation_forecast.json`
- Time resolution: store each source at native resolution, defer alignment to consumers — see [ADR-001](../docs/decisions/ADR-001-time-resolution-strategy.md)
- In-repo memory (this file + topic files) over auto-memory, per agent-ready-projects ADR-001
- OpenMeteo `OPENMETEO_SEMAPHORE_CAP` = collector count (not lower) — empirically required to avoid Open-Meteo's CDN per-source cooldown affecting late-scheduled collectors. See `collectors/_openmeteo_shared.py` docstring.
