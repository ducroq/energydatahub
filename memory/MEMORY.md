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

- **Pipeline**: Active development. 7 PRs merged 2026-06-06/07 (#10, #16, #18, #19, #20, #22, #23) + 14 direct-to-main commits on 2026-06-07 closing 5 issues end-to-end and addressing two reviewer batteries (tier-1 sweep 3dfc7fb → gas_storage hotfix 6eab742 → tier-2 sweep a74f662 → #27 schema-drift tripwire 7c0de64 → #25 TenneT structural fix 7673750 → #3 hydro collector 935c483 → reviewer bundle A 39a94d6 → reviewer bundle B 87566e7, plus four memory/doc refreshes). Daily 16:00 UTC cron + workflow_dispatch on demand.
- **Active collectors**: 22 datasets reaching the published file set. `grid_imbalance` now soft-gated (warning, not silent-absent) per #25. Completeness tripwire warns on missing expected files. Schema-drift tripwire (`scripts/detect_schema_drift.py`) runs in `--warn-only` mode initially; flip to fail-mode after ~2 weeks of clean runs (target ~2026-06-21).
- **Schema version**: 2.3 (bumped 2026-06-07 in 6eab742). `working_capacity_twh` renamed to `gas_in_storage_twh` (semantic fix). `energy_price_forecast` and `wind_forecast` use canonical `{metadata, data}` envelope (v2.2). All published files now carry `metadata.schema_changelog_entry` so consumers can read human-readable change descriptions inline. **Breaking change for downstream** — see SCHEMA_CHANGELOG in `utils/schema_registry.py` for the migration path.
- **Resilience layers in place**:
  - `entsoe`/`entsoe_de` critical retry: up to 3 rounds × 5 min on failure; workflow exits non-zero if still missing
  - `BaseCollector._retry_single()` for sub-requests (per-border, per-country)
  - OpenMeteo: shared `Semaphore(6)` + per-location exponential backoff (1s → 2s → 4s) via `collectors/_openmeteo_shared.py`
  - Luchtmeetnet: 24h station cache, refuses to cache empty results, station-number regex validation, instance-scoped filter stats
  - Quality gate: pipeline-level `overall_status == 'critical'` blocks publish; `collector_quality_issues` from any collector metadata feeds into the gate
- **Known degraded datasets (out of scope for current PR cycle)**:
  - `ned_production`, `market_history` — completeness < 50%
  - `market_proxies` — staleness timestamp format issue
  - `grid_imbalance` (TenneT) — 422→429 cascade (#25)
- **Open issues**: 5 total. Closed 2026-06-07: #4 (quick-win), #13, #14, #24, #26 (tier-1 sweep), #27 (schema-drift tripwire — Layer A + B). Structural fix shipped for #25 (TenneT 422 classifier + grid_imbalance soft-gate in 7673750); root-cause investigation of WHY 422 fires remains. #3 collector code + mocks shipped in 935c483 — awaiting one-shot smoke test against live ENTSO-E API before wire-in. **Still open**: #2 (JAO — needs API research), #3 (Nordic hydro — collector shipped, awaiting smoke test + wire-in), #9 (storage migration — defer), #21 (Liander watcher — low ROI), #25 (TenneT 422 root cause — structural noise removed, diagnosis remains).

## Active Decisions

- Market history stored as separate `market_history.json` (not in `market_proxies.json`) to avoid breaking existing consumers
- Generation mix uses same `EntsoeGenerationCollector` class with expanded params, separate from French nuclear `generation_forecast.json`
- Time resolution: store each source at native resolution, defer alignment to consumers — see [ADR-001](../docs/decisions/ADR-001-time-resolution-strategy.md)
- In-repo memory (this file + topic files) over auto-memory, per agent-ready-projects ADR-001
- OpenMeteo `OPENMETEO_SEMAPHORE_CAP` = collector count (not lower) — empirically required to avoid Open-Meteo's CDN per-source cooldown affecting late-scheduled collectors. See `collectors/_openmeteo_shared.py` docstring.
- **Multi-collector concurrent upstream pattern**: when adding a new collector that hits an API already used by other collectors, assume the upstream applies per-source rate-limits, CDN cooldowns, or "no data" 4xx responses that look like transients. Five incidents in `memory/gotcha-log.md` (ENTSO-E 503 cascade, Luchtmeetnet 429, Open-Meteo 429 storm, Open-Meteo CDN cooldown timeout, TenneT 422→429) all share the same shape. Default prescription: (1) shared semaphore cap ≥ collector count, (2) per-location retry with exponential backoff, (3) classify non-retryable HTTP statuses (422/400/401/403/404) at the collector level instead of letting BaseCollector retry them. See `collectors/_openmeteo_shared.py` for the cap pattern and `collectors/_http_classifier.py::raise_if_permanent` for the canonical HTTP-status bail-out (extracted from `tennet.py` on 2026-06-07 specifically so the next ENTSO-E/NED/GIE incident is two lines away from being fixed). Any `_fetch_raw_data` permanent-exit path MUST raise `NonRetryableError`, never plain `Exception`/`ValueError` — the outer retry loop catches anything else.
