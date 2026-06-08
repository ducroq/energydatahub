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
| `memory/project_published_dataset_checklist.md` | Wiring a new collector into the publish set | 8-touchpoint lock-step checklist; missing one silently breaks publishing |
| `memory/project_session_2026_06_08.md` | Picking up after the 4-issue + review-battery bundle | What shipped in 4c59378, multi-model review battery outcomes, follow-up issues #30/#31 |

## Current State

<!-- verify: gh run list --workflow=collect-data.yml --limit 1 — should show recent success
     verify: gh pr list --state merged --limit 10 — confirms recent shipping pace
     verify: gh issue list --state open — confirms open work -->

- **Pipeline**: Active development. Daily 16:00 UTC cron + workflow_dispatch on demand. Latest two sessions: 4c59378 + 5d1f64c (2026-06-08, closed #25/#28/#29 + multi-model review-battery integration, filed #30/#31); c40a53b + c5a0892 (2026-06-07, #3 hydro wire-in + reviewer-battery fixes). Older commit chain in `git log` and `memory/project_session_*.md`.
- **Active collectors**: 19 datasets reaching the published file set <!-- verify: python -c "import json; print(len(json.load(open('data/_shape_signatures.json'))['feeds']))" -->. `grid_imbalance` now soft-gated (warning, not silent-absent) per #25; balance_delta synthesised when endpoint dead (`metadata['balance_delta_status']`) with structured `balance_delta_synthesised` quality signal so Augur can distinguish synthesis from real balanced grid. `nordic_hydro` added 2026-06-07 (#3, c40a53b) — weekly cadence, NO+SE, leading indicator for NL import prices via NorNed; per-zone completeness signal added 2026-06-08 (#29, 4c59378). Completeness tripwire warns on missing expected files. Schema-drift tripwire (`scripts/detect_schema_drift.py`) runs in `--warn-only` mode initially; **2026-06-08 unblocking fix**: now splits within-feed shape drift (fail) from catalog drift (warn) so the 2026-06-21 fail-mode flip won't trip on transiently-recovered collectors. `CRITICAL_FEEDS` set escalates removed-critical-feed catalog drift to ::error::.
- **Schema version**: 2.3 (bumped 2026-06-07 in 6eab742). `working_capacity_twh` renamed to `gas_in_storage_twh` (semantic fix). `energy_price_forecast` and `wind_forecast` use canonical `{metadata, data}` envelope (v2.2). All published files now carry `metadata.schema_changelog_entry` so consumers can read human-readable change descriptions inline. **Breaking change for downstream** — see SCHEMA_CHANGELOG in `utils/schema_registry.py` for the migration path.
- **Resilience layers in place**:
  - `entsoe`/`entsoe_de` critical retry: up to 3 rounds × 5 min on failure; workflow exits non-zero if still missing
  - `BaseCollector._retry_single()` for sub-requests (per-border, per-country)
  - `BaseCollector._add_quality_issue()` + auto-reset in `collect()` + auto-deepcopy injection of `metadata['collector_quality_issues']` — single hook collapses the prior Luchtmeetnet/EntsoeHydro dialect divergence (refactoring H1 fix, 4c59378). New collectors emitting per-source quality signals should use this, not roll their own.
  - OpenMeteo: shared `Semaphore(6)` + per-location exponential backoff (1s → 2s → 4s) via `collectors/_openmeteo_shared.py`
  - Luchtmeetnet: 24h station cache, refuses to cache empty results, station-number regex validation, instance-scoped filter stats
  - Quality gate: pipeline-level `overall_status == 'critical'` blocks publish; `collector_quality_issues` from any collector metadata feeds into the gate
- **Known degraded datasets (out of scope for current PR cycle)**:
  - `ned_production`, `market_history` — completeness < 50%
  - `market_proxies` — staleness timestamp format issue
  - `grid_imbalance` (TenneT) — balance_delta endpoint 404s as of 2026-06-08; settlement_prices still healthy. Soft-gate active, `balance_delta_status='synthesised'` stamped in metadata. Auto-heals if/when TenneT restores the endpoint.
- **Open issues**: 5 total <!-- verify: gh issue list --state open --json number | jq length -->. #2 (JAO — needs API research), #9 (storage migration — defer), #21 (Liander watcher — low ROI), #30 (load cross-field consistency — defense-in-depth from security audit), #31 (EntsoeHydroCollector unexpected-zone defensive test). Recently-closed via `gh issue list --state closed`.

## Active Decisions

- Market history stored as separate `market_history.json` (not in `market_proxies.json`) to avoid breaking existing consumers
- Generation mix uses same `EntsoeGenerationCollector` class with expanded params, separate from French nuclear `generation_forecast.json`
- Time resolution: store each source at native resolution, defer alignment to consumers — see [ADR-001](../docs/decisions/ADR-001-time-resolution-strategy.md)
- In-repo memory (this file + topic files) over auto-memory, per agent-ready-projects ADR-001
- OpenMeteo `OPENMETEO_SEMAPHORE_CAP` = collector count (not lower) — empirically required to avoid Open-Meteo's CDN per-source cooldown affecting late-scheduled collectors. See `collectors/_openmeteo_shared.py` docstring.
- **Multi-collector concurrent upstream pattern**: when adding a new collector that hits an API already used by other collectors, assume the upstream applies per-source rate-limits, CDN cooldowns, or "no data" 4xx responses that look like transients. Five incidents in `memory/gotcha-log.md` (ENTSO-E 503 cascade, Luchtmeetnet 429, Open-Meteo 429 storm, Open-Meteo CDN cooldown timeout, TenneT 422→429) all share the same shape. Default prescription: (1) shared semaphore cap ≥ collector count, (2) per-location retry with exponential backoff, (3) classify non-retryable HTTP statuses (422/400/401/403/404) at the collector level instead of letting BaseCollector retry them. See `collectors/_openmeteo_shared.py` for the cap pattern and `collectors/_http_classifier.py::raise_if_permanent` for the canonical HTTP-status bail-out (extracted from `tennet.py` on 2026-06-07 specifically so the next ENTSO-E/NED/GIE incident is two lines away from being fixed). Any `_fetch_raw_data` permanent-exit path MUST raise `NonRetryableError`, never plain `Exception`/`ValueError` — the outer retry loop catches anything else.
- **Parallel hard-coded registries**: when you see two-or-more parallel lists keyed on the same identifier (anywhere — Python, YAML, JSON, doesn't matter), consolidate before adding the third instance. Two incidents: `data_quality.py` had 3 lists for missing-dataset severity (collapsed in 39a94d6 to one `DATASET_MISSING_SEVERITY` dict); `collect-data.yml` had 2 lists for publishable feeds (completeness tripwire at line 54 + docs-prepare at line 118) — adding #3's `nordic_hydro` to one but not the other was the BLOCKER flagged by the multi-model review battery on c40a53b. Hard rule: when adding the third item, collapse OR add a test that asserts the lists match.
- **Silent quality-gate skip**: validators that return "0 issues" look identical to "data is clean" — a registered validator might not actually run, and that goes unnoticed for months. Two incidents: GoogleWeather `API_KEY_INVALID` returned success exit codes for 7 months while the API was 401-ing every call; `validate_value_ranges` silently no-op'd on 2-level-nested feeds for ~3 months. Hard rule: when adding a new entry to a validation registry, **run a poison test** — inject an absurd value, assert the check fires. Don't trust that registration = enforcement. See commit c40a53b for the `_flatten_to_timestamp_records` fix that restored the 2-level-nesting path; #28 tracks the real range-bound issues it surfaced once enforcement was restored.
