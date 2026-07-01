---
stack: Python 3.12, asyncio/aiohttp, pandas, GitHub Actions CI/CD
status: Production (daily automated collection since Oct 2024)
repo: github.com/ducroq/energydatahub
framework: agent-ready-projects v1.10.1
---

# energyDataHub

Automated energy market data collection platform for electricity price prediction at HAN University of Applied Sciences. Collects from 15+ APIs (ENTSO-E, NED, weather, gas/carbon markets) into encrypted JSON, published via GitHub Pages for downstream ML consumers (Augur).

## Before You Start

| When | Read |
|------|------|
| Starting any session | Compare `framework:` version above against [CHANGELOG](https://github.com/ducroq/agent-ready-projects/blob/master/CHANGELOG.md). If behind, surface drift before starting work — adopting changes is your call. |
| Adding a new collector | `collectors/base.py` — BaseCollector pattern, `collectors/entsoe_generation.py` — good example, `collectors/entsoe_hydro.py` — minimal example. Also see `collectors/_http_classifier.py` for the HTTP-status bail-out pattern (raise_if_permanent) — use it from `_fetch_raw_data` to skip retries on permanent client errors (422/400/401/403/404). |
| Changing data output format | `utils/data_types.py` — EnhancedDataSet/CombinedDataSet, `utils/schema_registry.py` — versioning + migration chain. **Any shape change requires bumping `CURRENT_SCHEMA_VERSION` + adding a `_migrate_X_to_Y` function + a SCHEMA_CHANGELOG entry**. The CI tripwire (`scripts/detect_schema_drift.py`) enforces this. |
| Modifying CI/CD pipeline | `.github/workflows/collect-data.yml` — daily collection workflow. Includes completeness tripwire + schema-drift tripwire (fail-mode since 2026-06-10; auto-classifies data-volatile feeds from committed history since 2026-06-14). Actions are SHA-pinned + Dependabot-managed (`.github/dependabot.yml`). |
| Working with encryption/publish | `utils/secure_data_handler.py`, `docs/CI_CD_SETUP.md` |
| Debugging data quality issues | `utils/data_quality.py` — FMEA validation. Per-dataset config via `get_dataset_validation_config()`. Missing-dataset severity via `DATASET_MISSING_SEVERITY` dict (single source of truth). A critical feed that is *upstream-empty* (source healthy, published no data for the window — `UpstreamNoDataError`) is downgraded `critical`→`warning` so the run still publishes the healthy feeds; the orchestrator passes `validate_pipeline(upstream_empty=…)` and only `SystemExit(1)`s on a genuine collector failure. A *sustained* gap (≥`UPSTREAM_EMPTY_ESCALATION_RUNS`=3 consecutive runs, tracked in the committed `data/_upstream_empty_streak.json`) escalates back to a hard failure so it can't degrade silently forever (#38). |
| Adding a published dataset | `memory/project_published_dataset_checklist.md` — 8-touchpoint checklist across `data_fetcher.py`, `utils/data_quality.py`, and `.github/workflows/collect-data.yml`. **Missing one silently breaks publishing** (BLOCKER on c40a53b). Read before wiring a new collector into the publish set. |
| Stuck or debugging something weird | `memory/gotcha-log.md` — problem-fix archive |
| Ending a session | Run `/curate` — reviews gotcha log, promotes patterns, syncs docs, surfaces stale memory |
| Monthly or after major restructuring | Run `/audit-context` — structural audit (duplication, wrong-layer placement, broken refs) |

## Hard Constraints

- All timestamps normalized to Europe/Amsterdam timezone
- All published data encrypted with AES-CBC + HMAC-SHA256 (keys in secrets.ini / GitHub Secrets)
- Never commit secrets.ini or API keys — use environment variables in CI (enforced by GitHub secret-scanning push protection since 2026-06-14; secrets.ini is gitignored)
- Schema changes must be backward-compatible (see `utils/schema_registry.py` migration chain)
- Collectors must inherit from BaseCollector — provides retry, circuit breaker, validation
- Never claim tests pass without running them (`python -m pytest tests/ -x`)

## Architecture

```
data_fetcher.py              # Main orchestrator — initializes collectors, runs async gather,
                             # writes data/_shape_signatures.json sidecar pre-encryption
collectors/
  base.py                    # BaseCollector ABC: retry, circuit breaker, validation,
                             # NonRetryableError for permanent failures (#25),
                             # UpstreamNoDataError (subclass) for "source healthy but
                             # published no data" — fast-fails, no circuit-breaker trip,
                             # sets last_run_no_upstream_data + CollectorStatus.NO_DATA (#38), and
                             # `_add_quality_issue()` hook + auto-reset in collect() +
                             # auto-deepcopy injection of metadata['collector_quality_issues']
                             # (refactoring H1, 4c59378). Use the hook — don't roll your own.
  _http_classifier.py        # Shared HTTP status classifier (raise_if_permanent) for
                             # 422/400/401/403/404 → NonRetryableError. Used by tennet.py;
                             # available for adoption by any collector that hits 4xx cascades.
  _openmeteo_shared.py       # Shared Semaphore + per-location retry/backoff for OpenMeteo*
  entsoe*.py                 # ENTSO-E family (prices, wind, flows, load, generation, hydro)
  entsoe_hydro.py            # Nordic hydro reservoirs (A72, weekly cadence, NO+SE) — #3 closed c40a53b
  energyzero.py / epex.py / elspot.py  # Day-ahead price collectors (NL/EU)
  tennet.py                  # TenneT TSO (imbalance prices, grid balance) — uses _http_classifier
  ned.py                     # NED.nl Dutch production
  market_proxies.py          # Carbon EUA + gas TTF prices
  openmeteo_weather.py       # Strategic + demand + buurt weather (replaces Google Weather)
  openmeteo_solar.py         # Strategic + buurt solar irradiance
  openmeteo_offshore_wind.py # Offshore wind farm forecasts (open-sea coords)
  luchtmeetnet.py            # Air quality (RIVM stations), buurt-level
  gie_storage.py             # Gas storage levels
  entsog_flows.py            # Gas pipeline flows
  googleweather.py / openweather.py / meteoserver.py  # RETIRED — kept for cold revert
utils/
  data_types.py              # EnhancedDataSet, CombinedDataSet — canonical {metadata, data} envelope
  data_quality.py            # FMEA validation. DATASET_MISSING_SEVERITY (single registry),
                             # EXPECTED_DATA_TYPE (MITM defense), get_dataset_validation_config(),
                             # validate_load_cross_field_consistency (#30, ratio threshold 0.40),
                             # depth-walking _count_data_points / _extract_timestamp_keys (#32)
  schema_registry.py         # Version detection + migration (v1.0 → v2.0 → v2.1 → v2.2 → v2.3 → v2.4).
                             # stamp_metadata embeds the version's changelog slice (Layer B).
  shape_signature.py         # Structural fingerprint for schema-drift detection (#27)
  secure_data_handler.py     # AES-CBC + HMAC-SHA256 encryption
  calendar_features.py       # Holiday/DST features
scripts/
  detect_schema_drift.py     # CI tripwire diffing data/_shape_signatures.json against HEAD (#27).
                             # Splits within-feed shape drift (fail) from catalog drift (warn)
                             # per the 2026-06-08 buurt-drift fix; CRITICAL_FEEDS escalates
                             # removed-critical-feed catalog drift to ::error::. Data-volatile
                             # feeds warn (not fail): membership = VOLATILE_SHAPE_FEEDS (declared
                             # seed/override) UNION derive_volatile_feeds() (auto-derived from
                             # committed shape history, so a recurring data-driven false positive
                             # self-classifies without an allowlist edit). --volatility-window N.
  backfill_entsoe.py / archive_to_monthly.py / backfill_gas_storage.py
  sample_observed_ranges.py  # One-shot diagnostic: sample data/ files per feed, compute observed
                             # min/max per field. Used to derive #28's SOLAR_FIELD_RANGES /
                             # LOAD_FIELD_RANGES. Re-run when adding a new per-field range bound.
  probe_tennet_windows.py    # One-shot diagnostic: probe TenneT API across windows to identify
                             # endpoint availability. Used for #25 root-cause analysis.
data/                        # Timestamped output (yymmdd_HHMMSS_*.json) + current copies +
                             # _shape_signatures.json sidecar (unencrypted, committed)
docs/                        # GitHub Pages: encrypted JSON + project documentation
.github/
  dependabot.yml             # github-actions ecosystem, weekly grouped — auto-bumps the
                             # SHA-pinned action versions so the pins don't rot (added 2026-06-14)
  workflows/
    collect-data.yml         # Daily 16:00 UTC collection + publish. Includes completeness
                             # tripwire (warn on missing files) + schema-drift tripwire
                             # (fail-mode since 2026-06-10; was --warn-only during bedding-in).
                             # Actions SHA-pinned (Node-24 since 2026-06-14).
    test.yml                 # PR/push test pipeline (path-filtered, Python 3.12 only)
```

## Key Paths

| Path | What it is |
|------|-----------|
| `data_fetcher.py` | Main orchestrator — all collector wiring, save logic, shape-signature sidecar emission |
| `collectors/base.py` | BaseCollector ABC with retry/circuit breaker + `NonRetryableError` |
| `collectors/_http_classifier.py` | Shared HTTP status classifier (`raise_if_permanent`) — adopt this when adding a new API collector |
| `collectors/__init__.py` | All collector exports |
| `utils/data_types.py` | EnhancedDataSet / CombinedDataSet classes (canonical envelope since v2.2) |
| `utils/schema_registry.py` | Schema versioning + migration chain (currently v2.4). `stamp_metadata` embeds changelog slice. |
| `utils/shape_signature.py` | Structural fingerprint for the schema-drift CI tripwire |
| `utils/data_quality.py` | FMEA quality validation. `DATASET_MISSING_SEVERITY` registry + `get_dataset_validation_config()` lookup. |
| `settings.ini` | Public config (location, encryption flag) |
| `secrets.ini` | API keys (gitignored) |
| `.github/workflows/collect-data.yml` | Daily CI/CD pipeline (collect → sidecar → completeness tripwire → schema-drift tripwire → quality gate → publish) |
| `scripts/detect_schema_drift.py` | CI tripwire — diffs `data/_shape_signatures.json` against `git show HEAD:`. Data-volatile feeds (declared + history-derived) warn instead of failing. |
| `scripts/backfill_entsoe.py` | Backfill missing ENTSO-E prices into historical files |
| `scripts/archive_to_monthly.py` | Decrypt `data/` files into `05. Data/YYYY-MM/` monthly archive (idempotent) |
| `tests/backtest_data_quality.py` | Run FMEA quality framework against all historical files |
| `tests/` | Unit + integration tests <!-- verify: python -m pytest tests/ --collect-only -q \| tail -1 --> |

## How to Work Here

```bash
# Run all tests
python -m pytest tests/ -x

# Run specific test file
python -m pytest tests/unit/test_base_collector.py -v

# Run data collection locally (needs secrets.ini)
python data_fetcher.py

# Backfill missing ENTSO-E data (idempotent, safe to re-run)
python scripts/backfill_entsoe.py --dry-run  # report only
python scripts/backfill_entsoe.py            # patch files

# Archive decrypted data into 05. Data/<YYYY-MM>/ (idempotent)
python scripts/archive_to_monthly.py --since 260201

# Run data quality backtest on historical files
python tests/backtest_data_quality.py

# Check schema drift locally (after a `python data_fetcher.py` run)
python scripts/detect_schema_drift.py --previous-ref HEAD --warn-only

# Check GitHub Actions status
gh run list --limit 5

# Trigger a manual collection run (requires PAT secret in workflow)
gh workflow run "Collect and Publish Data"
```

## Commit Conventions

Imperative mood, concise. Examples from history:
- `Add data quality framework, schema registry, and DST-aware calendar features`
- `Update energy data`
- `Fix EnergyZero hour-00 edge case`
