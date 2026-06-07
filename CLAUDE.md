---
stack: Python 3.12, asyncio/aiohttp, pandas, GitHub Actions CI/CD
status: Production (daily automated collection since Oct 2024)
repo: github.com/ducroq/energydatahub
framework: agent-ready-projects v1.10.0
---

# energyDataHub

Automated energy market data collection platform for electricity price prediction at HAN University of Applied Sciences. Collects from 15+ APIs (ENTSO-E, NED, weather, gas/carbon markets) into encrypted JSON, published via GitHub Pages for downstream ML consumers (Augur).

## Before You Start

| When | Read |
|------|------|
| Starting any session | Compare `framework:` version above against [CHANGELOG](https://github.com/ducroq/agent-ready-projects/blob/master/CHANGELOG.md). If behind, surface drift before starting work — adopting changes is your call. |
| Adding a new collector | `collectors/base.py` — BaseCollector pattern, `collectors/entsoe_generation.py` — good example |
| Changing data output format | `utils/data_types.py` — EnhancedDataSet/CombinedDataSet, `utils/schema_registry.py` — versioning |
| Modifying CI/CD pipeline | `.github/workflows/collect-data.yml` — daily collection workflow |
| Working with encryption/publish | `utils/secure_data_handler.py`, `docs/CI_CD_SETUP.md` |
| Debugging data quality issues | `utils/data_quality.py` — FMEA-based validation framework |
| Stuck or debugging something weird | `memory/gotcha-log.md` — problem-fix archive |
| Ending a session | Run `/curate` — reviews gotcha log, promotes patterns, syncs docs, surfaces stale memory |
| Monthly or after major restructuring | Run `/audit-context` — structural audit (duplication, wrong-layer placement, broken refs) |

## Hard Constraints

- All timestamps normalized to Europe/Amsterdam timezone
- All published data encrypted with AES-CBC + HMAC-SHA256 (keys in secrets.ini / GitHub Secrets)
- Never commit secrets.ini or API keys — use environment variables in CI
- Schema changes must be backward-compatible (see `utils/schema_registry.py` migration chain)
- Collectors must inherit from BaseCollector — provides retry, circuit breaker, validation
- Never claim tests pass without running them (`python -m pytest tests/ -x`)

## Architecture

```
data_fetcher.py              # Main orchestrator — initializes collectors, runs async gather
collectors/
  base.py                    # BaseCollector ABC: retry, circuit breaker, validation
  _openmeteo_shared.py       # Shared Semaphore + per-location retry/backoff for OpenMeteo*
  entsoe*.py                 # ENTSO-E family (prices, wind, flows, load, generation)
  energyzero.py / epex.py / elspot.py  # Day-ahead price collectors (NL/EU)
  tennet.py                  # TenneT TSO (imbalance prices, grid balance)
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
  data_types.py              # EnhancedDataSet, CombinedDataSet
  data_quality.py            # FMEA validation framework
  schema_registry.py         # Version detection + migration (v1.0 → v2.0 → v2.1)
  secure_data_handler.py     # AES-CBC encryption
  calendar_features.py       # Holiday/DST features
data/                        # Timestamped output (yymmdd_HHMMSS_*.json) + current copies
docs/                        # GitHub Pages: encrypted JSON + project documentation
.github/workflows/
  collect-data.yml           # Daily 16:00 UTC collection + publish
  test.yml                   # PR/push test pipeline (path-filtered, Python 3.12 only)
```

## Key Paths

| Path | What it is |
|------|-----------|
| `data_fetcher.py` | Main orchestrator — all collector wiring and save logic |
| `collectors/base.py` | BaseCollector ABC with retry/circuit breaker |
| `collectors/__init__.py` | All collector exports |
| `utils/data_types.py` | EnhancedDataSet / CombinedDataSet classes |
| `utils/schema_registry.py` | Schema versioning and migration |
| `utils/data_quality.py` | FMEA quality validation |
| `settings.ini` | Public config (location, encryption flag) |
| `secrets.ini` | API keys (gitignored) |
| `.github/workflows/collect-data.yml` | Daily CI/CD pipeline |
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

# Check GitHub Actions status
gh run list --limit 5
```

## Commit Conventions

Imperative mood, concise. Examples from history:
- `Add data quality framework, schema registry, and DST-aware calendar features`
- `Update energy data`
- `Fix EnergyZero hour-00 edge case`
