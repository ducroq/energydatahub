# energyDataHub

Automated energy market data collection platform for electricity price prediction at HAN University of Applied Sciences. Collects from 15+ APIs (ENTSO-E, NED, weather, gas/carbon markets) into encrypted JSON, published via GitHub Pages for downstream ML consumers (Augur).

- **Stack**: Python 3.12, asyncio/aiohttp, pandas, GitHub Actions CI/CD
- **Status**: Production (daily automated collection since Oct 2024)
- **Repo**: github.com/ducroq/energydatahub
- **agent-ready-projects**: v1.3.2

## Before You Start

| When | Read |
|------|------|
| Adding a new collector | `collectors/base.py` — BaseCollector pattern, `collectors/entsoe_generation.py` — good example |
| Changing data output format | `utils/data_types.py` — EnhancedDataSet/CombinedDataSet, `utils/schema_registry.py` — versioning |
| Modifying CI/CD pipeline | `.github/workflows/collect-data.yml` — daily collection workflow |
| Working with encryption/publish | `utils/secure_data_handler.py`, `docs/CI_CD_SETUP.md` |
| Debugging data quality issues | `utils/data_quality.py` — FMEA-based validation framework |
| Stuck or debugging something weird | `memory/gotcha-log.md` — problem-fix archive |
| Ending a session | `memory/gotcha-log.md` — review, promote patterns, retire stale entries |

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
  entsoe*.py                 # ENTSO-E family (prices, wind, flows, load, generation)
  ned.py                     # NED.nl Dutch production
  market_proxies.py          # Carbon EUA + gas TTF prices
  googleweather.py           # Multi-location weather (10-day)
  openmeteo_*.py             # Solar, offshore wind, demand weather
  gie_storage.py             # Gas storage levels
  entsog_flows.py            # Gas pipeline flows
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
  test.yml                   # PR/push test pipeline
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
| `tests/` | Unit + integration tests (398 tests, ~67% coverage) |

## How to Work Here

```bash
# Run all tests
python -m pytest tests/ -x

# Run specific test file
python -m pytest tests/unit/test_base_collector.py -v

# Run data collection locally (needs secrets.ini)
python data_fetcher.py

# Check GitHub Actions status
gh run list --limit 5
```

## Commit Conventions

Imperative mood, concise. Examples from history:
- `Add data quality framework, schema registry, and DST-aware calendar features`
- `Update energy data`
- `Fix EnergyZero hour-00 edge case`
