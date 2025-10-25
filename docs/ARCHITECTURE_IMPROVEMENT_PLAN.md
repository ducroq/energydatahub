# Architecture Improvement Plan
**Energy Data Hub - Refactoring & Enhancement Roadmap**

**Date**: October 24, 2025
**Branch**: `dev`
**Status**: Planning Phase

---

## Executive Summary

This document outlines a comprehensive refactoring plan for the Energy Data Hub project to address:
- **Critical Bug**: Timezone malformation in Elspot data (+00:09 instead of +02:00)
- **Architecture Improvements**: Separation of concerns, testability, maintainability
- **Data Delivery**: Enhanced downstream integration with visualization frontend
- **Historical Data**: GitHub Actions-based archival replacing manual rclone

---

## Current System Overview

### Data Flow
```
API Sources → Collectors → Aggregation → Encryption → GitHub Pages → Visualizer
   (async)      (mixed)     (coupled)    (secure)    (published)    (Netlify)
```

### Critical Components
1. **Data Collection**: `data_fetcher.py` (cloud), `local_data_fetcher.py` (local)
2. **API Clients**: 7 separate fetcher modules (energy + weather)
3. **Data Processing**: `utils/` with helpers, data types, encryption
4. **Deployment**: GitHub Actions daily @ 16:00 UTC
5. **Frontend**: Hugo dashboard on Netlify (separate repo)

---

## Identified Issues

### 1. Critical Bug: Timezone Malformation ⚠️
**Priority**: CRITICAL
**File**: `energy_data_fetchers/nordpool_data_fetcher.py`

**Problem**: Elspot data shows `+00:09` timezone offset instead of proper `+02:00` (CEST) or `+01:00` (CET)

**Impact**:
- Trend misalignment in visualizer
- Invalid ISO 8601 format
- Client-side workarounds required

**Root Cause Hypothesis**: Timezone conversion error when parsing Nord Pool API response

### 2. Architecture Issues

#### A. Code Duplication
- `load_config()` vs `load_secrets()` overlap
- `local_data_fetcher.py` reimplements modular clients
- Commented-out code blocks throughout

#### B. Inconsistent Error Handling
- Some functions return `None`, others raise exceptions
- No retry mechanism for transient API failures
- Logging inconsistencies

#### C. Lack of Abstraction
- No base class for data collectors
- Each fetcher implements similar patterns independently
- Tight coupling in main orchestrator

#### D. Testing Gaps
- No unit test framework (only manual test scripts)
- No mocking for API calls
- Requires real API keys to test

#### E. Configuration Management
- Dual file system (settings.ini + secrets.ini) unclear
- Environment variable fallback logic duplicated

### 3. Data Delivery Issues

#### Frontend Integration
- **Current**: Visualizer fetches encrypted data from GitHub Pages
- **Issue**: No versioning or rollback mechanism
- **Missing**: Data quality validation before publish
- **Need**: Webhook trigger for Netlify rebuild

#### Historical Data
- **Current**: Commented-out rotation logic in workflow
- **Desired**: rclone-style backup to Google Drive via GitHub Actions
- **Challenge**: No automated archival of timestamped files

---

## Proposed Architecture

### New System Design

```
┌─────────────────────────────────────────────────────────────┐
│                     Data Collection Layer                    │
├─────────────────────────────────────────────────────────────┤
│  Abstract Base Collector (retry, error handling, logging)   │
│    ├── EnergyCollector (base for price data)                │
│    │   ├── EntsoeCollector                                  │
│    │   ├── EnergyZeroCollector                              │
│    │   ├── EpexCollector                                    │
│    │   └── ElspotCollector ← FIX TIMEZONE HERE              │
│    └── WeatherCollector (base for weather/air)              │
│        ├── OpenWeatherCollector                             │
│        ├── MeteoServerCollector                             │
│        └── LuchtmeetnetCollector                            │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   Data Processing Layer                      │
├─────────────────────────────────────────────────────────────┤
│  Normalizer: Timezone, units, format standardization        │
│  Validator: Data quality checks, timezone validation        │
│  Aggregator: Combine multiple sources into CombinedDataSet  │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    Storage & Delivery Layer                  │
├─────────────────────────────────────────────────────────────┤
│  Encryptor: SecureDataHandler (AES-CBC + HMAC-SHA256)       │
│  Publisher: GitHub Pages (current + timestamped)            │
│  Archiver: Google Drive backup via GitHub Actions           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    Downstream Integration                    │
├─────────────────────────────────────────────────────────────┤
│  Visualizer: Hugo dashboard (Netlify rebuild webhook)       │
│  API Consumers: Decryption + consumption                    │
└─────────────────────────────────────────────────────────────┘
```

### Directory Structure (Proposed)

```
energyDataHub/
├── collectors/                    # ← NEW: Unified collector layer
│   ├── base.py                   # Abstract base with retry/logging
│   ├── energy/
│   │   ├── __init__.py
│   │   ├── entsoe.py
│   │   ├── energy_zero.py
│   │   ├── epex.py
│   │   └── elspot.py            # ← FIX TIMEZONE HERE
│   └── weather/
│       ├── __init__.py
│       ├── openweather.py
│       ├── meteoserver.py
│       └── luchtmeetnet.py
├── processors/                    # ← NEW: Data processing logic
│   ├── __init__.py
│   ├── normalizer.py             # Timezone/unit normalization
│   ├── validator.py              # Data quality checks
│   └── aggregator.py             # Combine sources
├── storage/                       # ← NEW: Storage abstractions
│   ├── __init__.py
│   ├── encryptor.py              # SecureDataHandler wrapper
│   ├── local.py                  # Local file storage
│   ├── github_pages.py           # GitHub Pages publisher
│   └── gdrive.py                 # Google Drive archiver
├── utils/
│   ├── config.py                 # ← REFACTOR: Unified config
│   ├── timezone.py               # ← NEW: Centralized timezone handling
│   ├── logging.py                # ← NEW: Structured logging
│   ├── data_types.py             # Keep existing
│   └── secure_data_handler.py    # Keep existing
├── tests/                         # ← REFACTOR: Proper test suite
│   ├── unit/
│   │   ├── test_collectors/
│   │   ├── test_processors/
│   │   └── test_timezone.py     # ← CRITICAL: Test timezone fixes
│   ├── integration/
│   │   └── test_data_pipeline.py
│   └── fixtures/
│       └── mock_api_responses/
├── config/
│   └── data_sources.yaml         # ← NEW: Config-driven design
├── main.py                        # ← REFACTOR: Simplified orchestrator
├── local_fetcher.py              # ← DEPRECATE: Remove duplication
└── data_fetcher.py               # ← MIGRATE: Move to main.py
```

---

## Implementation Roadmap

### Phase 1: Critical Bug Fix (Week 1) 🔥
**Priority**: CRITICAL
**Branch**: `fix/elspot-timezone`

#### Tasks
- [ ] **1.1**: Locate timezone bug in `nordpool_data_fetcher.py`
- [ ] **1.2**: Implement `normalize_amsterdam_timestamp()` utility
- [ ] **1.3**: Add timezone validation before encryption
- [ ] **1.4**: Write unit tests for timezone normalization
- [ ] **1.5**: Deploy fix and monitor for 48 hours
- [ ] **1.6**: Verify no `+00:09` in new data

#### Success Criteria
```bash
# Zero malformed timestamps
grep -c '+00:09\|+00:18' data/energy_price_forecast.json  # Should be 0

# All timestamps valid ISO 8601 with correct Amsterdam offset
python -c "import json; data=json.load(open('data/energy_price_forecast.json')); \
  assert all('+02:00' in ts or '+01:00' in ts for ts in data['elspot']['data'].keys())"
```

#### Files to Modify
- `energy_data_fetchers/nordpool_data_fetcher.py` (fix)
- `utils/timezone_helpers.py` (add validation)
- `tests/unit/test_timezone.py` (new)

---

### Phase 2: Testing Infrastructure (Week 2) 🧪
**Priority**: HIGH
**Branch**: `feature/testing-framework`

#### Tasks
- [ ] **2.1**: Set up pytest configuration
- [ ] **2.2**: Create mock API response fixtures
- [ ] **2.3**: Write unit tests for all collectors
- [ ] **2.4**: Add integration tests for data pipeline
- [ ] **2.5**: Configure GitHub Actions for test automation
- [ ] **2.6**: Add coverage reporting

#### Test Coverage Goals
- Collectors: 80%+
- Processors: 90%+
- Utils: 95%+
- Overall: 85%+

#### Files to Create
- `pytest.ini`
- `tests/conftest.py`
- `tests/unit/test_collectors/*.py`
- `tests/fixtures/mock_responses.json`
- `.github/workflows/test.yml`

---

### Phase 3: Base Collector Architecture (Week 3-4) 🏗️
**Priority**: MEDIUM
**Branch**: `feature/base-collector`

#### Tasks
- [ ] **3.1**: Create `BaseCollector` abstract class
- [ ] **3.2**: Implement retry mechanism with exponential backoff
- [ ] **3.3**: Add structured logging
- [ ] **3.4**: Migrate Entsoe to new architecture
- [ ] **3.5**: Migrate remaining collectors
- [ ] **3.6**: Update orchestrator to use new collectors

#### Base Collector Features
```python
class BaseCollector(ABC):
    - fetch_data() → raw API response
    - parse_response() → normalized dict
    - validate() → data quality checks
    - collect() → orchestrate workflow with retry
    - normalize_timestamps() → timezone handling
    - log_collection_event() → structured logging
```

#### Files to Create
- `collectors/base.py`
- `collectors/energy/__init__.py`
- `collectors/weather/__init__.py`
- Migration of existing fetchers to new structure

---

### Phase 4: Data Processing Layer (Week 5) 🔄
**Priority**: MEDIUM
**Branch**: `feature/processors`

#### Tasks
- [ ] **4.1**: Extract normalization logic to `processors/normalizer.py`
- [ ] **4.2**: Create `processors/validator.py` with quality checks
- [ ] **4.3**: Implement `processors/aggregator.py` for data combination
- [ ] **4.4**: Add timezone validation middleware
- [ ] **4.5**: Create data quality report generation
- [ ] **4.6**: Unit tests for all processors

#### Validation Checks
- Timezone format validation (no +00:09, +00:18)
- Price range validation (catch outliers)
- Timestamp continuity (no gaps > 2 hours)
- Unit consistency (all EUR/MWh)
- Data freshness (not older than 48 hours)

---

### Phase 5: Configuration Management (Week 6) ⚙️
**Priority**: LOW
**Branch**: `feature/unified-config`

#### Tasks
- [ ] **5.1**: Create `config/data_sources.yaml`
- [ ] **5.2**: Consolidate `load_config()` and `load_secrets()`
- [ ] **5.3**: Add configuration validation with schema
- [ ] **5.4**: Environment-specific configs (dev, prod)
- [ ] **5.5**: Migrate from .ini to YAML
- [ ] **5.6**: Update documentation

#### Configuration Schema
```yaml
data_sources:
  entsoe:
    api_url: "https://transparency.entsoe.eu/api"
    timezone: "Europe/Amsterdam"
    units: "EUR/MWh"
    resolution: "hourly"
    retry_attempts: 3
    timeout: 30
  elspot:
    # ... similar structure
    normalize_timezone: true  # ← Flag for sources needing fixes
```

---

### Phase 6: Historical Data Archival (Week 7) 💾
**Priority**: MEDIUM
**Branch**: `feature/gdrive-archival`

#### Tasks
- [ ] **6.1**: Set up Google Service Account for GitHub Actions
- [ ] **6.2**: Create `storage/gdrive.py` archiver
- [ ] **6.3**: Implement upload logic with retry
- [ ] **6.4**: Add archival to GitHub Actions workflow
- [ ] **6.5**: Configure retention policy (keep 90 days locally)
- [ ] **6.6**: Add archival monitoring/alerts

#### GitHub Actions Integration
```yaml
- name: Archive to Google Drive
  run: python storage/gdrive.py upload data/*.json
  env:
    GDRIVE_SERVICE_ACCOUNT: ${{ secrets.GDRIVE_SA_KEY }}
    GDRIVE_FOLDER_ID: ${{ secrets.GDRIVE_FOLDER }}
```

#### Archival Structure
```
Google Drive/energyDataHub/
├── 2025/
│   ├── 10/
│   │   ├── 251024_161727_energy_price_forecast.json
│   │   ├── 251024_161727_weather_forecast.json
│   │   └── ...
│   └── 11/
└── 2024/
```

---

### Phase 7: Enhanced Data Delivery (Week 8) 🚀
**Priority**: MEDIUM
**Branch**: `feature/enhanced-delivery`

#### Tasks
- [ ] **7.1**: Add data versioning to output JSON
- [ ] **7.2**: Implement Netlify rebuild webhook trigger
- [ ] **7.3**: Add data quality metadata to output
- [ ] **7.4**: Create rollback mechanism for bad data
- [ ] **7.5**: Add CORS headers for API consumption
- [ ] **7.6**: Document API contract for downstream consumers

#### Enhanced Output Format
```json
{
  "version": "2.1",
  "generated_at": "2025-10-24T16:00:00+02:00",
  "data_quality": {
    "timezone_validation": "passed",
    "price_outliers": 0,
    "data_freshness": "ok",
    "completeness": 98.5
  },
  "entsoe": { ... },
  "elspot": { ... }
}
```

#### Webhook Integration
```yaml
- name: Trigger visualizer rebuild
  if: success()
  run: |
    curl -X POST -d {} ${{ secrets.NETLIFY_BUILD_HOOK }}
```

---

## Migration Strategy

### Backward Compatibility
- Keep existing `data_fetcher.py` during migration
- Run old and new systems in parallel for 2 weeks
- Compare outputs for consistency
- Gradual cutover after validation

### Deprecation Timeline
- **Week 1-4**: New system in development
- **Week 5-6**: Parallel operation (both systems)
- **Week 7**: New system primary, old as backup
- **Week 8**: Deprecate old system, remove `local_data_fetcher.py`

---

## Testing Strategy

### Unit Tests
```python
# tests/unit/test_timezone.py
def test_normalize_amsterdam_timestamp_summer():
    dt = datetime(2025, 7, 15, 12, 0)
    result = normalize_amsterdam_timestamp(dt)
    assert result == '2025-07-15T12:00:00+02:00'

def test_validate_timestamp_rejects_malformed():
    assert validate_timestamp('2025-10-24T12:00:00+00:09') == False
    assert validate_timestamp('2025-10-24T12:00:00+02:00') == True
```

### Integration Tests
```python
# tests/integration/test_data_pipeline.py
@pytest.mark.integration
async def test_full_pipeline():
    # Mock API responses
    # Run collection
    # Validate output format
    # Check timezone correctness
    # Verify encryption
```

### Data Quality Tests
```python
# tests/integration/test_data_quality.py
def test_no_malformed_timezones():
    data = load_latest_data()
    for source in ['entsoe', 'elspot', 'epex', 'energy_zero']:
        for timestamp in data[source]['data'].keys():
            assert not re.search(r'\+00:(09|18)', timestamp)
```

---

## Monitoring & Alerting

### Data Quality Monitoring
```yaml
# .github/workflows/data-quality-check.yml
- name: Validate data quality
  run: |
    python -m pytest tests/integration/test_data_quality.py
    if [ $? -ne 0 ]; then
      # Send alert
      curl -X POST $SLACK_WEBHOOK -d "Data quality check failed"
    fi
```

### Metrics to Track
- Collection success rate (per source)
- Data freshness
- Timezone validation pass rate
- API response times
- Encryption/upload success rate

---

## Success Metrics

### Phase 1 (Critical Bug Fix)
- ✅ Zero `+00:09` or `+00:18` in production data
- ✅ 100% timezone validation pass rate
- ✅ Visualizer trends align across sources

### Overall Project
- ✅ 85%+ test coverage
- ✅ All collectors inherit from BaseCollector
- ✅ Historical data archived to Google Drive
- ✅ Automated Netlify rebuild on data updates
- ✅ Zero critical bugs in production for 30 days

---

## Risk Assessment

### High Risk
- **Timezone Fix**: Could break existing visualizer if not backward compatible
  - *Mitigation*: Test with visualizer before deploy
- **Architecture Refactor**: Large code changes increase bug risk
  - *Mitigation*: Parallel operation, extensive testing

### Medium Risk
- **Google Drive Integration**: New dependency, authentication complexity
  - *Mitigation*: Thorough testing, fallback to local storage
- **Breaking Changes**: API consumers may depend on current format
  - *Mitigation*: Version output, deprecation notices

### Low Risk
- **Testing Infrastructure**: Non-production changes
- **Configuration Refactor**: Internal improvement

---

## Resource Requirements

### Development Time
- **Phase 1**: 3-5 days (critical path)
- **Phase 2-7**: 6-8 weeks
- **Total**: ~2 months

### Infrastructure
- Google Drive Service Account (free tier sufficient)
- GitHub Actions minutes (within free tier)
- Netlify builds (within free tier)

---

## Documentation Updates

### Files to Update
- `README.md`: New architecture diagram
- `CLAUDE.md`: Updated for new structure
- `docs/API_CONTRACT.md`: ← NEW: Document output format
- `docs/MIGRATION_GUIDE.md`: ← NEW: Old → new system
- `docs/TROUBLESHOOTING.md`: Common issues

---

## Next Steps

1. **Review this plan** with stakeholders
2. **Create GitHub issues** for each phase
3. **Set up project board** for tracking
4. **Start with Phase 1** (critical bug fix)
5. **Iterate and adjust** based on learnings

---

**Last Updated**: October 24, 2025
**Author**: Architecture Analysis
**Status**: Awaiting Approval
