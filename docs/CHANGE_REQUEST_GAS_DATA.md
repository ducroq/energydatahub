# Change Request: Gas Storage and Flow Data Collectors

**Date**: 2025-01-19
**Status**: Proposed
**Priority**: Medium

## Summary

Add two new collectors to enhance gas price forecasting capabilities and improve electricity price predictions (since gas-fired plants often set marginal electricity prices):

1. **GIE AGSI+ Collector** - European gas storage levels
2. **ENTSOG Collector** - Gas flow data at Dutch entry points

## Rationale

### Current State
- Gas prices are currently collected via Alpha Vantage using UNG ETF as a proxy (`market_proxies.py`)
- This is indirect (US market) and daily resolution only

### Value of Direct EU Gas Data
- **Storage levels are a strong price predictor** - when EU storage dips below ~80%, prices spike
- **Gas-fired peaker plants set marginal electricity price ~40% of the time in NL**
- Enables gas price forecasting as a separate output (not just electricity feature)
- Direct European data (TTF benchmark correlation) vs US proxy

## Implementation Details

### Collector 1: GIE Storage (`collectors/gie_storage.py`)

**Data Source**: Gas Infrastructure Europe AGSI+ Platform

| Aspect | Details |
|--------|---------|
| API | REST API, JSON response |
| Auth | Free API key (register at https://agsi.gie.eu) |
| Python Library | `gie-py` (`pip install gie-py`) |
| Data Fields | Fill level (%), working capacity, injection rate, withdrawal rate |
| Granularity | Country / Operator / Facility level |
| Coverage | EU-wide, historical data since 2011 |
| Update Frequency | Daily |

**Example Usage**:
```python
from gie import GiePandasClient

client = GiePandasClient(api_key="your_key")
df = client.query_gas_storage("NL", start="2025-01-01", end="2025-01-19")
```

**Output File**: `gas_storage.json`

**Data Structure**:
```json
{
  "metadata": {
    "data_type": "gas_storage",
    "source": "GIE AGSI+",
    "units": "percent",
    "country": "NL",
    "start_time": "...",
    "end_time": "..."
  },
  "data": {
    "2025-01-19T00:00:00+01:00": {
      "fill_level_pct": 72.5,
      "working_capacity_twh": 145.2,
      "injection_gwh": 0,
      "withdrawal_gwh": 892
    }
  }
}
```

**Resources**:
- API Docs: https://www.gie.eu/transparency-platform/GIE_API_documentation_v006.pdf
- Registration: https://agsi.gie.eu
- Python library: https://github.com/fboerman/gie-py
- PyPI: https://pypi.org/project/gie-py/

---

### Collector 2: ENTSOG Flows (`collectors/entsog_flows.py`)

**Data Source**: ENTSOG Transparency Platform

| Aspect | Details |
|--------|---------|
| API | Public REST API, no registration needed |
| Base URL | `https://transparency.entsog.eu/api/v1` |
| Auth | None required |
| Data Fields | Physical flow volumes, capacity bookings, congestion indicators |
| Granularity | Interconnection point level |
| Coverage | All EU gas transmission points |
| Update Frequency | Daily |

**Key Endpoints**:
- Operator Point Directions: `/operatorpointdirections`
- Operational Data: `/operationaldatas`

**Output File**: `gas_flows.json`

**Data Structure**:
```json
{
  "metadata": {
    "data_type": "gas_flows",
    "source": "ENTSOG Transparency Platform",
    "units": "kWh/d",
    "country": "NL",
    "start_time": "...",
    "end_time": "..."
  },
  "data": {
    "2025-01-19T00:00:00+01:00": {
      "entry_total_gwh": 1250,
      "exit_total_gwh": 980,
      "ttf_hub_flow_gwh": 2100
    }
  }
}
```

**Resources**:
- Platform: https://transparency.entsog.eu
- API Docs: https://transparency.entsog.eu/pdf/TP_REG715_Documentation_TP_API_v1.3.pdf
- Dashboard: https://gasdashboard.entsog.eu/

---

## Configuration Requirements

### secrets.ini additions
```ini
[api_keys]
gie = YOUR_GIE_API_KEY  # Required for GIE AGSI+
# ENTSOG requires no API key
```

### requirements.txt addition
```
gie-py>=0.3.0
```

### GitHub Secrets (for CI/CD)
- `GIE_API_KEY` - GIE AGSI+ API key

---

## Integration Points

### data_fetcher.py
Add to collector initialization:
```python
from collectors.gie_storage import GieStorageCollector
from collectors.entsog_flows import EntsogFlowsCollector

# In main collection tasks
gie_collector = GieStorageCollector(api_key=secrets.get('gie'))
entsog_collector = EntsogFlowsCollector()

tasks.extend([
    gie_collector.collect(today, tomorrow),
    entsog_collector.collect(today, tomorrow)
])
```

### BaseCollector Pattern
Both collectors should inherit from `BaseCollector` and follow existing patterns:
- Retry logic with exponential backoff
- Circuit breaker integration
- Timezone normalization to Europe/Amsterdam
- EnhancedDataSet output format

---

## Testing Requirements

- Unit tests for data parsing
- Integration tests with mock API responses
- Validation of timezone handling
- Circuit breaker behavior tests

---

## Estimated Scope

- 2 new collector files (~150-200 lines each)
- Updates to `data_fetcher.py` (~20 lines)
- Configuration updates
- Test files (~100 lines each)
- Documentation updates

---

## References

- Related existing collector: `collectors/market_proxies.py` (current gas proxy approach)
- Architecture docs: `docs/BASE_COLLECTOR_ARCHITECTURE.md`
- Similar pattern: `collectors/entsoe.py` (same API style as ENTSOG)
