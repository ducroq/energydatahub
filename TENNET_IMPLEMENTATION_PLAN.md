# TenneT Grid Imbalance Collector - Implementation Plan

**Created:** 2025-11-15
**Status:** Ready for Implementation
**Related Project:** energyDataDashboard (Feature 1.2: Grid Status Indicator)

---

## Executive Summary

Add **TenneT system imbalance data collection** to energyDataHub as the 9th data collector. This will enable the energyDataDashboard to display real-time grid health indicators and analyze grid-price correlations.

**Why energyDataHub?**
- ✅ Already has 8 collectors with proven BaseCollector pattern
- ✅ Built-in retry logic, circuit breakers, caching
- ✅ Automated encryption and publishing to GitHub Pages
- ✅ Robust testing infrastructure (49% code coverage, 177 tests)
- ✅ Separation of concerns: Backend collects, frontend visualizes

**Why NOT energyDataDashboard?**
- ❌ Client-side only (Hugo static site, no backend)
- ❌ No infrastructure for continuous data collection
- ❌ Violates separation of concerns

---

## 🎯 Goal

**Create `collectors/tennet.py`** that:
1. Fetches TenneT system imbalance data (hourly resolution)
2. Processes and normalizes to Amsterdam timezone
3. Encrypts and publishes to GitHub Pages as `grid_imbalance.json`
4. Runs daily via GitHub Actions (16:00 UTC with other collectors)

---

## 📊 TenneT Data Source

### API/Data Endpoint
- **Source:** TenneT TSO (Dutch transmission system operator)
- **API Portal:** https://developer.tennet.eu/
- **Registration:** https://www.tennet.eu/registration-api-token
- **Library:** tenneteu-py (official Python client)
- **Format:** DataFrame (pandas)
- **Authentication:** API key required
- **Update Frequency:** PTU (15 minutes) resolution
- **Historical Data:** Available via settlement_prices and balance_delta endpoints

**IMPORTANT:** The old TenneT.org website was decommissioned on December 1, 2024. Use the new tennet.eu API instead.

### Data Fields
```csv
DateTime,SystemImbalance_MW,ImbalancePrice_EUR_MWh,Direction
2025-11-15T00:00:00+01:00,-45.2,48.50,long
2025-11-15T01:00:00+01:00,12.8,52.30,short
...
```

**Fields:**
- **SystemImbalance_MW**: Imbalance volume in megawatts
  - Negative = Long (oversupply)
  - Positive = Short (undersupply)
- **ImbalancePrice_EUR_MWh**: Imbalance settlement price
- **Direction**: "long" (oversupply) or "short" (undersupply)

### Data Interpretation
- **Balanced grid**: ±50 MW
- **Moderate imbalance**: 50-200 MW
- **High imbalance**: >200 MW (prices can spike to €3000-4000/MWh)

---

## 🏗️ Implementation Design

### 1. Collector Structure

**File:** `collectors/tennet.py`

```python
"""
TenneT System Imbalance Collector
----------------------------------
Collects Dutch grid system imbalance data from TenneT TSO.

Data includes:
- System imbalance volume (MW)
- Imbalance settlement price (EUR/MWh)
- Direction (short/long)

File: collectors/tennet.py
Created: 2025-11-15
Author: Energy Data Hub Project
"""

from collectors.base import BaseCollector, CollectorStatus
from utils.data_types import EnhancedDataSet
from datetime import datetime, timedelta
import logging
import aiohttp
import csv
from io import StringIO

logger = logging.getLogger(__name__)


class TennetCollector(BaseCollector):
    """Collector for TenneT system imbalance data."""

    BASE_URL = "https://www.tennet.org/english/operational_management/export_data.aspx"

    def __init__(self, *args, **kwargs):
        super().__init__(
            name="TenneT",
            data_type="grid_imbalance",
            *args,
            **kwargs
        )

    async def _fetch_raw_data(self, start_time: datetime, end_time: datetime) -> dict:
        """
        Fetch raw CSV data from TenneT API.

        Args:
            start_time: Start of data range (Amsterdam timezone)
            end_time: End of data range (Amsterdam timezone)

        Returns:
            Dict with raw CSV data

        Raises:
            aiohttp.ClientError: If API request fails
        """
        params = {
            'DataType': 'SystemImbalance',
            'StartDate': start_time.strftime('%Y-%m-%d'),
            'EndDate': end_time.strftime('%Y-%m-%d'),
            'Output': 'csv'
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(self.BASE_URL, params=params) as response:
                response.raise_for_status()
                csv_data = await response.text()

                logger.info(
                    f"Fetched TenneT data: {len(csv_data)} bytes",
                    extra={'correlation_id': self.correlation_id}
                )

                return {'csv_content': csv_data}

    def _parse_response(self, raw_data: dict, start_time: datetime, end_time: datetime) -> dict:
        """
        Parse TenneT CSV response into normalized dict.

        Args:
            raw_data: Dict containing 'csv_content' field
            start_time: Start of data range
            end_time: End of data range

        Returns:
            Dict with timestamps as keys, imbalance data as values
            {
                '2025-11-15T00:00:00+01:00': {
                    'imbalance_mw': -45.2,
                    'price_eur_mwh': 48.50,
                    'direction': 'long'
                },
                ...
            }
        """
        csv_content = raw_data['csv_content']
        reader = csv.DictReader(StringIO(csv_content))

        parsed_data = {}

        for row in reader:
            timestamp = row['DateTime']  # Already in Amsterdam timezone

            # Parse values
            imbalance = float(row['SystemImbalance_MW'])
            price = float(row['ImbalancePrice_EUR_MWh'])
            direction = 'long' if imbalance < 0 else 'short'

            parsed_data[timestamp] = {
                'imbalance_mw': imbalance,
                'price_eur_mwh': price,
                'direction': direction
            }

        logger.info(
            f"Parsed {len(parsed_data)} TenneT data points",
            extra={'correlation_id': self.correlation_id}
        )

        return parsed_data

    def _create_dataset(self, parsed_data: dict, start_time: datetime, end_time: datetime) -> EnhancedDataSet:
        """
        Create EnhancedDataSet from parsed TenneT data.

        Args:
            parsed_data: Dict of timestamp -> imbalance data
            start_time: Start of data range
            end_time: End of data range

        Returns:
            EnhancedDataSet with metadata and data
        """
        metadata = {
            'data_type': 'grid_imbalance',
            'source': 'TenneT TSO',
            'units': 'MW',
            'country': 'NL',
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'data_points': len(parsed_data),
            'collection_timestamp': datetime.now().isoformat()
        }

        # Convert to simplified format for chart display
        # Separate imbalance volume from price
        imbalance_data = {ts: data['imbalance_mw'] for ts, data in parsed_data.items()}
        price_data = {ts: data['price_eur_mwh'] for ts, data in parsed_data.items()}
        direction_data = {ts: data['direction'] for ts, data in parsed_data.items()}

        dataset = EnhancedDataSet(
            metadata=metadata,
            data={
                'imbalance': imbalance_data,
                'imbalance_price': price_data,
                'direction': direction_data
            }
        )

        return dataset
```

---

### 2. Integration into data_fetcher.py

**Update:** `data_fetcher.py`

```python
# Add import
from collectors.tennet import TennetCollector

# In main collection function, add TenneT collector:
async def collect_all_data():
    """Collect data from all sources."""

    # ... existing collectors ...

    # TenneT Grid Imbalance
    tennet_collector = TennetCollector(
        retry_config=RetryConfig(max_attempts=3),
        circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3)
    )

    tennet_data = await tennet_collector.collect(
        start_time=today_start,
        end_time=tomorrow_end
    )

    if tennet_data:
        combined_data['tennet_imbalance'] = tennet_data
        logger.info("✅ TenneT grid imbalance data collected")
    else:
        logger.warning("⚠️ TenneT data collection failed")
```

---

### 3. Published Data Format

**Endpoint:** `https://ducroq.github.io/energydatahub/grid_imbalance.json` (encrypted)

**Decrypted structure:**
```json
{
  "version": "1.0",
  "tennet_imbalance": {
    "metadata": {
      "data_type": "grid_imbalance",
      "source": "TenneT TSO",
      "units": "MW",
      "country": "NL",
      "start_time": "2025-11-15T00:00:00+01:00",
      "end_time": "2025-11-16T23:59:59+01:00",
      "data_points": 48,
      "collection_timestamp": "2025-11-15T16:05:23+01:00"
    },
    "data": {
      "imbalance": {
        "2025-11-15T00:00:00+01:00": -45.2,
        "2025-11-15T01:00:00+01:00": 12.8,
        "2025-11-15T02:00:00+01:00": -8.5,
        ...
      },
      "imbalance_price": {
        "2025-11-15T00:00:00+01:00": 48.50,
        "2025-11-15T01:00:00+01:00": 52.30,
        "2025-11-15T02:00:00+01:00": 45.00,
        ...
      },
      "direction": {
        "2025-11-15T00:00:00+01:00": "long",
        "2025-11-15T01:00:00+01:00": "short",
        "2025-11-15T02:00:00+01:00": "long",
        ...
      }
    }
  }
}
```

---

### 4. Testing

**Create:** `tests/unit/test_tennet_collector.py`

```python
import pytest
from collectors.tennet import TennetCollector
from datetime import datetime, timedelta

@pytest.fixture
def tennet_collector():
    return TennetCollector()

@pytest.mark.asyncio
async def test_tennet_fetch_success(tennet_collector):
    """Test successful data fetch from TenneT."""
    today = datetime.now()
    tomorrow = today + timedelta(days=1)

    data = await tennet_collector.collect(today, tomorrow)

    assert data is not None
    assert 'metadata' in data
    assert 'data' in data
    assert data['metadata']['source'] == 'TenneT TSO'

@pytest.mark.asyncio
async def test_tennet_parse_csv(tennet_collector):
    """Test CSV parsing logic."""
    raw_csv = """DateTime,SystemImbalance_MW,ImbalancePrice_EUR_MWh,Direction
2025-11-15T00:00:00+01:00,-45.2,48.50,long
2025-11-15T01:00:00+01:00,12.8,52.30,short"""

    raw_data = {'csv_content': raw_csv}
    parsed = tennet_collector._parse_response(raw_data, datetime.now(), datetime.now())

    assert len(parsed) == 2
    assert parsed['2025-11-15T00:00:00+01:00']['imbalance_mw'] == -45.2
    assert parsed['2025-11-15T01:00:00+01:00']['direction'] == 'short'

def test_tennet_circuit_breaker(tennet_collector):
    """Test circuit breaker activation on failures."""
    # Test circuit breaker logic
    assert tennet_collector.circuit_breaker.state == "closed"
```

---

## 📋 Implementation Checklist

### Phase 1: Core Implementation (Day 1-2)
- [ ] Create `collectors/tennet.py` with BaseCollector inheritance
- [ ] Implement `_fetch_raw_data()` method (TenneT CSV fetch)
- [ ] Implement `_parse_response()` method (CSV parsing)
- [ ] Implement `_create_dataset()` method (EnhancedDataSet creation)
- [ ] Add timezone normalization (Amsterdam timezone)
- [ ] Test manually with `python -c "from collectors.tennet import TennetCollector; ...""`

### Phase 2: Integration (Day 2-3)
- [ ] Update `data_fetcher.py` to include TennetCollector
- [ ] Add TenneT data to combined dataset
- [ ] Update encryption/publishing logic (should be automatic)
- [ ] Verify `grid_imbalance.json` is created in output
- [ ] Test end-to-end: Fetch → Parse → Encrypt → Publish

### Phase 3: Testing (Day 3)
- [ ] Create `tests/unit/test_tennet_collector.py`
- [ ] Test successful fetch
- [ ] Test CSV parsing
- [ ] Test error handling (API down, malformed CSV)
- [ ] Test circuit breaker activation
- [ ] Test retry logic
- [ ] Run full test suite: `pytest tests/unit/test_tennet_collector.py -v`

### Phase 4: Documentation (Day 3-4)
- [ ] Update `README.md` to mention TenneT as 9th collector
- [ ] Add TenneT to data sources table
- [ ] Update architecture diagram (if applicable)
- [ ] Document TenneT-specific configuration (if any)

### Phase 5: Deployment (Day 4)
- [ ] Update GitHub Actions workflow (should auto-include new collector)
- [ ] Verify GitHub secrets (ENCRYPTION_KEY, HMAC_KEY)
- [ ] Trigger manual workflow run
- [ ] Verify `grid_imbalance.json` published to GitHub Pages
- [ ] Decrypt and validate data format

### Phase 6: Verification (Day 4)
- [ ] Check GitHub Actions logs for successful collection
- [ ] Verify encrypted file on GitHub Pages
- [ ] Decrypt locally and validate JSON structure
- [ ] Monitor for 24-48 hours to ensure daily collection works
- [ ] Check circuit breaker metrics

---

## 🔗 Related Documentation

**energyDataDashboard:**
- [ADR-002: Grid Imbalance Data in energyDataHub](https://github.com/ducroq/energyDataDashboard/blob/main/docs/decisions/002-grid-imbalance-data-in-energydatahub.md)
- [ENERGYLIVEDATA_FEATURES_PLAN.md](https://github.com/ducroq/energyDataDashboard/blob/main/docs/ENERGYLIVEDATA_FEATURES_PLAN.md)

**energyDataHub:**
- [README.md](../README.md) - Project overview
- [BaseCollector](../collectors/base.py) - Collector pattern reference
- [Existing collectors](../collectors/) - Reference implementations

**TenneT:**
- [System Imbalance Data](https://www.tennet.org/english/operational_management/)
- [Export Data Portal](https://www.tennet.org/english/operational_management/export_data.aspx)

---

## ⚡ Quick Start (For Claude Session)

**To start implementing:**

1. Read `collectors/base.py` to understand BaseCollector pattern
2. Read an existing collector (e.g., `collectors/energyzero.py`) as reference
3. Create `collectors/tennet.py` following the structure above
4. Test incrementally: fetch → parse → create_dataset
5. Integrate into `data_fetcher.py`
6. Write tests
7. Deploy and verify

**Key patterns to follow:**
- Use `async def _fetch_raw_data()` for API calls
- Use `self.correlation_id` for logging
- Return `EnhancedDataSet` from `_create_dataset()`
- Let BaseCollector handle retry/circuit breaker logic
- Normalize all timestamps to Amsterdam timezone

---

## 🎯 Success Criteria

**TenneT collector is successful when:**
- ✅ Fetches system imbalance data from TenneT API
- ✅ Parses CSV correctly (imbalance MW, price EUR/MWh, direction)
- ✅ Normalizes to Amsterdam timezone
- ✅ Passes all unit tests
- ✅ Publishes encrypted `grid_imbalance.json` to GitHub Pages
- ✅ Runs daily via GitHub Actions without errors
- ✅ Handles API failures gracefully (retry + circuit breaker)
- ✅ energyDataDashboard can decrypt and visualize the data

---

**Status:** Ready for implementation
**Next:** Start Claude session in energyDataHub repository and implement TennetCollector
