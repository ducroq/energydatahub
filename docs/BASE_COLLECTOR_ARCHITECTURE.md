# Base Collector Architecture

## Overview

The Base Collector Architecture provides a standardized, robust foundation for all data collectors in the Energy Data Hub. It eliminates code duplication, ensures consistent error handling, and provides built-in retry logic, logging, and metrics collection.

**Status**: ✅ Production Ready
**Created**: 2025-10-25
**Phase**: 3 - Core Architecture

## Key Benefits

- **Consistency**: All collectors behave the same way
- **Reliability**: Built-in retry with exponential backoff
- **Observability**: Structured logging with correlation IDs
- **Maintainability**: Single source of truth for common functionality
- **Quality**: Automatic data validation and normalization
- **Metrics**: Performance tracking and success rate monitoring

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      BaseCollector (ABC)                     │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Common Functionality (Implemented)                   │  │
│  │  • async collect() - main workflow orchestrator       │  │
│  │  • _retry_with_backoff() - exponential backoff        │  │
│  │  • _normalize_timestamps() - timezone handling        │  │
│  │  • _validate_data() - data quality checks             │  │
│  │  • _get_metadata() - metadata generation              │  │
│  │  • get_metrics() - performance metrics                │  │
│  │  • get_success_rate() - reliability tracking          │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Abstract Methods (Must Implement)                    │  │
│  │  • async _fetch_raw_data() - API call                 │  │
│  │  • _parse_response() - data parsing                   │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │ inherits
        ┌───────────────────┴────────────────────┐
        │                                        │
┌───────┴──────────┐                  ┌─────────┴──────────┐
│ ElspotCollector  │                  │ EntsoeCollector    │
│ (Nord Pool)      │                  │ (ENTSO-E)          │
│                  │                  │                    │
│ Implements:      │                  │ Implements:        │
│ • _fetch_raw..() │    ...etc...     │ • _fetch_raw..()   │
│ • _parse_resp()  │                  │ • _parse_resp()    │
└──────────────────┘                  └────────────────────┘
```

## Collection Workflow

The `collect()` method orchestrates the entire data collection process:

```
1. Generate correlation ID (for tracing)
   ↓
2. Fetch raw data (with retry & exponential backoff)
   ↓
3. Parse response to standardized format
   ↓
4. Normalize timestamps to Europe/Amsterdam
   ↓
5. Validate data quality
   ↓
6. Create EnhancedDataSet
   ↓
7. Record metrics
   ↓
8. Return dataset (or None if failed)
```

## Core Components

### 1. BaseCollector (Abstract Base Class)

**File**: `collectors/base.py`

**Purpose**: Provides common functionality for all data collectors

**Key Methods**:

```python
class BaseCollector(ABC):
    """Abstract base class for all data collectors."""

    def __init__(self, name, data_type, source, units, retry_config=None):
        """Initialize with collector metadata and retry configuration."""

    @abstractmethod
    async def _fetch_raw_data(self, start_time, end_time, **kwargs):
        """Fetch raw data from API - MUST BE IMPLEMENTED."""

    @abstractmethod
    def _parse_response(self, raw_data, start_time, end_time):
        """Parse API response - MUST BE IMPLEMENTED."""

    async def collect(self, start_time, end_time, **kwargs):
        """Main collection workflow - automatically handles:
        - Retry with exponential backoff
        - Timestamp normalization
        - Data validation
        - Metrics collection
        """
```

### 2. RetryConfig (Configuration)

**Purpose**: Configure retry behavior for transient failures

```python
@dataclass
class RetryConfig:
    max_attempts: int = 3           # Number of retry attempts
    initial_delay: float = 1.0      # Initial delay in seconds
    max_delay: float = 60.0         # Maximum delay between retries
    exponential_base: float = 2.0   # Exponential backoff base
    jitter: bool = True             # Add randomness to prevent thundering herd
```

**Default Behavior**:
- Attempt 1: Immediate
- Attempt 2: Wait ~1.0s
- Attempt 3: Wait ~2.0s
- Jitter adds ±50% randomness to delays

### 3. CollectorStatus (Enum)

**Purpose**: Track collection outcome

```python
class CollectorStatus(Enum):
    SUCCESS = "success"    # All data collected successfully
    FAILED = "failed"      # Collection failed completely
    PARTIAL = "partial"    # Some data collected, but with warnings
    SKIPPED = "skipped"    # Collection was skipped
```

### 4. CollectionMetrics (Dataclass)

**Purpose**: Record performance and quality metrics

```python
@dataclass
class CollectionMetrics:
    collection_id: str              # Unique correlation ID
    collector_name: str             # Name of collector
    start_time: datetime            # Collection start
    end_time: datetime              # Collection end
    duration_seconds: float         # Total duration
    status: CollectorStatus         # Success/Failed/Partial
    attempt_count: int              # Number of retry attempts
    data_points_collected: int      # Data points returned
    errors: List[str]               # Error messages
    warnings: List[str]             # Validation warnings
```

## Implementing a New Collector

### Step 1: Create Collector Class

```python
from collectors.base import BaseCollector, RetryConfig
from datetime import datetime
from typing import Dict

class MyCollector(BaseCollector):
    """Collector for My API."""

    def __init__(self, retry_config: RetryConfig = None):
        super().__init__(
            name="MyCollector",
            data_type="my_data_type",  # e.g., "energy_price", "weather"
            source="My API v1.0",
            units="my_units",          # e.g., "EUR/MWh", "°C"
            retry_config=retry_config
        )
```

### Step 2: Implement _fetch_raw_data()

```python
    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict:
        """
        Fetch raw data from API.

        This method should:
        - Make the API call
        - Handle API-specific authentication
        - Return raw response (any format)
        - Raise exceptions on errors (will be retried)
        """
        self.logger.debug(f"Fetching from My API: {start_time} to {end_time}")

        # Example: sync API wrapped in executor
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            my_api_client.get_data,
            start_time,
            end_time
        )

        if not response:
            raise ValueError("No data returned from API")

        return response
```

### Step 3: Implement _parse_response()

```python
    def _parse_response(
        self,
        raw_data: Dict,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Parse API response to standardized format.

        Must return:
            Dict mapping ISO timestamp strings to values

        Example:
            {
                '2025-10-25T12:00:00+02:00': 100.5,
                '2025-10-25T13:00:00+02:00': 105.2,
                ...
            }
        """
        data = {}

        for entry in raw_data['items']:
            # Parse timestamp (may be naive or in different timezone)
            timestamp = datetime.fromisoformat(entry['timestamp'])

            # No need to normalize here - BaseCollector does it automatically!
            # Just use ISO format
            data[timestamp.isoformat()] = entry['value']

        return data
```

### Step 4: (Optional) Override _get_metadata()

```python
    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict:
        """Add custom metadata to the dataset."""
        metadata = super()._get_metadata(start_time, end_time)

        # Add custom fields
        metadata.update({
            'api_version': '1.0',
            'resolution': 'hourly',
            'custom_field': 'custom_value'
        })

        return metadata
```

### Step 5: Use Your Collector

```python
from collectors.my_collector import MyCollector
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Create collector
collector = MyCollector()

# Collect data
amsterdam_tz = ZoneInfo('Europe/Amsterdam')
start = datetime(2025, 10, 25, 12, 0, tzinfo=amsterdam_tz)
end = start + timedelta(hours=24)

dataset = await collector.collect(start, end)

if dataset:
    print(f"Collected {len(dataset.data)} data points")
    print(f"Metadata: {dataset.metadata}")
else:
    print("Collection failed")

# Check metrics
metrics = collector.get_metrics(limit=1)
print(f"Duration: {metrics[0].duration_seconds:.2f}s")
print(f"Status: {metrics[0].status.value}")

# Check success rate
print(f"Success rate: {collector.get_success_rate():.0%}")
```

## Example: ElspotCollector

**File**: `collectors/elspot.py`

The Nord Pool Elspot collector demonstrates the architecture:

```python
class ElspotCollector(BaseCollector):
    def __init__(self, retry_config: RetryConfig = None):
        super().__init__(
            name="ElspotCollector",
            data_type="energy_price",
            source="Nord Pool Elspot API",
            units="EUR/MWh",
            retry_config=retry_config
        )

    async def _fetch_raw_data(self, start_time, end_time, country_code='NL', **kwargs):
        # Nord Pool API is synchronous, run in executor
        prices_spot = elspot.Prices()
        loop = asyncio.get_running_loop()

        fetch_func = partial(
            prices_spot.hourly,
            areas=[country_code],
            end_date=end_time.date()
        )

        prices_data = await loop.run_in_executor(None, fetch_func)

        if country_code not in prices_data.get('areas', {}):
            raise ValueError(f"Country {country_code} not found")

        return prices_data

    def _parse_response(self, raw_data, start_time, end_time):
        area_data = raw_data['areas']['NL']
        data = {}

        for day_data in area_data['values']:
            naive_timestamp = day_data['start']

            # Localize naive datetime properly
            if naive_timestamp.tzinfo is None:
                timestamp = localize_naive_datetime(
                    naive_timestamp,
                    start_time.tzinfo
                )
            else:
                timestamp = naive_timestamp

            # Filter to requested range
            if start_time <= timestamp < end_time:
                data[timestamp.isoformat()] = day_data['value']

        return data
```

## Retry Mechanism

### Exponential Backoff

The retry mechanism implements exponential backoff with jitter:

```
delay = min(
    initial_delay * (exponential_base ** (attempt - 1)),
    max_delay
) * (0.5 + random() * 0.5)  # if jitter enabled
```

**Example delays** (with default config):
- Attempt 1: Immediate (0s)
- Attempt 2: ~1.0s ± 50%
- Attempt 3: ~2.0s ± 50%

### When Retry Happens

Retry is triggered for ANY exception raised in `_fetch_raw_data()`:
- Network errors (ConnectionError, TimeoutError)
- API errors (HTTP 500, 503)
- Temporary failures (RateLimitError)

### When Retry Stops

Retry stops when:
1. Call succeeds
2. Max attempts exhausted
3. Non-retryable error (you can customize this by catching specific exceptions)

## Logging

### Structured Logging with Correlation IDs

Every collection gets a unique 8-character correlation ID for tracing:

```
[a1b2c3d4] Starting collection: 2025-10-25 12:00:00+02:00 to 2025-10-26 12:00:00+02:00
[a1b2c3d4] Fetching raw data...
[a1b2c3d4] Attempt 1/3
[a1b2c3d4] Parsing response...
[a1b2c3d4] Normalizing timestamps...
[a1b2c3d4] Validating data...
[a1b2c3d4] Collection complete: 24 data points in 1.23s (status: success)
```

### Log Levels

- **DEBUG**: Method entry/exit, intermediate steps
- **INFO**: Collection start/complete, retry delays
- **WARNING**: Data validation warnings, retry attempts
- **ERROR**: Collection failures, validation errors

## Data Validation

### Automatic Validation

The `_validate_data()` method automatically checks:

1. **Empty data**: Warns if no data points collected
2. **Null values**: Counts and warns about None values
3. **Data point count**: Warns if < 2 data points
4. **Timestamp format**: Validates timezone format
5. **Malformed timestamps**: Detects +00:09, +00:18 bugs

### Validation Results

Validation returns:
- `(True, [])` - No issues
- `(False, [warnings])` - Issues found

If warnings exist, status is set to `PARTIAL` instead of `SUCCESS`.

## Timestamp Handling

### Automatic Normalization

All timestamps are automatically normalized to `Europe/Amsterdam`:

```python
# Input (from API)
'2025-10-25T10:00:00Z'              # UTC
'2025-10-25T12:00:00+02:00'         # Already Amsterdam

# Output (after normalization)
'2025-10-25T12:00:00+02:00'         # Amsterdam (CEST)
'2025-10-25T12:00:00+02:00'         # Amsterdam (no change)
```

### Timezone Utilities Used

- `localize_naive_datetime()` - Properly localize naive datetimes
- `normalize_timestamp_to_amsterdam()` - Convert to Amsterdam timezone
- `validate_timestamp_format()` - Check for malformed offsets

## Metrics Collection

### Performance Metrics

Each collection records:
- **Duration**: Total time in seconds
- **Attempt count**: Number of retries
- **Data points**: Number of data points collected
- **Status**: Success/Failed/Partial
- **Errors**: List of error messages
- **Warnings**: List of validation warnings

### Accessing Metrics

```python
# Get last 10 collections
metrics = collector.get_metrics(limit=10)

for metric in metrics:
    print(f"{metric.collection_id}: {metric.status.value}")
    print(f"  Duration: {metric.duration_seconds:.2f}s")
    print(f"  Data points: {metric.data_points_collected}")
    print(f"  Warnings: {len(metric.warnings)}")

# Get success rate
success_rate = collector.get_success_rate()
print(f"Success rate: {success_rate:.0%}")
```

## Testing

### Test Structure

**File**: `tests/unit/test_base_collector.py`

**Coverage**: 12 comprehensive tests (99% coverage)

**Test Categories**:
1. **Successful Collection** - Happy path
2. **Retry Mechanism** - Transient failures
3. **Max Retries Exhausted** - Permanent failures
4. **Timestamp Normalization** - Timezone handling
5. **Metadata Generation** - Metadata correctness
6. **Metrics Collection** - Performance tracking
7. **Success Rate Calculation** - Reliability metrics
8. **Validation Warnings** - Data quality checks
9. **Empty Data Handling** - Edge cases
10. **Retry Config** - Configuration tests
11. **Multiple Collections** - Integration tests

### Running Tests

```bash
# Run all base collector tests
pytest tests/unit/test_base_collector.py -v

# Run with coverage
pytest tests/unit/test_base_collector.py --cov=collectors.base --cov-report=html

# Run specific test
pytest tests/unit/test_base_collector.py::TestBaseCollector::test_retry_on_failure -v
```

## Migration Guide

### Migrating Existing Fetchers

To migrate an existing fetcher to the new architecture:

1. **Identify the fetcher**: e.g., `energy_data_fetchers/nordpool_data_fetcher.py`

2. **Extract API logic**: Identify the API call and parsing logic

3. **Create new collector**: `collectors/nordpool.py`

4. **Implement abstract methods**:
   - `_fetch_raw_data()` - Extract API call logic
   - `_parse_response()` - Extract parsing logic

5. **Remove old code**: Delete duplicated retry, logging, validation logic

6. **Update imports**: Change imports from old fetcher to new collector

7. **Test**: Run existing tests and create new ones

### Backward Compatibility

Provide a compatibility function for existing code:

```python
# In collectors/elspot.py
async def get_Elspot_data(country_code, start_time, end_time):
    """Backward-compatible function for existing code."""
    collector = ElspotCollector()
    return await collector.collect(
        start_time=start_time,
        end_time=end_time,
        country_code=country_code
    )
```

## Future Enhancements

### Planned Improvements

1. **Circuit Breaker**: Automatically stop retrying if API is down
2. **Rate Limiting**: Built-in rate limit handling
3. **Caching**: Optional caching layer for duplicate requests
4. **Async Batch Collection**: Collect from multiple sources in parallel
5. **Alerting Integration**: Hook for alerting on failures
6. **Custom Validators**: Allow collectors to add custom validation rules

### Migration Roadmap

- [x] Phase 3: Base collector architecture (completed)
- [x] Phase 4: Migrate ENTSO-E collector ✅
- [x] Phase 4: Migrate EPEX collector ✅
- [x] Phase 4: Migrate EnergyZero collector ✅
- [x] Phase 4: Migrate OpenWeather collector ✅
- [x] Phase 4: Migrate Google Weather collector ✅
- [x] Phase 4: Migrate TenneT collector ✅
- [x] Phase 4: Add NED.nl collector ✅
- [x] Phase 5: Add Open-Meteo Solar collector ✅
- [x] Phase 5: Add Open-Meteo Weather (demand) collector ✅
- [x] Phase 5: Add Open-Meteo Offshore Wind collector ✅
- [x] Phase 5: Add ENTSO-E Wind collector ✅
- [x] Phase 5: Add ENTSO-E Flows collector ✅
- [x] Phase 5: Add ENTSO-E Load collector ✅
- [x] Phase 5: Add ENTSO-E Generation collector ✅
- [x] Phase 5: Add circuit breaker and rate limiting ✅

## Troubleshooting

### Common Issues

**Issue**: Tests fail with "EnhancedDataSet has no attribute 'data'"

**Solution**: Ensure `data_type` in metadata is one of: `energy_price`, `weather`, `sun`, `air`, or any other type (defaults to energy_price validation)

---

**Issue**: Retry not working

**Solution**: Check that your `_fetch_raw_data()` raises exceptions (not returning None). Only exceptions trigger retry.

---

**Issue**: Timestamps still malformed

**Solution**: Check that you're returning ISO format strings from `_parse_response()`. The normalization happens automatically in the base class.

---

**Issue**: Coverage too low

**Solution**: Temporarily adjust `pytest.ini` coverage threshold. Focus on testing your collector implementation, not the base class (already at 93% coverage).

## Best Practices

1. **Keep _fetch_raw_data() simple**: Just fetch and return raw data
2. **Keep _parse_response() pure**: No side effects, just transformation
3. **Let BaseCollector handle everything else**: Don't reimplement retry, logging, etc.
4. **Use type hints**: Helps with IDE autocomplete and catches errors
5. **Test edge cases**: Empty data, malformed timestamps, API errors
6. **Document API quirks**: Add comments for API-specific behavior
7. **Use correlation IDs**: Include in external logging/monitoring

## Related Documentation

- [Architecture Improvement Plan](ARCHITECTURE_IMPROVEMENT_PLAN.md)
- [Phase 1 Completion Summary](PHASE1_COMPLETION_SUMMARY.md)
- [CI/CD Setup](CI_CD_SETUP.md)
- [Google Drive Archival](GDRIVE_ARCHIVAL_SETUP.md)
- [Timezone Helpers](../utils/timezone_helpers.py)
- [Data Types](../utils/data_types.py)

---

**Last Updated**: 2025-12-03
**Status**: ✅ Production Ready
**Test Coverage**: 93% (collectors/base.py), 99% (test_base_collector.py)
**Active Collectors**: 16+ collectors using base architecture
