# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Energy Data Hub is a Python-based system for collecting, encrypting, and publishing energy price and weather forecast data. The system is designed for integration with LabVIEW and National Instruments hardware at HAN University of Applied Sciences.

**Key Features:**
- Asynchronous data collection from multiple energy price APIs (ENTSO-E, Energy Zero, EPEX SPOT, Nord Pool Elspot)
- Weather and solar forecast data from OpenWeather and MeteoServer
- Air quality data from Luchtmeetnet
- AES-256-CBC encryption with HMAC-SHA256 signing for secure data publishing
- Automated GitHub Actions workflow for daily data collection and GitHub Pages publishing
- Support for both cloud (GitHub Actions) and local (Raspberry Pi) deployment

## Architecture

### Entry Points

**Cloud Deployment (GitHub Actions):**
- `data_fetcher.py`: Main orchestrator that runs daily via GitHub Actions workflow
- Loads configuration from environment variables (set in GitHub Secrets)
- Publishes encrypted JSON files to GitHub Pages at `https://ducroq.github.io/energydatahub/`

**Local Deployment (Raspberry Pi):**
- `local_data_fetcher.py`: Simplified local version for Raspberry Pi
- Loads configuration from `secrets.ini` file
- Stores data in local `data/` directory
- Can be scheduled via cron (see comments in file for examples)

**Utility Scripts:**
- `data_decrypter.py`: Single file decryption utility
- `batch_data_decryption.py`: Batch decryption for archived data
- `energy_data_visualiser.py`: Data visualization tools

### Module Structure

**utils/**
- `helpers.py`: Configuration loading, file I/O, distance calculations
  - `load_secrets()`: Loads from environment variables OR secrets.ini file
  - `save_data_file()`: Handles both encrypted and plain JSON output
- `secure_data_handler.py`: AES-256-CBC encryption with HMAC-SHA256
- `data_types.py`: Data validation and serialization classes
  - `EnhancedDataSet`: Single data source with metadata
  - `CombinedDataSet`: Aggregates multiple datasets
- `timezone_helpers.py`: Timezone detection from coordinates

**energy_data_fetchers/**
- `entsoe_client.py`: ENTSO-E day-ahead energy prices (EUR/MWh)
- `energy_zero_price_fetcher.py`: Energy Zero consumer prices (EUR/kWh)
- `epex_price_fetcher.py`: EPEX SPOT market prices
- `nordpool_data_fetcher.py`: Nord Pool Elspot prices

**weather_data_fetchers/**
- `open_weather_client.py`: OpenWeather API for weather forecasts
- `meteoserver_client.py`: MeteoServer for weather and sun forecasts
- `luchtmeetnet_data_fetcher.py`: Dutch air quality measurements

### Data Flow

1. **Collection** (async): All API clients are called concurrently using `asyncio.gather()`
2. **Aggregation**: Data is combined into `CombinedDataSet` objects by type:
   - Energy price forecast (entsoe, energy_zero, epex, elspot)
   - Weather forecast (OpenWeather, MeteoServer)
   - Sun forecast (MeteoServer)
   - Air quality history (Luchtmeetnet)
3. **Storage**: Each dataset is saved with timestamp and "current" copy:
   - `{YYMMDD_HHMMSS}_{type}.json` (archived)
   - `{type}.json` (current, copied for deployment)
4. **Encryption** (if enabled): `SecureDataHandler` encrypts JSON to base64 string
5. **Publishing**: GitHub Actions commits to `data/`, deploys `docs/` to GitHub Pages

### Configuration

**Local Development:**
- `settings.ini`: Location and encryption flag
  ```ini
  [location]
  latitude = YOUR_LATITUDE
  longitude = YOUR_LONGITUDE

  [data]
  encryption = 1
  ```
- `secrets.ini`: API keys and encryption keys (NOT in repo)

**GitHub Actions:**
- Environment variables: `ENCRYPTION_KEY`, `HMAC_KEY`, `ENTSOE_API_KEY`, `OPENWEATHER_API_KEY`, `METEO_API_KEY`, `GOOGLE_API_KEY`, `PAT`
- Set in repository secrets
- Workflow: `.github/workflows/collect-data.yml`

## Development Commands

### Running Data Collection

**Cloud (GitHub Actions):**
```bash
# Manual trigger via GitHub Actions UI or:
# Workflow runs daily at 16:00 UTC automatically
```

**Local:**
```bash
# Install dependencies
pip install -r requirements.txt

# Run main data fetcher (requires secrets.ini)
python data_fetcher.py

# Run local version (simplified)
python local_data_fetcher.py

# Via shell script (for cron)
bash run_script.sh
```

### Data Decryption

```bash
# Single file
python data_decrypter.py

# Batch decryption (edit paths in script first)
python batch_data_decryption.py
```

### Testing

```bash
# Individual API tests (in tests/ directory)
python tests/testEntsoeApi.py
python tests/testEnergyZeroApi.py
python tests/testOpenWeather.py
python tests/testMeteoApi.py
```

### Dependencies

All dependencies are specified in `requirements.txt`:
- `entsoe-py`: ENTSO-E API client
- `easyenergy`, `energyzero`: Dutch energy price APIs
- `nordpool`: Nordic energy market
- `pyluchtmeetnet`: Dutch air quality
- `requests`: HTTP requests
- `cryptography`: AES encryption
- `timezonefinder`, `reverse_geocoder`: Location utilities

## Key Implementation Details

### Async Pattern
All data fetchers are async functions that can be run concurrently:
```python
tasks = [get_Entsoe_data(...), get_Energy_zero_data(...), ...]
results = await asyncio.gather(*tasks)
```

### Encryption
- AES-256-CBC with random IV per file
- HMAC-SHA256 signature for integrity
- Format: `base64(IV + ciphertext + HMAC)`
- Keys are 32-byte base64-encoded strings

### Windows Compatibility
```python
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

### Data Validation
`EnhancedDataSet` validates all values, converting invalid data (NaN, Infinity, '-', etc.) to `None` for JSON compatibility.

### Timezone Handling
- All API responses are converted to local timezone using `pytz`
- Timezone auto-detected from lat/lon using `timezonefinder`
- Country code derived for API queries

## Published Data Endpoints

All data is published to GitHub Pages:
- https://ducroq.github.io/energydatahub/energy_price_forecast.json
- https://ducroq.github.io/energydatahub/weather_forecast.json
- https://ducroq.github.io/energydatahub/sun_forecast.json
- https://ducroq.github.io/energydatahub/air_quality.json

Data is encrypted by default. Use `data_decrypter.py` with appropriate keys to decrypt.

## GitHub Actions Workflow

Workflow: `.github/workflows/collect-data.yml`
- Runs daily at 16:00 UTC (18:00 CET)
- Can be manually triggered via workflow_dispatch
- Steps:
  1. Install Python dependencies
  2. Run `data_fetcher.py` with env vars
  3. Copy current files to `docs/`
  4. Commit timestamped data to `data/`
  5. Deploy `docs/` to GitHub Pages

## Notes

- The system prioritizes environment variables over `secrets.ini` for configuration
- All timestamps are handled in UTC internally and converted for output
- Commented code sections in main files show evolution of architecture
- `energyDataHub_project_knowledge.md` contains extensive project documentation (1.1MB)
