# Historical Data Backfill Plan

## Purpose
Backfill 1+ year of historical data for energy price prediction model training.

## Data Sources for Backfill

### 1. ENTSO-E (existing API key)
- **Day-ahead prices**: Years of historical data available
- **Wind generation forecasts**: Historical generation data by country (NL, DE_LU, BE, DK_1)
- **Method**: Use existing `entsoe-py` library with date range parameters

### 2. Open-Meteo Historical Weather API (FREE)
- **URL**: https://open-meteo.com/en/docs/historical-weather-api
- **Coverage**: 1940 - present
- **Variables needed**:
  - `wind_speed_10m`, `wind_speed_100m` (hub height)
  - `wind_direction_10m`, `wind_direction_100m`
  - `temperature_2m`
  - `cloud_cover`
- **Locations**: All offshore wind farm coordinates from `data_fetcher.py`

### 3. TenneT Grid Imbalance
- **Coverage**: ~2 years historical
- **Method**: Use existing `tenneteu-py` library

## NOT Available for Historical Backfill
- Google Weather API: Only 24 hours history (even paid)
- MeteoServer: Forecast only, no historical

## Implementation Steps
1. Create `backfill_historical.py` script
2. Fetch ENTSO-E prices + wind generation (1 year)
3. Fetch Open-Meteo weather for offshore locations (1 year)
4. Store in same format as daily collection (timestamped JSON)
5. Optionally combine into training dataset (CSV/Parquet)

## Estimated Data Volume
- 365 days x 96 quarter-hours = 35,040 price points per source
- 365 days x 24 hours x 15 locations = 131,400 weather points
- Total: ~500MB uncompressed

## Notes
- Open-Meteo rate limits: 10,000 requests/day (free tier)
- ENTSO-E rate limits: Check API documentation
- Consider chunking requests by month to avoid timeouts

---
*Created: 2025-12-01*
*Status: TODO - implement when ready for ML training*
