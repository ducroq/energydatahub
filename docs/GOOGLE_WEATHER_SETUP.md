# Google Weather API Setup Guide

## Summary

The Google Weather API collector has been implemented with **separate API key** configuration to avoid conflicts with existing Google services.

## API Key Configuration

### Local Development (`secrets.ini`)

```ini
[api_keys]
google = AIzaSyB_zfr11b74KMsFzmOdR87MTZgyn2uf2EQ  # Original key (Drive, etc.)
google_weather = YOUR_WEATHER_API_KEY_HERE          # New key for Weather API
```

**Two separate keys:**
- **`google`**: Original key for Google Drive (used in legacy tests) - preserved
- **`google_weather`**: New key for Weather API - **you need to add this**

## Setup Steps

### 1. Enable Weather API in Your Billing-Enabled Project

1. Go to Google Cloud Console: https://console.cloud.google.com/
2. **Select your billing-enabled project** (the one with credit card linked)
3. Navigate to **APIs & Services → Library**
4. Search for **"Weather API"**
5. Click on it and click **"Enable"**

### 2. Get Your API Key

**Option A: Use Existing Key** (if you already have one)
1. Go to: https://console.cloud.google.com/apis/credentials
2. Copy your existing API key

**Option B: Create New Key** (recommended for security)
1. Go to: https://console.cloud.google.com/apis/credentials
2. Click **"+ Create Credentials" → "API Key"**
3. Copy the generated key
4. (Optional) Click **"Restrict Key"** and limit to "Weather API" only

### 3. Update Local Configuration

Edit `secrets.ini` and replace `YOUR_WEATHER_API_KEY_HERE` with your actual key:

```ini
google_weather = AIzaSyC_your_actual_key_here
```

### 4. Update GitHub Secrets (for CI/CD)

1. Go to: https://github.com/ducroq/energydatahub/settings/secrets/actions
2. Click **"New repository secret"**
3. Name: `GOOGLE_WEATHER_API_KEY`
4. Value: Paste your Weather API key
5. Click **"Add secret"**

**Note:** Don't replace `GOOGLE_API_KEY` - add this as a NEW secret.

## Testing

### Local Test

```bash
cd "C:\Users\scbry\HAN\HAN H2 LAB IPKW - Projects - WebBasedControl\01. Software\energyDataHub"
python test_google_weather.py
```

**Expected output:**
```
============================================================
GOOGLE WEATHER API COLLECTOR TEST SUITE
============================================================

============================================================
TEST 1: Single Location (Arnhem)
============================================================
✓ Successfully collected 48 data points

============================================================
TEST 2: Multi-Location (6 Strategic Locations)
============================================================
✓ Successfully collected data
  Hamburg_DE: 72 data points
  Munich_DE: 72 data points
  Arnhem_NL: 72 data points
  IJmuiden_NL: 72 data points
  Brussels_BE: 72 data points
  Esbjerg_DK: 72 data points

============================================================
TEST 3: Full 10-Day Forecast (240 hours)
============================================================
✓ Successfully collected 240 data points

============================================================
TEST SUMMARY
============================================================
✓ PASS: Single Location
✓ PASS: Multi-Location
✓ PASS: Full 10-Day Forecast

🎉 All tests passed!
```

### Production Test (GitHub Actions)

After updating GitHub secrets:
1. Go to: https://github.com/ducroq/energydatahub/actions
2. Select **"Collect and Publish Data"** workflow
3. Click **"Run workflow"** → **"Run workflow"** (manual trigger)
4. Check logs to verify Google Weather collection succeeded

## Strategic Locations Collected

The system collects weather for **6 strategic European locations**:

| Location | Coordinates | Purpose |
|----------|-------------|---------|
| **Hamburg, DE** | 53.55°N, 9.99°E | North German wind belt (offshore North Sea) |
| **Munich, DE** | 48.14°N, 11.58°E | South German solar belt |
| **Arnhem, NL** | 51.99°N, 5.90°E | Local location + central Netherlands |
| **IJmuiden, NL** | 52.46°N, 4.63°E | Dutch offshore wind proxy (North Sea coast) |
| **Brussels, BE** | 50.85°N, 4.35°E | Belgian market coupling |
| **Esbjerg, DK** | 55.48°N, 8.45°E | Danish North Sea wind |

**Rationale:** See `WEATHER_LOCATION_STRATEGY.md` for detailed explanation.

## Data Output

### Local Files
- **Timestamped**: `data/YYMMDD_HHMMSS_weather_forecast_multi_location.json`
- **Current**: `data/weather_forecast_multi_location.json`

### Published (GitHub Pages)
- https://ducroq.github.io/energydatahub/weather_forecast_multi_location.json

### Format

```json
{
  "metadata": {
    "generated_at": "2025-01-04T16:30:00Z",
    "source": "Google Weather API v1",
    "data_type": "weather",
    "units": "metric"
  },
  "datasets": [{
    "name": "GoogleWeather",
    "source": "Google Weather API v1",
    "data": {
      "Hamburg_DE": {
        "2025-01-05T00:00:00+01:00": {
          "temperature": 8.5,
          "feels_like": 6.2,
          "humidity": 82,
          "wind_speed": 12.3,
          "wind_direction": 270,
          "cloud_cover": 75,
          "pressure": 1013,
          "precipitation_probability": 20,
          ...
        },
        "2025-01-05T01:00:00+01:00": {...},
        ...
      },
      "Munich_DE": {...},
      "Arnhem_NL": {...},
      "IJmuiden_NL": {...},
      "Brussels_BE": {...},
      "Esbjerg_DK": {...}
    }
  }]
}
```

## Cost Estimate

**Google Weather API Pricing:**
- **Preview phase**: FREE (no charges)
- **After GA**:
  - First 10,000 requests/month: FREE
  - Additional: $0.15 per 1,000 requests

**Your usage:**
- 6 locations × 1 request/day = 180 requests/month
- **Cost: $0.00/month** (well under free tier)

## Troubleshooting

### Error: "Weather API has not been used in project"
**Solution:** Enable the Weather API in your Google Cloud project (Step 1 above)

### Error: "This API is not enabled for this project"
**Solution:** Make sure you enabled Weather API in the correct project (the one with billing)

### Error: "The request is missing a valid API key"
**Solution:** Check that `google_weather` key is set in `secrets.ini`

### Error: "API key not valid"
**Solution:**
1. Verify the key is correct (no extra spaces)
2. Check the key hasn't been restricted to exclude Weather API
3. Try creating a new unrestricted key

### Tests pass locally but fail in GitHub Actions
**Solution:** Make sure you added the `GOOGLE_WEATHER_API_KEY` secret to GitHub (Step 4)

## Integration with Model A

This multi-location weather data will be used by **Model A (Energy Price Predictor)** to predict pan-European electricity prices 2-7 days ahead.

**Feature engineering examples:**
```python
# Wind power proxy (Germany drives EU prices)
wind_power_de = (hamburg_wind_speed ** 3 + munich_wind_speed ** 3) / 2

# Solar power proxy
solar_power_de = (munich_solar_irradiance + arnhem_solar_irradiance) / 2

# Temperature-driven demand
heating_demand = mean([max(0, 18 - temp) for temp in temps])

# Cross-border flow indicators
pressure_gradient_de_nl = pressure_hamburg - pressure_arnhem
wind_gradient_de_nl = wind_hamburg - wind_arnhem
```

See `ENERGY_PRICE_PREDICTOR_REPO_PLAN.md` for full Model A design.

## Files Modified

✅ **Collectors:**
- `collectors/googleweather.py` - New collector (multi-location support)
- `collectors/__init__.py` - Export GoogleWeatherCollector

✅ **Main Scripts:**
- `data_fetcher.py` - Integrated Google Weather collection
- `test_google_weather.py` - Test suite

✅ **Configuration:**
- `secrets.ini` - Added `google_weather` key
- `utils/helpers.py` - Added `GOOGLE_WEATHER_API_KEY` env var mapping

✅ **CI/CD:**
- `.github/workflows/collect-data.yml` - Added `GOOGLE_WEATHER_API_KEY` secret

✅ **Documentation:**
- `WEATHER_LOCATION_STRATEGY.md` - Location selection rationale
- `GOOGLE_WEATHER_SETUP.md` - This file

## Next Steps

After Google Weather is working:
1. ✅ Collect data for 6 months (need historical data for training)
2. ✅ Set up `energyPricePredictor` repository (Model A)
3. ✅ Build feature engineering pipeline
4. ✅ Train baseline models (persistence, seasonal naive)
5. ✅ Train ML models (SARIMAX, Prophet, Random Forest)
6. ✅ Deploy daily prediction pipeline

---

**Last Updated:** 2025-01-04
**Status:** Ready for setup
**Questions?** Check the troubleshooting section or review `WEATHER_LOCATION_STRATEGY.md`
