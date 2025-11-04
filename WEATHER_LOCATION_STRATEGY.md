# Weather Location Strategy for Energy Price Prediction

## Document Purpose

This document explains the rationale for selecting specific geographic locations for weather data collection to support pan-European electricity price prediction (Model A).

**Created**: 2025-01-04
**Status**: Active Strategy
**Related Documents**: `ENERGY_PRICE_PREDICTOR_REPO_PLAN.md`

---

## Context

### Problem Statement

The energyDataHub collects day-ahead electricity prices from ENTSO-E for the Netherlands market. To predict prices 2-7 days ahead (Model A), we need weather forecasts that capture the primary drivers of European electricity prices.

### Key Insight: Why Multi-Location Weather Matters

European electricity markets are highly interconnected through **market coupling**. Dutch prices are not determined solely by Dutch weather, but by:

1. **Renewable generation across coupled markets** (primarily Germany)
2. **Cross-border electricity flows** (when German wind is high, prices drop across NL/BE/DE)
3. **Regional demand patterns** (temperature drives heating/cooling load)

**Local Arnhem weather alone is insufficient** for predicting pan-European market prices.

---

## European Electricity Market Structure

### Market Coupling Zones Affecting Dutch Prices

```
┌─────────────────────────────────────────────────────────┐
│  Pan-European Day-Ahead Market (ENTSO-E)                │
│                                                          │
│  Strong Coupling:                                        │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐           │
│  │ Germany  │ ─ │ Netherlands │ ─ │ Belgium │           │
│  │ (DE)     │   │ (NL)       │   │ (BE)    │           │
│  └──────────┘   └──────────┘   └──────────┘           │
│       │              │               │                   │
│       │         ┌────┴────┐          │                  │
│       │         │ Denmark │          │                  │
│       │         │ (DK)    │          │                  │
│       │         └─────────┘          │                  │
│       │                               │                  │
│  ┌────┴─────┐                   ┌────┴─────┐           │
│  │ France   │                   │ UK       │           │
│  │ (FR)     │                   │ (GB)     │           │
│  └──────────┘                   └──────────┘           │
│                                                          │
│  Weaker Coupling: Austria, Switzerland, Poland, Czech   │
└─────────────────────────────────────────────────────────┘
```

### Price Formation Drivers (Merit Order)

Electricity is dispatched in order of marginal cost:

1. **Renewables** (wind/solar) → €0-5/MWh (fuel cost = zero)
2. **Nuclear** → €10-20/MWh (inflexible baseload)
3. **Hydro** → Variable (opportunity cost)
4. **Gas** → €50-150/MWh (flexible, sets price most hours)
5. **Coal** → €80-200/MWh (backup)

**Key Takeaway**: High renewable generation → displaces gas → prices drop dramatically.

### Renewable Capacity by Country (2024)

| Country | Wind (GW) | Solar (GW) | Impact on NL Prices |
|---------|-----------|------------|---------------------|
| **Germany** | 70 | 80 | ⭐⭐⭐ Highest (market leader) |
| **Netherlands** | 8 | 20 | ⭐⭐ Direct (our market) |
| **Belgium** | 5 | 8 | ⭐⭐ High (coupled) |
| **Denmark** | 7 | 2 | ⭐ Medium (wind exports) |
| **France** | 22 | 16 | ⭐ Medium (nuclear-dominant) |
| **UK** | 28 | 15 | ⭐ Low (weak coupling) |

**Germany dominates** with ~150 GW renewables → German weather is critical for NL price prediction!

---

## Strategic Location Selection

### Selection Criteria

Locations were chosen based on:

1. **Renewable capacity** (GW wind/solar in region)
2. **Market coupling strength** (interconnector capacity to NL)
3. **Weather diversity** (capture different wind/solar patterns)
4. **Geographic spread** (North Sea wind vs. Continental solar)

### Tier 1: Essential Locations (6 locations)

These locations capture >80% of weather-driven price variance:

#### 🇩🇪 Germany (2 locations) - Most Important

**1. Hamburg, Germany**
- **Coordinates**: 53.5511°N, 9.9937°E
- **Represents**: North German wind belt
- **Importance**: ⭐⭐⭐⭐⭐
- **Rationale**:
  - Onshore wind: ~30 GW in North Germany (Schleswig-Holstein, Lower Saxony)
  - Offshore wind: North Sea (20+ GW) and Baltic Sea (8 GW)
  - When Hamburg has strong winds → German grid flooded with cheap wind power → NL prices drop
  - North Sea weather system early indicator

**2. Munich, Germany**
- **Coordinates**: 48.1351°N, 11.5820°E
- **Represents**: South German solar belt
- **Importance**: ⭐⭐⭐⭐⭐
- **Rationale**:
  - Bavaria/Baden-Württemberg: 30+ GW solar capacity
  - Different weather patterns than North (continental vs. maritime)
  - Summer solar generation → midday price collapse
  - Temperature gradient North-South → heating/cooling demand proxy

#### 🇳🇱 Netherlands (2 locations) - Our Market

**3. Arnhem, Netherlands**
- **Coordinates**: 51.9851°N, 5.8987°E
- **Represents**: Central Netherlands
- **Importance**: ⭐⭐⭐⭐
- **Rationale**:
  - Your lab location (local context)
  - Central NL position (representative of national weather)
  - Domestic solar (20 GW) and onshore wind (4 GW) proxy
  - Baseline for model validation

**4. IJmuiden, Netherlands**
- **Coordinates**: 52.4608°N, 4.6262°E
- **Represents**: Dutch North Sea coast
- **Importance**: ⭐⭐⭐⭐
- **Rationale**:
  - Offshore wind proxy (Borssele, Hollandse Kust wind farms: 4+ GW)
  - Coastal wind often differs from inland (Arnhem)
  - North Sea weather system landfall point
  - Critical for Dutch wind generation prediction

#### 🇧🇪 Belgium (1 location) - Coupled Market

**5. Brussels, Belgium**
- **Coordinates**: 50.8503°N, 4.3517°E
- **Represents**: Belgian market
- **Importance**: ⭐⭐⭐
- **Rationale**:
  - Belgium directly coupled to NL (high interconnector capacity)
  - Offshore wind: 2+ GW (Thornton Bank, etc.)
  - Belgian nuclear fleet (6 GW) - outages affect regional prices
  - Cross-border flow predictor

#### 🇩🇰 Denmark (1 location) - Wind Powerhouse

**6. Esbjerg, Denmark**
- **Coordinates**: 55.4760°N, 8.4516°E
- **Represents**: Danish North Sea wind
- **Importance**: ⭐⭐⭐
- **Rationale**:
  - Denmark: 7 GW wind (>50% of electricity from wind)
  - Horns Rev offshore wind farms (1+ GW)
  - Danish wind → German imports → affects NL via coupling
  - Northern North Sea weather system indicator

---

### Tier 2: Optional Locations (Add if Tier 1 insufficient)

**7. Paris, France** (48.8566°N, 2.3522°E)
- French demand + growing solar (16 GW)
- Nuclear dominance reduces weather sensitivity
- Add if cross-border flows NL↔FR matter

**8. Oslo, Norway** (59.9139°N, 10.7522°E)
- Norwegian hydro (drought/precipitation affects output)
- NorNed cable (700 MW interconnector to NL)
- Add if hydro imports significant for price

**9. London, UK** (51.5074°N, -0.1278°E)
- UK wind (28 GW)
- BritNed cable (1000 MW interconnector to NL)
- Add if UK flows matter

**10. Berlin, Germany** (52.5200°N, 13.4050°E)
- East German renewables
- Polish/Czech border interactions
- Add for comprehensive German coverage

---

## Geographic Coverage Map

```
North Sea Weather Systems:
            Esbjerg (DK) ●
                 ↓
    Hamburg (DE) ●     ← North German Wind Belt
         ↓
    IJmuiden (NL) ●    ← Dutch Offshore Wind
         ↓
    Arnhem (NL) ●      ← Central NL / Your Location
         ↓
    Brussels (BE) ●    ← Belgian Market

Continental Weather:
    Munich (DE) ●      ← South German Solar Belt
```

**Weather System Flow**: North Sea → Germany → Netherlands → Belgium
- Wind patterns: Typically west-to-east (Atlantic → North Sea → Continent)
- Solar patterns: North-South gradient (Bavaria sunnier than Hamburg)

---

## Feature Engineering from Multi-Location Data

### Raw Weather Features (per location × 6 locations)

- Temperature (°C)
- Wind speed (m/s)
- Wind direction (°)
- Cloud cover (%)
- Solar irradiance (W/m²) - computed from cloud cover + sun angle
- Precipitation (mm)
- Pressure (hPa)

**Total**: 7 features × 6 locations = 42 raw features

### Aggregate Features (derived)

**Wind Power Proxy Features:**
```python
# Wind power scales with wind_speed³
wind_power_proxy_north_sea = mean([
    wind_speed_hamburg³,
    wind_speed_ijmuiden³,
    wind_speed_esbjerg³
])

wind_power_proxy_germany = mean([
    wind_speed_hamburg³,
    wind_speed_munich³
])
```

**Solar Power Proxy Features:**
```python
# Solar power depends on irradiance (cloud cover + sun angle)
solar_power_proxy_germany = mean([
    solar_irradiance_munich,
    solar_irradiance_arnhem
])
```

**Temperature-Driven Demand:**
```python
# Heating demand (winter): low temp → high demand → high prices
heating_demand_proxy = mean([
    max(0, 18 - temp_hamburg),
    max(0, 18 - temp_arnhem),
    max(0, 18 - temp_brussels)
])

# Cooling demand (summer): high temp → high demand → high prices
cooling_demand_proxy = mean([
    max(0, temp_munich - 25),
    max(0, temp_arnhem - 25)
])
```

**Weather Gradients (cross-border flow indicators):**
```python
# Pressure gradient → wind direction → flow direction
pressure_gradient_de_nl = pressure_hamburg - pressure_arnhem

# Wind gradient → generation imbalance → price arbitrage
wind_gradient_de_nl = wind_speed_hamburg - wind_speed_arnhem

# Temperature gradient → demand imbalance
temp_gradient_north_south = temp_hamburg - temp_munich
```

**Temporal Features:**
```python
# Predictable patterns
hour_of_day           # 0-23 (solar during day, demand peaks morning/evening)
day_of_week          # 1-7 (weekday vs. weekend demand)
month                # 1-12 (seasonal patterns)
is_holiday           # 0/1 (low demand on holidays)
season               # winter/spring/summer/autumn
```

**Lagged Price Features (autoregressive):**
```python
# Historical prices predict future prices
price_lag_24h        # Same hour yesterday
price_lag_168h       # Same hour last week
price_rolling_mean_24h
price_rolling_std_24h
```

### Feature Importance Hypothesis

**Expected order of importance** (to be validated by model):

1. **German wind power proxy** (Hamburg) - Highest impact
2. **Hour of day** - Solar generation timing
3. **Temperature (heating/cooling demand)** - Seasonal
4. **Dutch wind power proxy** (IJmuiden) - Local market
5. **German solar power proxy** (Munich) - Summer midday
6. **Historical prices (lags)** - Autoregressive component
7. **Pressure gradients** - Weather system movement
8. **Day of week** - Demand patterns

---

## Data Collection Strategy

### Implementation Approach

**Use Google Weather API for multi-location collection:**

- **API**: Google Weather API (https://developers.google.com/maps/documentation/weather)
- **Forecast horizon**: 240 hours (10 days) - covers full 7-day prediction need
- **Update frequency**: Once daily (16:30 UTC, after ENTSO-E price publication)
- **Locations**: 6 (Tier 1)
- **Cost**: FREE (120 calls/month << 10,000 free tier)

### Parallel with Existing Data

**Keep existing collectors for:**

1. **OpenWeather (Arnhem)** - Backup + validation
2. **MeteoServer (Arnhem)** - Dutch HARMONIE model (high local accuracy)
3. **Google Weather (6 locations)** - Pan-European coverage

This redundancy allows:
- Model comparison (single-location vs. multi-location)
- Fallback if one API fails
- Validation of Google Weather quality

### Storage Format

Store multi-location weather as:

```json
{
  "metadata": {
    "generated_at": "2025-01-04T16:30:00Z",
    "source": "google_weather_api",
    "forecast_horizon_hours": 240,
    "locations_count": 6
  },
  "datasets": [
    {
      "name": "weather_forecast_multi_location",
      "locations": [
        {
          "name": "Hamburg_DE",
          "latitude": 53.5511,
          "longitude": 9.9937,
          "data": [
            {
              "datetime": "2025-01-05T00:00:00+01:00",
              "temperature": 8.5,
              "wind_speed": 12.3,
              "wind_direction": 270,
              "cloud_cover": 75,
              "pressure": 1013,
              "humidity": 82,
              "precipitation": 0.0
            },
            ...
          ]
        },
        ...
      ]
    }
  ]
}
```

---

## Model A Integration

### Training Data Requirements

**Minimum dataset for initial model:**
- **6 months historical data** (captures seasonal variation)
- **Hourly granularity** (6 months × 720 hours/month = 4,320 samples)
- **Features**: ~50-100 (raw + derived + temporal + lags)
- **Target**: Hourly electricity price (EUR/MWh)

**Data sources to combine:**
1. Historical ENTSO-E prices (target variable) - ✅ Already collecting
2. Historical weather (6 locations) - ⚠️ Need to start collecting now
3. Historical local generation (for Model B) - ⚠️ Future work

### Model Architecture

**Baseline models:**
- Persistence (tomorrow = today)
- Seasonal naive (tomorrow = last week)
- Linear regression (weather features → price)

**Advanced models:**
- SARIMAX (seasonal ARIMA with weather exogenous variables)
- Random Forest / Gradient Boosting (capture non-linear interactions)
- LSTM / Temporal CNN (if >1 year data available)

**Ensemble:**
- Weighted average of top 3 models
- Uncertainty quantification via ensemble spread

---

## Success Metrics

### Model Performance Targets

**Technical accuracy:**
- MAE < 10 EUR/MWh for 24h ahead predictions
- MAE < 15 EUR/MWh for 72h ahead predictions
- MAE < 20 EUR/MWh for 168h ahead predictions
- Directional accuracy > 70% (predict up/down correctly)

**Business value:**
- Cost reduction vs. baseline scheduling: >10%
- Electrolyzer run-time optimization: >5% efficiency gain

### Validation Strategy

**Walk-forward validation:**
- Train on months 1-6
- Validate on month 7
- Roll forward weekly
- Retrain monthly

**Comparison tests:**
1. **Single-location (Arnhem only)** vs. **Multi-location (6 locations)**
   - Hypothesis: Multi-location improves MAE by 15-25%
2. **Weather-based model** vs. **Persistence baseline**
   - Hypothesis: Weather features provide 30-40% error reduction
3. **Short-term (24h)** vs. **Long-term (168h)** accuracy
   - Hypothesis: MAE doubles from 24h to 168h

---

## Implementation Timeline

### Phase 1: Data Collection (Current)

- [ ] Implement Google Weather API collector for 6 locations
- [ ] Start daily collection (run for 6 months before model training)
- [ ] Store parallel with existing OpenWeather/MeteoServer
- [ ] Build data quality monitoring

**Duration**: 1 week implementation, then 6 months collection

### Phase 2: Historical Data Acquisition (Parallel)

- [ ] Check if Google Weather API has historical data access
- [ ] Alternative: Scrape/purchase historical weather from ERA5 reanalysis
- [ ] Align historical weather with ENTSO-E price history (go back 12+ months)

**Duration**: 2-3 weeks

### Phase 3: Model Development (After 6 months)

- [ ] Feature engineering pipeline
- [ ] Baseline model training
- [ ] Advanced model training
- [ ] Ensemble development
- [ ] Backtesting

**Duration**: 6-8 weeks (see ENERGY_PRICE_PREDICTOR_REPO_PLAN.md)

---

## Risks and Mitigations

### Risk 1: Google Weather API Quality Unknown

**Mitigation:**
- Collect in parallel with OpenWeather/MeteoServer (first 3 months)
- Compare forecast accuracy against actual weather
- Validate against historical reanalysis (ERA5)
- Switch provider if quality insufficient

### Risk 2: 6 Locations May Be Insufficient

**Mitigation:**
- Start with Tier 1 (6 locations)
- Monitor model residuals (where does it fail?)
- Add Tier 2 locations if specific gaps identified
- Maximum cost still trivial (<400 calls/month = $0.06/month)

### Risk 3: Weather-Price Relationship Complex

**Mitigation:**
- Use non-linear models (Random Forest, Neural Networks)
- Feature engineering (wind³, temperature gradients)
- Include autoregressive features (past prices)
- Ensemble multiple model types

### Risk 4: 6 Months Collection Delay

**Mitigation:**
- Start collecting immediately (don't wait for Model A repo setup)
- Use historical weather data if available (ERA5, ECMWF archives)
- Build Model A architecture in parallel during collection period
- Train on shorter dataset (3 months) for initial validation

---

## References

### Data Sources

- **ENTSO-E Transparency Platform**: https://transparency.entsoe.eu/
- **Google Weather API**: https://developers.google.com/maps/documentation/weather
- **ERA5 Reanalysis** (historical weather): https://cds.climate.copernicus.eu/

### Academic Literature

- Weron, R. (2014). "Electricity price forecasting: A review of the state-of-the-art"
- Lago, J. et al. (2021). "Forecasting day-ahead electricity prices: A review"
- Kiesel, R. & Paraschiv, F. (2017). "Econometric analysis of 15-minute intraday electricity prices"

### Market Information

- ENTSO-E Statistical Database: https://www.entsoe.eu/data/
- Wind Europe Statistics: https://windeurope.org/intelligence-platform/
- SolarPower Europe: https://www.solarpowereurope.org/

---

## Appendix: Alternative Location Sets

If computational budget allows more locations, consider these alternatives:

### Alternative A: Detailed Germany (10 locations)

Add Berlin, Frankfurt, Cologne, Hamburg, Munich, Stuttgart, Hannover, Dresden, Rostock, Freiburg
- **Pros**: Captures full German weather diversity
- **Cons**: Diminishing returns (Germany already covered)

### Alternative B: Full North Sea Ring (8 locations)

Hamburg, IJmuiden, Esbjerg, Aberdeen (UK), Bergen (Norway), Stavanger (Norway)
- **Pros**: Complete North Sea wind coverage
- **Cons**: Overlapping information (maritime weather similar)

### Alternative C: Bidding Zone Representatives (12 locations)

One location per ENTSO-E bidding zone: DE-LU, NL, BE, DK1, DK2, FR, NO2, GB, PL, CZ, AT, CH
- **Pros**: Theoretically complete market coverage
- **Cons**: Many zones weakly coupled (diminishing returns)

**Recommendation**: Stick with Tier 1 (6 locations) until validation shows need for more.

---

## Document Maintenance

**Review Trigger Events:**
- Model A shows poor accuracy in specific weather conditions → add locations
- Market coupling changes (new interconnectors) → add affected zones
- Renewable capacity significantly increases in a region → add monitoring
- Google Weather API quality issues → switch provider

**Next Review Date**: After Model A Phase 1 (6 months from collection start)

---

**Document Status**: Planning Complete - Ready for Implementation
**Approval**: Awaiting implementation and validation
**Last Updated**: 2025-01-04
