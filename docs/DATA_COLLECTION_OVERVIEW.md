# Energy Data Collection Overview

## Purpose
This document provides a comprehensive overview of all data collected by the Energy Data Hub for electricity price prediction in the Netherlands.

---

## Current Data Collection

### Collection Schedule
- **Frequency**: Daily at 16:00 UTC (18:00 CET / 17:00 CEST)
- **Method**: GitHub Actions workflow
- **Storage**: Timestamped JSON files + latest version for GitHub Pages

---

## Data Categories

### 1. Electricity Prices (Target Variable)

| Source | File | Variables | Resolution | Forecast Horizon |
|--------|------|-----------|------------|------------------|
| ENTSO-E | `energy_price_forecast.json` | Day-ahead price (€/MWh) | Hourly | Next day |
| EnergyZero | `energy_price_forecast.json` | Day-ahead price (€/MWh) | Hourly | Next day |
| EPEX (via Awattar) | `energy_price_forecast.json` | Day-ahead price (€/MWh) | Hourly | Next day |
| Nord Pool Elspot | `energy_price_forecast.json` | Day-ahead price (€/MWh) | Hourly | Next day |

**Historical Records**: ~89 days (since Sep 28, 2025)

---

### 2. Supply Side - Wind Generation

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| ENTSO-E | `wind_forecast.json` | Wind generation forecast (MW) | NL, DE_LU, BE, DK_1 |
| Google Weather | `wind_forecast.json` | Wind speed, direction | 9 offshore locations |
| Google Weather | `weather_forecast_multi_location.json` | Full weather data | 15 strategic + offshore |

**Offshore Wind Locations**:
- 🇳🇱 Borssele (1.5 GW), Hollandse Kust (3.5 GW), Gemini (600 MW), IJmuiden Ver (4 GW)
- 🇩🇪 Helgoland Cluster, Borkum Riffgrund
- 🇬🇧 Dogger Bank (3.6 GW)
- 🇩🇰 Horns Rev
- 🇧🇪 North Sea BE

**Historical Records**: ~7 days (wind_forecast), ~43 days (multi_location)

---

### 3. Supply Side - Solar Generation

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| NED.nl | `ned_production.json` | Solar actual + forecast (MW) | Netherlands |
| Open-Meteo | `solar_forecast.json` | GHI, DNI, DHI, cloud cover | 7 locations |

**Solar Irradiance Locations** (high solar density areas):
- 🇳🇱 Rotterdam, Eindhoven, Lelystad, Groningen
- 🇩🇪 Munich (Bavaria), Stuttgart (Baden-Württemberg)
- 🇧🇪 Antwerp (Flanders)

**Historical Records**: ~6 days (NED), ~4 days (solar_forecast)

---

### 4. Demand Side - Temperature & Weather

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| Open-Meteo | `demand_weather_forecast.json` | Temperature, apparent temp, humidity, precipitation, wind, cloud cover | 11 population centers |

**Computed Variables**:
- **HDD** (Heating Degree Days) = max(0, 18°C - temp) → Heating demand indicator
- **CDD** (Cooling Degree Days) = max(0, temp - 24°C) → Cooling demand indicator

**Population Centers** (~8.3M total):
- 🇳🇱 Amsterdam (873k), Rotterdam (651k), The Hague (546k), Utrecht (362k), Eindhoven (238k), Groningen (233k)
- 🇩🇪 Hamburg (1.9M), Cologne (1.1M), Dusseldorf (621k)
- 🇧🇪 Brussels (1.2M), Antwerp (531k)

**Historical Records**: ~2 days (new)

---

### 5. Grid Balance

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| TenneT | `grid_imbalance.json` | Imbalance prices, volumes | Netherlands |

**Historical Records**: ~24 days

---

## Gap Analysis: What's Missing?

### 🔴 Critical Gaps (High Impact on Price)

| Data Type | Why Important | Potential Sources | Status |
|-----------|---------------|-------------------|--------|
| **Gas Prices** | Natural gas often sets marginal electricity price | TTF (ICE), PEGAS | ❌ Not collected |
| **CO2/Carbon Prices** | EU ETS affects fossil generation costs | EEX, ICE | ❌ Not collected |
| **Cross-border Flows** | Import/export affects supply | ENTSO-E | ❌ Not collected |
| **Load Forecast** | Actual demand predictions | ENTSO-E, TenneT | ❌ Not collected |

### 🟡 Important Gaps (Medium Impact)

| Data Type | Why Important | Potential Sources | Status |
|-----------|---------------|-------------------|--------|
| **Nuclear Availability** | French/Belgian nuclear affects regional prices | ENTSO-E, EDF | ❌ Not collected |
| **Hydro Reservoir Levels** | Nordic hydro affects price dynamics | Nord Pool, ENTSO-E | ❌ Not collected |
| **Coal Prices** | Backup fuel for price setting | ICE, API2 | ❌ Not collected |
| **Interconnector Capacity** | Transmission constraints | JAO, ENTSO-E | ❌ Not collected |

### 🟢 Nice to Have (Lower Impact)

| Data Type | Why Important | Potential Sources | Status |
|-----------|---------------|-------------------|--------|
| **Industrial Production** | Economic activity affects demand | CBS, Eurostat | ❌ Not collected |
| **Holiday Calendar** | Demand patterns change | Public APIs | ⚠️ Can be derived |
| **Day Type Features** | Weekend vs weekday patterns | - | ⚠️ Can be derived |

---

## Recommended Additional Data Sources

### 1. Gas Prices (TTF) - HIGH PRIORITY
```
Source: ICE Endex / PEGAS
API: Paid subscription required
Alternative: EEX (European Energy Exchange)
Why: Gas plants often set marginal price, especially during low renewable periods
```

### 2. Carbon Prices (EU ETS) - HIGH PRIORITY
```
Source: EEX (European Energy Exchange)
API: https://www.eex.com/en/market-data/environmental-markets
Why: CO2 cost adds €20-100/MWh to fossil generation
Free alternative: Ember Climate (delayed data)
```

### 3. Cross-border Flows - MEDIUM PRIORITY
```
Source: ENTSO-E Transparency Platform
API: Already have API key
Endpoint: Physical Flows (A11)
Why: NL imports/exports significantly affect local prices
```

### 4. Load Forecast - MEDIUM PRIORITY
```
Source: ENTSO-E Transparency Platform
API: Already have API key
Endpoint: Load Forecast (A65)
Why: Demand forecasts directly impact price formation
```

### 5. French Nuclear Availability - MEDIUM PRIORITY
```
Source: ENTSO-E or EDF
API: ENTSO-E (Generation Forecast A71)
Why: French nuclear outages cause price spikes across Europe
```

---

## Data Quality Assessment

### ✅ Strengths
- Multiple price sources for cross-validation
- Good offshore wind coverage (major capacity areas)
- Free solar/demand weather via Open-Meteo
- Automated daily collection with CI/CD

### ⚠️ Weaknesses
- No fuel prices (gas, coal, carbon)
- No cross-border flow data
- No load/demand forecasts
- Short historical record for new data types
- Google Weather limited to 24h history (no backfill)

### 🔄 Backfill Capability

| Data Type | Backfill Available | Source |
|-----------|-------------------|--------|
| Electricity prices | ✅ Years | ENTSO-E |
| Wind generation | ✅ Years | ENTSO-E |
| Grid imbalance | ✅ ~2 years | TenneT |
| Solar irradiance | ✅ Since 1940 | Open-Meteo Historical |
| Demand weather | ✅ Since 1940 | Open-Meteo Historical |
| Offshore wind weather | ❌ 24h only | Google Weather |
| Gas prices | ✅ Years | ICE/EEX (paid) |
| Carbon prices | ✅ Years | EEX |

---

## API Cost Summary

| Source | Cost | Key Required |
|--------|------|--------------|
| ENTSO-E | Free | ✅ Yes |
| EnergyZero | Free | ❌ No |
| EPEX (Awattar) | Free | ❌ No |
| Nord Pool | Free | ❌ No |
| TenneT | Free | ✅ Yes |
| NED.nl | Free | ✅ Yes |
| Google Weather | **Paid** | ✅ Yes |
| Open-Meteo | **Free** | ❌ No |
| Open-Meteo Historical | **Free** | ❌ No |

---

## Recommended Next Steps

### Phase 1: Quick Wins (Free Data)
1. [ ] Add ENTSO-E cross-border flows (already have API key)
2. [ ] Add ENTSO-E load forecast (already have API key)
3. [ ] Add calendar features (holidays, day-of-week)
4. [ ] Backfill historical data using Open-Meteo + ENTSO-E

### Phase 2: Enhanced Coverage (May Require Paid APIs)
1. [ ] Add gas prices (TTF) - check free alternatives first
2. [ ] Add carbon prices (EU ETS) - Ember Climate has free delayed data
3. [ ] Add French nuclear availability from ENTSO-E

### Phase 3: Advanced Features
1. [ ] Interconnector capacity and congestion
2. [ ] Hydro reservoir levels (Nordic)
3. [ ] Economic indicators

---

## File Structure

```
data/
├── energy_price_forecast.json          # Combined price data (ENTSO-E, EnergyZero, EPEX, Elspot)
├── grid_imbalance.json                 # TenneT imbalance data
├── weather_forecast_multi_location.json # Google Weather (15 locations)
├── wind_forecast.json                  # ENTSO-E wind + offshore weather
├── ned_production.json                 # NED.nl solar/wind production
├── solar_forecast.json                 # Open-Meteo solar irradiance (7 locations)
├── demand_weather_forecast.json        # Open-Meteo demand weather (11 locations)
└── YYMMDD_HHMMSS_*.json               # Timestamped historical copies
```

---

## References

- [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/)
- [Open-Meteo API](https://open-meteo.com/)
- [TenneT API](https://www.tennet.eu/)
- [NED.nl](https://ned.nl/)
- [EEX Market Data](https://www.eex.com/)
- [ICE Endex](https://www.theice.com/endex)

---

*Document created: 2025-12-01*
*Last updated: 2025-12-01*
