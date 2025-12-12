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
| ENTSO-E (NL) | `energy_price_forecast.json` | Day-ahead price (€/MWh) | Hourly | Next day |
| ENTSO-E (DE_LU) | `energy_price_forecast.json` | German day-ahead price (€/MWh) | Hourly | Next day |
| EnergyZero | `energy_price_forecast.json` | Day-ahead price (€/MWh) | Hourly | Next day |
| EPEX (via Awattar) | `energy_price_forecast.json` | Day-ahead price (€/MWh) | Hourly | Next day |
| Nord Pool Elspot | `energy_price_forecast.json` | Day-ahead price (€/MWh) | Hourly | Next day |

**German Prices (DE_LU)**: German day-ahead prices are highly relevant for Dutch price prediction due to market coupling. When there is no congestion, NL and DE prices converge; price divergence signals interconnector constraints. The NL-DE price spread is a useful ML feature.

**Historical Records**: ~89 days (since Sep 28, 2025)

---

### 2. Supply Side - Wind Generation

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| ENTSO-E | `wind_forecast.json` | Wind generation forecast (MW) | NL, DE_LU, BE, DK_1 |
| Open-Meteo | `wind_forecast.json` | Wind speed at 10m/80m/120m/180m, direction, gusts, air density | 9 offshore locations |
| Google Weather | `weather_forecast_multi_location.json` | Full weather data | 15 strategic onshore locations |

**Offshore Wind Locations** (via Open-Meteo - actual offshore coordinates):
- 🇳🇱 Borssele (1.5 GW), Hollandse Kust (3.5 GW), Gemini (600 MW), IJmuiden Ver (4 GW)
- 🇩🇪 Helgoland Cluster, Borkum Riffgrund
- 🇬🇧 Dogger Bank (3.6 GW)
- 🇩🇰 Horns Rev
- 🇧🇪 North Sea BE

**Why Open-Meteo for Offshore?**
Google Weather API doesn't support open-sea coordinates (returns 404 errors). Open-Meteo's global ICON/GFS models work for any location including offshore, and provide wind at multiple heights relevant for wind turbine hub heights (80m, 120m, 180m).

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

### 5. Grid Balance & Cross-border Flows

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| TenneT | `grid_imbalance.json` | Imbalance prices, volumes | Netherlands |
| ENTSO-E | `cross_border_flows.json` | Physical flows (MW), net position | 10 NL borders |

**Cross-border Flow Pairs**:
- 🇳🇱↔🇩🇪 Netherlands ↔ Germany-Luxembourg
- 🇳🇱↔🇧🇪 Netherlands ↔ Belgium
- 🇳🇱↔🇳🇴 Netherlands ↔ Norway (NorNed cable)
- 🇳🇱↔🇬🇧 Netherlands ↔ Great Britain (BritNed cable)
- 🇳🇱↔🇩🇰 Netherlands ↔ Denmark (COBRAcable)

**Historical Records**: ~24 days (imbalance), ~1 day (flows - new)

---

### 6. Load Forecasts (Demand)

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| ENTSO-E | `load_forecast.json` | Day-ahead load forecast (MW), actual load | NL, DE_LU |

**Computed Variables**:
- **Forecast Error** = forecast - actual → Model accuracy indicator

**Historical Records**: ~1 day (new)

---

### 7. Generation by Type (Nuclear)

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| ENTSO-E | `generation_forecast.json` | Nuclear generation (MW), availability % | France |

**Key Metrics**:
- **Nuclear Actual/Forecast**: MW output from French nuclear fleet
- **Nuclear Availability**: % of 61 GW installed capacity online

French nuclear (~61 GW installed) is the largest single source in Europe. Outages cause price spikes across the continent.

**Historical Records**: ~1 day (new)

---

### 8. Calendar Features

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| Python holidays | `calendar_features.json` | Holiday flags, day type, season | NL, DE, BE, FR |

**Calendar Variables**:
- **is_weekend**: Weekend indicator
- **is_working_day**: Not weekend and not NL holiday
- **is_holiday_nl/de/be/fr**: Holiday flags per country
- **holiday_impact**: Weighted holiday impact (0.0-1.0)
- **is_bridge_day**: Day between holiday and weekend
- **season**: winter/spring/summer/fall
- **day_of_week**: 0=Monday to 6=Sunday

**Historical Records**: ~1 day (new)

---

### 9. Market Proxies (Carbon & Gas Prices)

| Source | File | Variables | Coverage |
|--------|------|-----------|----------|
| Alpha Vantage | `market_proxies.json` | Carbon price (KEUA ETF) | EU ETS proxy |
| Alpha Vantage | `market_proxies.json` | Gas price (UNG ETF) | US gas (correlates with EU) |

**Why ETF Proxies?**
Direct access to EU carbon (EUA) and TTF gas prices requires expensive exchange subscriptions. ETFs that track these commodities provide free, daily, market-reflective prices via Alpha Vantage API.

**Tickers Used**:
- **KEUA**: KraneShares European Carbon Allowance ETF (tracks EUA futures)
- **UNG**: United States Natural Gas Fund (correlates with EU gas prices)

**Collected Variables**:
- Current price, open, high, low, volume
- Lagged values (T-1, T-2, T-7) for ML features
- Rolling statistics (7d/30d mean, volatility, trend)
- 30-day price history

**Data Leakage Prevention**: Use lagged values (`price_lag1`, `price_lag7`) instead of current price for prediction models.

See [CARBON_GAS_PRICE_PROXIES.md](CARBON_GAS_PRICE_PROXIES.md) for detailed documentation.

**Historical Records**: ~1 day (new)

---

## Gap Analysis: What's Missing?

### 🔴 Critical Gaps (High Impact on Price)

| Data Type | Why Important | Potential Sources | Status |
|-----------|---------------|-------------------|--------|
| **Gas Prices** | Natural gas often sets marginal electricity price | Alpha Vantage (UNG ETF) | ✅ Collecting (proxy) |
| **CO2/Carbon Prices** | EU ETS affects fossil generation costs | Alpha Vantage (KEUA ETF) | ✅ Collecting (proxy) |
| **Cross-border Flows** | Import/export affects supply | ENTSO-E | ✅ Collecting |
| **Load Forecast** | Actual demand predictions | ENTSO-E, TenneT | ✅ Collecting |

### 🟡 Important Gaps (Medium Impact)

| Data Type | Why Important | Potential Sources | Status |
|-----------|---------------|-------------------|--------|
| **Nuclear Availability** | French/Belgian nuclear affects regional prices | ENTSO-E, EDF | ✅ Collecting |
| **Hydro Reservoir Levels** | Nordic hydro affects price dynamics | Nord Pool, ENTSO-E | ❌ Not collected |
| **Coal Prices** | Backup fuel for price setting | ICE, API2 | ❌ Not collected |
| **Interconnector Capacity** | Transmission constraints | JAO, ENTSO-E | ❌ Not collected |

### 🟢 Nice to Have (Lower Impact)

| Data Type | Why Important | Potential Sources | Status |
|-----------|---------------|-------------------|--------|
| **Industrial Production** | Economic activity affects demand | CBS, Eurostat | ❌ Not collected |
| **Holiday Calendar** | Demand patterns change | Python holidays lib | ✅ Collecting |
| **Day Type Features** | Weekend vs weekday patterns | Computed | ✅ Collecting |

---

## Recommended Additional Data Sources

### 1. Gas Prices - ✅ IMPLEMENTED
```
Source: Alpha Vantage API (UNG ETF proxy)
API: Free tier (25 requests/day)
Why: Gas plants often set marginal price, especially during low renewable periods
Note: UNG tracks US gas which correlates with EU prices via LNG trade
```

### 2. Carbon Prices (EU ETS) - ✅ IMPLEMENTED
```
Source: Alpha Vantage API (KEUA ETF proxy)
API: Free tier (25 requests/day)
Why: CO2 cost adds €20-100/MWh to fossil generation
Note: KEUA directly tracks EU ETS carbon allowance futures
```

### 3. Cross-border Flows - ✅ IMPLEMENTED
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
- Good offshore wind coverage (major capacity areas) using actual offshore coordinates
- Multi-height wind data (10m, 80m, 120m, 180m) for accurate turbine power estimation
- Free solar/demand weather via Open-Meteo
- Free offshore wind via Open-Meteo (no API key required)
- Automated daily collection with CI/CD
- Cross-border flows for all 5 NL interconnectors
- Load forecasts for NL and Germany
- French nuclear availability tracking

### ⚠️ Weaknesses
- No fuel prices (gas, coal, carbon)
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
| Offshore wind weather | ✅ Since 1940 | Open-Meteo Historical |
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
| Open-Meteo (Solar) | **Free** | ❌ No |
| Open-Meteo (Demand Weather) | **Free** | ❌ No |
| Open-Meteo (Offshore Wind) | **Free** | ❌ No |
| Open-Meteo Historical | **Free** | ❌ No |
| Alpha Vantage | **Free** (25/day) | ✅ Yes |

---

## Recommended Next Steps

### Phase 1: Quick Wins (Free Data) - ✅ COMPLETE
1. [x] Add ENTSO-E cross-border flows ✅ **DONE**
2. [x] Add ENTSO-E load forecast ✅ **DONE**
3. [x] Add French nuclear availability from ENTSO-E ✅ **DONE**
4. [x] Add calendar features (holidays, day-of-week) ✅ **DONE**
5. [x] Add gas/carbon price proxies via Alpha Vantage ✅ **DONE**
6. [ ] Backfill historical data using Open-Meteo + ENTSO-E

### Phase 2: Enhanced Coverage
1. [ ] Interconnector capacity and congestion (JAO)
2. [ ] Hydro reservoir levels (Nordic)

### Phase 3: Advanced Features
1. [ ] Economic indicators (industrial production)
2. [ ] More granular weather data

---

## File Structure

```
data/
├── energy_price_forecast.json          # Combined price data (ENTSO-E, EnergyZero, EPEX, Elspot)
├── grid_imbalance.json                 # TenneT imbalance data
├── cross_border_flows.json             # ENTSO-E physical flows (10 NL borders)
├── load_forecast.json                  # ENTSO-E load forecasts (NL, DE_LU)
├── generation_forecast.json            # ENTSO-E generation by type (FR nuclear)
├── weather_forecast_multi_location.json # Google Weather (15 locations)
├── wind_forecast.json                  # ENTSO-E wind + offshore weather
├── ned_production.json                 # NED.nl solar/wind production
├── solar_forecast.json                 # Open-Meteo solar irradiance (7 locations)
├── demand_weather_forecast.json        # Open-Meteo demand weather (11 locations)
├── calendar_features.json              # Calendar features (holidays, day type, season)
├── market_proxies.json                 # Carbon/gas prices via Alpha Vantage (KEUA, UNG)
└── YYMMDD_HHMMSS_*.json               # Timestamped historical copies
```

---

## References

- [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/)
- [Open-Meteo API](https://open-meteo.com/)
- [TenneT API](https://www.tennet.eu/)
- [NED.nl](https://ned.nl/)
- [Alpha Vantage API](https://www.alphavantage.co/)
- [KraneShares KEUA ETF](https://kraneshares.com/keua/)

---

*Document created: 2025-12-01*
*Last updated: 2025-12-03 (Added Open-Meteo offshore wind collector for actual offshore coordinates)*
