# Local Generation Predictor (Model B) - Planning Document

## Overview

Machine learning system for predicting local renewable energy generation (solar PV + wind turbines) at the HAN lab in Arnhem. This model enables the electrolyzer scheduler to optimize between self-consumption (free local energy) and grid consumption (during cheap price periods).

## Motivation

**Problem:** Electrolyzer can be powered by:
1. **Local renewables** (solar + wind) → free energy, prioritize when available
2. **Grid electricity** → paid energy, use when cheap AND local insufficient

**Need:** Week-ahead forecast of local generation to:
- Plan multi-day electrolyzer schedules considering both sources
- Coordinate thermal cycling with local generation patterns
- Maximize self-consumption ratio
- Buy from grid only during price valleys when local is insufficient

**Key Insight:** This is fundamentally different from grid price prediction:
- Input: Weather forecasts (same sources as Model A)
- Output: Local kW generation (site-specific physical model)
- Training data: Historical generation + weather (need sensor data!)

## Architecture Position

```
┌─────────────────────────────────────────────────────────┐
│ energyDataHub (data collection)                         │
│ ├── Weather forecasts (OpenWeather/MeteoServer/Google)  │
│ ├── Grid prices (ENTSO-E)                               │
│ └── Local generation sensors [NEW MODULE NEEDED]        │
└────────────────────┬────────────────────────────────────┘
                     │
                     │ Published to GitHub Pages
                     │
    ┌────────────────┼────────────────┐
    │                │                │
    ▼                ▼                ▼
┌─────────┐  ┌──────────────┐  ┌─────────────────┐
│ Model A │  │   Model B    │  │ Electrolyzer    │
│ Grid    │  │   Local Gen  │  │ Scheduler       │
│ Price   │  │   Prediction │  │ (combines both) │
└─────────┘  └──────────────┘  └─────────────────┘
             [THIS DOCUMENT]
```

**Model B sits between data collection and scheduling:**
- Consumes: Weather forecasts + historical generation data
- Produces: Week-ahead local generation forecast (kW, hourly)
- Used by: Electrolyzer scheduler for self-consumption optimization

## Data Requirements

### Input Data

**1. Weather Forecasts (already collected by energyDataHub):**
```json
// From weather_forecast.json
{
  "temperature": 12.5,        // °C
  "wind_speed": 5.2,          // m/s
  "wind_direction": 240,      // degrees
  "cloud_cover": 45,          // %
  "humidity": 78,             // %
  "pressure": 1013,           // hPa
  "precipitation": 0.0        // mm
}

// From sun_forecast.json
{
  "sun_elevation": 32.5,      // degrees
  "sun_azimuth": 180,         // degrees
  "sunrise": "07:23",
  "sunset": "17:45",
  "daylight_duration": 10.37  // hours
}
```

**2. Local Generation Data (NEW - needs sensor integration):**

**Option A: Direct sensor API (preferred if available):**
```json
// From local inverters/controllers
{
  "timestamp": "2025-01-04T14:00:00+01:00",
  "solar_generation_kw": 12.3,
  "wind_generation_kw": 3.7,
  "total_generation_kw": 16.0,
  "solar_irradiance_w_m2": 450,  // if available
  "panel_temperature_c": 25      // if available
}
```

**Option B: Utility meter scraping (if no API):**
```json
// From smart meter / energy management system
{
  "timestamp": "2025-01-04T14:00:00+01:00",
  "net_generation_kw": 16.0,     // total renewable output
  "consumption_kw": 8.5,          // lab load
  "net_export_kw": 7.5            // surplus to grid
}
```

**Option C: Manual logging (last resort):**
- CSV file with hourly readings
- Updated daily from monitoring displays
- Not ideal for automation

### Installation Specifications (REQUIRED)

**Solar PV System:**
```ini
[solar]
capacity_kwp = 50.0              # Peak capacity
panel_type = monocrystalline     # or polycrystalline
panel_tilt = 30                  # degrees from horizontal
panel_azimuth = 180              # degrees (0=N, 90=E, 180=S, 270=W)
inverter_efficiency = 0.96       # typical 95-98%
temperature_coefficient = -0.004 # %/°C (typical -0.3 to -0.5%/°C)
installation_date = 2020-03-15   # for degradation modeling
location_lat = 51.9851           # Arnhem
location_lon = 5.8987
shading_profile = urban_moderate # or open_field, urban_heavy, etc.
```

**Wind Turbine System:**
```ini
[wind]
capacity_kw = 10.0               # Rated capacity
cut_in_speed_ms = 3.0            # m/s - min speed to start
rated_speed_ms = 12.0            # m/s - speed at rated power
cut_out_speed_ms = 25.0          # m/s - max speed (shutdown)
hub_height_m = 15.0              # height above ground
rotor_diameter_m = 5.0
power_curve_file = turbine_model_X.csv  # manufacturer data
location_lat = 51.9851
location_lon = 5.8987
terrain_type = urban             # urban, suburban, rural, coastal
nearby_obstacles = building_NE   # affects wind profile
```

**Critical:** Without these specs, modeling accuracy drops significantly!

## Model Architecture

### Problem Formulation

**Type:** Multi-output time series forecasting with physics-informed features

**Target Variables:**
- Solar generation (kW) - hourly, 168h ahead
- Wind generation (kW) - hourly, 168h ahead
- Total generation (kW) - sum of above

**Approach:** Separate models for solar and wind, then combine

### Solar Generation Model

**Physics-Based Features (computed from weather):**

```python
# Solar irradiance estimation
def compute_solar_features(weather, sun_position, panel_specs):
    """
    Compute physics-based solar features from weather forecasts
    """
    # 1. Clear-sky irradiance (theoretical maximum)
    G_clear = compute_clear_sky_irradiance(
        sun_elevation=sun_position['elevation'],
        day_of_year=date.timetuple().tm_yday,
        altitude_m=50  # Arnhem elevation
    )

    # 2. Cloud attenuation
    cloud_factor = 1.0 - 0.75 * (cloud_cover / 100.0)
    G_horizontal = G_clear * cloud_factor

    # 3. Panel angle adjustment (cosine loss)
    angle_of_incidence = compute_AOI(
        sun_elevation=sun_position['elevation'],
        sun_azimuth=sun_position['azimuth'],
        panel_tilt=panel_specs['tilt'],
        panel_azimuth=panel_specs['azimuth']
    )
    G_panel = G_horizontal * cos(angle_of_incidence)

    # 4. Temperature derating
    panel_temp = ambient_temp + 25 * (G_panel / 1000.0)  # empirical
    temp_loss = panel_specs['temp_coefficient'] * (panel_temp - 25)
    efficiency = 1.0 + temp_loss

    # 5. Theoretical power output
    P_theoretical = (
        G_panel                           # W/m²
        * panel_specs['area_m2']          # m²
        * panel_specs['efficiency']       # typically 0.15-0.22
        * efficiency                      # temperature derating
        * panel_specs['inverter_eff']    # typically 0.96
    ) / 1000.0  # Convert to kW

    return {
        'G_clear': G_clear,
        'G_horizontal': G_horizontal,
        'G_panel': G_panel,
        'angle_of_incidence': angle_of_incidence,
        'panel_temperature': panel_temp,
        'P_theoretical': P_theoretical,
        'cloud_factor': cloud_factor
    }
```

**ML Model on Top of Physics:**

Why not just use theoretical model?
- Shading (trees, buildings) not captured
- Panel degradation over time
- Soiling (dust, bird droppings)
- Inverter clipping at high irradiance
- Snow cover in winter

**Architecture:**
```python
# Hybrid approach: Physics + ML correction

# Step 1: Compute theoretical power (physics)
P_physics = compute_solar_features(weather, sun, specs)

# Step 2: ML learns the correction factor
# Training data: actual_generation / P_theoretical
correction_factor = ML_model.predict(features=[
    P_physics['P_theoretical'],
    P_physics['angle_of_incidence'],
    P_physics['cloud_factor'],
    hour_of_day,
    day_of_year,
    days_since_cleaning,  # if tracked
    temperature,
    humidity,
    precipitation_last_24h  # snow/rain affects output
])

# Step 3: Final prediction
P_predicted = P_physics * correction_factor
```

**ML Model Options:**
1. **Random Forest** - captures non-linear interactions (shading patterns)
2. **Gradient Boosting** (XGBoost/LightGBM) - high accuracy
3. **Neural Network** - if sufficient data (>1 year hourly = 8760 samples)
4. **Simple linear regression** - if theoretical model is good

**Training Strategy:**
- Train on actual_generation ~ physics_features
- Cross-validate by season (summer/winter different patterns)
- Separate models for clear/cloudy/rainy conditions? (optional)

### Wind Generation Model

**Physics-Based Features:**

```python
def compute_wind_features(weather, turbine_specs):
    """
    Compute wind power from weather forecasts
    """
    # 1. Extrapolate wind speed to hub height
    # Weather API gives 10m height, turbine at hub_height
    wind_speed_hub = weather['wind_speed_10m'] * (
        (turbine_specs['hub_height'] / 10.0) ** 0.2
    )  # Power law approximation (exponent varies by terrain)

    # 2. Apply turbine power curve
    if wind_speed_hub < turbine_specs['cut_in_speed']:
        P_theoretical = 0.0  # Below cut-in
    elif wind_speed_hub > turbine_specs['cut_out_speed']:
        P_theoretical = 0.0  # Above cut-out (safety shutdown)
    elif wind_speed_hub >= turbine_specs['rated_speed']:
        P_theoretical = turbine_specs['capacity_kw']  # Flat rated power
    else:
        # Between cut-in and rated: cubic relationship
        P_theoretical = turbine_specs['capacity_kw'] * (
            (wind_speed_hub ** 3) / (turbine_specs['rated_speed'] ** 3)
        )

    # 3. Air density correction (temperature/pressure effects)
    air_density = compute_air_density(
        temperature=weather['temperature'],
        pressure=weather['pressure'],
        humidity=weather['humidity']
    )
    density_correction = air_density / 1.225  # standard density
    P_theoretical *= density_correction

    return {
        'wind_speed_hub': wind_speed_hub,
        'P_theoretical': P_theoretical,
        'air_density': air_density
    }
```

**ML Correction for Local Effects:**

Wind is **much harder** than solar due to:
- Local terrain (buildings create turbulence in Arnhem urban setting)
- Wind direction matters (obstacles in specific directions)
- Gustiness vs. sustained wind
- Wake effects (if multiple turbines)

**Architecture:**
```python
# Physics + ML correction (similar to solar)

P_physics = compute_wind_features(weather, turbine_specs)

correction_factor = ML_model.predict(features=[
    P_physics['P_theoretical'],
    P_physics['wind_speed_hub'],
    weather['wind_direction'],  # CRITICAL for urban sites
    hour_of_day,                # diurnal wind patterns
    season,
    temperature,                # affects air density
    pressure_trend              # weather front passing?
])

P_predicted = P_physics * correction_factor
```

**Wind Direction Encoding:**
- Circular encoding: `sin(direction)`, `cos(direction)`
- Or categorical: N, NE, E, SE, S, SW, W, NW
- Some directions have obstacles (buildings), others clear

**Training Strategy:**
- Wind is more variable than solar → need more data
- Separate models by wind direction quadrant? (if obstacles directional)
- Consider ensemble: avg of 3-5 weather forecasts (wind forecast uncertainty high)

### Combined Generation Model

**Simple approach:**
```python
P_total = P_solar_predicted + P_wind_predicted
```

**Advanced approach (if interaction exists):**
- Cloudy days → often windy (weather fronts)
- Sunny days → often calm (high pressure systems)
- Could train model on `P_total ~ P_solar_theoretical + P_wind_theoretical`
- Captures correlation in weather patterns

## Data Collection Module (for energyDataHub)

### New Module Structure

```
local_generation_fetchers/
├── __init__.py
├── sensor_data_client.py          # Read from local sensors
├── google_solar_client.py         # Fallback if no solar sensors
├── solar_power_calculator.py      # Physics-based solar model
└── wind_power_calculator.py       # Physics-based wind model
```

### Implementation Priority

**Phase 1: Sensor Integration (if sensors available)**

```python
# local_generation_fetchers/sensor_data_client.py

import requests
from datetime import datetime
from utils.data_types import EnhancedDataSet

async def get_local_generation_data(config):
    """
    Fetch current generation from local sensors

    Supports:
    - SolarEdge API (common solar inverter)
    - SMA Sunny Portal
    - Generic Modbus TCP (many industrial inverters)
    - Custom REST API
    """

    sensor_type = config.get('sensor_type', 'modbus')

    if sensor_type == 'solaredge':
        data = await fetch_solaredge_data(
            site_id=config['site_id'],
            api_key=config['api_key']
        )
    elif sensor_type == 'modbus':
        data = await fetch_modbus_data(
            host=config['modbus_host'],
            port=config['modbus_port'],
            registers=config['registers']
        )
    elif sensor_type == 'rest':
        data = await fetch_rest_api_data(
            url=config['sensor_url'],
            auth=config['auth']
        )
    else:
        raise ValueError(f"Unsupported sensor type: {sensor_type}")

    # Standardize format
    dataset = EnhancedDataSet(
        name="Local Generation",
        source=f"sensors_{sensor_type}",
        unit="kW",
        data=[{
            'datetime': datetime.now().isoformat(),
            'solar_generation_kw': data['solar'],
            'wind_generation_kw': data['wind'],
            'total_generation_kw': data['solar'] + data['wind']
        }]
    )

    return dataset
```

**Phase 2: Google Solar API (if no solar sensors)**

```python
# local_generation_fetchers/google_solar_client.py

from google.maps import solar_v1

async def get_google_solar_forecast(config):
    """
    Use Google Solar API for Arnhem location

    Advantages:
    - Considers actual building shading (satellite + 3D models)
    - Panel-specific configurations
    - No local sensors needed

    Disadvantages:
    - Costs $0.01-0.02 per request
    - Requires panel installation details
    """

    client = solar_v1.SolarClient(api_key=config['google_api_key'])

    response = client.get_solar_potential(
        latitude=config['latitude'],
        longitude=config['longitude'],
        # Panel configuration
        panel_capacity_watts=config['solar']['capacity_kwp'] * 1000,
        panel_count=config['solar']['panel_count'],
        panel_tilt_degrees=config['solar']['panel_tilt'],
        panel_azimuth_degrees=config['solar']['panel_azimuth'],
        # Quality
        imagery_quality='HIGH'
    )

    # Extract hourly forecast (next 7 days)
    forecast_data = []
    for hour_data in response.hourly_forecast:
        forecast_data.append({
            'datetime': hour_data.timestamp,
            'solar_generation_kwh': hour_data.dc_energy_kwh,
            'irradiance_w_m2': hour_data.irradiance
        })

    dataset = EnhancedDataSet(
        name="Solar Generation Forecast",
        source="google_solar_api",
        unit="kW",
        data=forecast_data
    )

    return dataset
```

**Phase 3: Physics Models (for ML feature engineering)**

```python
# local_generation_fetchers/solar_power_calculator.py

import numpy as np
from datetime import datetime
import pvlib  # Python library for solar PV modeling

class SolarPowerCalculator:
    """
    Physics-based solar PV power estimation
    Uses pvlib (NREL) for accurate solar position and irradiance
    """

    def __init__(self, panel_config):
        self.capacity_kw = panel_config['capacity_kwp']
        self.tilt = panel_config['panel_tilt']
        self.azimuth = panel_config['panel_azimuth']
        self.lat = panel_config['location_lat']
        self.lon = panel_config['location_lon']
        self.efficiency = panel_config.get('efficiency', 0.18)
        self.temp_coeff = panel_config.get('temperature_coefficient', -0.004)

    def calculate_power(self, weather_data, timestamp):
        """
        Calculate theoretical solar power from weather forecast

        Args:
            weather_data: dict with temperature, cloud_cover
            timestamp: datetime object

        Returns:
            dict with power_kw and intermediate calculations
        """

        # 1. Solar position
        solar_position = pvlib.solarposition.get_solarposition(
            time=timestamp,
            latitude=self.lat,
            longitude=self.lon
        )
        elevation = solar_position['elevation'].iloc[0]
        azimuth = solar_position['azimuth'].iloc[0]

        if elevation <= 0:
            # Night time
            return {'power_kw': 0.0, 'elevation': elevation}

        # 2. Clear-sky irradiance
        clearsky = pvlib.clearsky.ineichen(
            apparent_zenith=90 - elevation,
            airmass_absolute=pvlib.atmosphere.get_absolute_airmass(
                pvlib.atmosphere.get_relative_airmass(90 - elevation)
            ),
            altitude=50  # Arnhem elevation
        )
        ghi_clear = clearsky['ghi'].iloc[0]  # Global Horizontal Irradiance

        # 3. Cloud attenuation
        cloud_cover = weather_data.get('cloud_cover', 0)
        cloud_factor = 1.0 - 0.75 * (cloud_cover / 100.0)
        ghi = ghi_clear * cloud_factor

        # 4. Plane-of-array irradiance (tilted panel)
        poa_irradiance = pvlib.irradiance.get_total_irradiance(
            surface_tilt=self.tilt,
            surface_azimuth=self.azimuth,
            solar_zenith=90 - elevation,
            solar_azimuth=azimuth,
            ghi=ghi,
            dhi=ghi * 0.15,  # Diffuse (approximation)
            dni=ghi * 0.85   # Direct (approximation)
        )
        poa = poa_irradiance['poa_global']

        # 5. Temperature derating
        ambient_temp = weather_data.get('temperature', 20)
        # Empirical: panel temp = ambient + 25°C per kW/m² of irradiance
        panel_temp = ambient_temp + 25 * (poa / 1000.0)
        temp_loss_factor = 1.0 + self.temp_coeff * (panel_temp - 25)

        # 6. Power calculation
        power_kw = (
            poa                      # W/m²
            * self.capacity_kw       # kW peak (already accounts for area)
            / 1000.0                 # STC irradiance
            * temp_loss_factor       # temperature derating
            * 0.96                   # inverter efficiency
        )

        return {
            'power_kw': max(0.0, power_kw),
            'elevation': elevation,
            'azimuth': azimuth,
            'ghi': ghi,
            'poa': poa,
            'panel_temp': panel_temp,
            'temp_loss_factor': temp_loss_factor
        }
```

### Configuration (add to energyDataHub)

**settings.ini additions:**
```ini
[local_generation]
enabled = 1
solar_enabled = 1
wind_enabled = 1

[solar]
capacity_kwp = 50.0
panel_count = 150
panel_tilt = 30
panel_azimuth = 180
panel_type = monocrystalline
efficiency = 0.18
temperature_coefficient = -0.004
inverter_efficiency = 0.96
installation_date = 2020-03-15

[wind]
capacity_kw = 10.0
turbine_model = Generic_10kW
cut_in_speed_ms = 3.0
rated_speed_ms = 12.0
cut_out_speed_ms = 25.0
hub_height_m = 15.0
rotor_diameter_m = 5.0

[sensors]
# Choose one: modbus, solaredge, rest, google, none
solar_sensor_type = modbus
wind_sensor_type = modbus

# Modbus configuration (if applicable)
modbus_host = 192.168.1.100
modbus_port = 502
solar_register = 40001
wind_register = 40002

# Google Solar API (if no sensors)
use_google_solar = 0
google_api_key = YOUR_KEY_HERE
```

**secrets.ini additions (if using cloud APIs):**
```ini
[google]
api_key = <your_google_solar_api_key>

[sensors]
# If sensors require authentication
sensor_api_key = <your_sensor_api_key>
sensor_username = <username>
sensor_password = <password>
```

## ML Model Implementation (in energyPricePredictor repo)

### Repository Structure Additions

```
energyPricePredictor/
├── src/
│   ├── models/
│   │   ├── grid_price_predictor.py    # Model A
│   │   ├── local_generation_predictor.py  # Model B [THIS DOC]
│   │   └── combined_scheduler.py      # Uses both models
│   ├── data/
│   │   ├── feature_engineer_generation.py  # Solar/wind features
│   │   └── physics_models.py          # Solar/wind calculators
```

### Training Pipeline

```python
# src/models/local_generation_predictor.py

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit
import pandas as pd
import numpy as np

class LocalGenerationPredictor:
    """
    Predicts local solar + wind generation from weather forecasts
    """

    def __init__(self, config):
        self.solar_model = RandomForestRegressor(n_estimators=100)
        self.wind_model = RandomForestRegressor(n_estimators=100)
        self.solar_calculator = SolarPowerCalculator(config['solar'])
        self.wind_calculator = WindPowerCalculator(config['wind'])

    def prepare_features(self, weather_df, sun_df):
        """
        Engineer features from weather forecasts
        """
        features = pd.DataFrame()

        # Time features
        features['hour'] = weather_df.index.hour
        features['day_of_year'] = weather_df.index.dayofyear
        features['month'] = weather_df.index.month

        # Weather features (raw)
        features['temperature'] = weather_df['temperature']
        features['wind_speed'] = weather_df['wind_speed']
        features['wind_direction_sin'] = np.sin(np.radians(weather_df['wind_direction']))
        features['wind_direction_cos'] = np.cos(np.radians(weather_df['wind_direction']))
        features['cloud_cover'] = weather_df['cloud_cover']
        features['humidity'] = weather_df['humidity']
        features['pressure'] = weather_df['pressure']

        # Solar physics features
        for idx, row in weather_df.iterrows():
            solar_calc = self.solar_calculator.calculate_power(
                weather_data=row.to_dict(),
                timestamp=idx
            )
            features.loc[idx, 'solar_elevation'] = solar_calc['elevation']
            features.loc[idx, 'solar_poa_irradiance'] = solar_calc['poa']
            features.loc[idx, 'solar_theoretical_kw'] = solar_calc['power_kw']
            features.loc[idx, 'solar_panel_temp'] = solar_calc['panel_temp']

        # Wind physics features
        for idx, row in weather_df.iterrows():
            wind_calc = self.wind_calculator.calculate_power(
                weather_data=row.to_dict()
            )
            features.loc[idx, 'wind_speed_hub'] = wind_calc['wind_speed_hub']
            features.loc[idx, 'wind_theoretical_kw'] = wind_calc['power_kw']
            features.loc[idx, 'air_density'] = wind_calc['air_density']

        return features

    def train(self, historical_data):
        """
        Train solar and wind models separately

        Args:
            historical_data: DataFrame with columns:
                - weather features
                - sun position
                - actual_solar_kw (from sensors)
                - actual_wind_kw (from sensors)
        """

        # Prepare features
        X = self.prepare_features(
            weather_df=historical_data[weather_cols],
            sun_df=historical_data[sun_cols]
        )

        y_solar = historical_data['actual_solar_kw']
        y_wind = historical_data['actual_wind_kw']

        # Time series cross-validation
        tscv = TimeSeriesSplit(n_splits=5)

        # Train solar model
        print("Training solar generation model...")
        self.solar_model.fit(X, y_solar)
        solar_score = np.mean([
            self.solar_model.score(X[test], y_solar[test])
            for train, test in tscv.split(X)
        ])
        print(f"Solar model R² score: {solar_score:.3f}")

        # Train wind model
        print("Training wind generation model...")
        self.wind_model.fit(X, y_wind)
        wind_score = np.mean([
            self.wind_model.score(X[test], y_wind[test])
            for train, test in tscv.split(X)
        ])
        print(f"Wind model R² score: {wind_score:.3f}")

        return {
            'solar_r2': solar_score,
            'wind_r2': wind_score
        }

    def predict(self, weather_forecast, sun_forecast):
        """
        Predict generation for next 7 days

        Returns:
            DataFrame with columns:
                - datetime
                - solar_kw
                - wind_kw
                - total_kw
        """

        X = self.prepare_features(weather_forecast, sun_forecast)

        predictions = pd.DataFrame()
        predictions['datetime'] = X.index
        predictions['solar_kw'] = self.solar_model.predict(X)
        predictions['wind_kw'] = self.wind_model.predict(X)
        predictions['total_kw'] = predictions['solar_kw'] + predictions['wind_kw']

        # Clip negative predictions (can happen with ML)
        predictions[predictions < 0] = 0

        return predictions
```

## Evaluation Metrics

**Solar-specific:**
- MAE (kW) - overall accuracy
- RMSE (kW) - penalize large errors
- Relative error during daylight hours (ignore nighttime zeros)
- Peak power prediction accuracy (critical for planning)
- Energy yield error (daily kWh totals)

**Wind-specific:**
- MAE (kW)
- RMSE (kW)
- Directional forecast skill (did we predict wind direction correctly?)
- Calm period detection (wind < cut-in speed)
- High wind event detection (wind > cut-out speed)

**Combined:**
- Self-consumption opportunity detection
- Multi-day pattern accuracy (e.g., "sunny Mon-Tue, cloudy Wed-Thu")

## Integration with Electrolyzer Scheduler

**Combined scheduling logic:**

```python
# Pseudocode for scheduler using both models

# Get predictions
grid_prices = ModelA.predict()        # EUR/MWh, 168h ahead
local_generation = ModelB.predict()   # kW, 168h ahead

# Electrolyzer parameters
electrolyzer_power_kw = 20.0
electrolyzer_efficiency = 0.65        # kWh_electric → kWh_H2
min_runtime_hours = 4                 # thermal constraint
max_temp_gradient = 50                # °C/hour

# Optimization objective: minimize cost
for hour in range(168):

    # Option 1: Run on local generation (free)
    if local_generation[hour] >= electrolyzer_power_kw:
        schedule[hour] = 'local'
        cost[hour] = 0.0

    # Option 2: Run on grid (paid)
    elif grid_prices[hour] < threshold:
        schedule[hour] = 'grid'
        cost[hour] = grid_prices[hour] * electrolyzer_power_kw / 1000.0

    # Option 3: Idle
    else:
        schedule[hour] = 'idle'
        cost[hour] = 0.0

    # Apply thermal constraints
    if schedule[hour] != schedule[hour-1]:
        # State change - check thermal gradients
        if not thermal_constraint_satisfied(schedule, hour):
            schedule[hour] = schedule[hour-1]  # Keep previous state

# Output: 168-hour binary schedule + cost projection
```

**Advanced: Hybrid operation**
- Partial load: run at 50% when local_gen = 10kW, electrolyzer = 20kW
- Top-up from grid: use local + grid simultaneously
- Requires electrolyzer supports variable load

## Data Publishing

**New endpoint for energyDataHub:**
```
https://ducroq.github.io/energydatahub/local_generation_forecast.json
```

**Format (matches existing schema):**
```json
{
  "metadata": {
    "generated_at": "2025-01-04T16:00:00Z",
    "location": "HAN Arnhem",
    "latitude": 51.9851,
    "longitude": 5.8987,
    "solar_capacity_kwp": 50.0,
    "wind_capacity_kw": 10.0,
    "model_version": "v1.0.0",
    "prediction_horizon_hours": 168
  },
  "datasets": [
    {
      "name": "solar_generation_forecast",
      "source": "ml_hybrid_physics",
      "unit": "kW",
      "data": [
        {
          "datetime": "2025-01-05T00:00:00+01:00",
          "generation_kw": 0.0,
          "confidence": 0.95
        },
        {
          "datetime": "2025-01-05T01:00:00+01:00",
          "generation_kw": 0.0,
          "confidence": 0.95
        },
        {
          "datetime": "2025-01-05T08:00:00+01:00",
          "generation_kw": 5.2,
          "confidence": 0.82
        },
        {
          "datetime": "2025-01-05T12:00:00+01:00",
          "generation_kw": 38.7,
          "confidence": 0.78
        }
      ]
    },
    {
      "name": "wind_generation_forecast",
      "source": "ml_hybrid_physics",
      "unit": "kW",
      "data": [...]
    },
    {
      "name": "total_generation_forecast",
      "source": "combined",
      "unit": "kW",
      "data": [...]
    }
  ]
}
```

## Implementation Phases

### Phase 1: Data Collection (Week 1-2)
- [ ] Identify sensor types (solar inverter, wind controller)
- [ ] Implement sensor data fetcher in energyDataHub
- [ ] Start collecting historical generation data (need 3-6 months)
- [ ] Collect parallel weather data (already done)
- [ ] Store in `local_generation_history.json`

### Phase 2: Physics Models (Week 3)
- [ ] Implement solar power calculator (using pvlib)
- [ ] Implement wind power calculator
- [ ] Validate against 1 week of actual data
- [ ] Tune parameters (shading factors, terrain coefficients)

### Phase 3: ML Model (Week 4-6)
- [ ] Feature engineering pipeline
- [ ] Train solar generation model
- [ ] Train wind generation model
- [ ] Evaluate on holdout set (last month)
- [ ] Compare ML vs. physics-only accuracy

### Phase 4: Integration (Week 7-8)
- [ ] Daily prediction generation (GitHub Actions)
- [ ] Publish to GitHub Pages
- [ ] API for scheduler consumption
- [ ] Monitoring dashboard

### Phase 5: Scheduler Integration (Week 9-10)
- [ ] Combined optimization (local + grid)
- [ ] Thermal constraint handling
- [ ] Backtesting on historical data
- [ ] Real-world deployment

## Critical Success Factors

**Data quality:**
- Sensor data must be reliable (no gaps, outliers)
- Weather forecast aligned with generation timestamps
- At least 6 months training data (capture seasonal variation)

**Physics model accuracy:**
- Solar: 15-20% MAE typical for urban sites (shading uncertainty)
- Wind: 25-35% MAE typical for small urban turbines (terrain complexity)
- ML can reduce error by 5-10% if good training data

**Model maintenance:**
- Retrain quarterly (panels degrade, vegetation grows, turbulence changes)
- Monitor prediction error drift
- Update after equipment changes (cleaning, repairs)

## Questions to Answer Before Implementation

1. **What sensors do you have?**
   - Solar: inverter brand/model? API available?
   - Wind: controller brand/model? Modbus/REST/manual?

2. **Do you have historical data?**
   - How far back (months/years)?
   - Format (CSV, database, manual logs)?
   - Granularity (hourly, 15-min, real-time)?

3. **Installation details:**
   - Solar: exact panel count, tilt, azimuth, kWp?
   - Wind: turbine model, hub height, power curve data?
   - Any shading obstacles (trees, buildings)?

4. **Is electrolyzer variable load capable?**
   - Can it run at 50% power (important for partial local gen)?
   - Or binary on/off only?

5. **Budget for Google Solar API?**
   - ~$0.30/month if called daily
   - Only needed if no solar sensors

## Next Steps

1. **Save this document** for future reference
2. **Continue with Model A (grid price prediction)** as planned
3. **In parallel:** Start collecting local generation sensor data (if available)
4. **Timeline:** Model B implementation after Model A is working

Model B depends on historical data - start collecting now!

---

**Document Status:** Planning document for future implementation
**Priority:** Medium (after Model A)
**Dependencies:** Sensor access, 3-6 months historical data
**Owner:** [TBD]
**Last Updated:** 2025-01-04
