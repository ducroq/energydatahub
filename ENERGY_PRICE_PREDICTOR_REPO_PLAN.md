# Energy Price Predictor - Repository Plan

## Overview

A machine learning system for predicting electricity prices 2-7 days ahead to enable optimal scheduling of slow energy conversion processes (electrolyzers, heat pumps, large-scale storage). Consumes data from the [energyDataHub](https://github.com/ducroq/energydatahub) public API.

## Motivation

**Problem:** Day-ahead energy prices are insufficient for processes with slow thermal dynamics.

**Example - Electrolyzer Scheduling:**
- Thermal constraints: hours-long warm-up/cool-down cycles
- Optimal operation: sustained runs during multi-day low-price periods
- Planning horizon: 3-7 days to optimize thermal cycling
- Decision needed: Monday morning, need to see prices through Friday

**Solution:** ML model predicting week-ahead prices using weather forecasts as leading indicators.

## Data Source

**Upstream System:** [energyDataHub](https://ducroq.github.io/energydatahub/)

Published JSON endpoints (updated daily at ~16:00 UTC):
```
https://ducroq.github.io/energydatahub/energy_price_forecast.json
https://ducroq.github.io/energydatahub/weather_forecast.json
https://ducroq.github.io/energydatahub/sun_forecast.json
https://ducroq.github.io/energydatahub/air_quality.json
```

**Data Format:**
- Encrypted by default (AES-256-CBC + HMAC-SHA256)
- Schema defined by `CombinedDataSet` and `EnhancedDataSet` classes
- Contains multiple data sources per file (ENTSO-E, Energy Zero, EPEX, Nord Pool for prices)
- Hourly granularity for prices, 3-hourly for weather
- Timestamps in local timezone with UTC offsets

**Historical Data:**
- energyDataHub archives timestamped files in `data/` directory
- Format: `{YYMMDD_HHMMSS}_{type}.json`
- Training dataset built from these archives
- Need 3-6 months minimum for seasonal patterns

## Architecture

### Repository Structure

```
energyPricePredictor/
├── README.md
├── requirements.txt           # ML dependencies (sktime, pandas, numpy, scikit-learn)
├── config/
│   ├── settings.ini          # Model hyperparameters, location config
│   └── secrets.ini           # Decryption keys (not in repo)
├── data/
│   ├── raw/                  # Downloaded from energyDataHub API
│   ├── processed/            # Feature-engineered datasets
│   ├── predictions/          # Model outputs (published)
│   └── models/               # Trained model artifacts (.pkl, .h5)
├── notebooks/                # Jupyter notebooks for experimentation
│   ├── 01_exploratory_analysis.ipynb
│   ├── 02_feature_engineering.ipynb
│   ├── 03_model_comparison.ipynb
│   └── 04_backtesting.ipynb
├── src/
│   ├── __init__.py
│   ├── data/
│   │   ├── __init__.py
│   │   ├── data_loader.py        # Fetch from energyDataHub API
│   │   ├── data_decrypter.py     # Decrypt if needed (copy from energyDataHub)
│   │   └── feature_engineer.py   # Weather → price features
│   ├── models/
│   │   ├── __init__.py
│   │   ├── base_predictor.py     # Abstract base class
│   │   ├── sktime_forecaster.py  # sktime-based models
│   │   ├── prophet_forecaster.py # Facebook Prophet
│   │   └── ensemble.py           # Model ensembling
│   ├── evaluation/
│   │   ├── __init__.py
│   │   ├── metrics.py            # MAE, RMSE, directional accuracy
│   │   └── backtesting.py        # Walk-forward validation
│   ├── train.py                  # Training pipeline
│   └── predict.py                # Inference pipeline
├── tests/
│   ├── test_data_loader.py
│   ├── test_features.py
│   └── test_models.py
├── scripts/
│   ├── download_historical_data.py  # Bulk download from energyDataHub
│   ├── retrain_model.py             # Weekly retraining script
│   └── generate_predictions.py      # Daily prediction generation
├── docs/
│   ├── data_schema.md            # energyDataHub API contract
│   ├── model_architecture.md
│   └── deployment.md
└── .github/
    └── workflows/
        ├── train_model.yml       # Weekly/monthly retraining
        ├── daily_predict.yml     # Daily prediction generation
        └── tests.yml             # CI/CD testing
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│ energyDataHub (upstream)                                    │
│ - Collects data daily                                       │
│ - Publishes to GitHub Pages                                 │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ HTTPS API (JSON)
                      │
┌─────────────────────▼───────────────────────────────────────┐
│ Data Loader (src/data/data_loader.py)                      │
│ - Fetch latest data                                         │
│ - Decrypt if needed                                         │
│ - Download historical archives                              │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ Raw JSON
                      │
┌─────────────────────▼───────────────────────────────────────┐
│ Feature Engineering (src/data/feature_engineer.py)         │
│ - Align timestamps                                          │
│ - Weather → energy features:                                │
│   * Wind power proxy (wind_speed³)                          │
│   * Solar irradiance (sun angle + cloud cover)              │
│   * Temperature deviation from seasonal norm                │
│   * Hour-of-day, day-of-week, holiday flags                 │
│ - Lag features (prices t-24h, t-168h)                       │
│ - Rolling statistics                                        │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ Feature Matrix (pandas DataFrame)
                      │
┌─────────────────────▼───────────────────────────────────────┐
│ ML Models (src/models/)                                     │
│                                                             │
│ Training Mode:                                              │
│ - Walk-forward validation                                   │
│ - Hyperparameter tuning                                     │
│ - Model selection                                           │
│ - Save artifacts to data/models/                            │
│                                                             │
│ Inference Mode:                                             │
│ - Load trained model                                        │
│ - Generate 2-7 day ahead predictions                        │
│ - Include uncertainty bands                                 │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ Predictions JSON
                      │
┌─────────────────────▼───────────────────────────────────────┐
│ Output (data/predictions/)                                  │
│ - predicted_prices.json (same schema as energyDataHub)      │
│ - Optionally publish to GitHub Pages                        │
│ - Consumed by electrolyzer scheduler                        │
└─────────────────────────────────────────────────────────────┘
```

## ML Approach

### Problem Formulation

**Type:** Multivariate time series forecasting with exogenous variables

**Target Variable:**
- Hourly electricity price (EUR/MWh or EUR/kWh)
- Prediction horizon: 48-168 hours ahead (2-7 days)

**Features:**

*Endogenous (autoregressive):*
- Historical prices (lag 24h, 48h, 168h)
- Rolling mean/std (24h, 168h windows)
- Price trend indicators

*Exogenous (weather):*
- Wind speed (cubed for power proxy)
- Temperature (deviation from seasonal mean)
- Cloud cover / solar radiation proxy
- Precipitation
- Sun elevation angle
- Hour of day, day of week, month
- Holiday flags

### Model Candidates

**Baseline Models:**
1. **Persistence:** Tomorrow's price = today's price
2. **Seasonal Naive:** Tomorrow's price = same hour last week
3. **Moving Average:** Smoothed historical prices

**Traditional Time Series:**
1. **SARIMAX:** Seasonal ARIMA with exogenous weather variables
2. **Prophet:** Facebook's decomposable time series model (trend + seasonality + holidays)
3. **ETS:** Exponential smoothing with trend/seasonality

**ML Models (via sktime):**
1. **AutoARIMA:** Automated ARIMA order selection
2. **TBATS:** Complex seasonality handling
3. **Reduction methods:** ML regressors (RandomForest, XGBoost) adapted for time series
4. **Deep learning:** LSTM, Temporal Convolutional Networks (if sufficient data)

**Ensemble:**
- Weighted average of top 3 models
- Stacking with meta-learner
- Uncertainty quantification via ensemble spread

### Evaluation Strategy

**Metrics:**
- MAE, RMSE (absolute price accuracy)
- MAPE (percentage error)
- Directional accuracy (did we predict up/down correctly?)
- Peak price detection (critical for electrolyzer scheduling)

**Validation:**
- Walk-forward validation (no data leakage)
- Train on months 1-6, validate on month 7
- Roll forward weekly, retrain monthly
- Separate validation for each forecast horizon (1d, 2d, ..., 7d ahead)

**Backtesting:**
- Simulate real scheduling decisions
- Calculate cost savings vs. baseline strategies
- Penalize for thermal cycling costs (electrolyzer-specific)

## Implementation Phases

### Phase 1: Data Infrastructure (Weeks 1-2)
- [ ] Set up repository structure
- [ ] Implement data loader from energyDataHub API
- [ ] Download and decrypt historical data (3-6 months)
- [ ] Build feature engineering pipeline
- [ ] Create train/validation splits
- [ ] Exploratory data analysis (notebook)

### Phase 2: Baseline Models (Weeks 3-4)
- [ ] Implement persistence and seasonal naive baselines
- [ ] Build evaluation framework
- [ ] Develop backtesting infrastructure
- [ ] Document baseline performance (sets target to beat)

### Phase 3: ML Models (Weeks 5-8)
- [ ] Implement SARIMAX with weather exogenous variables
- [ ] Implement Prophet model
- [ ] Implement sktime forecasters (AutoARIMA, reduction methods)
- [ ] Hyperparameter tuning for each model
- [ ] Model comparison notebook

### Phase 4: Ensemble & Production (Weeks 9-10)
- [ ] Build ensemble predictor
- [ ] Uncertainty quantification
- [ ] Set up daily prediction pipeline (GitHub Actions)
- [ ] Output schema matching energyDataHub format
- [ ] Deploy to GitHub Pages (optional)

### Phase 5: Integration (Weeks 11-12)
- [ ] API for electrolyzer scheduler to consume predictions
- [ ] Monitoring and alerting (prediction quality drift)
- [ ] Automated retraining pipeline
- [ ] Documentation and handoff

## Key Design Decisions

### Weather Features Priority

**Most Important (based on EU electricity markets):**
1. **Wind speed** → Directly drives renewable generation
2. **Temperature** → Heating/cooling demand
3. **Solar radiation** → Solar PV output
4. **Hour/day/season** → Predictable demand patterns

**Less Important:**
5. Air quality (weak signal)
6. Precipitation (correlated with cloud cover)

### Model Selection Criteria

**Start simple:**
- SARIMAX or Prophet likely sufficient for first version
- Linear models interpretable (important for trust)
- Fast training/inference

**Add complexity only if needed:**
- Deep learning requires >1 year of data
- Diminishing returns vs. engineering effort
- Electrolyzer scheduling robust to 10-20% prediction error

### Uncertainty Quantification

**Critical for robust scheduling:**
- Prediction intervals (10th, 50th, 90th percentile)
- Scheduler should hedge: delay start if uncertainty high
- Ensemble spread as uncertainty proxy
- Conformal prediction for calibrated intervals

### Retraining Frequency

**Initial plan:**
- Retrain weekly (GitHub Actions, Sunday nights)
- Daily predictions using trained model (fast inference)
- Monitor prediction error drift
- Full retraining with architecture search: monthly or quarterly

## Dependencies

```txt
# Core ML
sktime>=0.20.0
scikit-learn>=1.3.0
pandas>=2.0.0
numpy>=1.24.0

# Time series specific
prophet>=1.1.4
statsmodels>=0.14.0
pmdarima>=2.0.3          # AutoARIMA

# Deep learning (optional)
# tensorflow>=2.13.0
# pytorch>=2.0.0

# Feature engineering
timezonefinder>=6.2.0
pytz>=2023.3

# Data handling
requests>=2.31.0
aiohttp>=3.9.0

# Encryption (if handling encrypted data)
cryptography>=41.0.0

# Visualization
matplotlib>=3.7.0
seaborn>=0.12.0
plotly>=5.14.0

# Utilities
pyyaml>=6.0
python-dotenv>=1.0.0

# Testing
pytest>=7.4.0
pytest-cov>=4.1.0
```

## Configuration Example

**config/settings.ini:**
```ini
[location]
latitude = 51.8126
longitude = 5.8372
country = NL

[data_source]
# energyDataHub API
base_url = https://ducroq.github.io/energydatahub
encryption = 1

[model]
prediction_horizon_hours = 168  # 7 days
training_window_days = 180      # 6 months
validation_window_days = 30
retrain_frequency_days = 7

[features]
weather_lag_hours = 0,3,6,12,24
price_lag_hours = 24,48,168
include_calendar_features = 1
include_holiday_features = 1
```

**config/secrets.ini (not in repo):**
```ini
[encryption]
encryption_key = <base64_encoded_32_byte_key>
hmac_key = <base64_encoded_32_byte_key>

[api]
# If needed for future integrations
# google_api_key = ...
```

## Output Format

**Predictions published as JSON (same schema as energyDataHub):**

```json
{
  "metadata": {
    "generated_at": "2025-01-15T16:00:00Z",
    "model_version": "v1.2.3",
    "prediction_horizon_hours": 168,
    "model_type": "ensemble",
    "mae_validation_eur_mwh": 8.5
  },
  "datasets": [
    {
      "name": "predicted_prices",
      "source": "ml_ensemble",
      "unit": "EUR/MWh",
      "data": [
        {
          "datetime": "2025-01-16T00:00:00+01:00",
          "price_mean": 45.2,
          "price_p10": 38.1,
          "price_p90": 52.3
        },
        ...
      ]
    }
  ]
}
```

**Fields:**
- `price_mean`: Point prediction (50th percentile)
- `price_p10`: Lower bound (10th percentile) - optimistic scenario
- `price_p90`: Upper bound (90th percentile) - pessimistic scenario

Electrolyzer scheduler uses these bands for robust optimization.

## GitHub Actions Workflows

### Daily Prediction (`.github/workflows/daily_predict.yml`)

```yaml
name: Generate Daily Predictions
on:
  schedule:
    - cron: '30 16 * * *'  # 16:30 UTC (after energyDataHub updates)
  workflow_dispatch:

jobs:
  predict:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python scripts/generate_predictions.py
        env:
          ENCRYPTION_KEY: ${{ secrets.ENCRYPTION_KEY }}
          HMAC_KEY: ${{ secrets.HMAC_KEY }}
      - run: |
          git config user.name "GitHub Actions"
          git config user.email "actions@github.com"
          git add data/predictions/
          git commit -m "Update predictions $(date +'%Y-%m-%d %H:%M:%S')"
          git push
```

### Weekly Retraining (`.github/workflows/train_model.yml`)

```yaml
name: Retrain Model
on:
  schedule:
    - cron: '0 2 * * 0'  # Sunday 02:00 UTC
  workflow_dispatch:

jobs:
  train:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python scripts/retrain_model.py
        env:
          ENCRYPTION_KEY: ${{ secrets.ENCRYPTION_KEY }}
          HMAC_KEY: ${{ secrets.HMAC_KEY }}
      - run: |
          git config user.name "GitHub Actions"
          git config user.email "actions@github.com"
          git add data/models/
          git commit -m "Retrain model $(date +'%Y-%m-%d')"
          git push
```

## Integration with Electrolyzer Scheduler

**Downstream consumer (separate repo or module):**

```python
import requests

# Fetch predictions
response = requests.get('https://your-github-pages/predicted_prices.json')
predictions = response.json()

# Extract price bands
hours_ahead = []
prices_mean = []
prices_lower = []
prices_upper = []

for entry in predictions['datasets'][0]['data']:
    hours_ahead.append(entry['datetime'])
    prices_mean.append(entry['price_mean'])
    prices_lower.append(entry['price_p10'])
    prices_upper.append(entry['price_p90'])

# Optimization problem (pseudocode)
# Minimize: sum(price[t] * power[t] * dt) for t in horizon
# Subject to:
#   - Thermal constraints: |temp[t+1] - temp[t]| < max_gradient
#   - Min runtime: if ON, stay ON for min_duration
#   - Production target: sum(power[t] * efficiency[t]) >= H2_target
#   - Robust: consider worst-case (prices_upper) or expected (prices_mean)

# Use cvxpy, PuLP, or scipy.optimize
```

**Scheduling algorithms (future work):**
- Mixed-integer linear programming (MILP)
- Dynamic programming
- Model predictive control (MPC)
- Reinforcement learning (advanced)

## Success Metrics

**Technical Performance:**
- MAE < 10 EUR/MWh for 24h ahead predictions
- MAE < 20 EUR/MWh for 168h ahead predictions
- Directional accuracy > 70% (did price go up or down?)

**Business Value (simulated backtesting):**
- Cost reduction vs. constant operation: > 15%
- Cost reduction vs. day-ahead only scheduling: > 5%
- Payback period for implementation effort: < 6 months

**Operational:**
- Prediction generation time: < 5 minutes
- Model retraining time: < 30 minutes
- Zero manual intervention (fully automated)

## Future Enhancements

**Phase 2 Features:**
1. **Additional data sources:**
   - ENTSO-E load forecasts (demand predictions)
   - Renewable generation forecasts (wind/solar capacity)
   - Gas prices (TTF) for price drivers
   - Carbon prices (EUA) for market trends

2. **Advanced models:**
   - Transformer architectures (if data sufficient)
   - Graph neural networks (model grid topology)
   - Transfer learning from other markets

3. **Online learning:**
   - Incremental model updates (don't retrain from scratch)
   - Adapt to market regime changes faster

4. **Multi-location:**
   - Predict prices for multiple bidding zones
   - Arbitrage opportunities across borders

5. **Probabilistic forecasting:**
   - Full predictive distributions (not just quantiles)
   - Scenario generation for stochastic optimization

## References

**Academic:**
- Weron, R. (2014). "Electricity price forecasting: A review of the state-of-the-art"
- Lago, J. et al. (2021). "Forecasting day-ahead electricity prices: A review of state-of-the-art algorithms"

**Tools:**
- sktime documentation: https://www.sktime.net/
- Prophet documentation: https://facebook.github.io/prophet/
- energyDataHub API: https://ducroq.github.io/energydatahub/

**Related Projects:**
- ENTSO-E Transparency Platform: https://transparency.entsoe.eu/
- Open Power System Data: https://open-power-system-data.org/

## License

[TBD - Align with energyDataHub license]

## Contributing

[TBD - Contribution guidelines when open-sourced]

## Contact

[TBD - Lab contact info, maintainer email]

---

**Document Status:** Initial draft for repository planning
**Last Updated:** 2025-01-04
**Next Review:** After Phase 1 completion
