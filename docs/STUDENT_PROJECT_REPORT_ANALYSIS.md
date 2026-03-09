# Student Project Report Analysis: Electricity Price Prediction

## Report Details

- **Title:** BDSD Minor Project - Electricity Price Prediction
- **Students:** Alexander Kuiper (2154899), Jordi de Bruin (1583574), Ronald van der Meer (2158166), Umesh Patel (2157906)
- **Institution:** Hogeschool van Arnhem en Nijmegen (HAN)
- **Supervisor:** Aishwarya Aswal
- **Date:** 19 January 2026
- **Data used:** energyDataHub historical JSON files (1533 files) + self-acquired ENTSO-E and Open-Meteo data

---

## Objective

Build a supervised ML model to predict electricity prices up to 7 days ahead using free, publicly accessible data sources. The motivation is cost-efficient scheduling of hydrogen production in the HAN H2 Lab.

## Research Question

*How can we use historical energy price data, combined with weather forecast and with day-ahead prices as given by the exchanges, to predict energy prices further ahead?*

---

## Data Analysis of Our Dataset

The students received 1533 JSON files from the energyDataHub:

| File Type | Count |
|-----------|-------|
| energy_price_forecast.json | 394 |
| weather_forecast.json | 393 |
| sun_forecast.json | 388 |
| air_quality.json | 14 |
| air_history.json | 344 |

### Key Findings About Our Data

1. **Energy prices** came from 4 sources (Elspot, EnergyZero, ENTSO-E, EPEX) with different time ranges and significant gaps. Only ENTSO-E and EnergyZero had reasonable continuity.
2. **Weather data** could only be parsed from 05-Sep-2024 to 27-Sep-2024 due to a format change after that date. Multiple gaps existed even within this short window.
3. **Sun data** had fewer gaps but still had some inconsistencies.
4. **Air quality data** had massive gaps, deemed unusable.
5. **Overall conclusion:** The dataset was deemed **unsuitable for ML** due to gaps, inconsistencies between sources, and format changes over time.

### Data Gap Impact

- With 48h input + 168h output windows, only 32 valid non-overlapping training sets could be constructed from the energy price data alone.
- Adding weather features (which also had gaps) would reduce this further.
- The students decided to abandon our dataset entirely and build their own.

---

## Their New Data Acquisition Strategy

### Energy Data: ENTSO-E Transparency Platform
- Free API (400 requests/token/IP/minute)
- Historical and forecast data available
- Legally mandated reporting platform for TSOs like TenneT
- Handled the Oct 2025 switchover from hourly to 15-minute intervals by averaging

### Weather Data: Open-Meteo
- Free, open-source archive with 80+ years of data
- Historical and forecasted weather
- Pulled from 6 NL locations: Amsterdam, Groningen, Maastricht, Borssele, Offshore Hollandse Kust Zuid, Offshore Gemini (North)

### Final Dataset
- **Period:** 1 Jan 2024 - 30 Oct 2025 (~21 months)
- **Size:** 44 features x 15,336 rows (hourly)
- **Features included:**
  1. Day-ahead prices (ENTSO-E)
  2. Day-ahead prices T-7d lagged
  3. Actual load T-7d
  4. Day-ahead load forecast T-7d
  5. Conventional generation T-7d
  6. Cross-border interconnect flows (NL-BE, NL-DE, NL-DK, NL-NO, NL-UK) - actual and scheduled
  7. Day-ahead solar generation forecast T-7d
  8. Day-ahead offshore wind generation forecast T-7d
  9. Day-ahead onshore wind generation forecast T-7d
  10. Day-ahead all-sources generation forecast T-7d
  11. Weekly temperature forecast (6 locations)
  12. Weekly wind speed forecast (6 locations)
  13. Weekly wind direction forecast (6 locations)
  14. Weekly GHI forecast (6 locations)
  15. Engineered: hour_of_day (sin/cos), day_of_week (sin/cos), time_of_year (sin/cos)

---

## Feature Correlation Results

Features with highest positive correlation to day-ahead price:
1. **Price lagged 7d** (strong +)
2. **Day-ahead forecasted load** (moderate +)
3. **Actual load** (moderate +)
4. **Conventional generation** (moderate +)

Features with negative correlation (renewable generation lowers price):
- Solar/wind generation forecasts
- Offshore wind speed forecasts
- Solar radiation forecasts

---

## Models Developed

### 1. Naive Baseline Model
- Uses lagged prices from exactly 7 days prior
- Predicts at 12:00 daily, outputs 132 hours of predictions
- **Results:** MAE 34.04, RMSE 55.24, R² 0.024

### 2. Feedforward Neural Network (MATLAB)
- Single hidden layer (24 neurons, regularization 0.9)
- Levenberg-Marquardt training algorithm
- Normalization inside walk-forward loop to prevent leakage
- 5 random theta initializations per fold
- **Results:** MAE 19.44, RMSE 25.75, R² 0.471, Correlation 0.79

### 3. Random Forest (MATLAB TreeBagger)
- OOB-based hyperparameter tuning
- No feature normalization (tree-based invariance)
- **Results:** MAE 21.8, RMSE 35.56, R² -0.009

### 4. Combined Model (NN + RF average)
- Simple unweighted average of NN and RF outputs
- **Results:** MAE 13.6, RMSE 23.03, R² 0.577, Correlation 0.786

### Performance Comparison (Test Set: November 2025)

| Metric | Naive | Neural Net | Random Forest | Combined |
|--------|-------|-----------|--------------|----------|
| MAE (EUR/MWh) | 27.27 | 19.44 | 21.8 | **13.6** |
| MSE | 1448.5 | 663.2 | 1264.3 | **530.3** |
| RMSE | 38.06 | 25.75 | 35.56 | **23.03** |
| MAPE | 0.368 | 0.314 | 0.359 | **0.269** |
| R² | -0.156 | 0.471 | -0.009 | **0.577** |
| sMAPE | 0.375 | 0.212 | 0.24 | **0.159** |
| Correlation | 0.452 | **0.79** | 0.353 | 0.786 |

---

## Feature Importance Results

### Neural Network Top Features
1. Actual_Load_Lagged7d
2. hour_of_day_cos (engineered)
3. hour_of_day_sin (engineered)
4. Price_Lagged7d
5. NL_Onshore_Groningen_shortwave_radiation_WeeklyForecast
6. NL_Offshore_Gemini_wind_speed_10m_WeeklyForecast

### Random Forest Top Features
1. Price_Lagged7d
2. Actual_Load_Lagged7d
3. NL_Offshore_HollandseKustZuid_wind_speed_10m_WeeklyForecast
4. NL_Onshore_Maastricht_shortwave_radiation_WeeklyForecast
5. NL_Onshore_Groningen_shortwave_radiation_WeeklyForecast

### Key Insight
Both models agree that **actual load** and **lagged price** are the most important drivers. The NN relies heavily on engineered time features (sin/cos), while the RF distributes importance more across weather features (wind speed, solar radiation).

---

## Validation Strategy

- **Expanding window walk-forward validation** (not random k-fold)
- First fold: 4 months of training, 1 week validation
- Expands by 1 week per fold
- Test set (November 2025) separated by 1 month from training end (September 2025)
- Normalization performed inside the fold loop to prevent leakage

---

## MoSCoW Requirements Assessment

### Met
- Trained on historic data (Jan 2024 - Oct 2025)
- Validated with walk-forward on historic subset
- Free API data sources identified (ENTSO-E, Open-Meteo)
- Feature importance investigation completed
- Predicts 7 days ahead (exceeds the >1 day requirement)
- Seasonal awareness via trigonometric encoding
- Naive baseline comparison included
- Hourly granularity
- Visualization of results

### NOT Met
- **Probabilistic confidence values** - Only standard error metrics, no per-prediction confidence
- **DST handling** - Used UTC, no DST feature engineered
- **Fuel price features** - Gas/oil prices not included despite literature identifying them as top predictors
- **User-friendly interface / dashboard** - Code runs via MATLAB scripts only
- **Rolling forecasts** - Not implemented

---

## Strengths

1. Thorough data quality analysis with clear visualizations of gaps
2. Correct decision to acquire clean data from ENTSO-E + Open-Meteo
3. Proper walk-forward validation preventing data leakage
4. Good feature engineering (sin/cos cyclical encoding)
5. Thoughtful feature selection including offshore wind park locations
6. Correctly handled the 15-minute resolution switchover (Oct 2025)
7. Well-structured report with clear methodology

## Weaknesses

1. Single hidden layer NN is too shallow - literature they cited supports deep architectures
2. Random Forest performed worse than naive baseline (negative R²) - poor extrapolation to unseen conditions
3. Test set is only 1 month - too small for robust conclusions
4. Simple unweighted ensemble averaging - stacking/weighted average would improve
5. MATLAB implementation limits reproducibility and deployment potential
6. Missing gas/fuel price features despite their own literature identifying these as important
7. Hyperparameter grid search range was narrow (3-24 neurons, lambda 0.01-0.9)
8. No analysis of what made the November test set challenging for the RF

---

## Recommendations From the Report

1. **Dataset expansion** - Continuously add data, re-evaluate features yearly
2. **Holiday and DST features** - Add binary holiday indicator and DST-aware features
3. **Feature selection** - Apply filtering to remove low-impact features
4. **Training algorithm** - Monitor LM vs SCG tipping point as data grows
5. **Better ensembling** - Weighted averaging, stacking, or meta-learning
6. **Rolling window retraining** - Monthly retraining with limited history for production
7. **15-minute resolution** - Align predictions with current market granularity
8. **Performance monitoring** - Track errors across seasons, weekdays vs weekends
9. **Multiple prediction horizons** - Train separate models for different "ahead times" within the 7-day window

---

## Relevance to energyDataHub

### Validated Data Sources
- **ENTSO-E** confirmed as primary free source for NL electricity market data (already in use by energyDataHub)
- **Open-Meteo** confirmed as viable free weather source (energyDataHub currently uses multiple weather sources)

### Confirmed Important Features
The following data we collect is confirmed as highly predictive:
- Day-ahead prices (lagged)
- Actual and forecasted load
- Conventional generation
- Cross-border flows (especially NL-DE, NL-BE)
- Offshore wind speed (Gemini, Hollandse Kust Zuid)
- Solar radiation forecasts
- Wind direction

### Gaps in Our Data Collection (from student analysis)
- Weather data format changed mid-collection, breaking parser compatibility
- Multiple price sources had inconsistent coverage
- Air quality data too sparse to be useful

### Actionable Items
1. Ensure consistent JSON schemas across data collection versions
2. The student work validates our ENTSO-E and weather data collection strategy
3. Consider adding explicit holiday/DST features to our feature set
4. Their combined NN+RF approach (MAE ~13.6 EUR/MWh at 7-day horizon) provides a benchmark for any future prediction work

---

*Analysis performed: 4 March 2026*
