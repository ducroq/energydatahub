# ADR-001: Time Resolution Strategy

**Status:** Accepted
**Date:** 2026-03-09
**Context:** Student project report (BDSD Minor, Jan 2026) and ENTSO-E market changes

## Decision

Store data at the native resolution of each source. Do not resample or align to a common grid at collection time. Downstream consumers (ML models, dashboards) are responsible for resampling to their required resolution.

## Context

The Dutch electricity market transitioned from hourly to 15-minute settlement periods on 1 October 2025. ENTSO-E now returns 96 data points per day for NL instead of 24. Our data pipeline stores timestamps as dict keys, making it inherently resolution-agnostic.

The student project (BDSD Minor) recommended moving to 15-minute resolution. We evaluated this and concluded that full alignment across all sources is not justified at this time.

## Current Resolution by Source

| Source | Native Resolution | Stored As |
|--------|------------------|-----------|
| ENTSO-E (NL prices) | 15-min (since Oct 2025) | As-returned |
| ENTSO-E (DE prices) | Hourly | As-returned |
| ENTSO-E (wind/load/generation) | Hourly or 15-min | As-returned |
| EnergyZero | Hourly | As-returned |
| EPEX | Hourly | As-returned |
| Elspot (Nord Pool) | 15-min (aggregated to hourly) | Hourly |
| TenneT (grid imbalance) | 15-min (PTU) | As-returned |
| NED.nl (production) | 15-min | As-returned |
| Google Weather | Hourly | As-returned |
| Open-Meteo (solar/wind/demand) | Hourly | As-returned |
| Calendar features | Hourly | Generated hourly |
| Market proxies (carbon/gas) | Daily | As-returned |
| GIE gas storage | Daily | As-returned |
| ENTSOG gas flows | Daily | As-returned |

## Rationale

### Why NOT align everything to 15-min now

1. **Limited benefit for current use cases.** The H2 Lab schedules hydrogen production on hour-to-day horizons. Intra-hour resolution adds complexity without meaningful scheduling improvement.

2. **Weather data is hourly.** Open-Meteo and Google Weather provide hourly forecasts. Interpolating to 15-min creates false precision — the forecast value for 14:15 is the same as 14:00.

3. **Day-ahead prices are still effectively hourly.** While ENTSO-E publishes at 15-min, NL day-ahead auction clears hourly. The 15-min prices within an hour are typically identical or very similar.

4. **Significant engineering effort.** Aligning all sources requires resampling logic (interpolation for weather, forward-fill for prices, aggregation for production). Each source needs different treatment. Risk of introducing artifacts.

5. **Student models worked at hourly.** The BDSD project achieved MAE 13.6 EUR/MWh at hourly resolution — adequate for hydrogen production scheduling.

### Why store native resolution (not aggregate to hourly)

1. **No information loss.** If a future model needs 15-min data, it's already there.
2. **TenneT grid imbalance is inherently 15-min.** PTU-level data is meaningful for real-time grid state.
3. **Timestamps-as-keys is resolution-agnostic.** No code changes needed — the storage format works for any interval.

## Changes Made

- ENTSO-E collector: metadata `resolution` updated from `'hourly'` to `'as-returned'`
- NED collector: granularity changed from `'hourly'` to `'15min'` to match market resolution
- Data quality framework: already validates at minute granularity (handles both 24 and 96 points/day)

## When to Revisit

This decision should be revisited if:
- An ML model is built that demonstrably benefits from 15-min input resolution
- The H2 Lab moves to intra-hour production scheduling
- A downstream consumer requires a common time grid (at that point, build a resampling utility)
