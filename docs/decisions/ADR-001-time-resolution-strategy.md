---
status: Accepted
date: 2026-03-09
deciders: [Jeroen Veen]
superseded_by:
---

# ADR-001: Time Resolution Strategy

## Context

The Dutch electricity market transitioned from hourly to 15-minute settlement periods on 1 October 2025. ENTSO-E now returns 96 data points per day for NL instead of 24. Our data pipeline stores timestamps as dict keys, making it inherently resolution-agnostic.

The student project (BDSD Minor, Jan 2026) recommended moving the entire pipeline to 15-minute resolution. This ADR documents why we chose not to.

## Options Considered

### Option A: Align all sources to 15-min at collection time

| Pros | Cons |
|------|------|
| Single common time grid for consumers | Weather forecasts are hourly — interpolating creates false precision |
| Matches NL market settlement | NL day-ahead auction still clears hourly; intra-hour prices typically identical |
| Eases downstream ML feature engineering | Each source needs different resampling logic (interpolation/forward-fill/aggregation), risk of artifacts |
| | H2 Lab schedules on hour-to-day horizons — intra-hour resolution adds complexity without scheduling benefit |
| | BDSD student model achieved MAE 13.6 EUR/MWh at hourly, adequate for hydrogen scheduling |

### Option B: Aggregate all sources to hourly

| Pros | Cons |
|------|------|
| Simple, uniform | Information loss — irreversible |
| | TenneT grid imbalance is inherently 15-min (PTU) and meaningful for real-time grid state |
| | Future models needing 15-min data would lose access |

### Option C: Store at native resolution per source (chosen)

| Pros | Cons |
|------|------|
| No information loss — full source fidelity preserved | Consumers must handle heterogeneous resolutions |
| Timestamps-as-keys is already resolution-agnostic — no storage change | No single grid for cross-source analysis until consumer resamples |
| Zero collection-time complexity | Downstream resampling logic lives in each consumer (not shared) |
| Defers the alignment decision until a model actually needs it | |

## Decision

**We chose Option C: store each source at its native resolution.**

Downstream consumers (ML models, dashboards) resample to their required resolution. The collection layer stays simple and lossless.

## Consequences

### Positive
- No engineering effort at collection time
- Lossless storage — any future model can choose its own resolution
- TenneT PTU-level granularity preserved for real-time grid state

### Negative
- Each downstream consumer reimplements resampling
- No common time grid for cross-source comparisons without consumer-side work

### Risks
- If many downstream consumers emerge, duplicated resampling logic could drift between them — at that point, build a shared resampling utility (not at collection time)

## Revisit If

- An ML model is built that demonstrably benefits from 15-min input resolution (e.g., improves MAE by a meaningful margin)
- H2 Lab moves to intra-hour production scheduling
- A second downstream consumer beyond Augur requires a common time grid — promote shared resampling to a utility
- ENTSO-E or another major source changes its native resolution again

## Implementation

1. ENTSO-E collector metadata `resolution` updated from `'hourly'` to `'as-returned'`
2. NED collector granularity changed from `'hourly'` to `'15min'` to match market resolution
3. Data quality framework already validates at minute granularity (handles both 24 and 96 points/day) — no change

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

## References

- Student project report (BDSD Minor, Jan 2026) — see `docs/STUDENT_PROJECT_REPORT_ANALYSIS.md`
- ENTSO-E 15-min transition announcement (Oct 2025)
