# Session 2026-07-07 — Buurt present-but-empty publish abort

## What triggered it
Maintainer received (another) daily GitHub Actions failure email: the 2026-07-06
16:00 UTC scheduled collect (run `28812717911`, commit `028de46`) failed with
`overall_status=critical`. Recurring pain — "everyday i get such an email".

## Diagnosis
Only two **secondary** FyE B1 feeds failed — `weather_forecast_buurt` and
`solar_forecast_buurt` — both `completeness: No data points collected (expected
>= 24)`. Every augur-consumed feed collected fine. The whole publish aborted over
two feeds Augur doesn't even consume.

Root cause is a three-layer compound (see gotcha-log.md 2026-07-07 entry for the
full write-up):
1. The buurt Open-Meteo collectors run **last** in the shared OpenMeteo request
   wave and on an unlucky run exhaust all retries on a `Connection timeout to host
   api.open-meteo.com` — the recurring late-wave class the 2026-06-07 semaphore
   tuning mitigated but can't eliminate.
2. `base.collect()` returns a **truthy `EnhancedDataSet` with `data={}`** on
   all-locations failure (it builds the dataset regardless of `_validate_data`,
   `base.py:736`) — so the feed is *present but empty*, not absent.
3. `validate_completeness` hardcodes `Severity.CRITICAL` for 0 points
   (`data_quality.py:807`) for any dataset → pipeline promoted to `critical`.

The inconsistency: a genuinely **absent** buurt feed is a non-event, but a
**present-but-empty** one hard-failed everything. #38's `upstream_empty`
downgrade only touches the missing-dataset path, never the per-dataset
completeness report, so it couldn't help.

## Fix (commit `ad008df`)
- `data_fetcher.py`: after unpacking `results['buurt_weather']`/`['buurt_solar']`,
  coerce a present-but-empty dataset (`ds is not None and not ds.data`) to `None`
  with a warning log. It then routes through the (non-blocking, `'info'`)
  missing-dataset path instead of the completeness gate — present-empty treated
  as absent, mirroring #38's "keep publishing the healthy feeds".
- `utils/data_quality.py`: registered `weather_forecast_buurt` /
  `solar_forecast_buurt` as `'info'` in `DATASET_MISSING_SEVERITY` (like
  `air_quality_buurt`) so absence is logged without promoting status.
- `tests/unit/test_data_quality.py`: 2 contract tests. Critical set stays
  `{entsoe, energy_zero}`; 681 tests pass.

## Decision made
Maintainer chose the **silent `'info'`** policy (like `air_quality_buurt`) over
wiring buurt into the #38 `UPSTREAM_EMPTY_ESCALATION_RUNS=3` streak machinery.
Consequence: a *sustained* multi-day buurt outage won't self-escalate to a hard
failure — it's only visible in the run log. Acceptable for a feed Augur doesn't
consume.

## Verification
Smoke run `28846743856` green end-to-end — collect, quality gate, publish, Pages
`deploy` job (attempt 1). BUT the buurt collectors **succeeded** that run (both
returned 2 neighbourhoods of real data, all 5 quality checks passed), so the
empty-path fix was NOT exercised live — only by unit test
(`test_missing_buurt_forecast_not_promoted`). The empty path fires only on a
random upstream timeout.

## Left undone (latent)
The same truthy-empty behaviour affects the other late-wave OpenMeteo feeds —
`demand_weather_forecast`, strategic weather/solar (`weather_forecast_multi_location`,
`solar_forecast`), `offshore_wind`. Scoped out because only buurt has been
failing, but the next timeout on one of *those* would abort publish identically.
Candidate follow-up: either extend the empty→None guard to them, or fix at the
root (base.collect() returning truthy-empty, or validate_completeness's hardcoded
CRITICAL taking per-feed severity from `DATASET_MISSING_SEVERITY`).

## Notes
- `air_quality_buurt` was absent from the smoke-run sidecar (19 feeds, not 20) —
  its normal RIVM/Luchtmeetnet flakiness, unrelated to this change.
