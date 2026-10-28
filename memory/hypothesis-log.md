# Hypothesis Log

<!-- Beliefs this project acts on that are NOT yet established fact: position,
     counter-position, the method that would settle it, and a review date or trigger.
     Not for settled decisions (-> project_active_decisions.md), solved problems
     (-> gotcha-log.md) or work plans (-> work-items/).
     Write the entry WHEN the claim is made, not at end of session.
     /curate surfaces overdue entries; resolving them is the engineer's call.
     RETIRE: resolved or dormant entries move to `memory/archive/hypothesis-log-resolved.md`;
     a dormant one leaves a one-line pointer below with its reopen trigger. -->

## Open

### H2 — Daily-run failures are transient upstream/runner faults, not defects
**Position**: failures are environmental. **Counter**: a failure rate called "transient" is how GoogleWeather 401'd for seven months.
**Method**: tabulate the last 30 scheduled runs by FAILING STEP (*Collect data*, *Assert data quality*, *Schema-drift tripwire*, runner). A non-infrastructure class above ~1 in 10 is a defect: open an issue, do not re-run.
**Standing conclusion**: the position does not hold. Every failure since 08-14 had a distinct, findable cause; the defect is that each all-or-nothing gate's only lever is withholding all 20 feeds. Remedy chosen 2026-09-04: detect, don't relent (#50 alerting, #53 span check). **Do not rebuild a relent** (measured and declined: `memory/archive/project_session_2026_09_04.md`).
**Last run**: 2026-10-06, drift class 3 of 30 = 10%, all causes known (#79 open). Since then: 22 of 22 scheduled runs published (10-06 → 10-27).
**Review by**: 2026-11-05, same Method.

### H7 — The diagnostic-key exemption must apply across the tripwire, not only member drift (#45)
**Status**: shipped 2026-09-21 (`prune_diagnostic_keys`, `_partition_diagnostic_only`, diagnostic-blind observation records `pruned_hashes` + `feeds_pruned`). Blind evidence applies per feed only once every record in the 60-run window carries it; until then the old raw-hash behaviour stands.
**Review by**: 2026-11-20 — confirm `grid_imbalance` survived a TenneT wobble without being classified volatile. Precondition for H6's registration and #79.

### H10 — Open-Meteo 429 storms are shared-runner-IP contention, not our concurrency (#58)
**Position**: our cap (6) is not the cause; 10-04's storm refused our very first request. **Counter**: storms come with "Too many concurrent requests" and sometimes 5xx; a 6-wide unpaced burst did 429 locally once.
**Method**: `scripts/probe_openmeteo_concurrency.py` via `.github/workflows/openmeteo-probe.yml` plus a local run close in time; both-clean is INCONCLUSIVE. Never dispatch within ~30 min of 16:00 UTC.
**Cost, reviewed 2026-10-28**: after the 10-05 429 budget (`a1a944e`), 22 of 22 scheduled runs from 10-06 to 10-27 published (10-05's own run failed on the unrelated late-cron day roll, fixed `bb39beb`). 9 logged a `rate-limited (retry` line; one real storm (10-13 `38466658651`: 68 rejections, 31 retries) lost 1 of 11 `demand_weather_forecast` locations (`Utrecht_NL`) and still published. Cause unsettled; cost now low. If storms block publishes again, lowering the cap is the next lever.
**Review by**: 2026-11-28, or immediately when a 429 storm blocks a publish.

## Dormant (full entries in `memory/archive/hypothesis-log-resolved.md`)

- **H6** — `MEMBER_MAPPED_FEEDS` + member homogeneity suffices for member drift (#44). **Reopens when a new per-location feed is added.**
- **H8** — merging all timestamp-map records ends the sampled-record false-positive class. **Reopens when #49 is built.**

Dated reviews behind H2, H7 and H10: `memory/archive/hypothesis-history-2026-10-28.md`.

## Resolved

Resolved entries (H1, H3, H5, H9) live in `memory/archive/hypothesis-log-resolved.md`. Move an entry there when you resolve it.
