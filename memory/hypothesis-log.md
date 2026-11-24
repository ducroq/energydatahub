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
**Last run**: 2026-11-18. Last 30 scheduled runs (10-19 → 11-17): 1 failure, drift class (1 of 30 = 3%): `load_forecast` NL lost all A67 actuals for the window, which is #79, cause known (run `44197847187`). 10-28 → 11-17: 20 of 21 published.
**Review by**: 2026-12-18, same Method; immediately if a third #79-shaped loss lands before #79 is decided.

### H7 — The diagnostic-key exemption must apply across the tripwire, not only member drift (#45)
**Status**: shipped 2026-09-21 (`prune_diagnostic_keys`, `_partition_diagnostic_only`, diagnostic-blind observation records `pruned_hashes` + `feeds_pruned`). Blind evidence applies per feed only once every record in the 60-run window carries it; until then the old raw-hash behaviour stands.
**Review by**: 2026-11-20 — confirm `grid_imbalance` survived a TenneT wobble without being classified volatile. Precondition for H6's registration and #79.

### H10 — Open-Meteo 429 storms are shared-runner-IP contention, not our concurrency (#58)
**Position**: our cap (6) is not the cause; 10-04's storm refused our very first request. **Counter**: storms come with "Too many concurrent requests" and sometimes 5xx; a 6-wide unpaced burst did 429 locally once.
**Method**: `scripts/probe_openmeteo_concurrency.py` via `.github/workflows/openmeteo-probe.yml` plus a local run close in time; both-clean is INCONCLUSIVE. Never dispatch while a collection run is in flight.
**Cost, reviewed 2026-10-28**: after the 10-05 429 budget (`a1a944e`), 22 of 22 scheduled runs from 10-06 to 10-27 published (10-05's own run failed on the unrelated late-cron day roll, fixed `bb39beb`). 9 logged a `rate-limited (retry` line; one real storm (10-13 `38466658651`: 68 rejections, 31 retries) lost 1 of 11 `demand_weather_forecast` locations (`Utrecht_NL`) and still published. Cause unsettled; cost now low. If storms block publishes again, lowering the cap is the next lever.
**Review by**: 2026-11-28. **Trigger fired 2026-11-20** (`44829093119`): every location got 429 `"Daily API request limit exceeded"` — a DAILY QUOTA message, not "too many concurrent". 3 weather feeds empty → critical-feed removal → drift gate blocked the publish. Our ~6-wide burst cannot spend a daily quota at 22:28Z; this favours the shared-IP position. Next lever to decide: retry-elsewhere is impossible on hosted runners, so either a paid/keyed Open-Meteo endpoint or grace the weather feeds on a quota-429.

### H11 — Slot-date anchoring + an earlier, off-hour cron ends half-size day-ahead publishes (2026-11-24)
**Position**: the 96-point entsoe publishes come from a run collecting the wrong day; anchoring COLLECTION_DATE to the cron slot (now − cron hh:mm) fixes that for any lag < 24h, and moving `0 16` → `23 13` lowers the lag. **Counter**: GitHub lag is unbounded (11-23: 10.7h, growing since Oct); some 96-point days (11-20, 11-22) may have other causes (#74 conflated two).
**Method**: for each scheduled run from 11-24: start lag (`createdAt` − 13:23Z), the `COLLECTION_DATE=` log line, entsoe point count, and any span-shortfall issue on `wind_forecast`/`generation_forecast` (ENTSO-E A69/A71 day-ahead forecasts are due only by 18:00 Brussels — an ON-TIME 13:23Z run can precede them; if so, move the cron to ~`23 16`). Settled-yes: 7 runs, all 192 points, none crossing a wrong date. Settled-no: a 96 with the correct slot date → separate cause; lag > 24h → off-host dispatch (#67) is required, not optional.
**Review by**: 2026-12-01. Augur (session `augur-0a`) is told the outcome.

## Dormant (full entries in `memory/archive/hypothesis-log-resolved.md`)

- **H6** — `MEMBER_MAPPED_FEEDS` + member homogeneity suffices for member drift (#44). **Reopens when a new per-location feed is added.**
- **H8** — merging all timestamp-map records ends the sampled-record false-positive class. **Reopens when #49 is built.**

Dated reviews behind H2, H7 and H10: `memory/archive/hypothesis-history-2026-10-28.md`.

## Resolved

Resolved entries (H1, H3, H5, H9) live in `memory/archive/hypothesis-log-resolved.md`. Move an entry there when you resolve it.
