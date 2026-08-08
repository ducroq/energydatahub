# Hypothesis Log

<!-- Beliefs this project acts on that are NOT yet established fact. Each entry states a
     position, the method that would settle it, and a date or trigger to come back to it.

     This exists because unverified beliefs otherwise decay into assumed truth: the entry
     that says "probably transient" gets read six weeks later as "known transient". An
     entry belongs here when acting on it has a cost and the evidence is incomplete.

     Not for: settled decisions (→ MEMORY.md Active Decisions or an ADR), solved problems
     (→ gotcha-log.md), or in-flight work plans (→ memory/work-items/).

     /curate surfaces entries whose `Review by:` has passed or whose `Revisit trigger:`
     has fired. It does NOT resolve them — reading the Method and applying it is the
     engineer's call. Move resolved entries to `## Resolved` with the outcome. -->

## Open

### H1 — Extending the present-empty coercion to all late-wave OpenMeteo feeds is safe
**Position**: The `data_fetcher` present-but-empty → `None` coercion applied to the two buurt feeds (`ad008df`) can be extended to `demand_weather_forecast`, strategic weather/solar, and `offshore_wind` without losing a real failure signal, because a 0-point feed is strictly less informative than an absent one either way.
**Counter-position**: Those feeds are closer to Augur's consumption path than buurt is. Silently downgrading them to `'info'` could mask a sustained outage the way #38 was specifically built to prevent.
**Method**: Decide between the three options written up in `memory/work-items/present-empty-guard-rollout.md`. The #38 streak-counter mechanism (`data/_upstream_empty_streak.json`) already exists and is the obvious middle path — coerce, but escalate after N consecutive runs.
**Status**: Issue #42 open, work item written, decision pending. Not started.
**Review by**: 2026-09-01, or sooner if a late-wave timeout aborts a publish.

### H2 — The daily-run failure rate is dominated by transient upstream/runner faults, not defects
**Position**: Recent failures are environmental, not regressions. Evidence: run `31123856009` (08-06) failed with "job was not acquired by Runner" (pure GitHub infrastructure); run `30838120578` (08-03) failed the drift tripwire on transient-driven shape churn, and the three surrounding runs were green with no code change.
**Counter-position**: Two failures in five days is a ~40% failure rate. Calling that "transient" is exactly the reasoning that let GoogleWeather 401 for seven months. The pattern may be a real degradation in the OpenMeteo late-wave that the retries merely paper over.
**Method**: Tabulate the last 30 scheduled runs by failure class (runner-acquisition / drift tripwire / quality gate / collector). If any *non-infrastructure* class exceeds ~1 in 10, treat it as a defect and open an issue rather than re-running.
**Revisit trigger**: Two consecutive failures, or any failure whose cause is not one of the two classes above.

### H3 — Committing the shape sidecar before the drift tripwire would let volatility self-classification work as designed
**Position**: `derive_volatile_feeds()` cannot learn from a run that fails, because the tripwire (`collect-data.yml:119`) precedes the commit step (`:149`) and a failing run commits nothing. Committing the sidecar *before* the gate would close the loop, so a recurring transient self-classifies after its second occurrence instead of never.
**Counter-position**: Committing pre-gate means a drifted (possibly genuinely broken) shape enters the baseline, so the *next* run diffs against a bad reference and the break becomes the new normal — precisely the silent-drift failure the fail-mode flip was introduced to end on 2026-06-10. A separate learning-only record, not the gate's baseline, may be the right shape.
**Method**: Prototype against the committed history of `ned_production` / `wind_forecast`, whose 08-03 drift is the known-good test case. Check whether a "would have been classified volatile" replay reaches the right verdict without the baseline poisoning above.
**Status**: Filed as a GitHub issue 2026-08-08. Not started — this needs the engineer's judgement on the baseline trade-off before any code.
**Review by**: 2026-09-08, or immediately if the tripwire fails again on a transient.

### H4 — `STALENESS_OVERRIDES` with a weekend-spanning floor fully fixes the weekend `error` (#36)
**Position**: Adding `market_proxies` / `market_history` at ~96h (matching `gas_storage`) removes the spurious weekend `error` without hiding a real market-data outage, because a genuine outage exceeds 96h by Monday.
**Counter-position**: A fixed floor is the same shape as the 48h threshold it replaces — cadence-blind. A long weekend or exchange holiday could still trip it, and 96h is late enough to delay noticing a real outage by a day.
**Method**: Weekday-aware staleness (skip non-trading days) is the principled fix; the flat override is the cheap one. Compare against a month of committed `market_*` files before choosing.
**Status**: Issue #36 open since 2026-06-14. Recurs every weekend, non-blocking.
**Review by**: 2026-10-01 — low urgency while it stays non-blocking, but it erodes the meaning of `overall_status=error` every single week.

### H5 — Git-as-archive remains viable until the repo approaches ~1 GB (#9)
**Position**: Deferring the storage migration is correct; `data/` growth is linear and predictable, and the monthly archive to `05. Data/` bounds the working set.
**Counter-position**: Clone time and Actions checkout cost degrade well before the 1 GB headline number, and the migration gets harder the longer it waits.
**Method**: Record `git count-objects -vH` size at each monthly archive. If growth is superlinear, or checkout time in the daily run exceeds ~60s, re-plan.
**Revisit trigger**: repo > 700 MB, or daily-run checkout > 60s.
**⚠ TRIGGERED 2026-08-08 — both clauses, on the first check after this entry was written:**
- `git count-objects -vH` → **size-pack 797.09 MiB** (threshold 700 MB), `.git` 799 MB on disk.
- Checkout step in run `31199044747` (08-07, a *successful* run) → **101s** (threshold 60s). That is ~half the total wall-clock of a healthy 3m26s collect, spent before any data is fetched.
- `data/` now holds **4,974** JSON files; MEMORY.md's #9 note said 3,909 as of 2026-06-14 — ~1,065 added in eight weeks.

The position above ("deferring is correct, growth is linear and predictable") is the part now in doubt: 1 GB is roughly one quarter away at this rate, and the *cost* the threshold was proxying for — checkout time — has already arrived. Not resolving this here; it needs the engineer's call on #9. What changed is that it is no longer a someday problem.

### H6 — The `cryptography<44` pin will block a venv rebuild on current Python
**Position**: `requirements.txt` pins `cryptography>=41.0.0,<44.0.0`, an upper bound that predates Python 3.13/3.14. The venv is uv-managed on 3.12.13 while the system interpreter is 3.14.4, so anyone recreating the venv from system Python lands on an untested combination, and `cryptography` — the AES-CBC/HMAC dependency named in Hard Constraints — is the most likely thing to fail to resolve or build.
**Counter-position**: The pin is deliberate and nothing forces a rebuild; uv reproduces 3.12.13 from `pyvenv.cfg`, and CI pins 3.12 explicitly in both workflows. This may be a non-problem that only bites on a machine migration.
**Method**: `uv venv --python 3.14 && uv pip install -r requirements.txt` in a throwaway directory. If it resolves, raise the bound and add 3.13/3.14 to `test.yml`'s matrix (currently `['3.12']`, a single entry, so nothing tests above 3.12). If it does not, record the floor explicitly — there is no `requires-python` declared anywhere today, so "we support 3.12" is convention rather than something enforced.
**Revisit trigger**: any venv rebuild, a machine migration, or Dependabot proposing a `cryptography` major bump.

## Resolved

<!-- Move entries here with the outcome and the date. Keep them: a hypothesis that turned
     out wrong is the most useful kind of record, and deleting it invites re-litigation. -->

_None yet — this log was created 2026-08-08._
