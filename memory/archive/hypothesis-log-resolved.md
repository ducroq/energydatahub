# Hypothesis log — resolved entries

Moved verbatim from `memory/hypothesis-log.md` on 2026-10-05 to shrink the read surface.

## Resolved

<!-- Move entries here with the outcome and the date. Keep them: a hypothesis that turned
     out wrong is the most useful kind of record, and deleting it invites re-litigation. -->

_None yet — this log was created 2026-08-08._

### H1 — [RESOLVED 2026-08-08 — position accepted with the counter-position's guardrail] Extending the present-empty coercion to all late-wave OpenMeteo feeds is safe
**Position**: The `data_fetcher` present-but-empty → `None` coercion applied to the two buurt feeds (`ad008df`) can be extended to `demand_weather_forecast`, strategic weather/solar, and `offshore_wind` without losing a real failure signal, because a 0-point feed is strictly less informative than an absent one either way.
**Counter-position**: Those feeds are closer to Augur's consumption path than buurt is. Silently downgrading them to `'info'` could mask a sustained outage the way #38 was specifically built to prevent.
**Method**: Decide between the three options written up in `memory/work-items/present-empty-guard-rollout.md`. The #38 streak-counter mechanism (`data/_upstream_empty_streak.json`) already exists and is the obvious middle path — coerce, but escalate after N consecutive runs.
**Status**: Issue #42 open, work item written, decision pending. Not started.
**Review by**: 2026-09-01, or sooner if a late-wave timeout aborts a publish.
**Outcome (2026-08-08, commit `7ff9623`)**: Neither position won outright, which is why the entry was worth writing. The coercion was extended to all six feeds (the position), but **time-boxed** to two runs before the completeness gate is allowed to fail loudly (the counter-position's objection, that these feeds sit closer to Augur than buurt does and could mask a sustained outage). The deciding evidence arrived after the entry was written: run `30838120578` showed every offshore location timing out at once, so "only buurt has ever failed this way" stopped being true. Residual risk, unchanged: the escalation branch has never fired in production. See `memory/work-items/present-empty-guard-rollout.md`.

### H3 — [RESOLVED 2026-08-08 — counter-position was right; built the third option] Committing the shape sidecar before the drift tripwire would let volatility self-classification work as designed
**Position**: `derive_volatile_feeds()` cannot learn from a run that fails, because the tripwire (`collect-data.yml:119`) precedes the commit step (`:149`) and a failing run commits nothing. Committing the sidecar *before* the gate would close the loop, so a recurring transient self-classifies after its second occurrence instead of never.
**Counter-position**: Committing pre-gate means a drifted (possibly genuinely broken) shape enters the baseline, so the *next* run diffs against a bad reference and the break becomes the new normal — precisely the silent-drift failure the fail-mode flip was introduced to end on 2026-06-10. A separate learning-only record, not the gate's baseline, may be the right shape.
**Method**: Prototype against the committed history of `ned_production` / `wind_forecast`, whose 08-03 drift is the known-good test case. Check whether a "would have been classified volatile" replay reaches the right verdict without the baseline poisoning above.
**Status**: Filed as a GitHub issue 2026-08-08. Not started — this needs the engineer's judgement on the baseline trade-off before any code.
**Review by**: 2026-09-08, or immediately if the tripwire fails again on a transient.
**Outcome (2026-08-08)**: The position's *mechanism* was wrong and the counter-position's objection was decisive — committing the sidecar pre-gate really would poison the baseline. The entry's own closing line ("a separate learning-only record, not the gate's baseline, may be the right shape") turned out to be the answer, so the fix was built that way:
- `data/_shape_observations.jsonl` — append-only, one compact line per run (feed → `shape_hash`, plus `schema_version`), written by `data_fetcher` every run regardless of outcome, capped at 400 lines.
- A new workflow step commits **only** that file, placed *before* the tripwire, so the gate's `git show HEAD:data/_shape_signatures.json` still resolves to the previous baseline.
- `derive_volatile_feeds()` prefers the log and falls back to the sidecar's git history when it holds <2 records.
- Backfilled 75 records from existing sidecar history so the classifier behaves identically from day one rather than going blind for two runs.

**Verified in production 2026-08-09**, dispatched run `31297706013` — the first to execute the new step. Commit `b94b9c5` (pre-gate) contained exactly 1 file, the `.jsonl`, with zero occurrences of `_shape_signatures.json`; the baseline advanced only afterwards in `04c201d`. Log grew 76→77; both jobs green; Pages deployed. The ordering that makes the whole thing work — observation, then gate, then baseline — held exactly as designed. #43 closed.

**Honest limit, worth keeping:** the fix is **prospective only**. The backfill reproduces the same classification as before, because it is rebuilt from the same committed sidecars that never contained the failing runs' drift. The 2026-08-03 `ned_production`/`wind_forecast` observations are gone for good. What changed is that the *next* occurrence gets recorded instead of discarded — verified by simulation: appending one drifted record flips `ned_production` to volatile, where previously no number of failing runs ever could.

### H5 — [RESOLVED 2026-08-09 — accepted by the maintainer, with retuned triggers] Git-as-archive remains viable until the repo approaches ~1 GB (#9)
**Position**: Deferring the storage migration is correct; `data/` growth is linear and predictable, and the monthly archive to `05. Data/` bounds the working set.
**Counter-position**: Clone time and Actions checkout cost degrade well before the 1 GB headline number, and the migration gets harder the longer it waits.
**Method**: Record `git count-objects -vH` size at each monthly archive. If growth is superlinear, or checkout time in the daily run exceeds ~60s, re-plan.
**Revisit trigger**: repo > 700 MB, or daily-run checkout > 60s.
**⚠ TRIGGERED 2026-08-08 — both clauses, on the first check after this entry was written:**
- `git count-objects -vH` → **size-pack 797.09 MiB** (threshold 700 MB), `.git` 799 MB on disk.
- Checkout step in run `31199044747` (08-07, a *successful* run) → **101s** (threshold 60s). That is ~half the total wall-clock of a healthy 3m26s collect, spent before any data is fetched.
- `data/` now holds **4,974** JSON files; MEMORY.md's #9 note said 3,909 as of 2026-06-14 — ~1,065 added in eight weeks.

The position above ("deferring is correct, growth is linear and predictable") is the part now in doubt: 1 GB is roughly one quarter away at this rate, and the *cost* the threshold was proxying for — checkout time — has already arrived. Not resolving this here; it needs the engineer's call on #9. What changed is that it is no longer a someday problem.

**Follow-up measurement, same day — this reframes the fix and kills the obvious one.**
The intuitive mitigation is to bound `fetch-depth` in `collect-data.yml` (currently `0`, "all history for all branches and tags"). **Measured: it does nothing.** A local depth-250 clone is 784 MB against 792 MB for a full clone, with no time saved. The reason:

| | |
|---|---|
| `data/` timestamped archive | **1,029 MB, 4,947 files** |
| `data/` current copies | 3.9 MB, 27 files |
| everything else (code, docs, memory) | 9.5 MB |

The files are **write-once**: 5,163 blobs reachable from HEAD's tree against 1,187 commits, so there is almost no churn and truncating history frees almost nothing. ~99% of the repo is the archive *at HEAD*, not in history.

Consequence for #9: the two mitigations are not independent, and neither works alone. `fetch-depth` alone is void (blobs are reachable from HEAD). Deleting old files alone leaves `fetch-depth: 0` still pulling all history. **Moving the archive out of the repo, and only then bounding fetch-depth, is what makes checkout cheap** — and it does *not* require the history rewrite previously assumed, because unreferenced history you never fetch costs nothing at checkout time. That is a substantially cheaper path than "migrate or rewrite" and should be weighed before either.

Also note `derive_volatile_feeds()` needed ~86 commits of sidecar history for its 60-commit window. **No longer true after #43** — it reads a working-tree file, and the tripwire itself only needs `git show HEAD:`, i.e. depth 1. So a shallow checkout became possible as a side effect of #43.

**Maintainer decision, 2026-08-09 — ACCEPTED, do not re-raise.**
Git-as-archive is kept deliberately. The reasoning is *storage*, not speed: GitHub provides durable, free, versioned hosting for the collected dataset, and that is a feature of the current design rather than an accident to be engineered away. The 101s checkout is explicitly acceptable at current volumes.

This closes the question the follow-up measurement opened. The migration work in #9 stays on the backlog as a someday item, not a pending decision.

**The old triggers (700 MB / 60s checkout) are retired — they fired on exactly what has now been accepted, and a check that re-derives an accepted non-finding every session is the "cries wolf" failure this framework exists to catch.** Replaced with the constraints that would genuinely change the answer:

- **Repo > 4 GB.** GitHub's guidance is a soft recommendation around 1 GB and a strong one around 5 GB; 797 MiB today, growing ~1,065 files / 8 weeks. Crossing the soft line is a nag, not a failure, so it is not the trigger — approaching the strong one is.
- **A push or clone actually fails**, or GitHub contacts the account about repo size.
- **Checkout exceeds ~50% of total run wall-clock** on a run that is otherwise healthy. Today it is ~70% of a 2m23s run, which sounds alarming and is not — the run is short. The signal is only meaningful if the *absolute* cost starts blocking the daily window.
- **A second consumer needs the archive** (an ML job, a dashboard) and cannot afford a full clone. That changes the cost/benefit rather than the size.

Nothing else here needs revisiting. If a future audit surfaces repo size again without one of the above, the correct response is to close it citing this entry.

### H9 — [RESOLVED 2026-08-08, position confirmed] The `cryptography<44` pin will block a venv rebuild on current Python
<!-- Renumbered from H6 on 2026-08-31: the 2026-08-14 session opened a SECOND H6 (MEMBER_MAPPED_FEEDS) without noticing this one, and the collision made every bare "H6" reference ambiguous — including two live source comments. The live entry keeps H6 because `scripts/detect_schema_drift.py` and `collectors/_entsoe_shared.py` cite it; this resolved one moved. -->
**Position**: `requirements.txt` pins `cryptography>=41.0.0,<44.0.0`, an upper bound that predates Python 3.13/3.14. The venv is uv-managed on 3.12.13 while the system interpreter is 3.14.4, so anyone recreating the venv from system Python lands on an untested combination, and `cryptography` — the AES-CBC/HMAC dependency named in Hard Constraints — is the most likely thing to fail to resolve or build.
**Counter-position**: The pin is deliberate and nothing forces a rebuild; uv reproduces 3.12.13 from `pyvenv.cfg`, and CI pins 3.12 explicitly in both workflows. This may be a non-problem that only bites on a machine migration.
**Method**: `uv venv --python 3.14 && uv pip install -r requirements.txt` in a throwaway directory. If it resolves, raise the bound and add 3.13/3.14 to `test.yml`'s matrix (currently `['3.12']`, a single entry, so nothing tests above 3.12). If it does not, record the floor explicitly — there is no `requires-python` declared anywhere today, so "we support 3.12" is convention rather than something enforced.
**Revisit trigger**: any venv rebuild, a machine migration, or Dependabot proposing a `cryptography` major bump.
**Outcome (2026-08-08) — confirmed, and fixed:**
- On 3.14, `<44` resolves to `cryptography` 43.0.3 → `cffi` 1.17.1, which ships **no 3.14 wheel**. uv falls back to a source build and dies on `fatal error: ffi.h: No such file or directory`. So the pin did block a rebuild, exactly as posited.
- Unpinned on 3.14 resolves cleanly to `cryptography` 50.0.0 + `cffi` 2.1.1 (prebuilt wheels), and `SecureDataHandler` round-trips correctly on 3.14.4.
- Raised the bound to `<51.0.0`. Validated on the **production** interpreter (3.12): resolves to `cryptography` 46.0.0, full suite **714 passed**. This also lifts a security-sensitive dependency that was seven majors behind, which matters more than the version-skew question that started this.
- **Did NOT add 3.13/3.14 to `test.yml`'s matrix.** `memory/project_actions_optimization.md` records that 3.13 was deliberately *dropped* on 2026-03-30 to save ~90 min/month after the account hit the 3,000 min/month GitHub Actions limit. Re-adding it would silently reverse a live cost decision. The floor stays convention-enforced rather than matrix-enforced; if that becomes unacceptable, the cheap fix is a `requires-python` in a `pyproject.toml`, not a second CI job.

### H11 — [RESOLVED 2026-10-06 — holds, with a per-member tolerance] The span check's derived expectations hold in production (tracked: #53)
**Position**: `MIN_SPAN_OBSERVATIONS=10` and `MIN_SPAN_AGREEMENT=0.6` separate "this member's span legitimately varies" from "this member lost days", with no false alarms on the scheduled run. Basis: 30 vintages of every published feed measured 2026-09-04. The two genuinely-growing series (`market_history` carbon_eua 13%, gas_ttf 30%) sit far below the floor and correctly get no expectation; everything else sits at 77-100% and gets one. 0.6 falls in an empty band — nothing measured between 30% and 77%.

**Counter-position worth holding**: the 77-87% band is entirely day-ahead-dependent members (`entsoe` 77%, `load_forecast` 80%, `nordic_hydro` 80/87%). Their minority readings are pre-auction runs. If the scheduled run ever drifts earlier, or an upstream publication time moves, those members flip to alarming every day — a recurring false alarm, which is how an alarm stops being read. The floor protects against *deriving* a wrong expectation; it does nothing about a *correct* expectation the run can no longer meet.

**Method**: after ~15 scheduled runs (enough for `MIN_SPAN_OBSERVATIONS` plus headroom), read `data/_span_shortfalls.json` history and the `span-shortfall` issue. Three outcomes: (a) no alarms and `members_with_expectation` climbing toward `members_checked` — position holds; (b) alarms only on manual pre-auction dispatches — expected and already suppressed, since the alert is gated to `github.event_name == 'schedule'`; (c) a member alarming on scheduled runs with no real loss — the expectation is wrong for that member and it needs a per-member override, not a global threshold change.

**Do NOT resolve this by loosening the threshold.** A global loosen buys silence on the false alarm and blinds every other member. Case (c) is a per-member problem and wants a per-member answer.

**Review by**: 2026-09-25, or immediately if the `span-shortfall` issue opens on a scheduled run.

**Data point 2026-10-06** (not a resolution): the 10-05 run `37381385635` reported 14 short members on #83. Cause established — the cron started 22:16 UTC, past Amsterdam midnight, so every day-ahead member carried only 10-06 (gotcha log 2026-10-06). That shortfall was a TRUE positive, not a bad expectation. Whether #83's earlier comments were too is still unchecked.

**Known inert period**: the check reports `checked=false` ("not verified") until 10 observations with spans accumulate. First span-bearing observation was 2026-09-04, so it cannot judge anything before ~2026-09-14. A clean result before then means nothing was verifiable, and the reporter says so.
**Outcome (2026-10-06)**: The Method was run against #83 and the last 30 observations. Outcome (c) applied to two members only. `gas_storage` (root) was 7 of 8 on 09-11, 09-14, 10-03 and 10-04, because GIE publishes its newest day late. `market_proxies` `gas_ttf/history` was 24 of 25 on every Sunday, because there is no weekend trading. Both get one day of slack in `SPAN_TOLERANCE_DAYS` (`utils/span_signature.py`), and no global threshold changed. Replaying the last three observations: 10-03 went from 1 shortfall to 0, 10-04 from 2 to 0, and 10-05 kept all 14, which were real (the late-cron day roll). Day-ahead members get no slack, and a test asserts it. **Do not add slack for a day-ahead member**: that is the #51 loss this check exists for.

### H4 — [RESOLVED 2026-10-06 — flat override chosen by the maintainer] `STALENESS_OVERRIDES` with a weekend-spanning floor fully fixes the weekend `error` (#36)
**Position**: Adding `market_proxies` / `market_history` at ~96h (matching `gas_storage`) removes the spurious weekend `error` without hiding a real market-data outage, because a genuine outage exceeds 96h by Monday.
**Counter-position**: A fixed floor is the same shape as the 48h threshold it replaces — cadence-blind. A long weekend or exchange holiday could still trip it, and 96h is late enough to delay noticing a real outage by a day.
**Method**: Weekday-aware staleness (skip non-trading days) is the principled fix; the flat override is the cheap one. Compare against a month of committed `market_*` files before choosing.
**Status**: Issue #36 open since 2026-06-14. Recurs every weekend, non-blocking.
**Review by**: 2026-10-01 — low urgency while it stays non-blocking, but it erodes the meaning of `overall_status=error` every single week.
**Outcome (2026-10-06)**: The Method was run against the committed September quality reports. The Sunday runs (09-06, 09-13, 09-27) set `overall_status=error` on `market_proxies` and `market_history` at 66.0–67.5h, and the Mondays were clean. Both feeds now have a 96h entry in `STALENESS_OVERRIDES`, matching `gas_storage`. Accepted costs: an outage that starts on a Friday is first flagged on Tuesday, and multi-day exchange holidays such as Easter still go past 96h. The trading-day-aware fix stays available if that matters.
