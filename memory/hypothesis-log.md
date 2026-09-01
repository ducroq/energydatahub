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

### H1 — [RESOLVED 2026-08-08 — position accepted with the counter-position's guardrail] Extending the present-empty coercion to all late-wave OpenMeteo feeds is safe
**Position**: The `data_fetcher` present-but-empty → `None` coercion applied to the two buurt feeds (`ad008df`) can be extended to `demand_weather_forecast`, strategic weather/solar, and `offshore_wind` without losing a real failure signal, because a 0-point feed is strictly less informative than an absent one either way.
**Counter-position**: Those feeds are closer to Augur's consumption path than buurt is. Silently downgrading them to `'info'` could mask a sustained outage the way #38 was specifically built to prevent.
**Method**: Decide between the three options written up in `memory/work-items/present-empty-guard-rollout.md`. The #38 streak-counter mechanism (`data/_upstream_empty_streak.json`) already exists and is the obvious middle path — coerce, but escalate after N consecutive runs.
**Status**: Issue #42 open, work item written, decision pending. Not started.
**Review by**: 2026-09-01, or sooner if a late-wave timeout aborts a publish.
**Outcome (2026-08-08, commit `7ff9623`)**: Neither position won outright, which is why the entry was worth writing. The coercion was extended to all six feeds (the position), but **time-boxed** to two runs before the completeness gate is allowed to fail loudly (the counter-position's objection, that these feeds sit closer to Augur than buurt does and could mask a sustained outage). The deciding evidence arrived after the entry was written: run `30838120578` showed every offshore location timing out at once, so "only buurt has ever failed this way" stopped being true. Residual risk, unchanged: the escalation branch has never fired in production. See `memory/work-items/present-empty-guard-rollout.md`.

### H2 — [RESOLVED 2026-08-14 — refuted for the drift-tripwire class; neither position was right] The daily-run failure rate is dominated by transient upstream/runner faults, not defects
**Position**: Recent failures are environmental, not regressions. Evidence: run `31123856009` (08-06) failed with "job was not acquired by Runner" (pure GitHub infrastructure); run `30838120578` (08-03) failed the drift tripwire on transient-driven shape churn, and the three surrounding runs were green with no code change.
**Counter-position**: Two failures in five days is a ~40% failure rate. Calling that "transient" is exactly the reasoning that let GoogleWeather 401 for seven months. The pattern may be a real degradation in the OpenMeteo late-wave that the retries merely paper over.
**Method**: Tabulate the last 30 scheduled runs by failure class (runner-acquisition / drift tripwire / quality gate / collector). If any *non-infrastructure* class exceeds ~1 in 10, treat it as a defect and open an issue rather than re-running.
**Revisit trigger**: Two consecutive failures, or any failure whose cause is not one of the two classes above.
**Outcome (2026-08-14)**: The Method was run — 30 scheduled runs, 2026-07-18 to 2026-08-14, 3 failures. Classified: `31123856009` (08-06) runner-acquisition, pure infrastructure; `30838120578` (08-03) and `31820625501` (08-14) both drift-tripwire. So the non-infrastructure class is 2/30 ≈ 1 in 15, *under* the 1-in-10 threshold the entry set — and the threshold was still the wrong instrument, because it measures rate and the answer turned on cause.

Root-causing the 08-14 failure refuted the position without vindicating the counter-position. The failure was not transient, and it was not upstream degradation papered over by retries. It was a **defect in the tripwire's discrimination**: one Open-Meteo location (`Elsweide_Arnhem_NL`) exhausted its retries and dropped out of `weather_forecast_buurt` and `solar_forecast_buurt`, the `data` key set changed, and the gate could not tell a member dropout from an unversioned schema break. It failed the publish of 18 healthy feeds to report a per-location fetch failure. The 08-03 failure is the same class, which is why "transient" fit both and explained neither.

The lesson generalises past this entry: *"the upstream blipped"* and *"we have a defect"* are not the only two options. A transient upstream event can expose a real defect in the thing that observes it, and the failure then recurs whenever the transient does. The rate-based Method could not have found that — only reading the failing run could.

**Residual, deliberately not claimed as resolved**: whether the OpenMeteo late-wave dropout rate is itself rising. The 08-14 dropout was real, and nothing here measures how often locations drop; the collectors now emit a `location_completeness` quality issue, so from this commit forward the committed quality report carries that signal. Revisit once ~30 runs of that data exist.

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

### H6 — A declared `MEMBER_MAPPED_FEEDS` set plus member homogeneity is a sufficient discriminator for member drift (tracked: #44)
**Position**: The member-drift downgrade (2026-08-14) is safely bounded by two gates: the feed must be declared member-mapped, and every member on both sides must share one signature. A feed that keys `data` by field name is rejected by either gate, so the tripwire keeps failing on real unversioned breaks while a dropped location warns.
**Counter-position**: The declared set is a hand-maintained registry keyed on a feed identifier, and this repo has two recorded incidents of exactly that shape drifting out of sync (`DATASET_MISSING_SEVERITY`'s three parallel severity lists; `collect-data.yml`'s two publishable-feed lists, where `nordic_hydro` was added to one and not the other). A sixth member-mapped feed added later, and not registered, silently reverts to publish-blocking on its first dropout — the 2026-08-14 failure again, with the fix already in the tree. Homogeneity does not save it: it is a *necessary* condition that gates nothing on its own, since an unregistered feed never reaches it.
**Method**: The registry is only checkable against reality by asking, per published feed, whether its `data` keys are locations. That is derivable — `data_fetcher` knows which collectors were constructed with a `locations=` list. A startup or CI assertion that every feed whose collector took `locations=` is either in `MEMBER_MAPPED_FEEDS` or explicitly excluded would make the set self-maintaining, the way `derive_volatile_feeds()` did for volatility. Until then this is a hand-maintained list and should be read as one.
**Status**: Shipped 2026-08-14 with the registry hand-written (5 feeds). The self-maintaining version is not built.
**2026-08-30 — trigger fired; counter-position NOT confirmed, and the registry turned out to be the wrong lever.** Four collectors gained per-member delivery tracking (`collectors/_entsoe_shared.py`), producing five zone-keyed feeds — the "new per-member feed" case. All five were checked with `classify_data_member_drift` against the live sidecar rather than assumed. Only `nordic_hydro.json` is eligible at all: `generation_mix.json` and `wind_forecast.json` return `None` (non-homogeneous members; member map two levels down), and `load_forecast.json` / `generation_forecast.json` short-circuit as CRITICAL_FEEDS. So homogeneity did far MORE gating than the counter-position credited — four of five were rejected by it, not by the registry, and the hand-maintained list was never the binding constraint. Note also that H6's **Method** would not have found these: it proposes deriving membership from collectors constructed with `locations=`, and all four take `country_codes=`.
**And registering the one eligible feed would have been a net regression**, which is the real finding. Declaring a feed also strips it from `derive_volatile_feeds()`. Registration rescues only a member-SET change; the likelier drift here is `metadata.collector_quality_issues` arriving while both zones are present, which classifies `None` and hard-fails. Unregistered, that flip fails once, seeds a second hash, and self-classifies volatile thereafter. Registered, it would abort the publish every run forever — `nordic_hydro` has exactly one hash across 97 observations, so the hazard is latent rather than absent. Deferred pending H7, which is its precondition. The reasoning is recorded in full in `scripts/detect_schema_drift.py`.
**Review by**: 2026-10-01, or immediately when a new per-member feed is added — that is the moment the gap bites.

### H7 — Scoping the diagnostic-key exemption to member drift is enough (tracked: #45 — now a PRECONDITION for H6's registration, not an improvement)
**Position**: `metadata['collector_quality_issues']` appears only when a run raises an issue, so its arrival changes a feed's envelope shape. `DIAGNOSTIC_ENVELOPE_KEYS` exempts it inside `classify_data_member_drift`, which is where it demonstrably broke something: without it, the collectors' new `location_completeness` issue would have re-blocked the publish for the exact reason of the 2026-08-14 incident (verified — the verdict came back `None`).
**Counter-position**: The churn is not specific to member drift. Every collector using `_add_quality_issue` — `tennet`, `luchtmeetnet`, `entsoe_hydro`, and now both OpenMeteo classes — flips its metadata shape the first time it raises an issue, on *any* feed. For a feed that is neither member-mapped nor volatile, that is still an unexplained hard failure waiting to happen, and it would look exactly like a schema break to whoever debugs it. It may also be part of why `air_quality_buurt` reads as volatile at all.
**Method**: Check the committed shape history for a hash flip that coincides with a `collector_quality_issues` appearance — `luchtmeetnet` and `tennet` are the candidates with the most issue-raising history. If one is found, the exemption belongs in the tripwire's envelope comparison generally, not only in the member-drift classifier.
**Status**: Narrow fix shipped 2026-08-14. Deliberately not generalised in the same change.
**2026-08-30 — trigger fired, counter-position CONFIRMED.** The 2026-08-29 ENTSO-E outage is the predicted case, and the zone-delivery change makes it concrete: on a degraded run `load_forecast.json` now gains `metadata.collector_quality_issues`, a second shape flip on top of the vanished zone. `_without_diagnostic_keys` strips it only inside `classify_data_member_drift`, which that feed never reaches (CRITICAL_FEEDS short-circuit). `wind_forecast.json` is worse: its ENTSO-E metadata sits at `data.entsoe_wind_generation.metadata`, inside the data block, where the exemption does not apply at all. So a collector raising its first-ever quality issue can still hard-fail a publish for a reason that looks nothing like a schema break. Generalising the exemption to the tripwire's envelope comparison is now the indicated fix, not a speculative one.
**Review by**: 2026-09-15 — or sooner; this is now a known live gap rather than an open question.

### H8 — Merging all timestamp-map records (instead of sampling one) ends the sampled-record false-positive class, without needing the diagnostic-key exemption generalised
**Position**: The 2026-08-23 `load_forecast` failure was the sampled-record defect the tripwire's own comments had documented since June, firing for the first time on a CRITICAL feed it could not downgrade. Merging every record into the `value_shape` (`_merge_signatures`) makes the fingerprint order-independent, so intra-day completeness variance can no longer masquerade as a schema break — a field gone from *every* record still drifts, a field gone from some does not.
**Counter-position**: The merge is lossy in the tolerant direction: a field surviving in even one record is "present", so a partial removal (191 of 192 records) no longer drifts, and a genuinely removed field is only caught once it leaves the rolling window. That boundary is the FMEA gate's job, not the fingerprint's — but if the DQ gate is itself blind (see the `ned_production` silent-skip gotcha), the union removes the tripwire's last sight of partial-availability drift without a replacement.
**Method**: Watch the next ~10 scheduled runs: if a partial upstream gap no longer aborts a publish (good) AND no shape break slips through the union uncaught (still-good), the boundary holds. The counter-position's objection is settled only when the DQ presence-check for the `ned_production` `actual` half is built — see follow-up issue.
**Status**: Shipped 2026-08-23 (uncommitted at session end, to land 2026-08-24). All 20 committed feeds verified byte-identical under the merge; live 08-23 payload confirmed to hash to baseline.
**Review by**: 2026-09-07, or immediately if the tripwire fails again on a partial-availability shape on a non-member feed.

### H4 — `STALENESS_OVERRIDES` with a weekend-spanning floor fully fixes the weekend `error` (#36)
**Position**: Adding `market_proxies` / `market_history` at ~96h (matching `gas_storage`) removes the spurious weekend `error` without hiding a real market-data outage, because a genuine outage exceeds 96h by Monday.
**Counter-position**: A fixed floor is the same shape as the 48h threshold it replaces — cadence-blind. A long weekend or exchange holiday could still trip it, and 96h is late enough to delay noticing a real outage by a day.
**Method**: Weekday-aware staleness (skip non-trading days) is the principled fix; the flat override is the cheap one. Compare against a month of committed `market_*` files before choosing.
**Status**: Issue #36 open since 2026-06-14. Recurs every weekend, non-blocking.
**Review by**: 2026-10-01 — low urgency while it stays non-blocking, but it erodes the meaning of `overall_status=error` every single week.

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

## Resolved

<!-- Move entries here with the outcome and the date. Keep them: a hypothesis that turned
     out wrong is the most useful kind of record, and deleting it invites re-litigation. -->

_None yet — this log was created 2026-08-08._
