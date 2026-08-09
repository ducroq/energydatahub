# Session 2026-08-08 — agent-ready-projects v1.17.0 adoption, reviewed against itself

No pipeline code was touched. Everything here is agent-harness: the verification hook, the
two project-local skills, work items, and the hypothesis log — plus the first tests for
`utils/secure_data_handler.py` and a diagnosis of the 08-03 daily-run failure.

## What shipped

- **Framework v1.10.1 → v1.17.0.** `.claude/settings.json` + `.claude/hooks/verify_edit.py`
  (PostToolUse compile-and-test hook), `.claude/skills/review-changes/` and
  `.claude/skills/release/`. `curate` and `audit-context` stay user-global by design.
- **`.gitignore`** now tracks the shared harness (`!.claude/settings.json`,
  `!.claude/hooks/`, `!.claude/skills/`) while `settings.local.json` stays personal.
- **`memory/work-items/`** — savepoints for multi-session work. Created under
  `docs/work-items/` and moved; see below.
- **`memory/hypothesis-log.md`** — new. Open positions (H1–H5) with the method that would
  settle each, and a review date or trigger. `/curate` surfaces overdue entries.
- **`tests/unit/test_secure_data_handler.py`** — 23 tests. Suite 681 → 704.

## The point of the session: the reviewer found three blockers in its own commit

`/review-changes` was run on the diff that introduced `/review-changes`. Full 5-lens
battery (HIGH tier). It found three blockers, all real, all in the adoption itself:

1. **The hook's workflow-YAML branch failed 100% of the time.** PyYAML was in neither the
   venv nor `requirements.txt`, and `run()` could not distinguish `ModuleNotFoundError`
   from a parse error — so every workflow edit got "not valid YAML — the daily run would
   fail to start" about the valid, currently-shipping `collect-data.yml`. This check had
   never once validated a workflow.
2. **`/review-changes` Step 1 could not see untracked files.** It prescribed only
   `git diff --stat/--cached/--summary`; none of those report untracked files. On its own
   change set that was 645 of 695 lines invisible, while the magnitude gate declared "a
   new file in a HIGH path" always-full-depth — a carve-out the procedure could not
   observe. The review only worked because `git status --porcelain` was run off-script.
3. **`docs/work-items/` was inside the GitHub Pages publish root.** `collect-data.yml`
   uploads all of `docs/` as the Pages artifact, so a work item describing an *unfixed*
   availability weakness — which feeds can abort the daily publish, and the trigger —
   would have been world-readable on the next daily run. Moved to `memory/work-items/`
   before anything published. This deviates from the upstream template on purpose; the
   reason is recorded in the work-items README so a future sync doesn't "fix" it back.

Also fixed: `settings.ini` promoted MEDIUM→HIGH in the tier table (it holds
`encryption = 1`, and the guarantee lens that owns the encryption invariant is HIGH-only,
so it would never have run on the one file that turns encryption off); `mapped_tests()`
now unions its glob patterns instead of stopping at the first (latent, not live — it
narrowed nothing across today's 43 files, but silently drops the natural second test file);
`realpath` so a symlinked checkout doesn't silently skip; and `/release` no longer credits
`detect_schema_drift.py` with enforcing migration functions it never checks — that is
`tests/unit/test_schema_registry.py`, and nothing at all enforces the changelog entry.

**Method note worth keeping:** two lenses reached opposite conclusions on the
`mapped_tests` `break`. Enumerating all 43 real files settled it — first-match and
full-union were identical, so the live-defect claim was wrong and the latent-trap claim
was right. Lens disagreement is a prompt to go measure, not to average the two reports.

## `utils/secure_data_handler.py` had zero tests

The AES-CBC + HMAC-SHA256 module named in Hard Constraints had no unit test. Because the
hook falls back to the full `tests/unit` suite when a file has no mapped test, editing it
produced a **green hook from 681 tests that never imported it** — the project's documented
silent-no-op signature, on the encryption boundary. Now covered: round-trip (incl. unicode,
empty, exact-block-multiple padding), IV freshness, and tamper detection on IV, ciphertext,
and signature independently. Exit 0 from the hook is not a coverage claim.

## Daily-run failures diagnosed

- `31123856009` (08-06) — "job was not acquired by Runner". Pure GitHub infrastructure.
- `30838120578` (08-03) — drift tripwire failed on `ned_production` + `wind_forecast`
  (transient-driven shape churn; quality gate itself was only `warning`). The three
  surrounding runs were green with no code change.

That second one exposed a structural gap. `derive_volatile_feeds()` learns from committed
sidecar history, but the tripwire (`collect-data.yml:119`) runs *before* the commit step
(`:149`) — so a run it fails commits nothing, and the classifier never sees the drift that
tripped it. Both feeds remain unclassified five days later and will fail the same way.
Filed as a GitHub issue; recorded as H3 in the hypothesis log, because the obvious fix
(commit the sidecar pre-gate) risks poisoning the baseline with a genuinely broken shape —
exactly what the 2026-06-10 fail-mode flip existed to stop.

## Post-adoption: `/audit-context` and a second `/curate`

**The audit found the memory index had outgrown the project file.** MEMORY.md was 24,053
chars against CLAUDE.md's 19,145, with `Current State` at 49% and one `- **Pipeline**`
bullet running 5,174 chars across 12 sessions — eight of which had dedicated session files
already indexed two sections above. Trimmed to 20,944 (`69d4114`); the Pipeline bullet lost
57% with no pointer dropped. The mechanism is worth remembering: `/curate` step 3 says
"update Current State" and nothing says "prune", so every session appended and none
truncated.

Reference integrity produced 26 raw hits and exactly **one** real defect (a `conftest.py`
that does not exist, introduced earlier the same day). The other 25 were structural
`data/`-vs-`docs/` filename pairs, glob patterns, cross-repo references, and basename
house style — enumerated in the audit report so the next run doesn't re-derive them.

**H5 fired on its first check.** Written that morning with thresholds of 700 MB and 60s
checkout, both were already exceeded when measured hours later: size-pack **797 MiB**,
checkout **101s** on a healthy 08-07 run, `data/` at **4,974** files against a
last-recorded 3,909 from eight weeks earlier. Measurements posted to #9. The lesson is
about the instrument, not the repo — the belief had never been measured, and writing the
revisit condition *as a number* is the only reason it surfaced.

**Python versions, since it came up twice**: venv 3.12.13 (uv-managed), system 3.14.4, CI
and production both pinned 3.12. The system interpreter is *newer*, not older; the
`unrecognized arguments: --cov=.` trap is a missing `pytest-cov`, not an old Python. 3.12
has security support to Oct 2028 and dev/CI/prod agree, which is what matters for an
unattended pipeline. Recorded as H6: `cryptography>=41,<44` predates 3.13/3.14, nothing
tests above 3.12, and no `requires-python` floor is declared.

## Late session: #42 shipped, #9 reframed, H6 closed

**#42 landed (`7ff9623`, unpushed).** The present-empty guard now covers all six
Open-Meteo feeds and is **time-boxed** — coerce for two runs, then let the completeness
gate fail loudly. That resolves H1: neither the position nor the counter-position won,
the answer was to take the coercion *and* bound it. Unblocking evidence was run
`30838120578`, where every offshore location timed out in one wave, retiring the "only
buurt has ever failed this way" objection. 10 tests; suite 704 → 714.

**H6 confirmed and fixed.** `cryptography<44` resolves to 43.0.3 → `cffi` 1.17.1, which
has no 3.14 wheel and dies building from source. Raised to `<51`, validated on the
production 3.12 interpreter (resolves 46.0.0, full suite green). Declined the CI-matrix
half of my own plan: `memory/project_actions_optimization.md` records 3.13 being dropped
on 2026-03-30 for the Actions budget, and re-adding it would have silently reversed a live
cost decision. One of the "stale" topic files earning its keep.

**A recommendation I withdrew after measuring it.** I proposed bounding `fetch-depth` as
the cheap fix for the 101s checkout. Measured: depth-250 clone is 784 MB against 792 MB
full, no time saved. `data/` is 1,029 MB of write-once files *at HEAD* (5,163 blobs,
1,187 commits), so there is no churn for history truncation to skip. The useful
consequence for #9 is that moving the archive out and *then* bounding depth makes checkout
cheap **without** the history rewrite everyone assumed was required — recorded on the
issue and in H5.

**Corrected a false alarm**: `offshore_wind.json` missing from the docs publish list is
not a parallel-registry defect — offshore wind is merged into `wind_forecast.json` via a
`CombinedDataSet`. But chasing it surfaced a real interaction, recorded in the work item:
coercing offshore to `None` changes `wind_forecast.json`'s shape fingerprint, and that feed
is one of the two that failed the drift tripwire on 08-03.

## #43 built — splitting baseline from history

The drift tripwire's `_shape_signatures.json` was serving two roles with opposite
durability rules: the gate's **baseline** (advance only on a passing run, or a break
becomes the new normal) and the classifier's **history** (record every run, or it cannot
learn from failures). Because the tripwire runs before the commit step, "baseline" won by
accident and `derive_volatile_feeds()` could only ever learn from its own near-misses.

Split them. `data/_shape_observations.jsonl` is append-only, one compact line per run
(feed → `shape_hash` + `schema_version`), capped at 400 lines, written by `data_fetcher`
regardless of outcome and committed by its own workflow step placed **before** the gate —
staging only the `.jsonl`, so the tripwire's `git show HEAD:` still resolves to the prior
baseline. `derive_volatile_feeds()` prefers the log, falling back to git history below 2
records. Backfilled 75 records from existing sidecar history. 15 tests; suite 714 → 729.

**The fix is prospective only, and that is worth stating plainly.** The backfill reproduces
the *same* classification as before, because it is rebuilt from the same committed sidecars
that never contained the failing runs' drift. The 08-03 observations are unrecoverable.
What changed: appending one drifted record now flips `ned_production` to volatile, where
previously no number of failing runs ever could. Verified by simulation.

Consequently the `VOLATILE_SHAPE_FEEDS` seed stopgap was **not** taken — it would have
loosened the gate for two feeds a week before #43 made it unnecessary.

The verification hook earned its place here: it caught a `NameError` from a missing import
in `detect_schema_drift.py` on the edit that introduced it, before any test run I'd have
done by hand.

## Continued into 2026-08-09

**Framework v1.17.0 → v1.18.0, triaged as "already in force."** `/update-drift` (itself the
v1.18.0 release) found one stamp, one release of drift, and the skill already installed
user-global with a clean estate. Nothing functional changed here — only documentation that
had gone wrong: the "Starting any session" row still prescribed a manual CHANGELOG
comparison, and two places listed the user-global set as two skills rather than three.
Incidentally caught a stale architecture-tree entry still showing `work-items/` under
`docs/`, and `memory/` missing from that tree entirely.

**#9 decided: keep git-as-archive.** The maintainer's reason is *storage*, not speed —
GitHub provides durable free versioned hosting for the collected dataset, and the 101s
checkout is acceptable at current volumes. The 700 MB / 60s triggers written the previous
day were retired: they fired on precisely what had just been accepted, which is the
cry-wolf failure the framework exists to catch. Replaced with ~4 GB, an actual push/clone
failure, checkout dominating a *long* run, or a second consumer needing the archive.

**#43 verified in production and closed.** Dispatched smoke run `31297706013`:

```
b94b9c5  Record shape observation     <- pre-gate, 1 file, 0 sidecar occurrences
         Schema-drift tripwire        <- warned on air_quality_buurt + market_proxies
04c201d  Update energy data           <- post-gate, advances the baseline
```

Log grew 76→77, both jobs green, Pages deployed in 8s. The ordering is the fix, and it
held. The same run exercised the volatility path on real drift without either feed being
a `CRITICAL_FEED` — which the previous day's guards now make impossible anyway.

**#42 remains unverified and that is not fixable by smoke-testing.** Its coercion only
fires on an actual all-locations Open-Meteo timeout; every feed collected fine. A green
run says nothing about it. Waiting on a real transient.

## Open / next

- **#42** present-empty rollout — still not started, decision pending (H1).
- **H3** drift-classifier starvation — needs an engineer call on the baseline trade-off.
- **Gotcha-log retrofit** — 24 entries predate the 2-3 line budget. The log header and
  CLAUDE.md now agree that they are legacy and not the format to copy, so the retrofit is
  optional cleanup rather than a live contradiction. Not done; wants sign-off on a sample.
- **Stale topic files** — `project_actions_optimization.md`, `project_data_backfill_gaps.md`,
  `project_entsoe_old_files.md` last touched 2026-06-07/08. Flagged, not audited.
