# Memory

<!-- NOT loaded automatically (agent-ready-projects v1.20.0). This file sits
     below the auto-loading cliff. The `Picking up where the last session left
     off` row in CLAUDE.md's "Before You Start" table is what brings it into a
     session; if that row is removed or made vague, nothing here is ever read,
     and the failure is SILENT — an index that was never loaded looks exactly
     like one with nothing to say. (Claude Code auto-loads a USER-LEVEL memory directory at
     ~/.claude/projects/<slug>/memory/ — for this project it exists and holds
     per-topic files but NO MEMORY.md, so nothing there shadows this file.
     Same mechanism, different tree. A CLAUDE.md `@memory/MEMORY.md` import would genuinely
     auto-load this one — a deliberate trade of context budget for
     reliability, not the default here.)

     Keep lean — navigational index only; it is read in full whenever it is
     reached. Deep knowledge lives in topic files (linked below). Reference
     paths and architecture live in CLAUDE.md, not here.

     END-OF-SESSION CURATION (~5 min):
     1. Review gotcha-log for recurring patterns — promote them here or to topic files
     2. Check if any entries below are stale — retire them
     3. Update "Current State" to reflect what shipped or changed
     4. Update the savepoint of any work item still in flight (memory/work-items/)
     Monthly: audit everything. Prune as much as you add.

     WORK ITEMS: anything spanning more than two sessions gets a file in
     memory/work-items/ and a one-line pointer in the "In flight" list below:
       - [Short description] → memory/work-items/slug.md [in progress]
     Mark it [done] or remove it when the Outcome section is filled in. -->

## Topic Files

| File | When to load | Key insight |
|------|-------------|-------------|
| `memory/gotcha-log.md` | Stuck or debugging | Open problem-fix entries + Promoted/Mechanized tables. Resolved ones: `memory/archive/gotcha-log-archive.md` |
| `memory/hypothesis-log.md` | Before acting on a "probably transient / probably safe" belief | Open positions with the method that would settle them. Dormant (H6, H8) and resolved (incl. H5: #9 git-as-archive **accepted, do not re-raise on size alone**): `memory/archive/hypothesis-log-resolved.md` |
| `memory/project_active_decisions.md` | Changing a severity, adding a collector on a shared host, adding a registry entry, building or loosening a guard | Standing decisions + promoted patterns. **`DATASET_MISSING_SEVERITY['entsoe'] = 'critical'` is load-bearing for Augur's training target — do not relax it without telling them** |
| `memory/project_data_backfill_gaps.md` | Outage recovery, prioritising manual runs | Which collectors can backfill vs lose data permanently. Tool: `scripts/backfill_missed_run.py` |
| `memory/project_actions_optimization.md` | Adding workflows or scheduled triggers | Account-wide 3k min/month budget, what we cut to fit |
| `memory/project_entsoe_old_files.md` | Touching pre-Oct-2025 historical files | 26 files have malformed timestamps; skip them unless fixing root cause |
| `memory/project_published_dataset_checklist.md` | Wiring a new collector into the publish set | 8-touchpoint lock-step checklist; missing one silently breaks publishing |
| `memory/archive/README.md` | A symptom or decision has history | Session retrospectives (index), resolved gotchas/hypotheses, the pre-2026-10-05 Current State |

## In flight

<!-- One line per active work item. Empty is a valid state — do not invent entries. -->

- **NEXT SESSION STARTS HERE (2026-10-06 handoff).** On "continue", in order:
  1. **Check the 2026-10-06 scheduled run** (not yet started at 19:11 UTC): `gh run list --workflow "Collect and Publish Data" --limit 2`. Expect success, #84 auto-closed, and `Late scheduled run` in the log if it started after 22:00 UTC (the `COLLECTION_DATE` fix, `bb39beb`, first exercise). Any 429s → `grep -c 'rate-limited (retry'` and record under H10 (review by 2026-10-19). Failed → diagnose fresh.
  2. **Confirm Augur recovered**: `git -C ~/repos/veen-systems/augur log origin/main -1`. On 10-06 it still read `[ALARM: t0 stale 2026-10-04]` + `no new publish since 2026-10-03` — expected: the backfill (`66c4201`) committed data files but is not a Pages publish. Tonight's run is the recovery.
  3. **Read-surface pass 3 — mechanize, then retire** → `memory/work-items/read-surface-thinning.md`. First: #85 (one test for the published-dataset registries + both workflow lists), then shrink `memory/project_published_dataset_checklist.md` to what the test cannot see. Then thin the three open hypotheses (H2/H7/H10 are ~6k chars each of dated review narrative).
  4. **#79** (decide the `CRITICAL_FEEDS` carve-out before `_partition_member_drift` classifies), and one adversarial pass over `observation_from_sidecar` + `volatile_feeds_from_observations`.
- Read-surface thinning → `memory/work-items/read-surface-thinning.md` [in progress; passes 1–2 done].
- Present-empty guard rollout (#42) → `memory/work-items/present-empty-guard-rollout.md` [DORMANT; reopens on the next graced-empty run that publishes].

## Current State

- **Pipeline**: daily 16:00 UTC cron (actually starts ~18:55–20:45 UTC) + `workflow_dispatch`. Last failure: 2026-10-05 `37381385635` (cron started 22:16 UTC, after Amsterdam midnight, so the collection day rolled over → empty `generation_forecast` → drift gate). Fixed by `COLLECTION_DATE` anchoring (`bb39beb`) and backfilled (`66c4201`) on 2026-10-06. The one before: 2026-10-04 `37226371364` (Open-Meteo 429 storm), fixed 2026-10-05. Detail: `memory/archive/project_session_2026_10_05.md`.
- **Harness**: framework agent-ready-projects v1.49.2 (2026-10-06). `CLAUDE.md` 13.8k chars after read-surface pass 2. Detail: `memory/archive/project_session_2026_10_06.md`.
- **Feeds**: 20 in the committed baseline <!-- verify: n=$(git show origin/main:data/_shape_signatures.json | python -c "import json,sys; print(len(json.load(sys.stdin)['feeds']))"); echo "committed baseline carries $n feeds (claim 20)"; [ "$n" -eq 20 ] || exit 1 -->. Schema 2.4.
- **Gates**: completeness, quality (`critical` blocks), schema drift (blocks, runs before publish), span (warn + alert, #53). **Do not add a second blocking gate** — see CLAUDE.md CI/CD row.
- **Open issues**: 27 on 2026-10-06 (#36 closed, #85 filed) <!-- verify: n=$(gh issue list --state open --json number | jq length); echo "open=$n (claim 27 on 2026-10-06)"; [ "$n" -eq 27 ] || exit 1 -->. #84 (publish failing) should self-close on the next green run. #83 (span shortfall: gas_storage, gas_ttf) is open; H11 resolved it as cadence, and `2fdad76` added the per-member tolerance — expect it to stay quiet. The per-issue narrative that used to sit here is in `memory/archive/current-state-2026-10-05.md`.
- **Known degraded**: `grid_imbalance` balance_delta synthesised (TenneT endpoint 404 since 2026-06-08). Weekend market staleness fixed by the 96h override (`2fdad76`, #36 closed, H4 resolved).
