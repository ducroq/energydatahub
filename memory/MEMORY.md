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
| `memory/project_published_dataset_checklist.md` | Wiring a new collector into the publish set | The wiring `tests/unit/test_published_feed_registries.py` cannot see |
| `memory/archive/README.md` | A symptom or decision has history | Session retrospectives (index), resolved gotchas/hypotheses, the pre-2026-10-05 Current State |

## In flight

<!-- One line per active work item. Empty is a valid state — do not invent entries. -->

- **NEXT SESSION STARTS HERE (2026-10-28 handoff).** On "continue", in order:
  1. **Read-surface pass 3, retire step** → `memory/work-items/read-surface-thinning.md`: gotcha entries whose lesson is now a check or a promoted pattern (log ~40k chars).
  2. **#91** — pick `info`/`warning` for the 9 feeds missing from `DATASET_MISSING_SEVERITY`, then extend `tests/unit/test_published_feed_registries.py` to require an entry.
  3. **#79** (decide the `CRITICAL_FEEDS` carve-out before `_partition_member_drift` classifies), and one adversarial pass over `observation_from_sidecar` + `volatile_feeds_from_observations`.
  4. **PR #76** (Dependabot: `actions/checkout` 5→7, `setup-python` 6→7, `deploy-pages` 5.0.1 in the publish workflow; rebase requested 10-28). Read the checkout v6/v7 breaking changes, merge right AFTER a scheduled run publishes, watch the next run. Its sibling remote branches (`dependabot/...-02ab6eb143`, `-3e1200532a`) and `fix/entsoe-upstream-nodata-38` have no open PR: confirm merged/superseded, then ask before deleting.
  5. **#90** (`nordic_hydro` NO/SE 10d of 11d, opened 10-27): auto-closes on recovery; if still open, check ENTSO-E's horizon.
- Read-surface thinning → `memory/work-items/read-surface-thinning.md` [in progress; passes 1–2 done, pass 3 mechanize + hypotheses done 2026-10-28].
- Present-empty guard rollout (#42) → `memory/work-items/present-empty-guard-rollout.md` [DORMANT; reopens on the next graced-empty run that publishes].

## Current State

- **Pipeline**: daily 16:00 UTC cron (actually starts ~18:55–20:45 UTC) + `workflow_dispatch`. Last failure: 2026-10-05 `37381385635` (cron started 22:16 UTC, after Amsterdam midnight, so the collection day rolled over → empty `generation_forecast` → drift gate). Fixed by `COLLECTION_DATE` anchoring (`bb39beb`) and backfilled (`66c4201`) on 2026-10-06. The one before: 2026-10-04 `37226371364` (Open-Meteo 429 storm), fixed 2026-10-05. Since then 22 of 22 scheduled runs published (10-06 → 10-27); Augur recovered. Detail: `memory/archive/project_session_2026_10_05.md`.
- **Harness**: framework agent-ready-projects v1.50.0 (2026-10-28). `Reviewer model` declined; reason in `memory/project_active_decisions.md`. Session: `memory/archive/project_session_2026_10_28.md`. `CLAUDE.md` 13.8k chars after read-surface pass 2.
- **Feeds**: 20 in the committed baseline <!-- verify: n=$(git show origin/main:data/_shape_signatures.json | python -c "import json,sys; print(len(json.load(sys.stdin)['feeds']))"); echo "committed baseline carries $n feeds (claim 20)"; [ "$n" -eq 20 ] || exit 1 -->. Schema 2.4.
- **Gates**: completeness, quality (`critical` blocks), schema drift (blocks, runs before publish), span (warn + alert, #53). **Do not add a second blocking gate** — see CLAUDE.md CI/CD row.
- **Open issues**: 26 on 2026-10-28 (#85 closed, #91 filed) <!-- verify: n=$(gh issue list --state open --json number | jq length); echo "open=$n (claim 26 on 2026-10-28)"; [ "$n" -eq 26 ] || exit 1 -->. #83 (span shortfall: gas_storage, gas_ttf) is open; H11 resolved it as cadence, and `2fdad76` added the per-member tolerance — expect it to stay quiet. The per-issue narrative that used to sit here is in `memory/archive/current-state-2026-10-05.md`.
- **Known degraded**: `grid_imbalance` balance_delta synthesised (TenneT endpoint 404 since 2026-06-08). Weekend market staleness fixed by the 96h override (`2fdad76`, #36 closed, H4 resolved).
