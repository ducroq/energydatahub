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

- **NEXT SESSION STARTS HERE (2026-11-24 handoff).** On "continue", in order:
  1. **Judge H11** (cron `23 13`, slot-date `COLLECTION_DATE`, shipped 2026-11-24): for each scheduled run since, note start lag, the `COLLECTION_DATE=` log line, entsoe points (want 192). Tell Augur the result (`ListAgents`; its 11-24 session `augur-0a` had ended before it heard the fix went live in `dc64c967`; if no Augur session is listed, tell Jeroen). Lag still > 8h or any run > 24h late → walk Jeroen through **#67** (off-host `workflow_dispatch` timer + liveness check; needs a workflow-scoped token on an infra host — his decision).
  2. **H10 decision** (2026-11-20 publish lost to an Open-Meteo *daily-quota* 429 on every location): keyed/paid endpoint vs grace on quota-429. Also unexplained: same run, ENTSO-E day-ahead NL/DE_LU `UpstreamNoDataError` for 11-19..11-21.
  3. **Read-surface cuts** → `memory/work-items/read-surface-thinning.md` (Next action). Jeroen wants prune/thin/mechanize/retire pushed hard.
  4. **PR #76** (Dependabot actions bumps): read checkout v6/v7 breaking changes, merge right AFTER a scheduled run publishes. Stale remote branches: confirm merged, ask before deleting.
  5. **#79** (2 lost publishes), **#91** (9 feeds lack a severity), **H7** review (overdue since 11-20).
- Read-surface thinning → `memory/work-items/read-surface-thinning.md` [in progress; passes 1–3 done, gotcha retire done 2026-11-18].
- Present-empty guard rollout (#42) → `memory/archive/work-item-present-empty-guard-rollout.md` [DORMANT, archived 2026-11-24; reopens on the next graced-empty run that publishes].

## Current State

- **Pipeline**: daily cron `23 13` UTC since 2026-11-24 (was `0 16`, started 4–11h late in Nov) + `workflow_dispatch`. Last failures: 11-20 `44829093119` (Open-Meteo daily-quota 429 → weather feeds empty → drift gate, H10), 11-17 `44197847187` (#79, backfilled `5f19eca0`). 11-23 slot published entsoe at 96 points (late run past UTC midnight, H11).
- **Harness**: framework agent-ready-projects v1.50.0 (2026-10-28). `Reviewer model` declined; reason in `memory/project_active_decisions.md`. Session: `memory/archive/project_session_2026_10_28.md`. `CLAUDE.md` 13.8k chars after read-surface pass 2.
- **Feeds**: 20 in the committed baseline <!-- verify: n=$(git show origin/main:data/_shape_signatures.json | python -c "import json,sys; print(len(json.load(sys.stdin)['feeds']))"); echo "committed baseline carries $n feeds (claim 20)"; [ "$n" -eq 20 ] || exit 1 -->. Schema 2.4.
- **Gates**: completeness, quality (`critical` blocks), schema drift (blocks, runs before publish), span (warn + alert, #53). **Do not add a second blocking gate** — see CLAUDE.md CI/CD row.
- **Open issues**: 25 on 2026-11-24 <!-- verify: n=$(gh issue list --state open --limit 500 --json number | jq length); echo "open=$n (claim 25 on 2026-11-24)"; [ "$n" -eq 25 ] || exit 1 -->. Auto-alert issues self-close on recovery: #100 (span shortfall, 11-23 late run) is open. Per-issue narrative before 10-05: `memory/archive/current-state-2026-10-05.md`.
- **Known degraded**: `grid_imbalance` balance_delta synthesised (TenneT endpoint 404 since 2026-06-08). Weekend market staleness fixed by the 96h override (`2fdad76`, #36 closed, H4 resolved).
