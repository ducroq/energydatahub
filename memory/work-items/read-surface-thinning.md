# Read-surface thinning — prune, mechanize, retire

## What & Why

Every session pays to read `CLAUDE.md` (auto-loaded) and `memory/MEMORY.md` (read
via the Before-You-Start row), and `/curate` measures the whole `memory/` corpus.
On 2026-10-05 these were well past their budgets, all measured in characters (`wc -m`):

| Surface | 2026-10-05 before | 2026-10-06 (after pass 2) | Target |
|---|---|---|---|
| `memory/` corpus (curate Step 0, excl. `archive/`) | 305,787 | 103,508 after pass 2 | < 300k hard, aim ~150k |
| `CLAUDE.md` (auto-loaded) | 37,636 | 13,764 after pass 2 | ≤ 15k flag; template ~6k |
| `memory/MEMORY.md` (read every session) | 40,123 | ~7.0k | ≤ ~8k |

The user asked for this directly: "Large Read surface → we need to start pruning,
thinning, mechanizing, retiring!" (2026-10-05).

## Current Status

**Pass 1 done 2026-10-05 (lossless moves only, nothing deleted):**
- Session retrospectives, `gotcha-log-archive.md` → `memory/archive/` (index in `memory/archive/README.md`).
- 25 `[RESOLVED]` gotcha entries → `memory/archive/gotcha-log-archive.md` (log 99.6k → 49.1k).
- Resolved hypotheses H1/H3/H5/H9 → `memory/archive/hypothesis-log-resolved.md` (log 46k → 33k).
- MEMORY.md Current State (20.7k of session narrative) → `memory/archive/current-state-2026-10-05.md`;
  Active Decisions (10.2k) → `memory/project_active_decisions.md` (on-demand topic file).

**Pass 2 done 2026-10-06:** `CLAUDE.md` 38,271 → ~13.8k chars (`wc -m`), under the 15k flag. Original
verbatim in `memory/archive/claude-md-2026-10-06.md`. Survival check run: every Hard Constraint line
`grep -F`-found, both `<!-- verify -->` probes kept and run (955 tests collected; hook exit 0).
Also: 7 gotcha entries archived (log 50.1k → 40.6k), H6/H8 dormant → archive (hypothesis log 31.7k → 24.0k),
both log headers thinned, `## Mechanized` table added (framework v1.41.0).

**Next action — mechanize: #85 first** (below). `memory/` corpus excl. archive is 103.5k chars on 2026-10-06, under the 300k hard limit; next targets are the three open hypotheses (~6k each). Pass 2 plan, kept for the record:
1. **Architecture block (~18.8k)** is per-file narrative: incident history, dates, run IDs.
   Cut each entry to *what the file is + the one rule you must not break*; move the history
   to the module docstrings (most already carry it) or `memory/archive/`. Do not lose any
   Hard Constraint clause, or any fact a `<!-- verify: -->` probe reads — list them first,
   `grep -F` each one after (curate Step 0.6 survival check).
2. **Before You Start table (~8.5k)**: the CI/CD, data-quality and review rows are
   paragraphs. Keep the pointer + the single "do not" sentence; the rest is in the files
   they point to.
3. **Key Paths** duplicates Architecture — merge into one.

**Then mechanize** (replace prose that asks the agent to remember with a check that fails):
- The 8-touchpoint published-dataset checklist → a unit test asserting every feed in
  `DATASET_MISSING_SEVERITY` appears in the workflow's publish list and `data_fetcher` save set.
- "Open issues: N" style counts → only as `<!-- verify -->` probes, never bare prose.
- Duplicated location/collector config (`scripts/backfill_missed_run.py` copies
  `data_fetcher.main()`'s `buurt_locations` + collector args) → hoist to module constants
  and import, or a test asserting they match.

**Then retire**: gotcha entries older than ~60 days whose lesson is now a check or a
promoted pattern; hypotheses that stay open past two review dates with no movement
(decide, or mark dormant).

## Decisions

- Pass 1 was moves only, so nothing could be lost. Pass 2 rewrites, so it needs the survival
  check before and after.
- Archive lives at `memory/archive/`, which curate's Step 0 measurement already excludes.

## Open Questions

- Is ~8k the right MEMORY.md ceiling, or should it be index-only (pointers, no state)?

## Outcome

_(fill when done)_
