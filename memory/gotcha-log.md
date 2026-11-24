# Gotcha Log

<!-- Problem/fix journal: Capture -> Surface -> Promote -> Retire.
     NEW ENTRIES: 2-3 lines, the lesson and the action — no session narrative.
     Entries live below the two tables; append new ones at the end of the file.
     PROMOTE when an entry recurs 2-3 times; keep counting in Occurrences after
     promotion and put `[xN]` in the entry heading — a promoted pattern that recurs
     means the promotion did not take.
     RETIRE: mark [RESOLVED] and move to `memory/archive/gotcha-log-archive.md`;
     also archive entries >60 days old whose lesson is promoted or now a check.
     MECHANIZE: a review finding a script could have caught goes in `## Mechanized`
     (proposed -> live); /curate reads both tables. -->

## Promoted

Pattern + count + destination. Incident history per row: `memory/archive/gotcha-promoted-history-2026-11-24.md`; add new dates there too.

| Pattern | Occurrences (last) | Promoted to |
|---------|-------------------|-------------|
| Many collectors on one host → rate-limit / CDN cascades. Remedy includes `host_breaker_key` | 6 (2026-09-03) | `memory/project_active_decisions.md` |
| Parallel hard-coded registries keyed on one identifier drift apart | 2 (2026-10-28, #91) | `memory/project_active_decisions.md` |
| Silent quality-gate skip. **A check reports its DENOMINATOR (`M of N judged`), and tests assert it.** Recurred after promotion 4× | 7 (2026-09-10) | `memory/project_active_decisions.md` |
| Warn-only bedding-in periods swallow real findings | 2 (2026-06-10) | `memory/project_active_decisions.md` |
| Data-driven within-feed shape churn → drift false positives; sampling fixed by `_merge_signatures`, cross-day churn by history-derived volatility. A field gone from a whole member is #79, not this | 7 (2026-11-17) | `memory/project_active_decisions.md` + `scripts/detect_schema_drift.py::derive_volatile_feeds` |
| Guard signal integrity: exercise known-good AND known-bad before trusting any guard or measurement — a grep and a `<!-- verify -->` probe ARE guards. Ablate (revert the predicate, delete the entry) and see red. Recurred after promotion | 4 (2026-09-10) | `memory/project_active_decisions.md` |
| History-derived classifiers learn only from recorded observations — and what the gate stops counting, the classifier keeps counting | 2 (2026-09-21) | `memory/project_active_decisions.md` + `memory/hypothesis-log.md` H7 |

## Mechanized

Review findings turned into deterministic checks (agent-ready-projects v1.41.0). `proposed` = named, not built; `live` = runs; `retired` = check removed, with why.

| Date | Finding | Check (what it greps/parses/runs) | Lands in | Status |
|------|---------|-----------------------------------|----------|--------|
| 2026-10-28 | Published-dataset lists drift apart silently (c40a53b, #3) | AST of `data_fetcher.main` + workflow YAML; asserts dated save, current copy, sidecar and both workflow lists agree; published ⊆ `quality_datasets` (2 named exemptions) ⊆ `EXPECTED_DATA_TYPE` | `tests/unit/test_published_feed_registries.py` | live (6 ablations red) |

### Re-fetching the window turns an opaque hash flip into a measured cause (2026-09-21)
`load_forecast` failed the tripwire with only two hashes to show for it. Re-fetching the same window from ENTSO-E a day later, then stripping candidate fields from the live payload and re-hashing, reproduced the exact failing hash and named both causes in ~10 minutes.
**Lesson**: the sidecar stores hashes, not reasons, but the upstream API still has the window. Reconstruct rather than infer — and a hash that matches byte-exactly is proof, where a plausible story is not. (#48 asks for the tripwire to report this itself.)

### A fake clock that sums sleeps makes concurrent retry tests pass anything (2026-10-05)
The first storm-replay test advanced one shared fake clock by every `asyncio.sleep`. Twenty-five concurrent locations therefore added their sleeps end to end and "outlasted" a 61 s storm under the OLD 3 s retry budget too. It passed on the pre-fix code, and only the deliberate run against `git show HEAD:` caught it. Rewritten on the real clock with every delay scaled down 100×.
**Lesson**: a fake clock is sound for ONE task. For concurrent tasks, scale real time instead, and always run a regression test against the unfixed code before trusting it.

### A late cron rolls the collection window past the slot's day (2026-10-06, again 2026-11-24)
GitHub starts the scheduled cron hours late. 10-05: 22:16Z = past Amsterdam midnight → `generation_forecast` empty, publish lost (`37381385635`); fixed with `COLLECTION_DATE` = UTC date at start. 11-23 slot: started 02:42Z on 11-24 → past UTC midnight too, so that fix collected 11-24..11-25 and entsoe published 96 points (`45314302508`, #100). Now `COLLECTION_DATE` = UTC date of (now − cron hh:mm), parsed from `github.event.schedule`; holds for lag < 24h (H11). Check `grep 'COLLECTION_DATE=' ` in the run log first.

### An index row describing a file is a state claim (2026-10-06)
`memory/MEMORY.md` said the gotcha log held "Promoted/Mechanized tables"; it never had a Mechanized table, and no check reads index descriptions. Found only by the drift triage's by-eye comparison. When an index row names a section of another file, either give it a `<!-- verify -->` grep or name the file only.

### Turning a checklist into a test inherits the checklist's blind spots (2026-10-28)
The #85 test first checked the lists the prose checklist named, and missed the one `docs/` actually reads: the copy to `data/<feed>.json`. The shrunk checklist also kept two steps that never existed in the code (`tasks.append`, a `fixed_count` unpack). Both were found by review, not by the test's own ablations, which only removed entries from lists already modelled. When mechanizing prose, trace from the consumer (what `docs/` copies) backwards, not from the prose forwards, and check every surviving prose step against the code.
