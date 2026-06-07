# Gotcha Log

<!-- Structured problem/solution journal. Append-only.
     Part of the self-learning loop: Capture -> Surface -> Promote -> Retire.

     PROMOTION LIFECYCLE:
     - New entries start here (Capture phase)
     - At end-of-session, review for patterns (Surface phase)
     - When an entry recurs 2-3 times, promote to topic file (Promote phase)
     - When root cause is fixed, mark [RESOLVED] (Retire phase) -->

## Promoted

| Entry | Promoted to | Date |
|-------|------------|------|
| Multi-collector concurrent → upstream rate-limit / CDN cascades (5-incident pattern: ENTSO-E 503, Luchtmeetnet 429, Open-Meteo 429, Open-Meteo CDN cooldown, TenneT 422→429) | `memory/MEMORY.md` → Active Decisions | 2026-06-07 |

### ENTSO-E API 503 caused silent data degradation (2026-03-27) [RESOLVED]
**Problem**: ENTSO-E API returned 503 on 2026-03-26. Price files lost `entsoe` and `entsoe_de` keys (27KB → 7.8KB). Augur's consumer price forecast broke completely. Scan revealed 123 degraded files (43% of all price data) going back to Sep 2025.
**Root cause**: `CombinedDataSet.add_dataset()` silently returned on `None` datasets. `data_fetcher.py` saved degraded files without warning. Workflow reported success.
**Fix**: (1) `add_dataset()` now logs WARNING on None datasets. (2) Critical collectors (`entsoe`, `entsoe_de`) retry up to 3 rounds × 5 min delay. (3) Workflow exits non-zero if critical data still missing after retries. (4) Backfilled 100 files with `scripts/backfill_entsoe.py` (2026-03-28).
**Negative result**: The BaseCollector's built-in retry (3 attempts, 1-60s backoff) is too fast for API-wide outages — the API was down for the entire collection window. Longer delays between retry rounds were needed.
**Remaining**: 26 early files (Sep-Oct 2025) have malformed timestamps in non-ENTSO-E datasets, preventing re-save.

### Encrypted data files can't be read back for accumulation (2026-03-27) [RESOLVED]
**Problem**: Market history accumulation needs to read the previous `market_history.json`, but published files are encrypted.
**Root cause**: `save_data_file()` encrypts when `encryption=True`. The code used raw `json.load()` on the encrypted file.
**Fix**: (2026-03-30) Replaced raw `json.load()` with `load_data_file(handler=handler)` which auto-detects encrypted vs plain JSON and decrypts as needed. Market history now accumulates correctly across runs.

### ENTSO-E API returns different column formats per country (2026-03-27) [RESOLVED]
**Problem**: `EntsoeGenerationCollector._parse_response()` matches columns by lowercase name (e.g., "nuclear" in column name). Some countries return MultiIndex DataFrames, others return flat columns.
**Root cause**: The `entsoe-py` library normalizes differently depending on what the ENTSO-E API returns per country.
**Fix**: The existing collector handles both Series and DataFrame with `isinstance` checks. When adding generation mix for NL/DE/BE, same logic applies — no code change needed, just awareness. Marked resolved 2026-06-07 (curate): the code is in place and has shipped reliably; the awareness item belongs in code comments, not the gotcha log.

### GIE gas storage timestamps were integer indices (2026-03-31) [RESOLVED]
**Problem**: 75 of 90 historical gas_storage files had integer keys ('0', '1', '2') instead of ISO timestamps. Data values were intact but unusable without proper time alignment.
**Root cause**: `pd.concat(ignore_index=True)` in `_fetch_raw_data` dropped the `gasDayStart` index. `_parse_response` fell back to `str(_)` which gave integer indices.
**Fix**: (1) Collector fix: preserve `gasDayStart` index in concat. (2) Backfill: `scripts/backfill_gas_storage.py` reconstructed timestamps from metadata `start_time` + daily offset. 75 files patched, 0 errors. Script is idempotent.

### Per-item ENTSO-E retries were missing (2026-03-30) [RESOLVED]
**Problem**: ENTSO-E 503 errors on individual borders/countries were not retried. BaseCollector's `_retry_with_backoff` wraps the entire `_fetch_raw_data`, but inside the loop, individual failures were caught and swallowed.
**Root cause**: Collectors loop over borders/countries and catch exceptions per-item to continue with others, bypassing the outer retry mechanism.
**Fix**: Added `BaseCollector._retry_single()` — retries individual sub-requests (3 attempts, 2s initial backoff). Applied to entsoe_flows, entsoe_wind, entsoe_load, entsoe_generation. Result: cross-border flows went from 4/10 borders to 10/10 on a run with ENTSO-E instability.

### Unicode arrow broke Windows console logging (2026-03-30) [RESOLVED]
**Problem**: `entsoe_flows.py` uses `→` (U+2192) in border names (data keys). Windows cp1252 console can't encode it, causing every flow log line to throw `UnicodeEncodeError`. Data collection still worked, but logs were unreadable.
**Fix**: Configured logging StreamHandler with UTF-8 encoding in `data_fetcher.py`. Border names kept as `→` to preserve backward compatibility with historical data files.
**Negative result**: Initially replaced `→` with `->` in data keys, which would have broken schema compatibility with 145+ historical cross-border flow files. Reverted.

### Optional result unpacking uses fragile index counting (2026-03-27)
**Problem**: Adding a new fixed task to `asyncio.gather()` requires updating the slice index (e.g., `results[:14]` -> `results[:15]`) and `optional_idx`. Easy to miscount.
**Root cause**: Results are unpacked positionally from a flat list. Optional collectors are appended conditionally, making the index depend on which optionals are enabled.
**Fix**: Bumped index from 14 to 15 when adding generation mix. Future improvement: consider using a dict-based result collection pattern instead of positional unpacking.

### Blanket `.claude/` gitignore hides project-shared skills (2026-05-28) [RESOLVED]
**Problem**: `.gitignore` had `.claude/` which silently excluded `.claude/skills/curate/SKILL.md` from version control. The skill is project-shared infrastructure (every contributor and future session needs it), not local config — but it was treated the same as `.claude/settings.local.json`.
**Root cause**: When the framework was first adopted (v1.3.4), the gitignore convention from generic Claude Code project setups was copied verbatim. The distinction between `.claude/skills/` (shared) and `.claude/settings.local.json` (per-user) wasn't made explicit.
**Fix**: Replaced `.claude/` with `.claude/*` + `!.claude/skills/`. The skills directory is now tracked; local settings remain ignored.

### agent-ready-projects framework drift went undetected for 2 months (2026-05-28) [RESOLVED]
**Problem**: Project was pinned to `agent-ready-projects: v1.3.4` (adopted 2026-03-29). Framework had advanced to v1.10.0 (2026-05-11) with 7 minor releases adding doc sync, freshness checks, audit-context skill, ADR template, frontmatter, self-verifying memory, etc. None of this was adopted because nothing prompted a check.
**Root cause**: The version line in CLAUDE.md was inert metadata — no instruction told any agent to check it against the changelog. The framework's `adopt.md` "Update" prompt required manual paste, which doesn't happen on its own.
**Fix**: (1) Upgraded to v1.10.0 — frontmatter, both skills (curate refreshed + audit-context new), ADR restructure, MEMORY.md refresh. (2) Added "Starting any session → compare framework version against CHANGELOG" row to CLAUDE.md's *Before You Start* table — this is the v1.10.0 fix for exactly this kind of drift.

### Coverage gate failed spuriously on filtered test job after PR #16 merge (2026-06-07) [RESOLVED]
**Problem**: After PR #16 merged, the Tests workflow's "Run critical timezone tests only" job failed with `Required test coverage of 20% not reached. Total coverage: 19.50%`. Generated an email notification even though HEAD of main was green (subsequent PR #18 + PR #20 merges added covered code and pushed coverage back over threshold).
**Root cause**: `pytest.ini` has `--cov-fail-under=20` which applies to *every* pytest invocation. The filtered job runs ~19 timezone-marked tests via `-m critical` and deselects ~400 others, so measured global coverage is always near-threshold; any small uncovered addition tips it under.
**Fix**: PR #22 — added `--no-cov` to the filtered job's pytest invocation. The full-suite job continues to enforce the 20% gate.

### Shared OpenMeteo Semaphore(5) caused buurt collector timeouts (2026-06-07) [RESOLVED]
**Problem**: After PR #18 merged (shared `Semaphore(5)` across 6 OpenMeteo collectors), two consecutive workflow_dispatch runs (27086755763, 27087174593) failed identically: late-scheduled collectors (offshore wind + both buurts) consistently got `Connection timeout to host api.open-meteo.com` while early strategic/demand collectors succeeded. The data-quality gate correctly blocked publish, but air_quality_buurt envelope verification was blocked.
**Root cause**: Open-Meteo's CDN applies a per-source connection-cooldown window after a request burst. With cap=5 < 6 collectors, 28 location requests queued FIFO behind the shared semaphore; the last ~7 (offshore + buurt) arrived during the cooldown. Pre-#11 the 6 collectors had their own `Semaphore(1)` each — 6 parallel sessions instead of one serialised queue — which avoided the trigger.
**Fix**: PR #23 — bumped `OPENMETEO_SEMAPHORE_CAP` from 5 → 6 (== collector count), restoring the effective pre-#11 concurrency pattern. Also added `fetch_location_with_retry` with exponential backoff (1s → 2s → 4s, max 3 attempts) per location for belt-and-suspenders resilience against any future transient.
**Lesson**: When sharing a budget across N independent consumers, FIFO < N means the late arrivals can hit upstream throttling that the early ones triggered. Cap should equal consumer count unless the consumers' arrival pattern is interleaved by design.

### Worktree path mismatch broke 4 of 12 reviewer agents (2026-06-07)
**Problem**: Created git worktrees at `/tmp/wt-prN` via bash for the multi-model review battery on 4 PRs. 4 of 12 spawned reviewer agents (Opus code-reviewer for PRs #16/#18/#19 + Sonnet for #16) returned "I cannot review code I cannot read" — their Read tool couldn't access the paths.
**Root cause**: On Windows, bash's `/tmp` maps to `C:\Users\<user>\AppData\Local\Temp` (a separate per-user temp dir, not the system `/tmp` that POSIX expects). The reviewer agents tried `/tmp/wt-prN` and got "not found"; only the agents that happened to try the Windows path succeeded.
**Fix (one-shot)**: Per-agent path translation worked for some agents; others gave up. For the failed ones, no review was produced — partial battery coverage.
**Lesson**: When briefing sub-agents that will use Read/Glob, give them OS-native absolute paths. On Windows, prefer `%LOCALAPPDATA%\Temp\...` or pass the diff inline rather than relying on `/tmp`. Better: have the orchestrator generate diffs and embed them in the agent prompts directly.
**Positive side-effect**: Three of the four agents that couldn't read the code refused to fabricate findings (cited the "When something is unclear, ask rather than guess" harness-defense rule), which is the desired behavior. The fix is upstream — don't put them in that position.

### Shallow `dict(issue)` mutated source collector metadata via nested `details` (2026-06-07) [RESOLVED]
**Problem**: PR #20's `assemble_buurt_air_envelope` did `tagged = dict(issue); tagged.setdefault('details', {})['location'] = loc['name']` to add a per-buurt location tag to aggregated quality issues. Two independent reviewers (Opus and Sonnet, separate models) flagged that the shallow copy shared the inner `details` dict with the source — adding `location` mutated the collector's own metadata.
**Root cause**: `dict(other_dict)` is a shallow copy. The top-level dict is new, but any nested mutable values (lists, dicts) are still shared references.
**Fix**: Use `copy.deepcopy(issue)` (or rebuild the dict explicitly with new inner dicts). Added a regression test `test_issue_details_location_tag_does_not_mutate_source` that builds an issue, runs the assembler, and asserts the original `details` dict doesn't gain a `location` key.
**Lesson**: When mutating a "copied" dict that came from a caller's state, default to `copy.deepcopy` unless you've audited every nested value. Multi-model review caught this where a single reviewer might have rationalised it as fine. Worth keeping the multi-model pattern for production data-flow code.
