# Gotcha Log — Archive

<!-- Historical [RESOLVED] entries moved here from gotcha-log.md to keep
     the active log focused on recent + unresolved incidents.

     Archived entries are NOT auto-loaded. Reach them by:
     - grep'ing the file for a symptom that matches a current bug
     - reading sequentially when investigating a recurring API/feed
     - consulting before designing a fix that may have a precedent here

     ARCHIVE CRITERIA (applied 2026-06-08 audit):
     - Pre-2026-05-01 [RESOLVED] entries
     - Pattern was either promoted to MEMORY.md Active Decisions, or
       fix is in code/commit history with a clear lesson

     Entries are kept verbatim — do not summarise or rewrite. -->

## Archived 2026-06-08

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

### Blanket `.claude/` gitignore hides project-shared skills (2026-05-28) [RESOLVED]
**Problem**: `.gitignore` had `.claude/` which silently excluded `.claude/skills/curate/SKILL.md` from version control. The skill is project-shared infrastructure (every contributor and future session needs it), not local config — but it was treated the same as `.claude/settings.local.json`.
**Root cause**: When the framework was first adopted (v1.3.4), the gitignore convention from generic Claude Code project setups was copied verbatim. The distinction between `.claude/skills/` (shared) and `.claude/settings.local.json` (per-user) wasn't made explicit.
**Fix**: Replaced `.claude/` with `.claude/*` + `!.claude/skills/`. The skills directory is now tracked; local settings remain ignored.

### agent-ready-projects framework drift went undetected for 2 months (2026-05-28) [RESOLVED]
**Problem**: Project was pinned to `agent-ready-projects: v1.3.4` (adopted 2026-03-29). Framework had advanced to v1.10.0 (2026-05-11) with 7 minor releases adding doc sync, freshness checks, audit-context skill, ADR template, frontmatter, self-verifying memory, etc. None of this was adopted because nothing prompted a check.
**Root cause**: The version line in CLAUDE.md was inert metadata — no instruction told any agent to check it against the changelog. The framework's `adopt.md` "Update" prompt required manual paste, which doesn't happen on its own.
**Fix**: (1) Upgraded to v1.10.0 — frontmatter, both skills (curate refreshed + audit-context new), ADR restructure, MEMORY.md refresh. (2) Added "Starting any session → compare framework version against CHANGELOG" row to CLAUDE.md's *Before You Start* table — this is the v1.10.0 fix for exactly this kind of drift.
