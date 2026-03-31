# Gotcha Log

<!-- Structured problem/solution journal. Append-only.
     Part of the self-learning loop: Capture -> Surface -> Promote -> Retire.

     PROMOTION LIFECYCLE:
     - New entries start here (Capture phase)
     - At end-of-session, review for patterns (Surface phase)
     - When an entry recurs 2-3 times, promote to topic file (Promote phase)
     - When root cause is fixed, mark [RESOLVED] (Retire phase) -->

## Promoted

<!-- Track gotchas promoted to topic files or memory index.

| Entry | Promoted to | Date |
|-------|------------|------|

-->

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

### ENTSO-E API returns different column formats per country (2026-03-27)
**Problem**: `EntsoeGenerationCollector._parse_response()` matches columns by lowercase name (e.g., "nuclear" in column name). Some countries return MultiIndex DataFrames, others return flat columns.
**Root cause**: The `entsoe-py` library normalizes differently depending on what the ENTSO-E API returns per country.
**Fix**: The existing collector handles both Series and DataFrame with `isinstance` checks. When adding generation mix for NL/DE/BE, same logic applies — no code change needed, just awareness.

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
