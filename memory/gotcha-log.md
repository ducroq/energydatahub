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

### Encrypted data files can't be read back for accumulation (2026-03-27)
**Problem**: Market history accumulation needs to read the previous `market_history.json`, but published files are encrypted.
**Root cause**: `save_data_file()` encrypts when `encryption=True`. The non-timestamped copy in `data/` is also encrypted.
**Fix**: The accumulation step reads from `data/market_history.json` before it's overwritten. On first run it starts empty. In CI, the `data/` dir is checked out from git, so previous encrypted files are present — but they're encrypted strings, not dicts. The code handles this by checking `isinstance(raw, dict)` and falling back to empty history if encrypted.

### ENTSO-E API returns different column formats per country (2026-03-27)
**Problem**: `EntsoeGenerationCollector._parse_response()` matches columns by lowercase name (e.g., "nuclear" in column name). Some countries return MultiIndex DataFrames, others return flat columns.
**Root cause**: The `entsoe-py` library normalizes differently depending on what the ENTSO-E API returns per country.
**Fix**: The existing collector handles both Series and DataFrame with `isinstance` checks. When adding generation mix for NL/DE/BE, same logic applies — no code change needed, just awareness.

### Optional result unpacking uses fragile index counting (2026-03-27)
**Problem**: Adding a new fixed task to `asyncio.gather()` requires updating the slice index (e.g., `results[:14]` -> `results[:15]`) and `optional_idx`. Easy to miscount.
**Root cause**: Results are unpacked positionally from a flat list. Optional collectors are appended conditionally, making the index depend on which optionals are enabled.
**Fix**: Bumped index from 14 to 15 when adding generation mix. Future improvement: consider using a dict-based result collection pattern instead of positional unpacking.
