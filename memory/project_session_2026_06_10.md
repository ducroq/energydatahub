# Session 2026-06-10 — Augur incident, schema-drift fail-mode flip, shape_signature date-only fix

## Trigger

Augur (downstream ML consumer agent) reported "EDH's ENTSO-E NL day-ahead price collector has been broken since ~2026-06-07 17:00 CEST". Its parquet was pinned at `2026-06-07 21:00Z`; ARF was empty; dashboard's 72h forecast envelope collapsed to a ~24h stub.

## Diagnosis correction

Augur's attribution ("ENTSO-E collector broken") was **wrong on cause, right on symptom**. ENTSO-E NL has been collected successfully on every run in the affected window. Every published file from `260607_170029_energy_price_forecast.json` onward contains 96 quarterly NL prices (or 192 on evening runs with D+1).

The real change: `CombinedDataSet.to_dict()` (`utils/data_types.py:111-125`) started wrapping combined feeds in the canonical `{metadata, data}` envelope in commit `3dfc7fb` (2026-06-07 12:43 CEST, schema bump v2.1 → v2.2). Augur's `parse_price_file` was written against pre-2.2 shape and silently rejects the new wrapper, pinning at the last accepted file.

Verified empirically by decrypting all 13 price files between 260607_073249 and the current `energy_price_forecast.json` — confirmed `entsoe` key present in every file with healthy data, just at `payload['data']['entsoe']` post-cutover instead of `payload['entsoe']`.

Methodology insight: **when a downstream agent reports "X is broken", inspect the published artifact directly before accepting the attribution.** Augur's defensive guard returned a coherent-looking diagnosis ("collector missing"); the truth was visible in 30 seconds of file decryption.

## Decision: hold the line, flip the tripwire

User picked **Option C** from a three-way fork:
- A: revert published envelope wrap (restore flat shape; loses 2.2 homogenization)
- B: dual-shape transitional output (publish both; clutters file, schedules cleanup that rarely happens)
- C: hold the new shape as contract; flip schema-drift tripwire to fail-mode; tell Augur to use `data.entsoe` or `read_json_file`

C honors EDH's own policy (which was followed correctly: bump + migration + changelog all shipped in `3dfc7fb`). Augur is responsible for tracking documented schema changes — fix lands at the consumer.

## Tripwire flip → immediate framework finding

Removed `--warn-only` from `scripts/detect_schema_drift.py` invocation in `.github/workflows/collect-data.yml` (commit `8e63148`). Dispatched workflow. **First fail-mode run failed** — caught a real framework gap that `--warn-only` had been silently absorbing:

`shape_signature._TS_PATTERN` (`utils/shape_signature.py:35`) was `r'^\d{4}-\d{2}-\d{2}T'` — only matched ISO-8601 timestamps **with the `T` separator**. Date-only keys (`'YYYY-MM-DD'`) fell through to per-key enumeration. Three dict locations in two feeds used rolling date windows:
- `market_proxies.gas_ttf.history` (25 dates)
- `market_history.carbon_eua.data` (8 dates)
- `market_history.gas_ttf.data` (75 dates)

Every day the window rolled (oldest dropped, newest added) → same structural shape → different signature → different hash → tripwire fired. False-positive class, not a real regression.

## Fix (commit `ce3dcaf`)

Broadened the regex to `r'^\d{4}-\d{2}-\d{2}([T ].*|$)'`. Matches `2026-06-10` (date only — the fix), `2026-06-10T13:00:00+02:00` (existing behavior preserved), and `2026-06-10 13:00:00` (defensive for space-separated ISO).

No `CURRENT_SCHEMA_VERSION` bump — only the introspection tool's semantics changed, not the published data shape.

Regenerated `data/_shape_signatures.json` from existing decrypted feed payloads in the same commit so the next CI run compared new-logic-vs-new-logic. Only the 2 affected feeds' hashes changed; the other 17 were byte-identical.

Tests added (`tests/unit/test_shape_signature.py`):
- `test_date_only_keys_collapse` — date-only keys produce `timestamp_map` signature
- `test_date_only_value_shape_change_still_detected` — collapse doesn't blind us to real shape drift inside
- `test_market_history_stable_across_days` — regression guard with realistic gas_ttf rolling-window payload

43 tests passing. Re-dispatched workflow — passed cleanly (run `27279671823`, exit 0, all 19 feeds unchanged, only operational catalog drift on `air_quality_buurt.json`).

## Augur recovery message

Drafted agent-to-agent message templating the corrected diagnosis + two patch options (use `utils.schema_registry.read_json_file` for full forward-compat, OR `get_collector(payload, name)` dual-path helper). User to relay to Augur. Augur's next scheduled fire (18:30 CEST) recovers automatically once parser is patched.

## Open items for next session

1. **Verify Augur's parquet advances past `2026-06-07T21:00Z`** after its next daily fire. If not, second issue exists.
2. **Watch tomorrow's EDH scheduled run** (2026-06-11 16:00 UTC) — first run where fail-mode tripwire sees a fully-rolled date window for `market_history.json` / `market_proxies.json` *without* the regeneration safety net. The math says it'll pass (timestamp_map collapse is date-independent), but empirical confirmation is still missing.
3. If either of those surfaces something new, escalate. Otherwise the loop is closed.

## Resolution (verified 2026-06-12) — loop closed

Both open items confirmed:

1. **Augur recovered.** Parser patch landed in augur `e11487b` (`fix(ml): unwrap EDH v2.2 {metadata, data} envelope in price + wind parsers`), plus follow-up hardening `c29671e` (parser tests + isinstance guards). Verified from augur's committed state (`C:\local_dev\augur`): `shadow_state.json` `last_run_utc=2026-06-11T18:45Z`, 120 pending predictions spanning `2026-06-12T22:00Z → 2026-06-15T21:00Z` (full 72h+ envelope restored, vs the ~24h stub during the incident), and `eval_log.jsonl` has realised-price eval rows for 2026-06-09 and 2026-06-11 — well past the `2026-06-07T21:00Z` pin. Daily commits 06-10/06-11 report `ARF OK | shadow rc=0/eval rc=0`.
2. **Fail-mode tripwire passed its first fully-rolled window.** Scheduled run `27369503754` (2026-06-11 18:42 UTC) exited clean: no within-feed shape drift on `market_history.json` / `market_proxies.json` after the date window rolled without the regeneration safety net. Only signal was operational catalog drift — `air_quality_buurt.json` *added* back (Luchtmeetnet buurt recovered), sidecar now 20 feeds.

No second issue surfaced. Incident fully closed.

## Commits this session

- `8e63148` — `ci(schema-drift): flip tripwire from --warn-only to fail-mode`
- `ce3dcaf` — `fix(shape_signature): collapse YYYY-MM-DD keys, not just ISO-T timestamps`
- `975af39` — `Update energy data` (auto-commit from the successful re-dispatch; first fail-mode-gated publish)

## Schema version: unchanged (2.4)
