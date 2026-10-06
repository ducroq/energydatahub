---
stack: Python 3.12, asyncio/aiohttp, pandas, GitHub Actions CI/CD
status: Production (daily automated collection since Oct 2024)
repo: github.com/ducroq/energydatahub
framework: agent-ready-projects v1.49.2   # a NUMBER, not a status — never write "current" here; the framework's release cadence falsifies the adjective, not the pin
---

# energyDataHub

Automated energy market data collection platform for electricity price prediction at HAN University of Applied Sciences. Collects from 15+ APIs (ENTSO-E, NED, weather, gas/carbon markets) into encrypted JSON, published via GitHub Pages for downstream ML consumers (Augur).

<!-- Thinned 2026-10-06 (read-surface pass 2). The pre-thinning text, with the incident
     history behind each rule, is verbatim in memory/archive/claude-md-2026-10-06.md. -->

## Before You Start

| When | Read |
|------|------|
| Picking up where the last session left off | `memory/MEMORY.md` — **the index itself, not the topic files it lists**. Nothing loads it automatically; this row is the only thing that does, so keep it near the top. |
| Starting any session | Run `/update-drift` — triages framework releases since the stamp; stops before editing anything normative. |
| Adding a new collector | `collectors/base.py` (pattern), `collectors/entsoe_generation.py` (good example), `collectors/entsoe_hydro.py` (minimal). Use `raise_if_permanent` from `collectors/_http_classifier.py` in `_fetch_raw_data`. **If the host is already hit by another collector, pass `host_breaker_key`** — per-instance breakers cannot see a host-wide outage. |
| Adding a published dataset | `memory/project_published_dataset_checklist.md` — 8 touchpoints across `data_fetcher.py`, `utils/data_quality.py` and the workflow. **Missing one silently breaks publishing.** |
| Changing data output format | `utils/data_types.py`, `utils/schema_registry.py`. **Any shape change = bump `CURRENT_SCHEMA_VERSION` + add `_migrate_X_to_Y` + a SCHEMA_CHANGELOG entry**; `scripts/detect_schema_drift.py` enforces it. |
| Modifying CI/CD pipeline | `.github/workflows/collect-data.yml`. The drift tripwire runs BEFORE publish, so its exit 1 costs the whole day's publish. **Do not add a second blocking gate**; new detection lands as warn + alert. A drift-gate time box and a 90% volatility floor were both measured and declined — do not rebuild either (`memory/archive/project_session_2026_09_04.md`). Pages deploy is owned by this workflow (`docs/CI_CD_SETUP.md`). |
| Adding or changing a gate | Name which of {presence, structure, span, values} it covers: `utils/data_quality.py` = presence + values, `utils/shape_signature.py` = structure, `utils/span_signature.py` = span. 96 and 192 records hash identically. Decide detection and blocking separately. |
| Debugging data quality issues | `utils/data_quality.py` — `DATASET_MISSING_SEVERITY` is the single severity registry. Upstream-empty critical feeds downgrade to warning; present-but-empty feeds in `PRESENT_EMPTY_GRACE_FEEDS` route through the missing path via `present_empty_feeds()`, which **counts points** (not `not ds.data`). Both graces escalate after `UPSTREAM_EMPTY_ESCALATION_RUNS`=3 consecutive runs (`data/_upstream_empty_streak.json`). Membership is the failure mode, not the vendor. |
| Working with encryption/publish | `utils/secure_data_handler.py`, `docs/CI_CD_SETUP.md` |
| Stuck or debugging something weird | `memory/gotcha-log.md`; resolved entries in `memory/archive/gotcha-log-archive.md` |
| About to act on "it's probably transient" / "that's probably safe" | `memory/hypothesis-log.md` — open positions and the method that would settle each |
| Picking up work that spans sessions | `memory/work-items/` — create one at the *start* of anything spanning >2 sessions (`memory/work-items/README.md`) |
| Before committing | Run `/review-changes` (user-global skill; this repo's half is `.claude/review-profile.md`) |
| Bumping the published data schema | Run `/release` — stops before the run that publishes. User-typed only. |
| Ending a session | Run `/curate` |
| Monthly or after major restructuring | Run `/audit-context` |

## Hard Constraints

- **Lead with the point; no narrative.** Each entry in this file is a fact or a rule in a line or two. How it got that way goes in `memory/` or `git log`: this file is loaded every session.
- All timestamps normalized to Europe/Amsterdam timezone
- All published data encrypted with AES-CBC + HMAC-SHA256 (keys in secrets.ini / GitHub Secrets)
- Never commit secrets.ini or API keys — use environment variables in CI (enforced by GitHub secret-scanning push protection since 2026-06-14; secrets.ini is gitignored)
- Schema changes must be backward-compatible (see `utils/schema_registry.py` migration chain)
- Collectors must inherit from BaseCollector — provides retry, circuit breaker, validation
- Never claim tests pass without running them (`venv/bin/python -m pytest tests/ -x`). Use `venv/bin/python` unless the venv is activated — the system interpreter lacks `pytest-cov` and `pytest.ini`'s `addopts` makes it fail with `unrecognized arguments: --cov=.`, which looks like a broken config rather than a missing dependency. The venv is uv-managed and has no `pip`; install with `uv pip install --python venv/bin/python -r requirements.txt`.
- **Tests are not modified to make them pass.** If a test is wrong, say so and stop. This exists because `.claude/hooks/verify_edit.py` puts a failing test in front of the agent after an in-scope edit, and that pressure is exactly what produces a loosened assertion or a `@pytest.mark.skip`. The single documented exception is a schema bump updating a version literal — see `/release`, which spells out exactly how narrow that carve-out is.
- **The hook is a backstop, not a guarantee.** It fires on `Edit`/`Write`/`MultiEdit` only, so a file rewritten through Bash (`sed -i`, a heredoc, `patch`, `git checkout`) is never verified. It covers `collectors/ utils/ scripts/ tests/ data_fetcher.py` + workflow YAML and nothing else — notably not `pytest.ini`, `requirements.txt`, `tests/conftest.py` (added 2026-09-03 — it resets the process-wide host-breaker registry between tests, so a break there silently un-isolates every test that touches an ENTSO-E collector), or the hook itself. And a file with no mapped test falls back to the full unit suite, which may not import it at all — `utils/secure_data_handler.py` was that case until it got `tests/unit/test_secure_data_handler.py` on 2026-08-08. Exit 0 from the hook is never a coverage claim. Run the full suite before committing.

## Architecture

Module docstrings carry the detail and the history; this is the map.

```
data_fetcher.py              # Orchestrator: collectors via async gather, save, shape-signature sidecar
collectors/
  base.py                    # BaseCollector: retry, circuit breaker, validation. NonRetryableError
                             # (permanent), UpstreamNoDataError (source healthy, no data — no breaker
                             # trip). Use `_add_quality_issue()`; don't roll your own.
  _host_breaker.py           # Process-wide breaker keyed by HOST (#52), opt-in via host_breaker_key
  _http_classifier.py        # raise_if_permanent: 422/400/401/403/404 → NonRetryableError
  _entsoe_shared.py          # Per-zone delivery tracking, measured on PARSED data → zone_completeness
  _openmeteo_shared.py       # Shared semaphore, per-location retry, 429 budget + module-wide cooldown;
                             # metadata publishes DELIVERED locations, never configured ones
  entsoe*.py                 # ENTSO-E family (prices, wind, flows, load, generation, hydro)
  energyzero.py / epex.py / elspot.py   # Day-ahead prices
  tennet.py / ned.py / market_proxies.py / gie_storage.py / entsog_flows.py / luchtmeetnet.py
  openmeteo_weather.py / openmeteo_solar.py / openmeteo_offshore_wind.py
  googleweather.py / openweather.py / meteoserver.py   # RETIRED — kept for cold revert
utils/
  data_types.py              # EnhancedDataSet / CombinedDataSet — the {metadata, data} envelope
  data_quality.py            # FMEA validation; severity registry; present-empty grace
  schema_registry.py         # Version detection + migration chain (currently v2.4)
  shape_signature.py         # STRUCTURE fingerprint + append-only observation log. The sidecar is the
                             # BASELINE (advances on pass); the .jsonl is HISTORY (every run). Don't conflate.
  span_signature.py          # SPAN fingerprint: per-member day counts, shortfall-only, reports its denominator
  secure_data_handler.py     # AES-CBC + HMAC-SHA256
  calendar_features.py       # Holiday/DST features
scripts/
  detect_schema_drift.py     # Drift tripwire. Within-feed drift fails; catalog drift, volatile feeds and
                             # member drift (MEMBER_MAPPED_FEEDS) warn; CRITICAL_FEEDS never downgrade;
                             # diagnostic-only diffs are not drift
  report_span_shortfall.py   # Span check — ALWAYS exits 0, an alarm not a gate
  backfill_missed_run.py     # Re-collect the six one-day feeds of an unpublished run (`git add -f` output)
  backfill_entsoe.py         # BROKEN on v2.2+ files (#57) — dry-run only
  archive_to_monthly.py / backfill_gas_storage.py / sample_observed_ranges.py
  decrypt_file.py / batch_decrypt.py / visualize_data.py   # local decrypt + plotting utilities
  smoketest_alerting.sh      # Six-link alerting smoke test against the real repo, self-cleaning
  probe_*.py                 # One-shot diagnostics; probe_openmeteo_concurrency.py goes with H10
data/                        # yymmdd_HHMMSS_*.json + current copies + committed sidecars:
                             # _shape_signatures.json, _shape_observations.jsonl,
                             # _upstream_empty_streak.json, _span_shortfalls.json
docs/                        # GitHub Pages PUBLISH ROOT — served verbatim. No internal notes here.
storage/                     # UNWIRED Google Drive archiver — nothing imports it; do not assume it works
legacy/                      # Retired code kept for cold revert
memory/                      # Tracked agent memory: MEMORY.md index, gotcha/hypothesis logs, topic
                             # files, work-items/, archive/ (not read by default)
.claude/                     # settings.json (hook wiring), hooks/verify_edit.py, review-profile.md,
                             # skills/release/. review-changes, curate, audit-context, update-drift
                             # are user-global.
.github/
  dependabot.yml             # Weekly bumps for the SHA-pinned actions
  workflows/
    collect-data.yml         # Daily collect → completeness → quality gate → observation commit →
                             # drift gate → span check → publish → Pages deploy (3-attempt retry) → alert
    test.yml                 # PR/push tests (Python 3.12)
    alerting-selfcheck.yml   # Manual: smoketest with secrets.PAT. Run after rotating the PAT.
    openmeteo-probe.yml      # Manual H10 probe. Not within ~30 min of 16:00 UTC. Delete with H10.
```

## Key Paths

| Path | What it is |
|------|-----------|
| `settings.ini` | Public config (location, encryption flag) |
| `secrets.ini` | API keys (gitignored) |
| `tests/backtest_data_quality.py` | FMEA quality framework over all historical files |
| `tests/` | Unit + integration tests <!-- verify: venv/bin/python -m pytest tests/ --collect-only -q \| tail -1 --> |
| `.claude/hooks/verify_edit.py` | Compile check + mapped unit tests; exit 2 + stderr on failure. Needs PyYAML for the workflow branch. <!-- verify: echo '{"tool_input":{"file_path":"collectors/base.py"}}' \| .claude/hooks/verify_edit.py; echo $?   # expect 0 --> |

## How to Work Here

```bash
# Run all tests
venv/bin/python -m pytest tests/ -x

# Run specific test file
venv/bin/python -m pytest tests/unit/test_base_collector.py -v

# Run data collection locally (needs secrets.ini)
venv/bin/python data_fetcher.py

# Backfill the one-day feeds of a run that failed to publish (then: git add -f data/<yymmdd>_235959_*.json)
venv/bin/python scripts/backfill_missed_run.py --run-date 2026-10-04 --dry-run

# Archive decrypted data into 05. Data/<YYYY-MM>/ (idempotent)
venv/bin/python scripts/archive_to_monthly.py --since 260201

# Run data quality backtest on historical files
venv/bin/python tests/backtest_data_quality.py

# Check schema drift locally (after a `venv/bin/python data_fetcher.py` run)
venv/bin/python scripts/detect_schema_drift.py --previous-ref HEAD --warn-only

# Check GitHub Actions status
gh run list --limit 5

# Trigger a manual collection run.
# ⚠️ NOT before ~midday UTC: tomorrow's day-ahead auction has not cleared, so `entsoe`
# ships same-day only (96 points, not 192), and a half-size publish is WORSE for Augur
# than none (#74). To recover a failed publish, wait for the scheduled run.
gh workflow run "Collect and Publish Data"

# Exercise the verification hook. A healthy file exits 0 and prints nothing — that is the pass:
echo '{"tool_input":{"file_path":"collectors/base.py"}}' | .claude/hooks/verify_edit.py; echo $?

# Then confirm it still FAILS on a real break (exit 2 + stderr) — exit 0 is also what a disabled hook looks like:
cp collectors/base.py /tmp/base.bak && printf '\nnot valid python(\n' >> collectors/base.py
echo '{"tool_input":{"file_path":"collectors/base.py"}}' | .claude/hooks/verify_edit.py; echo $?
cp /tmp/base.bak collectors/base.py && rm /tmp/base.bak   # always restore

# Re-derive the user-global skills after pulling agent-ready-projects
cd ~/repos/agent-ready-projects && ./scripts/install-global-skills.sh --check ~/repos
```

## Commit Conventions

Imperative mood, concise. Examples from history:
- `Add data quality framework, schema registry, and DST-aware calendar features`
- `Update energy data`
- `Fix EnergyZero hour-00 edge case`
