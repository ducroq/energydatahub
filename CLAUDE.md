---
stack: Python 3.12, asyncio/aiohttp, pandas, GitHub Actions CI/CD
status: Production (daily automated collection since Oct 2024)
repo: github.com/ducroq/energydatahub
framework: agent-ready-projects v1.18.0
---

# energyDataHub

Automated energy market data collection platform for electricity price prediction at HAN University of Applied Sciences. Collects from 15+ APIs (ENTSO-E, NED, weather, gas/carbon markets) into encrypted JSON, published via GitHub Pages for downstream ML consumers (Augur).

## Before You Start

| When | Read |
|------|------|
| Starting any session | Run `/update-drift` — finds every framework stamp, lists the intervening releases, and triages each as adopt / decline-with-reason / not-applicable / already-in-force. It stops before editing anything normative; adopting is your call. (Was a manual CHANGELOG comparison until v1.18.0 made it a skill.) |
| Adding a new collector | `collectors/base.py` — BaseCollector pattern, `collectors/entsoe_generation.py` — good example, `collectors/entsoe_hydro.py` — minimal example. Also see `collectors/_http_classifier.py` for the HTTP-status bail-out pattern (raise_if_permanent) — use it from `_fetch_raw_data` to skip retries on permanent client errors (422/400/401/403/404). **If the host is already hit by another collector, pass `host_breaker_key`** so they share one circuit breaker (`collectors/_host_breaker.py`, #52) — per-instance breakers cannot see a host-wide outage. |
| Changing data output format | `utils/data_types.py` — EnhancedDataSet/CombinedDataSet, `utils/schema_registry.py` — versioning + migration chain. **Any shape change requires bumping `CURRENT_SCHEMA_VERSION` + adding a `_migrate_X_to_Y` function + a SCHEMA_CHANGELOG entry**. The CI tripwire (`scripts/detect_schema_drift.py`) enforces this. |
| Modifying CI/CD pipeline | `.github/workflows/collect-data.yml` — daily collection workflow. Includes completeness tripwire + schema-drift tripwire (fail-mode since 2026-06-10; auto-classifies data-volatile feeds from committed history since 2026-06-14). Actions are SHA-pinned + Dependabot-managed (`.github/dependabot.yml`). Owns the GitHub Pages deploy (source = "GitHub Actions"): a `deploy` job runs `actions/deploy-pages` with a 3-attempt retry for transient GitHub-side deploy faults — the auto `pages-build-deployment` workflow no longer runs (see `docs/CI_CD_SETUP.md`). |
| Working with encryption/publish | `utils/secure_data_handler.py`, `docs/CI_CD_SETUP.md` |
| Debugging data quality issues | `utils/data_quality.py` — FMEA validation. Per-dataset config via `get_dataset_validation_config()`. Missing-dataset severity via `DATASET_MISSING_SEVERITY` dict (single source of truth). A critical feed that is *upstream-empty* (source healthy, published no data for the window — `UpstreamNoDataError`) is downgraded `critical`→`warning` so the run still publishes the healthy feeds; the orchestrator passes `validate_pipeline(upstream_empty=…)` and only `SystemExit(1)`s on a genuine collector failure. A *sustained* gap (≥`UPSTREAM_EMPTY_ESCALATION_RUNS`=3 consecutive runs, tracked in the committed `data/_upstream_empty_streak.json`) escalates back to a hard failure so it can't degrade silently forever (#38). Separately, a *present-but-empty* dataset (collector returned a truthy `EnhancedDataSet` with `data={}` — e.g. an all-locations Open-Meteo timeout) would otherwise hard-fail the completeness gate (`validate_completeness` → `CRITICAL` on 0 points) and abort the whole publish. `data_fetcher` coerces such a feed to `None` so it routes through the (non-blocking) missing path instead — treating present-empty as absent. Applies to all six Open-Meteo feeds in `PRESENT_EMPTY_GRACE_FEEDS` (generalised from buurt-only on 2026-08-08, #42) and is **time-boxed**: after `UPSTREAM_EMPTY_ESCALATION_RUNS` consecutive empty runs the coercion stops and the gate fails the publish loudly, so a sustained outage cannot degrade silently. Streaks share `data/_upstream_empty_streak.json` with the #38 counters (disjoint keys). |
| Adding a published dataset | `memory/project_published_dataset_checklist.md` — 8-touchpoint checklist across `data_fetcher.py`, `utils/data_quality.py`, and `.github/workflows/collect-data.yml`. **Missing one silently breaks publishing** (BLOCKER on c40a53b). Read before wiring a new collector into the publish set. |
| Stuck or debugging something weird | `memory/gotcha-log.md` — problem-fix archive |
| About to act on "it's probably transient" / "that's probably safe" | `memory/hypothesis-log.md` — open positions (H1–H5) with the method that would settle each. Check before treating a recurring failure as known-benign; `/curate` surfaces overdue entries. |
| Picking up work that spans sessions | `memory/work-items/` — savepoints for in-flight work (what is decided, what is still open). Create one at the *start* of anything spanning >2 sessions; see `memory/work-items/README.md`. Distinct from `memory/project_session_*.md`, which are retrospectives. |
| Before committing | Run `/review-changes` — picks review lenses from what changed (one adversarial pass for a small diff, up to the full 5-lens battery for `collectors/`, `utils/`, `scripts/`, CI, `.claude/**`, `.gitignore`, or `settings.ini`). Project-local skill, never install it globally. |
| Bumping the published data schema | Run `/release` — classifies the bump, verifies preconditions, writes the SCHEMA_CHANGELOG entry, syncs version references, **stops before the run that publishes**. User-typed only. |
| Ending a session | Run `/curate` — reviews gotcha log, promotes patterns, syncs docs, surfaces stale memory. New gotcha entries are 2-3 lines: the lesson and the action, not the narrative of the session that found it. The 24 pre-2026-08-08 entries use an older four-field long form — read them, don't imitate them; the budget is restated in the log's own header. |
| Monthly or after major restructuring | Run `/audit-context` — structural audit (duplication, wrong-layer placement, broken refs) |

## Hard Constraints

- All timestamps normalized to Europe/Amsterdam timezone
- All published data encrypted with AES-CBC + HMAC-SHA256 (keys in secrets.ini / GitHub Secrets)
- Never commit secrets.ini or API keys — use environment variables in CI (enforced by GitHub secret-scanning push protection since 2026-06-14; secrets.ini is gitignored)
- Schema changes must be backward-compatible (see `utils/schema_registry.py` migration chain)
- Collectors must inherit from BaseCollector — provides retry, circuit breaker, validation
- Never claim tests pass without running them (`venv/bin/python -m pytest tests/ -x`). Use `venv/bin/python` unless the venv is activated — the system interpreter lacks `pytest-cov` and `pytest.ini`'s `addopts` makes it fail with `unrecognized arguments: --cov=.`, which looks like a broken config rather than a missing dependency. The venv is uv-managed and has no `pip`; install with `uv pip install --python venv/bin/python -r requirements.txt`.
- **Tests are not modified to make them pass.** If a test is wrong, say so and stop. This exists because `.claude/hooks/verify_edit.py` puts a failing test in front of the agent after an in-scope edit, and that pressure is exactly what produces a loosened assertion or a `@pytest.mark.skip`. The single documented exception is a schema bump updating a version literal — see `/release`, which spells out exactly how narrow that carve-out is.
- **The hook is a backstop, not a guarantee.** It fires on `Edit`/`Write`/`MultiEdit` only, so a file rewritten through Bash (`sed -i`, a heredoc, `patch`, `git checkout`) is never verified. It covers `collectors/ utils/ scripts/ tests/ data_fetcher.py` + workflow YAML and nothing else — notably not `pytest.ini`, `requirements.txt`, `tests/conftest.py` (added 2026-09-03 — it resets the process-wide host-breaker registry between tests, so a break there silently un-isolates every test that touches an ENTSO-E collector), or the hook itself. And a file with no mapped test falls back to the full unit suite, which may not import it at all — `utils/secure_data_handler.py` was that case until it got `tests/unit/test_secure_data_handler.py` on 2026-08-08. Exit 0 from the hook is never a coverage claim. Run the full suite before committing.

## Architecture

```
data_fetcher.py              # Main orchestrator — initializes collectors, runs async gather,
                             # writes data/_shape_signatures.json sidecar pre-encryption
collectors/
  base.py                    # BaseCollector ABC: retry, circuit breaker, validation,
                             # NonRetryableError for permanent failures (#25),
                             # UpstreamNoDataError (subclass) for "source healthy but
                             # published no data" — fast-fails, no circuit-breaker trip,
                             # sets last_run_no_upstream_data + CollectorStatus.NO_DATA (#38), and
                             # `_add_quality_issue()` hook + auto-reset in collect() +
                             # auto-deepcopy injection of metadata['collector_quality_issues']
                             # (refactoring H1, 4c59378). Use the hook — don't roll your own.
                             # `host_breaker_key` (opt-in) shares one circuit breaker across
                             # every instance hitting a host; `_retry_single` consults it and
                             # takes `non_host_exceptions` for "answered fine, no rows" (#52).
  _host_breaker.py           # PROCESS-WIDE circuit breaker keyed by HOST, shared across
                             # collector instances (#52). BaseCollector's own breaker is
                             # per-INSTANCE and consulted once per collect(), so it cannot
                             # see a host-wide outage: 8 ENTSO-E collectors hit one host and
                             # none ever reached the threshold (2026-08-31: ~295 requests
                             # into an API 503-ing every one). Counts EXHAUSTED sub-requests,
                             # not HTTP attempts; any success closes it; empty-window and
                             # permanent-4xx errors never count (a healthy host with unpublished
                             # cable borders would otherwise trip it). One probe per 60s
                             # cooldown. Terminal-for-the-run for the six zone/border
                             # collectors (single gather, no retry round); the two price
                             # collectors consult it inline and DO get 3 rounds 300s apart,
                             # so the critical feed keeps its recovery path.
  _http_classifier.py        # Shared HTTP status classifier (raise_if_permanent) for
                             # 422/400/401/403/404 → NonRetryableError. Used by tennet.py;
                             # available for adoption by any collector that hits 4xx cascades.
  _entsoe_shared.py          # Per-zone delivery tracking for the four country-keyed
                             # ENTSO-E collectors (load, generation, wind, hydro) —
                             # the zone-level analogue of _openmeteo_shared's
                             # record_location_delivery. record_zone_request() at the
                             # top of _fetch_raw_data, record_zone_delivery() from
                             # _validate_data. Measured against the PARSED data, not
                             # the fetch: all four gate the per-zone assignment on a
                             # truthiness check of the parsed records, so a zone can
                             # fetch and still vanish from `data` (added 2026-08-30
                             # after the 2026-08-29 ENTSO-E 503 outage produced an
                             # envelope claiming NL while `data` carried only DE_LU —
                             # nothing was published; the tripwire aborted first).
                             # Emits `zone_completeness`. Does NOT unblock the
                             # publish: on a CRITICAL_FEED a degraded run still
                             # aborts before the quality report is committed. The
                             # five zone-keyed feeds are individually accounted for
                             # in its "Deliberately NOT in scope" section.
  _openmeteo_shared.py       # Shared Semaphore + per-location retry/backoff for OpenMeteo*.
                             # Also record_location_delivery() / published_locations()
                             # (2026-08-14): a location whose fetch exhausts its retries
                             # drops out of `data`, so metadata publishes the DELIVERED
                             # set — never the configured one — and a `location_completeness`
                             # quality issue routes the dropout through the DQ gate into the
                             # committed quality report. Before this the envelope claimed
                             # locations `data` did not carry and only the drift tripwire
                             # noticed, by failing the whole publish.
  entsoe*.py                 # ENTSO-E family (prices, wind, flows, load, generation, hydro)
  entsoe_hydro.py            # Nordic hydro reservoirs (A72, weekly cadence, NO+SE) — #3 closed c40a53b
  energyzero.py / epex.py / elspot.py  # Day-ahead price collectors (NL/EU)
  tennet.py                  # TenneT TSO (imbalance prices, grid balance) — uses _http_classifier
  ned.py                     # NED.nl Dutch production
  market_proxies.py          # Carbon EUA + gas TTF prices
  openmeteo_weather.py       # Strategic + demand + buurt weather (replaces Google Weather)
  openmeteo_solar.py         # Strategic + buurt solar irradiance
  openmeteo_offshore_wind.py # Offshore wind farm forecasts (open-sea coords)
  luchtmeetnet.py            # Air quality (RIVM stations), buurt-level
  gie_storage.py             # Gas storage levels
  entsog_flows.py            # Gas pipeline flows
  googleweather.py / openweather.py / meteoserver.py  # RETIRED — kept for cold revert
utils/
  data_types.py              # EnhancedDataSet, CombinedDataSet — canonical {metadata, data} envelope
  data_quality.py            # FMEA validation. DATASET_MISSING_SEVERITY (single registry),
                             # PRESENT_EMPTY_GRACE_FEEDS (#42, the six Open-Meteo feeds
                             # eligible for the time-boxed present-empty grace),
                             # EXPECTED_DATA_TYPE (MITM defense), get_dataset_validation_config(),
                             # validate_load_cross_field_consistency (#30, ratio threshold 0.40),
                             # depth-walking _count_data_points / _extract_timestamp_keys (#32)
  schema_registry.py         # Version detection + migration (v1.0 → v2.0 → v2.1 → v2.2 → v2.3 → v2.4).
                             # stamp_metadata embeds the version's changelog slice (Layer B).
  shape_signature.py         # Structural fingerprint for schema-drift detection (#27).
                             # Also the append-only observation log (#43):
                             # append_shape_observation / volatile_feeds_from_observations.
                             # _shape_signatures.json is the tripwire's BASELINE (advances
                             # only on a passing run); _shape_observations.jsonl is the
                             # HISTORY the volatility classifier learns from (every run,
                             # pass or fail). Do not conflate them — that was the #43 bug.
                             # classify_data_member_drift: is a shape diff purely a
                             # member-set change of the data block (a location/source
                             # dropped or recovered, survivors identical)? Powers the
                             # member-drift downgrade in the tripwire (2026-08-14).
                             # A timestamp map's value_shape is the MERGE of ALL its
                             # records, not one sampled record (2026-08-23) — a field
                             # present in ANY record survives, so an intra-day
                             # completeness gap is no longer a shape change. A field
                             # gone from EVERY record still drifts. `_merge_signatures`
                             # documents the boundary; lists still sample element 0.
  secure_data_handler.py     # AES-CBC + HMAC-SHA256 encryption
  calendar_features.py       # Holiday/DST features
scripts/
  detect_schema_drift.py     # CI tripwire diffing data/_shape_signatures.json against HEAD (#27).
                             # Splits within-feed shape drift (fail) from catalog drift (warn)
                             # per the 2026-06-08 buurt-drift fix; CRITICAL_FEEDS escalates
                             # removed-critical-feed catalog drift to ::error::. Data-volatile
                             # feeds warn (not fail): membership = VOLATILE_SHAPE_FEEDS (declared
                             # seed/override) UNION derive_volatile_feeds() (auto-derived from
                             # committed shape history, so a recurring data-driven false positive
                             # self-classifies without an allowlist edit). --volatility-window N.
                             # Member drift (2026-08-14) warns too, behind TWO gates: the
                             # feed is declared in MEMBER_MAPPED_FEEDS (its data keys are
                             # location names) AND the diff is purely a member-set change
                             # with every member sharing one shape. The registry is load-
                             # bearing — grid_imbalance/market_history/ned_production key
                             # `data` by FIELD name, where a vanished key IS the break.
                             # Classified BEFORE volatility, and MEMBER_MAPPED_FEEDS are
                             # excluded from derived volatility, so the blunt rule cannot
                             # pre-empt the precise one. CRITICAL_FEEDS always fail.
  backfill_entsoe.py / archive_to_monthly.py / backfill_gas_storage.py
  sample_observed_ranges.py  # One-shot diagnostic: sample data/ files per feed, compute observed
                             # min/max per field. Used to derive #28's SOLAR_FIELD_RANGES /
                             # LOAD_FIELD_RANGES. Re-run when adding a new per-field range bound.
  probe_tennet_windows.py    # One-shot diagnostic: probe TenneT API across windows to identify
                             # endpoint availability. Used for #25 root-cause analysis.
  probe_openmeteo_concurrency.py  # One-shot diagnostic (H10/#58): replays the production
                             # OpenMeteo shape — 38 locations, MAX_RETRIES, the ungapped head
                             # burst — and reports the 429 rate plus the source address. Run it
                             # on a runner and locally to compare egress. LOWER-BOUND: both-clean
                             # is inconclusive. Always exits 0; never a gate. Delete with H10.
data/                        # Timestamped output (yymmdd_HHMMSS_*.json) + current copies +
                             # _shape_signatures.json sidecar (unencrypted, committed) +
                             # _shape_observations.jsonl learning record (#43, committed
                             # by its own workflow step BEFORE the drift gate so a failing
                             # run still teaches the classifier) +
                             # _upstream_empty_streak.json (#38 + #42 counters)
docs/                        # GitHub Pages PUBLISH ROOT — the whole directory is uploaded as
                             # the Pages artifact and served verbatim. Encrypted JSON +
                             # project documentation. Do not put internal notes here.
storage/                     # UNWIRED. Google Drive archiver (gdrive.py, 2025-10-25) —
                             # nothing imports it; the only reference is its own docstring
                             # example. Relevant to #9 (replace git-as-archive): a partial
                             # implementation already exists here. Do not assume it works.
legacy/                      # Retired code kept for cold revert. LOW risk tier in
                             # /review-changes.
run_script.sh                # Local convenience wrapper for a collection run.
memory/                      # Layered agent memory (tracked). MEMORY.md index, gotcha-log.md,
                             # hypothesis-log.md (open positions + revisit triggers),
                             # project_session_*.md retrospectives, project_*.md topic files.
  work-items/                # Savepoints for in-flight multi-session work (agent-ready-projects
                             # work-item template). Temporary — deleted once the Outcome's
                             # residue is promoted to an ADR / gotcha log / CLAUDE.md.
                             # Under memory/, NOT docs/ — a work item describes what is not
                             # yet fixed, and docs/ is world-readable (moved 2026-08-08).
.claude/                     # Agent harness config (committed). `curate`, `audit-context` +
                             # `update-drift` are user-global and deliberately NOT here.
  settings.json              # PostToolUse verification hook wiring
  hooks/verify_edit.py       # Runs py_compile + the unit tests mapped to the edited file, and
                             # exits 2 so the failure reaches the agent (exit 0 = silent hook).
                             # Scoped to collectors/ utils/ scripts/ tests/ data_fetcher.py
                             # + workflow YAML. Test mapping is derived by glob, not hand-listed.
  skills/review-changes/     # /review-changes — pre-commit lens battery (project-local by design:
                             # its risk tiers name files in this tree)
  skills/release/            # /release — published-schema version cut. User-typed only.
.github/
  dependabot.yml             # github-actions ecosystem, weekly grouped — auto-bumps the
                             # SHA-pinned action versions so the pins don't rot (added 2026-06-14)
  workflows/
    collect-data.yml         # Daily 16:00 UTC collection + publish. Includes completeness
                             # tripwire (warn on missing files) + schema-drift tripwire
                             # (fail-mode since 2026-06-10; was --warn-only during bedding-in).
                             # Actions SHA-pinned (Node-24 since 2026-06-14). Owns the Pages
                             # deploy since 2026-07-06: uploads docs/ as the github-pages
                             # artifact, then a `deploy` job runs actions/deploy-pages with a
                             # 3-attempt retry (Pages source = "GitHub Actions", not branch;
                             # the auto pages-build-deployment workflow no longer runs).
    test.yml                 # PR/push test pipeline (path-filtered, Python 3.12 only)
    openmeteo-probe.yml      # Manual-only (workflow_dispatch) H10/#58 diagnostic. No schedule,
                             # no secrets, touches nothing in the publish path. Do NOT dispatch
                             # within ~30 min of 16:00 UTC — it draws on the same shared runner
                             # egress pool the collection run needs. Delete when H10 resolves.
```

## Key Paths

| Path | What it is |
|------|-----------|
| `data_fetcher.py` | Main orchestrator — all collector wiring, save logic, shape-signature sidecar emission |
| `collectors/base.py` | BaseCollector ABC with retry/circuit breaker + `NonRetryableError` |
| `collectors/_host_breaker.py` | Process-wide per-host circuit breaker (#52) — pass `host_breaker_key` when a host already has another collector |
| `collectors/_http_classifier.py` | Shared HTTP status classifier (`raise_if_permanent`) — adopt this when adding a new API collector |
| `collectors/__init__.py` | All collector exports |
| `utils/data_types.py` | EnhancedDataSet / CombinedDataSet classes (canonical envelope since v2.2) |
| `utils/schema_registry.py` | Schema versioning + migration chain (currently v2.4). `stamp_metadata` embeds changelog slice. |
| `utils/shape_signature.py` | Structural fingerprint for the schema-drift CI tripwire |
| `utils/data_quality.py` | FMEA quality validation. `DATASET_MISSING_SEVERITY` registry + `get_dataset_validation_config()` lookup. |
| `settings.ini` | Public config (location, encryption flag) |
| `secrets.ini` | API keys (gitignored) |
| `.github/workflows/collect-data.yml` | Daily CI/CD pipeline (collect → sidecar → completeness tripwire → schema-drift tripwire → quality gate → publish → upload Pages artifact → `deploy` job with retry) |
| `scripts/detect_schema_drift.py` | CI tripwire — diffs `data/_shape_signatures.json` against `git show HEAD:`. Data-volatile feeds (declared + history-derived) warn instead of failing. Volatility is derived from `data/_shape_observations.jsonl` (#43), falling back to the sidecar's git history when that log has <2 records. **Member drift** (a location dropping out of a feed declared in `MEMBER_MAPPED_FEEDS`, all members sharing one shape) also warns — see `classify_data_member_drift`. Classified before volatility; `MEMBER_MAPPED_FEEDS` are excluded from derived volatility; `CRITICAL_FEEDS` never downgrade. |
| `data/_shape_observations.jsonl` | Append-only learning record — one compact line per run (feed → shape_hash + schema_version). Written every run, committed *before* the drift gate. Never diff against it; it is history, not a baseline. |
| `scripts/backfill_entsoe.py` | Backfill missing ENTSO-E prices into historical files |
| `scripts/archive_to_monthly.py` | Decrypt `data/` files into `05. Data/YYYY-MM/` monthly archive (idempotent) |
| `tests/backtest_data_quality.py` | Run FMEA quality framework against all historical files |
| `tests/` | Unit + integration tests <!-- verify: venv/bin/python -m pytest tests/ --collect-only -q \| tail -1 --> |
| `.claude/settings.json` | Verification-hook wiring (PostToolUse on Edit/Write/MultiEdit) |
| `.claude/hooks/verify_edit.py` | The hook itself — compile check + mapped unit tests. Silent exit 0 on pass, exit 2 + stderr on failure. Needs PyYAML for the workflow branch. <!-- verify: echo '{"tool_input":{"file_path":"collectors/base.py"}}' \| .claude/hooks/verify_edit.py; echo $?   # expect 0 --> |
| `.claude/skills/` | `/review-changes` and `/release` — project-local by design; `curate`, `audit-context` + `update-drift` are user-global |
| `memory/work-items/` | Savepoints for in-flight multi-session work (deliberately NOT under `docs/`, which is the Pages publish root) |
| `memory/hypothesis-log.md` | Open positions the project acts on but has not established |

## How to Work Here

```bash
# Run all tests
venv/bin/python -m pytest tests/ -x

# Run specific test file
venv/bin/python -m pytest tests/unit/test_base_collector.py -v

# Run data collection locally (needs secrets.ini)
venv/bin/python data_fetcher.py

# Backfill missing ENTSO-E data (idempotent, safe to re-run)
venv/bin/python scripts/backfill_entsoe.py --dry-run  # report only
venv/bin/python scripts/backfill_entsoe.py            # patch files

# Archive decrypted data into 05. Data/<YYYY-MM>/ (idempotent)
venv/bin/python scripts/archive_to_monthly.py --since 260201

# Run data quality backtest on historical files
venv/bin/python tests/backtest_data_quality.py

# Check schema drift locally (after a `venv/bin/python data_fetcher.py` run)
venv/bin/python scripts/detect_schema_drift.py --previous-ref HEAD --warn-only

# Check GitHub Actions status
gh run list --limit 5

# Trigger a manual collection run (requires PAT secret in workflow)
gh workflow run "Collect and Publish Data"

# Exercise the verification hook by hand. A healthy file exits 0 and prints nothing —
# that is the pass, not a no-op:
echo '{"tool_input":{"file_path":"collectors/base.py"}}' | .claude/hooks/verify_edit.py; echo $?

# Then confirm it still FAILS on a real break — exit 2 + stderr. A hook you have not seen
# fail is a hook you cannot trust, and exit 0 is what a disabled hook also looks like:
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
