# memory/archive

Not read by default. Grep here when a symptom or decision has history.

- `project_session_*.md` — session retrospectives (index below)
- `gotcha-log-archive.md` — every `[RESOLVED]` gotcha
- `hypothesis-log-resolved.md` — resolved hypotheses (H1, H3, H5, H9)
- `current-state-2026-10-05.md` — the long Current State that MEMORY.md carried until 2026-10-05

## Session index

| File | When to load | Key insight |
|------|-------------|-------------|
| `memory/archive/project_session_2026_06_08.md` | Picking up after the 4-issue + review-battery bundle | What shipped in 4c59378, multi-model review battery outcomes, follow-up issues #30/#31 |
| `memory/archive/project_session_2026_06_09.md` | Picking up after #31/#30/#32 close-out + production verification | Smoke-test methodology, #30 first-fire on real morning ramp data, "validator ran but counts wrong" failure mode |
| `memory/archive/project_session_2026_06_10.md` | Picking up after Augur incident + schema-drift fail-mode flip | Diagnosis correction ("downstream agent says X is broken — inspect artifact, not the log"), tripwire flip caught real `_TS_PATTERN` gap on first fail-mode run, shape_signature date-only key fix. **Incident closed 2026-06-12** — Augur recovered (parser patch `e11487b`), fail-mode tripwire passed first fully-rolled window |
| `memory/archive/project_session_2026_06_12.md` | Picking up after incident-closure verification + June archive | Both 06-10 open items verified closed; `05. Data/2026-06/` archived (490 files, 0 errors); full-history backtest clean (3,790 files, 19 issues all historical); warn-only bedding-in audit pattern promoted |
| `memory/archive/project_session_2026_06_14.md` | Picking up after schema-drift hardening + Node-24/SHA-pin migration + security baseline | Self-maintaining `derive_volatile_feeds()` ends the volatile-feed whack-a-mole; multi-model review (ship-as-is); SHA-pinned actions + Dependabot; secret-scanning push protection ON; phantom `--cov` fix (10%→30%) |
| `memory/archive/project_session_2026_07_06.md` | Debugging GitHub Pages publish / touching the deploy path | Repo now owns the Pages deploy (source = "GitHub Actions") with a 3-attempt retry; the auto `pages-build-deployment` workflow no longer runs. Rerunning a failed deploy reuses the stuck deployment *version* — only a fresh deployment recovers |
| `memory/archive/project_session_2026_07_07.md` | Touching buurt/OpenMeteo feeds, or a `completeness: No data points` publish abort | A transient all-locations Open-Meteo timeout makes `base.collect()` return a truthy-but-empty dataset that hard-fails the completeness gate; `data_fetcher` now coerces empty buurt feeds to `None` (present-empty = absent, `'info'` severity). Same latent risk untreated for the other late-wave OpenMeteo feeds |
| `memory/archive/project_session_2026_08_08.md` | Touching the agent harness (`.claude/**`), or wondering why the verification hook behaves as it does | v1.17.0 adoption reviewed against itself: 3 blockers found in the commit that introduced the reviewer. Hook YAML branch was 100% false-fail; `/review-changes` Step 1 was blind to untracked files; work items sat in the Pages publish root. `utils/secure_data_handler.py` got its first tests |
| `memory/archive/project_session_2026_08_24.md` | Touching `utils/shape_signature.py` or the schema-drift tripwire | Timestamp-map value_shape now MERGES all records (`_merge_signatures`) instead of sampling the first — the 2026-08-23 `load_forecast` publish abort, fixed order-independently, measured byte-identical on all 20 feeds (no schema bump). `ned_production` still drifts by design; its DQ blind spot filed as #49 |
| `memory/archive/project_session_2026_08_31.md` | Triaging an ENTSO-E outage or a zone dropout | Per-zone delivery tracking (`_entsoe_shared.py`); upstream 503s are not schema changes |
| `memory/archive/project_session_2026_09_04.md` | Publishing is blocked, or you are about to build a relief valve / a new gate | Publish restored. Alerting shipped (#50); span check shipped as the FOURTH gate (#53, non-blocking). A time box on the drift gate was designed and ABANDONED — do not rebuild it. `wind_forecast`→CRITICAL_FEEDS closed unmerged and derived volatility left alone, both on measurement. **Do not add a second blocking gate**; new detection lands as warn + alert. |
| `memory/archive/project_session_2026_09_10.md` | A guard, gate or grace is not working just because it is registered | The `ned_production` present-empty grace shipped INERT — it tested `not ds.data` where the gate counts points, and 14 green tests asserted the wiring rather than the behaviour. Read before adding any feed to a registry |
| `memory/archive/project_session_2026_09_21.md` | A hash flip you cannot explain, or before adding any exemption to a gate | Re-fetch the window and strip fields to reconstruct a flip byte-exactly; an exemption removes the gate's vote, not the learning record's |
| `memory/archive/project_session_2026_09_03.md` | A run failed and you are deciding whether it is upstream or a defect | How to tell "we are blocked" from "it is down" (vary the credential, compare bytes); H2 reopened — the drift-tripwire class is 3/26 = 11.5%, past its own threshold, so it is a DEFECT not a transient |
| `memory/archive/project_session_2026_10_05.md` | An Open-Meteo storm or a missed publish to backfill | 429 storm outlasted a 3 s retry; 429 budget + shared cooldown (re-checked inside the semaphore); `backfill_missed_run.py` for one-day feeds |
