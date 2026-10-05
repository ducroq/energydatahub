# Session 2026-06-14 — schema-drift hardening (self-maintaining), Node-24 + SHA-pin migration, repo security baseline

## What happened

Started from a failed scheduled-class run (gitrun error email); ended with the recurring schema-drift false-positive class structurally closed + a broad CI/supply-chain hardening pass.

1. **Schema-drift false positives — diagnosed and hardened in two layers:**
   - **06-13 `air_quality_buurt`** (carried in from the prior incident): RIVM nearest-online station set + per-station pollutant set are dict keys → hash flips daily. Added to `VOLATILE_SHAPE_FEEDS` (warn, not fail). Built the partition mechanism (`_partition_within_feed_drift`), the `CRITICAL_FEEDS`∩`VOLATILE_SHAPE_FEEDS` disjoint import-time assert, and `[volatile]` summary annotation.
   - **06-14 `cross_border_flows` + `calendar_features`**: both week-stable then flipped together. Regenerated locally for exact shape diffs: cross_border drops the `NL→GB` border key when that interconnector reports no flow in the sampled hour; calendar's `metadata.upcoming_holidays` flips empty→populated when a holiday enters the window. Both confirmed data-driven, added to the seed set.
   - **Root fix (the real hardening):** `derive_volatile_feeds()` — auto-classifies a feed as volatile if its committed history shows >1 `shape_hash` at the same `schema_version` (versioned migrations excluded). `main()` unions declared + derived, so the allowlist is now a documented seed/override/fallback, not a hand-maintained list. Validated against real history: proactively flagged `grid_imbalance`, `market_history`, `market_proxies` — already-volatile feeds not yet broken in CI (three future false positives defused). `--volatility-window N` (default 60). 22→28 drift tests. Live-validated: collect run `27492006267` auto-classified the three and published.

2. **Node-20 → Node-24 action migration** (GitHub forces it 2026-06-16): `checkout@v4→v5`, `setup-python@v4→v6` (the floating `v5` tag still resolved Node-20 — needed a second round), `codecov-action@v4→v6` (v5 pinned a Node-20 `github-script@v7`; v6 uses `github-script@v8`; `file`→`files` rename). Then **SHA-pinned all actions** (`checkout@v5.0.1`, `setup-python@v6.2.0`, `codecov-action@v6.0.2`) + added `.github/dependabot.yml` (github-actions, weekly, grouped) so pins auto-update.

3. **Multi-model review battery** (Opus×2, Sonnet, Haiku; Fable unavailable in env) over the session diff — verdict ship-as-is, 0 must-fix. Applied all follow-ups: fixed phantom `--cov` targets (`energy_data_fetchers`/`weather_data_fetchers` don't exist → `--cov=collectors`; coverage 10%→30%), hardened drift tests, disjoint guard, doc polish.

4. **`if: always()` → `if: success()`** on the schema-drift tripwire step (only meaningful after a clean collection; completeness tripwire keeps `always()`).

5. **Repo security baseline** (via `gh api`): enabled Dependabot security updates, secret scanning, and **secret-scanning push protection** — platform-level enforcement of the "never commit secrets.ini/API keys" constraint. (`secrets.ini` confirmed gitignored + untracked; push protection scans pushed commit content only, so local secret use is unaffected.)

## Curation actions

- Gotcha log: 3 new entries (history-derived volatility / `setup-python@v5`-still-node20 / phantom `--cov`), all `[RESOLVED]`. Promoted the 3-incident data-driven-shape-churn pattern.
- MEMORY.md: schema-drift Current State rewrite (self-maintaining volatility); open issues 3→5 (#33, #34); new Active Decision; session pointer.
- CLAUDE.md: schema-drift mechanism, SHA-pin/Dependabot note, secret-scanning constraint, v2.3→v2.4 (×2), Key Paths.

## State at session end

- Working tree: code + memory/docs edits, committed this session. Local `main` synced to origin.
- CI: green — Tests + Collect & Publish both passing; data published (`6942b5f`, then derivation-validated `27492006267`).
- Schema version: 2.4 (unchanged — no shape changes shipped, only the drift *detector* changed).
- Security: Dependabot alerts + version updates + security updates ON; secret scanning + push protection ON.
- Open issues: 5 — #2 (JAO), #9 (storage migration / git-as-archive bloat — relevant to the 3,909 timestamped `data/` files), #21 (Liander), #33 (forecast span only today+tomorrow post-envelope-wrap), #34 (Elspot tomorrow-prices / NL duplication).

## Pickup for next session

- **Parked (my offer, user chose derivation instead):** root-cause `shape_signature` union-across-timestamp-values fix — would let `cross_border_flows` come off volatile classification entirely. Low value now that derivation handles the symptom.
- **Optional secret-scanning extras** still off: `non_provider_patterns` (catches custom AES/HMAC key formats, noisier) + `validity_checks`.
- **`data/` bloat**: 3,909 timestamped files back to 2025-09-28; tracked by #9. Not pruned (deleting committed historical data needs a deliberate call + archive-completeness check first).
- Carried watch items: #30 load cross-field (2026-06-09 gotcha), #33/#34 forecast-span/Elspot.
