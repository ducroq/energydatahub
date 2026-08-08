# Session 2026-08-08 — agent-ready-projects v1.17.0 adoption, reviewed against itself

No pipeline code was touched. Everything here is agent-harness: the verification hook, the
two project-local skills, work items, and the hypothesis log — plus the first tests for
`utils/secure_data_handler.py` and a diagnosis of the 08-03 daily-run failure.

## What shipped

- **Framework v1.10.1 → v1.17.0.** `.claude/settings.json` + `.claude/hooks/verify_edit.py`
  (PostToolUse compile-and-test hook), `.claude/skills/review-changes/` and
  `.claude/skills/release/`. `curate` and `audit-context` stay user-global by design.
- **`.gitignore`** now tracks the shared harness (`!.claude/settings.json`,
  `!.claude/hooks/`, `!.claude/skills/`) while `settings.local.json` stays personal.
- **`memory/work-items/`** — savepoints for multi-session work. Created under
  `docs/work-items/` and moved; see below.
- **`memory/hypothesis-log.md`** — new. Open positions (H1–H5) with the method that would
  settle each, and a review date or trigger. `/curate` surfaces overdue entries.
- **`tests/unit/test_secure_data_handler.py`** — 23 tests. Suite 681 → 704.

## The point of the session: the reviewer found three blockers in its own commit

`/review-changes` was run on the diff that introduced `/review-changes`. Full 5-lens
battery (HIGH tier). It found three blockers, all real, all in the adoption itself:

1. **The hook's workflow-YAML branch failed 100% of the time.** PyYAML was in neither the
   venv nor `requirements.txt`, and `run()` could not distinguish `ModuleNotFoundError`
   from a parse error — so every workflow edit got "not valid YAML — the daily run would
   fail to start" about the valid, currently-shipping `collect-data.yml`. This check had
   never once validated a workflow.
2. **`/review-changes` Step 1 could not see untracked files.** It prescribed only
   `git diff --stat/--cached/--summary`; none of those report untracked files. On its own
   change set that was 645 of 695 lines invisible, while the magnitude gate declared "a
   new file in a HIGH path" always-full-depth — a carve-out the procedure could not
   observe. The review only worked because `git status --porcelain` was run off-script.
3. **`docs/work-items/` was inside the GitHub Pages publish root.** `collect-data.yml`
   uploads all of `docs/` as the Pages artifact, so a work item describing an *unfixed*
   availability weakness — which feeds can abort the daily publish, and the trigger —
   would have been world-readable on the next daily run. Moved to `memory/work-items/`
   before anything published. This deviates from the upstream template on purpose; the
   reason is recorded in the work-items README so a future sync doesn't "fix" it back.

Also fixed: `settings.ini` promoted MEDIUM→HIGH in the tier table (it holds
`encryption = 1`, and the guarantee lens that owns the encryption invariant is HIGH-only,
so it would never have run on the one file that turns encryption off); `mapped_tests()`
now unions its glob patterns instead of stopping at the first (latent, not live — it
narrowed nothing across today's 43 files, but silently drops the natural second test file);
`realpath` so a symlinked checkout doesn't silently skip; and `/release` no longer credits
`detect_schema_drift.py` with enforcing migration functions it never checks — that is
`tests/unit/test_schema_registry.py`, and nothing at all enforces the changelog entry.

**Method note worth keeping:** two lenses reached opposite conclusions on the
`mapped_tests` `break`. Enumerating all 43 real files settled it — first-match and
full-union were identical, so the live-defect claim was wrong and the latent-trap claim
was right. Lens disagreement is a prompt to go measure, not to average the two reports.

## `utils/secure_data_handler.py` had zero tests

The AES-CBC + HMAC-SHA256 module named in Hard Constraints had no unit test. Because the
hook falls back to the full `tests/unit` suite when a file has no mapped test, editing it
produced a **green hook from 681 tests that never imported it** — the project's documented
silent-no-op signature, on the encryption boundary. Now covered: round-trip (incl. unicode,
empty, exact-block-multiple padding), IV freshness, and tamper detection on IV, ciphertext,
and signature independently. Exit 0 from the hook is not a coverage claim.

## Daily-run failures diagnosed

- `31123856009` (08-06) — "job was not acquired by Runner". Pure GitHub infrastructure.
- `30838120578` (08-03) — drift tripwire failed on `ned_production` + `wind_forecast`
  (transient-driven shape churn; quality gate itself was only `warning`). The three
  surrounding runs were green with no code change.

That second one exposed a structural gap. `derive_volatile_feeds()` learns from committed
sidecar history, but the tripwire (`collect-data.yml:119`) runs *before* the commit step
(`:149`) — so a run it fails commits nothing, and the classifier never sees the drift that
tripped it. Both feeds remain unclassified five days later and will fail the same way.
Filed as a GitHub issue; recorded as H3 in the hypothesis log, because the obvious fix
(commit the sidecar pre-gate) risks poisoning the baseline with a genuinely broken shape —
exactly what the 2026-06-10 fail-mode flip existed to stop.

## Open / next

- **#42** present-empty rollout — still not started, decision pending (H1).
- **H3** drift-classifier starvation — needs an engineer call on the baseline trade-off.
- **Gotcha-log retrofit** — 24 entries predate the 2-3 line budget. The log header and
  CLAUDE.md now agree that they are legacy and not the format to copy, so the retrofit is
  optional cleanup rather than a live contradiction. Not done; wants sign-off on a sample.
- **Stale topic files** — `project_actions_optimization.md`, `project_data_backfill_gaps.md`,
  `project_entsoe_old_files.md` last touched 2026-06-07/08. Flagged, not audited.
