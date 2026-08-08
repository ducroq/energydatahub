---
name: release
description: Cut a published-schema version — classify the bump, verify preconditions, write the SCHEMA_CHANGELOG entry, add the migration, sync version references, commit. Stops before the run that publishes it.
disable-model-invocation: true
---

Cut a schema release. Work through the steps in order and **stop at the end of Step 6** — publishing is the engineer's call, never the agent's. Step 7 runs only after the engineer confirms the production run.

Adopted from agent-ready-projects `templates/release.md` (v1.17.0), **adapted**: this repo cuts no git tags and ships no package. Its versioned, consumer-facing artifact is the **published data schema** — `CURRENT_SCHEMA_VERSION` in `utils/schema_registry.py`, its `SCHEMA_CHANGELOG`, and the migration chain. The downstream consumer is Augur, which reads `metadata.schema_version` off every published file and cannot see this code. The template's "a pushed tag is permanent" reasoning applies verbatim: once a run publishes files stamped `2.5`, Augur has seen `2.5`, and you cannot unsee it.

`disable-model-invocation: true` is deliberate. An agent deciding on its own that it is time to bump the schema is a failure the Step 6 stop-gate cannot catch, because by then the bump is already written. Type `/release` yourself.

## Step 0 — Substitute the version placeholder

Every command below is written with a literal `X.Y`. Substitute the actual version before running anything. A grep for `X\.Y` **succeeds quietly** against an unsubstituted placeholder and reports "no stale references" — exactly what a healthy release looks like. If any command emits the literal string `X.Y`, you did not substitute it: stop and start over.

You will not know the version until Step 2. Run Step 1 first, agree the version, then substitute throughout.

## Step 1 — Establish what changed

There are no tags to diff against. The last release is the highest key in `SCHEMA_CHANGELOG`:

```bash
venv/bin/python -c "from utils.schema_registry import CURRENT_SCHEMA_VERSION, SCHEMA_CHANGELOG; print(CURRENT_SCHEMA_VERSION, sorted(SCHEMA_CHANGELOG))"
git log --oneline -G 'CURRENT_SCHEMA_VERSION = ' -- utils/schema_registry.py   # every prior bump
git log <last-bump-commit>..HEAD --oneline
git diff <last-bump-commit>..HEAD --stat
```

Group the changed files by surface:

- **Published shape** — anything that changes what lands in `data/*.json` and therefore `docs/`: collectors' `_normalize`/`_parse` output, `utils/data_types.py`, `data_fetcher.py` save logic, metadata stamping
- **Guards and validation** — `utils/data_quality.py`, `scripts/detect_schema_drift.py`, workflow tripwires. These change what *blocks* a publish, not what a published file looks like
- **Internal** — tests, memory, docs, CI plumbing

Only the first group can require a bump. If nothing in it changed, report that and stop — and say so explicitly rather than bumping "to be safe". A no-op bump costs every consumer a migration-chain read for nothing.

**The authoritative signal is the shape signature, not your reading of the diff.** Run a collection and diff the sidecar:

```bash
venv/bin/python data_fetcher.py                                   # needs secrets.ini
venv/bin/python scripts/detect_schema_drift.py --previous-ref HEAD --warn-only
```

If that reports within-feed shape drift, a bump is required. If it reports only catalog drift (feeds added/removed), it is **not** — `schema_version` versions the envelope shape of existing feeds, not the catalog of which feeds are currently publishing. That distinction is the 2026-06-08 buurt-drift fix; re-deriving it wrongly here would undo it.

## Step 2 — Classify the bump

| Bump | When | Examples |
|------|------|----------|
| **MAJOR** (`3.0`) | Augur must change code to keep reading the files | Removed or renamed a field; changed a field's type or units; restructured the envelope |
| **MINOR** (`2.5`) | New data, additive and optional | New metadata field, new nested block, new feed-level signal Augur can ignore |
| **PATCH** | Not used by this schema | Two-component versions only. A change too small to be MINOR is a change that needs no bump at all |

Apply these rules **in order** — the first that fires decides:

1. **Does Augur have to do something to keep working?** If yes, MAJOR. This outranks everything below: a "small fix" that renames a field is MAJOR however small the diff. Precedent: `2.3` was MAJOR-shaped in effect (`gas_storage.working_capacity_twh` renamed) — check how that was handled and whether the precedent should be followed or corrected.
2. **Otherwise: is there new data to read, or only changes to existing plumbing?** New field or block → MINOR. No observable change to a published file → no bump.

State the proposed bump **with the reason**, and ask the engineer to confirm before continuing. Do not proceed on a guessed version.

Cite the closest precedent from `SCHEMA_CHANGELOG` rather than re-deriving the rule. `2.4` (additive metadata on `grid_imbalance.json`, migration a no-op except the version stamp) is the canonical MINOR. Where a precedent and these rules disagree, follow the precedent and flag the discrepancy.

## Step 3 — Verify preconditions

Run every check and report the results. **Do not continue past a failure** — surface it and stop.

```bash
git status --porcelain                        # 1. clean tree
git rev-parse --abbrev-ref HEAD               # 2. on main
venv/bin/python -m pytest tests/ -x           # 3. tests actually pass
venv/bin/python tests/backtest_data_quality.py    # 4. historical files still validate

# 5. Version references. git grep is gitignore-aware (skips venv/) and repo-root-relative.
git grep -n "2\.4" -- ':!data' ':!docs' ':!memory'
git grep -nE "schema[_ ]version[\"': ]*v?[0-9]+\.[0-9]+" -- ':!data' ':!docs'
git grep -n "CURRENT_SCHEMA_VERSION"
```

1. **Clean tree.** Uncommitted work must not be in a schema release.
2. **Right branch** — `main` unless you have a reason.
3. **Tests pass.** Report actual output. Never report a check as passing without running it. If you cannot run it, report **"could not verify"** as a *failure*, not a pass.
4. **The migration chain is complete and reversible-safe.** `venv/bin/python -m pytest tests/unit/test_schema_registry.py -v` must cover the new hop. A file at every prior version must still load through the chain to the new one.
5. **Version references located.** These are the files Step 5 must update.

Two things the greps cannot do, which you must cover by reading:

- `data/` and `docs/` are excluded above on purpose — they hold thousands of files stamped with historical versions. Those are *correct* and must not be rewritten. Excluding them is not laziness; including them is the failure mode.
- No grep distinguishes a stale reference from a legitimate historical citation ("the 2.3 rename") or a dated memory entry. Judge each hit; do not bulk-replace. The version-agnostic grep is **expected** to return hits you leave alone.

## Step 4 — Write the SCHEMA_CHANGELOG entry

`SCHEMA_CHANGELOG` in `utils/schema_registry.py` is a write-at-release dict, not a candidate block: add a new top-level key. A complete entry has `date` (ISO), `description` (one line), and `changes` (a list of specific, consumer-facing statements).

Write for someone deciding whether their parser needs work. "Updated metadata" is useless; "added `balance_delta_status` (complete/synthesised/unknown); absent on historical files, treat as 'unknown'" is what they need.

**Say what the migration does NOT do.** The `2.4` entry's most valuable line is that it deliberately does not backfill `balance_delta_status='complete'`, because a historical file may have come from a degraded run. Silent backfill assumptions are what break consumers months later.

This entry is not just repo documentation — `stamp_metadata()` embeds the slice for the file's own version into every published file (Layer B). Whatever you write here ships inside the data.

## Step 5 — Sync version references

Update the files from Step 3 check 5 **that are meant to track the current version** — not every hit. Typically in scope:

- `utils/schema_registry.py`: `CURRENT_SCHEMA_VERSION`, the new `SCHEMA_CHANGELOG` entry, and the new `_migrate_X_to_Y` function wired into the migration chain
- `CLAUDE.md`: the schema-version line in the architecture block
- `memory/MEMORY.md`: the **Schema version** bullet under Current State
- `README.md`, if it names a version
- Any test that asserts the current version — see the carve-out below before touching one

### The one time you may edit a test

CLAUDE.md states flatly that *tests are not modified to make them pass*, and the verification hook repeats it on every failure: *"Do not edit the test to make it pass — if the test is wrong, say so and stop."* Both are correct and both stay in force. A schema bump is the single documented exception, and it is narrow:

**Allowed:** updating a test's *expected version literal* (`"2.4"` → `"2.5"`) in a test whose purpose is to assert the current version — because the version genuinely changed and the old literal is now stale by design.

**Not allowed:** anything else. Loosening an assertion, widening a range, deleting a case, or adding `@pytest.mark.skip` because a migration test fails. A failing `test_v1_to_current` after a bump means the migration chain is broken — that is the test doing its job.

Expect the hook to fire mid-edit here: bumping `CURRENT_SCHEMA_VERSION` before the migration function is wired leaves the tree in a state where the tests correctly fail. That is not a signal to stop; it is the intermediate state of a three-part edit that must land together. Finish all three (version, migration hop, changelog entry), then re-run `venv/bin/python -m pytest tests/unit/test_schema_registry.py -v` and confirm green before moving on. If it is still red once all three are in, the chain is genuinely wrong — stop and report, do not touch the test.

Then re-run the **current-version** grep and confirm the only remaining hits are files you intended to leave (historical citations, `data/`, `docs/`). Do not drive the version-agnostic grep to zero — it never reaches zero, and chasing that will start rewriting history.

## Step 6 — Commit, then stop

Stage **only** the files this release touched:

```bash
git add utils/schema_registry.py <each file updated in Step 5>
git status --porcelain          # confirm nothing unexpected is staged
git commit -m "Bump published schema to X.Y — <one-line reason>"
```

Do not use `git add -A`. Step 3 verified a clean tree, but Steps 4 and 5 wrote files and any scratch work landed in between; `-A` stages every untracked, unignored file — including a stray decrypted `data/` dump.

Then **stop and report**:

- The commit, the version, and what changed in the published files
- The exact command the engineer should run to publish
- What Augur will see on the next run

**Do not run the following.** The next collection run publishes files stamped with the new version to a public GitHub Pages site, and Augur reads them within the day. Approval to *cut* a schema release is not approval to *publish* it.

    # DO NOT RUN — hand these to the engineer
    git push origin main
    gh workflow run "Collect and Publish Data"

## Step 7 — After the run is published

Only once the engineer confirms the run:

1. **Verify the version actually reached disk**, not just the code:

   ```bash
   gh run list --workflow=collect-data.yml --limit 1
   venv/bin/python -c "import json;print(json.load(open('data/_shape_signatures.json'))['schema_version'])"
   ```

   Do not accept a green run as proof. This project's documented three-incident pattern is
   precisely "registered but never reached the published file" — trace the version to the
   sidecar and to a decrypted published file before claiming the release landed.

2. Confirm the schema-drift tripwire passed a **fully-rolled window** — the first run after a bump can pass for the wrong reason.
3. Update `memory/MEMORY.md` Current State to the new version.
4. Close any issue the release resolves.
5. Fill in the Outcome section of any `memory/work-items/` file this release completed.

## Do not

- **Do not bump on a dirty tree.** The commit would not match what was tested.
- **Do not re-use or rewrite a published version.** If a released schema is wrong, cut the next version.
- **Do not choose the version number yourself.** Propose it with reasoning; the engineer confirms.
- **Do not claim a check passed without running it.** "I couldn't run it" is a valid report; "it passed" without evidence is not.
- **Do not bump the version without adding both the migration function and the changelog entry.** All three, or none. Know what actually enforces what, because the three are not equally guarded:
  - `scripts/detect_schema_drift.py` enforces exactly one thing — *shape changed without a version bump*. It contains no reference to `MIGRATIONS` or `_migrate_`; it only recommends adding one in its error text. A green drift tripwire is **not** evidence that the migration chain is intact.
  - `tests/unit/test_schema_registry.py::test_v1_to_current` is what actually catches a missing migration function: bump `CURRENT_SCHEMA_VERSION` without adding the chain hop and `migrate_to_current` stops at the old version, failing the assert.
  - Nothing at all enforces the `SCHEMA_CHANGELOG` entry. That one is on you.
- **Do not pick a version past `X.9`.** `migrate_to_current` selects hops with a lexicographic string compare (`utils/schema_registry.py:491`), so `2.10` sorts *below* `2.2` and the chain silently skips hops for every historical file. The scheme is single-digit-minor by accident, not by design. If `2.9` is the current version, the next cut is a major (`3.0`) — raise this with the engineer rather than choosing `2.10`.
- **Do not batch unrelated changes into a release** because they are sitting in the tree.
