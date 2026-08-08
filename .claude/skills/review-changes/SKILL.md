---
name: review-changes
description: Diff-driven pre-commit review — picks review lenses based on what changed, from a single adversarial pass to the full multi-lens battery
disable-model-invocation: false
---

Pre-commit review of pending changes. Scope and depth are driven by what changed, not a fixed checklist.

Adopted from agent-ready-projects `templates/review-changes.md` (v1.17.0). The risk tiers and the guarantee lens below name files in *this* tree — that is why this skill is project-local and must never be installed user-globally, where it would shadow every other repo's own copy.

## Step 1 — Diff and classify

Run all four of these — the first three see tracked changes, the fourth is the only one that sees new files:

```bash
git diff --stat; git diff --cached --stat      # tracked, modified
git diff --summary; git diff --cached --summary # modes, renames, submodules, binaries
git status --porcelain --untracked-files=all    # UNTRACKED — invisible to every command above
```

**`--stat` alone cannot see a mode change, a rename, a submodule, or a binary** — all four render as zero or near-zero lines, and three of them are carve-outs below.

**No `git diff` variant sees an untracked file at all.** This is not a refinement; it is the difference between reviewing a change and reviewing nothing. The commit that introduced this skill was 695 lines, 645 of them in new untracked files under `.claude/` and `docs/work-items/` — `git diff --summary` returned empty on it, and the magnitude gate below declares "a new file in a HIGH path" always-full-depth. Without `git status --porcelain --untracked-files=all`, that carve-out fires on a file set the skill cannot observe.

A carve-out you cannot observe is not in force.

Classify each changed file into a risk tier:

| Tier | File patterns | Depth |
|------|-------------|-------|
| **HIGH** | `collectors/**`, `utils/**`, `/data_fetcher.py`, `scripts/**`, `.github/workflows/**`, `.claude/**`, `.gitignore`, `settings.ini`, `data/_shape_signatures.json`, `data/_upstream_empty_streak.json`, `tests/**`, `pytest.ini`, `requirements.txt` | Full battery (4-5 lenses) |
| **MEDIUM** | `CLAUDE.md`, `docs/**`, `/README.md`, `.github/dependabot.yml` | Two lenses (adversarial + doc-accuracy) |
| **LOW** | `memory/**`, `memory/work-items/**`, `data/**`, `legacy/**` | One lens (adversarial) |

`**` crosses directory levels; a leading `/` anchors to the repo root. **The most specific matching pattern wins.** Where no pattern is more specific than another, take the highest tier.

The HIGH row is everything that reaches a downstream consumer or runs unattended. Six entries are easy to miss and each is here because it burned this project, or would: `scripts/**` and `.github/workflows/**` run on a machine nobody is watching at 16:00 UTC; `utils/**` holds the schema registry and the quality gate, so a defect there is published under encryption before anyone reads it; `.gitignore` is what keeps `secrets.ini` out of a public repo; and `tests/**` is the only thing standing between a plausible-looking collector change and a silent data gap.

The last two are the counter-intuitive ones, and both were MEDIUM/LOW until this skill was reviewed against itself:

- **`settings.ini`** holds `encryption = 1`. Flipping that one digit publishes plaintext to a public site — and the guarantee lens, which is the only lens that owns "AES-CBC + HMAC-SHA256 on everything published", is HIGH-only. At MEDIUM it would never have run on the single file that can turn encryption off.
- **`data/_shape_signatures.json` and `data/_upstream_empty_streak.json`** are committed *control state*, not data: the first is the schema-drift tripwire's comparison baseline, the second is the #38 escalation counter. Editing either changes what CI blocks on. The rest of `data/**` really is data and stays LOW — this is why "most specific pattern wins" matters.

### Magnitude gate

The tier above is set by *path*. Depth is also set by *size* — but size is the weaker signal, so the exceptions are stated first and override everything below them.

**Always full depth, regardless of size.** Each of these is dangerous *because* it is small:

- **`.gitignore`** — one line decides whether `secrets.ini` is published. This repo is public.
- **Renames and moves** — `git diff --stat` reports `0 insertions(+), 0 deletions(-)` under `-M`, while every reference to the old path breaks.
- **Permission changes** — also zero insertions and deletions, invisible without `--summary`.
- **Binary files and submodule pointers** — the other two members of the zero-line class.
- **Any change to a shell script, a workflow YAML, or anything under `scripts/`** — code that runs unattended, where shell and YAML both break in one character.
- **Any change to `CURRENT_SCHEMA_VERSION`, `SCHEMA_CHANGELOG`, or a `_migrate_X_to_Y` function** — a schema bump is a three-line diff with an unbounded downstream blast radius. Use `/release` for these, not just this review.
- **Any change to `DATASET_MISSING_SEVERITY`, `EXPECTED_DATA_TYPE`, `CRITICAL_FEEDS`, or `VOLATILE_SHAPE_FEEDS`** — one dict entry changes what the quality gate blocks on.
- **Any diff that removes or loosens a check** — a deleted guard, a weakened assertion, a widened `try/except`, a severity downgraded from `critical`, a threshold raised. Loosenings are characteristically a handful of lines.
- **A new file in a HIGH path** — the tier for new content has not been decided yet.

**Otherwise size sets the depth.** Size means the whole change that will land, not the slice in front of you. Sum staged, unstaged, and any local commits not yet pushed:

```bash
git diff --shortstat; git diff --cached --shortstat
git log @{u}.. --shortstat 2>/dev/null || echo 'no upstream — count all commits on this branch'
```

| Changed lines | Depth |
|---------------|-------|
| **< 20** | One adversarial pass |
| **20–200** | Path tier as above |
| **> 200** | Full battery, whichever tier the paths fall in |

Run that single pass in a **fresh context** — a subagent. Reviewing your own edit in the context that produced it is the self-certification failure this skill exists to prevent; the saving comes from running *one* independent reviewer instead of five, not from dropping independence.

The gate changes how many lenses run. It never changes *whether* a change is reviewed — every diff still gets at least one adversarial pass.

**If a changed file matches no pattern, treat it as MEDIUM, and name it in the report under "Unclassified"** even when a HIGH file in the same diff makes the tier moot. An unrecognized path is usually new content whose tier nobody has decided yet. Do not silently drop it, and do not default it to LOW. If it is executable, escalate to HIGH.

**`docs/` is the GitHub Pages publish root** — `.github/workflows/collect-data.yml` uploads the whole directory as the Pages artifact, served verbatim. Documentation prose there is MEDIUM because publishing it is the intent. But any *new kind of content* landing under `docs/` is published to the open internet on the next daily run, so ask what it is before accepting the MEDIUM tier: internal savepoints, incident notes, or anything describing an unfixed weakness do not belong there. This is not hypothetical — `docs/work-items/` was placed under the Pages root by the same commit that created this skill and had to be moved to `memory/work-items/`.

If no files changed, report "nothing to review" and stop.

## Step 2 — Execute review lenses

For each lens, spawn a subagent with the prompt below. Run lenses concurrently.

### Lens: guarantee-preservation (HIGH only)

```
You are reviewing changes to energyDataHub — a data collection pipeline whose output
is consumed by a downstream ML project (Augur) that cannot see this code. These files
carry guarantees that must hold for every published file.

For each changed file, identify what it guarantees:

- utils/schema_registry.py: any shape change bumps CURRENT_SCHEMA_VERSION *and* adds a
  _migrate_X_to_Y function *and* adds a SCHEMA_CHANGELOG entry. All three or none.
  Migrations are backward-compatible: an old file must still load.
- utils/data_types.py: the {metadata, data} envelope. EnhancedDataSet/CombinedDataSet
  shape is the published contract.
- utils/data_quality.py: DATASET_MISSING_SEVERITY is the SINGLE registry for missing-feed
  severity — no parallel list may reappear. Only 'critical' aborts a publish.
- utils/secure_data_handler.py: AES-CBC + HMAC-SHA256 on everything published; keys never
  inlined, never logged.
- settings.ini: `encryption = 1`. This is the switch that decides whether the published
  files are ciphertext or plaintext. Any diff that clears, comments out, or conditionalises
  it publishes the whole dataset in the clear to a public GitHub Pages site. Treat a change
  here as GUARANTEE WEAKENED unless the diff also explains why publishing plaintext is
  intended.
- data/_shape_signatures.json: the schema-drift tripwire's baseline — it must stay tracked,
  or `git show HEAD:` in scripts/detect_schema_drift.py resolves to nothing and the tripwire
  silently passes. Same for data/_upstream_empty_streak.json and the #38 escalation counter.
- collectors/base.py: retry + circuit breaker + validation contract. NonRetryableError
  bails without retry; UpstreamNoDataError fast-fails without tripping the breaker.
  collect() resets quality issues and auto-injects metadata['collector_quality_issues'].
  A collector overriding collect() must preserve that whole chain.
- data_fetcher.py: the 8-touchpoint published-dataset checklist in
  memory/project_published_dataset_checklist.md. Adding a feed to some touchpoints but
  not all silently breaks publishing — this was a real BLOCKER on c40a53b.
- .github/workflows/collect-data.yml: completeness tripwire, schema-drift tripwire, quality
  gate, and the Pages deploy job all still run and still fail loudly. Actions stay SHA-pinned.
- scripts/detect_schema_drift.py: within-feed drift fails, catalog drift warns, removed
  CRITICAL_FEEDS escalate. Volatile membership stays derived from history, not hand-listed.
- All timestamps normalized to Europe/Amsterdam.

For each guarantee: does the change preserve it? Flag any weakening.

Then ask: is the change broader than its stated intent?

Report: GUARANTEE OK or GUARANTEE WEAKENED for each surface touched.
```

### Lens: adversarial (all tiers)

```
You are an adversarial reviewer. Your job is to refute the changes — find what breaks,
what edge cases fail, what assumptions don't hold.

For each changed file:
1. What is the change trying to accomplish?
2. What could go wrong? Find at least one concrete failure scenario.
3. Are there silent failure modes — things that would pass but be wrong?
4. If this is a test or CI-guard change: what real failure does the weaker check now pass?
5. If this touches a collector or the publish path: what would Augur see, and would it be
   able to tell a degraded feed from a healthy one?

This pipeline runs unattended once a day. A failure that surfaces as a green run is worse
than a crash. Weight silent-failure scenarios accordingly.

Go in assuming the change is refutable. Report REFUTED with a concrete failure — a
triggering input, an edge case, or a contradiction between two things the change now
asserts. Prose contradictions count and often have no triggering input; do not withhold
one for lacking a repro. Report NOT REFUTED only after a thorough attempt has failed.

Report: REFUTED or NOT REFUTED, with failure scenario if refuted.
```

### Lens: doc-accuracy (MEDIUM and HIGH)

```
You are reviewing documentation changes for accuracy against disk state.

1. Does every file path mentioned actually exist on disk?
2. Does every command use correct flags and syntax? Run the read-only ones.
3. Do version numbers, dates, issue numbers, and run IDs match what shipped?
4. If a new collector, dataset, or script is documented, does it exist and is it wired in?
5. Internal inconsistencies — does CLAUDE.md say one thing and memory/MEMORY.md another?
6. Does any <!-- verify: ... --> comment still return what the surrounding claim asserts?

Report: ACCURATE or INACCURATE, with the specific mismatch.
```

### Lens: shell-and-yaml-correctness (HIGH only, when `scripts/**`, `*.sh`, or `.github/workflows/**` changed)

```
You are reviewing shell and GitHub Actions changes for correctness.

Shell:
1. set -u safe? No unbound variables on the changed paths.
2. Quoting correct? No word-splitting bugs, spaces in filenames handled.
3. Edge cases: empty input, missing files, unexpected exit codes.
4. Non-determinism introduced (date, random, network)?
5. Are error exits explicit and loud, not silent? A `|| true` added to quiet a failure is
   the single most common way this repo's tripwires stop tripping.

GitHub Actions:
6. Are all actions still SHA-pinned (not @v4 / @main)?
7. Does a step that must fail the run use `if: success()` rather than `if: always()`?
8. Does any step swallow a non-zero exit (pipes, `continue-on-error`, trailing `|| echo`)?
9. Are secrets referenced via ${{ secrets.* }} only, never echoed or written to a file
   that gets committed or published to docs/?

Report: OK or ISSUE, with the specific bug.
```

### Lens: end-to-end-trace (HIGH only, when a new field/check/signal is added)

```
This project has a documented three-incident failure pattern: a registered behaviour that
is expected to enforce or surface something silently no-ops at a downstream layer, and the
absence looks identical to "data is clean". Instances: GoogleWeather returned success exit
codes for 7 months while 401-ing; validate_value_ranges silently no-op'd on 2-level-nested
feeds for ~3 months; TenneT's collect() override bypassed BaseCollector's metadata
auto-inject so balance_delta_status populated in memory but never reached the published file.
Signature: unit tests green, live run silently drops the new field.

For every new or changed field, check, severity, or quality signal in this diff, trace it
through EVERY layer until it is observable on disk or in the operator log:

  reset -> fetch -> parse -> normalize -> validate -> metadata -> inject -> dataset
  -> data_fetcher save -> shape signature -> encryption -> docs/ publish -> quality report

Name the layer at which you lose sight of it, and say what would have to be observed —
a file under data/, a key in _shape_signatures.json, a line in the quality report — to
prove it survives. "The unit test asserts it" is not proof; that is exactly what was
green in all three incidents.

Second check, the parallel-registry rule: does this diff add an entry to a hand-maintained
list keyed on a feed/dataset identifier? If so, find every OTHER list keyed on the same
identifier and confirm the entry was added to all of them. Two incidents: data_quality.py
had 3 parallel severity lists; collect-data.yml had 2 parallel publishable-feed lists and
nordic_hydro was added to one but not the other.

Report: TRACED (naming the observable) or LOST AT <layer>, plus REGISTRIES CONSISTENT or
the list that is missing the entry.
```

## Step 3 — Synthesize

Combine all lens reports. For each finding:
- **Severity**: BLOCKER (must fix before commit) / WARNING (should fix) / NOTE (consider)
- **Lens**, **File**, **Finding**, **Fix**

If any BLOCKER: recommend fixing before commit.
If only WARNING/NOTE: recommend the user review and decide.

## Step 4 — Report

```
## Review: [N] files changed, [tier] risk, [M] lenses

### Findings

| # | Severity | Lens | File | Finding |
|---|----------|------|------|---------|
| 1 | BLOCKER | adversarial | ... | ... |

### Summary

- **Lenses run**: [list]
- **Blockers**: [N] (must fix before commit)
- **Warnings**: [N]
- **Notes**: [N]
- **Verdict**: [READY TO COMMIT | FIX BLOCKERS FIRST | REVIEW WARNINGS]
```

## Do not

- Do not review your own edit in the context that produced it. Spawn a subagent.
- Do not report a check as passed without running it. `venv/bin/python -m pytest tests/ -x`
  is the project's test command — run it, don't assert it. Use `venv/bin/python`, not bare
  `python`: the system interpreter lacks `pytest-cov`, so `pytest.ini`'s `addopts` makes it
  die with `unrecognized arguments: --cov=.`, which reads as a broken config rather than a
  missing dependency.
- Do not resolve a finding by weakening the check that produced it.
