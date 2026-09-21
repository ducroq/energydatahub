# Review profile — energydatahub

Read by `review-changes` Step 1. **This file is the project's half of that skill**: the skill is
the machinery and ships identically everywhere; everything below is specific to this repo.

MIGRATED 2026-09-14 from an inert project-local `review-changes` copy, which had been shadowed by
the user-global skill since framework v1.40.0. **Rows reviewed against the installed skill
2026-09-21** — the Depth column and the `shell-and-yaml-correctness` trigger were both wrong; see
below. The deleted copy remains in this repo's git history.

## Risk tiers

| Tier | File patterns |
|------|-------------|
| **HIGH** | `collectors/**`, `utils/**`, `/data_fetcher.py`, `scripts/**`, `.github/workflows/**`, `.claude/**`, `.gitignore`, `settings.ini`, `data/_shape_signatures.json`, `data/_upstream_empty_streak.json`, `tests/**`, `pytest.ini`, `requirements.txt` |
| **MEDIUM** | `CLAUDE.md`, `docs/**`, `/README.md`, `.github/dependabot.yml` |
| **LOW** | `memory/**`, `memory/work-items/**`, `data/**`, `legacy/**` |

**There is deliberately no Depth column.** The skill gates each lens on the tier itself
(guarantee-preservation = HIGH only; doc-accuracy = MEDIUM and HIGH; shell-correctness = HIGH and
only when shell files changed; adversarial = always), so a Depth column here can only restate that
or contradict it — and a stale one is read as authority.

**What actually runs at HIGH here is FOUR lenses**, because the skill runs this file's
`Project lenses` *in addition to* its own four (SKILL.md, Step 2): adversarial +
guarantee-preservation + doc-accuracy + `end-to-end-trace`, and five when shell or YAML changed.
⚠️ The deleted Depth column said "full battery (4-5 lenses)" and was **right**; the note that
replaced it on 2026-09-21 said "the skill ships four lenses total and fires three", which forgot
the project lenses. Corrected the same day, by the review of this very change.

**Read the cost here, before spawning anything.** The rules that override this table upward, and
are the usual reason a review is expensive:

- a diff over **200 changed lines** (SKILL.md, magnitude gate);
- **any diff that removes or loosens a check** — a deleted guard, a weakened assertion, a
  broadened exclusion, an added gate exemption;
- **any change to a shell script or an executable, wherever it lives.**

⚠️ The third is a predicate about the FILE, not a path glob. `scripts/**` is HIGH *tier* because
shell tends to live there, but a mode-644 Python file under `scripts/` does **not** trip the
carve-out. An earlier version of this section said "ANY change under `scripts/**` forces full
depth" — the same path-list-instead-of-predicate error corrected below for
`shell-and-yaml-correctness`, made one section above it, and caught by the review it was written
for. Check the mode and the shebang, not the directory.

Trimming tiers cannot make any of the three cheaper — only splitting the change can.

`**` crosses directory levels; a leading `/` anchors to the repo root. **The most specific
matching pattern wins.** Where no pattern is more specific, take the highest tier.

## Guarantee surfaces

⚠️ **Every path here must sit in the HIGH row above.** The guarantee lens is HIGH-gated, so a
guarantee on a lower-tiered path can never fire — and the report renders that as a clean pass.
Check it in this direction: read each entry, then find its tier above.

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
  CRITICAL_FEEDS escalate, and a diff consisting ONLY of
  `metadata.collector_quality_issues` is excused entirely (2026-09-21) — four outcomes,
  not three. Volatile membership stays derived from history, not hand-listed.
- All timestamps normalized to Europe/Amsterdam.

## Test baseline

```bash
venv/bin/python -m pytest tests/ -x          # full suite; 924 passing at 2026-09-21
venv/bin/python -m pytest tests/unit/test_x.py -v   # one file
```

Use `venv/bin/python`, not the system interpreter: the venv is uv-managed and has no `pip`, and
`pytest.ini`'s `addopts` carries `--cov=.`, which the system interpreter rejects as
`unrecognized arguments` — a missing dependency that reads as a broken config. A single-file run
fails the 20% coverage floor by design; only the full suite's exit code means anything.
There is no lint step in this project.

## Always-full-depth carve-outs (project additions)

None beyond the skill's own list.

## Project lenses

- `shell-and-yaml-correctness` — HIGH only, and **only when the diff actually contains shell or
  YAML**: a changed `*.sh`, a changed `.github/workflows/**`, or a changed executable with a
  shell shebang. ⚠️ **Corrected 2026-09-21.** The trigger read `scripts/**`, which fires this
  lens on every Python change under `scripts/` — a shell reviewer sent at a diff with no shell in
  it, on the commonest kind of change this repo makes there (`detect_schema_drift.py`,
  `report_span_shortfall.py`). One wasted subagent per review, for months. The framework's own
  `shell-correctness` lens has the condition right; this one is the project addition that copied
  the path list instead of the predicate.

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

- `end-to-end-trace` — HIGH only, and only when the diff adds or changes a field, check, severity
  or quality signal. (Reviewed 2026-09-21: trigger kept as written. Unlike its sibling above, this
  one is a predicate about the change, not a path list, so it had nothing to correct — the raw
  "fires on the tiers named in its original heading" phrasing was migration scaffolding, not a
  rule.)

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


## Project additions to the shipped lens prompts

None.

## Project procedure kept with the profile

None.
