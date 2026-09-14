# Review profile — energydatahub

Read by `review-changes` Step 1. **This file is the project's half of that skill**: the skill is
the machinery and ships identically everywhere; everything below is specific to this repo.

⚠️ **MIGRATED 2026-09-14 from an inert project-local `review-changes` copy**, which had been
shadowed by the user-global skill since framework v1.40.0. Nothing below had been in force since
then. Recovered verbatim before that copy was deleted (it remains in this repo's git history);
**review each row — it was written against an older framework and has not been exercised since.**

## Risk tiers

| Tier | File patterns | Depth |
|------|-------------|-------|
| **HIGH** | `collectors/**`, `utils/**`, `/data_fetcher.py`, `scripts/**`, `.github/workflows/**`, `.claude/**`, `.gitignore`, `settings.ini`, `data/_shape_signatures.json`, `data/_upstream_empty_streak.json`, `tests/**`, `pytest.ini`, `requirements.txt` | Full battery (4-5 lenses) |
| **MEDIUM** | `CLAUDE.md`, `docs/**`, `/README.md`, `.github/dependabot.yml` | Two lenses (adversarial + doc-accuracy) |
| **LOW** | `memory/**`, `memory/work-items/**`, `data/**`, `legacy/**` | One lens (adversarial) |

⚠️ **The Depth column is carried over verbatim and is the OLD default.** Framework v1.45.1 ships a
note offering a cheaper HIGH — one adversarial lens plus two conditional ones — with its evidence
and its counter-evidence. Read that before keeping or changing this.

`**` crosses directory levels; a leading `/` anchors to the repo root. **The most specific
matching pattern wins.** Where no pattern is more specific, take the highest tier.

## Guarantee surfaces

⚠️ **Every path here must sit in the HIGH row above.** The guarantee lens is HIGH-gated, so a
guarantee on a lower-tiered path can never fire — and the report renders that as a clean pass.
Check it in this direction: read each entry, then find its tier above.

- utils/schema_registry.py: any shape change bumps CURRENT_SCHEMA_VERSION *and* adds a
- utils/data_types.py: the {metadata, data} envelope. EnhancedDataSet/CombinedDataSet
- utils/data_quality.py: DATASET_MISSING_SEVERITY is the SINGLE registry for missing-feed
- utils/secure_data_handler.py: AES-CBC + HMAC-SHA256 on everything published; keys never
- settings.ini: `encryption = 1`. This is the switch that decides whether the published
- data/_shape_signatures.json: the schema-drift tripwire's baseline — it must stay tracked,
- collectors/base.py: retry + circuit breaker + validation contract. NonRetryableError
- data_fetcher.py: the 8-touchpoint published-dataset checklist in
- .github/workflows/collect-data.yml: completeness tripwire, schema-drift tripwire, quality
- scripts/detect_schema_drift.py: within-feed drift fails, catalog drift warns, removed
- All timestamps normalized to Europe/Amsterdam.

## Test baseline

```bash
# Not recorded in the inert copy. Add this project's test and lint commands.
```

## Always-full-depth carve-outs (project additions)

None beyond the skill's own list.

## Project lenses

- `shell-and-yaml-correctness` — fires on the tiers named in its original heading: `shell-and-yaml-correctness (HIGH only, when `scripts/**`, `*.sh`, or `.github/workflows/**` changed)`

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

- `end-to-end-trace` — fires on the tiers named in its original heading: `end-to-end-trace (HIGH only, when a new field/check/signal is added)`

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
