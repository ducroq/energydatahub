---
name: Adding a published dataset
description: 8-touchpoint checklist that must be hit when wiring a new collector into the publish set
type: project
---

Adding a published dataset requires touching **three files in lock-step**. Missing one silently breaks publishing — confirmed BLOCKER on commit c40a53b (#3 hydro wire-in) which the multi-model review battery caught. Repeated reminder because the failure is silent: the file gets collected, validated, committed to `data/`, but never copied to `docs/` and never reaches GitHub Pages.

## In `data_fetcher.py`

1. **Collector import** at top of file
2. **Instantiate** in the pipeline setup
3. **`tasks.append`** to the `asyncio.gather` list
4. **Unpack tuple** (`dataset, fixed_count`) when results come back
5. **Save block** writing `dataset.to_dict()` to the per-feed JSON
6. **`published_feeds` sidecar** entry so `_shape_signatures.json` includes the feed
7. **`quality_datasets`** entry so the FMEA gate runs

## In `utils/data_quality.py`

8. **`EXPECTED_DATA_TYPE` registry** entry — required MITM defense (CWE-20). The `test_published_feeds_all_pinned` test catches a missing entry only.

Plus as cadence + validation needs dictate:
- `EXPECTED_MIN_POINTS` — completeness floor
- `STALENESS_OVERRIDES` — for non-daily cadences
- `DATASET_MISSING_SEVERITY` — critical / warning / info on absence
- `FIELD_RANGES_BY_TYPE` — per-field bound validation (use the BaseCollector `_add_quality_issue` hook for structured signals, not flat warnings)

## In `.github/workflows/collect-data.yml`

The same feed name must appear in **both** parallel hard-coded lists:
- **Completeness tripwire list** (~line 54) — warns on missing publish output
- **Docs-prepare copy list** (~line 118) — actually copies the file into the published `docs/` set

No test asserts these two lists match. Update both together. This is the same shape as the "parallel hard-coded registries" pattern collapsed in `data_quality.py` (DATASET_MISSING_SEVERITY) — a YAML instance of the same anti-pattern.

## Why this is its own file

Lives behind the CLAUDE.md "Adding a published dataset" task trigger. The full 8-step list was inlined in CLAUDE.md as a 1-paragraph table cell — load-bearing detail that bloated the project file past the 100-line heuristic. Reachable on-demand instead of auto-loaded saves ~7 lines from every session.
