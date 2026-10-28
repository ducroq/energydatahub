---
name: Adding a published dataset
description: What wiring a new feed into the publish set needs beyond what tests/unit/test_published_feed_registries.py enforces
type: project
---

Missing a touchpoint silently breaks publishing (BLOCKER on c40a53b, #3): the file is collected but never reaches `docs/`.

**Enforced by `tests/unit/test_published_feed_registries.py` (#85)** — add the feed, run it, and it names every list you missed:
the dated save, the copy to `data/<feed>.json` and the `published_feeds` sidecar in `data_fetcher.main`; `quality_datasets` (two exemptions, named in the test); `EXPECTED_DATA_TYPE`; both lists in `.github/workflows/collect-data.yml` (*Verify expected output files*, *Prepare docs folder with only current data*).
Run it with `venv/bin/python -m pytest tests/unit/test_published_feed_registries.py --no-cov`.

**Not enforced — do these by hand** in `data_fetcher.py`: import the collector, instantiate it, add a `(key, coro)` entry to `task_specs`, read it back with `results['<key>']`.
If it is a one-day feed, add it to `FEED_FILES` in `scripts/backfill_missed_run.py`. If Augur consumes it, consider `CRITICAL_FEEDS` in `scripts/detect_schema_drift.py`.

**As cadence and validation need** (`utils/data_quality.py`; intentionally partial registries):
`EXPECTED_MIN_POINTS`, `STALENESS_OVERRIDES`, `DATASET_MISSING_SEVERITY` (absent = a missing feed never reaches the quality report), `FIELD_RANGES_BY_TYPE`.
