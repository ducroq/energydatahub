---
name: ENTSO-E backfill old files
description: 26 early data files (Sep-Oct 2025) have malformed timestamps preventing re-save after backfill
type: project
---

26 files from Sep 28 - Oct 24, 2025 have malformed timestamps in their existing (non-ENTSO-E) datasets. `save_data_file()` validation rejects them when trying to patch in ENTSO-E data.

**Why:** These files predate the timestamp validation added later. The existing data (weather, etc.) has bad timezone offsets.

**How to apply:** If these files ever need fixing, the existing timestamps need normalizing first — not just adding ENTSO-E data. User decided to skip these for now (2026-03-28).
