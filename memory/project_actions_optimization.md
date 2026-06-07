---
name: GitHub Actions minutes optimization
description: Optimized CI workflows to reduce Actions usage after hitting 3000 min/month limit (2026-03-30)
type: project
---

Hit GitHub Actions 3,000 min/month limit on ducroq account (2026-03-30). Resets April 1.

Changes made to energyDataHub:
- Added path filters to test.yml — data-only pushes no longer trigger tests (~270 min/month saved)
- Dropped Python 3.13 from test matrix — production is 3.12 (~90 min/month saved)
- Deleted redundant daily-update.yml — collect-data.yml already triggers Netlify rebuild

Opened issues for other repos:
- ducroq/NexusMind#110 — biggest consumer (~900 min/month), scheduled deploys every 2-4 hrs
- ducroq/Aegis#9 — 3 daily scheduled workflows (~180 min/month)

**Why:** Account-wide budget is shared across all repos. energyDataHub was ~360 min/month, now ~90.

**How to apply:** If adding new workflows or scheduled triggers, check account-wide budget impact first.
