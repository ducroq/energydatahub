---
name: session-2026-10-05
description: Open-Meteo 429 storm cost the 10-04 publish; retry fix, backfill of six one-day feeds, first read-surface archive pass
metadata:
  type: project
---

# Session 2026-10-05

**Ask:** diagnose the 2026-10-04 publish failure, then "fix it all, including the backfill", and trigger Augur once backfilled; then wrap up, prune the read surface, curate, commit.

**Threads:**
- Diagnosis — closed. Run `37226371364` failed the drift gate on `demand_weather_forecast` (8 of 11 locations lost, below the majority floor) and `weather_forecast_multi_location` (a CRITICAL_FEED, so never downgraded). Both are by design. The cause was a ~60 s Open-Meteo 429 storm that refused our first request; the old 1 s + 2 s retry gave up inside it. Reproduced locally to the exact pruned hash `92fea44d…`.
- Retry fix — closed, `a1a944e`. A separate 429 budget plus a module-wide cooldown, re-checked inside the semaphore. The review's adversarial lens found the inner re-check missing (135 of 178 attempts fired into a cooldown). Each new test goes red with its fix removed. Suite 947 green, CI green.
- Backfill — closed, `a095359` + `9baddf4`. New `scripts/backfill_missed_run.py`. All six one-day feeds were recovered, each with day coverage matching the 10-03 files. The files are gitignored, so they need `git add -f`.
- Augur trigger — closed by not triggering. Its nightly run pulls `data/` and rebuilds from every file; a manual start would wait on `wait_for_edh.sh` for tonight's publish anyway.
- Read surface — partial. Pass 1 (lossless archive moves) done; pass 2 is `memory/work-items/read-surface-thinning.md`.

**Lessons logged:** the fake-clock test trap (gotcha log). H10 got a trigger-fired review.
