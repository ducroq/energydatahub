"""
Shared rate-limit budget for Open-Meteo collectors.

File: collectors/_openmeteo_shared.py
Created: 2026-06-06
Author: Energy Data Hub Project

Why this exists
---------------
Five OpenMeteo* collectors run concurrently in data_fetcher.py (strategic
weather + solar, offshore wind, buurt weather + solar). Before this module
each collector owned its own `asyncio.Semaphore(1)`, so the *real* budget
against Open-Meteo's API was `n_collectors × 1` — a number that silently
drifts every time someone adds a new collector. That's exactly how the
2026-06-05 buurt additions broke the previous Semaphore(2)+0.1s budget
(CI run 27068482501, HTTP 429 storm).

Hoisting the semaphore to module level decouples peak concurrency from
collector count: adding a 6th collector no longer requires retuning each
file. See issue #11.

Tuning
------
Open-Meteo free tier (as of 2026-06): ~10 req/s burst, ~600/min sustained.
`OPENMETEO_SEMAPHORE_CAP = 5` keeps us at ~5 concurrent × ~2 req/s/slot
(via the 0.5 s gap) = ~10 req/s — matching the empirically-safe budget
from PR #10. To raise/lower, change the constant here; no per-collector
edit needed.
"""

import asyncio

OPENMETEO_SEMAPHORE_CAP: int = 5
OPENMETEO_GAP_SECONDS: float = 0.5

# Module-level construction is safe on Python 3.10+ (no running loop
# required). The project's minimum is 3.12 (see .github/workflows/test.yml).
OPENMETEO_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(OPENMETEO_SEMAPHORE_CAP)
