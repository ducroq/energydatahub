"""
Shared rate-limit budget for Open-Meteo collectors.

File: collectors/_openmeteo_shared.py
Created: 2026-06-06
Author: Energy Data Hub Project

Why this exists
---------------
Six OpenMeteo* collectors run concurrently in ``data_fetcher.py``
(strategic weather, strategic solar, demand weather, offshore wind, buurt
weather, buurt solar). Before this module each collector owned its own
``asyncio.Semaphore(1)``, so the *real* budget against Open-Meteo's API
was ``n_collectors × 1`` — a number that silently drifted every time
someone added a new collector. That's exactly how the 2026-06-05 buurt
additions broke the previous ``Semaphore(2)+0.1s`` budget (CI run
27068482501, HTTP 429 storm).

Hoisting the semaphore to module level decouples peak concurrency from
collector count: adding a 7th collector no longer requires retuning each
file. See issue #11.

Tuning
------
Free-tier limits: https://open-meteo.com/en/docs (consult the live docs
rather than trusting a number in this docstring — Open-Meteo has changed
the budget more than once). ``OPENMETEO_SEMAPHORE_CAP = 5`` is below the
old per-collector × 6 = 6 concurrent peak, so this is strictly more
conservative than the pre-#11 code. Raise/lower by changing the constant
here; no per-collector edit needed.

Fairness note
-------------
``asyncio.Semaphore`` is FIFO. With a cap below the collector count, the
first-scheduled collector's tasks queue ahead of late-scheduled tasks.
In practice ``asyncio.gather`` interleaves task creation roughly fairly,
and per-collector location counts are small (2–6 each), so monopolisation
is bounded. If a future collector with 20+ locations is added, revisit
this — either raise the cap or move to a token-bucket scheme.

Singleton lifecycle
-------------------
This module is imported once per process; the semaphore is constructed at
import time and never recreated. ``importlib.reload(_openmeteo_shared)``
would create a fresh semaphore and orphan any in-flight acquisitions on
the old one — do NOT reload this module inside tests or notebooks. The
``hasattr`` guard below makes accidental reloads a no-op.
"""

import asyncio

OPENMETEO_SEMAPHORE_CAP: int = 5
OPENMETEO_GAP_SECONDS: float = 0.5

# Module-level construction is safe on Python 3.10+ (no running loop
# required). The project's minimum is 3.12. The hasattr guard makes a
# subsequent importlib.reload() a no-op so we don't orphan in-flight
# acquisitions on the previous instance.
if 'OPENMETEO_SEMAPHORE' not in globals():
    OPENMETEO_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(OPENMETEO_SEMAPHORE_CAP)
