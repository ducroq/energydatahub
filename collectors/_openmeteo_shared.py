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
the budget more than once). ``OPENMETEO_SEMAPHORE_CAP = 6`` matches the
pre-#11 ``per-collector Semaphore(1) × 6 collectors`` peak. Raise/lower
by changing the constant here; no per-collector edit needed.

Why cap == collector count, not lower
-------------------------------------
First post-#11 deployment used cap=5. Two consecutive runs on 2026-06-07
exposed a regression: late-scheduled collectors (offshore wind + both
buurt) consistently got ``Connection timeout to host api.open-meteo.com``
while the early strategic/demand collectors succeeded. Open-Meteo's CDN
appears to apply per-source connection-cooldown after a burst, and the
late requests queued behind the shared FIFO arrived during that window.
Pre-#11 the 6 collectors ran with their own ``Semaphore(1)`` each — so 6
parallel sessions instead of one serialised queue — which avoided the
cooldown window. Setting cap == collector count restores that pattern
while preserving the architectural win (one place to tune).

If a 7th OpenMeteo collector is added, raise this constant to 7 (and
update this comment). Lowering below collector count risks reintroducing
the timeout regression — investigate Open-Meteo's behavior first.

Per-request retry
-----------------
The cap above mitigates the upstream cooldown trigger; ``MAX_RETRIES`` /
``RETRY_INITIAL_DELAY_SECONDS`` provide belt-and-suspenders resilience
for any transient (timeout, 429, 503) regardless of root cause. Used by
each OpenMeteo collector's per-location fetch helper.

Singleton lifecycle
-------------------
This module is imported once per process; the semaphore is constructed at
import time and never recreated. ``importlib.reload(_openmeteo_shared)``
would create a fresh semaphore and orphan any in-flight acquisitions on
the old one — do NOT reload this module inside tests or notebooks. The
``hasattr`` guard below makes accidental reloads a no-op.
"""

import asyncio
import logging

OPENMETEO_SEMAPHORE_CAP: int = 6
OPENMETEO_GAP_SECONDS: float = 0.5

# Per-request retry budget for per-location fetches. Three attempts with
# exponential backoff (1s → 2s → 4s). Triggered whenever the fetch helper
# returns ``data=None`` (covers aiohttp timeouts, HTTP non-200, transport
# errors — all caught and represented as ``{name, data=None, error=...}``
# by the collectors' ``_fetch_location_data``). Total worst-case wall time
# per location: ~3 attempts + 1s + 2s waits ≈ 6s before final failure.
MAX_RETRIES: int = 3
RETRY_INITIAL_DELAY_SECONDS: float = 1.0
RETRY_BACKOFF_BASE: float = 2.0

# Module-level construction is safe on Python 3.10+ (no running loop
# required). The project's minimum is 3.12. The hasattr guard makes a
# subsequent importlib.reload() a no-op so we don't orphan in-flight
# acquisitions on the previous instance.
if 'OPENMETEO_SEMAPHORE' not in globals():
    OPENMETEO_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(OPENMETEO_SEMAPHORE_CAP)


async def fetch_location_with_retry(
    session,
    location,
    fetch_fn,
    logger: logging.Logger,
    apply_gap: bool = True,
):
    """Shared per-location wrapper for the three OpenMeteo* collectors.

    Acquires ``OPENMETEO_SEMAPHORE``, optionally sleeps ``OPENMETEO_GAP_SECONDS``
    between requests, then calls ``fetch_fn(session, location)`` — which must
    return ``{'name': str, 'data': Any|None, 'error': str|None}`` per the
    OpenMeteo collectors' ``_fetch_location_data`` contract.

    On failure (``data is None``), retries up to ``MAX_RETRIES`` times with
    exponential backoff (``RETRY_INITIAL_DELAY_SECONDS`` × ``RETRY_BACKOFF_BASE^n``).
    Belt-and-suspenders resilience against the offshore+buurt timeouts seen
    on 2026-06-07 — even after the cap was raised to match collector count,
    transient Open-Meteo issues should self-heal without a full re-run.

    Args:
        session: shared aiohttp.ClientSession from the caller.
        location: dict with ``name``, ``lat``, ``lon`` (and optional extras).
        fetch_fn: async callable that does the actual HTTP request.
        logger: caller's logger for retry/failure messages.
        apply_gap: when True (typical: i > 0 in the caller's loop), waits
            ``OPENMETEO_GAP_SECONDS`` inside the semaphore. Set False for the
            first request of the batch so the wave isn't pre-delayed.

    Returns:
        The last response dict from ``fetch_fn``. ``data`` is the API payload
        on success or None after all retries exhausted; ``error`` carries the
        last failure message.
    """
    last_response = {'name': location.get('name', '?'), 'data': None, 'error': 'no attempt made'}
    for attempt in range(1, MAX_RETRIES + 1):
        async with OPENMETEO_SEMAPHORE:
            if apply_gap:
                await asyncio.sleep(OPENMETEO_GAP_SECONDS)
            last_response = await fetch_fn(session, location)
        if last_response.get('data') is not None:
            if attempt > 1:
                logger.info(f"{location['name']}: succeeded on attempt {attempt}/{MAX_RETRIES}")
            return last_response
        if attempt < MAX_RETRIES:
            delay = RETRY_INITIAL_DELAY_SECONDS * (RETRY_BACKOFF_BASE ** (attempt - 1))
            err_snippet = str(last_response.get('error', '?'))[:80]
            logger.info(
                f"{location['name']}: attempt {attempt}/{MAX_RETRIES} failed "
                f"({err_snippet}), retrying in {delay:.1f}s"
            )
            await asyncio.sleep(delay)
    logger.warning(
        f"{location['name']}: all {MAX_RETRIES} attempts failed; "
        f"final error: {str(last_response.get('error', '?'))[:120]}"
    )
    return last_response
