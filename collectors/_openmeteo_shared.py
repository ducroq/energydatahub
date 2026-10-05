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
for transients (timeout, 503, a malformed body). A 429 takes the separate
rate-limit budget below. Used by each OpenMeteo collector's per-location
fetch helper.

Rate-limit retry (2026-10-05)
-----------------------------
A 429 / ``Too many concurrent requests`` is handled on a separate, longer
budget, because the transient budget above cannot outlast a storm. Run
``37226371364`` (2026-10-04): Open-Meteo refused requests for ~60s
(18:57:52-18:58:53), starting with our very first request. The transient
budget waits only 1s + 2s between attempts (semaphore queueing stretched
that to ~25s for some locations, still well short), so 29 per-location
fetches were lost to the storm across the six collectors, plus two to
malformed 200 bodies. Only the six that ran as it thinned survived. Two
partial feeds then cost the whole publish at the drift gate.

So a rate-limited response (1) gets up to ``RATE_LIMIT_MAX_RETRIES`` retries
on a 5s -> 10s -> 20s -> 30s -> 30s schedule (+ jitter, ~95s+ in total), and
(2) sets a MODULE-WIDE cooldown that every location checks before each
attempt, both before and after taking a semaphore slot. Without (2) the other workers keep firing into the storm
and each 429 re-arms it. The cooldown costs nothing on a healthy run: it is
only ever set by a 429. Because the cooldown gates EVERY attempt, a
non-429 failure during a storm (e.g. a malformed 200 body, two of which
appeared on 2026-10-04) also waits it out rather than burning its 1s + 2s
transient budget inside the storm.

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
import random
import time

OPENMETEO_SEMAPHORE_CAP: int = 6
OPENMETEO_GAP_SECONDS: float = 0.5

# Transient retry budget for per-location fetches: MAX_RETRIES is the total
# number of attempts (so 2 retries), waiting 1s then 2s. Applies whenever the
# fetch helper returns ``data=None`` for anything but a rate limit (aiohttp
# timeouts, non-429 HTTP errors, transport errors, malformed bodies — all
# represented as ``{name, data=None, error=...}`` by the collectors'
# ``_fetch_location_data``). A rate-limited location can additionally spend
# the RATE_LIMIT_* schedule below (~95s + jitter) plus shared cooldown waits.
MAX_RETRIES: int = 3
RETRY_INITIAL_DELAY_SECONDS: float = 1.0
RETRY_BACKOFF_BASE: float = 2.0

# Rate-limit budget — see "Rate-limit retry" in the module docstring. These
# retries do NOT consume MAX_RETRIES: a location can take the full rate-limit
# schedule and still have its transient budget for a timeout afterwards.
RATE_LIMIT_MAX_RETRIES: int = 5
RATE_LIMIT_INITIAL_DELAY_SECONDS: float = 5.0
RATE_LIMIT_MAX_DELAY_SECONDS: float = 30.0
RATE_LIMIT_JITTER_SECONDS: float = 2.0

# Body markers Open-Meteo has been observed to send with a 429. The status
# code is the primary signal; the markers cover a fetch_fn that does not
# report one.
_RATE_LIMIT_MARKERS = ('too many concurrent requests', 'too many requests')

# Monotonic deadline before which no location may start a new attempt. Set
# only by a rate-limited response; see `_extend_cooldown`.
_cooldown_until: float = 0.0

# Module-level construction is safe on Python 3.10+ (no running loop
# required). The project's minimum is 3.12. The hasattr guard makes a
# subsequent importlib.reload() a no-op so we don't orphan in-flight
# acquisitions on the previous instance.
if 'OPENMETEO_SEMAPHORE' not in globals():
    OPENMETEO_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(OPENMETEO_SEMAPHORE_CAP)


def is_rate_limited(response: dict) -> bool:
    """True when a fetch_fn response is Open-Meteo refusing us for rate.

    Prefers the ``status`` key (the collectors set it on any non-200) and
    falls back to the body markers for a response that carries none.
    """
    if response.get('status') == 429:
        return True
    error = str(response.get('error') or '').lower()
    return any(marker in error for marker in _RATE_LIMIT_MARKERS)


def _extend_cooldown(seconds: float) -> None:
    """Push the shared cooldown out to at least ``now + seconds``. Never shortens it."""
    global _cooldown_until
    _cooldown_until = max(_cooldown_until, time.monotonic() + seconds)


def _cooldown_remaining() -> float:
    return _cooldown_until - time.monotonic()


async def _wait_for_cooldown() -> None:
    """Sleep until no shared cooldown is active. No sleep at all when none is.

    Loops, because another location's 429 can extend the cooldown while we
    sleep — a single sleep would wake into a cooldown that is still running.
    """
    while (remaining := _cooldown_remaining()) > 0:
        # Jitter so the waiting locations do not all fire at the same instant
        # the cooldown lifts — that would be the burst that re-arms it.
        await asyncio.sleep(remaining + random.uniform(0, RATE_LIMIT_JITTER_SECONDS))


def reset_rate_limit_state() -> None:
    """Clear the shared cooldown. For tests; production never needs it."""
    global _cooldown_until
    _cooldown_until = 0.0


async def fetch_location_with_retry(
    session,
    location,
    fetch_fn,
    logger: logging.Logger,
    apply_gap: bool = True,
):
    """Shared per-location wrapper for the three OpenMeteo* collector classes.

    Acquires ``OPENMETEO_SEMAPHORE``, optionally sleeps ``OPENMETEO_GAP_SECONDS``
    between requests, then calls ``fetch_fn(session, location)`` — which must
    return ``{'name': str, 'data': Any|None, 'error': str|None}`` per the
    OpenMeteo collectors' ``_fetch_location_data`` contract.

    On failure (``data is None``), makes up to ``MAX_RETRIES`` attempts in total with
    exponential backoff (``RETRY_INITIAL_DELAY_SECONDS`` × ``RETRY_BACKOFF_BASE^n``).
    A rate-limited failure (`is_rate_limited`) instead takes the separate
    ``RATE_LIMIT_*`` schedule and arms the shared cooldown; it does not consume
    the ``MAX_RETRIES`` budget.
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
    attempts = 0
    transient_failures = 0
    rate_limit_retries = 0
    while True:
        # The cooldown is checked AGAIN inside the semaphore. Checking only
        # before it is not enough: with more locations than slots, most workers
        # pass the outer check at t=0, queue on the semaphore, and then fire
        # into a cooldown armed while they queued. Measured in review before
        # this re-check: 135 of 178 attempts started during an active cooldown
        # (38 locations, 60s storm). On a hit, release the slot and wait again.
        await _wait_for_cooldown()
        async with OPENMETEO_SEMAPHORE:
            if apply_gap:
                await asyncio.sleep(OPENMETEO_GAP_SECONDS)
            if _cooldown_remaining() > 0:
                continue
            last_response = await fetch_fn(session, location)
        attempts += 1
        if last_response.get('data') is not None:
            if attempts > 1:
                logger.info(f"{location['name']}: succeeded on attempt {attempts}")
            return last_response
        err_snippet = str(last_response.get('error', '?'))[:80]

        if is_rate_limited(last_response):
            if rate_limit_retries >= RATE_LIMIT_MAX_RETRIES:
                break
            delay = min(
                RATE_LIMIT_INITIAL_DELAY_SECONDS * (RETRY_BACKOFF_BASE ** rate_limit_retries),
                RATE_LIMIT_MAX_DELAY_SECONDS,
            ) + random.uniform(0, RATE_LIMIT_JITTER_SECONDS)
            rate_limit_retries += 1
            _extend_cooldown(delay)
            logger.info(
                f"{location['name']}: rate-limited (retry {rate_limit_retries}/"
                f"{RATE_LIMIT_MAX_RETRIES}, {err_snippet}), backing off {delay:.1f}s"
            )
            await asyncio.sleep(delay)
            continue

        transient_failures += 1
        if transient_failures >= MAX_RETRIES:
            break
        delay = RETRY_INITIAL_DELAY_SECONDS * (RETRY_BACKOFF_BASE ** (transient_failures - 1))
        logger.info(
            f"{location['name']}: attempt {transient_failures}/{MAX_RETRIES} failed "
            f"({err_snippet}), retrying in {delay:.1f}s"
        )
        await asyncio.sleep(delay)
    logger.warning(
        f"{location['name']}: all {attempts} attempts failed "
        f"({rate_limit_retries} rate-limited); "
        f"final error: {str(last_response.get('error', '?'))[:120]}"
    )
    return last_response


def record_location_delivery(collector, delivered: dict) -> None:
    """Record which configured locations actually returned data this run.

    Why this exists
    ---------------
    A per-location fetch that exhausts its retries simply omits that location
    from the results dict. Before this, the omission was visible only as a
    `logger.warning` in the run log: `_get_metadata` built `locations` and
    `location_count` from `self.locations` — the CONFIGURED list — so a
    degraded feed published an envelope asserting a location that was not in
    `data`. Nothing downstream could tell. `validate_completeness` passed
    comfortably (768 points → 384 is still far above the floor), no quality
    issue was raised, and Augur saw a self-consistent-looking feed with half
    its locations missing.

    That went unnoticed until 2026-08-14, when `Elsweide_Arnhem_NL` dropped
    out of both buurt feeds and the schema-drift tripwire — which compares
    `data` key sets and knows nothing about weather — turned out to be the
    only thing in the pipeline that noticed at all. It failed the publish of
    18 healthy feeds to say so.

    This routes the signal properly: the collector records what was delivered,
    `_get_metadata` publishes the delivered set, and `_add_quality_issue`
    surfaces the dropout through the existing DQ gate into the committed
    quality report. No new state file, no new registry.

    Sets `collector._delivered_locations` (list of names, sorted by the
    configured order) for `_get_metadata` to consume.

    Args:
        collector: the OpenMeteo* collector instance (needs `.locations` and
            BaseCollector's `_add_quality_issue`).
        delivered: the results mapping location_name -> data built by
            `_fetch_raw_data`.
    """
    configured = [loc['name'] for loc in collector.locations]
    collector._delivered_locations = [n for n in configured if n in delivered]
    missing = [n for n in configured if n not in delivered]
    if not missing:
        return
    collector._add_quality_issue(
        check_name='location_completeness',
        severity='warning',
        message=(
            f"{len(missing)} of {len(configured)} location(s) returned no data "
            f"after retries: {', '.join(missing)}"
        ),
        details={
            'requested': configured,
            'delivered': list(collector._delivered_locations),
            'missing': missing,
        },
    )


def published_locations(collector) -> list:
    """The location names to publish in metadata: DELIVERED, not configured.

    Falls back to the configured list when `record_location_delivery` has not
    run — `_get_metadata` is reachable without a fetch (direct calls, tests),
    and the configured list is the correct answer there.
    """
    delivered = getattr(collector, '_delivered_locations', None)
    if delivered is None:
        return [loc['name'] for loc in collector.locations]
    return list(delivered)
