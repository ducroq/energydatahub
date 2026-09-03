"""
Process-wide circuit breaker shared by every collector that hits one host.

Why this exists
---------------
`BaseCollector` already has a circuit breaker, but it is keyed on the wrong
axis for this failure. It is per-INSTANCE state (`self._circuit_breaker`) and
it is consulted once per `collect()`. `data_fetcher` builds EIGHT ENTSO-E
collectors against `web-api.tp.entsoe.eu` — two `EntsoeCollector`, two
`EntsoeGenerationCollector`, plus flows, hydro, load and wind — so each
breaker only ever sees its own single failure per retry round. The default
`failure_threshold` of 5 is therefore unreachable inside a run that makes at
most 4 rounds, and eight breakers independently hammer one dead host.

The 2026-08-31 run (`33440325190`) spent 48 minutes and ~295 requests on an
API that returned HTTP 503 to every single one; 32 of those 48 minutes were
the first pass alone, before any retry round had started. 2026-09-01
(`33547613279`) repeated it (#52). See `memory/gotcha-log.md` for the evidence
that the platform was in scheduled maintenance throughout, and so that no
request could have succeeded.

This module adds the missing axis: state keyed by HOST, shared across
collector instances, and consulted per SUB-REQUEST rather than per collection.

What it deliberately is NOT
---------------------------
NOT a 503 special case. HTTP 503 is legitimately retryable and this project's
retry rounds are load-bearing: 2026-08-30 recovered and published cleanly
between the 2026-08-29 and 2026-08-31 failures. Short-circuiting on a status
code would trade a real recovery chance for speed. This breaker is
status-agnostic — it reacts to sustained failure of any kind, and re-probes
whenever a caller arrives after the cooldown.

Nor is the point wall-clock. The repo is public and Actions minutes are free.
The point is that 295 requests into an API that is telling us it is down is
poor citizenship toward a rate-limited upstream we depend on.

Design notes
------------
COUNTS EXHAUSTED SUB-REQUESTS, NOT HTTP ATTEMPTS. One `_retry_single` call
that burns all its attempts is one failure here. With the default threshold of
5 that means ~15 real HTTP attempts before the breaker opens — deliberately
conservative, because opening wrongly drops every remaining zone for the
cooldown, and partial delivery is meaningful in this pipeline (see
`_entsoe_shared`, which exists to track exactly those dropouts).

ANY SUCCESS CLOSES IT. `record_success` zeroes the counter, so the count means
"consecutive dead sub-requests". A host that is merely degraded — some zones
answering, some not — never trips it. Only a host where nothing works does.
Note the limit of what "success" observes: callers record it for any call that
did not raise, which includes an empty DataFrame. A host that degrades into
empty 200s rather than errors will keep the breaker closed.

AN EMPTY WINDOW IS NOT A HOST SIGNAL. Neither is a permanent 4xx for one query.
`NonRetryableError` and anything in `_retry_single`'s `non_host_exceptions`
(for ENTSO-E: entsoe-py's `NoMatchingDataError`) say something about that
request, not about the host, and are never recorded as host failures. This is
load-bearing, not hygiene: the NL cable borders are routinely unpublished, and
counting those empty responses would open the breaker on a healthy host and
suppress every remaining request in the process. The #52 review caught exactly
that before it shipped.

ONE PROBE PER COOLDOWN, AND NO STUCK-OPEN STATE. When the cooldown elapses
`allow()` restarts the clock and lets exactly one caller through; concurrent
callers arriving in the same window are refused because the clock has already
moved. That is the standard half-open probe, expressed so that a probe which
never reports back cannot wedge the breaker — there is no in-flight flag to
leak.

WHAT THE PROBE DOES AND DOES NOT RESCUE. Be precise about this, because the
two collector families have different topologies and the first draft of this
module got it backwards:

  - The six zone/border collectors (load, wind, generation ×2, flows, hydro)
    run ONCE each, inside `data_fetcher`'s single `asyncio.gather`. They get no
    retry round, and their collector-level backoffs are 1–2s, far under the
    60s cooldown. So once the breaker opens mid-gather it is effectively
    terminal for them: their remaining zones are suppressed and the run
    publishes without them. That is the intended trade — it is the "stop
    hammering a dead host" the module exists for — but it is a real cost, and
    it is why the false-open case above had to be fixed first.
  - The two price collectors (`EntsoeCollector`, the only members of
    `CRITICAL_DATASETS`) DO get up to 3 retry rounds 300s apart, and consult
    this breaker inline in `_fetch_raw_data`. The 60s cooldown is well under
    that gap, so every round begins with a live probe, and a success there
    closes the breaker.

So the breaker cannot end the run for the feed that decides whether the publish
happens at all. It can cost the zone feeds their run — deliberately, and only
once the host has failed 5 consecutive sub-requests for reasons that actually
implicate it.

State lives for the life of the process. Each scheduled run is a fresh
process, so there is nothing to reset in production; `reset_all()` exists for
tests (wired autouse in `tests/conftest.py`, because an open breaker leaking
between test modules is an order-dependent failure).

Known gaps, filed rather than fixed here: this state never reaches disk, so on
the `SystemExit(1)` path the diagnosis survives only in the Actions console
(#55); and a suppressed zone is indistinguishable downstream from one that was
never requested (#56).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

logger = logging.getLogger("collectors._host_breaker")


class HostBreakerOpenError(Exception):
    """No request was made: the shared breaker for this host is open.

    Deliberately a plain `Exception`, NOT a `NonRetryableError` and above all
    not an `UpstreamNoDataError`. A suppressed request is a host outage, and
    labelling it "source healthy, published no data" would route a genuine
    outage into the #38 grace window and let a price feed publish as a benign
    upstream gap. It must stay retryable so the collector-level retry and the
    orchestrator's retry rounds still get their probe.

    Raised only by collectors that make a single request per fetch and so
    cannot use `_retry_single`'s return-None convention — see
    `collectors/entsoe.py`.
    """

# Consecutive exhausted sub-requests before the host is considered dead.
DEFAULT_FAILURE_THRESHOLD = 5

# Seconds an open breaker refuses traffic before letting one probe through.
# Must stay comfortably below the orchestrator's 300s inter-round sleep so
# that every retry round starts with a live probe rather than a refusal.
DEFAULT_COOLDOWN_SECONDS = 60.0


@dataclass
class HostBreaker:
    """Shared failure state for a single upstream host.

    Not tied to any collector instance. Safe to call from concurrent
    coroutines on one event loop and from worker threads: every public method
    takes the instance lock, and no method awaits or blocks while holding it.
    """

    host: str
    failure_threshold: int = DEFAULT_FAILURE_THRESHOLD
    cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS
    time_source: Callable[[], float] = time.monotonic

    consecutive_failures: int = 0
    opened_at: Optional[float] = None
    suppressed_requests: int = 0
    open_events: int = 0

    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    @property
    def is_open(self) -> bool:
        with self._lock:
            return self.opened_at is not None

    def allow(self) -> bool:
        """Whether a sub-request may be attempted now.

        Mutates state when open: the caller that finds the cooldown elapsed
        restarts the clock and is let through as the single probe for this
        window. See the module docstring for why the probe is expressed this
        way rather than with an in-flight flag.
        """
        with self._lock:
            if self.opened_at is None:
                return True

            elapsed = self.time_source() - self.opened_at
            if elapsed >= self.cooldown_seconds:
                # Restart the clock so concurrent callers in this same window
                # are refused; this caller is the probe.
                self.opened_at = self.time_source()
                logger.info(
                    f"Host breaker for {self.host}: cooldown elapsed after "
                    f"{elapsed:.0f}s — allowing one probe request"
                )
                return True

            self.suppressed_requests += 1
            return False

    def record_success(self) -> None:
        """A sub-request against this host returned data. Close the breaker."""
        with self._lock:
            was_open = self.opened_at is not None
            self.consecutive_failures = 0
            self.opened_at = None

        if was_open:
            logger.info(
                f"Host breaker for {self.host}: CLOSED — probe succeeded, "
                "host is answering again"
            )

    def record_failure(self) -> None:
        """A sub-request exhausted its attempts against this host.

        Call once per exhausted `_retry_single`, not once per HTTP attempt,
        and never for a `NonRetryableError` — see the module docstring.
        """
        with self._lock:
            self.consecutive_failures += 1
            if self.consecutive_failures < self.failure_threshold:
                return

            already_open = self.opened_at is not None
            self.opened_at = self.time_source()
            if already_open:
                # A failed probe. `self.opened_at` was just refreshed above, so
                # this buys another full cooldown. Reached from `_retry_single`
                # via its `break`-not-`return` on refusal, and from
                # `EntsoeCollector`'s inline `record_failure`.
                return
            self.open_events += 1
            failures = self.consecutive_failures
            threshold = self.failure_threshold
            cooldown = self.cooldown_seconds

        logger.warning(
            f"Host breaker for {self.host}: OPEN after {failures} consecutive "
            f"failed sub-requests (threshold {threshold}). Suppressing "
            f"requests to this host for {cooldown:.0f}s, then probing. "
            "Collectors sharing this host will report undelivered zones."
        )

    def snapshot(self) -> Dict[str, object]:
        """Point-in-time counters, for end-of-run logging and tests."""
        with self._lock:
            return {
                "host": self.host,
                "open": self.opened_at is not None,
                "consecutive_failures": self.consecutive_failures,
                "suppressed_requests": self.suppressed_requests,
                "open_events": self.open_events,
            }


_BREAKERS: Dict[str, HostBreaker] = {}
_REGISTRY_LOCK = threading.Lock()


def get_host_breaker(host: str) -> HostBreaker:
    """Return the process-wide breaker for `host`, creating it on first use."""
    with _REGISTRY_LOCK:
        breaker = _BREAKERS.get(host)
        if breaker is None:
            breaker = HostBreaker(host=host)
            _BREAKERS[host] = breaker
        return breaker


def all_breakers() -> Dict[str, HostBreaker]:
    """Every breaker created so far, for end-of-run reporting."""
    with _REGISTRY_LOCK:
        return dict(_BREAKERS)


def reset_all() -> None:
    """Drop all breaker state. For tests; production runs start fresh."""
    with _REGISTRY_LOCK:
        _BREAKERS.clear()
