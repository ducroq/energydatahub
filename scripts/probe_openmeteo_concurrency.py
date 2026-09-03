"""
Probe for hypothesis H10 / issue #58: are the Open-Meteo 429 storms caused by
our concurrency settings, or by sharing an egress IP with other GitHub-hosted
runners?

Run it from a GitHub-hosted runner and from a developer machine close together
in time, then compare the two VERDICT lines. Same code, same settings, the
egress address is what differs.

WHAT THIS CAN AND CANNOT CONCLUDE
---------------------------------
This is a LOWER-BOUND instrument. Read the outcomes accordingly:

  runner storms, local clean  -> supports H10. Lowering OPENMETEO_SEMAPHORE_CAP
                                 is then the wrong lever; the fix is an API key
                                 (per-key rather than per-IP limits) or
                                 tolerating dropouts properly.
  both storm                  -> the concurrency ceiling really is near our cap
                                 and the cap should come down.
  both clean                  -> INCONCLUSIVE, and the most likely result. It
                                 does not support H10. See the fidelity gaps
                                 below: a clean probe is consistent with a
                                 stormy real run from the same address.

The /review-changes battery refuted the first draft of this script for claiming
more than it could deliver, so state the gaps plainly. Against a real run this
probe is still gentler in two ways it cannot fix without a refactor:

  - It issues ONE wave of `--n` requests through one aiohttp session. Production
    runs SIX collectors concurrently, each with its own session and its own
    location list, sharing one semaphore — six connection pools, six TLS
    handshakes. The 2026-06-07 regression recorded in `_openmeteo_shared` is
    specifically about Open-Meteo's per-source connection behaviour, which a
    single pool smooths away.
  - Request weight is one variable set at one forecast horizon. Production
    sends three variable sets across 7-, 10- and 16-day horizons.

Replaying the real six-wave shape needs the location lists lifted out of
`data_fetcher.main()` into an importable module — filed rather than done here.

What it DOES reproduce, deliberately, because these drive the storm:
  - the real aggregate location count (`PRODUCTION_LOCATION_COUNT`),
  - the retry amplification (`MAX_RETRIES`), which under any windowed limiter
    is strictly additive load and is what turns a few 429s into a storm,
  - the UNGAPPED HEAD BURST: every collector passes `delay=(i > 0)`, so the
    first request of each of the six waves skips the gap — up to `cap`
    simultaneous zero-spaced requests at the head of every real run. A bare
    6-wide gapless burst is the one configuration already measured to 429
    locally, and the first draft of this probe excluded it.

This is a DIAGNOSTIC, never a gate. It exits 0 whatever it finds, so it can
never fail a run on upstream weather.

Usage:
    python scripts/probe_openmeteo_concurrency.py
    python scripts/probe_openmeteo_concurrency.py --cap 3 --n 38
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import aiohttp  # noqa: E402

from collectors._openmeteo_shared import (  # noqa: E402
    MAX_RETRIES,
    OPENMETEO_GAP_SECONDS,
    OPENMETEO_SEMAPHORE_CAP,
)
from collectors.openmeteo_weather import OpenMeteoWeatherCollector  # noqa: E402

# Aggregate locations across the six OpenMeteo collectors in data_fetcher.main():
# strategic 6 + offshore 10 + solar 7 + population 11 + buurt weather 2 +
# buurt solar 2. DUPLICATED from data_fetcher because those lists are locals
# inside main() and cannot be imported; if you change them there, change this.
# Lifting them into a module is the proper fix (see the docstring).
PRODUCTION_LOCATION_COUNT = 38

# Spread over the CWE area. Distinct coordinates matter: identical ones could
# be served from an upstream cache and would understate the load.
BASE_LAT, BASE_LON = 51.0, 3.5

OPENMETEO_HOST = "api.open-meteo.com"


async def _one(session, sem, index, gap, url, variables, forecast_days, retries,
               head_burst):
    """One location's fetch, including the retry amplification production has."""
    params = {
        "latitude": round(BASE_LAT + (index % 37) * 0.013, 4),
        "longitude": round(BASE_LON + (index % 37) * 0.011, 4),
        "hourly": variables,
        "forecast_days": forecast_days,
        "timezone": "Europe/Amsterdam",
    }
    statuses = []
    for attempt in range(1, retries + 1):
        async with sem:
            # Skip the gap for the first `cap` requests: production passes
            # apply_gap=False for request 0 of each of its six waves, so the
            # head of a real run is an ungapped burst up to the cap width.
            if gap and index >= head_burst:
                await asyncio.sleep(gap)
            try:
                async with session.get(url, params=params) as response:
                    statuses.append(response.status)
                    body = "" if response.status == 200 else (await response.text())[:120]
            except Exception as exc:  # noqa: BLE001 — a probe reports, never raises
                statuses.append(0)
                body = f"{type(exc).__name__}: {exc}"[:120]
        if statuses[-1] == 200:
            return statuses, ""
        if attempt < retries:
            await asyncio.sleep(1.0 * (2 ** (attempt - 1)))
    return statuses, body



async def _trial(cap: int, gap: float, n: int, retries: int,
                 variables: str, forecast_days: int, url: str) -> dict:
    # Requests below this index skip the gap, reproducing the ungapped head
    # burst production creates by passing apply_gap=False for request 0 of
    # each of its six concurrent waves.
    head_burst = cap
    sem = asyncio.Semaphore(cap)

    started = time.monotonic()
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[
            _one(session, sem, i, gap, url, variables, forecast_days, retries,
                 head_burst)
            for i in range(n)
        ])
    elapsed = time.monotonic() - started

    codes: dict[str, int] = {}
    first_error = ""
    locations_lost = 0
    for statuses, body in results:
        for st in statuses:
            codes[str(st)] = codes.get(str(st), 0) + 1
        if statuses[-1] != 200:
            locations_lost += 1
            if not first_error:
                first_error = body
    requests_made = sum(codes.values())
    rate_limited = codes.get("429", 0)
    transport_failures = codes.get("0", 0)
    other_non_200 = requests_made - codes.get("200", 0) - rate_limited - transport_failures
    return {
        "cap": cap,
        "gap": gap,
        "locations": n,
        "retries": retries,
        "forecast_days": forecast_days,
        "requests_made": requests_made,
        "elapsed_seconds": round(elapsed, 2),
        "status_counts": codes,
        "rate_limited_429": rate_limited,
        "transport_failures": transport_failures,
        "other_non_200": other_non_200,
        "locations_lost": locations_lost,
        "rate_limited_pct": round(rate_limited * 100 / requests_made, 1) if requests_made else 0.0,
        "first_error": first_error,
    }


def _egress_ip() -> str:
    """Best-effort source address for a connection to Open-Meteo ITSELF.

    Deliberately not an ip-echo service on another host: Azure SNAT maps
    per-destination flows to different frontend addresses, so an address
    learned from a different destination network need not be the one
    Open-Meteo saw — which would make the correlation this probe invites
    unsound. This reads the local socket address instead, which on a runner is
    the private NAT address; the public frontend is not observable from
    inside. So treat it as a flow identifier, not as the address Open-Meteo
    logged. Masked off-CI, since local runs print a home address into output
    the docstring tells you to paste into a public issue.
    """
    try:
        with socket.create_connection((OPENMETEO_HOST, 443), timeout=10) as sock:
            local = sock.getsockname()[0]
    except OSError as exc:
        return f"unavailable ({type(exc).__name__})"
    if os.environ.get("GITHUB_ACTIONS") != "true":
        head = local.rsplit(".", 1)[0] if "." in local else local
        return f"{head}.x (masked: local run)"
    return local


def _emit_summary(lines: list[str]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
    except OSError as exc:
        print(f"(could not write job summary: {exc})")


async def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cap", type=int, default=OPENMETEO_SEMAPHORE_CAP)
    parser.add_argument("--gap", type=float, default=OPENMETEO_GAP_SECONDS)
    parser.add_argument("--n", type=int, default=PRODUCTION_LOCATION_COUNT,
                        help="Locations to fetch (default: the production total).")
    parser.add_argument("--retries", type=int, default=MAX_RETRIES)
    parser.add_argument("--forecast-days", type=int, default=10)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    for name, value in (("--cap", args.cap), ("--n", args.n), ("--retries", args.retries)):
        if value < 1:
            parser.error(f"{name} must be >= 1 (got {value})")
    if args.n > 200:
        parser.error(
            f"--n {args.n} exceeds the 200 cap. This probe issues real requests "
            "to an upstream that is already refusing us; it must not become the "
            "storm it measures."
        )

    collector = OpenMeteoWeatherCollector
    trial = await _trial(
        args.cap, args.gap, args.n, args.retries,
        ",".join(collector.WEATHER_VARIABLES), args.forecast_days,
        collector.BASE_URL,
    )
    ip = _egress_ip()

    at_production = (
        args.cap == OPENMETEO_SEMAPHORE_CAP
        and args.gap == OPENMETEO_GAP_SECONDS
        and args.retries == MAX_RETRIES
    )
    setting_label = (
        "production settings"
        if at_production
        else (f"cap={args.cap} gap={args.gap}s retries={args.retries} "
              f"(production is {OPENMETEO_SEMAPHORE_CAP}/{OPENMETEO_GAP_SECONDS}/{MAX_RETRIES})")
    )

    report = {"source_address": ip, "at_production_settings": at_production, "trial": trial}
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"source address : {ip}")
        print(f"settings       : {setting_label}")
        print(f"  {trial['locations']} locations x up to {trial['retries']} attempts "
              f"-> {trial['requests_made']} requests in {trial['elapsed_seconds']}s")
        print(f"  status counts : {trial['status_counts']}")
        print(f"  429           : {trial['rate_limited_429']} ({trial['rate_limited_pct']}%)")
        print(f"  transport fail: {trial['transport_failures']}")
        print(f"  other non-200 : {trial['other_non_200']}")
        print(f"  locations lost: {trial['locations_lost']}/{trial['locations']}")
        if trial["first_error"]:
            print(f"  first error   : {trial['first_error']}")
        print(f"VERDICT: {trial['rate_limited_429']}/{trial['requests_made']} requests "
              f"rate-limited ({trial['rate_limited_pct']}%) at {setting_label}")

    _emit_summary([
        "### Open-Meteo concurrency probe (H10 / #58)",
        "",
        f"- source address: `{ip}`",
        f"- settings: {setting_label}",
        "",
        "| locations | attempts each | requests | 429 | transport fail | other non-200 | lost | seconds |",
        "|----------:|--------------:|---------:|----:|---------------:|--------------:|-----:|--------:|",
        f"| {trial['locations']} | {trial['retries']} | {trial['requests_made']} | "
        f"{trial['rate_limited_429']} | {trial['transport_failures']} | "
        f"{trial['other_non_200']} | {trial['locations_lost']} | {trial['elapsed_seconds']} |",
        "",
        f"**VERDICT: {trial['rate_limited_429']}/{trial['requests_made']} rate-limited "
        f"({trial['rate_limited_pct']}%).** Transport failures and non-429 errors are "
        "broken out separately on purpose — a socket reset is not evidence of "
        "rate limiting, and the 2026-09-03 run mixed 87×429 with 12×500.",
        "",
        "Compare against the same command run locally. **Both clean is "
        "inconclusive**, not support for H10 — see `memory/hypothesis-log.md`.",
    ])

    marker = "warning" if trial["rate_limited_429"] else "notice"
    print(f"::{marker}::Open-Meteo probe: {trial['rate_limited_429']}/"
          f"{trial['requests_made']} rate-limited ({trial['rate_limited_pct']}%) "
          f"at cap={args.cap}, gap={args.gap}s, retries={args.retries}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
