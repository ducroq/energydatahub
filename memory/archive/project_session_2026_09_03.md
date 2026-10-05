# Session 2026-09-02 → 2026-09-03

Started from a runner-failure email. Ended with two shipped changes, five new
issues, and a measured reclassification of what is actually breaking the daily
publish.

## What the failure actually was

ENTSO-E's Transparency Platform was in **scheduled maintenance**, not flaking.
Settled by varying the credential and comparing bytes: `web-api.tp.entsoe.eu`
returned a byte-identical 18,847-byte branded maintenance page to a valid
token, an absent token and a garbage token, with no `Retry-After` and no
`X-RateLimit-*`. Auth was never reached, so it was not a ban or a quota. Two
independent networks (Actions runners, local) saw the same page. `web-api`,
`tp` and `transparency` all resolve to one Azure IP (20.23.37.29) and the other
two answered 200/404 — the edge was healthy and only the `web-api` vhost was
switched off. It recovered during 2026-09-02; the 06:06Z probe on 09-03 got 200
and 191 NL price points.

`transparency.entsoe.eu/api` returning 200 was a red herring — the SPA
catch-all serving `index.html`, not an API.

## Shipped

**`94bc0d7` — shared per-host circuit breaker (#52, closed).**
`collectors/_host_breaker.py`: state keyed by host, shared across the eight
ENTSO-E collector instances, consulted per sub-request. The `/review-changes`
battery caught three defects that would otherwise have shipped:

- `NoMatchingDataError` counted as a host failure in five collectors. It means
  "answered fine, no rows", and the NL cable borders are routinely unpublished,
  so ~6 empty borders would have opened the breaker on a **healthy** host and
  suppressed every remaining ENTSO-E request. `_retry_single` now takes
  `non_host_exceptions`.
- `EntsoeCollector` passed `host_breaker_key` but never called `_retry_single`,
  so the feature was inert on the **critical** price feed — and the wiring test
  asserted the constructor attribute, so it was green on the no-op.
- The docstring claimed every retry round gets a fresh probe. The rounds belong
  to the two price collectors; the six the breaker governs run once in a single
  gather.

**`5afa4c7` — Open-Meteo concurrency probe (#58, H10).** First draft was
refuted by the same battery for claiming more than it could deliver: 24
locations instead of 38, no retry amplification, and it excluded the ungapped
head burst that is the one configuration already measured to 429. Rewritten to
replay the production shape and to state plainly that it is a lower-bound
instrument — **both-clean is inconclusive**.

## The finding that outranks both

Five consecutive days without a publish, three unrelated causes:

| Date | Failed at | Cause |
|---|---|---|
| 08-29 | drift tripwire | ENTSO-E zone dropout |
| 08-31 | Collect | ENTSO-E 503 |
| 09-01 | Collect | ENTSO-E 503 |
| 09-02 | drift tripwire | `load_forecast` churn — transient, byte-identical to baseline next day |
| 09-03 | drift tripwire | Open-Meteo location dropouts |

Re-running **H2's own Method** over the last 26 scheduled runs: 20 success, 6
failure (23%). Classified by failing step — the axis H2's Method names — the
schema-drift-tripwire class is **4 of 26 = 15.4%** (08-14, 08-23, 08-29,
09-02), above its ~1-in-10 threshold; Collect-step ENTSO-E 503s account for the
other two. At n=26 that is 4 events against ~2.6 expected, so it is suggestive
rather than decisive on this window alone — what makes it hard to dismiss is
the same step failing four times. H2 reopened. The four do not share a root
cause: 08-14 was a real defect, the other three were transient upstream
degradation the tripwire could not downgrade — which argues for a downgrade
path rather than a collector fix. This is #50's thesis with a
measured threshold behind it: the binding constraint on delivery to Augur is
that one degraded feed aborts the publish of ~20 healthy ones — not any
individual upstream.

## Corrections made during the session

- "Three of the last five failures ran through the Open-Meteo path" — it is
  **one**. 09-01 had the largest storm (96 × 429) but died at Collect on the
  ENTSO-E cascade, so those 429s never reached a gate.
- "`weather_forecast_multi_location` should have downgraded but didn't" —
  it is a `CRITICAL_FEED` and can never downgrade; `detect_schema_drift.py`
  says so outright. The unexplained feed is `demand_weather_forecast`, and its
  likely cause is **H7**, not contention.

## Filed

#54 flows publishes configured borders (already false in production — `NL→GB`
absent from every record). #55 breaker state never reaches disk. #56 suppressed
vs never-requested. #57 `backfill_entsoe.py` reads the pre-v2.2 envelope —
reports all 108 dates degraded and would write malformed files; **do not run it
without `--dry-run`**. #58 Open-Meteo 429 storms / H10.

## Still open at session end

The publish is still blocked; last successful one was 2026-08-30 (`8e5cc52`).
The 08-31 and 09-01 forecast vintages are unrecoverable — those runs aborted
before writing anything, and `backfill_entsoe.py` only patches files that
exist (and is itself broken, #57).
