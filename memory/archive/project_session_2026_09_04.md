# Session 2026-09-04 — publish restored; alerting shipped; a time box designed and abandoned

## Outcome

Publishing works again. Three green runs today (`33846069377` 06:50 dispatch,
`33852197632` 10:14 dispatch, plus the scheduled run to come), after five
consecutive failures 08-31..09-03. Last prior success was 08-30 `8e5cc52`.

The 08-31 and 09-01 failures died in **Collect data** (ENTSO-E maintenance, now
over). The 09-02 and 09-03 failures died at the **schema-drift tripwire**, each
on a different feed. Nothing was changed to make the gate pass — upstream
recovered and `derive_volatile_feeds` reclassified `nordic_hydro` from the
observation log, which is #43 working as designed.

## Shipped

- **Publish-failure alerting** (`a5993e7`, PR #64, #50). An `alert` job needing
  both `collect-and-publish` and `deploy`; one labelled tracking issue,
  commented per failure, closed on recovery. Cannot affect publishing —
  separate job, runs last, every step `continue-on-error`.
- **`scripts/smoketest_alerting.sh`** — walks all six links against the real
  repo and cleans up.
- **`.github/workflows/alerting-selfcheck.yml`** (`44f068a`) — manual-only, runs
  that script with `secrets.PAT`, because the alert job's *create* path only
  runs during an outage and needs a different permission from its close path.
  Validated on a real runner: all six links pass, issue deleted.
- **`scripts/**` added to `test.yml`** push filters. A change to
  `detect_schema_drift.py` previously ran zero tests on push.

## Abandoned: a time box on the drift gate

Built a mechanism to relent after N consecutive drift-blocked runs. The
`/review-changes` battery rejected the **design**, not the implementation, and
it was deleted rather than patched. Three findings, each verified:

1. Its only *reachable* domain is `CRITICAL_FEEDS` ∪ `MEMBER_MAPPED_FEEDS` —
   every other feed self-classifies volatile by its second drift run, and
   derived volatility subtracts exactly those two sets. So it would relent
   almost exclusively on the feeds that must never be waved through.
2. It would have relented on **zero of the five** runs that motivated it.
3. It published a drifted feed under an unchanged `schema_version`, with the
   only signal an annotation in a log Augur never reads.

**Do not rebuild this.** If the all-or-nothing gate is revisited, per-feed
exclusion from the publish set is the shape to consider — but note it would
have rescued only 1 of the 3 drift-blocked runs, because the other two blocked
on `CRITICAL_FEEDS` members genuinely degrading. Diff parked at
`/tmp/.../timebox-abandoned.patch` (ephemeral); the reasoning is what matters.

The time box was cited as **#47** and then **#50**; neither is its number. It
has none. #47 is the member-drift duration guard and #50 is the
skipped-publishes issue that the alerting addresses — both live, both correct
references for other things.

## Parked, unmerged: `critical-feeds-wind-forecast`

Adds `wind_forecast.json` to `CRITICAL_FEEDS`. Rationale: of Augur's three
exogenous features (`load_forecast`, `solar_ghi`, `wind_speed_80m`) only
`load_forecast` was covered, so `wind_forecast` was eligible for derived
volatility, had auto-classified, and could no longer block — while Augur's own
gate inspects only the price and load feeds. Neither side checked a live model
input.

Held back deliberately: it makes the gate **stricter** (~1 extra blocked run a
month, measured), which is a hardening trade to make with someone watching, and
it is the one change from this session with no adversarial review behind it.
`solar_forecast.json` was deliberately *not* included — it is in
`MEMBER_MAPPED_FEEDS`, already excluded from derived volatility, so promoting it
would only strip its member-drift downgrade and re-arm the 2026-08-14 incident.

## #51 diagnosed — upstream, zone-asymmetric, partly recovered

`load_forecast` shipped 96 points / 1 day instead of 192 / 2 days for four
publishes from 08-28. Decrypted the whole archive: 192/2 is the normal shape
going back to 2025-12. Every *earlier* short vintage was an early-hour run that
recovered on the same day's normal-hour run; `260830_190255` at 19:02 is the one
unambiguous normal-hour short publish.

Direct A65 probe at 09:59 CEST: **NL 192/2 days (recovered), DE_LU 96/1 day**.
Widening the request to +3d changed nothing, so not window clamping. **Not our
parser** — `_parse_response` builds each zone's timestamps from that zone's own
series, no cross-zone alignment. `metadata.end_time` kept declaring +2 days
throughout, so the envelope over-declared its own coverage.

Augur confirmed from `ml/data/consolidate.py:320` that their features are
**NL-only** (`DE_LU` appears nowhere in their repo), so the DE_LU half does not
block them. Prediction registered with them for tonight's ~19:00 UTC run:
**NL 192/2, DE_LU 96/1, price entsoe 192/2**. Their Alternative 1 ("new steady
state") is falsified for NL if that holds — recorded by them as *falsified
before its signal could accumulate*, not as a non-event.

## Prediction outcome — NL leg right, DE_LU leg WRONG

Stated in advance for tonight's run: NL 192/2, **DE_LU 96/1**, price 192/2.
The DE_LU leg is falsified, and by our own publish 15 minutes after the probe:

  260904_065333 (06:53 UTC)  load NL  96/1   DE_LU  96/1   price 96/1
  260904_081356 (08:14 UTC)  load NL 192/2   DE_LU 192/2   price 96/1

The 09:59 CEST (07:59 UTC) A65 probe caught DE_LU at 96 because German
day-ahead load had not published yet — it appeared by 08:14 UTC. **The timing
confound flagged as "cannot rule out" was real, and the prediction was made
anyway.** The lesson is not about DE_LU: a probe at one instant cannot
distinguish "absent" from "not published yet" for a source whose publication
schedule is unknown, and the honest move was to withhold that leg rather than
predict it. Caught by the downstream consumer measuring the published vintage.

Reconciled #51 picture (both sides agree):

  through 08-26   NL 192 / DE_LU 192            healthy
  08-28..08-30    NL  96 / DE_LU  96            genuine both-zone A65 gap
  09-04           both 192 by 08:14 UTC         recovered; the morning 96 is timing

The 08-30 argument still stands and is what keeps this from being *entirely* a
timing artifact: `260830_190255` was a **19:02** publish with DE_LU at 96, far
later than today's ~08:14 UTC recovery. So 08-28..08-30 was a real gap.

**#51 disposition**: Alternative 1 ("new steady state") is falsified — before
its signal could accumulate, since the pre-committed threshold was three
consecutive normal-hour publishes at price 192 / load 96 and it reached one.
Keep #51 open until tonight's normal-hour publish confirms, then close as
**resolved-upstream**, not as transient: it was a genuine multi-day A65 gap that
recovered, and three vintages of forecast reach were really lost.

Note both of today's publishes carry price at 96/1 — they are pre-auction
(day-ahead clears ~12:00 CET), so neither is a usable vintage downstream. That
is expected and not a defect; tonight's 19:00 UTC run is the first usable one.

## Findings worth more than the code

Three gotcha-log entries added (`2b164fc`):

- **A five-lens review passed two defects a six-line smoke test caught in
  seconds.** For anything talking to an external API, run it. Reviewers test
  against already-settled state and cannot see a consistency model.
- **A shape signature cannot see span.** 96 and 192 records hash identically.
  A gate covers exactly one of {presence, structure, span, values} — name which,
  and never let a registry of "important" inputs imply the rest.
  `CRITICAL_FEEDS` is presence-and-structure with an importance-sounding name.
- **A derived classification with no evidence threshold learns from noise.**
  `wind_forecast` lost enforcement off two excursions in 60 runs against a
  dominant shape in the other 58.

## Known limitation of the alerting — it cannot see a run that never starts

The `alert` job is triggered BY a workflow run, so it covers "the run failed",
not "the run never fired". #50's title is *"Scheduled publish silently skips
whole days"*, and a skipped **schedule** is inside that title and outside this
fix. GitHub drops cron schedules on repos with no recent activity (not a risk
while the daily commit lands, but it is the classic way this happens), and a
queue outage or a disabled workflow produces the same silence.

So the alerting is a *failure* detector, not a *liveness* detector. Augur's own
t0 hold-back — which notices the absence of a fresh vintage rather than the
presence of a failed run — is the complementary check and must NOT be
downgraded on the strength of this. Told them so explicitly on 2026-09-04 after
they recorded "check the EDH issue before inferring an outage", which would be
wrong for the never-ran case.

A liveness check needs an external heartbeat: something that fires when a
publish has NOT happened for N hours. Cannot live in this workflow, by
construction.

## Wrap-up state (end of session)

Shipped and on `main`:

| | |
|---|---|
| `a5993e7` | publish-failure alerting (#50, PR #64) + `scripts/smoketest_alerting.sh` |
| `44f068a` | `.github/workflows/alerting-selfcheck.yml` — validates the PAT's issues-WRITE |
| `aa683d2` | `scripts/**` added to `test.yml` push filters |
| `5665d46` | span check — the fourth gate (#53), non-blocking |
| `0742555` | span report carries the DENOMINATOR; "not verified" is no longer reported as "clean" |

Closed: **#53** (span guard shipped). Commented: **#51** (diagnosis + my failed
DE_LU prediction), **#50** (detection half shipped, liveness half open),
**#46** (measurement; expanding CRITICAL_FEEDS is the wrong resolution).
Opened: **#67** (off-host liveness watcher, deliberately not started).

Decided and recorded, so they are not re-litigated:

- `wind_forecast` → `CRITICAL_FEEDS`: **closed unmerged**, branch deleted.
  Blocking withholds 19 healthy feeds and does not fix the degraded one.
- Derived volatility: **left alone**. A 90% share floor would classify
  correctly and cost ~1 blocked publish every 6 days; the imprecision is
  load-bearing for availability.
- No second blocking gate. New detection lands as warn + alert.

Hypotheses: **H2** updated with the remedy actually chosen (detection without
blocking — the downgrade path it pointed at was built and abandoned).
**H11** opened for the span check's thresholds, review 2026-09-25, with an
explicit "do not resolve this by loosening the threshold".

## Next

1. **A span/horizon check** — the missing instrument. Needs a per-source
   expected span (NL and DE_LU publish day-ahead on different schedules) *and* a
   minimum-observations floor before trusting a derived expectation.
2. Decide on `critical-feeds-wind-forecast`.
3. H10/#58 (Open-Meteo 429 storms) is the likeliest remaining cause of a blocked
   publish; it caused the 09-03 06:06 failure via a 10-of-11-location dropout.
4. Four of five review lenses never ran on the merged diff (session usage
   limit). The alerting path is smoke-tested end to end instead.
