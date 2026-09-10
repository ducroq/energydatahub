# Session 2026-09-10 — the fix that was dead on arrival

## What happened

Two publish failures triaged (09-07, 09-09), one fixed, and the fix caught being inert by
its own review battery before it shipped.

**09-07** (`34155885124`) — ENTSO-E host outage. The shared per-host breaker (#52) opened,
suppressed 103 sub-requests, `entsoe`+`entsoe_de` still missing after 3 retry rounds, exit 1
at *Collect data*. Genuinely upstream; self-healed by 09-08. Alert #69 opened and auto-closed
on recovery — the alerting path worked end to end for the first time in production.

**09-09** (`34392331572`) — **not** upstream in any useful sense. Prices collected fine. All six
NED.nl fetches timed out inside 50 seconds; `collectors/ned.py` logs and swallows each one and
still returns an envelope; `validate_completeness` scored 0 points → CRITICAL → *Assert data
quality* exit 1 → the publish of 19 healthy feeds lost. Alert #70. The asymmetry that makes it
absurd: `ned_production` was absent from `DATASET_MISSING_SEVERITY`, so a **missing** feed would
have scored the implicit `'info'` and published. Empty was punished harder than missing.

## The thing worth remembering

The fix — register `ned_production` in `PRESENT_EMPTY_GRACE_FEEDS` — **did nothing**, and the
full suite was green over it.

The guard tested `not ds.data`. `collectors/ned.py:255` assigns `parsed[energy_type] = {}` for
every configured type *before* it knows whether anything parsed, so the outage envelope is
`{'solar': {}, 'wind_onshore': {}, 'wind_offshore': {}}` — truthy, zero points. Registry entry,
coercion branch and 14 tests all inert; run 34392331572 would have replayed unchanged.

Truthiness was right for all six Open-Meteo feeds, which gate their per-location assignment on
`if location_data:` and really do collapse to `{}`. It was wrong for the seventh, and the
disagreement was invisible because **no test built an envelope** — they grepped
`inspect.getsource(data_fetcher.main)` for the right strings. Measured false-pass: deleting the
candidate entry left the grep test green, because `'ned_production':` also occurs in the
unrelated `quality_datasets` map further down the same function.

Three of four lenses found it independently. The tests, the compile check and the hook did not.

**Shipped instead**: `present_empty_feeds()` in `utils/data_quality.py`, whose predicate is
`_count_data_points(ds.data) == 0` — literally the expression `validate_completeness` uses to
emit CRITICAL. Guard and gate now read one expression and cannot disagree about "empty".
The `if x in _coerce:` ladder became a dict mutation plus an unconditional seven-line read-back.
Tests build the real envelopes; the wiring test parses the `_grace_candidates` dict literal out
of the AST instead of grepping. Both were ablated (revert the predicate → 2 red; delete the
registry entry → 1 red) before being believed.

Suite 905 → 908.

## Review battery

4 lenses on the first draft (guarantee / adversarial / doc-accuracy / end-to-end-trace; shell+YAML
skipped, nothing shell-shaped changed), 1 narrow adversarial round on the fix. 2 BLOCKERS, both in
the first draft, one of them the whole change being inert. Round 2 NOT REFUTED, with 5 residuals of
which 4 were applied.

The guarantee lens **missed** the blocker the other three found — it traced the time-box end to end
and reported the escalation reachable, all correct *conditional on a predicate it did not test*. Worth
knowing: a lens that verifies downstream mechanics assumes the trigger fires.

## Hypotheses

- **H2** — trigger fired again, on a **third** failing-step class (*Assert data quality*). Its 09-04
  conclusion — "the gate's only lever is withholding all 20 feeds" — was written about the drift
  tripwire and is now shown to be a property of the quality gate too. Generalised, not re-litigated.
- **H8** — overdue (09-07), reviewed. Its counter-position named `ned_production`'s DQ blind spot as
  its settling condition; today's grace made that blind spot load-bearing. #49 is now the event that
  settles H8.
- **H10** — review-by reached. Reviewed, still open. The 09-09 429 profile inverted the tracked one:
  Open-Meteo 1 (recovered), Luchtmeetnet 6 (both collectors lost). Neither confirms nor refutes;
  urgency revised **down** — 0 of the last 4 publish failures ran through that path.

## Issues

Commented #42, #49, #50, #58. Filed #71 (Luchtmeetnet/Open-Meteo lack `host_breaker_key` — the #52
gap two hosts over), #72 (a graced feed loses its shape baseline and returns as an "added feed",
never diffed — plus `weather_forecast_multi_location` being graced *and* `CRITICAL_FEEDS`, so its
grace never reaches the publish), #73 (`data_quality_report.json` never reaches `docs/`).

## Cross-repo

The Augur session asked what was happening upstream. Told it: 09-07 upstream, 09-09 ours; no backfill
(the vintages are gone — `backfill_entsoe.py` is broken against the v2.2+ envelope, #57); and the thing
it actually lacked — the `publish-failure` label is a machine-readable "stop waiting" signal its gate
could poll instead of timing out at 03:00 UTC. Its ARF backup is wall-clock anchored, so an EDH gap
takes out its primary and its fallback through one door; flagged as theirs.

## Outcome

Committed `6eee519`, rebased onto 8 CI commits (clean — they touch only `data/` and `docs/`),
pushed with the previously-unpushed framework catch-up `0699fdc`.

**Publish restored** by dispatched run `34450530525` (07:33 UTC, 7m57s): collect + deploy green,
`overall_status=warning` with the single known `grid_imbalance` issue, 19 feeds in the sidecar,
and the alert job auto-closed #70 on recovery. Augur's ~48h gap is closed; it was told the
vintage would jump rather than backfill.

Two things this run did **not** settle:

- `ned_production` published healthily — NED recovered by itself, so the coercion path *still*
  has never run in production and #42's closure gate is still unmet. The fix is deployed and
  unexercised, which is exactly the state this work item has been in since 2026-08-08.
- `air_quality_buurt` was lost **again** to a Luchtmeetnet 429 collision: both instances
  rejected within 14 seconds, at 07:35 UTC here against 19:04 UTC the day before. Two
  consecutive runs, two very different times of day — so not a diurnal egress pattern. #71
  updated; it is now a repeatedly-lost feed rather than only wasted wall time.
