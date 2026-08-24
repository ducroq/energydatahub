# Session 2026-08-24 — shape fingerprint made order-independent (merge, not sample)

## What happened

The 2026-08-23 16:00 UTC scheduled run failed the schema-drift tripwire and aborted
the publish — all 20 feeds unpublished, `deploy` skipped. Two feeds drifted:

- **`load_forecast`** — ENTSO-E published no actual load for DE_LU; the first record
  lacked `load_actual`/`forecast_error` while ~73 of 192 later records carried them.
  `load_forecast` is in `CRITICAL_FEEDS`, so it was exempt from every downgrade path.
- **`ned_production`** — NED returned `forecast` only, no `actual`, for two days
  (upstream gap; actuals present three days earlier). Not critical, so it was eligible
  for volatility downgrade — but it had not auto-classified yet.

Root cause: `compute_shape_signature` sampled ONE representative record of a timestamp
map (`next(iter(data.values()))`). NL passed and DE_LU failed purely because NL's `00:00`
record happened to be complete. This is the sampled-record defect the tripwire's own
comments had documented since June (`gotcha-log.md:106`), firing for the first time on a
critical feed.

## The fix

`utils/shape_signature.py`: the timestamp-map `value_shape` is now the **merge of every
record** via `_merge_signatures`/`_conflict_node` — a field present in ANY record survives,
a field gone from EVERY record still drifts. Order-independent; commutative + associative
(20k+ fuzz triples, zero violations across four independent reviewers).

**Measured before landing**: all 20 committed feeds hash byte-identical under the merge, so
**no `CURRENT_SCHEMA_VERSION` bump, no migration, no sidecar refresh** — the published
payload is untouched; the signature is a CI-internal artifact. Live 08-23 payload confirmed
to hash to the committed baseline (`fac9d3f0…`) under the merge, vs `fa4bf11a…` under the
old sampling (the CI failure).

Deliberately NOT fixed: `ned_production` still drifts (its `actual` block is gone from every
source — what a real removal looks like) and downgrades via derived volatility on the next
run. The genuine gap it exposed — DQ reported "all 4 checks passed" on a half-missing feed —
is a fourth silent quality-gate skip, filed as #49.

## Review-battery findings (all fixed in-session)

1. `_merge_signatures` ↔ `_conflict_node` recursed unboundedly for a same-kind node whose
   `_kind` the merge does not dispatch on → fixed with `_MERGEABLE_KINDS` + opaque bucket.
2. `_conflict_node` would `TypeError` on a bare `None` member (json round-trip) → filtered.
3. Dict merge sorted keys without `key=str` → aligned.
4. Committed `TestMergeSignatures.NODES` pool held only tidy nodes → widened.

## Verification

- 807 tests pass (781 baseline + 26 new, incl. the 2026-08-23 scenario, order-independence,
  null absorption, int/float widening, the "field gone from every record still drifts"
  guard, and the tolerance boundary).
- Hash-neutrality gate: all 20 feeds byte-identical to `data/_shape_signatures.json`.
- Tripwire: `detect_schema_drift.py --previous-ref HEAD` exit 0, no drift.
- Next-run simulation: `load_forecast` unchanged, `ned_production` volatile-warns, exit 0.

## Follow-ups filed

- **#48** — tripwire reports drift as opaque `hash1 -> hash2`; the conflict node (a new
  failure mode this change introduces) is discarded on a failing run. Diagnosis-opacity fix.
- **#49** — `ned_production` lost its `actual` half while DQ logged "all checks passed";
  needs a presence check on expected components.
- Hypothesis **H8** opened — the merge ends the sampled-record false-positive class; review
  2026-09-07.

## Not done (deliberate)

Pages deploy: not triggered by this change (code-only, not a data run). The next daily run
(16:00 UTC) publishes when it passes the gate.
