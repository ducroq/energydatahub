# Session 2026-09-21 — the drift gate stops failing on a collector's own degradation report

**Read this if**: a feed's shape hash flipped and you cannot see why; you are about to add an
exemption to any gate; or you are picking up #79.

## What happened

Triaged the 2026-09-20 publish failure (run `35528976372`), root-caused it byte-exactly, shipped
half the fix, and reviewed the fix hard enough that the review refuted it once and corrected two
of my own config edits.

## The failure, root-caused

`load_forecast.json` drifted `fac9d3f0…` → `a6b33dba…` at the drift tripwire, which runs BEFORE the
publish step, so 19 healthy feeds went unpublished. It is a `CRITICAL_FEED` and cannot downgrade.

Cause, reconstructed rather than inferred: **ENTSO-E published no A67 actual load for DE_LU
anywhere in the window** (0 points against NL's 127) from 09-20 00:00 until 09-21 06:30 CEST. So

1. `load_actual` + `forecast_error` left DE_LU's merged `value_shape` — a whole-window absence,
   which the 2026-08-24 `_merge_signatures` fix deliberately does not cover (it covers intra-day
   gaps); and
2. the collector correctly raised its `field_completeness` issue, which attaches
   `metadata['collector_quality_issues']` — **and that key is part of the shape hash.**

Two flips in one run. Verified by rebuilding the envelope from a re-fetch: field loss alone gives
`fa4bf11a…` (the 2026-08-23 hash), field loss + the diagnostic key gives `a6b33dba…` exactly.

**Method worth reusing**: the sidecar stores hashes, not reasons, but the upstream API still has
the window. Re-fetch, strip candidate fields from the live payload, re-hash, compare. ~10 minutes.

## What shipped

**#45 / H7 — the diagnostic-key exemption, generalised.** `prune_diagnostic_keys` (recursive over
dict nodes) replaces the one-node `_without_diagnostic_keys`; `classify_data_member_drift` prunes
both inputs at entry; `scripts/detect_schema_drift.py` gained `_partition_diagnostic_only`,
classified *before* the clean-pass check.

Three deliberate choices, each measured:

- **In the comparison, not in `compute_shape_signature`.** Of 20 feeds in the committed baseline
  the prune touches exactly one (`grid_imbalance`); pruning both sides leaves the comparison
  unchanged, where pruning the signature would have flipped that feed's hash and cost a publish to
  migrate.
- **No `CRITICAL_FEEDS` carve-out**, unlike every sibling downgrade. This is not a downgrade — the
  diff is not drift — so there is nothing to downgrade.
- **Diagnostic-blind volatility evidence.** Records now carry `pruned_hashes: true` + a
  `feeds_pruned` map (only the feeds the prune changes: 1 of 20, +100 bytes/record).
  `volatile_feeds_from_observations` prefers it per feed, and only when the whole window supplies
  it — a mixed window falls back to raw hashes, i.e. today's behaviour. **So the fix is not yet in
  force**; it arrives as the window rolls. H7 review set for 2026-11-20.

**This did NOT fix the 2026-09-20 failure**, and its own test asserts so: the field loss survives
the prune and still exits 1. That is #79.

## The review found more than the change

Two lenses (adversarial + guarantee-preservation), a deliberate trim from the four this repo runs
at HIGH. Both earned their cost:

- **Adversarial REFUTED it.** The exemption removed the gate's vote but not the observation log's,
  so one excused flip would make `grid_imbalance` — field-keyed, where a vanished key IS the break
  — derived-volatile for 60 runs. Measured: not volatile today, volatile after one flip, **still
  volatile five clean runs later** (54 of 60 records must age out). That also refutes the lens's
  own cheaper remedy. Honest framing it supplied: the exemption did not *create* this, since the
  observation is committed before the gate — it removed the exit 1 that made it visible.
- **"Never part of the shape contract" was false.** v2.4 documents the key on `grid_imbalance` and
  `data_quality.py` reads it. What carries the argument is the changelog's own word: *"**may**
  carry"*. Optional by declaration, so unusable as a structural invariant.
- **Ten of eleven guarantee entries in `.claude/review-profile.md` were truncated mid-sentence** by
  the 2026-09-14 migration. `settings.ini` had lost its operative half — "Treat a change here as
  GUARANTEE WEAKENED unless…" — i.e. the guarantee governing whether this repo publishes ciphertext
  or plaintext was a fragment for a week. Restored from `55ed8a1^`.
- **Two errors in my own config edits**, both corrected: the `scripts/**`-forces-full-depth claim
  (the carve-out is "shell script or executable"; `detect_schema_drift.py` is mode 644), and the
  lens count (project lenses are *additive*, so HIGH here is four lenses — the Depth column I
  deleted was right, my reason for deleting it was not).

## Review config, now durable

`.claude/review-profile.md`: Depth column removed with the correct reason, cost rules stated as
predicates, `shell-and-yaml-correctness` trigger corrected, test baseline filled in (it was
literally "not recorded"), guarantee entries restored. `CLAUDE.md`: the skill is user-global; only
`/release` is project-local.

## Where it stands

- Suite 908 → 935. Nothing dispatched: the fix needs no run to prove itself, and 07:xx UTC is
  before the day-ahead auction clears (#74).
- Tonight's scheduled run should publish normally — DE_LU actuals resumed and today's window hashes
  to baseline.
- Open decision recorded in H7: the blind-evidence fix is inert until the observation window rolls.
