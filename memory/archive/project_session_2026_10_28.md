# Session 2026-10-28 — framework v1.50.0, #85 mechanized, hypotheses thinned

**Ask**: `/update-drift`, then "any issues left?" → read-surface pass 3.

| Thread | State |
|---|---|
| Framework v1.49.2 → v1.50.0 (`f8cb835`) | closed: size gate already in force via global skill; `Reviewer model` declined (reason in `project_active_decisions.md`); checklist re-tiered MEDIUM |
| #85 registry test (`f3ee4be`, `ea471aa`) | closed: 6 lists asserted, 9 ablations red; old hand-copied test removed; checklist shrunk 2,367 → ~1.4k chars |
| Hypothesis log thinning (`85348d8`) | closed: 24.0k → 3.8k chars, history verbatim in `hypothesis-history-2026-10-28.md`; H10 reviewed, next 2026-11-28 |
| #91 `DATASET_MISSING_SEVERITY` gap (9 feeds) | open: filed, per-feed severity is the engineer's call |
| Gotcha-log retire step | open: next session |

Review notes: four lenses on #85 found the current-copy gap and two nonexistent checklist steps; one Sonnet adversarial pass on the hypothesis thinning made one valid finding and one false one (claimed an "invented" rule that was verbatim in the old text — checked with grep before acting).
