# Framework drift catch-up: agent-ready-projects v1.18.0 → v1.26.0

## What & Why

The project pins `framework: agent-ready-projects v1.18.0` (`CLAUDE.md:5`). The framework
is at **v1.26.0** — eight releases ahead. This surfaced on 2026-08-14 while answering a
different question, not from `/update-drift`, which `CLAUDE.md`'s own first row says to run
at session start and which was **not run that session**.

The concrete cost is already measurable. `/review-changes` is project-local by Hard
Constraint, so `scripts/install-global-skills.sh` deliberately does not carry it and will
not report it stale. The local copy is stamped v1.17.0 at 16,505 bytes against the
framework's 26,210 — roughly 40% smaller. Known-missing, from the framework CHANGELOG:

- **Step 1.5**, the deterministic structural pre-check (markdown table/fence corruption).
  Runs at every tier and magnitude; absent here entirely.
- The **CRLF fix** (framework #52) — on a CRLF checkout Step 1.5 examines no tables and
  prints what a clean run prints.
- The **`@{u}` change-set fix** (framework #64) — a pushed PR branch reports "nothing to
  review". Did not bite on 2026-08-14 (work was on `main` with nothing unpushed).
- The **"Unclassified" report slot** and the two v1.25.0 negative-claim rules
  ("state the check before the claim"; "an absolute in a description is a measurement").

That copy is what ran the 2026-08-14 battery — which still found two blockers, so it is
not useless, but it ran without its structural pre-check and without the rules that govern
how its own negative findings are reported.

## Current Status

Nothing adopted. Drift identified and quantified only. Filed upstream as
`ducroq/agent-ready-projects#68` (see Decisions).

## Decisions

- **2026-08-14 — filed a framework issue rather than only patching locally.**
  `templates/project-file.md`, the template adopters copy to become `CLAUDE.md`, has **no
  "Before committing" row and names none of the five skills**. Verified:
  `grep -o "curate\|audit-context\|update-drift\|review-changes\|release"` over that
  template returns exactly one hit, and it is the word *released* inside the
  framework-drift row. `docs/GUIDE.md` prescribes a seven-moment Documentation Rhythm; the
  template's table implements three of them and names no skill for any.
  This repo has the row only because someone hand-wrote it — and that hand-written row is
  the sole reason the pre-commit battery ran on 2026-08-14 and caught the two blockers.
  Filed as `ducroq/agent-ready-projects#68`. Partly answers that repo's open #47, which
  measures `review-changes` installed in 15 repos and explicitly leaves open whether any of
  them ever invoke it.

## Open Questions

- **Adopt or decline, per release, v1.19.0 → v1.26.0.** `/update-drift` triages; adopting
  is the engineer's call and it stops before editing normative surfaces.
- **Re-copy `review-changes` wholesale, or port selectively?** Wholesale is what the
  framework CHANGELOG instructs ("re-copy by hand"). But the local copy carries
  *project-specific* risk tiers and a guarantee lens naming files in this tree — including
  entries added since v1.17.0 that the template does not have (`data/_shape_signatures.json`
  and `data/_upstream_empty_streak.json` as HIGH committed control state; `settings.ini`'s
  `encryption = 1`). A naive overwrite loses those. This is a merge, not a copy, and that is
  the real work in this item.
- **Does anything make project-local skill staleness visible?** This drifted eight releases
  silently and by design — `install-global-skills.sh --check` excludes project-local skills
  precisely so it will not touch them. A version stamp comparison in `/update-drift`, or a
  lint rule, would close it. May belong upstream rather than here.

## Outcome

<!-- Fill in when the work lands or is abandoned. -->
