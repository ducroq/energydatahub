# Framework drift catch-up: agent-ready-projects v1.18.0 → v1.37.0

## What & Why

The project pinned `framework: agent-ready-projects v1.18.0` (`CLAUDE.md:5`) when this item was opened; it now reads v1.37.0. The framework
was at v1.26.0 when this item was opened on 2026-08-14 — eight releases ahead. By the time
`/update-drift` actually ran, on 2026-09-06, it was at **v1.37.0**: twenty-four. The item's
own headline understated the gap by a factor of three for three weeks, which is the cost of
a savepoint whose numbers are not re-derived when it is picked up. This surfaced on 2026-08-14 while answering a
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

**Done, 2026-09-06.** Triage at RELEASE granularity: 10 adopt, 13 already in force, 1 not applicable, 0 declined.
**Two declines were recorded WITHIN adopted releases** and are the load-bearing part — see
below. Do not read the release-level zero as "nothing was refused"; that reading is what
would re-adopt the dropped `--untracked-files=all` term on the next merge.
The 13 needed nothing because all three user-global skills (`curate`, `audit-context`,
`update-drift`) are byte-identical to their v1.37.0 reference installs — verified by diffing
the installed file against `.claude/skills/<name>/SKILL.md` at every tag (monotone fall to an
exact 0 at v1.37.0), and corroborated by `install-global-skills.sh --check`. The whole real
gap was the two project-local skills, which that installer deliberately never inspects.

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

## Answers

- **Adopt or decline, per release?** Answered in the triage table (session 2026-09-06).
  `#68` is closed upstream — the project-file template now names all five skills.
- **Re-copy `review-changes` wholesale, or port selectively?** **Merge.** Five pieces ported
  (`$BASE` block, Step 1.5 whole, the two adversarial claim rules, `### Unclassified` +
  structural count in the report, Step 5), and one template change deliberately **declined**:
  v1.37.0's Step 1 drops `git status --porcelain --untracked-files=all` and carries untracked
  files only in Step 1.5's list, so *classification* there cannot see a new file. This repo
  added that term after a measured blocker (695-line commit, 645 lines untracked). It stays,
  and the divergence is now recorded in the skill's own header so a future merge does not
  silently undo it.
- **Does anything make project-local skill staleness visible?** **No — confirmed by running
  it.** `install-global-skills.sh --check ~/repos` scans for *inert* project-local copies
  (ones shadowing a global skill) and correctly found none; it says nothing about whether a
  project-local skill is behind its template. `/update-drift` Step 3's content check —
  grepping the marker strings the framework names per #94 — is the only thing that saw this.
  Before the merge: `isdelim($(0))`, `sub(/\r$/, "")`, `infm`, `nrisk`, `\001`, `BASE`,
  `Step 1.5`, `Step 5` all returned **0** in a skill whose stamp gave no hint. The stamp is
  not the instrument; the markers are.

## Superseded Open Questions

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

**Landed 2026-09-06.** Seven files changed: both `.claude/skills/*/SKILL.md`, `CLAUDE.md`,
`memory/MEMORY.md`, `memory/gotcha-log.md`, `memory/hypothesis-log.md`, and this file —
which the first draft of this sentence omitted, having counted six. The review caught it,
three paragraphs below a note saying a count embedded in prose is not maintained. Stamps moved
v1.18.0/v1.17.0 → v1.37.0 **after** the content landed, not before.

Three things worth promoting out of here before this file is deleted:

1. **#64 was live and reproduced before it was fixed.** On a throwaway repo with a pushed,
   unmerged branch carrying a real 3-line commit, all four of the old Step 1 commands
   returned empty — the skill would have reported "nothing to review" on a whole PR. This
   repo pushes branches (`fix/entsoe-upstream-nodata-38`, `feature/local-forecasting`,
   `dev`; `entsoe-zone-delivery` was reviewed on 2026-08-31), so it was one push away.
   → belongs in the gotcha log.
2. **A negative needs a seeded positive, again.** Step 1.5 reported 0 hits across all 49
   tracked markdown files. That is only meaningful because a seeded file with three known
   defects returned exactly three hits from the *same extracted awk program*. This is the
   `[x3]` guard-signal-integrity pattern for the third time; it worked here because the
   pattern was consciously applied, not because anything enforced it.
3. **The Occurrences column found a defect on adoption day** — the shape-churn row said
   "4-incident pattern" while its own cell listed five. A count embedded in prose is not
   maintained.

Residue promoted → delete this file once 1–3 are in `memory/gotcha-log.md` and
`memory/MEMORY.md`.
