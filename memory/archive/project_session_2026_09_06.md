# Session 2026-09-06 — framework drift catch-up v1.18.0 → v1.37.0, reviewed against itself

## What this was

`/update-drift`, run at session start as `CLAUDE.md`'s first row instructs. It found the
project 24 releases behind on the project-file stamp and 25 behind on both project-local
skills. Nothing here touched pipeline code.

## Triage

10 adopt, 13 already in force, 1 not applicable, 0 declined at release granularity —
but **two declines inside adopted releases**, which is the part that matters.

The 13 needed nothing because all three user-global skills (`curate`, `audit-context`,
`update-drift`) are **byte-identical to their v1.37.0 reference installs**. Measured by
diffing the installed file against `.claude/skills/<name>/SKILL.md` at all 25 tags: a
monotone fall to exactly 0 at v1.37.0 on all three (`curate` 579→0, `audit-context`
163→0, `update-drift` 101→0). Corroborated independently by
`install-global-skills.sh --check`.

So the whole real gap was the two project-local skills — the copies that installer
deliberately never inspects.

## What shipped

Seven files: both `.claude/skills/*/SKILL.md`, `CLAUDE.md`, `memory/MEMORY.md`,
`memory/gotcha-log.md`, `memory/hypothesis-log.md`, and the work item.

- `review-changes` **merged, not re-copied** (265 → ~515 lines): the `$BASE` baseline
  block, Step 1.5 whole, both adversarial claim rules, `### Unclassified` + a structural
  count in the report, the Step 3 routing clause, and Step 5.
- `release`: the probe-rescoping rule in Step 7, and the two N/A template changes written
  down so a future merge does not re-derive the question.
- Memory layer: the `Occurrences` column, the "not auto-loaded" index header, the
  write-the-hypothesis-at-claim-time rule, and a corrupted duplicate `## Resolved`
  heading removed from `hypothesis-log.md`.
- `CLAUDE.md`: stamp → v1.37.0 with the *number-not-adjective* comment, and a
  `memory/MEMORY.md` row the table had never had — nothing loaded the index.

## The reviews found four defects a green suite did not

Two **cold** lenses (doc-accuracy, adversarial), one round, per v1.37.0's own new Step 5 —
cold rather than forks, because a fork inherits the blind spots that produced the diff.
They returned **largely disjoint** findings, which is the second recorded instance of that
here.

Two blockers, both in the merge:

1. **The dropped guard.** v1.37.0's Step 1 terminator reads "…stop — *but only after
   `$BASE` resolved*. If the baseline could not be resolved, that is the finding." The
   merge lost that sentence. Restored.
2. **The fallback beneath it was independently wrong**, and this one is a framework
   defect. `BASE=<root commit>` with `"$BASE"...HEAD` excludes the root commit's own
   content, because three-dot diffs from `merge-base(BASE,HEAD)` — which *is* the root
   commit. Measured: 2 of 3 insertions, 4 of 5, and **0 of 1** on a repo whose entire
   change is its first commit. `$BASE` *resolved*, so the guard in (1) could not have
   fired either. Fixed by naming the empty tree
   (`git diff --stat $(git hash-object -t tree /dev/null)..HEAD`, two dots).

Plus: two dangling framework paths, framework issue numbers colliding with real ones here
(`#64`, `#52`, `#50` all resolve to real energydatahub items), a five-vs-four lens-count
contradiction I introduced, "six files" when the diff was seven, and `CLAUDE.md`'s
"H1–H5" against a log holding H1–H11 with seven open. All fixed.

## Upstream

Two issues filed by the framework session on this session's findings —
**agent-ready-projects#145** (Step 1 cannot see untracked files while the magnitude gate
carves out "any new file in a HIGH path") and **#149** (the baseline fallback, as a
blocker), plus **#150** for three smaller Step 1.5 items. #145 is the 2026-08-08 gotcha
from this repo, still unfixed upstream 13 months on — and an adoption merge is exactly
where it comes back.

## Open for the engineer

- **Auto-loaded budget: 37,851 chars** (`CLAUDE.md` 35,265 + user-level 2,586). Over the
  35k soft target, under the 40k hard cap. Table padding is only 177 chars, so this is
  genuine content and trimming is a structural decision.
- **`/tmp` is a shared 15G tmpfs at 80%** and intermittently failed writes with
  `Disk quota exceeded` mid-session. The bulk is other live sessions' scratch
  (`llm-distillery` 4.7G, `pipeline-atlas` 2.4G, `ovr-news` 1.6G); only this project's
  dead-session scratch was freed.
- **`CLAUDE.md:337`** runs `install-global-skills.sh` from the maintainer clone's working
  tree, which moved during this session (`47016f9` → `db60a92`, i.e. `v1.37.0-3-g…`).
  Byte-identical to the tag today, so no exposure — but unpinned. Left unchanged
  deliberately: a peer session suggested pinning, and that is not a peer's call.
- **Lingering gotchas**: 13 entries older than 14 days carry no `[RESOLVED]` marker, most
  from 2026-08-08. Several are recorded resolved in the Promoted table but unmarked in
  their headings, which makes them re-report every run.
