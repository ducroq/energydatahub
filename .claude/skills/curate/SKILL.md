---
name: curate
description: End-of-session curation — review gotcha log, promote patterns, update memory index, check doc sync
disable-model-invocation: false
---

End-of-session curation for the agent-ready-projects framework.

Review the session's work and update the layered memory system:

## Step 0 — Freshness check

Check for context rot from *previous* sessions. This catches what the session-focused steps below miss.

1. **Dead references**: Read the memory index and project file. For every file path mentioned, verify it still exists. List any broken paths.
2. **Stale memory**: Check modification dates of files in `memory/`. Flag any that haven't been modified in 30+ days — they may be outdated. (Use `git log -1 --format=%ci -- <file>` for each.)
3. **Lingering gotchas**: Read the gotcha log. Flag any unresolved entries older than 14 days — they're either fixed (mark `[RESOLVED]`) or stuck (surface to the user).
4. **Ground truth drift**: If the project file has a "Ground Truth Designations" table, verify each listed file exists and has been modified more recently than the artifacts that defer to it. Flag any where a downstream artifact is newer than its source of truth.
5. **Unverified state claims**: Scan memory files for state claims ("shipped," "deployed," "live," "running," "working in production"). For each claim found:
   - **Has `<!-- verify: ... -->` comment**: Run the command. Report **PASS** or **FAIL**. If FAIL, flag the entry for correction or removal — the claimed state is no longer true. If the command errors (non-zero exit, command not found, no output), report **ERROR** and flag for investigation — the verify command itself may be stale.
   - **Has `<!-- verify: manual — ... -->` comment**: Flag as **MANUAL CHECK NEEDED** with the noted reason. Surface to the engineer.
   - **No verification comment**: Flag as **UNVERIFIED**. These claims decay immediately after the session that wrote them. Suggest adding a `<!-- verify: -->` comment or requalifying the claim as a session observation.

6. **Hypothesis log surface**: If `docs/hypothesis-log.md` exists, scan its `## Open` section. For each entry:
   - **Past `Review by:`**: Flag as **DUE FOR REVIEW** — the deadline has arrived. Surface to the engineer with the entry's Position and Method so they can resolve (move to `## Resolved`) or extend the deadline.
   - **`Revisit trigger:` fired**: If the trigger references an evidence threshold ("once 7 days of cycles complete," "after 14 contiguous eval rows"), check whether that threshold is now met. If yes, flag as **TRIGGERED**. The agent shouldn't resolve the hypothesis — only surface it; resolution requires reading the Method and applying it, which is the engineer's call.
   - **Stale (no movement, no trigger)**: Just count how many open entries exist. If more than ~10, flag as memory-cluttering — entries that never resolve should either be promoted to ADRs or marked `dormant` / closed.

7. **Project file size budget**: Check the project file (`CLAUDE.md` for Claude Code, equivalent for other tools). Claude Code warns at 40k chars; the soft target is under 35k to leave headroom. If the file is approaching or over budget:
   - The most common cause is **session-narrative footers** (blocks like `_Last updated: ..._` / `_Earlier ..._`) accreting from prior sessions. These duplicate content that already lives in `memory/project_session_*.md` and is indexed in `MEMORY.md`.
   - Rule: keep at most **one** session footer block (the most recent), and only if it adds at-a-glance value the index can't carry. Drop older `_Earlier ..._` blocks — their content is preserved in session-memory files.
   - Don't trim structural sections (Hard Constraints, Before You Start, Architecture, Key Paths). Those are what the project file is *for*.
   - If trimming wouldn't get under budget, surface to the engineer — structural restructuring is their call, not the agent's.

Report findings before proceeding. Don't fix anything in this step — just surface what's stale so the engineer can decide.

## Step 1 — Gotcha log review

Read `memory/gotcha-log.md`. For each existing entry:
- If the root cause was fixed during this session, mark it `[RESOLVED]`
- If the same issue came up again, note the recurrence

Then check: did anything go wrong or surprise you during this session? For each one, append a new entry:

```
### [Short description] (YYYY-MM-DD)
**Problem**: What went wrong or was confusing.
**Root cause**: Why it happened.
**Fix**: What solved it.
```

## Step 2 — Pattern detection and promotion

Scan the gotcha log for entries that have recurred 2-3 times. For each:
- Propose promoting it as an "if [situation], then [what to do]" pattern
- Suggest where it belongs: the memory index (if broadly relevant) or a topic file (if subsystem-specific)
- If approved, add it to the destination and update the Promoted table in the gotcha log

## Step 3 — Memory index update

Read the memory index (`MEMORY.md`). Update:
- **Current State** — reflect what shipped or changed this session
- **Key File Paths** — add any important files discovered during work
- **Active Decisions** — add any architectural choices made, with ADR pointers if created
- Remove or correct anything that is now stale

**Don't accrete session narrative onto the project file footer.** Session-level "what happened today" belongs in `memory/project_session_YYYY_MM_DD.md`, with a one-line pointer added to `MEMORY.md`. The project file is structural context (constraints, architecture, key paths) — appending session footers there bloats it past the 40k Claude Code perf threshold within ~7 sessions and duplicates what the index already holds. If a previous workflow left footer blocks behind, Step 0 sub-step 7 catches and trims them.

## Step 4 — Doc sync check

Check whether key docs reflect the current repo state. Code changes during a session can leave docs stale — this step catches drift that inline updates missed.

1. **Project file Architecture section**: Compare listed files/directories against actual repo contents. Flag new files not listed, or listed files that no longer exist.
2. **Project file Key Commands / How to Work Here**: Verify commands still match actual CLI flags and defaults. Flag any mismatches (e.g., a renamed flag, a changed default).
3. **Runbook** (if it exists): Check that operational details (environment setup, deployment steps, common problems) match reality. Flag anything that looks stale.
4. **Backlog / active work tracking**: Check if any open items were resolved during this session. Mark them.

Fix what you can. Flag anything that needs engineer input.

## Step 5 — Verify references

Skip if Step 0 already ran a full freshness check. Otherwise, spot-check that paths mentioned in the memory index and project file still exist. Flag any broken references.

## Step 6 — Report

Summarize what you changed:
- **Freshness**: Dead references, stale memory files, lingering gotchas, ground truth drift (from Step 0)
- **Verification**: State claims checked — N passed, N failed, N unverified, N manual check needed (from Step 0)
- **Gotchas**: New entries added, entries resolved or promoted
- **Memory index**: Updates made
- **Doc sync**: Project file, runbook, backlog updates made or flagged (from Step 4)
- **Action needed**: Anything flagged that requires engineer decision
