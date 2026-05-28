---
name: audit-context
description: Periodic structural audit of the layered memory system — checks for duplication, wrong-layer placement, bloat, and broken references
disable-model-invocation: false
---

Structural audit of the agent-ready-projects layered memory system. Run monthly or after major restructuring. Complements `/curate` (session-level cleanup) with framework-level health checks.

## Step 1 — Document size

Check the auto-loaded files (project file and memory index). For each:
- Count lines
- Flag if over ~100 lines (project file) or ~60 lines (memory index) — these are heuristics, not hard limits
- If too long, identify sections that are reference material (looked up on demand, not needed every session) and propose moving them to topic files behind "Before You Start" pointers

## Step 2 — Cross-layer duplication

Check whether the same fact appears in multiple places across the layers:
- Project file (CLAUDE.md / AGENTS.md / etc.)
- Memory index (MEMORY.md)
- Topic files (memory/*.md)
- Tool-specific auto-memory (e.g. ~/.claude/projects/ for Claude Code)

For each duplicate found, recommend which layer should be the single source of truth based on:
- Is it needed every session? → project file
- Is it navigational? → memory index
- Is it reference material loaded on demand? → topic file
- Is it user-specific (preferences, positions, local machine quirks)? → tool auto-memory

## Step 3 — Wrong-layer placement

Check for content that's in the wrong layer:
- **User-specific data in project files**: personal preferences, positions, local machine limitations → should be in tool auto-memory
- **Session navigation in the project file**: "Current State", task progress → should be in the memory index
- **Always-needed constraints buried in topic files**: hard rules, thresholds, non-negotiables → should be in the project file
- **Derivable-from-code content in any memory file**: things `git log`, `grep`, or reading the source would tell you → shouldn't be persisted at all

## Step 4 — Reference integrity

For every file path mentioned in the project file, memory index, and gotcha log:
- Verify the file exists
- Flag any broken references

For every "Before You Start" pointer:
- Verify the target file exists
- Check that the trigger language is task-based ("when doing X, read Y") not passive ("see Y")

## Step 5 — Topic file reachability

Check that every topic file in memory/ has a task-triggered pointer in the "Before You Start" table. Flag orphaned topic files — they exist but no pointer leads to them, so an agent will never know to load them.

## Step 6 — Gitignore correctness

Check what's tracked vs untracked:
- Project-level context (project file, memory index, gotcha log, topic files) should be tracked in git
- User-specific data (tool auto-memory, personal notes, local credentials) should be gitignored
- Flag any mismatches

## Step 7 — Report

Summarize findings by severity:
- **Fix now**: broken references, misplaced secrets/credentials, orphaned files
- **Fix soon**: duplication, bloated auto-loaded files, passive pointer language
- **Consider**: minor size optimizations, optional restructuring

For each finding, state what's wrong, where, and propose a specific fix. Don't make changes without showing the plan first.
