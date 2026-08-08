# Work items

Savepoints for work that spans more than two sessions. One file per item, named
`short-slug.md`, from the agent-ready-projects `work-item.md` template (v1.17.0).

Five sections: **What & Why**, **Current Status** (the savepoint), **Decisions**,
**Open Questions**, **Outcome**. Not a lifecycle state machine — just enough structure to
resume after a context reset.

- Create one at the *start* of multi-session work, and add a one-line pointer in
  `memory/MEMORY.md` under the **"In flight"** section (not "Current State" — that section
  holds the standing state of the project, and mixing the two splits the index in half):
  `- [Short description] → memory/work-items/slug.md [in progress]`
- Fill in **Outcome** when the work lands or is abandoned; update the pointer to `[done]`.
- These files are temporary. Once the Outcome's durable residue has been promoted to its
  permanent home (an ADR, `memory/gotcha-log.md`, a topic file, or CLAUDE.md), **delete the
  file** and remove its pointer. Keeping it "as implementation history" is what turns a
  savepoint directory into a second, unmaintained archive — the session files already cover
  that role.

These live under `memory/`, not `docs/`, on purpose: `docs/` is the GitHub Pages publish
root, and a work item describes what is *not yet fixed*. This directory was created under
`docs/work-items/` and moved on 2026-08-08 before anything was published.

How this differs from what is already here:

| | Captures |
|---|---|
| `docs/decisions/ADR-*.md` | One-way-door decisions, frozen |
| `memory/gotcha-log.md` | Problems already solved |
| `memory/project_session_*.md` | What a *past* session did — a retrospective |
| `memory/work-items/*.md` | What is **in flight** right now |

The session files are the closest neighbour and the reason this directory was added late:
they record work after the fact, so picking up an unfinished thread meant reading several
of them and inferring the current state. A work item states it directly.
