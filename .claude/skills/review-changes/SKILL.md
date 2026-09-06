---
name: review-changes
description: Diff-driven pre-commit review — picks review lenses based on what changed, from a single adversarial pass to the full multi-lens battery
disable-model-invocation: false
---

Pre-commit review of pending changes. Scope and depth are driven by what changed, not a fixed checklist.

Adopted from agent-ready-projects `templates/review-changes.md` (v1.37.0), **merged, not re-copied**. The risk tiers, the guarantee lens, the `end-to-end-trace` lens and the YAML half of the shell lens name files and failure patterns in *this* tree — that is why this skill is project-local and must never be installed user-globally, where it would shadow every other repo's own copy.

**One deliberate divergence from the template, kept across the v1.17.0 → v1.37.0 merge**: Step 1 below retains `git status --porcelain --untracked-files=all`. The template dropped the untracked term from Step 1 and carries it only in Step 1.5's file list, so *classification* there cannot see a new file. This repo added that term after a measured blocker — a 695-line commit, 645 of them untracked under `.claude/` and `docs/work-items/`, on which `git diff --summary` returned empty while the magnitude gate's "a new file in a HIGH path" carve-out was supposed to fire. Do not drop it on a future merge.

## Step 1 — Diff and classify

Resolve the review baseline first — **every command in this step depends on it.**

**The baseline is the default branch — `@{u}` only on the default branch itself.** On a branch that is committed and pushed but not merged — the commonest state in which anyone wants a pre-merge review — `@{u}` is *empty*, because the upstream exists and is current. Every `@{u}`-derived term then reports zero and the step reads as "nothing to review" on a whole PR. Resolve it once, **here, before anything else in this step**, and reuse it everywhere below — the tier table, the magnitude gate and Step 1.5 all read `$BASE`:

```bash
# Every arm ends in a success, or `set -e` aborts here — before the fallback below,
# which is the one place that reports the failure. Measured: a repo with no remote
# and no main/master branch died at the loop with no output at all.
BASE=$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD) ||
# Fully qualified: `rev-parse` resolves refs/heads/ before refs/remotes/, so a
# LOCAL branch literally named `origin/main` would win and silently reintroduce
# agent-ready-projects#64 — measured, with only a stderr `ambiguous` warning nothing reads.
BASE=$(for c in refs/remotes/origin/main refs/remotes/origin/master main master; do
         git rev-parse --verify --quiet "$c" >/dev/null && { printf %s "$c"; break; }; done) || :
# On the default branch HEAD...HEAD is empty, so the upstream is the baseline.
if [ -n "${BASE:-}" ] && [ "$(git rev-parse --abbrev-ref HEAD 2>/dev/null)" = "${BASE##*/}" ]; then
  BASE=$(git rev-parse --abbrev-ref '@{u}' 2>/dev/null || printf %s '')
fi
# A name that resolves to nothing is the dangerous case: an empty or dangling BASE
# makes "$BASE"...HEAD an empty diff, which is what a clean tree also yields. Fall
# back to the whole branch — over-reporting is the safe direction for a review tool.
# No `${BASE:-sentinel}` placeholder here: any word chosen as a sentinel is a
# legal branch name, and if it exists the check passes while BASE stays empty,
# so the guard below fires with a diagnosis that is simply wrong. Measured.
{ [ -n "${BASE:-}" ] && git rev-parse --verify --quiet "$BASE^{commit}" >/dev/null; } || {
  BASE=$(git rev-list --max-parents=0 HEAD 2>/dev/null | tail -1)
  echo "BASELINE UNRESOLVED — falling back to the root commit $BASE." >&2
  echo "⚠️  \"\$BASE\"...HEAD EXCLUDES the root commit's OWN content, so a repo whose entire" >&2
  echo "    change sits in its first commit diffs to NOTHING here. Use the EMPTY TREE," >&2
  echo "    which really does review everything:" >&2
  echo "      git diff --stat \$(git hash-object -t tree /dev/null)..HEAD   # two dots, not three" >&2
  echo "    and report the unresolved baseline as a FINDING, not as a clean result." >&2
}
: "${BASE:?no commits in this repository — nothing can be reviewed}"

git diff --shortstat "$BASE"...HEAD    # committed on this branch
git diff --shortstat                   # unstaged
git diff --cached --shortstat          # staged
```

**Resolving a name is not enough — it has to resolve to a commit.** A ref can look fine and diff to nothing. That is why the block validates `^{commit}` and, on failure, falls back to the root commit and says so: an unresolved baseline and a clean tree produce identical output.

⚠️ **The root-commit fallback is partial, and the template's wording for it was wrong.** `"$BASE"...HEAD` diffs from `merge-base(BASE, HEAD)` — which *is* the root commit — to HEAD, so the root commit's own content is excluded. Measured on scratch repos: a 3-commit repo reported 2 of 3 insertions, a 5-commit repo 4 of 5, and a repo whose entire change was its single first commit reported **0 of 1** while `git show --stat HEAD` listed the file. The template says "reviewing the whole branch instead"; it is not. That is why the block now names the **empty tree** — `git diff --stat $(git hash-object -t tree /dev/null)..HEAD`, two dots, not three — which genuinely does review everything, and calls the unresolved baseline a finding. Measured on the single-commit repro: the three-dot form reports nothing, the empty-tree form reports the full `1 insertion(+)`. The empty tree is not used as `$BASE` itself because every other consumer in this file uses the three-dot form and it is not a commit; it is the companion you run on this one path.

The loop covers the cases where `origin/HEAD` is absent: `git init` + `git remote add` with no fetch, and older git. (When it is set: `docs/rationale/review-changes.md` **in the agent-ready-projects clone** — not this repo, whose `docs/` is the Pages publish root and holds no such file.) ⚠️ It does **not** cover a remote whose default branch is neither `main` nor `master` *and* whose `origin/HEAD` is unset: a repo defaulting to `develop` falls through to the root-commit fallback and reviews the whole branch — over-reporting, the safe direction, but a fallback rather than the intended path.

⚠️ **`$BASE` lives in a shell, and Step 1.5 needs it. Run Step 1.5's blocks in the same shell invocation as this one** — paste them together, or re-run this block at the top of that shell. A tool call that starts a fresh shell does not inherit it, and Step 1.5 is written to abort rather than proceed with the term missing. That abort is the intended behaviour: the alternative is Step 1.5 quietly reviewing a fraction of the change, which is agent-ready-projects#64 one step later.

Now run all five lines below — seven commands, since lines 2 and 4 hold two each. Lines 1 and 3 carry the `"$BASE"...HEAD` term and see work **already committed on this branch**; lines 2 and 4 see tracked **working-tree** changes, staged and unstaged; line 5 is the only one that sees **new files**:

```bash
git diff --stat "$BASE"...HEAD                  # committed on this branch — the arp#64 term
git diff --stat; git diff --cached --stat       # tracked, modified
git diff --summary -M "$BASE"...HEAD            # modes, renames, submodules, binaries
git diff --summary; git diff --cached --summary #   — on the branch and in the tree
git status --porcelain --untracked-files=all    # UNTRACKED — invisible to every command above
```

**`--stat` alone cannot see a mode change, a rename, a submodule, or a binary** — all four render as zero or near-zero lines, and three of them are carve-outs below. `--summary` without the baseline term cannot observe any of them on a pushed branch, which is agent-ready-projects#64 surviving its own fix in the place nobody re-read.

**No `git diff` variant sees an untracked file at all.** This is not a refinement; it is the difference between reviewing a change and reviewing nothing. The commit that introduced this skill was 695 lines, 645 of them in new untracked files under `.claude/` and `docs/work-items/` — `git diff --summary` returned empty on it, and the magnitude gate below declares "a new file in a HIGH path" always-full-depth. Without `git status --porcelain --untracked-files=all`, that carve-out fires on a file set the skill cannot observe.

A carve-out you cannot observe is not in force.

Classify each changed file into a risk tier:

| Tier | File patterns | Depth |
|------|-------------|-------|
| **HIGH** | `collectors/**`, `utils/**`, `/data_fetcher.py`, `scripts/**`, `.github/workflows/**`, `.claude/**`, `.gitignore`, `settings.ini`, `data/_shape_signatures.json`, `data/_upstream_empty_streak.json`, `tests/**`, `pytest.ini`, `requirements.txt` | Full battery (4-5 lenses) |
| **MEDIUM** | `CLAUDE.md`, `docs/**`, `/README.md`, `.github/dependabot.yml` | Two lenses (adversarial + doc-accuracy) |
| **LOW** | `memory/**`, `memory/work-items/**`, `data/**`, `legacy/**` | One lens (adversarial) |

`**` crosses directory levels; a leading `/` anchors to the repo root. **The most specific matching pattern wins.** Where no pattern is more specific than another, take the highest tier.

The HIGH row is everything that reaches a downstream consumer or runs unattended. Six entries are easy to miss and each is here because it burned this project, or would: `scripts/**` and `.github/workflows/**` run on a machine nobody is watching at 16:00 UTC; `utils/**` holds the schema registry and the quality gate, so a defect there is published under encryption before anyone reads it; `.gitignore` is what keeps `secrets.ini` out of a public repo; and `tests/**` is the only thing standing between a plausible-looking collector change and a silent data gap.

The last two are the counter-intuitive ones, and both were MEDIUM/LOW until this skill was reviewed against itself:

- **`settings.ini`** holds `encryption = 1`. Flipping that one digit publishes plaintext to a public site — and the guarantee lens, which is the only lens that owns "AES-CBC + HMAC-SHA256 on everything published", is HIGH-only. At MEDIUM it would never have run on the single file that can turn encryption off.
- **`data/_shape_signatures.json` and `data/_upstream_empty_streak.json`** are committed *control state*, not data: the first is the schema-drift tripwire's comparison baseline, the second is the #38 escalation counter. Editing either changes what CI blocks on. The rest of `data/**` really is data and stays LOW — this is why "most specific pattern wins" matters.

### Magnitude gate

The tier above is set by *path*. Depth is also set by *size* — but size is the weaker signal, so the exceptions are stated first and override everything below them.

**Always full depth, regardless of size.** Each of these is dangerous *because* it is small:

- **`.gitignore`** — one line decides whether `secrets.ini` is published. This repo is public.
- **Renames and moves** — `git diff --stat` reports `0 insertions(+), 0 deletions(-)` under `-M`, while every reference to the old path breaks.
- **Permission changes** — also zero insertions and deletions, invisible without `--summary`.
- **Binary files and submodule pointers** — the other two members of the zero-line class.
- **Any change to a shell script, a workflow YAML, or anything under `scripts/`** — code that runs unattended, where shell and YAML both break in one character.
- **Any change to `CURRENT_SCHEMA_VERSION`, `SCHEMA_CHANGELOG`, or a `_migrate_X_to_Y` function** — a schema bump is a three-line diff with an unbounded downstream blast radius. Use `/release` for these, not just this review.
- **Any change to `DATASET_MISSING_SEVERITY`, `EXPECTED_DATA_TYPE`, `CRITICAL_FEEDS`, or `VOLATILE_SHAPE_FEEDS`** — one dict entry changes what the quality gate blocks on.
- **Any diff that removes or loosens a check** — a deleted guard, a weakened assertion, a widened `try/except`, a severity downgraded from `critical`, a threshold raised. Loosenings are characteristically a handful of lines.
- **A new file in a HIGH path** — the tier for new content has not been decided yet.

**Otherwise size sets the depth.** Size means the whole change that will land, not the slice in front of you. Sum staged, unstaged, and any local commits not yet pushed:

```bash
git diff --shortstat; git diff --cached --shortstat
git diff --shortstat "$BASE"...HEAD    # committed on this branch
```

⚠️ **Not `git log @{u}..`.** On a branch that is committed and pushed but not merged — the commonest state in which anyone wants a pre-merge review — `@{u}` is empty, because the upstream exists and is current. This skill used `git log @{u}..` until the v1.37.0 merge; reproduced on a scratch repo with a pushed feature branch, all four of its Step 1 commands returned empty on a real 3-line change, so the skill reported "nothing to review" on a whole PR.

| Changed lines | Depth |
|---------------|-------|
| **< 20** | One adversarial pass |
| **20–200** | Path tier as above |
| **> 200** | Full battery, whichever tier the paths fall in |

Run that single pass in a **fresh context** — a subagent. Reviewing your own edit in the context that produced it is the self-certification failure this skill exists to prevent; the saving comes from running *one* independent reviewer instead of five, not from dropping independence.

The gate changes how many lenses run. It never changes *whether* a change is reviewed — every diff still gets at least one adversarial pass.

**If a changed file matches no pattern, treat it as MEDIUM, and name it in the report under "Unclassified"** even when a HIGH file in the same diff makes the tier moot. An unrecognized path is usually new content whose tier nobody has decided yet. Do not silently drop it, and do not default it to LOW. If it is executable, escalate to HIGH.

**`docs/` is the GitHub Pages publish root** — `.github/workflows/collect-data.yml` uploads the whole directory as the Pages artifact, served verbatim. Documentation prose there is MEDIUM because publishing it is the intent. But any *new kind of content* landing under `docs/` is published to the open internet on the next daily run, so ask what it is before accepting the MEDIUM tier: internal savepoints, incident notes, or anything describing an unfixed weakness do not belong there. This is not hypothetical — `docs/work-items/` was placed under the Pages root by the same commit that created this skill and had to be moved to `memory/work-items/`.

If no files changed, report "nothing to review" and stop — **but only after `$BASE` resolved.** If the baseline could not be resolved, that is the finding: report it, and never as a clean result. An unresolved baseline and a genuinely clean tree emit byte-identical output, and this skill exists because the first of those looks like the second.

**Step 1.5 below is never skipped** — not by the magnitude gate, not by a LOW tier, not by a small diff. It trims lenses, not the deterministic check.

## Step 1.5 — Structural pre-check

⚠️ **Issue numbers in this section (`#50`, `#52`, `#77`, `#103`) are agent-ready-projects issues, not this repo's**, and they collide: our `#52` is the shared host circuit breaker, our `#50` is the alert tracking issue, our `#64` is a merged PR. Elsewhere in this file a bare `#NN` means *this* repo (e.g. `#38`). The awk block below is byte-identical to the framework's, which is what keeps a future merge a diff rather than a re-read — do not renumber its comments; this note is the fix.

Runs at **every tier and every magnitude**, before any lens, on every changed markdown file — the single-adversarial-pass gate above trims lenses, not this. It is deterministic, so it costs nothing to run and does not need a model to evaluate, which is the reason it is a step rather than a lens.

The lenses below all read *content*: does this path exist, is this flag right, what would a future session do wrong. None of them asks whether the file is still **valid markdown** after the edit. That gap matters disproportionately here, because the memory layer is predominantly wide tables — inventories, index files, machine lists — where a row is one very long line. A `|` added inside a cell (a regex like `'recordfail|initrdfail'`, an `||` in a shell fragment, an alternation in a note) pushes cells past the end of the table, and **GFM drops the excess silently**. It reads fine as prose in the diff and is wrong only when rendered, so a human reviewer and an adversarial lens both pass it.

```bash
  : "${BASE:?run the Step 1 baseline block in THIS shell invocation — a fresh shell does not inherit it}"
{ git -c core.quotePath=false diff --name-only
  git -c core.quotePath=false diff --cached --name-only
  git -c core.quotePath=false diff --name-only "$BASE"...HEAD 2>/dev/null
  git -c core.quotePath=false ls-files --others --exclude-standard; } |
  sort -u | grep '\.md$' | while IFS= read -r f; do
  [ -f "$f" ] || continue
  awk -v F="$f" '
    function cells(s,   t, n) {
      t = s; gsub(/\\\|/, "", t)
      sub(/^[ \t]+/, "", t); sub(/[ \t]+$/, "", t)
      sub(/^\|/, "", t); sub(/\|$/, "", t)
      n = gsub(/\|/, "|", t); return n + 1
    }
    function isdelim(s,   t) {
      t = s; gsub(/\\\|/, "", t); gsub(/[ \t]/, "", t)
      return (t ~ /-/ && t ~ /^[|:-]+$/)
    }
    # `$(0)`, never `\$0`: skill ARGUMENTS are substituted into the skill BODY, so a
    # bare `\$0` arrives as the first argument word and this program examines a
    # constant while printing what a clean run prints. See #77.
    { sub(/\r$/, "") }               # CRLF: strip before anything reads the line,
                                     # or isdelim() never matches and no table in
                                     # the file is examined. See #52.
    # YAML frontmatter, skipped whole: `isdelim()` accepts a bare `---` and its
    # guard is satisfied by a pipe in the PREVIOUS line, so a closing `---` under
    # `description: Runs a | b` reported as a malformed table — and every SKILL.md
    # here has a `description:`. Preferred over requiring a pipe in the delimiter
    # row, which would reject the pipe-less rows GFM permits (#52).
    NR == 1 && $(0) ~ /^---[ \t]*$/ { infm = 1; next }
    infm && $(0) ~ /^(---|\.\.\.)[ \t]*$/ { infm = 0; prev = ""; next }
    infm { next }
    {
      bare = $(0); sub(/^ ? ? ?/, "", bare)
      if (bare ~ /^```/ || bare ~ /^~~~/) {
        c = substr(bare, 1, 1); n = 0
        while (substr(bare, n + 1, 1) == c) n++
        if (fch == "") { if (n >= 3) { fch = c; flen = n } }
        else if (c == fch && n >= flen) fch = ""
        intbl = 0; prev = ""; next
      }
      if (fch != "") next
      # Emphasis spans — the third construct with Step 1.5’s property: correct in
      # the diff, wrong when rendered (#50). Deliberately NARROW. The table check
      # reached a 39% false-positive rate before being anchored, so this reports
      # only the shape actually observed to break: a backticked token whose
      # content abuts `**`, sitting on a line that also carries bold OUTSIDE the
      # backticks. A formatter can join the two runs and corrupt both. Broader
      # rules (counting `**` per line, balancing across lines) were rejected —
      # they fire on ordinary bold and on multi-line spans.
      # Mask each code span to ONE character — \001 if its content abuts `**`,
      # \002 otherwise — so bold runs can be paired positionally. Adjacency is
      # the discriminator, and nothing weaker works: "a risky token anywhere on a
      # bold line" reported 28 lines here, and "two of them" still reported 15 —
      # every risk-tier row, where `**HIGH**` opens AND CLOSES in one cell and the
      # globs sit in the next. Only a token INSIDE an open bold run can be joined.
      { masked = ""; rest = $(0)
        while (match(rest, /`[^`]*`/)) {
          inner = substr(rest, RSTART + 1, RLENGTH - 2)
          mark = "\002"
          if (inner ~ /\*\*$/ || inner ~ /^\*\*/) mark = "\001"
          masked = masked substr(rest, 1, RSTART - 1) mark
          rest = substr(rest, RSTART + RLENGTH)
        }
        masked = masked rest
        inb = 0; nrisk = 0
        for (i = 1; i <= length(masked); i++) {
          if (substr(masked, i, 2) == "**") { inb = 1 - inb; i++; continue }
          if (inb && substr(masked, i, 1) == "\001") nrisk++
        }
        # The backtick test guards against a literal \001/\002 byte in the source
        # masquerading as a masked span: without it, a line with no backticks at
        # all reported "two backticked tokens", a message that is simply false.
        # No tracked file here contains those bytes; the message would be wrong
        # anyway, and a wrong message is what sends a reader to the wrong line.
        if (nrisk > 1 && index($(0), "`"))
          printf "%s:%d: two backticked tokens abutting ** inside one bold span — a formatter can join the runs and corrupt both\n", F, NR
      }
      if (isdelim($(0)) && prev != "" && (index($(0), "|") || index(prev, "|"))) {
        base = cells($(0)); intbl = 1
        if (cells(prev) != base)
          printf "%s:%d: header has %d cells, delimiter row defines %d — not a valid table\n", F, NR-1, cells(prev), base
        prev = $(0); next
      }
      if (intbl) {
        if ($(0) ~ /^[ \t]*$/) intbl = 0
        else if (index($(0), "|") && cells($(0)) > base)
          printf "%s:%d: row has %d cells, table defines %d — the excess is dropped when rendered\n", F, NR, cells($(0)), base
      }
      prev = $(0)
    }
    END { if (fch != "") printf "%s: unclosed %s code fence\n", F, fch
          # An unclosed frontmatter leaves `infm` set, so `infm { next }` swallows
          # every remaining line and NO table is examined — the check then prints
          # exactly what a clean run prints. That is the same silence the
          # frontmatter rule was added to remove, in the one step whose purpose is
          # catching corruption invisible in the diff (#103). Report the state
          # rather than recovering from it: a file whose frontmatter never closes
          # is malformed on its own, and guessing where it should have ended is
          # how a check starts inventing findings.
          if (infm) printf "%s: unclosed YAML frontmatter — NO table in this file was examined\n", F }
  ' "$f"
done
```

The file list is the union of unstaged, staged, **everything committed on this branch**, and **untracked** — `git diff` in any form never lists a file git has not seen, and a brand-new document is where fresh corruption is most likely. `core.quotePath=false` is load-bearing: git otherwise renders a non-ASCII path as `"caf\303\251.md"`, which does not end in `.md`, so the file is dropped from both the check and the count with no error.

**The delimiter row defines the table, and only *excess* cells are reported.** GFM inserts empty cells when a row is short and discards them when a row is long, so a short row renders exactly as intended and is not a defect — a section-divider row like `| **PART ONE** |` inside a wide table is idiomatic, not corruption. A long row loses data.

Hits come in **five** shapes — the template says three, but the program carries five `printf` sites and two of them are outside that enumeration. The two unlisted ones are an **emphasis-span** hit (`two backticked tokens abutting ** inside one bold span`) and an **unclosed-frontmatter** hit, which is a *denominator* signal — it says NO table in that file was examined — and not a table defect at all, so the escape-the-pipe fix below does not apply to it. The three the template enumerates: a row whose excess cells are discarded, a header that disagrees with its own delimiter row (which means GFM renders no table at all), and an unbalanced code fence. This includes pipes inside backticks — GFM splits a row into cells *before* it parses inline content, and its spec says so explicitly, so a `|` in an inline-code span breaks the row exactly like a bare one. Fix each (escape as `\|`, or move the command out of the table) before running the lenses.

**Treat a hit as real until you have looked at it, not as proven.** A hit says the row supplies more cells than the delimiter row defines, and GFM discards the excess. That is a loss only when the discarded cells carry content — `| 1 | 2 | |` against a two-column delimiter reports, and loses nothing. And it says nothing about whether you are looking at a table at all: `isdelim()` accepts a bare `---` and its guard is satisfied by a pipe in the *previous* line, so a setext heading and a spaced `- - -` break can each report. **Frontmatter no longer can, at line 1** — the merged program skips a line-1 block whole; seeded `description: a | b` produces no hit. It still fires on frontmatter that does *not* start at line 1. Classes and repros in #52.

⚠️ **One false-positive class the template does not name**: `sub(/^ ? ? ?/, "", bare)` strips at most three leading spaces, so a **4-space-indented** fenced block — the normal shape inside a list item — and a plain 4-space indented code block are both scanned as markdown. Seeded: each reports `row has 3 cells, table defines 2`. Check the indentation before believing a hit inside a list.

**Known blind spots, so a clean result is not read as more than it is**: tables inside blockquotes are not examined, nor is a table whose delimiter row is itself missing. The check finds lossy rows in well-formed tables; it is not a markdown validator. **Lone CR is still a blind spot** and a worse-behaved one: awk sees the whole file as a single record, so no table is examined and the fence check misreports — a lone-CR file whose fence is correctly *closed* is reported as unclosed. Seeded both ways: that false hit fires only when the fence is the file's **first** construct; with a heading above it the check goes silent instead, which is the more dangerous of the two.

The command prints nothing on a clean run — which is also what it prints when the file list was empty. **Report the count alongside the result** so the two are distinguishable:

```bash
  : "${BASE:?run the Step 1 baseline block in THIS shell invocation — a fresh shell does not inherit it}"
{ git -c core.quotePath=false diff --name-only
  git -c core.quotePath=false diff --cached --name-only
  git -c core.quotePath=false diff --name-only "$BASE"...HEAD 2>/dev/null
  git -c core.quotePath=false ls-files --others --exclude-standard; } |
  sort -u | grep -c '\.md$'
```

That count is files *in scope*, not files you edited: the baseline term includes everything committed on this branch, and `ls-files --others` includes every untracked markdown in the tree. If it is zero while the Step 1 diff listed markdown files, the pipeline is broken — not the changes clean. It reads `$BASE` from Step 1, **in the same shell**: an unset `$BASE` aborts this pipeline rather than dropping its largest term, so a fresh shell gives you a loud failure and not a small number.

## Step 2 — Execute review lenses

For each lens, spawn a subagent with the prompt below. Run lenses concurrently.

### Lens: guarantee-preservation (HIGH only)

```
You are reviewing changes to energyDataHub — a data collection pipeline whose output
is consumed by a downstream ML project (Augur) that cannot see this code. These files
carry guarantees that must hold for every published file.

For each changed file, identify what it guarantees:

- utils/schema_registry.py: any shape change bumps CURRENT_SCHEMA_VERSION *and* adds a
  _migrate_X_to_Y function *and* adds a SCHEMA_CHANGELOG entry. All three or none.
  Migrations are backward-compatible: an old file must still load.
- utils/data_types.py: the {metadata, data} envelope. EnhancedDataSet/CombinedDataSet
  shape is the published contract.
- utils/data_quality.py: DATASET_MISSING_SEVERITY is the SINGLE registry for missing-feed
  severity — no parallel list may reappear. Only 'critical' aborts a publish.
- utils/secure_data_handler.py: AES-CBC + HMAC-SHA256 on everything published; keys never
  inlined, never logged.
- settings.ini: `encryption = 1`. This is the switch that decides whether the published
  files are ciphertext or plaintext. Any diff that clears, comments out, or conditionalises
  it publishes the whole dataset in the clear to a public GitHub Pages site. Treat a change
  here as GUARANTEE WEAKENED unless the diff also explains why publishing plaintext is
  intended.
- data/_shape_signatures.json: the schema-drift tripwire's baseline — it must stay tracked,
  or `git show HEAD:` in scripts/detect_schema_drift.py resolves to nothing and the tripwire
  silently passes. Same for data/_upstream_empty_streak.json and the #38 escalation counter.
- collectors/base.py: retry + circuit breaker + validation contract. NonRetryableError
  bails without retry; UpstreamNoDataError fast-fails without tripping the breaker.
  collect() resets quality issues and auto-injects metadata['collector_quality_issues'].
  A collector overriding collect() must preserve that whole chain.
- data_fetcher.py: the 8-touchpoint published-dataset checklist in
  memory/project_published_dataset_checklist.md. Adding a feed to some touchpoints but
  not all silently breaks publishing — this was a real BLOCKER on c40a53b.
- .github/workflows/collect-data.yml: completeness tripwire, schema-drift tripwire, quality
  gate, and the Pages deploy job all still run and still fail loudly. Actions stay SHA-pinned.
- scripts/detect_schema_drift.py: within-feed drift fails, catalog drift warns, removed
  CRITICAL_FEEDS escalate. Volatile membership stays derived from history, not hand-listed.
- All timestamps normalized to Europe/Amsterdam.

For each guarantee: does the change preserve it? Flag any weakening.

Then ask: is the change broader than its stated intent?

Report: GUARANTEE OK or GUARANTEE WEAKENED for each surface touched.
```

### Lens: adversarial (all tiers)

```
You are an adversarial reviewer. Your job is to refute the changes — find what breaks,
what edge cases fail, what assumptions don't hold.

For each changed file:
1. What is the change trying to accomplish?
2. What could go wrong? Find at least one concrete failure scenario.
3. Are there silent failure modes — things that would pass but be wrong?
4. If this is a test or CI-guard change: what real failure does the weaker check now pass?
5. If this touches a collector or the publish path: what would Augur see, and would it be
   able to tell a degraded feed from a healthy one?

This pipeline runs unattended once a day. A failure that surfaces as a green run is worse
than a crash. Weight silent-failure scenarios accordingly.

Go in assuming the change is refutable. Report REFUTED with a concrete failure — a
triggering input, an edge case, or a contradiction between two things the change now
asserts. Prose contradictions count and often have no triggering input; do not withhold
one for lacking a repro. Report NOT REFUTED only after a thorough attempt has failed.

**A claim that needs a measurement gets one, gets hedged, or is not ready.**
Two shapes need one, and they are the same failure from two sides:

- **Negatives.** "0 rows", "not called anywhere", "nothing reads it", "no other
  callers", "all clean" — a negative cannot distinguish a real absence from a
  broken instrument, an empty sample, or a mismatched population. Report the
  claim, the command that produced it, and **what a non-empty result would have
  looked like**. If you cannot state the shape of a positive, the claim is not
  ready to make. Where a negative is being used to *license a loosening* — "no
  false positives", "nothing was affected" — seed a positive first: a run that
  finds nothing cannot distinguish a fixed check from a disabled one.
- **Absolutes in descriptions.** *every*, *all*, *always*, *never*, *none*,
  *zero*, *cannot*, *impossible*, *no … can*, *not permitted*, *guaranteed* —
  in a claim about how a tool, spec or codebase **behaves**. An absolute in an
  *instruction* is a decision and is fine: `never edit in place` prescribes.
  An absolute in a *description* is a measurement, and it ships unmeasured by
  default. Each needs a measurement with its scope, a spec citation, or a hedge
  ("in the cases measured", "for well-formed tables") — and if none of those is
  available, the claim is not ready to make.

**A claim whose measurement cannot be taken yet is a finding in its own right.**
Report it as one. Do not attempt to register it here: this lens reports, it does
not write, and a hypothesis needs a Method and a Revisit trigger that the
reviewer of a diff is not placed to supply. `memory/hypothesis-log.md` says
what an entry requires, and it is written **by the author, at the time of the
claim** — not deferred to `/curate`, which runs at end of session and so
reinstates exactly the delay the log exists to remove. `/curate` Step 0 sub-step 7
keeps the entries that exist honest, reviewing open ones for staleness and due
dates;
it does **not** detect a claim that never got an entry, so writing it at claim
time is the only thing that does.

This is not a step to perform; it is the sentence to write. A separate
"verify your claims" step is skippable in exactly the cases where it matters.
Making the check travel with the claim is what makes omitting it visible.

Report: REFUTED or NOT REFUTED, with failure scenario if refuted. Every negative
in your report carries its command and the shape of a positive; every absolute
about behaviour carries its measurement, its citation, or its hedge.
```

### Lens: doc-accuracy (MEDIUM and HIGH)

```
You are reviewing documentation changes for accuracy against disk state.

1. Does every file path mentioned actually exist on disk?
2. Does every command use correct flags and syntax? Run the read-only ones.
3. Do version numbers, dates, issue numbers, and run IDs match what shipped?
4. If a new collector, dataset, or script is documented, does it exist and is it wired in?
5. Internal inconsistencies — does CLAUDE.md say one thing and memory/MEMORY.md another?
6. Does any <!-- verify: ... --> comment still return what the surrounding claim asserts?

Report: ACCURATE or INACCURATE, with the specific mismatch.
```

### Lens: shell-and-yaml-correctness (HIGH only, when `scripts/**`, `*.sh`, or `.github/workflows/**` changed)

```
You are reviewing shell and GitHub Actions changes for correctness.

Shell:
1. set -u safe? No unbound variables on the changed paths.
2. Quoting correct? No word-splitting bugs, spaces in filenames handled.
3. Edge cases: empty input, missing files, unexpected exit codes.
4. Non-determinism introduced (date, random, network)?
5. Are error exits explicit and loud, not silent? A `|| true` added to quiet a failure is
   the single most common way this repo's tripwires stop tripping.

GitHub Actions:
6. Are all actions still SHA-pinned (not @v4 / @main)?
7. Does a step that must fail the run use `if: success()` rather than `if: always()`?
8. Does any step swallow a non-zero exit (pipes, `continue-on-error`, trailing `|| echo`)?
9. Are secrets referenced via ${{ secrets.* }} only, never echoed or written to a file
   that gets committed or published to docs/?

Report: OK or ISSUE, with the specific bug.
```

### Lens: end-to-end-trace (HIGH only, when a new field/check/signal is added)

```
This project has a documented three-incident failure pattern: a registered behaviour that
is expected to enforce or surface something silently no-ops at a downstream layer, and the
absence looks identical to "data is clean". Instances: GoogleWeather returned success exit
codes for 7 months while 401-ing; validate_value_ranges silently no-op'd on 2-level-nested
feeds for ~3 months; TenneT's collect() override bypassed BaseCollector's metadata
auto-inject so balance_delta_status populated in memory but never reached the published file.
Signature: unit tests green, live run silently drops the new field.

For every new or changed field, check, severity, or quality signal in this diff, trace it
through EVERY layer until it is observable on disk or in the operator log:

  reset -> fetch -> parse -> normalize -> validate -> metadata -> inject -> dataset
  -> data_fetcher save -> shape signature -> encryption -> docs/ publish -> quality report

Name the layer at which you lose sight of it, and say what would have to be observed —
a file under data/, a key in _shape_signatures.json, a line in the quality report — to
prove it survives. "The unit test asserts it" is not proof; that is exactly what was
green in all three incidents.

Second check, the parallel-registry rule: does this diff add an entry to a hand-maintained
list keyed on a feed/dataset identifier? If so, find every OTHER list keyed on the same
identifier and confirm the entry was added to all of them. Two incidents: data_quality.py
had 3 parallel severity lists; collect-data.yml had 2 parallel publishable-feed lists and
nordic_hydro was added to one but not the other.

Report: TRACED (naming the observable) or LOST AT <layer>, plus REGISTRIES CONSISTENT or
the list that is missing the entry.
```

## Step 3 — Synthesize

Combine all lens reports. Structural hits from Step 1.5 do not enter this table — they were fixed before the lenses ran; carry their count into the Step 4 summary instead. A hit you deliberately left unfixed enters here as a BLOCKER with the lens recorded as `structural`, and the summary count still includes it.

For each finding:
- **Severity**: BLOCKER (must fix before commit) / WARNING (should fix) / NOTE (consider)
- **Lens**, **File**, **Finding**, **Fix**

If any BLOCKER: recommend fixing before commit.
If only WARNING/NOTE: recommend the user review and decide.

## Step 4 — Report

```
## Review: [N] files changed, [tier] risk, [M] lenses

### Findings

| # | Severity | Lens | File | Finding |
|---|----------|------|------|---------|
| 1 | BLOCKER | adversarial | ... | ... |

### Unclassified

[Every changed file matching no tier row in Step 1, one per line — or "none".
Never omit this section. An empty one is evidence the check ran; a missing one
is indistinguishable from a check that was skipped.]

### Summary

- **Structural pre-check**: [N] markdown files checked, [N] problems
- **Lenses run**: [list]
- **Blockers**: [N] (must fix before commit)
- **Warnings**: [N]
- **Notes**: [N]
- **Verdict**: [READY TO COMMIT | FIX BLOCKERS FIRST | REVIEW WARNINGS]
```

## Step 5 — Fixing, and whether to run another round

**Reviewing is not where the cost is. Fixing is.** On this framework's own review ledger, of the findings classified as to origin, half were defects the previous round's own fixes created — 14 of 28, against 6 misses. ⚠️ A further **15** findings were never classified, so that is half of the classified subset, not of everything. Rounds appear to multiply because each round's fixes need another round to check them, so **a round cap may save nothing on its own — it can ship the introduced defects instead.** Sample size, exclusions, limits: `docs/rationale/review-changes.md` **in the agent-ready-projects clone**, not this repo.

- **A fix is a change, and takes the tier of the file it lands in.** Treating it as a correction too small and too well-understood to re-read is self-certification in miniature: small is why loosenings hide, and knowing the intent is what stops you seeing the result.
- **Re-read the steps that consume what you changed.** These defects live in the *relationship between* steps, which is why re-reading the fixed step alone finds nothing.
- **Fix one finding at a time when findings touch the same file.** Batched fixes interact, and the interaction is invisible in a diff that shows them as separate hunks.
- **Name what the fix could have broken before starting another round.** If you cannot name a candidate you have not looked; if you can, that is the next round's scope — far narrower than a battery.

### Budget the round before you spawn it

State the lens set and the ceiling **before** starting; record the cost after. Left unbudgeted, a review tends to expand toward the most expensive form available — observed here as four lenses on one diff, two stopped part-way for cost.

- **Decide the lens set up front.** Stopping a reviewer part-way spends its cost to that point and returns nothing. A lens not worth its cost should not be started.
- **Never run a lens for a class a deterministic check covers *completely*.** Partial coverage is not coverage: where the tier table mandates a lens and a check covers only part of its class, the tier table wins — say which part the check already settled, and let the lens have the rest.
- ⚠️ **Do not economise by collapsing lenses into the author's own context.** This is about *whose context* the reviewer holds, not how many run: one independent reviewer instead of this repo's five is a legitimate saving, and the magnitude gate prescribes exactly that for small diffs. Asking the questions inside the context that wrote the change is not — that reviewer holds the author's blind spots, the failure this skill exists to prevent. Observed once: two lenses on one diff returned **disjoint** findings.
- **Promote a recurring finding to a deterministic check instead of catching it again.** A check is written once and costs a fraction of a review per run; a finding that stays a finding is paid every round. Of the levers here it is the one that plausibly makes review cheaper *and* better, rather than trading one against the other.

## Do not

- Do not review your own edit in the context that produced it. Spawn a subagent.
- Do not report a check as passed without running it. `venv/bin/python -m pytest tests/ -x`
  is the project's test command — run it, don't assert it. Use `venv/bin/python`, not bare
  `python`: the system interpreter lacks `pytest-cov`, so `pytest.ini`'s `addopts` makes it
  die with `unrecognized arguments: --cov=.`, which reads as a broken config rather than a
  missing dependency.
- Do not resolve a finding by weakening the check that produced it.
