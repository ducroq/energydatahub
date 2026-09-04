#!/usr/bin/env bash
#
# Smoke test for the publish-failure alerting path (#50, collect-data.yml's
# `alert` job).
#
# WHY THIS EXISTS. The alert job talks to the GitHub Issues API, and two
# separate defects in it survived a five-lens review on 2026-09-04:
#
#   1. `gh` resolves its target repo from the git remote, not from
#      GITHUB_REPOSITORY. The alert job has no actions/checkout, so every gh
#      call died with "fatal: not a git repository". Combined with
#      continue-on-error that is a green job that alerts nobody.
#   2. Dedup used `gh issue list --search "... in:title"`. GitHub's search
#      index is eventually consistent (~4s), so back-to-back failures each
#      opened a duplicate issue. `--label` as a server-side filter lagged too.
#      Only the unfiltered list is read-after-write consistent.
#
# Reviewers read both versions and missed both. One of them explicitly
# "verified" the search query — against an issue that was already indexed.
# Creating an issue and immediately looking for it caught both on the first
# run. The lesson is general: for anything that talks to an external API,
# exercise it rather than reading it.
#
# WHAT IT DOES. Walks the six links of the alerting chain against the REAL
# repo, then cleans up after itself:
#
#   1. query with no tracking issue present   -> expect empty
#   2. create the tracking issue              -> the first-failure path
#   3. find it IMMEDIATELY                    -> the dedup race (both bugs)
#   4. comment on it                          -> the repeat-failure path
#   5. close it                               -> the recovery path
#   6. query again                            -> expect empty
#
# It uses a distinct throwaway title so it can never be mistaken for a real
# alert, and deletes the issue at the end (falling back to closing it if the
# token cannot delete). It does NOT touch data/, docs/, or any collector.
#
# Usage:
#   scripts/smoketest_alerting.sh                 # against the current repo
#   GH_REPO=owner/name scripts/smoketest_alerting.sh
#
# Requires: gh, authenticated with issues-write. Keep this in step with the
# `alert` job in .github/workflows/collect-data.yml — if the queries there
# change, change them here and re-run.
#
# File: scripts/smoketest_alerting.sh
# Created: 2026-09-04

set -euo pipefail

: "${GH_REPO:=$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"
export GH_REPO
LABEL="${LABEL:-publish-failure}"
export LABEL
TITLE="[smoke test] alerting self-check — safe to ignore"

echo "Smoke-testing the alerting path against ${GH_REPO}"
echo "Label: ${LABEL}"
echo

cleanup() {
  if [ -n "${ISSUE:-}" ]; then
    gh issue delete "$ISSUE" --yes >/dev/null 2>&1 \
      || gh issue close "$ISSUE" --comment "Smoke test aborted." >/dev/null 2>&1 \
      || true
  fi
}
trap cleanup EXIT

# The query under test — must match the `alert` job exactly.
find_issue() {
  gh issue list --state open --limit 200 --json number,labels \
    --jq "[.[] | select(any(.labels[]; .name == env.LABEL))] | .[0].number // empty"
}

echo "1. query with no tracking issue"
pre=$(find_issue)
if [ -n "$pre" ]; then
  echo "   SKIP: issue #${pre} already carries the ${LABEL} label."
  echo "   That is a REAL open alert — resolve it before smoke-testing."
  trap - EXIT
  exit 0
fi
echo "   empty, as expected"

echo "2. create the tracking issue"
gh label create "$LABEL" --description "Automated: daily publish is failing (#50)" \
  --color B60205 >/dev/null 2>&1 || true
url=$(gh issue create --title "$TITLE" \
  --body "Smoke test of the alerting path. Created and removed automatically." \
  --label "$LABEL")
ISSUE="${url##*/}"
echo "   created #${ISSUE}"

echo "3. find it immediately (the dedup race)"
found=$(find_issue)
[ "$found" = "$ISSUE" ] || {
  echo "   FAIL: created #${ISSUE} but the query returned '${found}'."
  echo "   Dedup is broken — consecutive failures would open duplicate issues."
  exit 1
}
echo "   found #${found}"

echo "4. comment (repeat failure, no duplicate)"
gh issue comment "$found" --body "Second failure — dedup held." >/dev/null
echo "   commented"

echo "5. close (recovery path)"
gh issue close "$found" --comment "Recovered — smoke test complete." >/dev/null
echo "   closed"

echo "6. query again"
after=$(find_issue)
[ -z "$after" ] || {
  echo "   FAIL: #${after} still matches after close — a stale alert would"
  echo "   collect comments from unrelated future failures."
  exit 1
}
echo "   empty, as expected"

echo
# An if/else, not `A && B || C`: that chain parses as `(A && B) || C` and then
# any trailing `&& D` runs whenever the whole thing succeeded — which printed
# BOTH the success and the failure line on the first run of this script.
if gh issue delete "$ISSUE" --yes >/dev/null 2>&1; then
  echo "cleaned up #${ISSUE}"
else
  echo "could not delete #${ISSUE} (needs admin) — left closed, safe to ignore"
fi
ISSUE=""
trap - EXIT

echo
echo "ALL SIX LINKS PASS"
