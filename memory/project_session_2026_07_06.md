# Session 2026-07-06 — Own the GitHub Pages deploy (transient-failure fix)

**Trigger**: GitHub notification — `pages build and deployment: Some jobs were
not successful` (run `28748294005`, following the 2026-07-05 16:00 collect).
Second occurrence of the same transient (first was 2026-07-04, run `28674750245`).

## What happened

1. **Diagnosed** the failure: `collect-and-publish` succeeded, the Pages *build*
   succeeded (artifact `github-pages` found), only `actions/deploy-pages@v5`
   failed with `##[error]Deployment failed, try again later.` — a GitHub-side
   transient, nothing in the repo.
2. **Key finding**: `gh run rerun --failed` on the legacy `pages-build-deployment`
   does **not** recover — a rerun re-attempts the same stuck deployment *version*
   (`6ee78df…`) and fails identically. Only a *fresh* deployment (new artifact)
   recovers. This is why the 07-04 rerun worked (a brief blip) but the 07-05 one
   did not (a stuck version).
3. **Fix** (`341fc99`), chosen by the user over a fragile auto-rerun watcher:
   - Switched Pages source `legacy` → `workflow` via
     `gh api -X PUT repos/ducroq/energydatahub/pages -f build_type=workflow`.
   - `collect-and-publish` now uploads `docs/` as the `github-pages` artifact
     (`configure-pages` + `upload-pages-artifact`, SHA-pinned).
   - New `deploy` job runs `actions/deploy-pages` with **3 attempts + 60s/120s
     backoff**; `environment.url` takes whichever attempt wins.
   - Workflow-level `concurrency: pages` (cancel-in-progress: false) serializes
     deploys.
4. **Consequence**: the auto `pages-build-deployment` workflow no longer runs.
   Pages is deployed only by the `deploy` job inside the daily collect run —
   future Pages debugging looks there, not at a separate workflow.
5. **Verified** end-to-end via manual dispatch (run `28777188653`): both jobs
   green, `deploy` succeeded on attempt 1, `build_type: workflow / status: built`,
   site HTTP 200.
6. **Docs synced**: `CLAUDE.md` (Before-You-Start CI row, Architecture comment,
   Key Paths pipeline line), `docs/CI_CD_SETUP.md` (rewrote the Continuous
   Deployment section). Gotcha-log 2026-07-04 entry marked `[RESOLVED 2026-07-06]`.

## Lesson

A failed deploy is not always retryable *in place*. When a deploy step targets a
versioned deployment (GitHub Pages, and likely other "create deployment then poll
status" APIs), re-running the same job re-attempts the same stuck version. Recovery
requires producing a *fresh* deployment. Owning the deploy in-workflow (vs. relying
on a provider's auto-injected workflow) is what makes bounded retry possible at all.

Same diagnostic shape as the recurring "downstream says publish is broken — inspect
which *stage* actually failed" lesson: collect → build → deploy are three stages;
only the last was ever at fault here.
