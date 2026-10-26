# Weekly Literature

Standalone deterministic retrieval, strict clinical triage, checkpointed recovery,
and verified Notion delivery for colorectal surgery/cancer and clinical AI.
Python standard library only. No model API key or paid inference is required.
AI medical review remains in the existing separately managed assistant task.

This repository contains reusable code and synthetic tests, **not** a copy of
any private repository history, production database, medical-review payloads,
Notion export, account identifiers, or unpublished reports.

## Offline validation

```sh
PYTHONPATH=weekly-literature/src python -m unittest discover -s weekly-literature/tests -v
```

## Production configuration

Production is OFF until repository variable `ENABLE_PRODUCTION` is exactly `true`.
Set the following as repository Actions **secrets**, not source or public variables:

- `CLOUDFLARE_API_TOKEN`: least-privilege D1 API access for the existing account.
- `CLOUDFLARE_ACCOUNT_ID` and `LITERATURE_D1_ID`: existing state source, not a new database.
- `NOTION_TOKEN`: existing integration limited to its report and draft destinations.
- `NOTION_DATA_SOURCE_ID` and `NOTION_ARTICLE_DATA_SOURCE_ID`.
- `NCBI_EMAIL`: optional NCBI contact address.

Private-repository secrets cannot be read back or automatically inherited by a
new public repository. Provision them through authorized secure settings; never
paste them in an issue, PR, commit, workflow input, or chat message.

Only scheduled or explicit main-branch workflows can access production secrets.
Pull-request tests have no secrets or production access. Do not enable debug
logging, log uploads, Actions artifacts, shell tracing, unreviewed third-party
actions, or `pull_request_target` execution of untrusted code.

The public runner emits only allowlisted statuses. Exception details and internal
stdout/stderr are suppressed, including URLs and content returned by upstream
services. Synthetic offline test output is public. Public code and the timing
and success/failure of workflow runs are visible to everyone.

## Safe cutover

1. Obtain explicit approval to publish this exact reviewed file set in a new
   public repository. Never change the original repository's visibility.
2. Create a fresh repository (no private Git history), keep production OFF,
   provision authorized secrets, and pass offline tests.
3. Run **readiness** manually with production still OFF; this checks D1/Notion
   access read-only. Only this explicitly chosen read-only mode bypasses the
   production enable flag. It does not migrate or mutate D1 or Notion.
4. In the original repository, set `WEEKLY_LITERATURE_PAUSED=true` and drain
   all existing literature production/recovery/deploy jobs. Block manual old
   recovery entry points too. GitHub concurrency is repository-local: the same
   group name across repositories does NOT prevent simultaneous writes.
5. Enable the new production entry point. First run `scheduled`: it re-triages
   the oldest legacy READY queue before starting any new ETL, preserving saved
   analyses. This step prunes only unanalysed out-of-scope candidate links;
   metadata and source checkpoints are retained. Approve this operation before
   the first production run.
6. Verify through the private status API: strict policy, real selected/analyzed/
   pending progress, preserved analyses, source-specific coverage and windows.
7. Continue existing assistant analysis; never duplicate saved decisions.
8. Publish only after selected == analyzed. Verify the Notion readback and D1
   completion. Then declare migration complete; do not infer success from a
   green test or merged commit.

The schedules are Monday/Tuesday retrieval or recovery, and Friday publication
check. Manual dispatch can publish immediately after medical review completes.
GitHub scheduled starts can be delayed and are not an exact-time SLA.

## Rollback and free-tier boundaries

Disable `ENABLE_PRODUCTION` in this repository and drain its jobs BEFORE
reenabling the original repository's entry points. Reuse the same D1 history,
analyses, checkpoints and existing Notion pages; do not restore an old database
snapshot over new results. Leave the assistant automation unchanged.

Free standard GitHub-hosted runners for public repositories remain subject to
GitHub policies and platform limits. This does not remove Cloudflare D1 daily
quotas or upstream API limits. On D1 quota exhaustion stop without repeated
saves and continue from checkpoints after reset. No Queues, Workflows, R2,
paid model inference, billing changes, or automatically created cloud accounts.

