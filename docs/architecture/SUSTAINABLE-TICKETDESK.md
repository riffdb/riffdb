# Sustainable TicketDesk milestone

This is the implementation handoff for the milestone selected on October 1,
2026. Its architecture records were **accepted** in session that day, and no local archive,
source-reclamation scheduler, provider retirement, or successful rehearsal is
claimed by this page. The evidence checker is preparatory acceptance code.

The target is 65,536 seeded tickets across 64 organizations, 100 commands/s
plus 400 named reads/s, at most 32 in flight, and recovery to the first
successful write within 60 seconds. Qualification uses an inventoried 8-vCPU,
32-GiB Linux host with durable SSD storage. Normal-window p95 is below 100 ms
and p99 below 250 ms. Existing stricter applicable gates remain effective.

## Decisions and dependencies

The maintainer accepted these exact records with "Accept all three as written":

- Storage lifecycle (`adr/0265-sustainable-ticketdesk-storage-lifecycle.md`):
  production local delivery, receipt reclamation, validated-prefix refresh,
  custody, and a separately identified 24-hour rehearsal.
- Provider retirement (`adr/0266-bounded-exact-provider-registration-retirement.md`):
  existing 256-slot caps plus a 1-GiB aggregate ownership budget, exact policy
  identities, protected epochs, and reclamation after final reference release.
- Ordering boundary (`adr/0267-ordering-boundary-for-the-sustainable-application-milestone.md`):
  preserve today's global sequence; partition locality does not discharge R-13.

| Package | Deliverable | Hard dependencies |
|---|---|---|
| WP-821 | Exact architecture acceptance and evidence contract | WP-814, WP-815, WP-817 |
| WP-822 | Durable local archive and production outbox worker | WP-821 |
| WP-823 | Receipt/extent reclamation and retention-aware recovery | WP-821, WP-822, WP-815 |
| WP-824 | Exact-provider retirement and byte accounting | WP-821 |
| WP-825 | TicketDesk workload and actual campaign runner | WP-822, WP-823, WP-824 |
| WP-826 | Real uninterrupted 24-hour qualification | WP-825 |

Run `./scripts/brief WP-821` and the corresponding downstream brief before
implementation. Architecture acceptance is recorded in the separate governance
commit `d6d5dd5b`. Package closure still requires its own validation and evidence;
acceptance alone does not establish implementation or campaign results.

## Findings carried into implementation

- `FileTestConnector` only calls its injected writer's `flush`; it cannot
  establish durable destination acceptance. The proposal reuses the existing
  redb engine for a separate archive instead of designing another append log.
- The source-control reclamation method is currently private, with retained
  observations exercised by fixtures. Production needs a closed owner that
  survives independent checkpoint observations without creating command-path
  drains or using reclamation's own receipt as perpetual progress.
- Online pruning invalidates validated-prefix evidence. Fresh validation must
  establish a new binding; retaining a deleted certificate is forbidden.
- Exact-provider keys include policy/capability revisions. Increasing slot
  ceilings or sharing similar-looking policies does not solve lifetime ownership.
- The existing alpha endurance validator requires its full domain/language
  matrix. A TicketDesk rehearsal cannot masquerade as that release campaign.

The ordering review covers two independent routes observed through one current
global token, authority/catalog changes spanning owners, rejection of a
cross-route write, recovery of a committed command whose reply was lost, and
fencing an old owner before promotion. No new token, route counter, quorum,
or distributed implementation is authorized by this milestone.

## Evidence checker

Run the hostile-evidence tests with:

```bash
./scripts/check-ticketdesk-lifecycle --self-test
```

The tests construct synthetic inputs only in memory or temporary directories.
They prove refusals and checker behavior, not actual delivery, reclamation,
recovery, resource use, or throughput. They are wired into `scripts/ci-all`.

The preparatory verifier accepts a frozen manifest and a receipt:

```bash
./scripts/check-ticketdesk-lifecycle \
  --manifest ./evidence/manifest.json \
  --manifest-sha256 "$FROZEN_MANIFEST_SHA256" \
  --receipt ./evidence/receipt.json
```

Freeze the manifest digest before the run; do not recalculate it to excuse
changed targets or artifacts. Both JSON files have a 4-MiB bound, closed
fields, exact schemas, duplicate-key rejection, and integer-only counters.
Artifact paths are relative to the receipt directory; traversal and symlink
components are refused, and each regular artifact is streamed with a 1-GiB
bound and checked against its frozen digest.

The receipt identifies itself as measured rehearsal evidence and has 288
contiguous five-minute windows. Planned faults and actual actions match;
five-minute outages have at most ten minutes to recover, and restart recovery
including the action itself has at most 60 seconds. The last six hours have no
fault interval and no positive eligible-history trend or endpoint growth. Physical
reclamation requires three distinct extent generations. Resource ceilings are
frozen separately per resource; archive growth cannot excuse process-memory
growth. Missing samples, unexplained errors, interference, unaccounted retries,
artifact drift, and an alpha classification refuse evidence.

This is an evidence-contract verifier, not an attestation service. Labels and
booleans cannot establish that measurements happened. WP-825 still owns the
real runner, independent state/resource reconciliation, trustworthy collection
of action and artifact identities, and tests of that producer. No receipt is
eligible for WP-826 closure until those producer checks and every prerequisite
have passed. No campaign receipt is included in this change.

## Current handoff

Package: WP-821; tier: guarantee. This preparation adds accepted decisions,
dependency-ordered packages, and internal evidence-validation tests. Existing
database/application behavior and formats have not changed. The handbook page
is linked for review and explicitly distinguishes planned behavior from implemented behavior.

Validation on October 1, 2026: all 20 evidence-checker tests pass, and
`./scripts/acceptance --wp WP-821 --range HEAD..HEAD` passes all 11 selected
checks, including formatting, handbook build/links, requirement coverage,
ADR obligations, index, acceptance-commit discipline, and allowed paths.
Every downstream package brief resolves. `bash -n scripts/ci-all`,
`git diff --check`, and the full `./scripts/ci-all` battery also pass. WP-821
is complete; WP-822 through WP-826 remain open. No real rehearsal has run.
Full alpha qualification, broad epoch reset, driver replacement, remote webhooks,
distribution, and additional micro-optimization remain outside this milestone.
