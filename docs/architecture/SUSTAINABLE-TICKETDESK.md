# Sustainable TicketDesk milestone

This is the implementation handoff for the milestone selected on October 1,
2026. Its architecture records were **accepted** in session that day. WP-822
adds the optional [durable local archive](../operations/LOCAL-OUTBOX-ARCHIVE.md)
and production dispatch worker; scoped acceptance and full CI pass. Source
receipt reclamation, provider retirement, and the real rehearsal remain open.
The evidence checker is preparatory acceptance code.

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
  establish durable destination acceptance. WP-822 adds a separate redb archive
  with Immediate two-phase publication and exact event deduplication. The
  fixture connector remains test infrastructure.
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
is complete. WP-822 is also complete as recorded below; WP-823 through WP-826
remain open. No real rehearsal has run.
Full alpha qualification, broad epoch reset, driver replacement, remote webhooks,
distribution, and additional micro-optimization remain outside this milestone.

## WP-822 implementation and PR note

Package: WP-822. Tier: guarantee, implementing accepted ADR-0265 decisions 2–7.

Behavior added: an optional primary-only durable local archive, protected
restart-applied configuration, and a server-owned recovered dispatcher. Real
compiled commands emit events that the worker archives with atomic encoded-byte
accounting. Equal uncertain retries preserve the original record; unequal bytes
are corruption. Known custody or quota refusal pauses before a claim, while
post-claim failure preserves existing bounded retry/dead-letter semantics.
Command acknowledgement does not wait for destination I/O. Shutdown cancels
between bounded recovery pages and joins the worker before writer close.

Compatibility: the new record has a sealed destination registry, a separate
Protobuf source, and a stable compatibility vector. Primary registry membership
and record schema hashes are unchanged. No-destination behavior, compiled writes,
authorization, idempotency, provenance, and database-wide ordering are preserved.
Followers reject destination configuration. Local inspection requires Linux and
exclusive engine custody; it exposes no payloads or application RPC/MCP method.

Checks run: `./scripts/ci-all` passes, including workspace tests, clippy,
documentation, dependency checks, four-language operational conformance,
generated fixtures, release source installation, and existing recovery gates.
`./scripts/acceptance --wp WP-822 --range HEAD..HEAD --no-tests` passes all 19
selected non-test checks; tests ran in full CI. The corrected exact schema
inventory test also passes independently. Process tests cover four destination
commit/publication cuts with repeated restart, source-completion uncertainty,
quota, permission recovery, corruption, real production delivery, original
command replay, and conflicting idempotency input. `git diff --check` passes.

Implementation choices: recover 256 rows/4 MiB per archive step; reserve a
3-MiB outer record payload bound and 8-MiB engine cache. Linux custody pins
directory/file identities and verifies effective-user ownership, private modes,
symlink refusal, and one hard link. Inspection reports incomplete until exact
end and caps each invocation at 256 pages. Generators own all package assets,
format/fixture digests, adapter templates, and CLI reference changes.

Updated handbook pages: [local archive operations](../operations/LOCAL-OUTBOX-ARCHIVE.md),
[configuration](../configuration.md), [domain events](../contracts/DOMAIN-EVENTS.md),
[CLI reference](../reference/CLI.md), and this milestone handoff; `SUMMARY.md`
links the new operations page.

Hazards and follow-ups: archive rows are intentionally retained, with no automatic
deletion; quota excludes separately measured engine allocation and cache memory.
Dead letters continue to fence source history. WP-823 owns repeated receipt and
physical extent reclamation plus retention-aware recovery. WP-824 owns provider
retirement, and WP-825/WP-826 own the real workload and uninterrupted rehearsal.

## WP-823 source-reclamation implementation and PR note

Package: WP-823, still open. Tier: guarantee, implementing accepted ADR-0265
decisions 8–10 and the receipt portion of decision 11.

Behavior added: the production retention lane retains one source-control owner
across checkpoint observations. Complete immutable history and source evidence
are validated in bounded pages outside the writer gate. Only exact-end proof
can request nonwaiting admission; queued writers retain precedence, and an
in-flight checkpoint, journal suffix, or changed publication makes it yield.
Pin capture also waits for extent sealing by yielding; it never joins that
worker while holding the writer gate.
All existing source and derived-reader holds remain binding. A later independent
durable checkpoint permits removal of at most 256 receipts. Reclamation and
prefix-refresh receipts cannot manufacture independent progress.

At an already drained boundary, admission retires the empty internal composite
publication cache using the existing operation. Captured readers retain their
pins. No journal flush is added. Receipt removal, logical command pruning, and
physical file retirement remain separate transitions, with receipt gate time
included in the existing maintenance pacing. The closed shutdown diagnostic
reports only removed receipts and gate microseconds.

Checks run so far: the 24-test source-control suite, page/exact-end proof test,
stale-hold proof refusal, four process-crash cuts with repeated reopen, numeric
diagnostic test, and production-lane extent integration test pass. The latter
drives actual checkpoints, proves captured readers prevent physical retirement,
and proves retirement after release while retaining idempotent outcomes and the
latest command. The first scoped run exposed an extent-sealing race in that
integration test. Admission now yields while sealing is unfinished, and the
fixture explicitly joins the actual sealer. The eight-test online regression
subset and corrected integration test pass. The full rerun passed 1,644 of
1,647 tests. The remaining failures exposed an outdated gate-time assertion,
a catalog/storage architecture-boundary violation in the next implementation
slice, and a vector-health test's assumption about background worker readiness.
The assertion now separates receipt time; storage retains only private completion
flags rather than catalog proof objects; the vector test explicitly observes
projection readiness before asserting aggregate vector health. Individual
regressions pass. The next scoped run passes all 1,655 storage/server tests and
all seven acceptance steps, including Clippy, handbook checks, file-size guards,
and panic allowances. Full repository CI is being run for this slice.
This is not a closure receipt.

The next slice rebuilds optional validated-prefix evidence through the production
maintenance lane. A sealed private candidate requires all three complete passes
on the scrub's original immutable pin. Planning runs off gate. Publication uses
the existing Immediate receipt owner, refuses changed history, unfinished sealing,
a journal suffix, queued writers, or cancellation, and never overwrites newer
evidence from a stale snapshot. Shutdown cancels at page boundaries and joins
the bounded-stack maintenance thread before writer close. The maintenance lane
yields between receipt-validation pages instead of imposing its idle recheck
interval on every page; writer occupancy still determines maintenance rest.
Seven private completion/publication tests cover exact ends, original-pin
preservation, cancellation, writer admission, stale-candidate refusal, and
one-time maintenance publication. Four process-kill edges preserve either the
original state or one complete receipted checkpoint across repeated reopen.
The composed extent test now also rebuilds the checkpoint after pruning,
restarts without a clean-close ceremony through bounded dirty validation,
and verifies the next durable command and retained original outcome.

Implementation choices: use 64-row scrub pages, a 384-KiB maintenance stack,
the existing sampled startup inventories, private completion flags at the
catalog/storage boundary, and the existing checkpoint encoding and Immediate
publication owner. Require unchanged complete history for this initial
candidate admission. Account pin capture and publication in maintenance pacing;
continue receipt pages between explicit yields when a pass remains incomplete.

Compatibility: durable record formats, command sequencing, authorization,
idempotency, resume anchors, and existing extent retirement machinery are
unchanged. Updated handbook pages: [configuration](../configuration.md) and
this milestone page, both already reachable from `SUMMARY.md`.
The [startup integrity](../operations/STARTUP-INTEGRITY.md) handbook page also
distinguishes a missing checkpoint from a completely validated replacement.

Hazards and follow-ups: validated-prefix refresh has initial production wiring;
recovery to a successful write after pruning remains to be proved. Busy-workload
progress, repeated
physical generations, and the campaign gates remain unqualified. A stale
candidate is discarded rather than allowed to authorize deletion. No measured
rehearsal or alpha qualification is claimed.
