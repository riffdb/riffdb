# Sustainable TicketDesk milestone

This is the implementation handoff for the milestone selected on October 1,
2026. Its architecture records were **accepted** in session that day. WP-822
adds the optional [durable local archive](../operations/LOCAL-OUTBOX-ARCHIVE.md)
and production dispatch worker; scoped acceptance and full CI pass. Source
receipt reclamation and exact-provider retirement are implemented and validated.
WP-823 and WP-824 are complete; the real workload and rehearsal remain open.
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

The latest 1,500-second preparation meets sink catch-up but fails normal-window
latency and finishes some requests after the load deadline. A stopped-source
private copy passes complete archive reconciliation, then fails to reach
readiness within 60 seconds after pruning. Prefix-refresh setup is repaired
to release the ordered writer before population work while preserving exact
binding and stale-candidate refusal. These native blockers still require
measured resolution; component tests do not establish rehearsal readiness.
See the [actual observations](WP-825-LIVE-OBSERVATIONS.md#extended-preparation-and-populated-recovery-blockers).

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

### Native observation preparation

WP-825 adds a persistent Rust `lifecycle native-observer` worker that uses the
existing verified TLS client and an operator credential. Its closed stdin
protocol accepts only `sample 1`, `sample 2`, or `stop`, each followed by a
newline. A sample performs one health, statistics and one-row projection call
sequentially in one auxiliary slot, with no retries and a five-second total
deadline. Output contains numeric measurements and the exact projection plan
identity, with no event payloads, credentials or process identifiers. The worker
refuses missing counters, changed incarnations, unavailable projections and
substituted plan/frontier identity. It remains preparation code until integrated
and measured with the final campaign controller.

The production statistics provider previously always omitted delivery counts.
It now reads the cardinality of the existing validated, commit-coupled
undelivered index on its bounded read worker. Claims, retries and dead letters
remain counted; delivery completion removes membership. Unbuilt or degraded
membership remains unavailable. This adds no durable record, population walk,
writer, deletion authority or authorization exception. See
[live delivery counts](../operations/LOCAL-OUTBOX-ARCHIVE.md#live-delivery-counts).

The owned-process collector verifies all thread child lists against an exact
tree of named persistent owners. It refuses untracked children, replacement,
disconnected ownership and missing memory measurements. It records current
RSS/address space and per-incarnation lifetime high-water marks; the sum of
lifetime peaks is a conservative upper bound, not a simultaneous measured peak.
Transient descendants between censuses still require launch/exit accounting.
These collector tests do not establish the campaign resource-model check.

Package: WP-825, open. Tier: guarantee under accepted ADR-0265 decision 11.
Behavior: native delivery-count observation and preparatory public-client/process
collection. Compatibility: existing statistics fields and capability checks;
durable formats and command ordering are unchanged. Checks: 88 lifecycle
checker tests, native mixed-state/reopen and completion tests, a deterministic
durable-but-unpublished event schedule, and scoped Clippy pass. Final source
acceptance, full CI and actual observer host integration remain required; no
qualifying rehearsal is claimed.
Updated handbook pages: this handoff and local archive operations. Follow-ups:
integrate the observer, inventory transient lifecycle processes, measure eligible
history bytes, and complete all seven fault paths with six final fault-free hours.

## Persistent native observation in population preparation

`ticketdesk-lifecycle-smoke --native-observations` selects the persistent Rust
health/statistics/projection sampler for an observed population preparation.
It requires `--observe` and a supported positive `--population-probe-seconds`.
The preparation operator receives the existing `read_statistics` capability;
application credentials and public authorization behavior are unchanged.

One reserved auxiliary slot issues three sequential public RPCs per successful
sample, with no retry. The controller checks the exact projection identity,
contract selector, sample sequence, history incarnation and monotonic frontiers.
Missing output, a changed identity, partial RPC failure or expired child shutdown
refuses the preparation. Samples retain actual delivery counts and projection
lag. The existing five-second cadence and every-window coverage check remain.

Memory collection includes the native sampler in a verified five-process
persistent tree and records lifetime peak upper bounds. It still sets
`qualifies_resource_model` to false: transient CLI launch/exit accounting,
eligible-history bytes, fault integration and final-source host validation
remain required. This preparation does not establish a qualifying campaign.

Package: WP-825, open. Tier: internal. Behavior: connect the native sampler to
population preparation while preserving load, latency and refusal gates.
Checks: closed observation/continuity tests, actual child ownership and shutdown
tests, and all 98 lifecycle checker tests. Compatibility: no native interface,
durable format, application permission or fixture changes. Updated handbook:
this page. Scoped acceptance: all five checks pass. The first local population
preparation failed before sampler startup: the event observer refused a consumer
acknowledgement during seeding, and cleanup exceeded its shutdown bounds.
That failed run is diagnostic evidence only; its unoptimized binaries and local
host do not qualify performance. A separate small public TLS smoke successfully
used the real native sampler for health, statistics and the compiled projection,
then joined it normally; this does not prove population-load sampling.
Hazards/follow-ups: identify the acknowledgement
result, measure real public RPCs and the five-process census together, then run
final-source full CI and required-host preparation.

## Closed acknowledgement refusal diagnostics

The event observer preserves the distinct public mutation result in its closed
failure code: changed state, missing consumer, outstanding lease, stale lease,
or expired lease. Only an applied acknowledgement permits continued observation.
It does not retry, lengthen leases or accept a refused acknowledgement as success.
No event identity, lease token, payload or credential enters these diagnostics.

Package: WP-825, open. Tier: internal. Checks: the generic-cause regression fails
before the correction; all 28 lifecycle-worker tests pass. Compatibility: public
RPCs, lease timing, durable formats, credentials and workload gates are unchanged.
Updated handbook: this page. Follow-up: use the actual result to diagnose the
failed population seed; it is not an established root cause or native repair.

## Driver startup boundary in full CI

The frozen candidate passed all 20 scoped checks, including 2,862 native tests,
but full CI failed in the nested retained TypeScript smoke. Its owned temporary
root produced a 110-byte socket pathname; Linux accepts at most 107 bytes.
The driver refused startup, and the cleanup exception replaced that primary
failure with `driver_shutdown`. Both required-host launch guards refused; no
host workload or 24-hour run started.

Application and operator drivers now preflight addresses through the standard
library's platform Unix socket address constructor, before artifact reads,
remote connections or filesystem mutation. Typed refusals and private-directory
ownership checks remain. Preparation uses a shorter owned temporary directory
name and retains the primary failure while separately recording failed cleanup.
A successful smoke still fails if cleanup refuses or exceeds its deadline.

Package: WP-825, open. Tier: surface; implementation repair under ADR-0106,
existing safe configuration boundaries and D-003. Checks: native path-limit
regressions fail before the repair; all 44 native driver tests, cleanup refusal
tests, all 100 lifecycle checker tests and nine scoped acceptance checks pass. Compatibility: no dependency, protocol, durable format,
credential or fixture change. Updated handbook: this page and Driver Host.
Follow-ups: repeat actual nested four-language preparation, then complete
final-source full CI and required-host validation.



## Actual logical-history byte observations

The native online lane can emit `riffdb-retention-logical-bytes-v1` after
finishing the complete eligible prefix of one original composite snapshot.
It uses the native retained-sequence target and fencing inputs, checks actual
delivery in that snapshot, and stops before protected or unsealed whole rows.
A 256-row scan page is incomplete evidence until the original eligible end is
known; it is never reported as the total backlog.

The value count covers resolved canonical command rows, their prunable event,
outbox and status rows, and obsolete command locators. The key count covers
removed keys only. Retained outcomes, provenance, audit, request indexes and
event routes stay in their separate resource model, including compatible
rewrites of retained full records. The accounting adapter executes the existing
native retained-view validation against private bounded staging; it changes no
database row and confers no deletion authority.

Capture occurs off the writer gate at most once per 60 seconds. Each page
visits at most 256 command rows, with the existing journal-frame byte and
collection bounds on per-row related work and staging. A captured pin expires
at 30 seconds, and completion, refusal and joined worker retirement release it.
Incomplete, expired or failed measurements remain unknown. A closed
`riffdb-retention-logical-bytes-refusal-v1` diagnostic invalidates accounting
without replacing or changing the native pruning plan.

This is a separate diagnostic component. It does not fill
`eligible_history_bytes`, qualify the aggregate resource model, or establish
physical retirement. Unit fixtures cover canonical byte sizes, retained views,
the original prefix beyond one deletion batch, operator holds, actual delivery,
target boundaries, corruption, workspace limits, overflow and pin lifetime.
Required follow-ups are collector integration, comparison with real native
prune and extent retirement, and measurement pressure on the required host.

Package: WP-825, open. Tier: guarantee, implementation under ADR-0265 and D-003.
Compatibility: no durable encoding, application interface, authorization,
command ordering, dependency or reviewed compatibility-fixture change.
Updated handbook: this page. All 10 focused native accounting tests pass;
full scoped checks and final-source host evidence remain required. This
component authorizes no 24-hour qualification.

## Separate logical-byte collection during observed load

Observed online-retention load with native observations also collects
`riffdb-retention-logical-bytes-v1` into private logical-byte samples and an
observation summary. It reuses the bounded daemon-log reader, checks the closed
numeric record and native 256-command whole-row bound, and binds samples to the
independently observed incarnation. Protected rows may lower the eligible end;
the original durable head and watermark cannot rewind. Unknown, partial,
expired, malformed or refused observations fail the preparation instead of
becoming zero. Each five-minute window needs complete observations, with the
same frozen 95-second maximum gap as source accounting.

Logical observations preserve canonical value bytes and removed-key bytes
separately. They do not substitute for source bytes or physical allocated extent
bytes, fill `eligible_history_bytes`, or qualify the aggregate resource model.
The native logical producer must accompany this collector before host use.

Package: WP-825, open. Tier: internal implementation and acceptance under
ADR-0265. Compatibility: no public interface, durable encoding, authorization,
dependency or reviewed compatibility-fixture change. Updated handbook: this
page. Six focused collector tests pass; native and required-host evidence remain
required. This component authorizes no 24-hour qualification.
## Linux RSS counter observations

A preparation failed closed at resource collection. A bounded allocation-only
host reproducer, with no database workload, also reached the collector's
high-water monotonicity refusal. Linux's RSS accounting is asynchronous; its
reported high-water counter can decrease between reads. The collector now
preserves each raw reported reading and the maximum already observed for the
same live process incarnation. Both bracketing tree passes retain their raw
values. The observation explicitly labels the asynchronous RSS basis.

Virtual-address high water still cannot rewind. Missing, malformed, oversized,
inconsistent, replaced-process or unowned-child measurements still refuse.
Neither the workload nor any latency, recovery, memory or coverage limit changes.
Reported RSS counters remain approximate; this repair does not establish precise
RSS, complete transient launch/exit coverage, or campaign resource qualification.
See the [Linux process accounting documentation](https://docs.kernel.org/filesystems/proc.html).

Package: WP-825, open. Tier: internal. Compatibility: no database semantics,
public API, durable format, authorization, dependency or reviewed compatibility
fixture change. Updated handbook: this page. Preserve the failed preparation;
repeat unchanged-host preparation after the repair's checks, and investigate any
remaining failure rather than assuming the original refusal had this cause.

## Actual source-receipt byte observations

The online source owner can report `riffdb-retention-source-bytes-v1` after
completely scanning the actual uncapped eligible prefix from its validated
barrier. It counts resolved canonical receipt bytes and actual key bytes
separately. A sequence gap, padded journal frame length or the next 256-row
reclamation transaction is never substituted for the complete byte count.

Capture retains the original registered checkpoint root and native ceiling,
including the independently durable checkpoint and existing derived/durable
holds. Every advance scans at most 256 rows within the existing changelog frame
byte ceiling, outside the writer gate. Collection performs no mutation, journal
drain, new reader capture or extra writer admission. It leaves source and logical
reclamation decisions intact and never confers deletion authority.

Capture is limited to once per 60 seconds. Collection checks a 30-second
freshness deadline before each bounded page and again before publication;
expired or incomplete work emits no successful byte measurement. Completion,
expiry, failed decoding, owner retirement and switching to offline source control
release the registered reader pin. Offline source control does not advance an
online observation, so it must retire that observation before acquiring its barrier.
An in-progress collection can delay physical reclamation, so the final campaign
must account for this pressure. Storage I/O uses the existing bounded page
operations; the deadline does not cancel an I/O operation already in progress.

Output includes original root, incarnation, resume and ceiling, exact row count,
canonical receipt and key bytes, capture age, collection time and page count.
This is one component of history accounting. It does not qualify total eligible
history: logical command/event/outbox rows and physically eligible extents still
need independent native measurements, with protected resources counted separately.

Package: WP-825, open. Tier: internal; implementation diagnostics under accepted
ADR-0265 decisions 8–11 and D-003, without a durable identity, application surface
or guarantee change. Checks: unknown-prefix regression fails before implementation;
the initial six source-byte tests and all 45 selected source-control/extent
regressions pass. That revision passes all seven scoped acceptance checks,
including 1,245 tests across eight binaries with two existing skips. A subsequent
offline-switch regression reproduces a retained metric pin; the repaired switch
passes all 15 source-control tests, including all seven byte-observation tests,
and all six final scoped non-test checks, including all-target Clippy and the
handbook check.
Updated handbook: this page.
Follow-ups: complete aggregate native byte accounting, observe actual collection
pressure and run final-source full CI.

A separate local release-mode population diagnostic seeded 65,536 tickets,
completed 500 updates and 2,000 reads without retry or late work, reconciled all
tickets, and sampled real public RPCs with a verified five-process memory census.
It failed the existing latency gate: p95 was 105.306 ms, above 100 ms; p99 was
157.170 ms. Archive reconciliation was not reached after that refusal. Its
source-receipt byte collector was absent. This failed five-second diagnostic on
the local host is not required-host or campaign performance qualification.

## Source-byte evidence during observed preparation

Preparations with online retention and native observations now collect the
native `riffdb-retention-source-bytes-v1` records after the load-ready boundary.
They preserve canonical receipt and key bytes separately, original root and
uncapped range, row count, collection time and capture age. The independently
sampled public history incarnation must match. Seed-time lines cannot fill load
coverage; a partial line stays pending, and replaced or oversized logs refuse.

Every five-minute window requires a complete measurement. Collection refuses
missing observations and gaps above 95 seconds: the native 60-second capture
cadence plus strictly less than 30 seconds to complete, with five seconds for
collector scheduling. This is a new component bound; existing provider and
projection coverage limits remain intact. Native stale or incomplete scans
cannot be reported as zero, and unknown diagnostic versions refuse.

This component never fills `eligible_history_bytes` or qualifies the complete
resource model. Logical command/event/outbox history, physical extent
eligibility, process lifetime accounting and real collection pressure remain
required. Collection adds no RPC or sampling thread and reuses the bounded
diagnostic log reader. It does not change native reclamation or admission.

Package: WP-825, open. Tier: internal. Decisions: component evidence under
ADR-0265 and D-003, with no new database surface or guarantee. Checks: collector
regression fails before implementation; all 106 lifecycle checker tests and all
five scoped acceptance checks pass. A real native record from the source-control
regression also parses with its original receipt and key counts. Updated handbook:
this page. Follow-ups: integrated final-source CI, real observed preparation and
complete campaign accounting. Synthetic checker tests are not host qualification.


## Concurrent outbox health refreshes

A deterministic native regression reproduces overlapping dispatcher/commit
refreshes: an older empty snapshot finishes after a newer nonempty snapshot
and incorrectly clears degraded status. Health is a cached diagnostic; a
separate health/statistics sample crossing new commits does not itself prove
this race or failed sink catch-up.

The native cache now gives each refresh an ephemeral generation and marks its
in-progress result degraded. Completion uses an exact-generation compare and
exchange, so an older result cannot overwrite a newer snapshot. Generation
exhaustion remains permanently degraded and cannot wrap or be cleared by the
last delayed observation. Destination readiness stays independently checked.
No new lock spans repository work; unknown reads still become degraded.

Package: WP-825, open. Tier: internal. Behavior: preserve conservative native
health under overlapping refreshes. Decisions: implementation repair under
ADR-0265 and D-003; no delivery, command ordering, authoritative mutation,
durable format, authorization or RPC schema change. Checks: explicit-channel
schedule fails on the original implementation; all six native cache tests,
all 647 server tests across 17 binaries with eight existing skips, and all seven
scoped acceptance checks pass. Updated handbook: this page. Follow-ups: validate
the integrated source in full CI and required-host preparation.

The socket-preflight candidate separately passed the complete actual lifecycle
preparation on its clean revision: nested retained TypeScript, Go and Python,
Rust workload, additive deployment, graceful restart, process kill, archive
reconciliation and capability rotation. That repair removes the confirmed
110-byte-path blocker; it does not turn the earlier full CI failures into passes.
The source-byte producer, collector and health-cache repair are now integrated
for final-source validation. Full CI and required-host qualification remain pending.


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
can request the bounded FIFO source turn accepted in ADR-0271; earlier
writers retain precedence. One fixed request expires after 100 ms, and graph
retirement cancels pending eligibility. An in-flight checkpoint or extent
sealer makes it yield. A complete acknowledged suffix may be drained through
the unchanged native protocol only with exact accounting of at most 256
transitions and 1 MiB; unknown or larger work yields before materialization.
Complete current qualification and every hold are rechecked before deletion.
Pin capture also waits for extent sealing by yielding; it never joins that
worker while holding the writer gate.
All existing source and derived-reader holds remain binding. A later independent
durable checkpoint permits removal of at most 256 receipts. Reclamation and
prefix-refresh receipts cannot manufacture independent progress.

At an already drained boundary, admission retires the empty internal composite
publication cache using the existing operation. Captured readers retain their
pins. No journal flush is added. Receipt removal, logical command pruning, and
physical file retirement remain separate transitions, with receipt gate time
included in the existing maintenance pacing. Initial capture starts validation
only; the owner rests before requesting a subsequent source turn. The closed
shutdown diagnostics separate queue waiting from full actual source residence
and report only numeric admission, expiry, cancellation and drain-yield counts.
Admission changes scheduling, not deletion authority or any durable boundary;
required-host verification remains outstanding.

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
and panic allowances. Full repository CI also passes for the committed slice,
including workspace tests, Clippy, dependency checks, handbook and generated
references, four-language operator conformance, existing recovery gates, and
release source installation. The implementation commit is `dbe974bd`.
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

## WP-823 population recovery measurement

The explicit storage-fixture test
`population_65536_first_successful_write_after_online_pruning_meets_60_seconds`
seeds 65,536 distinct commands in atomic groups of 64, prunes through the online
lane and restarts without refreshing prefix evidence or certifying a clean
close. It measures the next successful durable command, then checks that the
original first command's idempotent outcome survives. The final group is
retained whole when the requested 16-sequence floor intersects it: 65,472
sequences are pruned, rather than the incorrect initial expectation of 65,520.

The release-profile run on the local development host measured readiness at
24,549,339 microseconds and the first successful write at 24,552,101 microseconds,
below the unchanged 60-second target. It passed all assertions. Run explicitly:

```bash
cargo test --release -p riffdb-storage-redb --all-features \
  --test storage_recovery_matrix \
  population_65536_first_successful_write_after_online_pruning_meets_60_seconds \
  -- --ignored --nocapture
```

This is a storage recovery measurement, not TicketDesk or host qualification.
The fixture marks its outbox events delivered while stopped; production delivery
is covered separately by WP-822. The local machine has 32 CPUs, rather than the
frozen 8-vCPU rehearsal shape. A debug-profile attempt was stopped after its
structural audit validation exceeded the target; it remains a failed attempt,
not release performance evidence. The separate rehearsal host must measure the
real public workload and all required fault paths with its frozen artifacts.

PR note: Package WP-823 (open); Tier guarantee; Behavior: explicit population-
sized recovery measurement, including deferred validation and the next durable
write. Checks: the release measurement, formatting, scoped Clippy and allowed
paths pass. Compatibility: no production behavior, formats or interfaces change.
Updated handbook page: this milestone handoff. Hazards/follow-ups: full CI,
the integrated retention process-kill fixture and the real rehearsal remain open.

The integrated process-kill fixture now offers at most 32,768 public commands
and checks at most 64 directory entries per 64-command batch for an actual
sealed checkpoint extent. Its former 40-command seed stayed below the sealing
threshold and correctly remained ineligible for pruning. No forced checkpoint,
offline prune or production threshold change is used. The complete real-daemon
recovery matrix passes in release mode, including online pruning, a genuine
process kill, retained original-outcome replay and resumed durable writes.
Both recovery scripts select the shipping release profile; their existing
readiness and RPC deadlines remain unchanged.

Release execution also identified a race in the uncertain-replay fixture: it
expected its first projection request to be Ready even while the derived
generation was Building. The fixture now observes typed Building/Rebuilding
responses until the exact required frontier arrives within its existing
20-second deadline; failure and invalid responses still refuse the test.
All six scoped static checks pass, including the handbook and named artifacts.
The debug readiness failure and the first release projection assertion remain
failed attempts. Full CI and qualification remain open.

D003 decisions: seed through bounded public commands until ordinary checkpoint
publication, run the real-process matrix in the shipping profile, and synchronize
projection checks through typed frontier observations rather than execution speed.
Updated handbook pages: this milestone handoff and contributing instructions.

## WP-823 closure, October 4

WP-823 is complete. Earlier progress sections retain the failure history and
are snapshots, rather than the current package status. The fifth full CI run
passes at implementation revision `1bf6920d2`, with four concurrent tests and
six build jobs. The full release recovery matrix separately passes all ten
cases, including genuine process kill after online pruning. The population
measurement reaches the first durable write in 24.55 seconds. All nine scoped
acceptance checks pass for this closure's documentation and governance changes.

PR note: Package WP-823; Tier guarantee; Behavior: composed bounded receipt
reclamation and complete retained-prefix refresh; Checks: full CI, shipping
recovery matrix, population recovery measurement, source hold and physical
extent tests; Compatibility: unchanged ordering, durability, formats,
idempotency, authorization and application interfaces. Updated handbook pages:
configuration, startup integrity and this milestone handoff. Hazards and
follow-ups: the storage fixture's stopped outbox preparation is not production
qualification; WP-825 and WP-826 must demonstrate the real application and
repeated physical generations on the frozen host. No 24-hour run has started.

## WP-824 closure, October 4

WP-824 is complete; its [implementation record](WP-824-IMPLEMENTATION.md)
contains the accounting model, semantic tests, failure history and closure note.
Full CI passes at the same revision recorded for WP-823. Current authority,
minimum retention and active pins remain binding during retirement. A retired
historical module remains callable through ordinary fresh-owner admission.
The aggregate budget refuses protected saturation before allocating uncharged
workspace. WP-825 can now implement the actual campaign against completed
runtime prerequisites.

## WP-825 preparation progress

WP-825 is open. The separate `TicketDeskLifecycle` example lineage preserves
existing TicketDesk generated identities and the alpha endurance workload.
It adds an expected-revision update without creating tickets or comments,
emits `TicketUpdated`, declares a counting projection, and generates all four
language bindings plus MCP artifacts. The compiler rejects removal of the
caller fence at both replacement-write spans. Generation checks exercise the
real compiler and exact lock, rather than inspecting command names alone.

Build the preparation client and run its public TLS smoke:

```bash
cargo build -p riffdb-server --bin riffdbd -p riffdb-cli --bin riffdb \
  -p riffdb-ticketdesk --bin lifecycle
./scripts/ticketdesk-lifecycle-smoke \
  --root /tmp/riffdb-lifecycle-preparation \
  --server target/debug/riffdbd --cli target/debug/riffdb \
  --worker target/debug/lifecycle
```

The smoke uses generated commands, named reads, event acknowledgements and a
live-query snapshot. The real local run passes replay, stale-fence refusal,
returned-state, partition and event checks, followed by production signal
shutdown. The first harness attempt incorrectly combined a legacy projection
flag with named configuration and was refused. A second attempt passed client
checks but failed shutdown because closing stdin does not stop the TLS daemon.
The corrected harness selects a named owned root and sends the production
termination signal. `scripts/check-ticketdesk-lifecycle-preparation` builds the
four binaries and runs this real smoke in full CI. Failed attempts remain failed; no runtime check is relaxed.

The evidence checker has an explicit `--short-rehearsal` mode for WP-825. The
frozen target remains 24 hours and ordinary validation still requires all 288
windows. Short mode accepts fewer windows but preserves every other check,
including six final fault-free hours, all seven faults, three physically
reclaimed generations, service levels, resource ceilings and artifact binding.
Its success message explicitly says the 24-hour duration remains unmet. All 24
hostile-evidence tests pass; their synthetic fixtures are not measurements.

PR note: Package WP-825 (open); Tier guarantee; Behavior: generated lifecycle
application, bounded public preparation, and the accepted short-rehearsal
exception; Checks: all five scoped acceptance steps, Clippy, all 32 generators,
24 hostile-evidence tests, two compiler semantic tests, and the real CI smoke
entrypoint pass; requirement coverage and ADR obligations pass. Compatibility: existing application closure, alpha gate and normal
24-hour verifier remain intact. Updated handbook pages: this milestone handoff
and TicketDesk acceptance. Hazards/follow-ups: sustained offered-load scheduling,
queried projection observations, TypeScript smoke, lifecycle faults and
independent campaign reconciliation still require implementation and a real
host run before WP-825 closes. No 24-hour run has started.

## WP-825 population probe and independent oracle

Population preparation now records actual public native projection observations
during offered load rather than assuming zero backlog. One sequential auxiliary
lane samples health, then queries the selected projection's available frontier
without an after-sequence wait. Private bounded samples bind the exact lineage,
projection ID and plan hash and report the nonnegative difference between the
earlier sampled application head and the observed applied frontier. This is
sampled sequence lag, not simultaneous head or eligible-history byte evidence.
Every offered-load window needs a completed observation; unavailable, missing,
inconsistent or regressing evidence refuses success. Independent final aggregate
reconciliation is still required. The 50 evidence/preparation tests include
positive lag, missing-window refusal, selected-plan substitution, numeric bounds,
failure propagation and barrier-controlled observer shutdown. Real host
measurement of this sampler remains pending; this change qualifies no campaign.

The release preparation smoke also passes on the inventoried eight-vCPU host.
This establishes the real generated command/read/event/live-query carriage
there; it is not a campaign or a physical-reclamation result.

The Rust client now has a separate offered-load `probe` and full-population
`reconcile` mode. It assigns deterministic original update identities across
all 65,536 tickets, with explicit expected revisions. The independent oracle
rejects stale, missing, substituted, unoffered and wrong-partition results;
final reconciliation reads every ticket through named queries. Its baseline
without validation failed both rejection tests before implementation.

The load lane offers 100 commands and 400 named reads in each one-second batch,
with at most 28 admitted requests and four reserved auxiliary/control slots.
Its queue has 32,768 fixed slots; latency histograms are bounded. No production
concurrency, writer, durability or retention setting is changed. Latency includes
queueing from the offered time. Measurements close at the observation deadline;
later completions are separately disclosed. Submission counts cover settled
load command/named-read submissions; SDK same-key outcome lookups are auxiliary
reconciliation, rather than another command submission. This preparation uses
one submission attempt per SDK call and fails on transport errors or unexpected
replay; fault-aware original-identity recovery remains campaign work.

Six tests cover oracle substitutions, the final partial population round,
protected saturation, cancellation release and latency overflow. They and
Clippy pass. The updated real TLS smoke also passes. A new preparation option,
`--population-probe-seconds 300`, seeds the full population, runs a five-minute
load probe and reconciles every row. It must pass on real observations before
any rates are claimed. No population load result is claimed by this change.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded load
preparation and independent generated-query reconciliation; Compatibility:
unchanged existing application identities, interfaces and qualification gates;
Updated handbook pages: this milestone handoff; Checks: six semantic/schedule
cases, Clippy and the real TLS preparation; Hazards/follow-ups: run the host
population probe, then complete active observations, faults and the qualifying
short rehearsal. The 24-hour run remains unstarted.

## WP-825 retained TypeScript smoke and host load preparation

The inventoried durable-SSD host completed the fresh five-minute population
probe at source revision `50ade9e2`: all 30,000 updates and 120,000 named reads
completed within the window, with zero retries, p95 73.830 ms and p99 84.490 ms.
The peak load concurrency was 28, and no completions were added after the
window. All 65,536 ticket rows reconciled afterward. Latency overflow remained
visible (392 samples above 250 ms); it was included in the quantiles. This
result covers offered load and state reconciliation, without lifecycle faults,
active campaign observers or physical reclamation qualification.

The preparation gate now also exercises retained generated TypeScript bindings
through the native Rust driver over verified TLS. It checks a named read,
wrong-partition refusal, a stale expected-revision command and its exact replay,
unchanged state, and a live-query snapshot. This smoke creates no durable
consumer that could hold later campaign history. Its build uses the existing
pinned runtime dependencies and stages compiled output only in the private
preparation root. The integrated real preparation gate passes.

PR note: Package WP-825 (open); Tier guarantee; Behavior: retained TypeScript
semantic smoke and observed full-population load preparation; Checks: real
Rust/TypeScript TLS preparation and host population reconciliation; Compatibility:
no database or public-interface changes; Updated handbook pages: this handoff;
Hazards/follow-ups: active observers, all seven faults, resource accounting,
archive/restore reconciliation, a frozen campaign and its short rehearsal remain
required. These observations do not qualify WP-825. No 24-hour run has started.


## WP-825 active observation preparation

The public TLS preparation gate now registers a generated event consumer and
live query before any seed command, waiting for an explicit ready message.
Two auxiliary slots cover the serial consumer calls and live stream. Its
bounded organization-zero oracle stores at most 1,024 latest ticket events,
checks creation/update fields and contiguous revisions, distinguishes equal
redelivery from substitution, and rejects gaps and partition mistakes. It
acknowledges through generated methods and checks native provenance metadata,
consumer leases/retries, incarnation and monotonic consumer/live frontiers.
Unplanned resets and lifecycle interruptions currently fail this preparation;
reconnect, rotation and restore handling remain campaign work.

The preparation queries `TicketUpdates` through the public operator CLI. A
first-party compiler descriptor checks the exact generated contract bundle hash
and supplies the projection ID and plan hash. Reconciliation requires that
identity, the organization-zero group, expected independent count and a frontier
at least as new as the sampled committed head. Native canonical capability
permission ordering is preserved. No runtime or application interface changes.

The real Rust/TypeScript TLS preparation with both observers and the queried
projection passes. Eight Rust semantic/schedule tests, two hostile projection
oracle tests and Clippy pass. The no-validation event and projection baselines
failed their rejection assertions before implementation. They are intentionally
failing tests, not database daemon panics. Raw failed-attempt logs remain private.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded public event,
live-query and projection preparation observations; Checks: eight Rust tests,
two projection oracle tests, Clippy, integrated real TLS preparation and scoped
acceptance; Compatibility: unchanged native semantics and qualification gates;
Updated handbook pages: this handoff; Hazards/follow-ups: repeat full-population
load with observers on the durable-SSD host, complete actual faults, freeze the
campaign, reconcile archives/restores and run the required short rehearsal.
The 24-hour run remains unstarted.


## WP-825 observer scheduling and exact-parent successor

The first full-population attempt with observers ended with a terminal live
response while its serialized acknowledgement loop left the live head far
behind the committed head. Its failure
and logs remain preserved; it is not a passing observation result. Live-stream
draining now has a separate task within the same two auxiliary slots. A
deterministic bounded-channel schedule holds consumer acknowledgement while
both live updates advance, then stops the reader explicitly. Another test
rejects incarnation drift and decreasing frontiers without publishing them.

The additive fixture is an exact-parent V2 closure. Its extra compiled
`LifecycleVersion` command leaves all parent entities and command identities
intact. The public TLS application-lock workflow generates all bindings against
the active V1 parent; the independent compiler test checks the retained identity
and exact generated V2 bundle hash. Retired V1 bindings remain refused after
activation; preparation binds the current V2 role. Rust and TypeScript public
V2 smoke operations pass, including a positive fenced update, original-key
replay, unchanged stale refusal, named state and partition reconciliation.
The one-ticket preparation stops the old observer before activation. Continuous
observer handover and queued offered work remain actual campaign requirements.

PR note: Package WP-825 (open); Tier guarantee; Behavior: independent live
reader and exact-parent V2 preparation; Checks: eleven Rust semantic/schedule
cases, generated V2 closure, real two-version Rust/TypeScript TLS preparation,
Clippy, all 33 generators, three compiler/schema checks, current-role denial of
the added command, and scoped acceptance; Compatibility: native version/policy refusal and
qualification gates preserved; Updated handbook pages: this handoff;
Hazards/follow-ups: repeat the corrected observer population probe on the host,
complete the fault controller and independent campaign reconciliation, freeze
artifacts and run the required short rehearsal. No 24-hour run has started.


## WP-825 seeding and live-watch admission

The revised host attempt also reached native `buffer_pressure` while seeding;
its independent reader did not remove that source-side notification backlog.
Both failed observer attempts remain failed. The service retains its bounded,
fail-closed watch behavior. Population preparation now registers the durable
consumer before seeding, then starts the live query against the completed seed
before any offered-load window begins. An explicit file gate and observed live
progress coordinate that transition; the initial view must contain exactly the
expected top fifty seeded tickets with correct partition, identities and state.
The one-ticket CI preparation continues to cover a watch active during writes.

The observer also supports a private consumer-pause marker that stops issuing
consumer calls while the independently drained live stream continues. This is
controller preparation, not a measured five-minute outage. All twelve Rust
semantic/schedule cases and the integrated two-version TLS preparation pass.
No buffer, scan, service-level or qualification limit is raised.

PR note: Package WP-825 (open); Tier guarantee; Behavior: explicit admission of
live observation after population seeding and private consumer-pause control;
Checks: twelve Rust tests, Clippy, real two-version preparation and scoped
acceptance; Compatibility: native bounded refusal preserved; Updated handbook
pages: this handoff; Hazards/follow-ups: verify the host population probe with
this gate, implement and measure actual faults and observer handover, then
complete the frozen short rehearsal. The 24-hour run remains unstarted.

## WP-825 bounded watch renewal and consumer-outage preparation

The third observed host preparation failed with native live-query
`SERVICE_UNAVAILABLE` about thirty seconds after watch admission. That timing
matches the daemon's existing thirty-second request deadline. The observer now renews every
twenty seconds through the generated public watch method and opaque cursor,
cancelling the old request before admitting its replacement. A renewed snapshot
must retain exact partition and ticket identities, canonical ordering, and a
monotonic frontier. Renewal counts are actual observations. Native terminal
refusals and unexpected cursor resets still fail; no service bound changes.
The integrated one-ticket preparation now requires a real successful renewal.

Population load is monitored while it runs; observer exit, invalid or oversized
output, and deadline failure stop the owned load child. Before load, the seed's
1,024 organization-zero creation events must be consumed and checked. A new
optional fifteen-minute preparation schedules one real five-minute consumer
pause, requires frozen consumer progress and advancing live observation, times
the subsequent backlog drain, and reconciles all 65,536 final tickets. Private
nanosecond timings remain measurements, not a synthetic campaign receipt.

The separate sink preparation uses
`--population-probe-seconds 900 --observe --sink-outage`. It denies access to
the owned private archive directory for five minutes. The production connector
checks custody before claiming delivery; the preparation requires actual
degraded delivery health, advancing public write and live-query frontiers, and
delivery-drained health within ten minutes after restoring the same archive
objects, with offered load continuing. Pinned read-only descriptors serve only
identity checks and cleanup; no archive engine is opened by the controller.
Missing, changed or unavailable observations refuse preparation. Unit checks
cover custody preservation, substitution, cleanup and observation refusal;
real sink-fault measurement remains pending and no campaign is qualified.

PR note: Package WP-825 (open); Tier guarantee; Behavior: counted public cursor
renewal, supervised population load, and consumer-outage preparation; Checks:
twelve Rust cases, six preparation oracle/child-ownership cases, Clippy,
integrated two-version TLS preparation and scoped acceptance; Compatibility:
native request, retention, cursor and authorization guarantees preserved;
Updated handbook pages: this handoff; Hazards/follow-ups: measure the revised
observed host preparation, complete all remaining faults and version handover,
then freeze and pass the short campaign. No 24-hour run has started.

## WP-825 consumer preparation measurements and load quiescence

The first fifteen-minute host attempt completed 90,000 commands and 360,000
reads with no retries or post-deadline completions, and reconciled every seeded
ticket. Native live observation stayed active through 45 renewals without a
terminal refusal. Its preparation gate failed: the window overlapping consumer
recovery had p95 102.681 ms. The final normal window passed with p95 93.743 ms
and p99 140.542 ms. This failed attempt remains private and unqualified.

Preparation now applies the existing campaign checker's latency exception only
to windows overlapping the measured outage and drain, requires a complete
normal window, and leaves every throughput, error, concurrency and final-state
check binding. It also starts the full five-minute pause clock when the
observer confirms quiescence, rather than when the marker is requested.
Independent hostile tests reject an inactive watch, moving consumer, invented
recovery count, excessive timing, and a fault exception extending into the
normal window.

The load driver adds private controller admission/release controls. Quiescence
blocks new admissions while offered work continues into the same bounded
queue. An exact generated release or protected credential can change only
after every admitted original operation settles; rollback and repeated
generations refuse. A deterministic schedule verifies that resumption between
waiter registration and waiting cannot lose its wakeup. No authoritative
runtime or application-facing interface changes.

PR note: Package WP-825 (open); Tier guarantee; Behavior: confirmed consumer
pause timing, measured fault-window selection, and quiescent generated release
admission; Checks: fifteen Rust cases, seven preparation cases, Clippy, real
two-version TLS preparation and scoped acceptance; Compatibility: offered rates,
queue/flight bounds, native ordering, authorization and 24-hour/72-hour gates
preserved; Updated handbook pages: this handoff; Hazards/follow-ups: repeat the
corrected observed host preparation, integrate actual deployment/observer
handover and remaining faults, then pass a frozen short campaign. No 24-hour
run has started.

## WP-825 measured consumer recovery and observer handover

The corrected fifteen-minute preparation passed on the inventoried 8-vCPU,
32-GiB durable-SSD host. It completed 90,000 commands and 360,000 reads with no
retries or post-deadline completions, reconciled all 65,536 tickets, and checked
3,072 consumer events without duplicates. The confirmed consumer pause lasted
300.050 seconds; its missing-event backlog drained in 13.089 seconds. The final
normal window measured p95 85.436 ms and p99 138.481 ms. This is preparation
with one fault and retained history, not a campaign or physical-reclamation gate.

The same observer now survives exact-parent V2 activation. It finishes and
acknowledges its current batch, cancels the watch, and publishes an atomic
quiescence receipt. Before activation the operator retires the V1 consumer.
After activation the V2 application credential registers the successor consumer
and negatively acknowledges its exact initial lease. The operator then seeks
that consumer to the recorded acknowledged checkpoint. Fresh status must match
the checkpoint and incarnation, with no leases, retries or dead letters. The
observer preserves its event oracle and resumes the generated V2 watch; the
integrated TLS preparation verifies the subsequent V2 event without duplicates.
Seek authority is confined to the operator credential. No application role,
native runtime boundary, durable format, request bound or ordering changes.

PR note: Package WP-825 (open); Tier guarantee; Behavior: quiescent consumer and
watch handover through exact generated releases and existing public maintenance
operations; Checks: seventeen Rust semantic/schedule cases, Clippy, real
two-version Rust/TypeScript TLS preparation and scoped acceptance;
Compatibility: existing authority, history, lease and checkpoint checks remain
binding; Updated handbook pages: this handoff; Hazards/follow-ups: exercise
handover under online retention, finish original-identity fault recovery and all
campaign actions, then freeze and pass the short rehearsal. The 24-hour run
remains unstarted.

## WP-825 original-input interruption recovery

The load worker now resolves interruptions with the original generated command
input and idempotency key. Recovery requires a private declared restart, process
kill or restore interval and a native retry/uncertainty disposition. Permission,
input, protocol and response failures remain immediate refusals. One operation
keeps its original release and admission slot throughout recovery; its deadline
is sixty seconds with at most 240 generated submissions. Clearing a fault marker
cannot abandon an already interrupted identity or extend its deadline.

The integrated TLS preparation now also stops and restarts the real daemon,
and separately kills and reopens it. Each check first connects the generated
client, proves an actual failed submission while the daemon is absent, resumes
that same client, and requires a successful write, exact original-key replay
and named-state reconciliation within sixty seconds of the interruption.
These are small preparation databases with retained history; they do not prove
recovery after campaign pruning or a committed command losing its reply.

Load observations count settled generated submissions and their retries. The
native SDK can additionally look up an uncertain outcome; the submission count
is not represented as an exact wire-RPC count. Campaign accounting must preserve
that distinction. The only added dependency is the existing first-party error
crate, so the worker uses its canonical recovery guidance directly.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded original-input
interruption recovery and actual daemon restart/kill preparation; Checks:
Rust semantic/budget cases, Clippy, integrated Rust/TypeScript handover plus
two interruption preparations, and scoped acceptance; Compatibility: generated
input, release, idempotency and authority boundaries preserved; Updated handbook
pages: this handoff; Hazards/follow-ups: complete independent command/archive
witnesses, campaign fault orchestration and pruning measurements before the
frozen short rehearsal. No 24-hour run has started.

## WP-825 acknowledgement witnesses and state reconciliation

Private seed and load witnesses now record each acknowledged ticket command's
ordinal, commit sequence, exact contract version and history incarnation. The
files have fixed, bounded slots and private permissions; records can arrive out
of order, but a second acknowledgement cannot overwrite a slot. Completion
requires every expected slot and synchronizes the evidence file. Missing,
truncated, malformed, oversized and linked inputs refuse reconciliation.
These files are independent test evidence, never authoritative database storage.

An immutable witness snapshot reconstructs each ticket's acknowledged revision,
including holes at a quiescent backup cut. A later revision without its earlier
ticket revision refuses. Population preparation now compares every named row
against these witnesses rather than relying only on an offered-command counter.
Private release controls also carry the expected history incarnation; an
observer resumption must confirm it through native consumer status.

The small interruption preparations passed on the inventoried host as well:
graceful restart took 0.375 seconds and process kill took 0.323 seconds through
successful write, exact replay and named-state checks. Each had one retried
generated submission. These results do not establish recovery after pruning.

PR note: Package WP-825 (open); Tier guarantee; Behavior: independent bounded
acknowledgement witnesses and witness-based named-state reconciliation; Checks:
Rust witness identity, corruption, gap and out-of-order cases, Clippy, real TLS
preparations and scoped acceptance; Compatibility: authoritative formats and
application interfaces unchanged; Updated handbook pages: this handoff;
Hazards/follow-ups: validate archive payloads against these witnesses, complete
campaign actions and actual physical-retention measurements, then pass the
frozen short rehearsal. The 24-hour run remains unstarted.

## WP-825 independent offline archive reconciliation

The private Rust operator checker now opens the stopped database only for its
dormant identity probe, then uses the existing exclusive durable-archive owner.
It validates every archive page through the native envelope/hash checker,
matches each witnessed database/incarnation/event identity, and independently
reconstructs complete TicketCreated and TicketUpdated payloads from the ordinal
and exact compiler-owned schema. Declared policy anchors must match exactly;
these workload declarations use the accepted unanchored event shape, whose
absence is also checked. Writer-version evidence comes from command replies in
the witness; unanchored archive records do not independently encode that version.
All records must be accounted for, with bounded steps and no payload diagnostics.

The integrated preparation checks two real archived events after production
shutdown. Hostile witness substitutions prove that valid native archive hashes
and counters cannot excuse a different event or history incarnation. Population
preparation also performs this offline reconciliation after stopping its daemon.
The new dependencies are existing first-party compiler, semantic storage, archive
and type owners; the operator checker adds no engine decoder or runtime mutation.

The first witness-enabled five-minute host preparation completed all 30,000
commands and 120,000 reads and reconciled all 65,536 tickets. Its p95 100.550 ms
missed the normal-window limit; p99 was 139.531 ms. It remains failed. Recovery
controls are now consulted only after a native failure or returned replay,
rather than by every successful operation. A repeat measurement is still needed;
no latency target or workload rate has changed.

PR note: Package WP-825 (open); Tier guarantee; Behavior: offline native archive
validation against independent acknowledgements and actual semantic substitution
tests; Checks: twenty-three Rust cases, Clippy, four real TLS preparations,
archive substitution cases and scoped acceptance; Compatibility: native archive
formats, authority and qualification gates preserved; Updated handbook pages:
this handoff; Hazards/follow-ups: repeat the host preparation, measure online
retention and pruning recovery, finish all campaign actions and conformance,
then pass the frozen short rehearsal. No 24-hour run has started.

## WP-825 delivery admission and physical-retention observation

Offline inspection of the failed host population database found a missing
archived event. That additional failure is preserved; the database is not
qualified by its completed commands or reconciled entity state. Population
preparation now observes the public outbox component's explicit healthy status
before starting load and before stopping the daemon. The initial seed drain and
post-load drain have separate bounded timing records. These waits occur with
offered writes stopped and cannot prove the campaign's outage-drain requirement
at continuing offered load. The revised host measurement remains pending.

The optional `--online-retention` population preparation selects the existing
32,768-sequence retention target and 256-sequence maintenance budget. A private
read-only observer inventories actual sealed file identities once a second and
records their disappearance. Reappearing generations, changed identities,
links, excess entries, or disappearance exceeding the production deletion/reuse
counters refuse evidence. Shutdown counters also retain reference and reader
holds. The observer's tests use explicitly synthetic files, not reclamation
claims. A short preparation reports actual counts without claiming the three
physically reclaimed generations required by the complete rehearsal.

PR note: Package WP-825 (open); Tier guarantee; Behavior: explicit public archive
drain observation and physical file/counter inventory under existing online
retention; Checks: eleven preparation cases, thirty-four evidence/checker cases,
integrated TLS preparation and scoped acceptance; Compatibility: no runtime,
retention, archive or qualification guarantee changes; Updated handbook pages:
this handoff; Hazards/follow-ups: measure the revised host run and online
retention, finish campaign faults and current-language conformance, then pass
the frozen short rehearsal. The 24-hour run remains unstarted.

## WP-825 explicit workload transport accounting

The private measured worker now submits updates through the existing native
`RiffDbClient::execute_generated` API with one attempt. Compiler-generated V1/V2
inputs and outcome decoding remain authoritative. Its retained-input recovery
loop resubmits the original command identity; it does not invoke the SDK
facade's automatic outcome lookup. Reads retain the generated named-query
facade, which submits one query RPC. Measured update/read attempts therefore
include every explicit retry while the original in-flight reservation remains
held. Auxiliary consumer, watch and operator calls remain separate from offered
load and must be counted separately by the campaign controller.

The worker exposes `transport_attempts` alongside the earlier submission field.
Historical preparations keep their original observations and accounting limits;
the new field cannot retroactively qualify them. This change does not alter
server recovery semantics, authorization, native SDK behavior or public
application bindings.

PR note: Package WP-825 (open); Tier guarantee; Behavior: private workload
one-attempt native generated submission and explicit original-input recovery;
Checks: workload semantic tests, Clippy, public TLS preparations and scoped
acceptance; Compatibility: existing protocols and generated bindings preserved;
Updated handbook pages: this handoff; Hazards/follow-ups: complete auxiliary
accounting, all lifecycle actions and language conformance, then qualify the
frozen short rehearsal before the 24-hour run.

## WP-825 native blockers before further harness development

Harness expansion is paused for native diagnosis and repair. The required host
preparation with online retention and a consumer outage failed with live-query
`BufferPressure`. It logically pruned 115,460 sequences but released zero source
receipts and zero physical extents. A deterministic storage regression now
reproduces zero receipt progress across 24 interleaved durable writer turns;
its positive control releases 256 receipts with the same owner and data after
an idle validation pass and a later independent durable checkpoint.

The real capability-rotation preparation initially failed because revoked-token
command admission emits a details-free permission denial that the native SDK
correctly rejected as a protocol violation. The native fix supplies the
existing structured kernel authorization refusal at the server boundary and
preserves symbolic `CapabilityRevoked` responses. Client protocol validation
remains strict. All 114 API gRPC unit tests and scoped Clippy pass. A fresh
actual TLS rotation preparation now proves old-credential refusal and exact
same-principal replacement replay, state and archive reconciliation. This does
not establish rotation under continuing campaign load.

The promoted backup recovery timeout from full CI remains unexplained. Its
isolated rerun and a complete 25-test replication rerun pass; neither erases
the original failure or establishes full CI. The failing reclamation regression
is intentionally unresolved. The maintainer approved necessary native repairs;
the proposed source-proof continuation amendment, ADR-0268,
still requires exact-text acceptance before implementation.
See [the concrete repair scope and findings](WP-825-LIVE-OBSERVATIONS.md).
No failed preparation qualifies the milestone, and no 24-hour run has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: repair native kernel
revocation error carriage while preserving symbolic refusal and strict SDK
decoding; Checks: 114 API gRPC tests, scoped Clippy, actual TLS rotation
preparation and handbook checks pass; scoped storage acceptance still fails only
the new reclamation regression (1,141 other tests pass), and all 25 replication
tests pass on rerun; Compatibility: existing error encodings, authorization
decisions, durable formats and qualification gates preserved; Updated handbook
pages: application errors, known limitations, native findings and this handoff;
Hazards/follow-ups: accept the reclamation amendment, resolve native starvation
and explain live-query and backup readiness behavior before resuming campaign
implementation. Full CI and the complete short rehearsal remain pending.

The next native candidate makes authoritative commit readers use the existing
graph-owned command-admission plan cache. A real catalog regression measured
17 full bundle reads for 17 mappings of one commit; reuse needs one. Every
lookup still checks the current durable active pointer, and cold or changed
identity still requires full native validation. Controlled concurrent first
reads and deployment races, historical outcomes, forged plans and missing
current pointers are covered. All 25 read-adapter tests and scoped server
Clippy pass; the complete server unit suite passes 556 tests with one existing
skip. Notification buffers, workload rates and watch terminal behavior
are unchanged. The host overflow remains an unresolved failure until actual
load evidence explains it.

PR note: Package WP-825 (open); Tier guarantee; Behavior: share the already
bounded exact-plan owner with authoritative commit readers, preserving durable
pointer qualification and full cold validation; Checks: red/green bundle-read
regression, deterministic concurrency/deployment schedules, 25 adapter tests
and scoped Clippy, 556 server unit tests, handbook checks; Compatibility: no
public interface, error class, durable
format, authority or capacity change; Updated handbook pages: native findings
and this handoff; Hazards/follow-ups: actual required-host load verification,
native reclamation repair, full CI and complete short rehearsal remain required.

The focused required-host preparation at `a69d664c` completed 90,000 commands
and 360,000 reads, the actual 300.049-second consumer pause and its 10.027-second
drain, with live progress through 45 renewals. It still **failed**: the final
fault-free five-minute window had 1,676 requests above 250 ms, so p99 missed
the gate despite p95 of 83.413 ms. The bounded profiling samples ended before
that window. Source receipts and physical extents again showed zero reclamation.
No final archive reconciliation or qualification follows a failed latency gate.
The complete findings also identify a separate full-population prefix-refresh
scheduling concern. The pending source-proof amendment does not waive that
boundary or any requirement. No 24-hour run has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: record actual required-
host preparation failure and the additional native scheduling concern; Checks:
all 450,000 offered requests completed, exact consumer outage/drain recorded,
live progression observed, unchanged normal-window latency gate refuses the
run, production shutdown reports zero reclamation; Compatibility: unchanged
workload, concurrency, latency, source authority and recovery requirements;
Updated handbook pages: this handoff and native findings; Hazards/follow-ups:
accept ADR-0268 exactly, repair reclamation, diagnose latency and prefix refresh,
explain the intermittent recovery failure, then complete all rehearsal gates.


ADR-0268 was accepted exactly on 2026-10-04 in standalone commit `313f8e77`.
Native continuation now passes the unchanged 600-command, 24-writer-turn
starvation regression. It also repairs the drained journal source allocator
after reclamation and live validated-prefix publication. Completed certificates
retain checked native state without an obsolete reader pin between turns.
The [native findings](WP-825-LIVE-OBSERVATIONS.md) describe mutation coverage,
corruption and recovery checks, streamed-row measurements and bounded numeric
diagnostics. Required-host load, prefix-refresh scheduling, latency, the
intermittent recovery failure, full CI and the complete short rehearsal remain
open. No 24-hour qualification has started.

The failed required-host database exposed two further native qualification
defects: canonical audit keys were misclassified, and command pruning's retained
Service audit materialization invalidated source proof. The native repair uses
actual preimages and the existing key/record decoders. A new 600-command,
24-turn composition regression now reclaims receipts with a write, checkpoint
and logical prune on every turn; malformed or mismatched evidence still refuses
qualification. Required-host physical progress and latency remain unverified.

PR note: Package WP-825 (open); Tier guarantee; Behavior: qualify canonical
audit appends and checked command-audit representation changes during native
journal admission and checkpoint materialization; Checks: deterministic native
composition, full-source oracle and hostile evidence tests, scoped Clippy;
Compatibility: unchanged durable formats, audit semantics, source-control
fences, ordering and public interfaces; Updated handbook pages: this handoff,
native findings and known limitations; Hazards/follow-ups: required-host
continuing-load verification, full acceptance, intermittent recovery failures
and complete short rehearsal remain required.

PR note: Package WP-825 (open); Tier guarantee; Behavior: bounded native source
continuation under accepted ADR-0268, exact deletion preimages, completed-pin
release and drained source-allocation publication repair; Checks: targeted
native progress, corruption, hold, population, pin and process-exit tests,
registered/fence/promotion oracles and scoped Clippy; the full storage suite
ran 1,151 tests (1,148 passed, three architecture layout assertions failed).
All 59 architecture tests pass after updating those assertions to the sealed
footprint commit owner; full package acceptance/CI are not yet established; Compatibility:
unchanged durable formats, ordering, authorization, application semantics and
validated-prefix proof requirements; Updated handbook pages: this handoff,
native findings, TicketDesk acceptance and known limitations;
Hazards/follow-ups: source acknowledgement changes conservatively invalidate;
required-host physical progress and latency, refresh scheduling, recovery
investigation and the complete short rehearsal remain required.

The real local preparation also passes V1/V2 TLS checks through Rust,
TypeScript, Go and Python, plus graceful restart, kill, archive/retention and
capability rotation preparations. These checks retain exact replay, revision,
organization and bounded snapshot assertions. They do not establish the
hash-bound campaign, continuous-load fault coverage or required-host gates.

The subsequent scoped acceptance observed an unsuccessful daemon shutdown in
the repeated-kill replication test. Its isolated repeat passes; the refusal
remains unexplained. Test-only, bounded closed failure-code evidence now makes
the next occurrence diagnosable without retaining raw child diagnostics.
The [native findings](WP-825-LIVE-OBSERVATIONS.md) record that limitation.

PR note: Package WP-825 (open); Tier internal within the guarantee package;
Behavior: closed fixture failure-code evidence after unsuccessful daemon exit;
Checks: source-redaction unit test, real invalid-shutdown process test and
isolated repeated-kill replication test pass; Compatibility: no production
interface, format, deadline or successful lifecycle check changes;
Documentation: this handoff and native findings; Hazards/follow-ups: the original
shutdown refusal and promoted-backup timeout still require diagnosis, and the
final CI receipt must bind a clean final revision.

The unprofiled required-host probe of `2fb963b7` still refuses normal-window p99.
It completed 450,000 requests with zero retries, but production removed zero
receipts or extents. New counters reveal 288 source-proof invalidations and no
continued suffix work. The append-only regression does not cover that native
composition; repair its invalidation/proof handling before campaign expansion.
The [native findings](WP-825-LIVE-OBSERVATIONS.md) preserve the failed run.

Optional prefix refresh now discards a changed root at history, structural and
catalog page boundaries while preserving complete exact-root publication.
This is an implementation choice within the existing full-proof requirement,
not permission to publish partial evidence. Actual source progress and latency,
stable refresh scheduling, intermittent recovery and the complete rehearsal
remain open. No 24-hour qualification has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: exact-root early
discard of stale optional prefix work, closed fixture diagnostics and bounded
read-only namespace forensics; Checks: 11 native prefix/publication tests and
six static scoped acceptance steps pass; the previous full scoped acceptance
had three replication process failures; Compatibility: unchanged full prefix
proof, durable encodings, application semantics, deadlines and fail-closed
backend refusals; Documentation: this handoff and native findings;
Hazards/follow-ups: actual composition still invalidates every source candidate,
normal-window p99 fails, stable refresh paging and recovery diagnosis remain
open, and clean final CI plus the complete short rehearsal are still required.

## WP-825 fair-admission host diagnostic

Native revision `1841f1f1` passes full CI and the release source installation
check. Its unchanged 900-second consumer-outage preparation on the required
8-vCPU, 32-GiB durable-SSD host still fails normal-window latency. All 90,000
commands and 360,000 reads complete without retries, and full population state
reconciliation passes. The final five-minute window has 11,565 requests above
250 ms, so both p95 and p99 fail. The measured consumer pause starts at 5.267
seconds, lasts 300.048 seconds and drains in 5.715 seconds; only the first two
windows overlap that outage and recovery. The failed preparation is preserved.

Whole-preparation native counters report 61,884 source receipts removed, one
extent recycled and none deleted. Source admission grants 570 of 572 queued
turns, with two expirations; continuation streams 413,586 rows after 178 initial
rows. Its last planning sample has resume 61,629, checkpoint ceiling 364,681
and tail 372,644, with no source holds observed at that barrier. These are
planning positions and cumulative counters, not eligible bytes, per-pass
durations or proof of physical reclamation for the retained range.

The production scheduling regression separately demonstrates that actual
source deletion still receives a one-second idle recheck when logical retention
is held or at its target. Refining that policy requires exact review under
ADR-0271; it has not been implemented. The new sink-outage and queried-projection
preparations pass 50 semantic tests and five integrated scoped checks. Their
separate host diagnostic remains unqualified. Native performance, measured
resource classification, complete fault coverage and the qualifying short
rehearsal remain open. No 24-hour run has started.

PR note: Package WP-825 (open); Tier internal; Behavior: record measured failed
host preparation and its limits; Checks: 50 evidence/preparation tests and five
integrated scoped checks pass; Compatibility: no native behavior or guarantee
changes; Updated handbook: this handoff; Hazards/follow-ups: exact pacing review,
normal-window performance repair and complete milestone evidence remain pending.

## WP-825 source-progress rechecks

ADR-0272 is accepted. Source steps that actually commit receipt deletion use a
progress recheck even when the separate logical watermark is held or at its
target. They still rest for the complete actual writer residence using the
unchanged factor and cap. Zero removal keeps the existing idle backoff; floor
plans, validation counters and granted requests cannot select progress. The
production scheduling regression passes after failing on the preceding policy.
The deterministic rest test covers complete phase accounting, exclusion of
queue waits, idle backoff and cancellation before every rest duration. Public
health labels, checkpoint waits, safety checks and deletion bounds are unchanged.
All nine scoped checks pass, including 632 server tests with eight existing
skips. Full CI passes at native revision `6a6128fb`, including conformance and
the final source-installation check. The first CI attempt stopped because its
environment omitted the pinned TypeScript compiler from `PATH`; that attempt
is preserved. Repeating the complete battery with the repository's locked
TypeScript 7.0.2 compiler passes on unchanged source. A fresh required-host
900-second consumer-outage preparation completed with the exact verified
native binaries. All 90,000 commands and 360,000 reads completed without retries;
full state and archive reconciliation passed. The final normal window measured
p95 56,425 us and p99 77,406 us. Consumer recovery took 8.674 seconds after its
300-second pause. These results cannot support closure: native retention entered
its terminal `refused` state after 54 recorded attempts, removing no source
receipts, advancing no logical retention steps and retiring no physical extents.
Latency with that stopped lane does not prove sustainable lifecycle performance.

PR note: Package WP-825 (open); Tier guarantee; Behavior: actual committed source
deletion receives progress rechecks with unchanged gate rest; Checks: native
scheduling, rest and worker lifecycle tests, nine scoped checks and full CI at
`6a6128fb`; Compatibility: unchanged public controls, formats, ordering, holds,
checkpoint evidence and durability; Updated handbook: this handoff;
Hazards/follow-ups: diagnose the native terminal refusal, then establish
required-host latency with actual reclamation and all remaining campaign gates.
This repair does not qualify a rehearsal or start the 24-hour run.

## WP-825 native health wire correction

The separate sink/projection diagnostic stopped before fault injection because
the harness incorrectly expected an integer health sequence. Native CLI health
correctly returns a canonical decimal string. The real projection query was
Ready at sequence 65,731 after health sampled 65,744, proving an observed lag
of 13 sequences; the harness failed to record it. Both the projection sampler
and sink oracle now validate the native decimal representation before numeric
comparison. All 51 evidence/preparation tests pass, including rejection of
JSON numbers, malformed decimals and overflow. The failed preparation remains
preserved. This correction changes no native interface, query semantics or
milestone gate. The repeated consumer preparation collected queried projection
samples successfully. The separate sink preparation collected 48 samples before
the native retention thread's stack overflow described below.

## WP-825 preparation memory observations

Population preparations now write private `memory-samples.jsonl` observations
of the controller, server, Rust load worker and Rust observer every five seconds.
Every load window needs an actual sample. The collector reads bounded Linux
process status, checks process start-time identity before and after collection,
and refuses missing fields, exit, zombie state, substitution, duplicate fields,
overflow and reused process identifiers. It reports counts without process
identifiers, process names or filesystem paths. Collection uses the controller
thread and adds no application request slot.

The `allocator_bytes` diagnostic is explicitly a **whole-process virtual address
space upper bound**, including executable/file mappings, thread stacks and
reserved or free address space. It is not allocator live bytes. `VmData` is not
used as a substitute for allocator accounting. The record names its basis and
sets `qualifies_resource_model` to false. These four-process samples exclude
transient CLI children and unobserved peaks between samples; they cannot set
the campaign's `resource_model_verified` check. Qualifying accounting still
requires the complete process inventory, a frozen disclosed resource model and
native eligible-history byte observations. No per-command allowance may excuse
process-memory or reclaimable-history growth.

All 61 checker/preparation tests pass, including real owned child collection,
deterministic replacement during a sample, reused process identity, missing
window coverage and preservation of private failed observations. The required-host
sink repetition collected 48 observations before a native stack overflow. Complete
campaign resource accounting and a successful repetition remain pending.

PR note: Package WP-825 (open); Tier internal; Behavior: bounded private
preparation memory diagnostics with explicit incomplete scope and conservative
basis; Checks: 61 evidence/preparation tests pass, including a signal-scheduled
real child collection test; Compatibility: unchanged native/public interfaces,
request limit and campaign gates; Documentation: this handoff and the lifecycle
example; Hazards/follow-ups: transient process accounting, observed peaks,
native eligible-history bytes, host repetition and the complete short rehearsal
remain required before qualification.

## WP-825 native maintenance refusal acceptance gap

The consumer preparation returned success despite the native shutdown census
reporting `online_retention=refused`. Its command, state and archive checks were
valid, but omitted this independent maintenance outcome. Online-retention
preparations now require the native lane's normal `cancelled` owner-shutdown
state. Missing, malformed, repeated inconsistently or refused terminal evidence
fails preparation. Actual extent observations are retained before that refusal;
zero removed files never becomes proof of reclamation.

The new regression fails on the previous collector for all seven hostile
terminal inputs and passes after the correction. All 62 checker/preparation
tests and all five scoped acceptance checks pass. The separate sink preparation
used its frozen preceding controller; its native fatal failure is recorded below.

PR note: Package WP-825 (open); Tier internal; Behavior: refuse online-retention
preparation success when native maintenance stopped or its terminal evidence is
missing or malformed; Checks: 62 checker/preparation tests and five scoped
acceptance checks; Compatibility:
unchanged native behavior, public interfaces and qualification thresholds;
Updated handbook: this handoff and lifecycle example; Hazards/follow-ups:
native refusal diagnosis and repair, measured reclamation and complete short
rehearsal remain required. No 24-hour run has started.

## WP-825 closed native refusal diagnostics

Terminal maintenance refusal now emits one private, bounded observation naming
only numeric codes for the failing phase and existing closed storage error kind.
Phase 1 is `step`; phase 2 is `prefix_refresh`. Kind codes 1–8 mean unavailable,
unknown commit status, corrupt data, incompatible format, limit exceeded,
invariant violation, sequence exhausted and pruned history, respectively.
The previous handler discarded this cause before recording
its generic incident. The observation includes no source error, incident
identity, filesystem path, payload or caller text. Normal shutdown cancellation
remains separate; native refusal still stops the lane without retrying past it.
No public health field, RPC, durable format or maintenance policy changes.

The regression fails on the previous generic diagnostic and checks every
storage error kind in both phases. All six focused retention-worker tests pass,
including actual native refusal, terminal-state preservation, cancellation and
unchanged source pacing. The broader test run passes all 633 server tests across
17 binaries, with eight existing skips. Its initial acceptance fails formatting;
after correcting the formatting and narrowing the diagnostic to numeric codes,
all six focused worker tests and six scoped non-test checks pass. Required-host
reproduction with these diagnostics is
still pending; the exact native refusal cause has not yet been measured.

PR note: Package WP-825 (open); Tier guarantee; Behavior: retain closed private
phase/kind evidence at the existing terminal refusal; Checks: six focused native
worker tests, 633 broader server tests, 62 checker/preparation tests and six scoped
non-test checks pass; Compatibility: unchanged
public interfaces, native refusal, durable formats, ordering and pacing;
Updated handbook: this handoff; Hazards/follow-ups: exact native reproduction
and repair, full CI and complete lifecycle qualification.

## WP-825 sink diagnostic native stack overflow

The separate `15f133c7` controller / native `6a6128fb` sink-outage diagnostic
failed around 241 seconds into offered load, during the archive pause. The
server's native `riffdb-retention` thread overflowed its stack and aborted the
daemon. The next sampled health call produced no response before its deadline;
observer and daemon cleanup also failed. No complete load window or final state,
archive or extent reconciliation was produced. All failed artifacts are retained.

There were 48 successful projection and four-process memory observations before
the abort. The last memory observation recorded server RSS 689,913,856 bytes
and virtual address space 4,540,047,360 bytes; these sampled values do not measure
the failing thread's stack or establish complete resource accounting. An earlier
native source floor plan observed resume 89,826 and prior checkpoint 148,863;
these positions are not byte measurements or a substitute for the missing final
removal census. The fatal native stack path needs diagnosis and repair. Increasing
sampling deadlines or hiding the abort cannot qualify this milestone.

## WP-825 iterative publication-lineage retirement

A process-isolated storage regression reproduces a native stack overflow when
the default destructor retires the supported 65,536-node publication lineage
on a 384-KiB maintenance stack. The lineage fits the existing allocation bound.
This demonstrates a database defect independently of the workload controller.
It does not yet establish the call path of the required-host abort.

Retirement now detaches uniquely owned predecessor links iteratively, using
`Arc::into_inner` to preserve ownership when snapshot owners retire concurrently.
Shared predecessor nodes remain intact until their final owner releases them.
The regression checks surviving snapshot contents and ancestry, final release
of every node, and concurrent release from distinct successors sharing a chain.
Stack sizes, overlay limits, qualification checks, durable encodings, writer
ordering and maintenance pacing remain unchanged.

The initial regression aborts before the repair; all 325 storage API unit tests
pass afterward. All eight scoped acceptance checks pass, including 555 tests
with all features enabled. The strengthened retirement regression also passes
in the release profile. Required-host testing and full CI on the final source
remain pending. A local debugger workload did not reproduce the host abort;
it ended with incomplete sink recovery and supplies no qualifying evidence.
No 24-hour run has started.

PR note: Package WP-825 (open); Tier guarantee; Behavior: retire supported
publication ancestry without recursive stack growth while preserving shared
snapshot pins; Checks: failing process-isolated reproduction, 325 unit tests,
555 all-feature scoped tests, release-profile regression and eight scoped checks;
Compatibility: unchanged public interfaces, durable bytes, ordering and limits;
Updated handbook: this handoff and live observations; Hazards/follow-ups: confirm
the host crash call path, diagnose terminal refusal, run final-source full CI
and repeat the unchanged required-host workload.

## WP-825 joined retention shutdown census

The daemon's shutdown-trigger observation is captured before the retention
thread joins. Previously it remained the final census even when that thread
later cancelled normally. A healthy lane could therefore report its live idle
or fenced state, causing the terminal-state collector to reject it. This does
not invalidate the earlier measured refusal: refused owners retain that state.

The retention owner now replaces the live observation after joining its thread
and releasing source-validation pins. It records the actual final state and
counters; it never substitutes cancellation for refusal. The real native-worker
regression fails on the old census and passes after the repair, checking both
healthy cancellation and preserved terminal refusal. All six focused worker
tests pass with all features enabled. Scoped acceptance passes all 13 checks and
all 633 server tests, with eight existing skips, using an owned temporary root.
The initial broad run fails six tests while concurrent runners share temporary
storage across separate PID namespaces. Observed stale-directory sweeps can
remove another runner's live fixtures. That failed run and diagnostic traces
are retained; the isolated rerun keeps every assertion and deadline unchanged.
Final-source full CI and host testing remain pending; the public health labels
and census field registry are unchanged.

PR note: Package WP-825 (open); Tier guarantee; Behavior: record retention
shutdown evidence after owner join; Checks: baseline regression fails, six
focused all-feature worker tests and all 633 scoped server tests pass, with
13 scoped checks; Compatibility: unchanged storage, public
interfaces, ordering and maintenance policy; Updated handbook: this handoff and
live observations; Hazards/follow-ups: final-source acceptance, CI, native refusal
diagnosis and unchanged host workload before lifecycle qualification.

## WP-825 maintenance worker drain

Ordinary shutdown explicitly joins the durable-local dispatcher, online
retention owner and background scrub before draining the coordinator. The
maintenance path omitted those joins and deferred them to field destruction
after its drain boundary and graceful close. A real public backup regression,
with a configured local archive and online retention, reaches `DrainComplete`
with background task handles still owned. The dedicated crash fixture refuses
that boundary rather than reporting the expected crash. The failed run remains
failed.

Maintenance now explicitly joins those same owners before coordinator drain.
The process fixture checks actual task ownership at the crash boundary; it
does not inspect source text or change the production crash controller. Its
recovery arm also checks the original command outcome and provenance, a first
successful subsequent write, and that write's exact idempotent replay. The
preparation gate runs this arm before TicketDesk lifecycle smokes. The release
regression passes. Scoped acceptance passes all 15 checks and all 798 tests,
with eight existing skips, using owned temporary storage. Full CI and the
unchanged required-host diagnostic on the final source remain pending.

The earlier full CI on the joined-census source fails one archive-publication
test at sealing with `Unavailable`. The unchanged exact test and the complete
parallel storage suite subsequently pass (876 tests, one existing skip). Those
passes do not explain the original failure or replace full CI. A separate older
CI stops at the redirected TypeScript build because its isolated checkout lacks
the pinned Node type definitions. Required CI checkouts need the complete locked
TypeScript dependency installation, not just a compiler on `PATH`. No host
diagnostic or 24-hour qualification started after either failed CI.

PR note: Package WP-825 (open); Tier guarantee; Behavior: join native background
owners before the maintenance drain and CLEAN boundary; Checks: real baseline
crash-boundary refusal, passing release recovery/replay regression, 798 scoped
tests and 15 acceptance checks; Compatibility: unchanged public maintenance
receipts, durable bytes, command ordering and durability profiles; Updated
handbook: backup and restore, this handoff and live observations;
Hazards/follow-ups: final-source full CI and host repetition, the unresolved
earlier archive-publication failure, native eligible-history bytes and complete
lifecycle qualification. No 24-hour run has started.

## WP-825 final native CI and scratch isolation

Full CI passes on frozen native revision `f7402c87`, including workspace tests,
four-language operational conformance, lifecycle preparations and release source
installation. Its clean source and CI-log digest are retained privately. The
unchanged 900-second sink-outage diagnostic failed on the required host:
delivery remained degraded after access restoration and the last two windows
overflowed the latency histogram. This is diagnostic work, not the seven-fault short
rehearsal or a 24-hour qualification.

A separate failing test reproduces shared testkit scratch cleanup deleting
a directory whose PID namespace is unknown. Linux scratch names and sweeps now
bind the actual kernel PID namespace. Same-namespace dead-session cleanup,
panic cleanup and kept-evidence behavior remain required; foreign namespace
and legacy names of unknown ownership survive. The change affects test fixture
lifecycle only. It does not explain the earlier archive-publication failure:
that storage suite already has its own namespace isolation.
Frozen integrated testkit revision `4fbb44a5` also passes full CI and release
source installation. This receipt excludes the later checkpoint-wait changes.

PR note: Package WP-825 (open); Tier internal implementation within the
guarantee package; Behavior: isolate testkit stale-scratch cleanup by PID
namespace; Checks: baseline deletion reproduced, all 42 testkit tests and all
13 scoped acceptance checks pass; Compatibility: no database/application API,
storage format, workload, native validation or test-deadline changes; Updated
handbook: contributing, this handoff and live observations; Hazards/follow-ups:
unknown legacy scratch directories need deliberate owner cleanup, and native
delivery/latency repair, byte measurements and full short rehearsal remain required.

## Captured source validation during checkpoint waiting

The native retention worker previously left an existing read-only source
candidate unadvanced during its checkpoint wait. It now advances one bounded
page between the existing 100-ms wait slices, stopping page work at exact end.
It does not capture another root or request a writer turn. Writer admission,
deletion, independent checkpoint licensing and full writer-residence rest
remain in the ordinary step. Cancellation and validation errors stop the work;
stale qualification discards the candidate and exact end releases its reader.

This repairs a separate native coupling, not the observed host failure: the
failed diagnostic recorded zero logical checkpoint-wait holds. Its final lane
was cancelled without an incident, removed 113,922 source receipts and recycled
one extent, while incoming source history advanced faster. All 90,000 commands
and 360,000 reads completed without retries. Final sink health remained degraded;
state/archive reconciliation did not run after the controller refused recovery.
These results cannot qualify a rehearsal or prove a cause for the latency.
Complete canonical archive validation on a private copy observes 142,532
records and 36,980,305 encoded bytes, below the 1-GiB quota. The record count is
13,004 below the expected acknowledged workload; complete identity/payload
reconciliation remains unproved. Original failed data is preserved.

PR note: Package WP-825 (open); Tier guarantee; Behavior: continue captured
read-only validation inside the existing checkpoint wait; Checks: baseline
writer-admission regression fails, three deterministic worker scheduling tests
and eight storage drain/validation tests pass; Compatibility: unchanged public
interfaces, ordering, holds, durable formats, fsyncs, page/deletion bounds and
writer rest; Updated handbook: this handoff and live observations; Hazards and
follow-ups: scoped acceptance, final-source full CI, host delivery/latency
diagnosis, native byte accounting and the complete short rehearsal remain
required. No 24-hour run has started.
The retention change's scoped acceptance passes all 13 checks and all 1,840
tests, with ten existing skips. Subsequent archive timing instrumentation is
separately tested below and still requires integrated final-source acceptance.

## Native archive cost observations

Observed outage preparations accept `--population-probe-seconds 900` or
`--population-probe-seconds 1500` with `--observe` and one of `--sink-outage`
or `--consumer-outage`. The longer bounded mode leaves a complete five-minute
normal window after a pause of five minutes and the existing recovery budget
of ten minutes. Every measured window retains its rate, concurrency and
observation checks; normal windows retain p95 below 100 ms and p99 below 250 ms.
The shorter mode can refuse `probe_normal_window` despite meeting the recovery
budget. Both are preparation evidence; neither qualifies the 24-hour campaign.

The native worker now emits bounded timing observations for archive admission,
acceptance and per-record commit beside its existing primary-writer stages.
Each graph owns its counters; restart starts a new census and the worker's
retirement prints the final snapshot. A read-only timing handle retains no
archive, reader, writer lease or delivery capability. It cannot acknowledge
an event, grant reclamation, or turn failed delivery into healthy status.

Counters measure attempts, including refusals and equal retries. Commit timing
is included in acceptance timing; an equal retry may also call admission from
acceptance. Do not sum overlapping stages or treat attempt counts as deliveries.
Archive creation/initial validation and startup timings are outside these
per-record observations. Two-phase Immediate commits, sync boundaries, quota,
deduplication, fault duration, offered load and all qualification gates remain.

PR note: Package WP-825 (open); Tier guarantee; Behavior: native archive timing
observations for diagnosis; Checks: missing-observation test fails before the
implementation, real archive retry/refusal/accounting and concurrent-counter
tests pass; Compatibility: no application protocol, health label, durable
format or persistence-policy change; Updated handbook: this handoff and live
observations; Hazards and follow-ups: final-source acceptance/CI and required-host
delivery/latency attribution remain required. Diagnostic counters alone do not
qualify a rehearsal.
All 33 outbox tests, both server diagnostic-format tests and all-feature Clippy
for the three affected crates pass. These receipts do not replace final-source
CI or the failed host recovery/latency gates.

## Native provider-budget observations in preparation

Observed population preparations also collect the daemon's existing
`exact_provider_lifecycle` census. The pool's charge is the sum of fixed owner
bytes, retained owned bytes and reserved unpublished work, checked against the
unchanged aggregate 1-GiB limit. This is independent of RSS and the conservative
whole-process virtual-address-space observations. A smaller VmSize does not
prove provider retirement or complete provider charge accounting.

The bounded collector starts at the load-ready boundary and reads only new
complete diagnostic lines. Each five-minute window needs actual samples; gaps
above 75 seconds, unavailable counters, counter rewinds within one pool,
inconsistent ownership, changed log identity and over-budget charges refuse the
preparation. Allocation count is a live gauge that may fall during retirement;
released allocations and released owners are cumulative counters. The native census is due every 60 seconds. Partial lines remain
pending; old seeding samples cannot fill load-window coverage. Output contains
numeric counters and closed labels, with private exclusive publication.

The observation explicitly does not qualify the complete campaign resource
model. Transient process accounting, eligible-history bytes, all lifecycle
faults and the six final fault-free hours remain required. This collector
changes preparation evidence only; database ownership, admission, requests,
durable bytes and application interfaces are unchanged.

PR note: Package WP-825 (open); Tier internal; Behavior: independently collect
native aggregate-provider charges during observed preparations; Checks: missing
collector regression, twelve provider-accounting/coverage/log tests and all
78 lifecycle checker tests pass; Compatibility: existing native diagnostic
format and all workload/service gates preserved; Updated handbook: this
milestone handoff; Hazards and follow-ups: real final-source preparation and
complete campaign resource accounting remain unproved. No 24-hour run is started.

## Bounded shutdown evidence collection

The extent observer streams the closed daemon diagnostic log instead of loading
the whole file. Its fixed limits are 16 MiB total, 4 KiB per complete line and
262,144 lines. It retains at most three copies of each shutdown record kind,
allowing the existing one-or-two-copy check to reject duplicates. A changed,
replaced, linked, oversized or incomplete log refuses collection.

The previous 1-MiB whole-file limit rejected a complete native shutdown census
behind an otherwise bounded volume of diagnostics. This repair changes evidence
collection only. Native deletion/reuse counters alone still cannot qualify a
particular physically reclaimed generation: its disappearance must have been
observed on this volume. Missing or refused maintenance still fails, and measured
extent observations survive a terminal maintenance refusal.

PR note: Package WP-825 (open); Tier internal; Behavior: bounded streaming of
shutdown evidence; Checks: the larger-log regression fails before the repair;
all 81 lifecycle checker tests pass, including log replacement, size, line-count,
partial-line and custody refusals; Compatibility: no runtime, durable format,
application interface or qualification threshold change; Updated handbook: this
milestone handoff; Hazards and follow-ups: final-source checks and an actual
complete short rehearsal remain required. No 24-hour run is started.
