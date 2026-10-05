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
