# WP-825 native blockers and bounded live observation proposal

WP-825 is open. The maintainer approved expanding scope to cover necessary
native fixes in session on 2026-10-04: "okay, i approve expanding the scope to
cover any necessary fixes". The separate widening commit records the identified
runtime paths before implementation. This is scope approval; an accepted
architecture decision or normative guarantee still requires separate review
before it changes.

The campaign needs periodic retention eligibility, watermark, physical extent
and hold observations while offered load continues. The current public
`retention status` command requires a closed database. Shutdown counters prove
only the final accumulated state. Neither can supply independent observations
for the final six fault-free hours without stopping the daemon.

Approved additional observation scope:

- `crates/riffdb-server/src/**`
- `crates/riffdb-storage-redb/src/**`
- Consequence tests for those crates.

Implementation decisions within ADR-0265:

1. Add bounded internal, payload-free observations to the existing owned
   retention lane and daemon diagnostic stream. No new application RPC,
   operator mutation, configuration control or storage format.
2. Report real logical watermarks and retention eligibility, deletion/reuse
   counters and reference/reader holds. Clearly separate modeled upper bounds
   from exact byte counts; unavailable accounting refuses campaign evidence.
3. Use existing native ownership and fencing logic. Do not reproduce journal
   formats in the Python controller or infer eligibility from file age.
4. Bound collection work, diagnostic size and emission rate. Do not acquire
   authoritative write authority, hold a gate across diagnostic I/O, pin old
   snapshots or repeatedly scan retained history.
5. Report only counts, closed states and durations. No tenant, capability,
   payload, machine address, path or secret in emitted observations.
6. Preserve current ordering, authorization, pruning eligibility, source
   receipts and native reclamation. Observations cannot authorize deletion.

Validation must include deterministic collection/retirement schedules,
malformed or unavailable observation refusal, existing retention and recovery
tests, scoped acceptance and full CI. The complete short rehearsal still needs
all seven actual faults, three physically reclaimed generations and the final
six fault-free hours. No qualification is established by this proposal.

AGENTS.md requires human review when a work package needs paths outside its
declared scope. Approval extends the package paths only; a discovered conflict
with a specification or accepted decision still stops for separate review.

## Native defects and repair scope

The real preparation now blocks on database behavior. Harness expansion is
paused until these issues are resolved. Failed preparations stay failed, and
no 24-hour run has started.

Approved additional repair scope:

- `crates/riffdb-api-grpc/src/**`
- `crates/riffdb-service/src/**`
- The server and storage paths listed above, including consequence tests.

This supersedes the earlier proposed SDK status-message exception. The Rust
SDK's strict validation must remain intact. Scope approval permits diagnosis,
regression tests and repairs within accepted decisions; it does not accept a
new storage, ordering, authorization or interface guarantee.

The approved repair scope also includes the proposed ADR-0268 record needed to
review the source-proof lifecycle change. Listing its path permits proposal
and review; it does not accept the record or authorize its implementation.

### Revocation interoperability: repaired; real TLS preparation passes

The real capability-rotation preparation refuses both old-bearer reads and
same-key command submissions, but its command check fails. Named queries
return native `CapabilityRevoked`. Native Execute returns the server's existing
fixed, details-free revoked-token `PermissionDenied` status. The Rust SDK treats
that canonical status as `MissingPublicErrorDetails`, so applications receive a
protocol failure instead of an authorization refusal. This cannot qualify as
successful typed revocation handling.

The correction is at the server boundary: encode the existing kernel
`PublicError::authorization_denied()` for an exactly matched revoked credential.
Symbolic application boundaries continue to return existing `CapabilityRevoked`.
The current kernel encoding deliberately omits application-only hints; generated
kernel-backed command facades consequently retain `AuthorizationDenied`.
No client may guess a semantic refusal from unvalidated peer prose. Unknown or
malformed details remain protocol failures. The regression gate is real old-
credential refusal and same-principal replacement replay through the native SDK.
The regression failed against the previous details-free status. All 114 API
gRPC unit tests and scoped Clippy now pass. A fresh TLS preparation verified
old-bearer read and command refusal, replacement replay of the original sequence,
version and outcome, unchanged named state, and reconciliation of the actual
two archived events. This is preparation evidence, not continuous-load fault
qualification or a complete milestone pass.

### Live-query notification overflow: reproduced; cause unresolved

The required 100-command/s, 400-read/s preparation with online retention and a
consumer outage ended with `BufferPressure`. The server notification owner
queues at most 256 first-commit notifications per subscriber and terminates when
that bound is exceeded. The independent client drains continuously and records
the terminal refusal as a failure. This is not a qualification success.

Trace server acknowledgement progress, port admission and checkpoint/prune
interference before choosing a fix. Preserve the queue ceiling, exact commit
validation, current authorization and terminal semantics. Automatic reconnect,
discarded checkpoints, larger buffers or slower offered load cannot repair the
observed availability failure.

The native commit reader independently reconstructed the complete catalog
lineage for every notification, bypassing the command-admission adapter's
existing bounded exact-plan cache. A real catalog regression measured 17
durable bundle reads for 17 translations of the same commit. The candidate
shares that graph-owned cache with commit point and scan readers, checks the
current durable pointer on every lookup, retains full cold resolution and exact
plan identity, and does not increase the existing 4,096-plan capacity.
The regression now loads once across the unchanged pointer. Controlled competing
readers also load once; a raced deployment cannot publish a result under the
wrong pointer. Historical outcome preservation, successor validation, forged
plan refusal and absent-current-pointer refusal pass. All 25 read-adapter tests
and scoped server Clippy pass. The complete server unit suite also passes:
556 tests, one existing skip. This removes measured repeated native work; it
does not establish the cause or resolution of the host buffer-pressure failure.
A new continuing-load host probe and the complete rehearsal are still required.

The next C3D preparation used committed source `a69d664c`, fresh release
artifacts and a fresh database on the required 8-vCPU, nominal 32-GiB SSD host.
It completed 90,000 commands and 360,000 reads in three five-minute windows,
with zero retries and at most 28 workload requests plus four auxiliary slots.
The actual consumer pause lasted 300.049 seconds and recovered 2,048 missing
events in 10.027 seconds. Live progress continued through 45 cursor renewals
without buffer pressure during the offered load. This does not establish the
earlier failure's cause or a performance improvement against the older artifacts.

The preparation **failed** its normal-window latency gate. In the final
600–900-second window, p95 was 83,413 microseconds, but 1,676 of 150,000
completed requests exceeded the histogram's 250,000-microsecond ceiling.
Thus p99 could not satisfy the required strictly lower 250-ms bound. The two
bounded CPU profiling samples ended before 300 seconds after worker spawn;
neither overlapped this final window. The failed preparation remains failed.
Shutdown again reported zero source receipts removed and zero extents deleted
or recycled. Final archive reconciliation did not run after the latency refusal.
No complete rehearsal, full CI receipt or 24-hour qualification is established.

### Source-receipt reclamation: deterministic starvation reproduction

The failed preparation logically pruned 115,460 application sequences across
582 steps, but removed zero source receipts and zero physical extents. It had
five sealed extents of roughly 320 MiB each. The final reference-hold counter
was 1,145 repeated blocked attempts, not a count of distinct retained objects.
Offline native status reported watermark 115,460 and consumer fence 132,287.

`reclaim_history_online` validates at most 256 receipts per step against one
immutable root. At completion, `Barrier::try_from_validated` requires a drained
database and the identical current root; a changed root discards the proof.
The same lane performs logical pruning between validation pages, and ordinary
writes also advance the root. This creates a starvation candidate under ongoing
load. Existing small tests primarily establish safe refusal and idle progress.

The new failing regression
`production_receipt_reclamation_progresses_with_interleaved_durable_writes`
uses 600 complete command fixtures and explicit checkpoint hooks. It removed
zero receipts across 24 interleaved independently durable writer turns. Its
positive control uses the same owner and data: an idle validation pass, a later
durable checkpoint and another idle pass removed 256 receipts. Application
history remains retained throughout, isolating receipt admission from logical
pruning and legitimate application history retention. No correctness sleeps
or directly injected history rows are used.

The full retained receipt/audit walk is also restarted for each admission.
Measure this repeated work against END-008 rather than counting only the
256-row deletion budget. An actual ongoing-load proof is still required.
Separate legitimate source/provider holds from stale-proof and admission
refusals. Any solution must preserve complete validation, current binding,
independently durable checkpoint evidence, every hold, 256-receipt steps and
maintenance pacing. If bounded progress requires changing ADR-0265's validation
or scheduling decision, propose that exact change for separate human acceptance;
do not implement weaker evidence or a harness maintenance pause.

### Validated-prefix refresh: additional scheduling concern

Inspection found that `refresh_validated_prefix` performs the complete retained-
history, structural and catalog passes, then prepares the candidate, in one lane
call. Each individual page is bounded and cancellation is checked, but the lane
does not regain control between those pages. Publication can yield or go stale
after all population work. Repeated attempts can therefore delay source work
and repeat population validation. Actual pass counts, cancellation, changed-
binding schedules and recovery consequences still need a regression and diagnosis.
ADR-0268 deliberately preserves this separate proof boundary; its source-only
continuation must not be treated as permission to reuse a partial structural or
catalog proof. Complete validation and exact current publication remain required.

### Promoted backup recovery: unresolved intermittent failure

Full CI timed out waiting for promoted maintenance to regain source readiness.
An isolated rerun passed, then the complete replication group passed all 25
tests in 148.51 seconds. Passing repeats do not erase the original failure or
explain it. Preserve the 30-second operation deadline and investigate native
recovery progress and test
interference before deciding whether a production repair is needed.
