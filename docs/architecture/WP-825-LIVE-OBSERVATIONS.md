# WP-825 native blockers and bounded live observation proposal

## Maintenance drain repair: scoped validation passes

A public backup with native retention and a durable-local archive reaches the
maintenance drain boundary without explicitly joining those owners or the
background scrub. Ordinary shutdown already joins them before coordinator
drain. The process regression checks actual task ownership at `DrainComplete`
and refuses the crash fixture's expected abort when a task remains unjoined;
the baseline fails. Maintenance now uses the same explicit joins, preserving
the existing receipt, authoritative ordering, durability and CLEAN protocol.
The recovery regression also exercises original outcome/provenance replay and
a successful subsequent command with exact idempotent replay. The release
regression and scoped acceptance pass: 798 tests, eight existing skips and
15 checks. Final-source full CI and host repetition remain pending; no rehearsal
or 24-hour run is qualified.

The joined-census source's full CI fails one native archive-publication test
with `Unavailable` while sealing. Its unchanged exact regression and complete
parallel storage suite subsequently pass: 876 tests and one existing skip.
The initial failure is retained and its cause is not established. A different
older CI fails the redirected TypeScript build with missing pinned Node type
definitions. Neither failed CI permits host workload launch.

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

The maintainer accepted the exact text of ADR-0268 in session on 2026-10-04.
The standalone acceptance commit is `313f8e77`. It authorizes bounded native
source-proof continuation; the separate validated-prefix refresh boundary and
all rehearsal gates remain intact.

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

### Source-receipt reclamation: starvation regression repaired; load verification pending

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

The original failing regression
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

The implementation now qualifies complete actual mutation sets at the closed
native capture, journal submission and checkpoint owners. Raw or uncovered
transactions invalidate the process-local epoch before publication. Catalog,
lineage, source authority, registration and other incompatible changes require
a fresh complete pass. Source acknowledgement changes currently take that
conservative invalidation path; no hold or acknowledgement is manufactured.

One native validation owner retains its registration, fence and promotion
census, validates new receipt/audit suffixes to exact end, and rechecks every
current hold under the existing nonwaiting drained gate. Suffix closure is
bounded by one existing receipt page and one audit page. Own prefix deletion
checks exact removed preimages, the surviving floor and canonical control
receipt before rebasing. Reclamation and prefix-refresh receipts remain excluded
from independent progress. Completed certificates release their read pins
between turns; incomplete candidates retain one pin and replacement uses at
most two. No ancestor chain or mutation queue was added.

The original writer schedule and idle positive control now pass. Native
population tests count streamed source rows at 7, 300 and 600 receipts: one
initial walk plus exactly five added receipts, with no repeated idle walk.
Additional tests cover uncovered publication with equal source roots, overflow,
changed receipt preimages, gaps, hold registration, failed-pin release and
process exit before admission and after durable certificate rebasing. Separate
full pinned-state oracles cover registered evidence, primary fences and
promotion. These are implementation checks; they do not establish actual
required-host latency or physical retirement under continuous load.

The first green-progress attempt exposed an additional native publication bug:
a drained journal runtime retained the source allocator preceding a control
receipt. Its next ordinary command returned `CorruptData`. Both source
reclamation and validated-prefix refresh now rebind that runtime only after
checking its exact predecessor, drained state and unchanged application and
administration frontiers. A separate live-prefix regression failed before the
repair and passes after it without an intervening restart. Formats, ordering,
startup validation and public application semantics remain unchanged.

Shutdown adds the fixed numeric `riffdb-retention-source-validation-v1` line:
`initial_rows`, `continuation_rows`, `invalidations` and `admission_refusals`.
Row counts describe streamed receipt/audit rows, excluding ancillary point
lookups. Existing actual receipt, physical retirement and hold counters remain
separate. These observations never grant deletion authority.

The full storage suite ran 1,151 tests: 1,148 passed and three static architecture
assertions failed because they named the former commit/layout owners. Updating
those assertions preserves the sole engine commit, unknown-write invalidation,
sealed transaction and closed control ownership checks. All 59 architecture
tests subsequently pass. Scoped Clippy and handbook checks pass; this is not a
claim of full CI or package acceptance.

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

A deterministic native regression now changes publication after the first
history page. Previously the optional prefix scrub completed the entire stale
walk; it now stops before its second page. Structural and catalog page boundaries,
proof completion and publication also check the original exact root. Ordinary
background scrubs continue to validate their original immutable pin beside live
writes. A dedicated stale flag identifies this discard; unrelated native
`Unavailable` errors and cancellation still propagate. Ten background-proof
tests pass, including equal-history/new-publication refusal. This reduces stale
population work; it does not yet make every stable complete refresh a single
bounded lane turn or establish a required-host latency improvement.

### Promoted backup recovery: unresolved intermittent failure

Full CI timed out waiting for promoted maintenance to regain source readiness.
An isolated rerun passed, then the complete replication group passed all 25
tests in 148.51 seconds. Passing repeats do not erase the original failure or
explain it. Preserve the 30-second operation deadline and investigate native
recovery progress and test
interference before deciding whether a production repair is needed.

### Replication shutdown: additional unresolved process failure

The scoped server/storage acceptance run after native continuation observed
`replication_stream_resumes_gap_free_after_repeated_kills` failing during the
fixture's required clean shutdown. The child exited unsuccessfully; the old
controller retained only stdout/stderr byte counts, so the closed native failure
class was unavailable. Its isolated repeat passed in 114.13 seconds. That pass
does not explain or erase the original refusal.

Test fixtures can now request one bounded stdout line containing only the
existing closed daemon kind. Failed fixture shutdowns inspect that line after
the unsuccessful exit; successful shutdown and ordered lifecycle evidence
checks remain strict. A real malformed shutdown verifies `shutdown_input`,
and a source-redaction test verifies that internal error text is absent. This
instrumentation is compiled only with `test-fixtures`, does not expose an
application or operator control, and is not a production repair. Diagnose the
shutdown refusal before claiming milestone readiness.

The subsequent scoped acceptance completed 1,773 tests: 1,770 passed and three
failed (the repeated-kill shutdown and both promoted backup crash/restore
readiness waits). All eight other acceptance steps passed. The full `ci-all`
script completed successfully after starting at `08aa6e8a`, but diagnostic
changes were made during its execution; it is not a clean final-revision CI
receipt. Later diagnostic repeats pass the two promoted cases in 47.68 seconds.
Those repeats do not diagnose the failures. Receipt polling now records only
bounded started/completed/refused counts and the last closed numeric phase/kind
on the original 30-second timeout.

### Required-host continuation probe: fails composition and normal latency

An unprofiled preparation from clean committed native source `2fb963b7` ran on
the same 8-vCPU, nominal 32-GiB, persistent-SSD host. All 90,000 updates and
360,000 reads settled, with zero retries and at most 28 workload requests plus
four reserved auxiliary slots. Five-minute window p95 values were 67.691,
68.116 and 97.159 ms. The first p99 was 112.323 ms; the second and third
overflowed the 250-ms histogram bound. The final normal window had 2,661
overflows among 150,000 requests, so preparation refuses `probe_latency`.

The consumer pause was 300.048 seconds, and its 2,048-event recovery took
11.983 seconds. Live observation continued, recording 45 renewals and 89,991
updates without resetting its cursor. None of these facts waive the latency
failure or qualify the campaign. Final offline archive reconciliation was not
run after refusal.

Production shutdown reported zero receipt and extent reclamation, 26 reference-
held retirement attempts, zero reader-held attempts, and 2,418 microseconds
of receipt-gate time. The source-validation counters reported 155,651 initial
row reads, zero continuation rows, 288 invalidations and zero admission refusals.
The append-only unit regression therefore does not prove sustainable progress
in the actual writer/retention composition. Trace the invalidating native paths
and repair their proof handling before advancing the harness or starting the
24-hour run. A bounded, read-only test child can census rejecting namespaces in
the failed database's actual receipts; it emits only closed namespace names and
counts, never keys, payloads, identities or paths. It is forensic evidence,
not a passing workload, benchmark or proof of deletion eligibility.

### Native audit qualification repair

The read-only census of that failed database, using the preceding classifier,
decoded 375,364 receipts. It rejected 39,768 receipts and counted 268,100 audit
mutations; the remaining rejected namespaces were one-time bootstrap/catalog
installation. Inspection and deterministic native reproduction identified two
missing cases:

- The classifier assumed an eight-byte audit key. Native audit keys have a
  canonical prefix and nine bytes. It now uses the existing native key decoder,
  rather than an artificial key shape, for append succession.
- Logical command pruning inserts retained Service audit records where command
  segments previously provided audit through derived indexes. These inserts can
  lie below the administration frontier. The native owner now checks actual
  absence, the mutation preimage, canonical Service decoding and exact audit key.
  These rows grant no replication registration, primary fence or promotion
  evidence. Every previously validated source-control audit stays unchanged.

For historical physical command-audit locators, qualification additionally
resolves the authoritative native command segment and compares the entire
selected audit record. Wrong members, changed records, missing commands and
preimage substitutions refuse qualification. Only one existing bounded decoded
segment is held temporarily; no evidence queue or additional snapshot is retained.
Journal admission and actual checkpoint materialization both perform the checks.
Unclassified writes and source-control changes continue to invalidate the proof.

The new native composition regression retains 600 real commands and interleaves
24 writer/checkpoint/prune turns without an idle validation window. It failed
with zero receipt progress and 23 invalidations before this repair, and passes
afterward without changing its schedule or limits. A canonical audit append also
agrees with the independent complete native source validator. Targeted hostile
preimage, locator, key and decoding checks pass. This is development evidence;
the required-host preparation and full acceptance still must establish sustained
receipt/extent progress and normal latency. No 24-hour run has started.

### Required-host audit repair: receipt progress, failed normal latency

Preparation from clean committed native source `577f2445` completed all 90,000
updates and 360,000 reads on the required 8-vCPU, nominal 32-GiB, persistent-SSD
host, with zero retries. The original population, offered rates and request
limits were preserved. The three five-minute observations were:

| Window (seconds) | p95 (ms) | p99 (ms) | Requests at least 250 ms |
| --- | ---: | ---: | ---: |
| 0–300 | 92.528 | at least 250 | 3,595 |
| 300–600 | 57.066 | 74.782 | 0 |
| 600–900 | 112.198 | at least 250 | 4,601 |

The consumer outage overlapped the first two windows. Its actual pause lasted
300.050 seconds; recovery of 2,048 events took 8.824 seconds, while live progress
continued. The final window was fault-free and failed both latency limits.
Preparation therefore exited unsuccessfully with `probe_latency`; final offline
archive reconciliation was not run after refusal.

Shutdown reported 4,610 source receipts removed, 109,594 continuation rows,
434,305 initial validation rows, 315 invalidations and 109 admission refusals.
Receipt-gate work totaled 67,881 microseconds. Physical extent deletion and
recycling both remained zero, with 122 reference-held retirement attempts and
zero reader-held attempts. These are accumulated shutdown observations, not
periodic proof of physical eligibility or final-six-hour resource stability.
The audit repair restores some real continuing-load receipt progress; it does
not establish sustainable retirement or explain the remaining invalidations,
reference holds or tail latency. Preserve the failed artifact for diagnosis.

Scoped acceptance for that native change passed all seven planned steps,
including 1,159 native storage tests with two skips, formatting, Clippy, handbook,
file-size and panic-allowance checks. The three previously failing daemon recovery
tests also passed a reproduction, but the earlier intermittent failures remain
unexplained. Neither receipt qualifies a final dependency revision or full CI.

### Engine reliance review: a separate qualification blocker

The supplied read-only redb review was checked against the pinned engine and
native scrub implementation. Ordinary redb reads do not run the page-tree
checksum walk. RiffDB's offline scrub consumes the complete structural and
catalog-semantic streams but never calls `Database::check_integrity`. Native
row decoding, CRC, schema and key/identity comparisons remain necessary;
they do not prove physical-tree integrity. Query entity and stored-outcome
readers do compare decoded identities, so the review's possible wrong-record
case is not evidence that those paths omit that check.

The engine checker can repair and return `Ok(false)`. Its result must not be
treated as a successful diagnostic of an unchanged authoritative artifact.
The 4.3.0 release notes describe a recovery fix after integrity repair and new
custom-backend locking methods. The inspected manifests additionally introduce
Linux/Apple libc locking code. Upgrading therefore requires dependency, unsafe-
surface, wrapper file-lock and compatibility review, rather than just a pin edit.

ADR-0269, `adr/0269-explicit-engine-integrity-verification-and-reviewed-redb-upgrade.md`,
records private-copy physical verification, original preservation, deterministic
corruption evidence and the reviewed upgrade. At this observation the proposal
was awaiting exact acceptance and production behavior was unchanged; the
accepted implementation progress is recorded below.
The current C3D preparation of `577f2445` measures the audit/retention repair;
it cannot qualify the final dependency revision or the complete campaign.

Recurring hard-coded two-phase commits in follower, archive and retention paths
are also confirmed. Consolidating profile selection deserves a separate native
review; merging follower apply/acknowledgement would change durable protocol
boundaries and is not authorized by the dependency proposal. Preserve the journal
and record CRC: they protect different artifacts and semantic boundaries from
redb's own engine checks. Process kills also do not establish power-loss coverage.
The cited `retention.rs` helper is not the Standard online logical-prune hot path:
that transition uses the journal, followed by its existing one-phase checkpoint.
Source-receipt control writes have their own hardened boundary. Do not infer a
measured latency improvement from removing the cited helper's extra sync.

#### Isolated dependency and locking preparation

An isolated graph comparison preserves disabled default features and no enabled
redb features. On Linux, 4.2.0 has no normal dependency edge; 4.3.0 introduces
`libc`, while the cross-target graph already contains that package for WASI.
The isolated candidate can retain the workspace's existing `libc` 0.2.186.
Both engine versions require Rust 1.90 and use MIT OR Apache-2.0 licensing.
The candidate registry checksum is
`fb338a6c67830a61bed824b78c2bb034ab1ff401a20616bd7957524f4fdc2f22`.
Isolated `cargo deny` passes advisories, licenses, bans and sources using the
repository policy; isolated `cargo audit` passes. The first license check rejected
the temporary review crate's missing license declaration; declaring its actual
license fixes that fixture, without changing the policy. These checks do not
replace a final resolved-workspace review or root-lock security checks.

Source comparison finds seven added textual unsafe occurrences, all in the new
platform-gated range-lock implementation. Linux GNU uses checked lock bounds,
an initialized `flock64` and the large-file kernel syscall; Apple and other Linux
environments use their corresponding `fcntl` ABI. Windows has separate handle
and overlapped-operation ownership. These dependency internals grant no first-
party unsafe exception. Target execution and lock-failure checks remain required.

A sacrificial two-process reproduction compiles the actual current native
`CheckpointRewriteBackend` unchanged against each isolated engine version.
Under 4.2.0 the second process receives `DatabaseAlreadyOpen`; under 4.3.0 it
opens the same file successfully. The constructor no longer acquires the lock,
and the unchanged wrapper inherits unsupported locking methods, which the
engine permits in exclusive-writer mode. This proves why every file-backed
wrapper must delegate the new methods before upgrading. It is upgrade-hazard
evidence, not a claim that the current 4.2.0 production writer admits two owners.
At that isolated reproduction the production pins and scrub behavior remained
unchanged pending ADR-0269 acceptance.

PR note: Package WP-825 (open); Tier internal documentation; Behavior: disclose
the physical-verification gap, isolated upgrade-lock hazard and failed continuing-
load preparation with actual receipt progress; Checks: engine/native source and
isolated graph inspection, two-process wrapper reproduction, isolated dependency
policy/security checks, scoped documentation acceptance and separately recorded
native acceptance; Compatibility: production code, dependency pins, formats and
interfaces unchanged; Updated handbook pages: known limitations and native
findings; Hazards/follow-ups: exact ADR-0269 acceptance and implementation,
unresolved retirement, latency and intermittent daemon failures, final-hash full
CI and rehearsal gates.


### Accepted engine-integrity repair: implementation progress

The maintainer accepted the exact ADR-0269 text on 2026-10-05. The standalone
acceptance commit records that response and widens the implementation scope.
Production pins and the simulator's existing test-only pin now select exactly
redb 4.3.0 with default features disabled. The root graph retains libc 0.2.186;
root dependency-policy and advisory checks pass. Every existing file-backed
wrapper delegates all six new locking methods. A native two-process test proves
exclusion and release for ordinary, dynamic-media and external-kill owners.

A deterministic corruption regression first demonstrated the missing guarantee:
changing an engine leaf byte covered by its checksum left the exact canonical
contract envelope readable, and the old scrub incorrectly returned success.
The repaired scrub captures one fully allocated private copy under original-file
custody, verifies its bytes against the original, invokes the engine checker with
repair refusal, and runs the unchanged complete exact-end logical validator on
that same copy. Native journals are validated through the existing recovery plan
without replay or anchor mutation. Pending recovery refuses a partial receipt.
No original primary, marker, journal or extent is opened for engine repair or
published from the verification copy.

Targeted native evidence passes corruption refusal, full allocator-repair
refusal, healthy repeated scrub with byte preservation, cancellation, capacity
refusal, substituted source binding, unknown scratch and original-hardlink
refusal, and process-exit cleanup on the next scrub. An intermediate targeted run passes
79 native/simulator architecture, physical, journal, wrong-key and genuine
predecessor-fixture tests after extracting
the affected code into focused modules without raising file-size limits.
The first genuine 4.2.0 source and offline backup were generated before the pin
changed; current open, physical scrub and restore pass the complete native
validator. That fixture has a compiler-backed catalog and administration history
but no command outcomes. A second fixture was generated in an isolated accepted
predecessor checkout with the exact 4.2.0 pins, two complete native commands and
public point-read outcomes. Repeated current open, physical/logical scrub and
backup restore preserve both semantic outcomes and their exact canonical bytes.
This Hardened fixture has no source side journal; verification follows the
existing optional-journal recovery rules, validates every journal that exists,
and refuses a damaged journal without rewriting it.

Two simulator cases reuse existing traced positional writes and sync outside
engine transactions to represent deterministic durable leaf damage and an
unsynced mutation whose actual recovery counter proves it was dropped. Both
replay identical trace hashes. These add no RNG, seed-corpus or trace-format
change. The existing native wrong-key entity test remains a typed corruption
refusal and now names its REC-006/REC-008 proof tags.

An expanded scoped acceptance run passed all eleven steps, including 1,257
native/simulator/outbox tests with ten existing skips; it preceded the final
module extraction and compatibility additions. The extracted architecture and
new behavior have targeted passing evidence. The final-source scoped run then
passed all eleven steps, including 1,262 tests with ten existing skips and the
generated-reference, handbook, workspace, dependency and architecture checks.
Full CI remains required; scoped acceptance is not campaign qualification.
The earlier preparation's zero physical extent retirement, fault-free latency
failure and intermittent daemon failures still require native diagnosis and
repair. No workload, retirement hold, journal, sync boundary, commit profile or
qualification gate is weakened; the 24-hour campaign has not started.

PR note: Package WP-825 (open); Tier guarantee under accepted ADR-0269;
Behavior: add private-copy physical verification to selector-free offline scrub,
preserve original artifacts, retain exact-end logical and native journal proofs,
and delegate reviewed engine locks; Checks: targeted native and simulated-media
regressions, root dependency/advisory checks and final scoped acceptance (1,262
tests, all eleven steps); Compatibility: genuine predecessor source/backup fixtures, unchanged
RiffDB formats and interfaces, no downgrade claim; Updated handbook pages:
startup integrity and known limitations; Hazards/follow-ups: full compatibility
and crash evidence, full CI, unresolved native retention
and latency, final-host preparation and the complete short rehearsal.

### Primary creation directory publication

The engine review's directory-publication gap is reproducible independently of
the workload. Two native real-file regressions first failed: ordinary source
open performed no directory sync after engine creation, and an injected failure
at that missing boundary could not refuse the open. Hardened databases may have
no source side journal, so a later journal sync is not a durability proof.

Ordinary source open now uses the existing media port to synchronize the parent
after the engine file exists, before exposing a store. Every ordinary reopen
repeats the check, preventing a failed first publication from being bypassed by
the leftover primary and CURRENT marker. Both native regressions pass, including
two failed attempts followed by successful initialization and identity-preserving
reopen. Follower and private-artifact publication keep their existing custody
owners. This is a directory-operation/failure regression, not a claim that
process kill simulates loss of unsynchronized directory entries. Broader
acceptance and final-host qualification remain outstanding.

The first simulator suite passes 57 of 58 tests; the corpus replay refuses a
stale historical displacement receipt after the new sync changes the seeded
operation stream. The existing diagnostic audit completes every corpus entry
with the state oracle holding: two historical coordinates return for a third
time, one returns for the first time, and two restored coordinates move away.
All successor witnesses still cover the same predicates and the retired
commit-present window remains absent. The movement receipts must name this
repair's exact commit before the corpus can pass. No seed, generator, fault
model, predicate, retirement or successor will be removed or weakened.

The repair is committed as `f07a5ae0`. Append-only corpus receipts now record
those five movements against that exact causal revision, adding a third-return
slot that requires all preceding receipts. The existing history-preservation
test covers that slot; the full corpus replay and deliberately drifted-pin
refusal pass (five targeted tests, two existing ignored diagnostics). All 59
native architecture tests and 15 native creation, scrub, predecessor and lock
tests also pass. Full scoped acceptance for the repair and receipts then passes
all seven steps and 1,233 native/simulator tests, with ten existing skips. Full
CI and required-host qualification remain outstanding.

PR note: Package WP-825 (open); Tier guarantee within existing durable-write
requirements; Behavior: require ordinary primary-directory synchronization
before store admission, preserving typed failure on retries; Checks: two native
regressions fail before the repair and pass afterward; Compatibility: unchanged
formats, profiles and interfaces; Updated handbook: startup integrity;
Hazards/follow-ups: broader acceptance, actual continuing-load reclamation and
latency diagnosis, full CI and the complete short rehearsal.

### Native prune publication followed by an outbox drain

The asynchronous checkpoint already qualified canonical command-audit
materialization using actual preimages. Its synchronous live journal-drain
counterpart instead classified the resulting receipt after applying it. A
canonical historical Service audit below the administration frontier therefore
invalidated source continuation even though no source-control evidence changed.
This is a native lifecycle defect, independent of campaign scheduling.

A new regression uses the genuine predecessor command fixture, complete native
structural/catalog validation and real outbox transitions. It completes only
the first event's controlled transition to release that hold, publishes one
logical prune and claims the second event, forcing the actual live drain. The
unrepaired main checkout fails: its source-continuation epoch advances from one
to two. This transition fixture does not qualify archive delivery or campaign
evidence and is not a rehearsal workaround.

The live drain now qualifies the canonical receipt before changing any rows.
It shares the checkpoint's closed audit proof: exact actual preimage hash,
canonical Service record and key/frontier agreement, and, for a locator,
the exact command and audit member from one bounded decoded native segment.
Changed physical audit or source-control evidence, missing command evidence,
malformed values and unknown writers continue to invalidate qualification.
Normal recovery still uses the unchanged materialization path. Ordered source
mutations retain all existing preimage checks, including cancelled net changes.
There is no additional mutation queue or snapshot pin.

The repaired main regression passes and independently checks complete retained
history, unchanged resume/frontiers, exact canonical outcomes and provenance.
Seventy targeted native continuation, journal and architecture tests pass.
Final scoped acceptance passes all seven steps and 1,176 native tests, with
the two existing skips. Full CI then passes on clean revision `bf067f23`,
including operator acceptance, generated artifacts and source-install smoke.
All three previously intermittent daemon tests pass in that run; their earlier
failures remain unexplained. No measured cause is
assigned to the previous preparation's invalidation count or latency failures;
the required-host run must measure this repair's effect.

PR note: Package WP-825 (open); Tier guarantee under accepted ADR-0268;
Behavior: preserve qualified source continuation through a preimage-checked
live prune drain; Checks: native regression fails before and passes after the
repair, plus 70 targeted tests; Compatibility: unchanged durable formats,
interfaces, ordering, profiles and hold rules; Updated handbook: known
limitations; Final scoped checks: all seven steps and 1,176 tests pass;
Hazards/follow-ups: unresolved physical retirement and latency, required-host
preparation and the complete short rehearsal. The 24-hour campaign has not started.

### Required-host preparation of the complete repaired revision

Clean source `bf067f23` passes full CI and a locked release build on the required
8-vCPU, nominal 32-GiB, persistent-SSD host. The unchanged 900-second preparation
uses 64 organizations, 65,536 seeded tickets, 100 commands/s, 400 reads/s,
28 load slots plus four auxiliary slots, live observation, a real consumer
outage and online retention. Its three windows report:

| Seconds | Completed commands | Completed reads | p95 ms | p99 | Histogram overflow |
| --- | ---: | ---: | ---: | --- | ---: |
| 0–300 | 29,800 | 119,308 | 194.345 | beyond range | 7,172 |
| 300–600 | 30,200 | 120,692 | 63.580 | beyond range | 2,795 |
| 600–900 | 30,000 | 120,000 | 169.229 | beyond range | 6,960 |

Every window offered 30,000 commands and 120,000 reads. All 90,000 commands and
360,000 reads eventually complete with zero retries, but work spills from the
first window into the second. The preparation correctly refuses `probe_rate`.
The final window is fault-free and also independently fails both latency gates.
The overflow sentinel is not a measured microsecond percentile. State
reconciliation completes; final offline archive reconciliation is not reached.

The consumer pauses for 300.002 seconds, recovers 2,048 events in 9.979 seconds
and reports continuing live progress. Final native counters show 9,730 reclaimed
source receipts and 1,099,347 microseconds of receipt-gate work; initial
validation streams 10,103 rows, continuation 408,771 rows, with one invalidation
and 674 admission refusals. Physical deletion and recycling remain zero, with
91 reference-held and one reader-held attempt. Logical pruning advances 121,959
sequences over 555 steps. These accumulated shutdown counters do not identify
the admission-refusal causes or establish continuing physical eligibility.

This revision establishes native proof continuation and integrity checks, not
sustainable service levels or retirement. The source barrier's existing checks
currently merge busy/queued writers, pending journal data, checkpoint/sealing
publication, changed qualification and oversized catch-up into one refusal
counter. A diagnostic-only continuation of WP-825 separates those actual native
checks into five bounded numeric counters. It preserves the original aggregate
line, adds a private diagnostic line, and changes no admission, pacing, hold,
durability, ordering or application interface. Deterministic tests check refusal
state preservation, bootstrap-hold preservation and per-step counter reset;
maximum-value formatting stays bounded and payload-free. Fifteen targeted
tests pass, followed by all seven scoped acceptance steps and 1,802 tests,
with the ten existing skips. The checkpoint-sealing fixture also checks the
exact refusal cause. Full CI and required-host measurement of this diagnostic
revision remain pending. No latency or retirement repair is claimed.

PR note: Package WP-825 (open); Tier guarantee within accepted ADR-0268;
Behavior: distinguish closed native source-admission refusal causes;
Checks: prior complete source passes full CI; actual required-host preparation
fails rate, normal latency and retirement; 15 targeted tests and all seven
scoped checks pass, including 1,802 tests with ten existing skips;
Compatibility: unchanged public protocols, durable formats and source admission;
Updated handbook: known limitations; Hazards/follow-ups: identify and repair
native admission/retirement and latency limitations, qualify the final source,
then complete the short rehearsal. The 24-hour run has not started.


### Native retry after a temporary extent barrier

Read-only investigation of the preserved failed preparation finds five sealed
files while the active generation is 30,285. Their indexes have valid SHA-256
checksums, but this inspection does not validate every native frame or qualify
campaign evidence. The native reclaimer probes at most 64 generation names per
pass and advances its cursor before checking reader and writer barriers.
A temporary global barrier can therefore defer a candidate across thousands of
absent names even after that barrier clears.

An isolated actual-transition regression reproduces this delay. It opens the
genuine two-command fixture, applies real outbox claim/completion, native
logical prune and legitimate source receipt reclamation, then seals the drained
prune extent early and advances empty generations through the existing media
protocol. With a real old reader retained, the file remains intact. After
release, the unchanged reclaimer fails the next-pass retirement assertion.
This controlled early-sealing fixture is not campaign evidence or an application
workaround. No acknowledgement or source checkpoint is invented.

The repair retains the candidate only for global reader, scrub, writer or
publication barriers. Actual surviving authoritative references still advance
the cursor so other candidates are visited. Every retry repeats the existing
reference, frontier, resume-anchor, reader, exact-key and full-frame checks.
The pass remains off-gate, probes at most 64 names and retires at most one file;
no durable state, allocator, hold, ordering or public protocol changes.

The reader regression passes after repair. Expanded deterministic coverage
checks old application snapshots, real scrub pins and the existing writer
lease: repeated blocked attempts preserve the file and durable epoch, followed
by next-pass retirement after release and unchanged complete retained history.
All 38 focused tests pass, including existing corruption refusals and the
seven process-crash retirement/recycle windows. All seven scoped acceptance
steps pass, including 1,178 native tests with two existing skips.
The diagnostic preparation on clean `9a5d2e89` is separate and excludes this
repair; full CI and required-host qualification of the final source are pending.

PR note: Package WP-825 (open); Tier guarantee within accepted ADR-0263
Amendment 4 and ADR-0265; Behavior: retry an otherwise eligible extent after a
temporary global barrier without rescanning absent generations; Checks:
reader regression fails before and passes after repair; all 38 focused tests
pass; all seven scoped checks pass, including 1,178 tests and two existing
skips; Compatibility: unchanged durable formats, public interfaces, holds
and retirement boundary; Updated handbook: known limitations; Hazards/follow-ups:
full CI, continuing-load retirement and latency, then the complete short
rehearsal. No 24-hour run starts.


### Required-host source-admission observations

A separate unchanged 900-second diagnostic preparation on clean `9a5d2e89`
completes all 90,000 commands and 360,000 reads without retries. Its final
fault-free window fails latency; the preparation exits with `probe_latency`.
State reconciliation completes, but archive reconciliation is not reached after
the failed gate. This diagnostic excludes the native extent-retry repair above
and cannot qualify WP-825 or the 24-hour campaign.

| Window (seconds) | Commands | Reads | p95 (microseconds) | p99 | Overflow |
|---|---:|---:|---:|---|---:|
| 0–300 | 30,000 | 120,000 | 176,265 | beyond range | 7,320 |
| 300–600 | 30,000 | 120,000 | 57,806 | beyond range | 1,784 |
| 600–900 | 30,000 | 120,000 | beyond range | beyond range | 7,968 |

The first window overlaps the planned consumer outage; it is not a normal
service-level sample. Beyond-range percentiles are overflow sentinels, not
measured durations. All windows retain 28 load slots and four auxiliary slots.

Native shutdown counters record 8,962 removed source receipts and one recycled
extent, with no deleted extents. Reference-held attempts are 91 and reader-held
attempts one. The source validator visits 10,700 initial rows and 403,614
continuation rows, with one invalidation and 657 admission refusals. The new
closed cause counts are 204 busy writers, 363 pending-journal refusals, one
checkpoint publication, zero changed qualifications and 89 catch-up page bounds.
The two diagnostic emission boundaries repeat the same counters; they are not
added together. Actual recycling is native evidence; disappearance of a file
alone is not evidence of retirement.

The consumer pauses for 300.050 seconds while live progress continues, then
reconciles its 2,048 organization-scoped events in 7.670 seconds. This count
does not represent aggregate archive traffic across all organizations.

These observations identify the checks that refused admission, not the cause
of all retained receipts or slow command tails. Current source and derived
holds, the independently durable checkpoint ceiling, and actual eligible
history need bounded periodic observation before changing admission or holds.
The failed preparation and its private roots remain preserved. Full CI on the
final source, sustained retirement, service levels and the complete short
rehearsal remain outstanding. No 24-hour run has started.


### Global-barrier repair: full CI passes; required-host latency still fails

Clean source `f92548bc` passes full CI, including the source-install checks,
with unchanged source throughout the run. Its separate locked release build
and unchanged 900-second preparation use the required 8-vCPU AMD EPYC 9B14,
nominal 32-GiB host with a 500-GiB persistent SSD-backed disk. This source
excludes the later allocation work and the directory-discovery repair below.

The preparation completes 90,000 commands and 360,000 reads without retries,
but exits with `probe_latency`. State reconciliation completes; final archive
reconciliation is not reached after the failed latency gate.

| Window (seconds) | Commands | Reads | p95 (microseconds) | p99 | Overflow |
|---|---:|---:|---:|---|---:|
| 0–300 | 30,000 | 120,000 | 218,560 | beyond range | 7,420 |
| 300–600 | 30,000 | 120,000 | 59,863 | beyond range | 1,784 |
| 600–900 | 30,000 | 120,000 | 105,631 | beyond range | 6,053 |

The first window overlaps the planned consumer outage. Beyond-range values
are histogram overflow sentinels, not measured durations. Every window retains
28 load slots and four auxiliary slots. The actual consumer pause lasts
300.049 seconds; live progress continues, and recovery reconciles its 2,048
organization-scoped events in 9.076 seconds.

Native shutdown records 8,194 removed source receipts, 761,933 microseconds of
receipt-gate work and one recycled extent, with no deleted extents. Reference
holds are 95 blocked attempts; reader holds are zero. Validation visits 10,688
initial and 406,363 continuation rows, with one invalidation. The 666 admission
refusals comprise 205 busy writers, 383 pending-journal cases and 78 catch-up
page bounds; checkpoint and changed-qualification refusals are zero. Logical
pruning advances 121,850 sequences over 572 steps. Repeated shutdown emission
boundaries report the same counters and are not added together.

The earlier diagnostic preparation also recycled one extent. These runs do
not establish a retirement or latency improvement from the global-barrier
repair. Actual source ceilings and continuing physical eligibility remain
unmeasured. Failed preparation roots and receipts remain preserved privately;
WP-825 and the 24-hour campaign remain unqualified.

### Native extent discovery after legitimate source-reference release

Extending the genuine two-command fixture reproduces a separate defect. A
sealed extent is correctly retained by its actual source receipt references.
After legitimate native source reclamation releases those references, the
numeric cursor still walks absent generation names and misses next-pass
retirement. The failure is below the harness: no workload change or invented
checkpoint is involved.

The repair advances through actual directory entries instead of numeric
names. Each pass advances at most 64 entries and performs at most one existing
extent operation. One process-local directory handle and one globally blocked
candidate replace the numeric cursor; no population-sized file list, durable
identity or mutation authority is added. Exact filename parsing selects only
this database's older sealed generations. Every original authoritative
reference, reader, footer, frame, source-anchor and file-retirement check stays
in place, outside the ordered writer gate and under existing maintenance pacing.
Iterator errors refuse progress rather than become successful end of history.

The hidden media seam now owns read-only directory iteration for both real and
simulated media. The simulator uses bounded ordered namespace lookups, traces
open/advance/refusal operations and refuses stale handles after recovery.
Its directories remain implicit in descendant files; these tests establish
namespace iteration behavior, not directory power-loss durability.

The source-release regression fails before and passes after repair, alongside
the existing reader, scrub and writer cases. Five focused native tests check
bounded discovery, directory-error refusal and existing reader reservation.
Three simulator tests establish real sibling-name agreement, deterministic
traces, stale-handle refusal and missing/non-directory refusal. The daemon
restart interrupts the first scoped run before a completion receipt; that
partial log stays preserved and is not counted as a pass. The fresh run passes
all seven scoped checks and all 1,241 native/simulator tests, with ten existing
skips, including the existing corruption and process-crash coverage.
Final-source full CI and continuing-load measurement remain pending.

PR note: Package WP-825 (open); Tier guarantee within accepted ADR-0263
Amendment 4 and ADR-0265; Behavior: bound discovery by actual directory entries
so released references do not defer retirement across generation holes;
Checks: source-release RED/GREEN, five native unit tests, three simulator tests,
all seven scoped checks and 1,241 tests with ten existing skips;
Compatibility: unchanged application interfaces, durable formats, holds,
ordering and retirement validation; Updated handbook: known limitations;
Hazards/follow-ups: full CI, required-host measurement,
latency and continuing retirement, then the complete short rehearsal.
No 24-hour run has started.


### Actual source-reclamation ceilings

The source owner now observes the existing native floor plan at the same exact
drained barrier. The bounded sample records the retained resume position,
current receipt tail, previously observed durable checkpoint, oldest derived
source pin, oldest active durable source hold, uncapped minimum ceiling and
active durable hold count. Presence bits distinguish absent holds from numeric
fences. The capped deletion still removes at most 256 receipts and applies all
original checks and writes. The sample is planning evidence, not a deletion
receipt, validated file inventory or authority to release any hold.

Separate closed counters distinguish the first observation, an unusable prior
observation or insufficient independent progress, and a floor held at or below
the retained resume position. Existing source-admission cause counters stay
separate. No new scan, durable identity, mutation, writer, application API,
admission opportunity or maintenance-pacing rule is introduced.

The server keeps the last measured plan and the attempt on which it was
observed; missing plans do not replace it with zeros or present it as fresh.
A payload-free private diagnostic line is emitted at most once per minute
while the retention lane runs, and at the existing shutdown boundaries. An
unavailable sample stays explicit. The numeric observation can identify
reclaimable receipt backlog and legitimate source fences without inferring a
resume position from the accumulated deletion count. Periodic file eligibility
and complete campaign resource observations remain separate required work.

An actual native fixture test checks combined derived pins and each of the
three durable hold kinds, independent publication, repeated observations and
held floors. All 26 native source-control tests pass, including the existing
256-receipt limit and retained-reader checks. New server checks cover sample
age, replacement rather than summed fences, missing observations and the
maximum-value output bound; both server tests pass. Scoped formatting, Clippy
and all 1,806 native/server tests pass with ten existing skips. The isolated
checkout initially lacked the pinned handbook tools on its PATH; with the
installed tools made available, the unchanged handbook check passes. Final
documentation and governance checks cover the recorded results.
The required-host preparation on `f92548bc` excludes this diagnostic change.

PR note: Package WP-825 (open); Tier guarantee within ADR-0265 and ADR-0268;
Behavior: observe the actual native source-reclamation ceiling and distinguish
its measurement age from continued attempts; Checks: 26 native source-control
tests and both new server tests pass; scoped formatting, Clippy, 1,806 tests
and handbook validation pass; Compatibility: unchanged public
application protocols, durable formats, holds, ordering and admission; Updated
handbook: known limitations; Hazards/follow-ups: complete validation and measure
actual eligible history and writer-stage costs under continued load. The full
short rehearsal and 24-hour campaign remain unqualified.


### Required-host source ceilings: unheld ranges outpace native removal

Clean diagnostic source `645cda9f`, with a locked release build, completes the
unchanged 900-second preparation on the required host. This source includes
floor observations and the global-barrier retry; it excludes later allocation,
actual-directory discovery and outbox-stage instrumentation.

| Window (seconds) | Commands | Reads | p95 (microseconds) | p99 (microseconds) | Overflow |
|---|---:|---:|---:|---:|---:|
| 0–300 | 30,000 | 120,000 | 101,138 | beyond range | 5,918 |
| 300–600 | 30,000 | 120,000 | 55,541 | 88,550 | 892 |
| 600–900 | 30,000 | 120,000 | beyond range | beyond range | 8,185 |

All 90,000 commands and 360,000 reads complete without retries, with 28 load
slots and four auxiliary slots. The first window overlaps the planned outage;
the final fault-free window fails both latency limits. Beyond-range values are
histogram sentinels. The preparation exits with `probe_latency`; independent
state reconciliation completes, but final archive reconciliation is not reached.
The consumer pauses for 300.050 seconds while live progress continues, and
recovers its 2,048 organization-scoped events in 7.972 seconds.

Every available measured floor plan records no derived or durable source hold.
At attempt 1,337, the source resume is 8,451 and its prior checkpoint ceiling
219,562. The final pre-deletion plan, measured at attempt 2,404 of 2,437,
records resume 8,707, ceiling 219,782 and tail 366,549. These are exact planning
positions, not final post-deletion state, a physical inventory or authority to
delete the whole interval. The unheld plan permits a range exceeding 211,000
positions beyond the resume, while native removal totals only 8,962 receipts.
A large unheld range therefore remains; source-admission progress needs a
native repair, not a workload pause or classification as protected history.

The owner visits 10,793 initial and 403,942 continuation rows, with one
invalidation and 834 admission refusals: 391 busy writers, 316 pending journals
and 127 catch-up page bounds. Other admission causes, unusable prior progress
and held-floor yields remain zero; the first-observation counter is one.
The snapshot's 33-attempt measurement age is explicit. Shutdown emission
boundaries repeat counters and are not added together. Native physical work
records one recycled extent, zero deletions, 102 reference-held attempts and
27 reader-held attempts; its total work duration is 14,816,211 microseconds.
These counters establish neither sustained retirement nor the cause of latency.

ADR-0268's nonwaiting drained admission gives the source owner no FIFO ticket.
The gate correctly refuses it whenever another writer is held or queued, and
published journal data independently refuses entry. Revisiting that accepted
scheduling boundary requires an exact architecture decision. The source,
failed preparation, complete floor stream and native counters stay preserved;
WP-825 and the 24-hour campaign remain unqualified.

### Native outbox writer-stage observations

The native outbox claim and completion path uses the sole ordered writer. Each
transition can publish an acknowledged journal prefix, wait for checkpoint
installation, take and materialize the published suffix, then complete its
engine transaction and durable source publication. These are authoritative
storage operations below the application boundary. Aggregate command/read
shutdown timings cannot attribute their interference with command tails.

A graph-owned census observes eight existing native stages: gate wait, prefix
publication, checkpoint wait, journal take, engine open, suffix application,
held begin work and commit completion. Each stage retains only attempted-stage
count, cumulative and maximum microseconds. Its fixed atomic storage is at
most 256 bytes. Other attributions do not activate clocks or change these
counts. Held begin overlaps its component stages; live atomic snapshots are
diagnostic observations, not coherent completion receipts or a partition of
wall time. An empty suffix phase still counts as a measured phase attempt.

The owned local outbox emits one closed numeric private line at most once per
minute between dispatch pages, and once when its worker retires. Diagnostic
I/O follows release of native writer leases; failed output changes no delivery
result, acknowledgement or readiness. No event identity, payload, tenant,
path, capability or machine identity is recorded. The original dispatcher,
archive durability, profiles, writer order, checkpoints and source admission
are unchanged. This observes costs; it authorizes no transaction batching or
sync-boundary removal.

A genuine native fixture first fails when its two real claim/completion writes
produce no attributed observations. It passes after instrumentation while
preserving exact statuses, source qualification and retained history. Other
administrative operations leave the outbox census unchanged. Two deterministic
counter tests cover concurrent publication, saturation, graph/stage isolation
and inactive attributions; a server formatter test covers closed labels and
maximum numeric output. The final native rerun passes; all seven scoped checks
pass, including 1,809 tests across 25 binaries with 10 existing skips, Clippy,
formatting and handbook generation.
Required-host measurement excludes this new instrumentation until its source
and locked artifacts are explicitly installed.

PR note: Package WP-825 (open); Tier guarantee within accepted ADR-0265 and
ADR-0268; Behavior: observe actual native outbox writer stages with fixed
metadata and payload-free diagnostics; Checks: native RED/GREEN and three
focused counter/formatter tests, 1,809-test scoped acceptance; Compatibility: unchanged application
protocols, durable formats, attribution, admission, holds and ordering;
Updated handbook: known limitations; Hazards/follow-ups:
required-host writer-cost measurement, source-admission progress, normal
latency and the complete short rehearsal. No 24-hour run has started.


### Combined source validation and required-host preparation

Frozen clean source `819fdfbd` passes the full `ci-all` battery, ending with
verified source installation. This combines the actual-directory reclaimer,
source-floor and outbox-stage observations, and the independently merged
allocation repairs. The result qualifies that source's correctness checks;
it does not qualify the host workload or the subsequent ADR-0271 implementation.

The unchanged 900-second preparation, built from locked release artifacts on
8 vCPU, 32 GiB Linux with persistent SSD storage, again fails `probe_latency`.
It completes all 90,000 commands and 360,000 reads without retries, using
28 load slots and four auxiliary slots. Independent state reconciliation
completes; final archive reconciliation is not reached after the latency gate.
No 24-hour qualification has started.

| Window (seconds) | p95 (microseconds) | p99 (microseconds) | Overflow |
|---|---:|---:|---:|
| 0–300 | 106,222 | beyond range | 6,888 |
| 300–600 | 57,878 | beyond range | 1,784 |
| 600–900 | beyond range | beyond range | 7,852 |

The first two windows overlap the measured consumer pause or its recovery;
the pause lasts 300.049 seconds. Live progress continues; recovery takes 6.568 seconds and covers
2,048 organization-scoped events in 308 samples. Those events are not a global
archive count. The wholly fault-free final window fails both p95 and p99.
Beyond-range values are histogram sentinels, not measured quantiles.

Native receipt removal totals 12,802 and actual receipt gate residence totals
1,348,381 microseconds. Physical work records one recycled extent, zero deleted
extents, 1,452 reference-held attempts and 15 reader-held attempts, with
12,570,077 microseconds of work. The complete source walk reads 10,629 initial
and 411,154 continuation rows. Its 835 admission refusals comprise 394 busy
writers, 304 pending journals and 137 catch-up page bounds, with one proof
invalidation. Duplicate shutdown emissions are not added.

The last available floor sample is attempt 2,435 of 2,466: resume 12,547,
prior checkpoint ceiling 355,534 and tail 376,473. Every measured sample
reports no derived or durable source hold. The last plan permits 342,987
positions beyond resume; its 31-attempt age remains explicit. This is a
pre-deletion plan, not final state, a physical inventory or permission to
remove the interval. The backlog remains a native admission/progress problem.

The final outbox worker-retirement stage sample reports 311,072 attempts for
each stage. Values are cumulative whole-run observations, including preparation
and post-load work; successive samples are not summed.

| Stage | Total microseconds | Maximum microseconds |
|---|---:|---:|
| Gate wait | 21,370,601 | 4,026,084 |
| Prefix publication | 13,599,149 | 338,352 |
| Checkpoint wait | 52,314 | 35,882 |
| Journal take | 573 | 15 |
| Engine open | 280,922 | 397 |
| Suffix application | 61,824,652 | 87,130 |
| Held begin | 97,634,199 | 351,553 |
| Commit completion | 374,436,207 | 318,987 |

Held begin overlaps its component stages, so this table is not a partition of
writer time. Checkpoint waiting is rare in this aggregate; commit completion
and materialization remain substantial costs. These observations do not locate
individual normal-window spikes or authorize batching, profile or sync changes.

### Accepted fair source admission implementation

The maintainer accepts exact ADR-0271 version `9d70c483` on 2026-10-05. Its
standalone acceptance commit records that approval. The implementation gives
the graph's sole source owner one fixed FIFO ticket behind existing writers,
with a compiled 100-ms admission deadline and irreversible graph retirement.
Grant, expiration and cancellation resolve under the gate mutex. A cancelled
slot remains fixed until its turn is consumed; an ordinary successor cannot
be stranded, and another source attempt cannot allocate a second slot.

A granted turn inspects existing metadata without initializing a journal lane.
It checks for unfinished checkpoint or extent sealing before attempting
root capture, then yields without waiting for native publication. A separate
regression exhausts the actual 4,096 reader-pin capacity with sealing held:
the earlier ordering wrongly attempts another pin; the revised ordering yields
and leaves ordinary writers available. Owner retirement releases its actual
registered validation pin, not just its bookkeeping flag. Only a complete
acknowledged suffix with exact native accounting of at most 256 transitions and
1 MiB may use the unchanged canonical checkpoint/drain machinery. Unknown,
unpublished or excessive work yields before taking the suffix. Raw scanner
bounds precede payload allocation and decoding, while existing frame, checksum,
canonicality, preimage and final-frontier checks remain active. Ordinary recovery
keeps its previous larger limits. No durable format or sync boundary changes.

Initial capture authorizes no removal and returns before a second source turn,
allowing the owner to apply its existing rest discipline. The complete walk
stays outside the gate. Admission of a completed candidate rechecks lineage,
qualification, the exact closed materialization footprint, every hold and the
existing independent-checkpoint rules. Queue waiting is separately observed;
full actual lease residence, including drain and error cleanup, is charged to
existing pacing. Owner retirement cancels pending eligibility and releases
retained validation pins after in-progress atomic work finishes.

A genuine predecessor fixture provides a semantic RED/GREEN: the predecessor
leaves an acknowledged published prune undrained, whereas the repair durably
materializes it while removing no source receipt and preserving the exact
original outcomes, provenance and source frontier. The fixture explicitly
settles the existing extent sealer outside admission; production still yields
to unfinished sealing. Deterministic gate tests cover FIFO order, expiry,
grant-versus-retire in both orders, dropped requests, fixed capacity and ticket
exhaustion. Scanner and accounting tests exercise exact byte/transition limits,
oversized raw headers, extra padding and uncertain/unpublished counts. Process
crash tests cut before commit, after commit and after anchor publication and
check complete recovered native authority before startup lifecycle receipts.

PR note: Package WP-825 (open); Tier guarantee, accepted ADR-0271; Behavior:
one cancellable bounded FIFO source turn and exact small native drain, with
separate queue/residence diagnostics and prompt owner retirement; Checks:
103 focused native source tests, including three process crash cuts, pass;
52 focused server tests, five drain/cancellation cases and the complete
26-test source-control suite pass. The first scoped run passes six checks and
fails three fixtures that assumed capture and validation shared a turn; those
fixtures now observe the proved first checkpoint explicitly and preserve every
exact hold, crash and no-self-stimulation assertion. Final scoped validation
passes all seven checks and all 1,831 tests across 25 binaries, with ten existing
skips; Compatibility: unchanged application
protocols, ordering, acknowledgement, durable formats, hold authority and fsync
boundaries; Updated handbook: known limitations and sustainable milestone;
Hazards/follow-ups: final-source full CI, unchanged required-host latency
and sustained retirement, all lifecycle faults and complete short rehearsal.
Acceptance of the decision does not qualify a rehearsal or start the 24-hour run.

## Source-progress host repetition and stopped maintenance

The native `6a6128fb` required-host consumer-outage preparation completed all
90,000 commands and 360,000 reads with zero retries, reconciled every ticket and
155,536 archived events, and measured p95 56,425 us / p99 77,406 us in the final
600–900-second normal window. The consumer resumed after its five-minute pause
and recovered in 8.674 seconds. Queried projection observations were collected.

The native shutdown census independently reports terminal retention refusal,
54 attempts, zero logical steps, zero source receipts removed and zero physical
extents retired. The file observer saw four sealed generations and no observed
disappearance. These are failed lifecycle observations, despite the preceding
controller's zero exit. They cannot demonstrate sustainable reclamation or
attribute the latency improvement to ADR-0272's deletion progress policy.

The collector regression now rejects refused or missing/malformed terminal
maintenance evidence while preserving measured file observations. All 62
checker/preparation tests pass. A separate 900-second sink-outage diagnostic
uses the same native binaries and the frozen `15f133c7` memory-observation
controller. It aborted around 241 seconds into load with a stack overflow in the
native retention thread, before completing a load window or final reconciliation.
An isolated storage test independently reproduces recursive destruction of the
supported publication-lineage depth on the existing maintenance stack. Iterative
retirement passes the storage unit suite; the exact host abort call path and
required-host repair validation remain pending.
The daemon also captured its retention census before joining the owner, which
could reject a healthy lane's live idle/fenced state. A failing real-worker
regression now proves that the owner records its actual terminal state after
join and preserves refusal. Six focused all-feature worker tests, all 633
scoped server tests and 13 acceptance checks pass in an owned temporary root.
An earlier interfered run fails six tests and is retained: PID-based scratch
sweeping can remove another runner's live files across PID namespaces sharing
temporary storage. The isolated rerun changes no native assertion or deadline.
Final-source CI and host validation remain pending.
Native refusal diagnosis, complete resource evidence and the continuous short
rehearsal remain required. No 24-hour run has started.

## Final native CI and shared-scratch regression

Frozen revision `f7402c87` passes full CI, including release source installation
and the public maintenance background-drain crash/recovery regression. The
required host built the same clean native/controller revision and started only
the unchanged 900-second sink-outage diagnostic. Its source and binary hashes
are retained privately. No 24-hour run has started, and the diagnostic is not
the complete short rehearsal.

The shared testkit scratch regression independently fails because cleanup
deletes a fixture with unknown PID-namespace ownership. Linux construction and
sweeping now use the actual kernel namespace in the scratch family, retaining
foreign/unknown ownership while preserving dead-session cleanup within the
current namespace. This test-infrastructure repair does not relax a native
assertion, deadline or campaign gate. The earlier archive-publication failure
remains unexplained; its storage-local scratch implementation was already
namespace-isolated. All 42 testkit tests and all 13 scoped acceptance checks
pass. Full CI on frozen integrated testkit revision `4fbb44a5` also passes,
including release source installation. It excludes the subsequent checkpoint-wait
validation changes; those still require their own final-source full CI.


## Completed sink diagnostic and checkpoint-wait validation

The frozen `f7402c87` required-host 900-second sink diagnostic exits 1 with
`sink_outage_incomplete`. Every offered command (90,000) and read (360,000)
completed with no retries and the unchanged 28 load plus four auxiliary slots.
The final public samples keep only outbox degraded. The second and third
five-minute windows have 17,465 and 16,065 latency overflows respectively;
these failed samples are retained, not assigned a finite percentile.
No panic, stack overflow, native retention refusal or internal incident was
observed. Joined retention ends cancelled with 418 steps and 66,060 logical
sequences, 113,922 source receipts removed, zero extents deleted and one
recycled. Source tail 358,429 and resume 113,667 show it falling behind;
these sequence values are not eligible-byte measurements. Sink recovery
refusal occurs before final state/archive reconciliation, so it remains unproved.
An additional private-copy diagnostic uses the frozen native archive owner's
complete canonical validation. It observes 142,532 records, 36,980,305 encoded
bytes and 44,011,520 physical bytes. The count is 13,004 below the acknowledged
workload's expected 155,536 events; the 1-GiB quota is not exhausted. This
diagnostic preserves the original failed data and does not substitute for
complete acknowledged-event identity/payload reconciliation.

Actual outbox transition-stage observations include maximum suffix application
3,398,863 us and gate wait 2,769,738 us. These maxima identify native work to
investigate; they do not establish a causal attribution for all latency.
No offered-load, recovery deadline, delivery guarantee or evidence gate changes.
The timing maxima include preparation. Successive periodic stage observations
after the pause add roughly 14,000–16,000 claim/completion operations per nominal
minute, not that many delivered events. Count/sum differences are retained
privately; exact delivery rates and causal profiling remain separate evidence.

A separate native regression shows captured source validation requesting a
writer turn at exact end when called through ordinary reclamation, even with
an ordinary writer held. A read-only continuation now advances one existing
bounded page and leaves complete authority for the next paced admission.
Actual gate census is unchanged; exact end, stale qualification and corruption
release the reader pin. It never captures an absent candidate or initializes
an absent journal. Eight focused storage tests and three deterministic worker
wait tests pass. The worker uses these pages only inside its existing checkpoint
wait, preserving checkpoint wake, the recheck deadline and cancellation.
The failed host run recorded zero logical checkpoint-wait holds, so this
repair cannot explain that failure. Scoped acceptance and final-source full CI
remain required; no 24-hour qualification has started.
The retention change subsequently passes all 13 scoped acceptance checks and
all 1,840 tests with ten existing skips. Archive timing instrumentation added
after that test build has separate focused evidence and still needs integrated
final-source acceptance/CI.


## Native archive timing instrumentation

The failed host observation motivates measuring the external archive separately
from the existing primary outbox transition stages. The native graph now owns
three bounded timing stages: admission, acceptance and per-record commit. The
worker emits their count/sum/max values after operations release writer leases
and once at retirement; the retained handle owns only atomic numeric counters.
Restart resets these process-local observations. No event, principal, path,
credential, payload or new public health field is logged.

Counts are attempts, including refusals. Equal retries and identity conflicts
still preserve the original record/accounting; they issue no additional record
commit. Commit is inside acceptance, and an equal retry's acceptance calls
admission: durations overlap and are not additive. Creation/initial validation
is excluded. Two-phase Immediate commits, syncs, quota, recovery policy, load,
request slots and every campaign gate remain unchanged.
The new test fails to compile on the prior source because no observation handle
exists. Actual archive operations and a deterministic concurrent-counter test
pass after implementation; final-source acceptance/CI and required-host timing
observations remain pending. No batching, commit-profile change or hidden retry
has been introduced. No 24-hour run has started.
All 33 outbox tests, both server diagnostic-format tests and three-crate
all-feature Clippy pass. The failing test is a missing-method compilation
failure establishing the new observation surface, not a reproduced durability
bug; the separately retained writer-admission regression is a semantic failure.

A separate private-copy pending-scan diagnostic exhausted 45-second and
300-second investigation ceilings without reaching structural exact end. Those
binaries are unoptimized debug tools, not the frozen production server or a
backup/rehearsal arm; their elapsed times cannot establish production restart
performance or weaken its 60-second gate. The canonical archive count remains
valid, while source pending-count and full event reconciliation remain unproved.

Final-source full CI on `52d03291` stops at the capture-retirement test:
the process-wide released-owner counter increases by two where the test expects
its own one release. That exact test passes alone on the same compiled binary.
Its runtime uses the production process-wide budget, so concurrent tests can
change both the byte and lifetime counters between observations. The test now
runs in a bounded dedicated process, retaining all assertions and the real
production budget rather than relaxing equality or replacing accounting.
This is a test-isolation repair; it does not explain the host delivery deficit.
The host release build succeeds, but the CI guard launches no workload on this
failed revision. The failed receipt remains retained and full CI is required
again for the repaired source. No 24-hour run has started.
The repaired production-budget test passes with its complete capture-lifetime
and accounting assertions. Handbook, requirement coverage and ADR obligations
also pass; integrated acceptance and full CI remain pending.

The optimized private-copy diagnostic subsequently completes the native source
structural and catalog validation, then reaches pending-outbox exact end:
13,004 source events remain pending. The canonical archive has 142,532 records;
these counts sum to the failed preparation's 155,536 expected emitted events.
This confirms a real pending-delivery backlog and count agreement, not complete
identity/payload reconciliation or a passing sink-recovery receipt. The original
failed database and archive remain untouched.

The diagnostic's complete structural walk takes about 193 seconds, catalog
validation completes at about 202 seconds, and pending scan ends at about
224 seconds. This tool explicitly requests complete native evidence on a copy;
it does not exercise the production daemon's bounded startup path or measure
time to its first successful write. A host release build overlaps part of the
inspection. These timings cannot establish or waive the 60-second restart gate.

The archive-instrumentation acceptance run completes all 1,875 tests with
ten existing skips. Twelve checks pass; only the handbook step fails because
its environment cannot find the pinned `lychee` executable. With the pinned
tool explicitly available, the same frozen source's handbook check passes.
The original failed acceptance receipt remains unchanged.

Full CI then passes on `81644271`, including the repaired capture-retirement
test, archive publication, recovery, application adapters and verified release
installation. The source remains clean and frozen. All 13 final server-scoped
acceptance checks pass, including 638 tests with eight existing skips. The
host's locked release build succeeds, and its guarded 900-second sink-outage
diagnostic launches only after both successful validation receipts. Native
artifacts are preserved with their hashes before launch. This is implementation
validation and a diagnostic, not host qualification; no 24-hour run has started.

The diagnostic finishes with `sink_outage_incomplete`. All 90,000 commands and
360,000 reads complete with zero retries, using 28 load slots and four reserved
auxiliary slots. The first five-minute window reports p95 28,783 microseconds
and p99 88,439 microseconds. The following two windows overflow the latency
histogram; their maximum-integer percentiles are sentinels, not measured times.
These windows overlap the fault and recovery and cannot certify normal latency.
The run ends about 595 seconds after resumption, so it does not establish that
the complete 600-second recovery deadline was exceeded. It still supplies no
successful recovery receipt or final application/archive reconciliation.

There are no panic, stack-overflow, fatal-runtime or retention-refusal markers
and no incidents. Joined maintenance ends Cancelled with `undelivered_outbox`,
431 logical steps and 66,099 sequences. It removes 115,970 source receipts and
reports one recycled extent, zero deleted extents and zero logical checkpoint-
wait holds. Existing sealed-file names cannot prove three physical reclamations.

Matched periodic observations after resumption contain roughly 7,000–8,200
archive acceptance attempts per reporting interval. Mean archive record-commit
cost is about 1.27–1.47 milliseconds per attempt; primary commit-finish cost is
about 1.58–1.75 milliseconds per transition, with two primary transitions per
acceptance attempt. Intervals are at least 60 seconds, not exact throughput
windows. Counts are attempts, durations overlap, and primary commit-finish
includes receipt sealing, engine commit, frontier refresh, journal cleanup and
index publication. These measurements neither isolate fsync nor prove a causal
delivery rate. They identify native transaction granularity as the next concrete
review target, while preserving archive durability and all campaign gates.

## Bounded success-completion implementation

ADR-0273 is accepted before runtime implementation. An actual sixteen-event
dispatcher regression on the previous runtime performs 32 primary commits and
fails the expected 17-commit assertion. The repaired native dispatcher performs
sixteen individual claims and one successful-completion commit, while retaining
sixteen separate archive record commits and all exact archive bytes. A separate
storage regression first observes sixteen completion commits, then proves one
canonical OutboxTransition receipt with sixteen exact preimages/post-images and
an unchanged application/administration frontier.

Focused native evidence covers ordinary CAS mismatches without overwriting raced
rows, atomic refusal for missing authority and corruption, input count/order
bounds, unchanged all-mismatch groups, and uncertain completion fencing later
writes. Real process aborts after archive acceptance and immediately before and
after grouped completion preserve all records and statuses. Existing recovery
redelivers uncertain groups with zero new archive record commits. These are
process-crash tests, not power-loss persistence evidence.

Dispatcher tests verify sixteen-entry and page-end flushes, flushing accepted
successes before ordinary retry handling, and no Delivered telemetry after an
uncertain group. Repositories without atomic group support retain sequential
completion; the production mutation bridge forwards the new port and the
read-only bridge retains its refusal. The handbook describes the bounded source
completion delay. Focused native/outbox tests and four-crate Clippy pass;
integrated acceptance, frozen final-source CI and the unchanged host diagnostic
remain required. WP-825 remains open and no 24-hour run has started.

The first integrated run also detects two failures caused by an unnecessary
change to the shared after-commit test controller: invalidating already updated
indexes prevents catalog and command identity resolution after a known successful
commit. That change is removed. Actual commit/publication failure invalidation
and write fencing remain unchanged. The failed run is retained; fresh validation
of the corrected final source is required.

At revision `34c94324`, fresh frozen-source acceptance passes all 17 checks:
2,441 tests pass, with ten existing skips and no failure or timeout. Nine
focused regressions also pass, including both earlier semantic failures and
both timed-out recovery cases. Every assertion and test deadline is preserved;
the fresh run uses dedicated scratch storage and four concurrent test slots.
Full repository CI passes, including operational conformance, generated assets,
public-source authoring acceptance and release source installation. The earlier
failed run remains retained rather than replaced by these successful receipts.

The required host's locked release build also passes. After verifying the
frozen source, acceptance and CI receipt identities and log hashes, the guarded
controller launches one unchanged 900-second sink diagnostic with 100 commands/s,
400 reads/s, 28 workload slots plus four observer/sampling slots, and a
300-second sink outage. Its result remains pending. Component validation does
not establish delivery catch-up, sustained reclamation, normal-window latency,
the complete short campaign or 24-hour readiness. WP-825 remains open.

That diagnostic completes all 90,000 commands and 360,000 reads with zero
retries. The actual pause lasts 300.030 seconds and delivery drains under
continuing load in 311.675 seconds after access returns, meeting the unchanged
600-second recovery bound. Public writes and the live query progress throughout
the pause. No panic, stack overflow, fatal runtime or retention refusal is
reported. Joined retention remains Cancelled with no active terminal fence,
637 steps and 121,856 sequences. These observations do not prove archive
payload reconciliation.

The final sampled native census reports 83,107 removed source receipts, zero
deleted extents and one recycled extent. This does not meet the required three
distinct physically reclaimed generations. Longer observation must establish
sustained receipt release and extent reclamation independently of delivery.

The run exits with `probe_normal_window`: the outage starts at 5.017 seconds,
so its recovery ends at approximately 616.722 seconds. Every five-minute window
in the 900-second diagnostic therefore overlaps the pause or recovery. The
reported latency overflow sentinels cannot qualify normal latency, including
the first window. The failed run and exact timing receipt remain retained.

Preparation now additionally accepts a bounded 1,500-second outage mode, with
five complete measured windows and required observation coverage in each.
This permits a whole normal window after the full existing recovery budget.
The outage length, recovery deadline, rates, latency limits, request slots,
archive semantics and final reconciliation requirements are unchanged. The
900-second mode and its refusal remain intact. Duration and memory-coverage
regressions fail on the earlier controller and pass after this change; fresh
controller acceptance and a measured normal window remain required.

## Extended preparation and populated recovery blockers

The isolated controller at `4c8e493d` passes 66 focused Python tests and all
11 controller-scoped acceptance checks. Its native artifacts remain the
CI-validated `34c94324` artifacts; unrelated native changes are excluded from
this diagnostic. This does not substitute for final integrated-source CI.

The 1,500-second diagnostic exits with `probe_latency`. Its actual sink pause
is 300.029 seconds, starting at 5.017 seconds; delivery drains under continuing
load 310.711 seconds after access returns. Recovery meets the unchanged
600-second bound, with public write and live-query progress during the outage.
Both complete normal windows, 900–1,200 and 1,200–1,500 seconds, overflow the
250-millisecond latency histogram. Maximum-integer percentiles are overflow
sentinels, not latency measurements. There are zero retries, but 200 commands
and 692 reads finish after the load deadline. All 150,000 commands and 600,000
reads eventually finish; public state reconciliation checks all 65,536 tickets.
Eventual completion does not satisfy the normal latency, rate or drain gates.

The final native census reports 103,682 removed source receipts, one deleted
extent and two recycled extents. Its source plan has ceiling 327,977 and resume
103,427, a sequence gap of 224,550. These counts do not measure eligible-history
bytes or prove three distinct qualified physical generations: refusal precedes
the controller's final extent receipt. No panic, fatal-runtime or retention
refusal marker is observed. The failed run remains preserved.

An independent, stopped-source private-copy diagnostic reconciles all 215,536
archive records, including canonical payloads and policy anchors: 53,552,213
encoded bytes and 88,023,040 physical bytes. Starting that populated copy then
declines the clean-close shortcut with `bounded_roots_unavailable` and does not
reach readiness within 60 seconds. It has not attempted a fresh write or an
actual restart fault. This is neither public backup/restore evidence nor a
passing recovery campaign. Required full validation remains fail closed.

Investigation finds native prefix-refresh session construction holding the
ordered writer lease while scanning the full audit stream and tombstone chain.
The repair releases that lease after capturing the original publication binding,
before session construction. Deterministic tests first fail because an ordinary
writer cannot acquire the lease during setup; they then pass after the repair.
A second test performs a real publication during setup and proves the original
candidate is discarded without a checkpoint write. Existing complete validation,
exact-binding publication and process-crash regressions also pass. No format,
ordering, public interface or validation guarantee changes. Broader acceptance
and final-source CI remain required; moving setup off the gate alone does not
prove sustainable reclamation or bounded populated startup. WP-825 remains open
and no 24-hour run has started.

## Registry-completion candidate: driver shutdown refusal

Frozen source `411daf6d` passes all 20 scoped acceptance checks, including the
reviewed writer-3 fixture assertions. Full CI passes workspace tests,
documentation and the bounded operational-query corpus, then stops in the real
TicketDesk preparation with `driver_shutdown`. TypeScript, Go and Python smoke
observations passed before cleanup. The driver stderr is empty; the original
controller did not record whether cleanup timed out or observed a nonzero exit.
Neither cause is inferred from that missing evidence. Both required-host gates
remain closed; the prebuilt host artifacts have started no workload.

Fresh TypeScript-only and full four-language reproductions with the unchanged
binaries pass; the latter measures a normal driver exit in 3.24 milliseconds.
The intermittent failure remains unresolved. Preparation now retains a closed
numeric shutdown diagnostic with the actual timeout flag, exit code and elapsed
time, excluding paths, credentials and process identities. The existing
15-second graceful wait, five-second kill wait and refusal are preserved.
Tests require signal exits and timeouts to remain failures, including a process
that returns zero only after its graceful deadline expired. All 90 lifecycle
checker tests pass; this diagnostic is not a native repair or rehearsal evidence.

Package: WP-825, open. Tier: internal. Behavior: retain the actual driver
shutdown failure class without changing pass conditions. Checks: failing
missing-diagnostic regression and 90 lifecycle checker tests. Compatibility:
native behavior and application interfaces are unchanged. Updated handbook:
this observation page. All five scoped acceptance checks also pass.
Hazards/follow-ups: reproduce and fix any native lifecycle
defect, repeat final-source full CI, and complete required-host preparations.

## Native driver cancellation ownership repair

Deterministic regressions prove that dropping a pending invocation or batch
leaves its entry in the Rust host's cancellation registry. Cleanup previously
ran only after an ordinary return; socket-owner aborts could skip it. Repeated
aborts retained local request identities and prevented their reuse. This is a
native resource defect, separately established from the intermittent
`driver_shutdown` refusal; it is not evidence of that refusal's cause.

Within accepted ADR-0106 and `DRV-011`, each registered request now retains an
ownership guard. The guard synchronously retires only its exact cancellation
channel on return or future drop. Registry critical sections contain no await;
command execution remains outside the lock. Dropping a batch aborts its owned
children, whose guards release registration when cancellation is polled. Pool
admission, compiled dispatch, deadlines, structured errors and original
idempotency recovery remain unchanged. No protocol or fixture changes occur.

The baseline fails both future-drop regressions. Tests cover repeated
invocation ID reuse, a batch dropped before its child starts, a batch dropped
after the child's actual transport connection begins, explicit cancellation
with uncertain command status, and an old guard preserving a newer exact
owner. Connection acceptance and released admission provide schedule barriers;
correctness tests use no sleeps. These tests and the existing driver suite pass.

Package: WP-825, open. Tier: surface. Authority: ADR-0106 cancellation ownership,
`DRV-010`, `DRV-011`, and D-003 implementation discretion. Behavior: retire
nondurable native request registrations on cancellation. Checks: failing
baseline regressions, all 42 driver-host tests and all seven scoped acceptance
checks. Final-source full CI remains required. Compatibility: public signatures,
protocol bytes, durable formats and command uncertainty are unchanged. Updated
handbook: `sdks/DRIVER-HOST.md`. Hazards/follow-ups: retain the intermittent
shutdown failure as unresolved and repeat the exact-source real preparation;
this repair does not establish rehearsal readiness.


### Required-host follower acknowledgement validation blocker

Frozen `c59c42a0` required-host full validation fails two replication campaigns
and two missing-tool checks. Graphviz and Python YAML are now installed; the
failed receipt remains failed. The exact follower-frontier test also fails in
isolation with its unchanged 60-second progress watchdog. A separate native
phase diagnostic measures 24 source acknowledgements consuming 60.356 seconds,
with individual calls increasing from 224 ms to as much as 3,483 ms. It stops
at frontier 380 of 408 after 102.38 seconds. These are failed debug diagnostics,
not release throughput measurements or rehearsal qualification.

Source acknowledgements repeatedly validate the whole retained history inside
the general drained writer barrier. This is native population-dependent writer
work. It requires a bounded proof/preparation protocol, not a relaxed deadline
or a smaller workload. ADR-0283 was accepted exactly in session on 2026-10-07 for that authority
extension. Its acceptance and commit-layer scope are recorded separately
before native implementation. Development validation is in progress; no
final-source runtime qualification is established. The
[follower acknowledgement review](WP-825-ADR-0283-IMPLEMENTATION-REVIEW.md)
records the concrete boundary and remaining proofs. Other earlier intermittent
failures remain unexplained. WP-825 stays open; the 24-hour run is unstarted.


ADR-0283 development snapshot validation on the required host now passes 11
native acknowledgement tests, including independent complete-validation oracles,
shared-owner cancellation, qualification overflow and process-level atomicity;
one explicit-clock source-pacing test; one coordinator FIFO test; and all-target,
all-feature Clippy for the four affected crates. An overlapping write-table
ownership error found by the new tests is corrected. Its failed development logs
remain preserved. The original 26-case replication process suite fails with
21 passes and five failures. Bootstrap attachment, exact reads, frontier
progress, repeated-kill resume and source revocation are unresolved. The
frontier stops at 13 of 408, so the development repair cannot yet qualify.
A separate diagnostic checkout captures private child stderr while preserving
the original workload, deadlines and native implementation.
The internal numeric acknowledgement-work stream distinguishes off-gate and
gate-held elapsed time without exposing hold IDs or source payloads. These
checks do not replace frozen final-source CI, complete rehearsal, fault/resource
reconciliation or 24-hour qualification. The 24-hour run remains unstarted.


The subsequent production-bridge repair passes its real-storage regression.
The storage trait now requires explicit forwarding in every adapter. On the
required host, the original replication suite passes 23 of 26 cases: the
byte-faithful 408-frontier campaign, exact-provider replication, repeated-kill
resume and source revocation now pass. Bootstrap held attachment times out;
two exact/causal cases fail during shutdown. Twelve native acknowledgement
tests, five actual process edges, three coordinator ownership/order/refusal
tests and four-crate all-target Clippy pass. A preceding complete redb library
run passes 988 tests with only the existing synthetic shutdown timing evidence
test ignored. Failed snapshots remain failed.

A failing-before-repair native regression identifies permanent source-admission
retirement being misclassified as retryable during shutdown. The repair returns
a typed terminal refusal and preserves the durable hold while releasing private
preparation. The original process suite is being rerun unchanged. This does not
explain the initial bootstrap attachment timeout or establish final-source CI
or rehearsal readiness. WP-825 remains open and the 24-hour run is unstarted.


After terminal retirement refusal, all 13 native acknowledgement tests, source
admission schedule tests, production bridge, coordinator ownership tests and
four-crate Clippy pass on the required host. The unchanged replication suite
passes 25 and fails one: initial bootstrap held attachment exceeds its original
60-second deadline. Both earlier shutdown failures now pass. A separate private
diagnostic build measures bootstrap phases with the same original process
workload and deadlines. The failed suite is preserved as failed evidence;
final-source validation and rehearsal are still outstanding.


Broader acceptance passes 13 non-test gates, including handbook validation, but
its test stage reveals three explicit-step native retention regressions. The
recorded failures concern source receipt retirement, interleaved writer progress
and command-audit materialization. The failed test stage is stopped after those
failures are preserved; it is not a complete test pass. The scheduling policy
had been applied before a graph scheduler existed. Native graph and
acknowledgement composition now bind the shared scheduler monotonically, while
storage primitives retain their explicit-step semantics and all validation and
mutation bounds. The original recovery tests remain unchanged; C3D validation
of the repair is in progress. Bootstrap proof reuse separately requires exact
acceptance of proposed ADR-0284 before implementation. No final-source full CI
or rehearsal qualification is established.


The scheduling ownership repair passes all 14 acknowledgement tests, 21 native
source-admission tests, all three unchanged retained-extent progress regressions,
the actual production graph binding check, shared pacing, production bridge,
three coordinator tests and four-crate all-target Clippy on the required host.
The original process suite remains 25 passing with initial bootstrap held
attachment failing. Complete scoped acceptance is being rerun on the corrected
source. The interrupted earlier acceptance stays failed. Proposed ADR-0284 has
passed proposal governance checks and still awaits exact acceptance.


## Accepted bounded bootstrap custody repair

The maintainer accepted the exact ADR-0284 text and its decision-path scope in
session on 2026-10-07. Acceptance and scope are recorded before implementation.
The native closed read custodian and its three named proof tests are authored;
new required-host validation remains pending. Previous failed bootstrap receipts
remain failures. The original process suite, final-source full CI, unchanged
preparation and complete rehearsal are still required; no 24-hour run has begun.

The prior acknowledgement-only corrected snapshot completes scoped acceptance:
all fourteen checks and 2,855 tests pass under the stock CI profile. The accepted
bootstrap custody implementation subsequently passes its three named native
proofs, the additional actual page-I/O race proof, acknowledgement/coordinator
regressions, seventy-one server bootstrap tests and four-crate Clippy. Its
unchanged original process suite passes twenty-five of twenty-six cases,
including held bootstrap attachment, but concurrent promotion fencing fails.
A separate diagnostic run captures an actual `riffdb-columnar` stack overflow
and daemon abort during fencing and two existing replication deadline failures.
Those failed receipts remain failed; the native abort and deadline failures
require investigation before final-source qualification. No harness watchdog,
workload or assertion is weakened. All execution remains on the required host.

A deterministic native receipt-source cleanup schedule then reproduces
thirty-three nested destructor calls where iterative ownership release was
required. A failed `Arc::try_unwrap` retained a reference that could become the
last owner before the failed result was dropped. `Arc::into_inner` now releases
or transfers that reference atomically. The race regression and source-cursor
module pass on the required host; production stacks remain 384 KiB. A preceding
2-MiB stack trial is excluded from the implementation. This is a proven native
cleanup bug; the exact cause of the earlier process abort is not claimed as
exclusive. All eleven source-cursor cases, sixty-four columnar tests, four-crate
Clippy and the unchanged whole twenty-six-case process suite subsequently pass
with the original reservation. All thirteen non-test acceptance gates pass.
The old cursor architecture guard fails on its obsolete unwrap requirement;
that consequence assertion is updated after recording its exact test-path scope.
All fifty-nine storage architecture tests pass, followed by scope,
acceptance-commit separation, coverage, obligations, file-size and performance
checks against the separately recorded scope commit. A preceding scope check
used the earlier baseline and included the scope extension in its own range;
that failed receipt is preserved, and the corrected comparison passes.
Final-source full CI and complete rehearsal remain required.

## Bounded checkpoint on the frozen native repair

Frozen source `0d26ff43` passes full acceptance on the required 8-vCPU,
32-GiB Linux SSD host: all twelve gates pass, including the complete CI battery,
with 6,922 recorded passing test executions. The source remains clean. Earlier
attempts fail on missing TypeScript compiler/declarations and a Go C-compiler
name unavailable on that host. Locked dependencies and the installed compiler
repair those prerequisites; their failed receipts remain failed. The locked
shipping release build also passes, and its four runtime binaries are copied
and hash-bound before preparation.

The unchanged preparation seeds all 64 organizations and 65,536 tickets and
drains the seed archive, then starts the original 100-command/s, 400-read/s load
with 28 load workers and four reserved auxiliary slots. It stops with
`source_bytes_sample_gap` before the first complete five-minute window. During
load, only one complete source-byte observation arrives; logical and physical
observations continue. No complete normal-window throughput or latency result
is established, and populated restore is not reached.

At shutdown, native diagnostics report 4,887 receipts removed, zero deleted or
recycled extents and 9,904 reference holds. The last source-plan observation has
resume 4,632, tail 102,256 and eligible ceiling 18,048. Native source validation
continues, but the byte observation does not complete within the existing
freshness and observation-gap limits. The exact starvation cause remains
unproven. Preserve the population, artifacts and failed observations; neither
relaxing the gap nor reporting missing bytes as zero would repair this native
blocker. Campaign integration remains deferred while this bounded checkpoint
is assessed. WP-825 stays open and the 24-hour run remains unstarted.

### Diagnostic accounting expiry after a ceiling advance

A separate bounded numeric diagnostic on the same runtime source reproduces
`source_bytes_sample_gap` under the original population, load and deadlines.
It confirms an accounting-progress failure mechanism without qualifying release
performance. Seed accounting completes 2,985 new rows in 23.958 seconds, then
reuses that exact interval. During load, the old certificate is retired when an
actual cut advances resume beyond its ceiling. The next capture covers resume
4,780 through ceiling 15,667; a checked own cut advances resume to 5,036. Ten
bounded pages check 2,560 receipts, reaching 7,596, before capture age reaches
30.475 seconds and the candidate expires. It never completes the remaining
interval, and the unchanged 95-second gap gate fails. No complete five-minute
load window or populated recovery result is established.

This trace identifies native accounting expiry on a larger newly eligible
interval. It does not yet isolate the costs of receipt reconstruction, shared
source validation or pacing, nor prove a repair. Next work must establish a
bounded native progress regression and remove the repeated population-dependent
work without weakening canonical checks, source qualification, maintenance rest,
exact eligibility or the freshness/gap limits. Preserve both failed populations;
the diagnostic changes are absent from the release source. No runtime change,
fixture change, campaign integration or 24-hour launch follows this observation.

### Cost isolation and captured-observation scheduling repair

A release-mode diagnostic on an owned copy of the failed population compares
1,024 identical native receipts totaling 50,537,099 canonical bytes. The current
path spends 304,163 microseconds reading/reconstructing, 31,799 encoding and
32,389 validating history; the equivalent borrowed-canonical path spends
263,278 reading/reconstructing and 31,250 validating. Both produce identical
byte totals and terminal history points. This diagnostic qualifies no release
performance and shows that the smaller codec cleanup alone is insufficient.

The production lane gates captured source-byte pages on the shared source-proof
preparation cooldown, unlike logical and physical observations. A deterministic
regression charges one second of proof work through the production scheduler,
then requests two existing off-gate observation pages. On unchanged runtime it
fails: the byte candidate remains pending throughout charged proof rest.

Separate advancement of already captured source-byte observations from source
mutation-proof preparation in the existing lane. Observation pages retain all
qualification, canonicality, row/byte, capture and expiry checks. They capture
no root and request no writer turn; mutation-proof preparation retains its
shared pacing, candidate custody and unchanged validation. The regression now
checks exact accounting completion, unchanged charged deadline, zero proof
progress, unchanged writer census and unchanged authoritative history. Targeted
source-byte, pacing, logical-observation and architecture checks pass on the
required host, as does all-target storage Clippy. The broader storage suite
then passes 1,322 active tests with zero failures and two existing ignored tests;
all 19 retention-worker tests pass. The qualification-loss regression also
proves retirement during proof rest without a new capture. Initial metadata
acceptance fails on an unquoted colon in the new decision note; correct that
note and preserve the failed receipt. The unchanged measured workload and
final-source full CI remain pending; no endurance result is claimed.


### Required-host candidate and per-page checkpoint polling limit

The captured-page scheduling candidate passes 1,322 active storage tests, all
19 retention-worker tests, all-target storage Clippy and twelve metadata gates.
Its shipping release build succeeds in 297.3 seconds. The unchanged preparation
then fails in 446.9 seconds with `source_bytes_sample_gap`, before its first
complete in-load source-byte sample. No full five-minute load window, populated
restore or final-source full-CI result qualifies this candidate.

An instrumented copy of the same workload completes a 10,453-row interval in
41 pages and 8.108 seconds, unlike the earlier 2,560-row expiry. It records
three complete in-load samples at 78.096, 145.970 and 200.244 seconds. A later
capture starts at resume 17,812 and ceiling 94,915: 77,103 eligible rows. An
initial checked source cut skips 256 rows; accounting then checks 61,440 rows
in 240 pages and reaches sequence 79,508. It expires at 30.125 seconds with
15,407 rows still unknown. The diagnostic preparation fails in 637.8 seconds,
again before a complete five-minute load window. Failed populations and raw
numeric traces remain private; diagnostic binaries qualify no release behavior.

The worker imposes a 100-millisecond checkpoint polling wait before each
captured page. The large interval needs 302 pages: polling alone requires
30.2 seconds, exceeding the unchanged 30-second freshness limit before any
canonical reads. Two deterministic worker regressions fail on that scheduling
with explicit clocks, including checkpoint waiting and the full writer rest.

Distinguish ready captured read-only pages from proof work awaiting budget.
Ready pages check stop/wake and checkpoint completion without a polling sleep;
every page retains its row/byte limit and its original capture deadline. Proof
work keeps the shared source scheduler and polling waits. Retain the complete
wall-clock writer rest and the original capture, observation and workload
limits. No codec shortcut, mutation authority or new source writer is added.
Scoped acceptance and another unchanged required-host preparation remain open.


### Ready-page candidate: progress restored, later accounting gap remains

The ready-page change passes twelve source-byte tests, all 23 retention-worker
tests, all-target storage/server Clippy, 59 architecture tests and twelve
metadata gates on the required host. Its shipping release build succeeds in
294.3 seconds. The unchanged preparation fails in 1,072.7 seconds with
`source_bytes_sample_gap`; populated restore and final-source full CI do not
run. Preserve the failed population and its immutable artifacts.

This candidate completes two five-minute load windows, each with 30,000 commands
and 120,000 reads and no retries. Ten complete in-load source-byte samples are
recorded, the last at 636.611 seconds. A large interval checks 78,373 receipts in
307 bounded pages and completes in 2.217 seconds, within the unchanged
30-second capture limit. The per-page polling failure is cleared. The second
load window overlaps catch-up and exceeds the latency histogram's range;
normal-window latency is not yet qualified.

At shutdown, native diagnostics report 26,764 source receipts removed, one
recycled extent, no deleted extents and 18,812 reference holds. Source
validation continues while a later exact byte observation is missing. Logical
and physical observations continue through approximately 698 and 706 seconds.
The last outbox observation has a backlog of 20 and degraded status, while the
projection backlog is zero. This does not establish the cause of the missing
source-byte capture. Trace its native capture, qualification and proof state
before changing behavior. WP-825 remains open; campaign integration and the
24-hour run remain unstarted.


### Qualified completed certificate stalls before a new capture

A bounded numeric diagnostic on the same ready-page runtime repeats the
unchanged preparation and fails `source_bytes_sample_gap` in 1,074.0 seconds.
It completes two load windows at the required rates with no retries and ten
in-load byte samples; diagnostic binaries qualify no release performance.
The last sample is at load time 634.900 seconds. Its new capture completes in
3,235 microseconds by reusing the already checked interval, with zero new rows
or scan pages. Checked own cuts subsequently advance resume to 33,895 while
preserving the fixed ceiling of 95,070 and exact byte accounting.

The certificate remains complete, with no pending reader pin, no expiry and no
qualification retirement. Its native qualification remains unchanged after the
last checked cut; trace age reaches 91.236 seconds before the unchanged gap gate
fails. The source owner continues taking turns. Its accounting capture is below
the exact-tail mutation-proof continuation branch, so receipt/audit catch-up can
return before that capture even when the fixed-prefix bytes remain known. This
is a capture-lifecycle blocker, distinct from the cleared large-page expiry.
Keep mutation proof and read-only evidence separate: a fresh prefix observation
must still bind an actual qualified immutable pin and current exact eligibility.
No reuse may expand a ceiling, omit a hold, extend a pending candidate's lifetime
or confer deletion authority. Preserve this failed attempt and prove the native
repair with a failing-before regression before repeating preparation.


Both corrected native regressions pass on the repaired candidate. The freshness
regression also refuses reuse when an actual derived pin narrows eligibility and
retires the observation on qualification loss. The first broader storage run
reports 998 passing unit tests, one failure and one existing ignored test. Its
failure is an offline-upgrade fixture expecting immediate success after closing
its source lock. The test passes alone; an explicit-pipe required-host check
proves an inherited close-on-exec descriptor retains the lock after parent close
until the final child owner exits. Isolate that lock-lifetime fixture in one
process after exec, preserving its original assertions and native fail-closed
behavior. Repeat the original concurrent suite and process/recovery checks;
neither the isolated pass nor the OS diagnostic clears the failed suite.


The corrected concurrent suite passes 1,324 active storage tests with two
unchanged ignored tests and no failures. The subsequent certificate-lifecycle
completion passes 15 source-byte tests, 12 continuation tests and five license
tests, including both commit profiles, qualification loss, protected eligibility
and preservation of a newer completed observation when an older qualified proof
is offered. It finishes exact zero-row reuse before another checkpoint capture
and retains the two validation-pin bound. All 23 worker tests, 59 architecture
tests and storage/server Clippy pass on the required host. These are native
implementation checks; original shipping preparation, populated restore and
final-source full CI still gate measured qualification. Preserve every earlier
failure. WP-825 remains open and the 24-hour run remains unstarted.


### Outbox prefix materialization blocks mandatory statistics audits

Shipping revision `d6e44640` builds with the ordinary release profile, but its
unchanged preparation fails `projection_sample_failed` after 745.1 seconds.
The aggregate native observer deadline is five seconds. There is no server
panic; its shutdown succeeds. The native outbox census records a maximum
published-suffix application of 3,899,091 microseconds under the writer gate.
A separate checkpoint benchmark overlaps this attempt, so its latency is not
qualified as isolated host performance.

A separate numeric diagnostic retains the original deadlines, rates, faults
and release settings. After the earlier competing benchmark finishes, this
attempt records 98 complete observer triplets. Its maximum Statistics call is
4,075,279 microseconds, versus 12,577 for Health and 6,867 for projection. The
outbox census records suffix application of 3,527,529 microseconds under the
writer gate. The original observer timeout does not recur; preparation instead
fails `source_bytes_sample_gap` after 838.2 seconds. The last exact byte sample
is at 398.991 seconds. Neither attempt reaches populated restore or final CI.
Preserve both failed populations and their immutable artifact identities.

Statistics retains its intrinsic durable audit. That audit shares the native
writer with outbox status transitions. Those transitions previously drained
an entire published journal suffix while holding the sole writer gate. Native
admission now prepares oversized prefixes through the existing asynchronous
checkpoint and waits outside the writer gate and checkpoint state lock. Its
closing suffix uses the existing closed 256-transition, 1-MiB scanner. The
outbox transaction still uses the selected Immediate profile, exact reciprocal
validation and status CAS, one captured receipt and post-commit membership.
Claims remain individual; successful completion groups remain at most sixteen.

An explicitly paused checkpoint regression fails against the original native
path, then passes after repair: another audit acknowledges durably before the
checkpoint is released. The negative run also exposes a fixture-only watchdog
poisoning its pause lock before cleanup; release that lock before asserting the
watchdog result and repeat the original negative without destructor abort.
A second native regression covers unfinished spare preparation before rotation.
The combined repair passes both native regressions, 1,330 active storage tests
with two unchanged ignored tests, all 59 architecture checks, storage/server
Clippy and thirteen metadata gates on the required host. Restrict the unused
unbounded-drain convenience wrapper to its existing test callers; focused
regressions and Clippy pass after that annotation. The first broad candidate
exposes an architecture guard tied to the old scanner name; its corrected guard
retains one transaction and verifies the bounded outbox scanner before capture.
An intermediate compile failure identifies two test-only sealer constructors;
both now publish the same completion signal when their explicit schedule ends.
Unchanged measured preparation, populated recovery and final-source full CI
remain pending. The separate source-byte freshness gap remains open; no deadline or workload gate
has been relaxed. WP-825 remains open and the 24-hour run remains unstarted.

### Production outbox admission retains the server port-cell lock

Shipping revision `88420cc2` builds the four ordinary release binaries in
296.1 seconds. Unchanged preparation fails after 41.7 seconds, before measured
load: initial archive drain reports `archive_drain_liveness`, and the event
observer reports `observer_ack`. The server has no panic and shuts down cleanly.
The 65,536-ticket seed finishes. Preserve the failed population and immutable
artifacts. The dependent full-CI run correctly remains unstarted.

The production bridge holds its operational-port cell while calling all six
outbox mutations. Lower-level admission releases the writer gate during an
asynchronous checkpoint wait, but the bridge retains its cell. Event consumer
acknowledgements and inspection also acquire that cell. A bridge regression
explicitly holds the cell during a completion attempt against a real database;
the original composition fails the watchdog after ten seconds. Release the
barrier before asserting so a failed regression still cleans up normally.

The repair obtains a restricted native outbox capability from the activated
storage bundle and forwards the six mutations without the cell. It delegates
to the same outbox implementation, sole FIFO writer, canonical reciprocal
checks, status CAS, captured Immediate transaction and post-commit publication.
No raw transaction, command or administration authority escapes the capability.
Pure-read handles still refuse mutation. Repaired C3D validation and unchanged
shipping preparation remain pending; the source-byte freshness gap remains open.

The native outbox capability remains move-only in its own server sharing cell.
Only that cell is held across outbox admission; event consumer operations use
the original cell. Both cells share one fail-closed poison flag, published on
unwind before releasing the local lock. Deterministic tests cover lock exclusion
and poison refusal in both directions. Writer sequencing and pure-read authority
remain unchanged. The final move-only composition passes all four bridge
regressions, both checkpoint regressions, the full server suite, the compile-fail
move-only capability proof, 59 architecture checks, storage/server Clippy and
scoped metadata acceptance on C3D. The original bridge regression fails before
repair, and the first draft also fails the poison-refusal regression before that
policy is restored. No accepted ordering, durability, archive protocol, public
interface, fixture or qualification gate changes. Unchanged preparation, populated
public restore and final-source full CI remain required.

### Licensed source-byte capture after eligibility growth

Shipping revision `a605ec69` builds the four ordinary release binaries in
295.9 seconds. Initial archive drain finishes in 230.2 seconds and the event
observer keeps acknowledging. Unchanged preparation nevertheless fails
`source_bytes_sample_gap` after 1,510.8 seconds. No competing host activity is
recorded during build or preparation. Preserve the failed population and its
immutable artifacts; populated restore and dependent full CI remain unstarted.

The last in-load exact sample has resume 98,036 and ceiling 98,504. The final
native source observation has resume 102,856 and eligibility ceiling 244,138.
The accounting refresh required an identical completed ceiling and an existing
completed accounting certificate. Releasing holds can expand eligibility, and
checked reclamation can consume that old accounting interval. Either case
prevents an ordinary accounting capture while unrelated tail proof is unfinished.

Under ADR-0279 decision 6, start a paced read-only capture below the existing
finite checkpoint license on an actual current qualified pin. Reuse a completed
exact prefix only when its interval is covered by current eligibility. Validate
newly eligible receipts through the existing bounded canonical pages; absent
accounting starts a complete bounded walk of the licensed interval. Incomplete
counts remain unknown. Current holds, coverage, mutation proof, two validation
pins, capture cadence and freshness retain their existing checks and bounds.

Four native regressions fail before repair and pass afterward, covering growing
eligibility and absent accounting under both commit profiles. All nineteen
source-byte, twelve continuation and five checkpoint-license tests pass on C3D.
The accounting candidate passes all 1,334 active storage tests with two
unchanged ignored tests, including 59 architecture checks, and storage/server
Clippy. Final-source acceptance and CI remain required. The third five-minute
load window also exceeds the latency histogram bounds during recovery; its
cause remains unestablished. This failed preparation does not qualify sustainable
latency, the complete short rehearsal or a 24-hour run.

### Native event-consumer checkpoint waits also block durable audits

A deterministic native regression pauses a real checkpoint before materialization.
The original consumer admission retains the ordered writer while waiting for it;
another audit cannot acknowledge before the ten-second watchdog. A separate
real-database bridge regression explicitly holds the operational-port cell;
the original consumer transition cannot finish until that cell is released.
Both negative tests release their pauses before asserting, preserving cleanup.

Consumer admission now uses the existing asynchronous checkpoint preparation and
bounded 256-transition, 1-MiB closing scanner, releasing the FIFO writer during
the wait. The production bridge owns a separate move-only native consumer port
cell sharing the original poison flag. Consumer transitions and protected lease
coordination, validation, resolution and replay delegate to their original native
implementation. Transaction-current authority, lease CAS, source holds, selected
Immediate profiles, captured receipts and uncertainty fencing remain unchanged.

Both consumer checkpoint tests, three consumer bridge tests, both outbox
checkpoint tests, four existing outbox bridge tests, the move-only compile-fail
proof, all 59 architecture checks and storage/server Clippy pass on C3D. The
native blocking defect is established; its contribution to the recovered-load
latency spike still requires frozen-source measurement. No harness deadline,
workload, observation rule, protocol or compatibility fixture is changed.

### Frozen consumer/accounting candidate: progress, latency refusal and seed failure

Revision `883e1987` passes the final server suite and scoped metadata acceptance
on the required host. Its ordinary four-binary release build takes 293.8 seconds.
Unchanged 1,500-second preparation completes all 150,000 commands and 600,000
reads with zero retries and zero post-duration operations, but refuses
`probe_latency`. It records 24 complete in-load source-byte captures across all
five windows and one recycled physical generation. The final exact capture
contains 163,826 rows, 859,365,208 canonical bytes and 1,310,608 key bytes. This
does not prove the required three retired generations or populated restore.

| Load seconds | p95, microseconds | p99, microseconds | Histogram overflows |
| --- | ---: | ---: | ---: |
| 0–300 | 32,209 | 92,033 | 392 |
| 300–600 | 105,342 | 132,058 | 784 |
| 600–900 | beyond range | beyond range | 22,144 |
| 900–1200 | 84,291 | beyond range | 1,960 |
| 1200–1500 | beyond range | beyond range | 21,672 |

The sink pause lasts 300.05 seconds and recovery takes 322.27 seconds with
29,972 public writes and continuing live progress. The last two windows are
fault-free; both fail the unchanged latency limits. Beyond-range percentiles
are sentinels, not measured latency values. Final archive reconciliation and
dependent full CI correctly remain unstarted after the preparation refusal.

A private diagnostic replay reproduces the failure without competing heavy
work. Bounded kernel sampling and five short debugger snapshots establish a
native outbox engine flush holding the writer gate while the audit writer and
event consumer wait. They also observe retention planning under the gate and
source-byte canonical accounting outside it. These are sampled paths, not a
complete attribution of the tail latency. Aggregate gate residence does not
identify which stalls produced the overflowing requests: a short writer stall
can delay many offered requests in the bounded queue.

Read-only cloud metrics independently show approximately 6,000 sustained write
IOPS during catch-up on the 500-GiB `pd-balanced` disk. Normal windows later show
minute-average I/O queues exceeding 40 requests. Peak metrics and sustained
metrics have different meanings; the observations do not prove one limit causes
every failed window. No disk setting, durability profile or workload gate changes.
Raw samples, stack traces, machine identities and cloud responses remain private.

The subsequent consumer-isolation diagnostic exits after 45.4 seconds during
seeding with `observer_consumer` and `archive_drain_liveness`. The observer had
published readiness, but no measured load or planned consumer pause occurs.
Preserve this new failure; do not classify it as an isolation result. The closed
return code lacks the nested native class, so add bounded fatal-only observer
diagnostics for consumer, acknowledgement and status calls. Both new tests fail
before classification is implemented; they then pass with TicketDesk tests and
Clippy on C3D. Only validated registry codes and closed local variants may render;
an untrusted status payload remains absent. Existing fatal codes, no-retry
behavior, application interfaces and fixtures remain unchanged.

PR note: Package WP-825 (open); Tier guarantee for the cumulative package;
Behavior: fatal observer diagnostics retain a closed native failure category;
Checks: paired negative/repaired diagnostic assertions and TicketDesk tests and
Clippy on C3D; Compatibility: unchanged application behavior, reviewed fixtures,
ordering and durability; Updated handbook: Sustainable TicketDesk milestone;
Hazards/follow-ups: identify the seed failure, repair native latency, qualify
unchanged preparation and populated public restore, run final-source full CI,
integrate all actual campaign handlers and complete the short rehearsal. No
24-hour run has started.


### Paused-consumer and retention-disabled isolation controls

The observer-diagnostic revision 435984fd uses native binaries from
883e1987 in both controls. Neither control qualifies preparation: one changes
consumer progress, and the other disables retention and its source-accounting
collector. Both retain the offered rate, request slots, workload population,
timeouts, sink outage and durability profiles. No competing heavy work is
recorded. These are diagnostic comparisons, not proposed campaign settings.

The first control pauses its consumer after load second 600.909. It completes
all 150,000 commands and 600,000 reads with no retries or post-duration work.
The consumer checked 2,048 events without duplicates before pausing; live updates
continue. The helper exits successfully after 1,861.3 seconds. Pausing also freezes
the consumer retention floor, so this does not isolate acknowledgement I/O alone.

The second control keeps its consumer active while disabling retention. It also
completes all 150,000 commands and 600,000 reads without retries or post-duration
work. The consumer checks 4,096 events without duplicates. The helper exits
successfully after 1,883.7 seconds. Source accounting is also absent in this
control; the result cannot separate accounting, logical pruning and their
interaction with checkpoint or outbox I/O.

| Load seconds | Paused consumer p95/p99, microseconds | Disabled retention p95/p99, microseconds |
| --- | ---: | ---: |
| 0–300 | 48,099 / 80,777 | 28,668 / 33,449 |
| 300–600 | 107,989 / 147,440 | 98,674 / 123,234 |
| 600–900 | 68,954 / 93,291 | 68,949 / 88,736 |
| 900–1200 | 72,975 / 123,782 | 68,642 / 87,902 |
| 1200–1500 | 70,171 / 89,459 | 69,402 / 89,439 |

The controls narrow the investigation to retention-related work. They do not
establish a single cause or prove a repair. Consumer and delivery transitions
are explicitly source-independent in the native continuation classifier; direct
consumer checkpoint updates invalidating that source epoch are not established.
The earlier seed observer failure does not recur in either control and remains
unexplained, rather than being declared fixed by a successful replay.

A bounded syscall diagnostic also needs qualification limits. The first capture
writes stack samples to the measured disk and is unsuitable for an uncontaminated
I/O attribution. Replacement captures use bounded RAM with no user stacks. They
show substantial outbox writes during a passing paused-consumer recovery window;
retained samples do not prove exact operation counts, complete capture coverage
or exclusive responsibility for the failed baseline. Private capture preservation
initially fails on privileged file ownership, then succeeds with verified file
hashes. Raw samples and machine identity stay outside the repository.

The baseline reports 695,212 target/row-boundary holds and 135,938 fenced holds.
The pending-validation branch selects zero idle rest even while the shared native
source scheduler refuses further proof work until its already-charged rest ends.
That repeatedly reenters full maintenance preparation instead of parking between
bounded progress checks. The deterministic worker regression fails with zero
delay instead of the outstanding source-rest slice. The real native observer
regression separately fails to report that charged rest. Both negatives run on
C3D before repair, without sleeps as synchronization.

The native phase now reports the shared scheduler's remaining rest. An idle or
fenced worker with pending validation observes at most the existing 100-ms poll
slice of that already-charged rest. Ready source work and actual committed
source deletion retain immediate progress rechecks. Captured read-only pages
continue inside the wait; writer-residence rest, source admission and proof
checks remain intact. The worker regression also proves cancellation and that
neither checkpoint authority nor writer residence can be inferred from waiting.

The repaired worker's 24 tests, 22 native source-byte tests, four continuation
tests, the checkpoint-license test, shared-source pacing test and 59 architecture
tests pass on C3D, as do storage/server Clippy and scoped metadata acceptance.
Final edits, frozen release builds and the unchanged workload still need checks.
The passing paused-consumer control also polls heavily, so this defect is not yet
established as the sole cause of the latency refusal. Unchanged preparation,
public populated restore, final-source CI and the complete short rehearsal remain
required. The 24-hour run remains unstarted.



PR note: Package WP-825 (open); Tier guarantee for the cumulative package;
Behavior: the native worker observes existing charged source rest instead of
polling full maintenance continuously while proof work is unavailable;
Checks: paired negative/repaired worker and actual-native rest observations,
source/worker/architecture suites, Clippy and scoped metadata acceptance on C3D;
Compatibility: unchanged application interfaces, fixtures, source validators,
FIFO admission, maintenance budgets, durable formats and durability profiles;
Updated handbook: Sustainable TicketDesk milestone;
Hazards/follow-ups: this bounded-work repair does not prove the complete latency
cause; qualify final unchanged preparation, populated restore, final-source CI
and the complete short rehearsal before 24-hour readiness.

The frozen `120b1b0f` release builds successfully, but its unchanged preparation
stops during initial archive drain, before measured load. Seeding completes all
65,536 tickets in 31.2 seconds. The observer's closed diagnostic reports
`stage=observer_consumer`, `class=TRANSPORT_UNAVAILABLE`; neither process reports
a panic. No latency result, populated restore or full-CI result follows from this
attempt. The preceding charged-rest repair therefore remains unqualified under
the unchanged workload.

A separate, unqualified seed diagnostic captures the same native release at
three points. The first two captures show the authoritative event reader loading
and decoding command segments; a later capture shows reactive-module lookup
waiting for the operational bridge's write lock. The observer subsequently makes
progress, so these captures do not establish the cause of the fatal attempt or
prove checkpoint starvation. Inspection confirms that this immutable module read
still uses the broad operational cell. A deterministic real-storage regression
holds that cell with explicit barriers while requesting the module through the
production bridge; the native correction delegates the unchanged repository
read through the existing narrow shared reader and preserves shared poison
refusal. These observations grant no readiness claim or workload exemption.

That seed diagnostic finishes with `probe_latency`, rather than the fatal
consumer error. All 30,000 commands and 120,000 reads complete with zero retries
or post-duration work, but both percentile values exceed the 250-millisecond
histogram range (13,103 overflow samples). The observer checks 2,048 events with
no duplicates. Its captured pauses occur during seed, before this measured load;
the shortened diagnostic and successful seed still grant no qualification. The
new production-bridge regression then fails on C3D with the original locked
reader: its deadlock watchdog expires while the barriers keep the cell occupied.
Cleanup releases the holder and joins both threads before the assertion. This
establishes unnecessary reader serialization, not responsibility for the complete
latency failure. The repaired bridge's 24 tests, native exact-module and absence
checks, 59 architecture tests, Clippy, formatting and scoped metadata acceptance
pass on C3D. The exact-module regression also proves that neither a mutation-gate
ticket nor an audit record is added. These checks do not constitute full CI.
Unchanged preparation, populated public restore, final-source CI and the complete
short rehearsal remain required before 24-hour readiness.

PR note: Package WP-825 (open); Tier guarantee for the cumulative package;
Behavior: immutable reactive-module lookup uses the existing native narrow reader
instead of waiting for the operational writer cell, retaining poison checks before
and after the read and on unwind; Checks: failing original barrier schedule,
repaired bridge and exact-native module/absence/nonmutation assertions, architecture,
Clippy, formatting and scoped metadata acceptance on C3D; Compatibility: original
repository validation, authorization, ordering, durable formats, fixtures, workload
rates and deadlines; Updated handbook: Sustainable TicketDesk milestone;
Hazards/follow-ups: the complete seed-failure and latency causes remain unresolved;
unchanged preparation, public restore, final-source CI and the full short rehearsal
remain required. The 24-hour campaign remains unstarted.

The frozen `912f974a` release builds normally, then unchanged preparation fails
`source_bytes_sample_gap`. Seeding completes 65,536 tickets in 118.2 seconds.
Measured load is terminated after 962.7 seconds; its last complete source-byte
observation is at second 867.6. The observer checks 3,072 events without duplicates
and exits cleanly, as does the server. No populated restore or full CI follows.
The third five-minute latency window exceeds the histogram range. The first
window overlaps the preparation's sink outage, which starts near second five;
its passing percentile values are not a normal-window qualification result.

A deterministic native schedule independently reproduces discarded accounting:
the last page completes an older mutation-proof snapshot while a newer exact
accounting capture still needs bounded pages. Existing sharing correctly refuses
the older snapshot for that unfinished capture, discarding it until the next
capture cadence. An initial test misses the final proof page and passes; the
corrected schedule calibrates the bounded proof on the same immutable source,
then fails the missing-candidate assertion on C3D. This establishes a native
scheduling defect, without establishing it as the sole cause of the measured gap.

The repair lets already captured accounting pages finish before offering a
completed mutation proof's pin. It keeps both existing pins, every snapshot
sharing validator, the original capture timestamp and cadence, and the ordinary
source-proof admission path. No new writer turn, checkpoint, snapshot, deletion
authority or relaxation of freshness is introduced. A first repaired run proves
the accounting survives and completes, then fails a test assumption that the
refreshed source proof finishes in one page. The final test requires completion
through the existing bounded steps before the first observation can be recorded.
The corrected 34 accounting tests, five checkpoint-license tests, 24 retention
worker tests, 59 architecture tests, Clippy, formatting and scoped metadata
acceptance pass on C3D. Unchanged preparation and complete qualification remain
pending. The initial remaining-check runner rejects an extra test-filter argument
before executing worker tests; the corrected worker, architecture, Clippy and
metadata commands all pass. No runtime failure is silently ignored.

The separate controller handover component passes 17 focused tests on C3D. Its
shared release file stays unpublished until both original workers acknowledge
quiescence. Load and observer acknowledgements remain until the actual new
observer generation and a subsequent load completion can be confirmed, preventing
a paused load poll from recreating a stale acknowledgement during resume. Restore
markers survive until that confirmation. Stale identity, duplicate events,
oversized or recursive records, clock rewind and timeout fail closed. The complete
seven-fault runner, process replacement and resource reconciliation remain open.

PR note: Package WP-825 (open); Tier guarantee for the cumulative package;
Behavior: bounded native accounting finishes before offering an older completed
mutation proof's pin; the controller helper fences generation handover and delays
acknowledgement cleanup until both workers resume; Checks: corrected negative
native schedule, accounting/license/worker/architecture tests, Clippy, formatting,
focused controller tests and scoped metadata acceptance on C3D; Compatibility:
unchanged snapshot validators, source authority, two-pin bound, maintenance rest,
workload, formats, fixtures and application interfaces; Updated handbook:
Sustainable TicketDesk milestone; Hazards/follow-ups: unchanged preparation,
public restore, final-source CI and the actual complete short rehearsal remain
required. The 24-hour run remains unstarted.

Frozen `2bc25f14` builds with the ordinary release profile and completes the
unchanged 1,500-second preparation load. All 150,000 commands and 600,000 reads
finish with zero retries and no post-duration operations; independent state
reconciliation checks every one of the 65,536 tickets. Source accounting now
records 25 complete observations, five in each window, with a maximum gap of
66.8 seconds. Logical accounting records 25 observations and physical accounting
24, both covering every window with gaps below 67 seconds. This resolves the
previous measured accounting-gap refusal for this preparation, without claiming
that all native performance defects are fixed.

Preparation refuses `probe_latency`. The actual sink pause lasts 300.03 seconds
and recovery 302.75 seconds, ending near load second 607.8. The first three
windows therefore overlap the injected fault or recovery; their percentiles
cannot qualify normal operation. In the 900–1,200-second normal window p95 is
88.6 ms and p99 exceeds the histogram's 250-ms range. In the final normal window
both percentiles exceed that range. The exact-provider charge peaks at 8,913,408
bytes below its unchanged 1-GiB limit. Server, load and observer exit cleanly;
there is no concurrent heavy test or build interference. These component
observations still do not qualify the complete resource model, and allocator
ownership at process exit remains unknown. The native latency limitation remains
a blocker; no populated restore, full CI or 24-hour run follows this failure.

The controller follow-up replaces caller-supplied resumed-command counters with
a one-shot receipt published by the original Rust load process only after a new
command has been validated, witnessed and acknowledged in the new release.
Neither a read completion nor release publication supplies that proof. A test
using actual control files fails on the preceding worker at the expected missing
receipt assertion, then passes with the revised worker. Retained quiescence
acknowledgements cannot be overwritten by another release before the controller
consumes the resume receipt. Observer-first quiescence preserves active offered
load during a restart fault, while full handover still requires both original
workers' acknowledgements.

Rotation can now validate the exact quiesced command count from the load owner
against its acknowledgement witness, including actual current ticket revisions,
without scanning the preallocated unoffered suffix. It still refuses revoked
reads and replay, then validates same-principal replay of the original command.
The existing single-update smoke remains strict. All 37 TicketDesk Rust tests,
194 lifecycle harness tests, formatting, Clippy and scoped metadata acceptance
pass on C3D. This is component validation; integrated population rotation,
process replacement, resource reconciliation and the complete campaign remain
unproved.

PR note: Package WP-825 (open); Tier guarantee for the cumulative package;
Behavior: native post-handover acknowledgement receipt, observer-first pause and
actual-prefix rotation reconciliation; Checks: original-worker negative,
37 TicketDesk tests, 194 lifecycle harness tests, formatting, Clippy and scoped
metadata acceptance on C3D; Compatibility: unchanged application interfaces,
authorization, command identity, ordering, durable formats, reviewed fixtures,
workload rates and deadlines; Updated handbook: Sustainable TicketDesk milestone;
Hazards/follow-ups: native normal-window latency still blocks preparation;
integrated campaign faults, complete resource accounting, populated restore,
final-source full CI and the short campaign remain required. No 24-hour run starts.

The native command-payload path holds the complete index publication read guard
while loading pinned storage bytes, resolving history stubs and decoding cold
segments. Prior captures show an event reader in this decoding path, but neither
that stack nor the correlation with consumer progress proves the entire latency
cause. Cache hits already avoid decoding; there is no claim that every lookup
decodes or that canonical validation can be omitted.

A deterministic native regression uses an existing valid command fixture and a
hook before payload storage I/O. The original command read succeeds, then fails
the assertion that the publication writer guard is available. Earlier setups
stop at unavailable or corrupt fixture authority, or fail to compile after the
test moves across a private field boundary; none counts as the target negative.
The corrected original test reaches and fails the exact guard assertion on C3D.

The repair captures one immutable metadata entry and one optional cache candidate
under the original publication/frontier check, releases the guard, and resolves
the same pinned storage view through the unchanged canonical and semantic
validators. Conservative decoded ownership charging also runs outside that
guard. Reinsertion requires the exact metadata entry to remain current; a
replaced index receives no stale payload. Complete-index negative lookups,
eight bounded capture attempts, cache bounds and writer-side continuous
published/unpublished guards remain intact. Existing registered read roots
retain extent custody without another storage pin. Follower maintenance adopts
the same metadata ownership; its first compile catches that required type
adjustment before any positive test runs.

All 11 focused native tests pass, including cold and cached read progress,
provenance and both audit paths, index replacement, original pinned authority
after physical overwrite, and cached refusal of a corrupt successor. The full
storage suite and all 59 architecture tests also pass on C3D. Clippy initially
rejects only the placement of a test helper after the test module. Moving that
unchanged helper preserves the production source byte for byte; all 11 focused
tests, Clippy and formatting then pass on the corrected layout. The storage
suite retains its two existing ignored qualification tests. Scoped metadata
acceptance subsequently passes on C3D. Unchanged measured preparation is still required;
no sole-cause or qualification claim is made from these deterministic tests.

PR note: Package WP-825 (open); Tier guarantee for the cumulative package;
Behavior: release the index publication guard before pinned payload I/O and
decoding, with exact metadata ownership for cache reinsertion; Checks: original
negative, 11 focused native tests, full storage suite, 59 architecture tests,
Clippy and formatting on C3D; Compatibility: unchanged application interfaces,
authorization, ordering, durable formats, canonical validation, reviewed fixtures,
cache bounds and writer publication custody; Updated handbook: Sustainable
TicketDesk milestone; Hazards/follow-ups: unchanged preparation, populated
restore, final-source full CI, complete fault controller, resource reconciliation
and the short campaign remain required. No 24-hour run starts.


The frozen payload-custody revision passes its ordinary release build in
293.6 seconds, but the first unchanged preparation refuses before measured load.
The seed worker acknowledges all 65,536 tickets in 29.7 seconds. The original
event observer then returns `observer_consumer` with `TRANSPORT_UNAVAILABLE` at
the existing 30-second client deadline. Archive-drain liveness and observer
cleanup refuse; the server closes with exit zero. No panic, load-window result,
populated restore or full-CI success is inferred from that attempt.

A separate debugger-instrumented diagnostic seeds in 109.1 seconds and observes
all 1,024 selected seed events with zero duplicates. Its stacks place an event
consumer at the existing exclusive write gate while outbox and projection paths
resolve and decode command history. Those captures neither establish one cause
nor reproduce the original timeout. The intentionally one-second diagnostic
ends at the existing provider-observation coverage refusal. It cannot qualify
preparation. An uninterrupted repetition retains the failed receipt and uses
the exact frozen release binaries, workload, limits and deadlines. That
uninterrupted repetition also stops before load: seeding completes in 30.3
seconds, and the consumer again reaches the original transport deadline. Both
failed attempts remain evidence; the debugger diagnostic does not supersede them.

The next private controller component keeps an exact live process roster across
retirement and replacement. Each role must release its actual prior owned child
with the expected exit code, kernel RSS high water and finite inherited birth
bound before admitting a successor. The original process-owner wrapper exposes
a detached, locked usage snapshot without rereading a path or reaping the child.
Short operator children retain their exit evidence even if they finish before
the first live census. Earlier RSS peaks survive replacement; live measurements
remain measurements of the current exact descendant tree.

The roster freezes per-role hard address-space ceilings and includes one
serialized launch's larger inherited birth ceiling in its conservative aggregate
bound. It never sums sequential birth ceilings as if all launchers overlapped,
nor substitutes those ceilings for measured live allocation. The controller must
already have its finite hard ceiling. Role identity, expected exit, missing birth
proof, unknown RSS, changed ceilings, untracked children, process replacement,
clock rewind, cross-thread launch and the 64-incarnation bound all refuse.
Unknown or failed evidence fences subsequent roster observations.

All nine roster tests, all 12 persistent-owner tests and the complete 204-test
lifecycle suite pass on C3D. The process test actually launches children under a
finite parent ceiling, kills and replaces the server owner, executes a short
operator and reconciles six exit records. These are component results. Sample
continuity, native observation epochs, every actual campaign launch and complete
fault execution still require integrated proof; `qualifies_resource_model`
remains false. Package scope and metadata acceptance for this component remain
pending, and WP-825 stays open.

PR note: Package WP-825 (open); Tier internal for the roster component;
Behavior: preserve exact live ownership and prior exit peaks across bounded
process replacement; Checks: nine roster tests, 12 persistent-owner tests and
all 204 lifecycle tests on C3D; Compatibility: unchanged database semantics,
application interfaces, ordering, durable formats, reviewed fixtures, workload
and qualification gates; Updated handbook: Sustainable TicketDesk milestone;
Hazards/follow-ups: initial native consumer timeout, uninterrupted preparation,
complete fault controller, continuous sampling, populated restore, final-source
full CI and the short campaign remain required. No 24-hour run starts.


Final prefix acceptance passes all 12 scoped checks on C3D, and the tested
sources are committed. The unchanged ordinary release build passes in 293.7
seconds. Preparation fails in 40.7 seconds before measured load: all tickets
seed in 30.2 seconds, the consumer exits at its original transport deadline, and
the server closes normally. There is no external heavy-work interference, panic,
populated restore result or full-CI success. The failed receipt is preserved.

Caller tracing corrects the investigation: TicketDesk uses an unprotected
consumer and the reactive event-window adapter, not protected symbolic replay.
The prefix repair removes real excess work but lies outside this failure path.
The implementer should have established that caller path before selecting it.

A separate repetition reproduces the transport failure before debugger capture.
Only afterward is the exact owned controller paused briefly to prevent cleanup
while the server is inspected; it is resumed afterward. The consumer is waiting
in checkpoint completion during `transition_event_consumer`. The checkpoint
worker is materializing a batch and resolving retained history through the
physical frame checksum; the projection worker is decoding command segments.
This diagnostic supplies no qualification result.

Another repetition enables the existing bounded checkpoint diagnostic. Fourteen
checkpoints start and finish before cleanup while no consumer progress is
published. Selected batches grow from 1,159,982 to 65,858,775 encoded bytes, and
ordinary capacity trigger counters remain zero. This repetition ends with a
distinct protocol refusal and does not attach a debugger; its result is retained
without calling it another reproduction of the transport error. Together with
the actual admission loop, these observations identify repeated catch-up rather
than a single unfinished checkpoint. New acknowledged writes can replenish the
tail each time the direct owner drops its FIFO ticket to wait.

ADR-0282 is proposed for exact human review before implementation. It preserves
writer progress during the earlier checkpoint, then gives the direct owner a
finite FIFO reservation. Later writers would wait while the fixed remaining
tail is materialized; reads and checkpoint publication would continue. The
proposal explicitly changes native backpressure, preserves authority and
formats, and grants no extra source-maintenance priority. Its acceptance alone
would establish neither latency compliance nor readiness.

PR note: Package none, guarantee proposal for subsequent WP-825 implementation;
Tier guarantee; Behavior added or changed: none yet, proposed finite checkpoint
handoff and corrected investigation record; Checks: scoped metadata acceptance
on C3D, preceding native component checks and failed preparation retained;
Compatibility: no runtime, format, fixture or interface change in this proposal;
Updated handbook: Sustainable TicketDesk milestone; Hazards/follow-ups: exact
human acceptance, deterministic handoff and crash coverage, unchanged measured
gates, complete controller and short rehearsal remain required. WP-825 stays
open, and no 24-hour run starts.


The process-roster component subsequently passes package scope, internal tier
classification and scoped metadata acceptance on C3D. Its seven-file commit
retains the 204-test component evidence and both native preparation failures.
Full campaign execution and resource-model qualification remain unproved.

A later seed diagnostic captures the original event reader inside command
payload decoding at seed second 25. Like the earlier debugger attempt, it
observes a slower successful seed and does not reproduce the uninterrupted
consumer timeout. This capture identifies work on the request path, not an
exclusive cause or a passing preparation.

A separate deterministic native regression proves an eviction defect. With the
existing 64-entry cache full, it successfully reads the oldest command, publishes
one newer segment, then finds that reading the same active older command decodes
its canonical payload again. Eviction selected the smallest commit sequence
regardless of verified read activity. The original test reaches and fails that
exact assertion on C3D; it does not fail on fixture authority or compilation.

The repair retains the same 64-entry and 64-MiB decoded-payload ceilings, but
selects the least recently verified payload for eviction. Only a successful
current canonical-byte or checked-stub identity and metadata validation refreshes
retention. A captured cache candidate can survive concurrent eviction and
reenter only while its exact metadata entry still belongs to the index. The
bounded recency queue and payload map remove and evict entries together; stale
or inconsistent bookkeeping refuses. Cold authority still decodes through the
original validators, and a corrupted successor still refuses despite a cached
candidate. No storage format, writer ordering, read authority, scan ceiling,
canonical validation, cache size or request deadline changes.

All 13 focused native tests pass on C3D, exercising the original writer-side
lookup and the captured read path, eviction between capture and resolution,
corrupt successor refusal and the existing publication-custody schedules.
The full storage suite, all 59 architecture checks and Clippy also pass on
C3D. Formatting also passes; final metadata acceptance remains in progress. A fresh unchanged measured preparation remains required; this
regression does not establish the entire latency or consumer-timeout cause.
WP-825 remains open, and no 24-hour run starts.

PR note: Package WP-825 (open); Tier internal for the cache implementation;
Behavior: preserve actively read historical payloads within the existing bounded
cache, refreshing retention only after authority validation; Checks: original
negative, 13 focused native tests, full storage suite, 59 architecture checks,
Clippy and formatting on C3D; Compatibility: unchanged application
interfaces, authorization, ordering, durable formats, reviewed fixtures, cache
ceilings and qualification gates; Updated handbook: Sustainable TicketDesk
milestone; Hazards/follow-ups: final metadata acceptance, unchanged preparation,
complete campaign integration, populated restore, final-source full CI and the
short campaign remain required.


A separate explicit pre-exec barrier reproduces a launcher observation race.
The original launcher closes its finite-limit proof pipe before exec, so the
owner can pin the launcher's memory image rather than the target's. The test
fails at the exact requirement that the proof stay open until target image
installation. The repair relies on the existing non-inheritable descriptor:
successful exec closes it after image installation. Launch errors still refuse
and join the owned child; no memory bound or counter check is weakened.

All 13 persistent-owner tests, nine process-roster tests and the complete
205-test lifecycle suite pass on C3D with the repair. The deterministic barrier
uses pipes rather than synchronization sleeps. Native preparation, continuous
resource sampling and full campaign qualification remain required.

PR note: Package WP-825 (open); Tier internal for launch observation;
Behavior: complete the finite-limit launch handshake at exec before pinning
worker memory; Checks: original deterministic negative, 13 persistent-owner,
nine roster and all 205 lifecycle tests on C3D; Compatibility: unchanged
application interfaces, database semantics, durable formats, reviewed fixtures,
resource bounds and qualification gates; Updated handbook: Sustainable
TicketDesk milestone; Hazards/follow-ups: final metadata acceptance, unchanged
preparation and integrated campaign proof remain required. No 24-hour run starts.


The cache and launcher component passes final scope, internal classification and
metadata acceptance on C3D and is committed. Its unchanged ordinary release build
passes. Preparation then stops at the same consumer transport deadline before
measured load: seeding completes in 30.8 seconds, and the consumer exits after
30.8 seconds. There is no external heavy-work interference, panic, populated
restore result or full-CI success. The failed receipt remains evidence.

A separate diagnostic permits a debugger attachment only after the original
consumer reports failure. This repetition instead seeds in 116.1 seconds and
consumes all 1,024 selected seed events with zero duplicates. No debugger attaches.
It is stopped before load using the exact owned controller; its children close
through the normal cleanup path. The diagnostic supplies no preparation gate,
latency result or resource qualification and does not supersede the failures.

Inspection finds that native protected replay first authorizes up to 1,024 event
candidates, then retains only the prefix through the requested admitted limit.
This workload requests 32 visible events. The candidate repair evaluates that
same prefix incrementally, keeping every denied observation before the last
selected visible event. The candidate ceiling and existing final transaction-
current recheck remain unchanged. All-selected authorization for lease operations
remains separate. The existing authorization, anchor, current-row, relationship,
capability and error checks still govern every required observation.

The maintainer's existing approval to cover necessary database fixes extends
WP-825's implementation paths to query-executor sources for this prefix walk and
its unit regressions. It grants no change to authority rules, public surfaces,
formats, ordering or qualification gates. The scope extension is recorded in a
separate predecessor commit after scope-only metadata acceptance on C3D.
WP-825 stays open, and no 24-hour run starts.


The deterministic eager-walk regression reaches the intended assertion on C3D:
a 32-item visible prefix is selected, but the original walk evaluates all 1,024
candidates before discarding the suffix. The candidate uses the same evaluator
and neutral observation construction, stopping once the selected admitted count
is reached. Hidden observations before that point remain in order. The final
backend still receives the original admitted-prefix truncation and revalidates
all selected observations under its mutation fence. Full authorization remains
the path for lease candidates. Seven prefix regressions, the full query-executor
suite and ten row-policy conformance tests pass on C3D. The next protected-storage
command initially names the wrong package and runs no tests; its failed receipt
is preserved. A continuation over the exact same formatted Rust sources runs the
correct protected-consumer test successfully, followed by the server suite,
Clippy and formatting. Final metadata acceptance remains required.

PR note: Package WP-825 (open); Tier internal for prefix evaluation;
Behavior: avoid authorizing the unused suffix beyond a protected replay's
selected visible prefix; Checks: intended eager-walk negative, seven focused
regressions, full query-executor suite, ten row-policy conformance tests,
protected-consumer test, server suite, Clippy and formatting on C3D; final
metadata acceptance required; Compatibility: unchanged authority rules, final safe
point, hidden-row semantics, candidate ceiling, public interfaces, formats,
reviewed fixtures, ordering and qualification gates; Updated handbook:
Sustainable TicketDesk milestone; Hazards/follow-ups: final acceptance,
unchanged preparation, complete fault controller, populated restore, final-source
full CI and the short campaign remain required. No 24-hour run starts.


Main integration reconciles 18 missing commits, including checkpoint root
validation once per ordinary batch. The follower acknowledgement record is
renumbered from ADR-0280 to ADR-0283 under explicit maintainer approval dated
2026-10-09. Its accepted decisions and those of the bootstrap record remain
byte-identical; original acceptance history is retained. Main's extent-pacing
record keeps ADR-0280. Handbook references use the reconciled follower identity.

The merge preserves borrowed canonical receipt inspection, one canonical
encoding when receipt bytes are persisted, and stub-only preparation otherwise.
Source cuts still run their complete validators and carry the exact retained
floor into the following frame. The new mixed audit/cut/audit regression reaches
the intended CorruptData refusal without that carry, then passes with it.
C3D passes both focused checkpoint regressions, the full storage API and backend
suites, Clippy and formatting. Final integrated acceptance remains a separate
required check; these tests establish no preparation, full-CI or campaign result.

PR note: Package none (main integration of WP-825 and completed WP-828);
Tier guarantee; Behavior added or changed: preserve both branches' accepted
storage behavior, reconcile their checkpoint paths and accepted ADR identities;
Checks: C3D standalone metadata acceptance and native merge regressions, full
storage suites, Clippy and formatting; final integrated acceptance required;
Compatibility: no new format, fixture or public interface change beyond the
accepted branch implementations; Updated handbook: Sustainable TicketDesk and
follower/bootstrap review pages; Hazards/follow-ups: repeat unchanged
preparation on integrated source, retain ADR-0282 as proposed, complete full CI
and the short fault campaign. WP-825 and WP-826 stay open.


The next main synchronization integrates completed WP-829 and its accepted
ADR-0281 transaction-level replay/drain root validation. The bootstrap custody
record is renumbered to ADR-0284 under the maintainer's explicit 2026-10-09
identifier-only approval. Its complete Decision section is byte-identical to
its original accepted ADR-0281 text, whose acceptance history is retained.
Main's replay decision and performance record keep their ADR-0281 identity.

The native reconciliation preserves WP-825's bounded two-pass streaming
recovery and applies the chained materialization to its actual write
transaction. Every source-cut frame still receives complete bounded proof
validation before mutation and exact root verification after staging. Its
changed retained floor becomes the predecessor for the next frame. Two new
real recovery/drain regressions reach Corrupt/CorruptData without that carry;
the existing mixed checkpoint test still passes in the negative candidate.
The positive candidate, three ordinary root-count regressions, full backend
suite, Clippy and formatting require C3D confirmation before branch advancement.

The preceding unchanged preparation over the earlier main integration builds
ordinary release binaries, then fails after 1,731.3 seconds with the closed
resource_process_inventory refusal. There is no external heavy interference.
Four complete five-minute windows each settle all 30,000 offered commands and
120,000 offered reads without retry. Their p95 values are 30,534, 99,635,
108,705 and 82,702 microseconds; p99 values are 62,069, 126,305, 157,448 and
136,569 microseconds. One window exceeds the normal p95 ceiling; outage-window
classification, full reconciliation and qualification are incomplete. The last
memory sample is at elapsed 1,377.263 seconds; the server has accumulated 15
task-census restarts. The generic refusal does not identify the failing check,
so census exhaustion remains a hypothesis. The failed source, artifacts,
observations and dataset are preserved. No populated restore or full CI ran.
ADR-0282 remains proposed, without a finite-handoff implementation or acceptance.

PR note: Package none (main integration of WP-825 and completed WP-829);
Tier guarantee; Behavior: preserve streaming recovery, complete source cuts and
main's transaction-level root validation while reconciling accepted identities;
Checks: C3D intended replay/drain negatives and approved metadata acceptance;
positive native suites and final integrated acceptance required; Compatibility:
no new formats, fixtures, public interfaces, workload, deadlines or gates;
Updated handbook: bootstrap review, summary and Sustainable TicketDesk evidence;
Hazards/follow-ups: classify the resource refusal, resolve measured latency,
repeat unchanged preparation, complete populated restore, full CI and the
required short fault campaign. WP-825 and WP-826 stay open; no 24-hour run starts.


The resource-accounting follow-up adds closed refusal context rather than
changing census behavior. A diagnostic negative first reaches the missing
observation interface. The corrected source passes 17 owned-process tests,
32 preparation tests and all 209 controller tests on C3D. An actual child test
proves the original private refusal record survives a second cleanup error and
the load child is reaped. Arbitrary input and invalid diagnostic context are
redacted; no PID, process identity or path enters the report. The earlier
zero-test setup remains failed and is not counted as negative evidence.
No retry, process/thread ceiling, workload, deadline or qualification gate
changes. The new report has yet to identify the original run's generic refusal.

PR note: Package WP-825 (open); Tier internal for refusal diagnostics;
Behavior: preserve bounded private failure reasons before cleanup while retaining
the original fail-closed error and every observation bound; Checks: C3D intended
negative, 17 owned-process, 32 preparation and 209 controller tests; final
integrated acceptance required; Compatibility: no application interface, format,
fixture or qualification change; Updated handbook: Sustainable TicketDesk;
Hazards/follow-ups: repeat unchanged preparation and resolve actual measured
refusals and latency before any rehearsal qualification.


The reconciled main integration passes three actual mixed source-cut tests,
three ordinary root-count tests, the full native backend suite including
doctests, Clippy and formatting on the required host. The clean integrated
source passes all 13 final scoped acceptance checks. An earlier draft's failed
doctest command remains failed evidence and supplies no qualifying result.

The unchanged preparation then passes on that integrated source. Its five
five-minute windows each settle all 30,000 offered commands and 120,000 reads
without retries or post-duration completions. Window p95 values are 30,455,
98,728, 96,744, 71,881 and 88,887 microseconds; p99 values are 48,429, 126,364,
155,153, 114,255 and 136,422 microseconds. Native observations, archive and
independent ticket-state reconciliation complete without external heavy-work
interference. This preparation result establishes neither the complete fault
campaign nor its three physical retirements and six final quiet hours.

The subsequent private populated-restore helper stops before backup creation
because it expects `sync` rather than the actual standard-profile `group`
durability label. The original command replays with the witnessed sequence,
contract version and Updated outcome. SPEC 10.5 and the explicit production
component graph permit and select `group`; no database semantics are changed.
A separately frozen helper changes only its two exact durability expectations
to `group`, retaining every original identity, independent state reconciliation
and 60-second first-successful-write check. The first corrected retry waits
for other host activity and encounters expired one-hour preparation credentials
before maintenance principal creation. Both failures and the original source,
dataset and artifacts remain intact. A fresh unchanged preparation with the
same verified binaries encounters external benchmark activity during load and
cannot qualify. The maintainer pauses this attempt; its owned children close,
and its queued follow-up acceptance is cancelled. The failed receipt retains
both the interference samples and explicit cancellation. The maintainer then
reserves the host for WP-825 and resumes work. No full-CI, restore or 24-hour
success is claimed.

The memory collector now consumes the actual campaign replacement roster while
preserving its original global clock, sample count, per-window coverage and
75-second gap. An intended missing-interface failure precedes twelve roster,
five memory and all 212 controller passes on the required host. A separate
seven-role case also passes under the unchanged 4-KiB bound and retains both
transient owners' exit peaks. Final acceptance remains required.

PR note: Package WP-825 (open); Tier internal; Behavior: connect continuous
memory observation to exact declared replacements and record actual preparation
and restore outcomes; Checks: required-host missing-interface negative, roster
and memory suites, 212 controller tests and seven-role bound case; final
acceptance pending; Compatibility: unchanged native behavior, application
interfaces, formats, reviewed fixtures, workload and qualification thresholds;
Updated handbook: Sustainable TicketDesk milestone and live observations;
Hazards/follow-ups: fresh populated restore, full CI, complete fault controller,
required short campaign and independent resource reconciliation remain open.


Further review of the successful preparation finds five distinct native
retirement witnesses, matching five physically removed extent generations:
three recycled and two deleted. Those actual preparation observations do not
supply the required later campaign's retirement evidence. The separate source
component grows from 14,046,476 to 734,207,280 canonical receipt/key bytes during
load, while eligible logical values and removed keys fall from 66,872,540 to
506,419 bytes. At shutdown, the source resume is 126,143 against an independently
covered ceiling of 260,789 and tail 335,663. The source admission census reports
616 granted turns, 2,461,861 microseconds held and 1,108,954 microseconds waiting.
These observations warrant investigation of native proof, pacing and checkpoint
progress; they neither prove a sole cause nor establish six-hour stability.
The source, logical and physical bases remain independent, with unchanged
rest, sampling, foreground workload and qualification gates.

The continuous memory change passes all 213 controller tests and final scoped
acceptance on the required host. A fresh preparation using the same verified
native binaries then fails after about 359 seconds, before its first completed
load window, without observed competing heavy work. The observer reports
`observer_ack` with `storage_unavailable`; the server records an
`AuditUnavailable` runtime stop and exits unsuccessfully. The harness also
reports observer and server shutdown failures. Original data, receipts and
process-exit observations remain preserved. Restore and full CI do not run.
This is a native failure requiring investigation, not a harness retry or an
excuse to weaken fail-closed audit behavior.

The proposed controller epoch component preserves global component gaps and
bounds across actual declared process replacements. Its missing-module
regression is confirmed on the required host. Positive component and scoped
acceptance checks remain required; no complete campaign is claimed.

PR note: Package WP-825 (open); Tier internal; Behavior: bind continuous native
observations to exact declared process replacements; Checks: required-host
missing-module negative, positive component and scoped acceptance pending;
Compatibility: unchanged native runtime, interfaces, durable formats, reviewed
fixtures, workload and qualification thresholds; Updated handbook: Sustainable
TicketDesk milestone and live observations; Hazards/follow-ups: investigate
native audit failure, then populated restore, full CI and the real fault
rehearsal with complete resource reconciliation. No 24-hour run has started.

All 229 controller tests and actual scoped acceptance pass for the exact epoch
component. The native audit failure remains under investigation using a
separately preserved diagnostic build with fixed stage and error-kind messages.
The original failed preparation remains failed evidence; an instrumented or
debugger-observed run cannot qualify the measured workload.

Sampler integration adds explicit finite initial launch, exact roster-backed
successor launch and epoch collection from the owned sampler's real output.
Regression cases require a real bounded parent, native-protocol child handshake,
normal sampler shutdown, explicit exact-owner retirement and retained exit RSS.
Separate hostile-input cases require refusal of conflicting limits, wrong
sampler identity and substituted elapsed time. Intended missing-interface
negatives and positive acceptance remain required on the host; these cases do
not claim execution of the seven-fault campaign.

PR note: Package WP-825 (open); Tier internal; Behavior: connect native sampler
launch and continuous epoch collection to exact owned processes; Checks:
required-host regression and scoped acceptance pending; Compatibility: original
preparation behavior, native runtime, public interfaces, formats, reviewed
fixtures and qualification gates unchanged; Updated handbook: Sustainable
TicketDesk milestone and live observations; Hazards/follow-ups: native audit
failure, full campaign assembly, populated restore, full CI and real rehearsal
remain open.


The sampler ownership checks first stop at a missing `subprocess` import in the
new test; this supplies no intended-negative proof. After correcting the import,
both actual missing-interface regressions reach their intended errors. All 233
controller tests and actual scoped acceptance then pass for the exact six-file
sampler integration. The independently preserved audit diagnostic completes all
150,000 commands and 600,000 reads with no retries or post-duration operations,
without reproducing the native audit stop. Its instrumented build and debugger
attachment prevent qualification. The original failure remains unresolved.

Code review identifies a separate native exact-lookup defect. An isolated test
publishes one canonical command segment and stages a newer two-command segment
only in a private epoch. The newest terminal audit resolves, but its started
audit incorrectly returns absent. The actual required-host regression fails at
that exact assertion, rather than at setup or compilation. The repair retains
the tail fast path and includes the newest segment in the bounded exact-member
fallback. Positive coverage also requires exact absence, published predecessor
identity and refusal of duplicate private authority. The positive regression, full storage suite, Clippy, formatting and scoped
acceptance subsequently pass. No causal relationship to the failed preparation
is claimed.

PR note: Package WP-825 (open); Tier guarantee; Behavior: preserve exact lookup
of every private command-owned audit member without consuming the newest segment
in the fast path; Checks: required-host intended negative, positive regression, full storage, Clippy,
formatting and scoped acceptance pass;
Compatibility: unchanged authorization, ordering, atomicity, durable formats,
reviewed fixtures, public interfaces, workload and qualification thresholds;
Updated handbook: Sustainable TicketDesk milestone and live observations;
Hazards/follow-ups: native audit-stop cause, populated restore, full final-source
CI, complete fault runner and real short rehearsal remain open.


A second separately instrumented diagnostic completes all 150,000 commands and
600,000 reads without retries or post-duration operations. It records no audit
failure or first-stop diagnostic, with no debugger attached. Instrumentation
still prevents qualification, and the original failed preparation remains
unresolved evidence rather than becoming a pass.

The diagnostic retains 25 observations in each native byte component and
records six physical retirements over the whole preparation, including preload.
The qualified source-prefix bytes rise from 21,062,904 at load second 21.7 to
159,242,142 at second 1490.1. The interval and ceiling also change; this does not
measure every later unqualified source receipt or establish six-hour stability.
The final physical sample reports zero eligible allocated bytes but 1,419,018,240
reference-protected allocated bytes. Those bases remain distinct. Real physical
progress cannot substitute for source accounting, and an observed native
keep-up limitation must be fixed rather than excused by a per-command allowance.

The log-ownership regression reaches the missing binding interface on the
predecessor. All 244 controller tests pass after integration. Collection binds
actual stderr ownership, preserves complete retired records and replacement
startup records, and keeps every existing global observation gap. Actual bounded
parent/child tests supplement the explicitly numeric fixtures. The initial
administrative-owner negative is one missing-module loader error, with zero
actual test functions executed; it is absence evidence, not a semantic negative.
All 249 controller tests then pass, including five administrative-owner
cases and actual short-child exit/output-bound checks. A further actual
regression proves that the preceding helper rejects the native backup/restore
uncertainty protocol. The repair admits only exit 3 with the exact registered
uncertainty class, poll action and canonical maintenance operation ID. Other
nonzero exits remain fatal, and no uncertainty is classified as action success
or permission to retry the mutation. All 250 controller tests pass afterward,
including hostile maintenance evidence.

PR note: Package WP-825 (open); Tier internal; Behavior: bind continuous native
logs across exact observed replacements and serialize bounded public CLI child
ownership with retained exit peaks; Checks: required-host intended missing-log
interface, administrative missing-module absence, typed maintenance uncertainty, all 250 controller tests and final scoped
acceptance; Compatibility: unchanged native runtime, public
interfaces, formats, reviewed fixtures, request slots and qualification gates;
Updated handbook: Sustainable TicketDesk milestone and live observations;
Hazards/follow-ups: assemble and validate actual fault execution and maintenance
recovery, complete resource reconciliation, ordinary preparation, populated
restore, final-source full CI and the real short rehearsal. No 24-hour run has
started.
