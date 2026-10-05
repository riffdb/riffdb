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
