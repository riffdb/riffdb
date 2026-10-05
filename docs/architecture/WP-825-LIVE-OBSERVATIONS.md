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
