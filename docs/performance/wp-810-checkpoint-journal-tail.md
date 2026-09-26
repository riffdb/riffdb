# WP-810: checkpoint-coupled journal tail latency

Identifier correction: this package was first registered locally as WP-806,
then WP-808 and WP-809. Main independently assigned those numbers to the
semantic write-path comparison, governance verification, and same-root revision
fence tasks. WP-810 preserves this package's original checkpoint scope and evidence.

**Open storage/journal package.** A registered asynchronous checkpoint makes
the Standard-profile writer drain journal durability and ordered publication
before continuing. This shared writer path can stall command acknowledgement
without any archive consumer. **WP-749 is now blocked on reduction of this shared
term**, per maintainer direction on 2026-09-21. WP-810 owns its repair;
WP-749 subsequently assesses its separate archive-presence gate by the defined
direct off/on measurement. WP-810 does not depend on WP-749.

## Established finding

The valid archive-disabled cell from the
[isolated checkpoint-wait comparison](wp749-archive-isolated-pair-2026-09.md)
contains 263,888 measured successful commands in 16,607 groups. Of those groups,
**123 wait over 50 ms** for their first journal receipt and **158 wait over
10 ms**. Every one has the actual checkpoint drain predicate set. With that
predicate false, the maximum first wait is only 4.036 ms.

| Archive-disabled observation | Value |
| --- | ---: |
| Raw command acknowledgement P99 | 88.347 ms |
| First receipt wait P99, checkpoint drain required | 56.855 ms |
| First receipt wait P99, no checkpoint drain required | 1.068 ms |
| Maximum first receipt wait | 160.026 ms |
| Maximum journal sync | 159.922 ms |
| P99-tail requests whose own group requires drain | 95.72% |
| Checkpoints, including warmup | 98 |
| Checkpoint transitions, minimum–maximum | 3,008–3,327 |
| Byte-triggered / transition-triggered selections | 98 / 0 |

The measured source makes the connection explicit:
`RedbStore` consults `async_checkpoint_in_flight` when setting the submitted
group's drain requirement; the commit writer's `POST_DRAIN_ALL` then waits for
completion. The real journal media writes bytes and performs durability sync;
the receipt completes before storage publishes the covered frontier and the
command response becomes eligible. A checkpoint's installation must preserve
the exact checkpoint-plus-suffix view and ordered publication. Removing the
drain without placing that guarantee elsewhere is not a permissible repair.

The trace names the blocking path. It does not identify the kernel/device
reason for a long sync, nor quantify this cost on every workload. The path is
shared by Standard-profile writers; the measured magnitude is from c32
write-only on the full app-baseline dataset. Hardened-profile behavior is not
inferred from this evidence.

## Evidence already sufficient to open the package

Frozen source: `be189cac4325c77303c5b4fe749ef9b73f4e4b42` plus the retained bounded
observational patch. Rust 1.98.1, thin LTO, one codegen unit, generic x86-64;
8-vCPU AMD EPYC 9B14 with required SHA-NI, AES, PCLMULQDQ and SSE4.2. The cell used
15 seconds warmup and 90 seconds measurement, with valid host checks and zero
errors, unavailable outcomes or dropped trace evidence.

`release/evidence/wp-749/archive-ack-isolated-20260921/summary.json` is the
existing immutable evidence bank. Its disabled-cell entry retains exact
source/binary/raw-input hashes, all checkpoint events, the slowest twenty
requests, group and sync/wait distributions, host checks and reconciliation.
The raw request arrays and traces have verified private copies. The evidence
stays at its original path so this handoff does not manufacture another run.

The repaired receipt counter includes the first wait. Deduplicated journal
sync totals and receipt-wait calls reconcile against the same trace. Accounting
still requires the [writer-census warnings](wp749-writer-census-defects.md):
wait and sync overlap, request-weighted shared group durations are not writer
wall, and the client time outside its own submitted group remains unattributed.
For the disabled P99 population, that remainder averages **72.272 ms**. Do not
assume it is all checkpoint-induced queueing.

The [archive report](wp749-archive-isolated-pair-2026-09.md#the-pre-sync-write-finding-survives-isolation)
separately preserves the enabled **141.387 ms journal write inside a
142.265 ms receipt wait**, with only 0.799 ms of sync. That is a distinct
pre-sync blocking observation on the same wait path, not evidence that archive
created the shared checkpoint dependency.

## Scope and next owner's work

The manifest entry owns a deterministic production-path reproduction,
guarantee-preserving disposition, recovery/crash coverage, and qualification
of any eventual mitigation. Existing ADR-0101/0104/0132 decisions remain in
force. No new codec, durable identity, cadence, bound, response ordering or
public surface is authorized by registration. A required decision change must
first receive exact-text review under AGENTS.md and D-003.

Registration needs **no further measurement** from the WP-749 investigation.
The later maintainer disposition makes this work WP-749's prerequisite and stops
that investigation; it does not start a campaign or reserve a host. A future
implementation must re-baseline its actual source/toolchain
under ADR-0239 and preserve ADR-0249's named-mechanism disclosure discipline.
No P99 threshold is invented here.

## Candidate repair, 2026-09-25

The local candidate keeps the ordered installation barrier while allowing the
existing bounded pipeline to run during checkpoint materialization. It changes
no checkpoint threshold, journal or capsule encoding, durability worker,
publication order, or acknowledgement requirement. The bounded comparison below
shows higher throughput and lower P50/P95, with mixed P99 movement. **WP-810
remains open and WP-749 remains blocked**; removing the writer drain has not
established reduction of the shared acknowledgement tail.

Implementation decisions within ADR-0101/0104/0132:

- A pending materializer alone no longer sets `requires_pipeline_drain`.
  The epoch-opening poll requests that drain after it observes successful
  materialization with unpublished predecessors. Completion racing a seal is
  observed at the next epoch opening. If the prefix is already quiescent, the
  existing poll can install directly before new apply.
- Installation still requires zero unpublished transitions and bytes, exact
  covered-root/frontier/hash checks, and publication of the rebased view before
  the covered extent is recycled. A completed checkpoint is not permission to
  acknowledge an unpublished command.
- At capacity pressure, the sole writer publishes the already-submitted FIFO
  prefix before retrying capacity or checkpoint installation. The staged next
  frame is excluded. Original completion tickets retain their results. The
  existing transition, encoded-byte, physical-extent and overlay bounds remain
  enforced; no larger queue or reserve is introduced.
- An observed checkpoint failure fences new apply immediately, even if earlier
  durable groups still await publication. Quiescence gates successful
  installation, not error detection.

The deterministic real-storage test failed on the old predicate and passes on
the candidate. Separate controls pause checkpoint materialization, the actual
journal sync, and ordered publication. The production coordinator test proves
that the next command can reach apply and seal while its predecessor's journal
sync is held, with both responses and application notifications withheld until
ordered publication. The fixture has no sleeps or replacement I/O results.

`tests/storage_recovery/checkpoint_pipeline.rs` also exercises the actual
256-transition unpublished ceiling and the 257th transition's backpressure,
original completion tickets after a pressure drain, and immediate reads of
entities, outcomes, commits, provenance and events. A checkpoint-failure arm
proves refusal of new apply with an outstanding predecessor. Six abrupt
process-exit cases cover before journal sync, durable-but-unpublished state,
checkpoint staging, checkpoint durability, installed-before-reclaim, and
reclaimed extent. Every case reopens twice and checks atomic, idempotent
authority without extra commits, events or outcomes. Before sync, an
unacknowledged complete frame may survive or disappear; both outcomes must be
atomic and stable on the second restart.

Local qualification exercised 1,315 storage/commit tests: 1,313 passed in the
broad run and two existing promotion/bootstrap crash tests reached its
120-second watchdog. Both passed in a serial rerun, together with all six new
schedule/recovery tests, without changing a deadline or assertion. One existing
test remains skipped by the normal runner. A fixture-only Clippy allowance was
corrected before the passing lint recheck. The initial failures remain in the
private qualification bank; they are not represented as a clean first run.

The bounded measurement plan is three counterbalanced control/candidate pairs,
c32 write-only, 15 seconds warmup and 90 seconds measurement per cell. Both
arms use the same source baseline, Rust 1.98.1, release flags and common
observation patch, with one client binary and distinct server digests. Retain
all attempts, exact raw P50/P95/P99 samples, throughput, checkpoint counts and
corrected waits. Reconcile disjoint writer stages against an independently
retained iteration sum and continuous span, with signed remainders; busy plus
idle is not wall time. A local debug smoke check verifies evidence plumbing
only and supplies no performance conclusion.

### Bounded comparison result

All six fixed cells completed, with zero application errors, unavailable
outcomes, checkpoint failures or dropped raw samples. Every host interval and
signed level-0/1 reconciliation passed. No retries or cells were discarded.
The evidence bank at `release/evidence/wp-810/checkpoint-pipeline-20260925/`
retains source and executable identities, exact raw percentiles, all slowest
twenty samples, checkpoint counts, timing windows and private-input digests.

The frozen control is `d9ae2633015501b0d36f2753c894b1b7a1edb905`; the frozen
candidate is `66c394fc755e57ec774bba5c4b6de830bddacd90`. Both use Rust 1.98.1,
thin LTO, one codegen unit, generic x86-64 and the same observation patch on
8-vCPU AMD EPYC 9B14 hardware with the required instruction features. Server
digests differ, the client digest is shared, and all six raw sample arrays have
different digests. This is a bounded scheduling comparison, not a complete
alpha sweep or an archive off/on qualification.

| Cell | Arm | Commands/s | P50 ms | P95 ms | P99 ms | Checkpoints started |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Control | 3,016 | 8.417 | 12.544 | 81.886 | 99 |
| 2 | Candidate | 3,180 | 8.001 | 10.962 | 101.052 | 106 |
| 3 | Candidate | 3,122 | 8.174 | 11.252 | 94.750 | 104 |
| 4 | Control | 3,002 | 8.373 | 12.846 | 86.203 | 100 |
| 5 | Control | 3,023 | 8.249 | 12.360 | 105.191 | 100 |
| 6 | Candidate | 3,176 | 8.018 | 10.694 | 92.216 | 106 |

Median paired changes are **+5.06% throughput, −2.80% P50 and −12.62% P95**.
P99 changes are **+23.40%, +9.92% and −12.33%**, with a +9.92% median paired
change. Do not turn that mixed tail result into a reduction claim, a numeric
P99 gate, or a precise attribution from aggregate counters. P50/P95 improve
in every pair; three-run `(max − min) / median` spreads are at most 5.10% for
those two percentiles. P99 spread is 27.03% in control and 9.33% in candidate.

### What changed, and what remains

The production-path census confirms the scheduling change ran: total
`POST_DRAIN_ALL` falls from **23.859–25.002 seconds** per control cell to
**0.034–0.048 seconds** per candidate cell, over approximately 105 seconds of
independently measured writer span. The median per-iteration value falls from
1.218 ms to 0.002 ms. This is a reduction in that particular writer barrier,
not elimination of journal service time.

| Observation | Control range | Candidate range |
| --- | ---: | ---: |
| Journal queue time, mean per frame | 22.8–25.4 µs | 983.6–1,051.5 µs |
| Journal sync time, mean per flush | 1.405–1.454 ms | 1.507–1.548 ms |
| Maximum journal sync | 155.118–167.948 ms | 159.654–160.906 ms |
| Writer pre-execution drain, total | 0.589–0.664 s | 1.756–2.431 s |
| Writer work-receive stage, total | 1.558–2.407 s | 18.465–19.879 s |
| Complete frame bytes per command | 5,515.6–5,516.1 | 5,517.3–5,517.7 |

`JournalLane::submit` timestamps each frame; the journal census measures from
that timestamp to the start of physical encoding. The sole journal worker
finishes the current write and sync before receiving the next batch. Allowing
foreground apply during materialization permits frames to queue behind that
work, while acknowledgement still waits for durability and ordered publication.
The larger queue times and persistent long syncs are evidence that the journal
delay remains after the foreground barrier is removed. They do **not** identify
the kernel/device cause or assign the exact P99 movement to individual requests.
The candidate's tail variation remains unresolved at that finer granularity.

All checkpoint selections are byte-triggered, at 2,992–3,328 transitions across
the six cells. Thresholds are unchanged. Counts include warmup and precede
graceful close, so installed counts can trail materialized counts by one at the
snapshot. Journal/publication timers overlap the writer and each other; none
is added to writer wall. The earlier 141.387 ms positional-write observation
remains separate evidence: these cells instead retain long sync maxima, with
maximum positional writes of 4.812 ms in control and 0.865 ms in candidate.

### Timing remainder and instrument limits

Signed level-0 and level-1 reconciliation errors are zero in every cell. The
between-iteration remainder is 16.7–19.3 ms per cell and the within-iteration
level-0 residual is 8.4–10.6 ms. Level-1 unnamed time remains inside
`unit_execute`: 5.949–6.248 seconds in control and 1.255–1.561 seconds in
candidate. Adding those three disjoint unnamed portions leaves **5.69–5.98%**
of control writer span and **1.22–1.51%** of candidate writer span unattributed.
The evidence retains all six signed values, including the invalid level-2
parent mapping; level 2 is excluded from attribution.

Busy plus idle undercounts continuous span by **22.91–23.99%** in control and
**0.235–0.246%** in candidate. W1 remains an instrument defect: its smaller
undercount after changing the schedule is not a repair or a portable correction
factor. See the [writer-census warnings](wp749-writer-census-defects.md).

### Checkpoint and next decision

#### Retained trace narrows the next investigation

Reanalysis on 2026-09-26 checks actual sync intervals against checkpoint
registration, materialization completion and installation in the already
qualified isolated pair. This is the historical source `be189cac4325`; it is
**not new timing evidence for the pipeline candidate**. Input hashes match the
original public bank. Shared physical flushes are counted once, and selection
uses the measured command-sequence range.

| Journal sync observation | Archive disabled | Archive enabled |
| --- | ---: | ---: |
| Measured physical flushes | 16,607 | 15,198 |
| Over 10 ms, overlapping materialization | 158 / 158 | 173 / 173 |
| Over 50 ms, overlapping materialization | 123 / 123 | 116 / 116 |
| Over 10 ms, extending into post-materialization | 0 | 1 |
| Maximum with no registered checkpoint overlap | 3.917 ms | 2.521 ms |

All 158 disabled slow syncs finish before their checkpoint materializes. The
159.922 ms maximum finishes 1.967 ms before that event. This localizes the
association to the materialization interval rather than installation or
reclamation. The separate **141.387 ms positional write inside a 142.265 ms
receipt wait** remains the earlier pre-sync observation; it is not reclassified
as a sync here.

Source inspection at integration `a8b3737d9` identifies the next boundary to
observe: `materialize_checkpoint_batch` begins one redb transaction, replays
the retained frames, and calls `commit_durable` with `Immediate` durability
and two-phase commit disabled. Concurrently, `journal_worker` calls the real
file's `sync_data`; it dequeues no successor while that syscall is pending.
The trace's registration-to-materialized interval includes replay **and**
durable commit. It cannot distinguish their effects or establish a
kernel/device cause. The next useful diagnostic must separate checkpoint
replay, page writes and engine sync, while retaining journal write/sync
intervals and request/group joins on the same source. Aggregate queue means
or another subtraction of P99 values cannot answer that question.

The reproducible analyzer, seven boundary/refusal tests, every slow flush and
its checkpoint boundaries are retained under
`release/evidence/wp-810/checkpoint-overlap-20260926/`. No remote work or new
host reservation was used for this analysis. It narrows the next investigation;
it neither resolves the candidate's mixed P99 nor closes either package.

#### Materialization diagnostic prepared

Integration `5d9b8cc4c` adds committed main `539330cec`. An observation-only
patch against that source now separates checkpoint transaction setup, replay
and durable commit, and records the real engine's read/write/resize/sync calls
alongside journal flushes and request/group identities. It uses the production
daemon constructor and the existing redb file backend with unchanged open
flags, locking, bytes and error handling. The patch remains outside the
production candidate.

Local functional validation joins 9,504 measured requests to 594 groups and
flushes, retaining three checkpoints and 65,912 engine operations without
dropped records. Journal write, sync and queue timing reconciles exactly with
the existing census. The pre-close census reports two completed checkpoints;
the final trace includes the third completing after the last publication.
These populations remain separate. All collector, backend, raw-sample and
analyzer checks pass; this debug-build check is not performance evidence.

The prepared diagnostic and qualification are retained under
`release/evidence/wp-810/materialization-probe-20260926/`. They freeze one
90-second archive-disabled c32 cell, after a 15-second warmup, on
admitted hardware with Rust 1.98.1. Trace output stays in RAM during measurement.
The analyzer reports phase and I/O overlap and explicit unexplained time; it
does not infer device causation from overlap. That original preparation had
no remote result or mitigation-benefit claim.

The same observation patch is now qualified on integration `3514c4af2` using
the exact release binaries built for the remote diagnostic. A local
90-second functional workload joins 2,652 requests to 221 groups and flushes,
with one completed checkpoint and 16,856 backend operations; no rows are
dropped. Earlier attempts remain retained: one debug seed stopped with
`AuditUnavailable`, and two clean short workloads were refused because they
did not trigger a checkpoint. The first stop's cause is not established.
Only the local functional duration was extended; the real thresholds and
coverage assertions are unchanged. The refreshed qualification and frozen
protocol are in the same evidence directory. These local observations are
not performance evidence.

#### Remote diagnostic stopped during setup

The one authorized remote attempt on `3514c4af2` stopped at its fixed
900-second cell deadline during full-dataset seeding, before warmup or the
90-second c32 workload. The last seed progress report was 7,837 of 19,220
commands at 785.391 seconds; batch-completion reporting does not establish
the final applied-command count. This remote trace does not establish the
slow seed's cause; the subsequent local CPU diagnosis is recorded below.
There was no retry or deadline extension.

The last periodic checkpoint snapshot, at process elapsed 599.001 seconds,
records two byte-triggered selections, both materialized and installed, with
2,921 and 3,572 selected transitions and no checkpoint failures. These are
partial observations: the reporter reached its fixed 600-sample limit before
the cell stopped. The seed trace retains 188 groups, two checkpoints and
22,590 backend operations, but no final coverage footer. The analyzer therefore
refuses completeness. No measured request samples, workload report or
postflight qualification exist, and no latency or materialization attribution
is claimed from this attempt.

`release/evidence/wp-810/materialization-attempt-20260926.json` retains the
source/binary identities, snapshot limits and private evidence digests.
All 60 retained files, including the failed database snapshot, were verified
locally before removing the remote source, build and data directories. The
owned processes stopped, firewall and prior timer states were verified
restored, and the host was released. This attempt does not resolve WP-810's
shared delay or WP-749's prerequisite.

#### Local seed diagnosis

A bounded local reproduction on production source `09188882f`, with no
checkpoint-I/O observation patch, identifies repeated admission-history
decoding during full-dataset seeding. The 120-second frame-pointer CPU sample
retains 12,188 samples and loses none. Inclusive
`decode_command_segment_v1` samples account for 52.22% on the command writer
and 41.34% on the idempotency inspection thread: 93.56% across those disjoint
threads. These are sampled CPU proportions, not a writer wall-clock tiling.
Nested call-stack percentages must not be added.

`AdmissionRepository::lookup_admission_group` pins one composite snapshot,
but each novel identity independently decodes the same bounded suffix after
the derived-member, durable-locator and index-coverage checks miss. The cold
index state is supported behavior after bounded startup. No checkpoint
selection occurs in this 120-second sample, so checkpoint I/O cannot explain
the sampled CPU work. A separate observer-disabled release attempt records
its first byte-triggered checkpoint at 158 seconds and installation at
169 seconds; it is not evidence that the entire seed runs without checkpoints.

The regression test `cold_admission_batch_scans_its_pinned_suffix_once`
initially records nine scans for ten lookups against one snapshot. A local
prototype reuses only the requested keys proved absent by that complete
suffix scan, within that one call. Each identity still checks its derived
member and exact durable locator first. Positive suffix matches retain their
existing decoding and validation path. The batch stores no segment payloads,
warms no population index, and discards its results on return. This is not
the persistent fresh-locator coverage removed by ADR-0236.

Single-request writer rechecks also account for substantial CPU and cannot
share this batch's scan. The prototype passes all 185 storage-recovery tests
and eight scoped acceptance checks. A second fixed 120-second CPU sample
loses no samples, but still does not complete full seeding; writer rechecks
account for 97.39% of its samples. These proportions identify the work left
after batch reuse, not a qualified speedup. The tested patch is retained as
`release/evidence/wp-810/cold-admission-batch-prototype.patch`, outside the
production candidate. No admission optimization is selected as a WP-810
mitigation and no setup-success claim is made.

Passing the regression test does not establish that
the prototype fixes setup or improves checkpoint latency. The reproduction
uses Rust 1.98.1 and a local AMD Ryzen 9 7950X with the required accelerators;
it has no host-validity qualification or performance verdict. Source, binary,
sampling and retained-evidence identities are in
`release/evidence/wp-810/seed-diagnosis-20260926.json`.

#### Integration qualification

The fixed comparison is complete and the host is released with its original
firewall/timer state restored. All 124 retained private evidence files were
hash-verified locally before removing the remote task's source, build and data
directories. The temporary observer and current-main control worktrees were
also removed.

Full CI on the frozen candidate stopped at two startup/restart failures already
seen on its older main baseline. Newer main `408cbff3b` fixes both: its clean
control passed all seven tests in that target. Local integration `fcc6e1183`
includes that main and passes the six WP-810 schedule/recovery tests plus all
seven startup/restart tests. Integration `a8b3737d9` additionally merges
committed main `0d9edbce0`, including accepted ADR-0260's bounded V2 dirty
startup. Full CI on that integration passes workspace Clippy and 3,975 tests
before stopping at
`corpus::regression_corpus_replays_and_reproduces_its_territory`. Seed
`0x51c2c067` records 29 torn decisions against a retained minimum of 30. The
campaign completes with 14 recoveries and its oracle intact; the failure is
the pinned coverage assertion. The exact test on clean main `0d9edbce0`
produces the same report and fails at the same assertion. Both logs and source
identities are retained; the temporary control worktree was removed. No
assertion is relaxed, and the earlier WP-749 landing exception does not apply
to this package. That invocation failed and its later commands did not run.
The integrated source has no performance measurement; the timing verdict
remains bound to the frozen candidate above.

Scoped storage/commit qualification runs 1,316 tests: 1,314 pass concurrently;
the same two promotion/bootstrap cases named in the earlier qualification hit
the 120-second watchdog. Both pass in a serial rerun totaling 49.209 seconds,
with unchanged deadlines and assertions. One existing test is skipped by the
normal runner. The concurrent failure remains recorded; the serial result does
not relabel that first invocation as a pass. The overlap analyzer's seven
boundary/refusal tests also pass. These results do not waive the independently
reproduced full-CI corpus failure. All eleven non-test acceptance steps pass,
including scoped Clippy, generated artifacts, handbook and dependency checks.

Integration `ff7dd3036` additionally brings in committed main `ef0164e41`.
The subsequent complete simulator audit identifies the corpus failure's exact
cause: accepted ADR-0260 implementation `0d9edbce0` changes recovery's physical
operation stream. Its immediate parent passes every corpus expectation; its
child produces six stale historical classifications and exactly matches the
current integration's complete audit. Twelve copied-binary repeats are
identical. Six causal movement references repair the corpus without changing
any seed, expectation, numeric threshold, successor, retirement or oracle.
The exact gate now passes with the full workspace's all-feature configuration.
The source/binary identities, full audit rows, repeatability and refusal checks
are retained in `release/evidence/wp-810/corpus-receipts-20260926/`.
This removes the earlier first CI blocker; it does not turn the earlier
aborted full-CI run into a pass.

Full CI subsequently passes on `3514c4af2`, with 6,134 reported test passes
including repeated checks and crash children. An intervening invocation
stopped at the expansion proof because Cargo treated an older `riffdb-types`
artifact in the shared build directory as fresh. The exact proof passed after
rebuilding that artifact without changing source bytes. Every tracked Rust
input was then timestamp-invalidated before the complete successful CI rerun.
The old artifact, both failed attempts and their logs remain retained. Observer
qualification now uses a separate temporary build directory.

The successful CI invocation also exposed a nonfatal tool-selection warning:
`python3 -m maturin` selected system 1.15.0 even though the executable on `PATH`
was pinned to 1.14.1. A private Python environment fixes both entry points at
1.14.1; the affected operator acceptance and both dependent generated-surface
proofs pass again with that pin. A separate supplemental invocation of main's
WP-760 dirty-recovery referee refuses its missing default evidence files;
that script is not part of this revision's CI battery. These results, explicit
limits and private-log digests are retained in
`release/evidence/wp-810/integration-qualification-20260926.json`.

#### Integration of main's index-warming repair

While the seed diagnosis was being banked, main merged `a696db66c`, followed
by WP-760's evidence and closure through `a1809310f`. That repair restores
the commit-time outbox refresh that warms the command index. It addresses
the repeated cold admission-history scans identified by the local profile.
The first write after restart again pays the one-time rebuild documented in
[startup integrity](../operations/STARTUP-INTEGRITY.md); the work is not
eliminated or moved into the measured CPU sample retroactively.

Integration `422d172ee` takes those three main commits. The batch-reuse
prototype remains outside production. The prepared diagnostic using older
binaries and a longer setup budget was superseded before execution, and its
reservation question was withdrawn. C3D remained released.

Full CI on that integration reached two existing grouped-command tests before
their fixed 10,000-yield waits observed the writer's admission-clock call.
`31aeab76` replaces those scheduling guesses with the fixture's existing
bounded first-call notification, retaining every outcome, grouping and
durability assertion. All 23 tests in that target pass; the failed complete
CI invocation is retained. Full CI then passes on `31aeab76`, with 6,133
reported test passes including repeated proofs and crash children. The merged
WP-760 default evidence now passes too. Existing legacy-ADR length and
dependency-duplicate warnings remain policy warnings, not suppressed failures.

The local debug-build functional check seeds all 19,220 commands, records zero
checkpoint failures, exits successfully and writes its complete report.
Seeding takes 35.984 seconds and the whole process 60.126 seconds on the local
host. This verifies setup completion; it is not a latency comparison against
the earlier release build or another host. The example adapter retains four
existing missing-documentation warnings on scrub diagnostic fields. Source,
binary, result and private-log identities are retained in
`release/evidence/wp-810/index-warming-qualification-20260926.json`.

`index-warming-measurement-protocol.json` in the diagnostic evidence directory
freezes `31aeab76` and restores the original 900-second full-cell deadline.
It excludes the admission prototype. After a fresh reservation, rebuilt
observer binaries must match both their build receipt and the exact-binary
local functional qualification before the unchanged 15-second warmup and
90-second measurement can start. No result is transferred from the older
binaries or source.

#### Refreshed diagnostic completed, but I/O coverage overflowed

The single reserved diagnostic on `31aeab76` completed full seeding, the
15-second warmup and the 90-second c32 write-only measurement. Both host checks
passed. All 282,570 measured commands succeeded; the process including warmup
committed 331,006 commands. There was no remote retry or deadline extension.

The analyzer **refused complete attribution**: the load process attempted
9,442,900 backend I/O observations, retained its fixed 4,194,304 rows and dropped
5,248,596. The last retained I/O ended at process elapsed 51.151 seconds. The
discarded records have no retained operation-class breakdown. The complete
run cannot be attributed from this prefix or by extrapolating its operation mix.

The pre-close census records 105 byte-triggered checkpoints, selecting between
3,011 and 3,328 transitions, with no failures: 105 materialized and 104 installed
at the snapshot. The final trace has all 105 completions and 21,000 groups,
without dropped checkpoint or group rows. Raw request values and independent
writer-wall reconciliation pass their separate checks; neither establishes
complete backend-I/O coverage. Unqualified diagnostic values are retained in
`release/evidence/wp-810/materialization-overflow-20260926.json`, with source,
binary and evidence digests. Observer overhead remains unmeasured, so these
values do not establish a mitigation effect or a new baseline.

Local qualification exposed the sizing error first: its 90-second functional
case completed 99 checkpoints but dropped 495,276 I/O records. A separately
retained 20-second case with the same binaries passed every completeness and
join assertion across 91,216 requests, 31 checkpoints and 1,178,078 I/O rows.
That shorter case proved tracing correctness, not full-cell capacity. **The
known long-case overflow should have blocked the remote run.** A future
diagnostic must qualify record and log capacity for the intended full workload
before booking a host; shortening the functional check is not that proof.

All 43 retained remote files were verified locally. Task-owned firewall and
timer changes were restored, task processes ended, and remote source, build
and data directories were removed before releasing the host. This attempt
does not answer the complete materialization question or close WP-810.

#### Full-workload capacity repair qualified locally

The replacement diagnostic in
`release/evidence/wp-810/materialization-capacity-20260926/` retains a fixed
16,777,216 I/O-row limit, a 1,312 MiB record-memory ceiling and a 2 GiB log
ceiling. The source remains `31aeab76`. Only the observer's I/O limit and
memory-bound assertion change in Rust; the production candidate is unchanged.
The earlier 4,194,304-row probe and all failed evidence remain retained.

The full local workload now passes: full dataset, c32 write-only, 15-second
warmup, 90-second measurement, 357,879 successful measured requests, 139
checkpoints and 12,924,427 I/O records. No records are dropped. The final
footer, exact request/group/flush joins, phase bounds, journal counters and
independent writer-wall reconciliation all pass. The 998,090,680-byte log
exceeds the old ceiling, confirming that both record and log sizing needed
repair. This exercises 37% more I/O records than the failed remote cell and
leaves approximately 23% of the new record capacity unused.

The launch gate now requires the full-capacity proof and both exact executable
digests. Tests show the legacy gate admitted 16 invalid cases, including a short
or smoke qualification; the new gate refuses them. Five collector tests, two
backend tests and 24 analyzer/gate/retention tests pass. Source, binary and
private evidence digests are retained in the new directory's `qualification.json`.
These are local correctness and capacity results, not qualified latency or
evidence that the shared delay is resolved. A new exclusive interval and full
qualification of the exact rebuilt remote binaries are still required before
one replacement remote cell. No additional run has started.

The remaining decision is a maintainer-reviewed resolution of the shared
journal delay and disposition of this common-case improvement. **No merge or
completion is claimed.** The earlier diagnostic failed during setup; the
refreshed diagnostic finished its workload but failed I/O coverage. Neither
answers the remaining materialization question. These are separate from
the finished comparison. The candidate does
not establish that WP-749's shared tail prerequisite is resolved. Archive
presence must still be assessed separately after that prerequisite is met.

### Candidate PR note

Package WP-810; tier guarantee. Behavior: retain bounded pipelining during
checkpoint materialization and the ordered drain for installation; publish
existing predecessors under capacity pressure and fence observed checkpoint
errors immediately. Checks: deterministic real coordinator/storage schedules,
six process-crash edges, scoped storage/commit tests, path-triggered acceptance,
six valid comparison cells, current-main integration qualification, the
retained-trace overlap checks and the causally verified corpus-receipt repair.
The complete CI battery passes on `31aeab76`, including the merged dirty-recovery
evidence and pinned Python proofs; the full-seed functional check also passes.
The [draft implementation/fixture review](../architecture/WP-810-CHECKPOINT-IMPLEMENTATION-REVIEW.md)
maps the requirements to their proofs and identifies the unmet exit-gate items.
Human review and resolution of the shared journal delay remain pending.
Compatibility: existing durable bytes, public interfaces, thresholds and
authority guarantees remain unchanged. Updated handbook: this page and the
writer-census defect warning. Hazards:
long journal service times and mixed acknowledgement P99 remain; the measured
P50/P95 benefit does not establish WP-749's gate or close WP-810.

## Registration PR note

Package WP-810; registration change tier internal, planned implementation tier
guarantee. Behavior: no runtime change; give the shared finding independent
scope and an open exit gate. Updated handbook: this page and the linked archive
report. Checks: manifest brief, governance and documentation acceptance.
Compatibility: all existing bytes and guarantees unchanged. Hazards: the device
cause and propagated client queueing are not established; implementation and
its correctness/performance evidence remain future work.
