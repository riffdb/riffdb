# WP-820: intake and preparation investigation

This investigation is open. No throughput optimization is retained or qualified
by this report yet. The frozen base is
`ded67de55dbabaf55b0600df86f8dacb241eb0dc`, the completed WP-819 runtime.
Measurements use the ADR-0239 development machine: 8 vCPU, AMD EPYC 9B14.
The structured, sanitized diagnostic evidence is
`release/evidence/wp-820/intake-20260930.json`.

## CPU attribution

A separate profiling build uses frame pointers and captures all daemon threads
at 997 Hz during a 25-second window of c32 write-only load. There are 51,789
samples and no lost samples. Direct thread CPU deltas report 17.97 seconds for
the ordered writer, 0.52 seconds for the coordinator, and 0.68 seconds across
seven preparation workers. These are CPU seconds, rather than elapsed times
of nested execution stages.

Checkpoint workers contribute about 26% of process CPU samples. Some of them
start and finish between the endpoint thread observations, so those
observations cannot supply complete checkpoint CPU. Thread names are truncated
by the operating system; caller stacks distinguish writer, coordinator,
completion, preparation, and checkpoint roles. Profiling builds are diagnostic
only. Future comparisons must use production release builds after removing the
temporary probes and disabling diagnostics.

Transaction-local snapshot construction contributes 2.17% of process samples
and approximately 6.3% of writer samples. The checkpoint point-read path is
nested within other callers; its samples cannot be added to snapshot or
validation CPU as a separate cost. Backend requests are not physical reads.

## Intake wall time

Temporary diagnostics keep 48 fixed numeric counters per coordinator and emit
one bounded line at shutdown. They carry no keys, values, principals, command
names, or identifiers. The existing checkpoint and writer census formats are
unchanged. The diagnostic runs use 32 clients, 60 seconds of load, and 10
seconds of warmup. Interactive includes journeys. Both runs complete.

| Quantity | Write-only | Interactive |
|---|---:|---:|
| Process committed commands, including warmup | 264,213 | 170,300 |
| Writer iterations | 16,672 | 24,361 |
| Fresh formation wall time | 68.6 ms | 273.3 ms |
| Completion-edge formation calls | 0 | 0 |
| Unit selection wall time | 182.9 ms | 233.8 ms |
| Feedback-to-dispatch wall time | 11.544 s | 8.837 s |
| Idle intake receive wall time | 11.234 s | 7.942 s |
| Writer work-receive wall time | 11.951 s | 11.878 s |
| Mean pending messages at feedback | 15.63 | 6.30 |

Fresh formation costs approximately 0.26 microseconds per write command in this
run. It is not a material explanation for the writer's work-receive total.
Most feedback-to-dispatch time contains intake waiting for new messages. The
wait totals do not identify why producers have not supplied more messages.
Required drain policy, batch limits, and formation deadlines remain unchanged.

The fixed writer census has 64 ordinal windows of 256 units, with overflow in
the final window. Several write windows contain around one second of total
receive wait. That is an aggregate, not an individual pause. Publication
counters in the later preparation diagnostic run report maxima of 685 ms for
receipt wait and 69 ms for publication work. These different windows and
counters do not establish causality. CPU samples, blocked intervals, and
pipeline elapsed windows must remain distinct.

Interactive has 170,301 logical command attempts and 170,300 committed commands;
normalization must state which count is used. Whole-process counters include
warmup and process setup rather than only the measured load window.

## Preparation task evidence

A subsequent c32 write-only diagnostic run observes 256,498 committed commands.
The worker-local census has three fixed task classes and five numeric counters
per class, with one bounded line per worker at shutdown.

| Task class | Tasks | Commands | Mean queue wall time per task | Mean work wall time per task |
|---|---:|---:|---:|---:|
| Published evaluation | 819 | 2,173 | 276.4 us | 182.8 us |
| Writer-private evaluation | 10,953 | 27,909 | 68.0 us | 117.4 us |
| Detached preparation | 10,953 | 27,909 | 110.5 us | 24.5 us |

Maximum task cardinalities are three for published evaluation and four for the
other classes. Writer-private pool work covers about 11% of commands. Existing
`exec_evaluate` wall time also includes serial snapshot materialization and
evaluation; it cannot all be assigned to worker scheduling. The measured queue
sums overlap across workers, and work wall time includes scheduling and reads
as well as CPU. No task-granularity optimization is selected from these totals.

## Positive snapshot opportunity

The current batch already retains validated entity observations and their
encoded bytes for transaction-current validation. Every staged post-image
updates those observations synchronously. Snapshot construction currently reads
and decodes those targets again. A temporary probe counts positive observation
reuse opportunities separately for bindings, root validation, and cascades,
without changing read behavior. It also counts cached absence and missing
entries to quantify opportunity; it does not use them to answer reads.

The probe at `81f8c37786` observes 882,077 binding reads across 240,111
process-scope committed commands. Of those reads, 286,234 (32.45%) already
have positive current observations with matching encoded bytes. Re-reading
them takes 808 ms, or 3.37 us per command. There are no inconsistent positive
entries and no root/cascade calls in this workload. These are elapsed read
intervals, not CPU or physical I/O measurements.

Candidate `aaaa1ac01438aec848a31849d1246eb03cf152f3` retires every temporary
probe and reuses only those existing positive observations. It retains no new
entries. Absence and misses use the ordinary access path; missing bytes for an
existing positive fail closed. Existing immutable fields and canonical bytes
are shared by the observation clone, with existing conservative memory charges.
All eleven focused snapshot tests and affected all-feature clippy pass.
Candidate acceptance passes all fifteen checks, including 1,408 tests with
four runner threads and one existing skipped scale test. The
immutable-field sharing assertion fails before this implementation. Repeated production comparison and full CI remain required.

A candidate must preserve current private state, immutable retained snapshots,
exact rollback, absence-to-present transitions, checkpoint identity, epochs,
error propagation, authorization, idempotency, provenance, and recovery. It
must not add a negative lookup cache, enlarge allocator thread caches, or change
extent recycling. WP-815 continues to own extent retirement and allocation
stalls. No durable format, public surface, or accepted decision changes here.

## Initial production comparison

Production release candidate `aaaa1ac0` and base `ded67de55` run in alternating
base/candidate/candidate/base order, c32, 60 seconds with 10 seconds warmup.
Every temporary probe is removed and diagnostics are disabled.
Sanitized numbers are in
`release/evidence/wp-820/positive-ab-60-20260930.json`.

| Load | Base ops/s | Candidate ops/s | Change | p95 ratio | p99 ratio |
|---|---:|---:|---:|---:|---:|
| Write-only | 3,679.5 | 3,682.5 | +0.08% | 1.000 | 0.815 |
| Interactive journeys | 16,105 | 16,092 | -0.08% | 1.000 | 1.000 |
| Read-only | 17,558.5 | 17,126 | -2.46% | 1.068 | 0.978 |

These are medians of two runs per arm. Read-only individual results range
from 14,255 to 19,997 ops/s; that noisy median is not a demonstrated causal
regression. The negligible write-throughput change does not justify retention.
The longer alternating write-only comparison also fails to support retention.

Write-only sampled writer CPU is 196.29 us per process-scope command at base
and 196.96 us at candidate. The final unsampled interval can contribute up to
500 ms CPU; this small difference is within that measurement uncertainty.
These Linux CPU counters cover the whole measured daemon process, including
warmup, rather than a particular writer-stage elapsed interval. Candidate
write-only median RSS is 1,182,304,256 bytes versus 1,190,506,496 at base;
heap envelope is 1,413,380,096 versus 1,409,110,016 bytes.

Individual write-only maximum latencies remain 715–1,025 ms, and interactive
maxima are 694–1,041 ms. These runs do not identify each maximum's cause and
do not solve WP-815's extent allocation stalls. All twelve runs exit normally.

## Candidate rejected

The 90-second, 15-second-warmup write-only comparison records base medians of
2,714 ops/s and candidate medians of 2,432.5 ops/s (-10.37%). Candidate/base
ratios are 1.226 for p95 and 0.913 for p99. Sampled writer CPU is 239.49 versus
242.72 us per process-scope command; median RSS is 1,190,025,216 versus
1,135,151,104 bytes and heap envelope is 1,378,459,648 versus 1,445,011,456.
Individual maximum latencies range from 697 to 1,033 ms.

The two base throughputs drift from 3,019 to 2,409 ops/s. A prior exclusivity
preflight refused a busy machine, and post-run inspection finds another active
RiffDB runner. This does not prove overlap. Further bench evidence requires
clarification of exclusive availability. The numeric report is preserved as an
unqualified negative finding in
`release/evidence/wp-820/positive-ab-90-20260930.json`; no claim assigns the
entire throughput gap to this candidate.

The candidate is removed because a throughput benefit is not demonstrated in
either comparison. The positive-only reuse helper and its mechanism tests are
removed together; reordered preparation failure coverage remains. No added
cache or changed absence path remains. The probes from that experiment are
removed; the next phase uses a separate temporary diagnostic.
WP-820 stays open. The next measurement will count exact-byte repeat positive
snapshot reads inside existing private admitted batches, before selecting any
new retained-entry implementation. No PostgreSQL benefit is claimed for this
rejected candidate.

## Next diagnostic phase

A temporary probe counts exact-byte repeated positive reads whose targets are
not already held by transaction-current validation. Each private batch retains
at most 64 entries and 64 KiB of copied key/value payload. The report separately
names fixed slot and counter storage, peak payload/entry counts, copied key and
value bytes, and skipped diagnostic records. Allocator overhead is separate
from these logical byte charges and must be measured by process memory.

Every original entity read and decode still runs. Successful reads are split
by binding, root validation, and cascade role. Existing positive observations,
existing absence, new positive observations, absence, exact byte repeats,
changed bytes, and budget refusals are distinct counters. Read wall intervals
stop before diagnostic retention and comparison work; they are not CPU time.
Errors stay on their ordinary path and are excluded from successful-read counts.
No values, keys, identifiers, or principals enter the bounded numeric line.
The existing 15-field checkpoint census remains unchanged.

Two probe tests prove exact-byte classification and entry/payload bounds; all
eleven focused snapshot tests and affected all-feature clippy pass. Scoped
acceptance at `3863271c` passes six checks and all 1,094 tests, with one existing
skipped scale test. The following drop-order correction at `df66aead` passes
affected all-feature clippy and both probe tests; the handbook check passes. The last full CI was interrupted by the agent server
restart and is incomplete, despite having no failure in its available log.
The later diagnostic run passes an idle-machine preflight and exclusive lock,
with 100 once-per-second sharing checks and no detected sharing. This resolves
availability for that run; it does not retroactively qualify the drifting
production comparison. The temporary repeat probe is removed afterward. The
package remains open and no throughput improvement is claimed.

## Additional positive repeats: insufficient opportunity

The diagnostic at `df66aead` completes c32 write-only, 60 seconds plus 10 seconds
warmup, on the unchanged `ded67de55` runtime. Sanitized evidence is in
`release/evidence/wp-820/snapshot-repeat-20260930.json`. The measured process
commits 260,334 commands, including warmup. Binding reads total 956,174:
313,004 are existing positive observations, 332,969 are additional positive
reads, and 310,201 are absences. Of the additional positives, 80,627 repeat
identical bytes within their existing batch. Root-validation and cascade reads
are zero for this workload; it provides no opportunity estimate for those paths.

Exact-repeat read/decode wall time totals 155,223,706 ns, or 0.596 us per
process-scope command. There are no changed-value events, budget refusals, or
skipped diagnostic records in the measured process. A new positive-value cache
is not selected: this small measured read opportunity does not justify added
retention, synchronization, validation interactions, and accounting work. This
finding does not revisit absence caching or allocator thread caches.

The diagnostic copies 9,588,996 key bytes and 72,081,251 value bytes in total.
Peak retained payload is 9,957 bytes across at most 30 entries, with a 2,048-byte
fixed slot array and 328-byte optional per-batch probe header. These logical
charges exclude allocator overhead. Process peak RSS is 1,192,493,056 bytes and
heap envelope is 1,415,290,880 bytes; there is no matched control for attributing
these process totals to the probe.

Diagnostic throughput is 3,648 ops/s, p95 8.389 ms, p99 11.010 ms, and maximum
717.543 ms. These are diagnostic results, not production qualification. The
writer's direct CPU counter advances 18.38 seconds in a separate 25.127-second
window without an aligned command counter. Scheduler counters advance
18.387 seconds on CPU and 0.121 seconds waiting on a runqueue, following the
[Linux scheduler statistics field definitions](https://www.kernel.org/doc/html/latest/scheduler/sched-stats.html).
The remaining 6.618 seconds are an elapsed-minus-CPU-minus-runqueue residual,
not caller-attributed blocked time. Separate endpoint reads limit precision.

The next diagnostic uses filtered scheduler switch/wakeup events and caller
stacks on the unchanged base's frame-pointer build. It will distinguish blocked
intervals from runnable waits and retain only bounded aggregate public evidence.
No drain, batch limit, delay, durable format, public interface, or accepted
architecture decision changes. Production A/B and same-run PostgreSQL
qualification still apply to any subsequently retained implementation.

## Writer scheduler attribution

A separate filtered scheduler capture uses the unchanged `ded67de55`
frame-pointer build, c32 write-only, 90 seconds plus 15 seconds warmup.
The exclusive lock, idle preflight, and 137 sharing checks report no competing
benchmark. The trace contains 47,930 events and 16,269 complete switch-out to
switch-in intervals, with no detected lost records. One initial incoming event
and one final outgoing interval cross the capture boundaries and are excluded.
Sanitized evidence is `release/evidence/wp-820/writer-offcpu-20260930.json`.

The following categories are disjoint caller classifications over a roughly
25-second trace window. Off-CPU time includes both sleep until wakeup and
runnable time after wakeup; runnable switch-outs have no sleep component.

| Writer wait site | Off-CPU total | Sleep until wakeup | Maximum complete interval |
|---|---:|---:|---:|
| Writer work receive | 4.908 s | 4.874 s | 687.977 ms |
| Required completion drain | 0.734 s | 0.731 s | 326.582 ms |
| Transaction-current idempotency index read | 0.413 s | 0.394 s | 1.255 ms |
| Provenance index read | 0.217 s | 0.209 s | 2.324 ms |
| Publication queue registration | 0.234 s | 0.234 s | 61.761 ms |
| Writer-private evaluation completion | 0.165 s | 0.143 s | 0.732 ms |
| Detached preparation completion | 0.127 s | 0.083 s | 0.711 ms |
| Runnable switch-out | 0.054 s | 0 s | 0.466 ms |
| Other blocked sites | 0.038 s | 0.032 s | 1.487 ms |

Idempotency and provenance stacks block acquiring the transient-index read
lock. Publication registration stacks block acquiring the publication queue
mutex. This identifies writer wait sites, not the particular lock-holder work
that caused every interval. Work receive accounts for most off-CPU time;
its 688 ms maximum does not identify why the producer stopped supplying work.
Required drains and all batching policy remain unchanged. No extent-retirement
cause or fix is inferred; WP-815 retains that scope.

The diagnostic records 3,611 ops/s, p95 8.389 ms, p99 14.156 ms, maximum
1,021.993 ms, peak RSS 1,384,099,840 bytes, and heap envelope 1,429,446,656 bytes.
These are profiling results. Direct writer endpoints advance 18.22 CPU seconds
in a separate 25.138-second window with no aligned command counter. The trace
and endpoint windows have different boundaries and must not be added.

The next bounded measurement will attribute transient-index lock-held work,
including metadata/key construction, canonical payload checks, memory charging,
and payload eviction/destruction. Only then can a mechanism be selected.
Moving work must preserve exact canonical validation, complete retained-memory
bounds, atomic publication, transaction-current idempotency and provenance,
immutable snapshots, and failure/cancellation behavior.

A temporary private probe now times ten phases: publication transient-lock
acquisition/hold, segment metadata construction, manifest validation/insertion,
payload metadata validation, periodic canonical decode verification, decoded
ownership charging, cache-lock acquisition, payload removal/destruction, and
cache insertion. Each phase has a count, wall-time sum, and maximum. Three
additional counters report eviction count, canonical bytes removed, and their
conservative ownership budget charge. The charge is not an allocator-byte
measurement. The new shutdown line has exactly 33 numeric fields and records
no keys, values, identities, or principals; existing census lines stay intact.
All checks, cache bounds, lock scopes, and publication ordering remain unchanged.
Nested phase wall times overlap and must not be summed as CPU costs.
Existing eviction/cold-authority, decoded-ownership, seeded-canonical-byte,
history-stub identity, and sparse-tree charging tests cover the instrumented
paths. All five pass with diagnostics enabled at `03884ff2`, and affected
all-feature clippy passes.

A retirement acceptance run is invalidated when the agent rebuilds test
artifacts while its crash campaigns are still active. One crash restart tries
to use a replaced/deleted executable; the run is interrupted, cancelling four
other active tests. Its test step reports 647 passes, one restart failure,
four cancellations, and 440 unrun tests. The other eight planned checks pass.
The affected promotion-cutover crash test passes alone with stable artifacts
and diagnostics enabled. This does not substitute for complete acceptance:
final acceptance and full CI must run with stable test artifacts. Final acceptance and package completion remain pending.


## Publication costs and bounded cache-room candidate

The publication probe at `03884ff2` completes c32 write-only, 60 seconds plus
10 seconds warmup, with 101 sharing checks and none detected. The measured
process commits 264,754 commands. Sanitized evidence is
`release/evidence/wp-820/publication-cost-20260930.json`.

Publication holds the transient-index write lock for 2.111 seconds in total.
Payload eviction/removal/destruction totals 842 ms (3.182 us per process-scope
command), manifest validation/insertion 703 ms, periodic canonical decode
verification 132 ms, decoded ownership charging 58 ms, and metadata construction
36 ms. These phase wall times are nested; some also execute during startup.
They are not additive CPU costs or an exact partition of lock-held time.
Evictions remove 931,419,776 canonical bytes across 16,665 payloads, with
63,808,870,832 bytes of cumulative conservative ownership charges. These are
cumulative budget charges, not simultaneously retained memory or allocator bytes.
Diagnostic throughput is 3,707 ops/s, p95 8.126 ms, p99 15.204 ms, maximum
718.350 ms, peak RSS 1,201,668,096 bytes, and heap envelope 1,403,027,456 bytes.
The separate writer window advances 18.32 CPU seconds over 25.126 seconds.
No production benefit is inferred from these diagnostic figures.

A prototype prepares room in the existing disposable payload cache before
acquiring the transient-index write lock. It holds a shared index guard and
keeps the cache mutex across removal and complete destruction. Evicted payloads
never escape the mutex or its existing 64-entry/64-MiB bound. The original
publication path still checks metadata, canonical bytes, duplicate entries,
poison, decoded ownership charges, and cache bounds before advancing the
published frontier. A concurrent cold read may refill the cache; ordinary seed
insertion repeats its original eviction loop. No absence is cached.

This candidate copies no extra payload bytes and retains no additional payloads.
It adds a shared index-lock acquisition, a cache-lock acquisition, and one
bounded ownership-charge walk per seeded segment. These synchronization and CPU
costs require production measurement. Published indexes, writer-private state,
frontiers, durability fences, errors, and checkpoint identity retain their
existing validation and update paths. No extent recycling or retirement changes.

A deterministic barrier test fails before preparation is implemented: an index
reader cannot observe the existing absence while eviction holds the write lock.
It passes with the prototype and also proves the old publication frontier,
absence-to-present transition, and final cache bounds. Separate tests explore a
cold read refilling the slot before seed insertion and prove that a poisoned
cache still fails through the original seed path. Eight focused tests pass;
affected all-feature clippy passes after removing two redundant test borrows.
The publication probe is removed before production comparison. Repeated c32
base/candidate/candidate/base write-only, interactive journeys, and read-only
comparisons, same-run PostgreSQL qualification if retained, complete acceptance,
full CI, and package closure remain pending.

WP-815 lands at `555a526f848c` with this candidate integrated. The production
base is its validated `445927ccef` revision: code differences from the integrated
head are the cache-room mechanism and test-only coverage. Both arms include
WP-815 and writer marker 2. No throughput benefit is claimed for this prototype.

## Post-WP-815 refresh (2026-10-01)

Scoped acceptance over `445927ccef..555a526f848c` passes all 15 checks and
1,969 tests, with one existing skip. Full `./scripts/ci-all` passes at the
integrated head. Test artifacts and full-CI recovery scratch are isolated from
other checkouts. The reduced-feature benchmark harness emits five existing
unused gRPC-codec warnings and four missing-documentation warnings on retention
evidence fields; the daemon builds and scoped all-feature clippy are clean.

Fresh diagnostic c32 write-only runs use base `445927ccef`, profiling builds
with frame pointers, 90 seconds load and 15 seconds warmup. The CPU capture has
50,376 samples and no loss; 17,865 samples belong to the ordered writer. The
no-inline symbol view attributes 10.36 percent of writer samples inclusively to
overlay point resolution, 8.82 percent to checkpoint-root accesses, 6.68 percent
to mutation-stage validation, 5.83 percent to overlay insertion, and 5.41 percent
to changelog-successor preparation. These totals overlap. SHA-256 compression
has 1,296 writer self samples (7.25 percent of writer samples); overlay-node
`Arc::make_mut` has 619 (3.47 percent). Unresolved leaf symbols account for
4,269 writer samples (23.90 percent); their cause is not assigned. The endpoint
window advances 18.08 writer CPU seconds over 25.141 seconds and differs from
the capture and process command-count windows.

A separate scheduler capture has 45,267 events, 15,360 complete intervals, and
no detected trace loss. Disjoint writer off-CPU intervals include 330 ms at
transaction-current idempotency index reads, 175 ms at provenance index reads,
241 ms at publication registration, 400 ms at required completion drains,
149 ms at writer-private evaluation completion, 118 ms at detached preparation,
and 5.241 seconds receiving work. The largest work-receive interval is 686.752
ms. These are wait sites, not proof of the upstream cause or the lock-holder
activity responsible for each wait. No required drain changes.

The CPU and scheduler runs have 135 and 136 sharing checks with none detected.
Sanitized evidence is `release/evidence/wp-820/post815-diagnostics-20261001.json`.
Production base/candidate/candidate/base comparisons are underway for all three
loads. Fresh bounded allocation/copy counts, repeated production qualification,
same-run safe-app PostgreSQL qualification if retained, and closure remain
pending. Diagnostic throughput is not banked as a production result.

## First post-WP-815 production bank (2026-10-01)

Production release binaries with diagnostics disabled compare base `445927ccef`
with candidate `555a526f848c` at c32, 60 seconds load and 10 seconds warmup,
in base/candidate/candidate/base order. Interactive uses journeys. All twelve
cells complete successfully and the sharing monitor detects no competing run.
These are medians of two cells per arm:

| Load | Base ops/s | Candidate ops/s | Change | Base / candidate p95 | Base / candidate p99 |
| --- | ---: | ---: | ---: | --- | --- |
| Write-only | 3,695 | 3,681 | −0.38% | 8.126 / 8.126 ms | 13.631 / 12.059 ms |
| Interactive | 16,064 | 16,052.5 | −0.07% | 7.078 / 7.078 ms | 9.437 / 9.437 ms |
| Read-only | 25,013.5 | 24,996 | −0.07% | 2.359 / 2.359 ms | 3.408 / 3.408 ms |

Sampled writer CPU per process-generation command rises from approximately
195.9 to 200.2 µs for write-only and 258.9 to 264.1 µs for interactive. This
includes warmup and uses cumulative thread ticks sampled every nominal 500 ms;
final shutdown ticks may be missed. It is not the writer wall-service measure.
The worst observed write-only maximum is 1.021 seconds for base and 743 ms for
candidate; interactive maxima are 718 ms and 1.039 seconds. Neither the tail
changes nor the maxima establish a cause.

The first bank shows no throughput benefit. A longer write-only repeat is
prepared but cannot start until local cloud authentication is refreshed. Full
raw-report collection and memory evidence remain pending. Selected numeric
results are in `release/evidence/wp-820/post815-first-ab-20261001.json`; no
PostgreSQL or retention qualification is claimed.

## Cache-room candidate rejected after repeat (2026-10-01)

The longer production write-only bank uses the same named commits and frozen
release binaries, 90 seconds load plus 15 seconds warmup, again B/C/C/B.
All four cells succeed with 137–138 sharing checks each and none detected.
Base median throughput is 3,610 ops/s versus candidate 3,605 (−0.14 percent).
Base/candidate p95 is 8.389/8.258 ms; p99 is 14.942/15.729 ms (+5.26 percent).
Maximum stalls are 1.024/1.026 seconds. Sampled writer CPU per process-generation
command is 200.54/201.17 µs, subject to the polling limitation above.

| Bank/load | Base / candidate median heap envelope | Base / candidate worst sampled RSS |
| --- | --- | --- |
| 60 s write-only | 1,405,241,344 / 1,409,097,728 bytes | 1,205,645,312 / 1,194,414,080 bytes |
| 60 s interactive | 1,094,864,896 / 1,105,866,752 bytes | 1,023,328,256 / 1,030,041,600 bytes |
| 60 s read-only | 47,108,096 / 45,520,896 bytes | 86,196,224 / 87,035,904 bytes |
| 90 s write-only | 1,431,130,112 / 1,424,652,288 bytes | 1,371,947,008 / 1,380,032,512 bytes |

The first and repeated banks show no reproducible throughput benefit. Remove
cache-room preparation, its test hooks, and its three dedicated tests; restore
the original bounded eviction path from `445927ccef`. WP-815 code remains intact.
The failed-before barrier test proved the intended overlap but did not establish
a production benefit. Additional synchronization and charging work are not
justified by these results. Neither CPU nor tail differences establish a cause.
Same-run PostgreSQL qualification is not run for this rejected candidate.

The completed raw summaries are collected after the evidence run. Sanitized
numeric cells and resource results are in
`release/evidence/wp-820/post815-production-ab-20261001.json`. Removal acceptance
passes all nine checks, including the storage all-feature test run. Fresh
read/allocation/copy attribution and package closure remain
pending; write throughput is not fixed.

## Resolved C runtime CPU (2026-10-01)

Matching C runtime debug symbols resolve the existing `445927ccef` CPU capture,
without another profile run or an installed-library change. Of 17,865 ordered
writer samples, 2,044 (11.44 percent) are in the copy implementation and 2,024
(11.33 percent) are in the byte comparison implementation. The corresponding
sample-period totals are 2.050 and 2.030 CPU seconds. These self samples were
previously unresolved; they must not be added to the old unresolved bucket.

Inclusive comparison callers include checkpoint-root reads (297), overlay point
resolution (297), rebase (236), mutation validation (59), and insertion (53).
These counts overlap. Copy samples also occur under command preparation and
staging, receipts, allocator resizing, and rebase. Direct caller frames may
contain inlined code, so they do not establish which value move or owned copy
causes the CPU work. Copied sizes and complete allocation counts are still
needed before selecting another mechanism.

A separate five-second allocation-entrypoint capture records 224,840 allocation
requests totaling 30,508,696 requested bytes. Requested sizes exclude allocator
rounding, metadata, fragmentation, and success/failure distinction. The observed
record-application entrypoint is not yet proven complete, so no allocation-per-
command estimate is claimed. Probe overhead produces a 5.245-second diagnostic
maximum; this is not banked as an ordinary production stall. A bounded caller-
class byte-size capture follows. Sanitized CPU evidence is
`release/evidence/wp-820/resolved-copy-cpu-20261001.json`.

## Temporary bounded read/copy counters (2026-10-01)

A temporary storage-only counter phase records explicit overlay allocations,
node clones and ownership observations, owned point/before-image copies,
lineage construction and rebase keys, journal-to-overlay conversion, receipt
construction and coalescing, and table-specific checkpoint requests/copy bytes.
Writer-private snapshot targets, preparation snapshots, transaction-current
idempotency/provenance calls, and command record applications are separate.
Counters never supply values to a read or mutation and never change validation.

Each participating thread keeps two fixed arrays of 48 and 112 saturating u64
counters, plus tags/padding. Counter updates copy these arrays and add diagnostic
CPU/stack traffic. One bounded numeric line per array is emitted on graceful
thread exit. Private process-generation tags separate seed, measured, and reopen
processes; exact roles distinguish the ordered writer from preparation workers,
coordinator, completion, and other threads. Published evidence drops process
tags. These explicit constructor counts are not complete allocator totals or
allocator rounding/fragmentation, and backend requests are not physical reads.

The counter phase uses a profiling build; production comparisons retain the
frozen binaries without these probes. All 312 storage-API library tests and eight
follower-index tests pass, as do the eight scoped non-test acceptance checks.
The diagnostic bundle is frozen at `02497ca4` and shipped for a profiling-only
build. The temporary source is removed from main before full CI; the diagnostic
arm remains pinned to that bundle. A local session interruption loses its live
SSH handle before load startup. Recovery confirms no benchmark or build remains
active, then restarts the same pinned diagnostic. The completed run has 99
exclusive-use checks, no sharing, and a zero exit status. Four process
generations produce 69 bounded counter lines. The measured ordered writer's
248,918 record applications exactly match the harness's complete-generation
command count; seed and reopen generations are excluded.

Per complete-generation command, the ordered writer records:

| Operation | Count or logical bytes |
|---|---:|
| Actual overlay node clones | 51.93 |
| New overlay nodes | 13.44 |
| Overlay value copies | 4,602.61 bytes |
| Journal-to-composite value clones | 4,590.99 bytes |
| Temporary receipt put-value copies | 5,688.53 bytes |
| Owned point-result copies | 684.40 bytes |
| Owned before-image copies | 1.20 bytes |
| Checkpoint entity requests | 4.06 |
| Checkpoint entity value copies | 380.21 bytes |
| Checkpoint index-epoch requests | 0.000145 |

Writer-private snapshots account for 247,020 calls, while preparation workers
record 1,898 snapshot calls. The private writer path preserves the transaction's
current state; preparation-worker grouped reads alone do not remove this work.
Idempotency and provenance remain separate: one transaction-current idempotency
call and one provenance-existence call per command. Several nested checkpoint
requests check absence, including about two requests per command in each of
idempotency, pending admission, and idempotency locators. These are not physical
reads, and this process reports zero physical read bytes. Negative lookup caching
is not selected again.

Temporary receipt coalescing sees only 4,520 repeated-key records among
3,674,693 records. This establishes construction volume, not its independent CPU
cost. Likewise, node-clone allocation and byte-copy counts do not establish that
changing ownership will improve throughput. Published numeric evidence and
coverage limits are in
`release/evidence/wp-820/storage-construction-counts-20261001.json`.

## Streaming checksum candidate rejected (2026-10-01)

Production releases at base `de902b30` and candidate `e6db75fe` run exclusive
c32 B/C/C/B comparisons with diagnostics disabled. The first bank uses 60-second
loads and 10-second warmups; interactive includes journeys.

| Load | Base median ops/s | Candidate median ops/s | Change |
|---|---:|---:|---:|
| Write-only | 3,634.5 | 3,627 | -0.21% |
| Interactive | 16,181 | 16,027.5 | -0.95% |
| Read-only | 24,984 | 24,857.5 | -0.51% |

The longer write-only repeat uses 90-second loads and 15-second warmups. Base
median throughput is 3,604.5 ops/s versus 3,586 for candidate (-0.51%). Both
arms' median p95 is 8.389 ms; median p99 is 15.204 ms versus 14.418 ms. Worst
maximum is 1.021 s versus 0.744 s. Sampled writer CPU per complete-generation
command is 201.709 us versus 203.790 us. Median heap envelopes are 1,437,581,312
versus 1,414,606,848 bytes; maximum sampled RSS is 1,346,592,768 versus
1,349,033,984 bytes. These memory measurements do not establish the temporary
buffer's independent cost or a retained memory benefit.

The candidate passes all seven scoped acceptance checks, 544 storage-API tests,
and full `./scripts/ci-all`. It is nevertheless removed because throughput does
not improve in either bank. The receipt codec is restored byte-for-byte to
`de902b30`. No PostgreSQL qualification is performed for this rejected candidate.
Full numeric cells, resource scopes and limitations are in
`release/evidence/wp-820/streaming-checksum-ab-20261001.json`.

Compiler-only diagnostics show 1,864 bytes for pending attempt state, 2,752 for
evaluated attempts, and 3,184 for the backend's private batch core. These are
representation sizes, not dynamic copy counts or measured CPU costs. Native
call-site assignment remains unverified; do not assign sampled copy sizes from
a different build. A private ownership change may be investigated separately
after confirming exact capture identity and charging its allocation cost.

## Private batch ownership candidate (2026-10-01)

The rejected single-mechanism experiment placed the private `BatchCore` behind one
unique `Box` in both empty and nonempty redb batches. All consuming candidate
states transfer that owner. The frozen production base is `4dfaae784`.
The core requests 3,184 bytes on this diagnostic build; the allocation happens
once per batch rather than once per command. Allocator rounding and metadata
are additional costs, not included in that requested size. Production heap,
RSS and CPU measurements must determine whether this cost is worthwhile.

A matched-binary direct-call probe observes 5,968- and 6,064-byte moves on the
append path and a 5,960-byte move in transaction-current validation. This does
not measure their independent CPU cost. The complete-copy probe reaches its
256-key map bound, so its aggregate counts and byte shares are rejected. The
compiler layout and individually resolved call sites select a cheap ownership
prototype, not an established throughput benefit. The earlier CPU profile
continues to identify copy work as material without assigning it all to batches.

The candidate changes no protocol method, durable bytes, ordering, validation,
or publication boundary. The existing unique owner still drops the uncommitted
transaction on rollback, cancellation or error. Relevant semantic coverage is
`sealed_decision_rejection_discards_every_earlier_bulk_effect`,
`equal_key_commands_commit_once_and_replay_exactly`,
`create_before_delete_refuses_then_delete_releases_unique_value_for_reinsert`,
`audited_standard_group_uses_one_unpublished_root_and_one_immediate_tail`,
`cancellation_before_acquire_or_after_snapshot_releases_all_capabilities`, and
`rebase_retains_only_exact_post_checkpoint_lineage`. No new semantic behavior
is claimed. All-feature check and clippy pass, as do all 37 command-concurrency
and lost-response replay tests. Production comparison and full storage
acceptance remain required.

## Private batch owner rejected

Production candidate `71eac4b0` is compared with base `4dfaae784`, c32, in
B/C/C/B order with diagnostics disabled. First-bank medians are:

| Load | Base ops/s | Candidate ops/s | Change |
|---|---:|---:|---:|
| Write-only | 3,680 | 3,651.5 | -0.77% |
| Interactive journeys | 16,084.5 | 16,081 | -0.02% |
| Read-only | 25,008 | 24,918.5 | -0.36% |

Write-only p95 is 8.126 ms in both arms, p99 is 12.845 versus 11.796 ms,
and worst maxima are 1.012 versus 1.023 s. Sampled writer CPU per complete
process-generation command is 197.095 versus 197.837 us. Median heap envelopes
are 1,315,674,112 versus 1,212,575,744 bytes; maximum sampled RSS is
1,183,395,840 versus 1,170,485,248 bytes. These memory differences do not
measure the independent effect of the allocation.

The 90-second/15-second-warmup write-only repeat gives 3,562 versus 3,591.5
ops/s (+0.83%). CPU is 203.628 versus 202.864 us per process-generation
command. Both throughput and CPU changes reverse direction between banks.
Median p95 is 8.651 versus 8.389 ms, p99 15.466 versus 13.369 ms, and worst
maxima 747 versus 718 ms. Heap envelopes are 1,432,068,096 versus
1,434,554,368 bytes and maximum sampled RSS is 1,364,180,992 versus
1,347,493,888 bytes. No repeated throughput benefit supports retention.

The candidate passes all six storage acceptance checks (1,110 tests, with one
existing skipped scale test) and all 37 command-concurrency/replay tests.
It is removed; `application.rs` is restored byte-for-byte to `4dfaae784`.
Restoration passes 1,110 storage tests and seven other scoped checks.
The generated check initially sees concurrent diagnostic edits and is invalid;
its unchanged-tree rerun passes all 30 generators.
Same-run PostgreSQL qualification is reserved for a retained candidate.
Full numeric cells and resource scopes are recorded in
`release/evidence/wp-820/private-batch-owner-ab-20261001.json`.

## Mutation ownership census (2026-10-01)

Frozen diagnostic `e4825074` uses base `bbc4d6d01` and fixed-size, thread-local
numeric counters. Its measured writer records 253,396 successful applications,
exactly matching the harness's complete-generation command count. The exclusive
run completes with 100 sharing checks and no competing workload. Probes are
removed at `0922ffa0`; production paths are restored byte-for-byte.

| Writer operation | Per complete-generation command |
|---|---:|
| Receipt mutation clones | 0 |
| Composite mutation clones | 0 |
| Journal mutation clones | 0.0631 |
| Journal clone key/value bytes | 1.83 / 1.20 |
| Journal clone elapsed interval | 0.00734 us |
| Receipt mutation constructors | 14.7726 |
| Receipt constructor key/value bytes | 896.49 / 5,684.64 |
| Composite mutation constructors | 8.9889 |
| Composite constructor key/value bytes | 525.03 / 4,590.01 |
| Journal mutation constructors | 10.4689 |
| Journal constructor key/value bytes | 568.28 / 8,250.85 |

The clone intervals are elapsed diagnostic time, not CPU measurements or
production comparisons. Constructor bytes describe owned content; the census
alone does not establish a fresh allocation or its independent CPU cost.
Sharing mutation clone storage is not selected: receipt and composite clones
are absent, and journal clone volume is negligible. Constructor copying and
private command-state moves remain distinct opportunities. Sanitized counts
and coverage limits are in
`release/evidence/wp-820/payload-ownership-counts-20261001.json`.

## Review status

Restored production main passes full `./scripts/ci-all` at `dd2cc540`; the
storage-count evidence at `de902b30` passes all five scoped acceptance checks.
The rejected checksum candidate streamed the canonical receipt body into
SHA-256 when only its history checksum was needed, avoiding a temporary encoded
receipt buffer. Encoding and hashing share one private emitter. Frozen-byte
coverage fails to compile before the emitter exists and passes with it; empty,
put/delete and SHA block-boundary coverage precedes production qualification.
The candidate does not change receipt coalescing, canonical bytes, public or
durable interfaces. Both production banks reject it; the codec is restored. Restoration at
`a138fb89` passes all nine scoped checks, including 542 storage-API tests.
No throughput candidate is retained. Copy sizes and caller attribution remain
the next diagnostic step, rather than an established performance gain.

Package: WP-820. Tier: guarantee. Behavior: bounded diagnostics, rejected
positive-observation reuse, and an unqualified disposable payload eviction
prototype; package completion remains open.
Registration acceptance passes eight checks; intake and preparation probe
acceptance each pass six checks, including all-feature clippy and commit tests.
Reordered preparation completion coverage asserts independent cancellation,
deadline, and missing-slot failures before any optimization. Snapshot probe acceptance passes 1,407 tests but fails the equal-key admission
clock count (one versus two) and times out one bootstrap publication campaign.
Candidate acceptance passes separately with bounded runner concurrency; no
assertions or test limits are weakened. The earlier full CI run was interrupted; post-WP-815 full CI now passes at
`555a526f848c`. Any subsequent runtime change requires its appropriate checks.
Rejected candidates have production A/B evidence; same-run PostgreSQL
qualification remains required for any future retained candidate. Final
acceptance and closure remain pending. This page and its handbook link report the current investigation;
they do not announce a new application or operator capability.
