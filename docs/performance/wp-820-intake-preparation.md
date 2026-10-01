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
Live measurement is pending clarification of exclusive benchmark availability.
The package remains open and no throughput improvement is claimed.

## Review status

Package: WP-820. Tier: guarantee. Behavior: bounded diagnostics and an unqualified positive-observation reuse
candidate, now rejected; package completion remains open.
Registration acceptance passes eight checks; intake and preparation probe
acceptance each pass six checks, including all-feature clippy and commit tests.
Reordered preparation completion coverage asserts independent cancellation,
deadline, and missing-slot failures before any optimization. Snapshot probe acceptance passes 1,407 tests but fails the equal-key admission
clock count (one versus two) and times out one bootstrap publication campaign.
Candidate acceptance passes separately with bounded runner concurrency; no
assertions or test limits are weakened. The full CI run was interrupted and must be repeated for the final runtime. Full CI for the final retained runtime, any new candidate production A/B and
PostgreSQL qualification, final handbook update, and closure remain required
before completion. This page and its handbook link report the current investigation;
they do not announce a new application or operator capability.
