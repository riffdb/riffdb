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
All eleven focused snapshot tests and affected all-feature clippy pass. The
immutable-field sharing assertion fails before this implementation. Production
comparisons and complete candidate acceptance remain required.

A candidate must preserve current private state, immutable retained snapshots,
exact rollback, absence-to-present transitions, checkpoint identity, epochs,
error propagation, authorization, idempotency, provenance, and recovery. It
must not add a negative lookup cache, enlarge allocator thread caches, or change
extent recycling. WP-815 continues to own extent retirement and allocation
stalls. No durable format, public surface, or accepted decision changes here.

## Review status

Package: WP-820. Tier: guarantee. Behavior: bounded diagnostics and an unqualified positive-observation reuse
candidate; package acceptance and production qualification remain open.
Registration acceptance passes eight checks; intake and preparation probe
acceptance each pass six checks, including all-feature clippy and commit tests.
Reordered preparation completion coverage asserts independent cancellation,
deadline, and missing-slot failures before any optimization. Snapshot probe acceptance passes 1,407 tests but fails the equal-key admission
clock count (one versus two) and times out one bootstrap publication campaign.
Candidate acceptance runs separately with bounded runner concurrency; no
assertions or test limits are weakened. Full CI and any production A/B, PostgreSQL
qualification, final handbook update, and closure remain required before
completion. This page and its handbook link report the current investigation;
they do not announce a new application or operator capability.
