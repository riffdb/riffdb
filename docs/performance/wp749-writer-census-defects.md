# Writer-evidence defects: incomplete wall time, nesting and receipt waits

Status: **W1/W2 open; W3 repaired by receipt-wait instrument V2**.
Package: WP-749 investigation.
These limitations have misled interpretation in both the mechanism-cost review
and capture-cost investigation. The warning belongs in the writer evidence
documentation, not only in a benchmark handoff.

## W3: publication V1 misses the initial journal receipt wait

**Do not use V1 `receipt_block` as total journal waiting.** The completion owner
first waits in `RedbSubmittedCommandFence::wait`; publication later rechecks the
same, normally ready receipt in `publish_through`. V1 times only that recheck.
A small V1 value cannot rule out journal waiting or apportion `POST_DRAIN_ALL`.
Historical values remain recheck measurements; they cannot be repaired from
their aggregates. Independent journal-thread write/sync timers are unaffected.

The repaired `riffdb-writer-publication-stages-v2` line measures at
`JournalFenceCompletion::wait`, starting before its result mutex acquisition.
It retains the initial wait, ready rechecks and failed calls for command-bearing
receipts, and adds call/error counts. Nonblocking polls and noncommand receipts
are excluded. These are **method invocations**, not one sample per publication;
concurrent callers may overlap. Neither their sum nor the journal-thread sum is
a partition of writer wall. Counters are process-generation totals, including
warmup, captured before graceful close; a wait still running at snapshot is
not yet recorded.

The benchmark reader distinguishes V1 from V2. For V1, complete
`receipt_block_sum_us`/`receipt_block_max_us` are null; the old numbers remain
under `legacy_publication_recheck_*`. V2 includes `receipt_wait_calls`,
`receipt_wait_errors` and an explicit coverage label. Missing counts are never
silently treated as complete coverage.

An explicitly scheduled regression test holds the first receipt pending for
25 ms on an injected monotonic clock, releases it, then performs the ready
publication recheck. It requires 25 ms retained across two calls. Removing the
recording reproduces the failure; restoring it passes. A second test covers
failed and noncommand waits. Neither test uses sleeps as synchronization.

The accompanying fixed-size checkpoint census records actual asynchronous
checkpoint selections: starts, summed/minimum/maximum selected transitions and
bytes, selected commands, nonexclusive limit-crossing counts, materializations,
installations and worker/spawn failures. It does not count synchronous close
checkpoints. Snapshots are independent atomics; in-flight checkpoints can leave
starts, materializations and installations different. `failures` does not cover
every possible rotation/installation error. No checkpoint ordering, drain,
limit or durable bytes change.

When writer-batch diagnostics are enabled, bounded checkpoint snapshots also
appear on stderr while the daemon runs (`riffdb-checkpoint-progress-v1`). Retain
that stream before starting the workload; a failed run may never return a final
JSON report. The ready supervisor samples immediately and once per second, at
most 600 times, skipping missed ticks. Each row includes PID, sample/limit,
monotonic elapsed milliseconds and the same 15 counters as the final census.
Successive rows are cumulative and must not be added together. They can lag a
crash or stalled supervisor, stop at the sample cap, and straddle atomic updates.
No observer-overhead verdict exists; hold settings constant in timing arms.

See [receipt repair and checkpoint investigation](wp749-receipt-wait-repair-2026-09.md)
for the production-capture distinction and bounded rerun disposition.

## W1: busy plus idle omits writer intervals

`busy_us + idle_us` is not writer wall time or CPU time. Post-submission
completion draining executes after busy stops and before the idle edge resets.
Submission bookkeeping and feedback also fall outside those counters. A close
match to client wall time in another workload is not a general tiling proof.

Two N1 c32 write-only probe runs on Rust 1.98.1 measured **42.093511024 s** of
continuous writer wall. Busy plus idle undercounted it by **6.442015024 s
(15.30%)**. Post-submission draining was the main named missing interval. The
load-only harness interval was **40.035292398 s**; its different boundary and
exclusion of warmup make it an invalid denominator for process-generation
counter sums. Seed and load daemon generations must also remain separate.

The subsequent C3D comparison found the same defect: **8.654267947 s (20.54%)**
missing from **42.138019947 s** of independent writer wall. The percentage is
workload/host dependent; 15.30% is an observation, not a correction factor.

### Required reconciliation

1. Measure independently from the first writer-iteration start to the last
   completed iteration end. Also sum individual iteration durations. Preserve
   the difference as time between iterations, rather than dropping it.
2. Match the raw writer/capture census to the load daemon's command and iteration
   counts. Do not combine seed and load shutdown lines.
3. Sum the eight disjoint level-0 stages. Subtract from the independently measured
   iteration sum using **signed arithmetic**. Report the remainder and check it
   against `loop_residual`; reject negative residuals or a nonzero mismatch.
   Saturating subtraction in the producer is not evidence of correct tiling.
4. Replace the `unit_execute` parent with its disjoint level-1 children, excluding
   nested `drive_total` and `exec_alternate_path`. Check the signed difference
   against `exec_drive_unnamed + exec_outer_residual`; report the unnamed part.
5. Add the within-iteration, between-iteration and level-1 unnamed remainders.
   For N1 this was **77.220 us/group (0.626% of writer wall)**. Level-0/1
   reconciliation errors were zero in all four control/probe cells.
   C3D independently reconciled all four cells and left **35.283 us/group
   (0.784%)** unnamed. Do not pool these host-specific denominators.
6. Keep journal-thread encoding, writes and sync separate: those intervals
   overlap writer work. Do not sum them into writer wall or apportion fsync by
   byte fractions. Some capture precedes `commit_started_at`; a capture/commit
   ratio is normalized work, not a partition of that timer. The N1 commit
   interval left **3.023 ms/group** beyond apply and seal, including fence waits,
   completion/publication and overlapping I/O.

The privately banked diagnostic patch and analyzer preserve independent
counters and signed checks. See the [host comparison and closure record](wp749-capture-attribution-2026-09.md).
The production counters have not been changed or certified by this investigation.

## W2: level-2 serial counters do not tile their advertised parent

The `serial_*` stages overcount `exec_alternate_path` by **415.753 us/group** in
the N1 probe runs. `command_attempt.rs` charges `SERIAL_EVALUATE` in a shared
evaluation function also called by the compatible path under `EXEC_EVALUATE`.
That work therefore need not lie inside the advertised alternate-path parent.
Saturated `serial_residual` hides the negative parent reconciliation.

Exclude this level from wall attribution until the producer's parent mapping is
corrected and semantically tested. Its excess is already inside level-1
evaluation; it is neither extra capture work nor additional unknown wall time.
Preserve the signed error in evidence. A future fix must demonstrate each
child's enclosing interval across compatible and alternate execution paths.
