# Open writer-evidence defects: incomplete wall time and nested stage accounting

Status: **open; documented, not repaired**. Package: WP-749 investigation.
These limitations have misled interpretation in both the mechanism-cost review
and capture-cost investigation. The warning belongs in the writer evidence
documentation, not only in a benchmark handoff.

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
