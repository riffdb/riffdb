# WP-810 materialization observation preparation

This bank retains **diagnostic preparation and functional qualification**.
The unchanged observation patch applies to original integration
`5d9b8cc4c597e8868bb60c7fb5fcdc721463aa68` and refreshed integration
`3514c4af29f4b266f2d5be726d7510cf8e54a73f`; it is outside the production
candidate. `qualification.json` retains the original checks, and
`refresh-qualification.json` records the current checks and private evidence
digests. The remote attempt in
[`../materialization-attempt-20260926.json`](../materialization-attempt-20260926.json)
stopped during seeding at its 900-second deadline, before the timed workload;
its partial trace is not complete measurement evidence. The candidate's
existing comparison keeps its original source identities and verdict.

## Question and production path

The retained trace places long journal syncs before checkpoint materialization
finishes. This diagnostic separates materializer dispatch, `begin_write`,
mutation replay, durable commit and installation. The normal daemon constructor
uses the same redb `FileBackend`, file-open flags and locking through an
observation wrapper. Each backend read, write, resize and sync executes exactly
once with its original bytes, result and error. No syscall, durable byte,
checkpoint threshold, acknowledgement fence or authority decision is replaced.

The pinned redb 4.2.0 source also syncs during file growth, before replay has
reached its final commit. ADR-0004 Amendment 5 explicitly requires this ordering
to prevent a persisted header referring beyond the durable file length. The
analyzer therefore retains each checkpoint sync's preceding operation in that
same checkpoint phase, including any resize. This distinguishes observed
growth-barrier sequences from final-commit sequences without removing a fence
or assuming that either sequence caused a concurrent journal delay.

The original local functional check joins 9,504 measured requests to 594 command
groups and physical flushes. It retains three checkpoints and 65,912 backend
operations with no dropped rows. Journal write, sync and queue durations match
the existing census exactly. These are debug-build functional observations on
an unqualified local host; their latency is not performance evidence.

The refreshed check uses the exact release daemon and runner built for the
remote diagnostic, with both executable digests verified locally. Its
90-second local workload joins 2,652 requests to 221 measured groups and
flushes; the complete trace retains one real checkpoint and 16,856 backend
operations with no drops. The earlier local attempts remain recorded: one
debug seed stopped with `AuditUnavailable`, and two clean short workloads
were refused by the analyzer because they never reached a checkpoint. The
first stop's cause is not established. Extending the functional workload
preserves the real checkpoint thresholds and all coverage assertions. These
local observations support no performance conclusion.

## Bounds and lifecycle

With `RIFFDB_WRITER_CORRELATION=1`, fixed buffers hold 65,536 groups, 512
checkpoints and 4,194,304 I/O operations in at most 352 MiB of record storage.
`RIFFDB_WRITER_BATCH_DIAGNOSTICS=1` enables the existing periodic census that
drains completed records in bounded pages. Overflow preserves the real I/O
operation but invalidates diagnostic completeness. Publication uses release /
acquire synchronization; partially written rows are not emitted.

The normal checkpoint census is taken **before graceful close**. Backend close
emits a final trace afterward. In the final local check the census reported two
materialized and installed checkpoints; the third finished after the last
command publication. The analyzer retains these distinct populations, requires
every additional completion to be after the final publication, and uses only
flushes joined to measured requests for load-period overlap. It does not count
shutdown completion as measured work.

Each record contains numeric identities, counts and process-local monotonic
times, with no data values, keys, file paths or credentials. Raw logs still
contain ordinary daemon output and remain private. Do not commit the runner's
generated host records, logs, source bundle or binaries.

## Frozen diagnostic

`measurement-protocol.json` retains the original source freeze;
`refresh-measurement-protocol.json` records the current source freeze. Both
fix one archive-disabled c32 write-only cell:
15 seconds warmup and 90 seconds measurement, Rust 1.98.1, thin LTO, one codegen
unit, generic x86-64 and the existing server allocator. A fresh exclusive
interval on an admitted 8-vCPU AMD EPYC 9B14 host is required before remote work.
The runner writes trace output to a private tmpfs spool during the cell and
banks it afterward; it refuses insufficient RAM or a non-tmpfs destination.
It stops at the first failed attempt and retains that attempt.

`build_then_gate.py` and `supervise_gated.py` allow the authorized release
build to finish while local qualification proceeds. Measurement waits for an
atomic, source- and patch-bound success receipt. The same copied release
binaries must pass local functional coverage before that receipt is released.
The supervisor retains the deadline and restores task-owned host isolation.

The analyzer requires complete final coverage, exact request/group joins,
consistent deduplicated flushes, valid phase bounds, checkpoint counts and
reasons, corrected wait coverage, and independent writer-wall reconciliation.
It uses the union of overlapping I/O intervals, reports the remaining time,
and never subtracts clocks across processes. Overlap is association, not proof
of kernel/device causation. Observer overhead is unmeasured; this diagnostic
does not establish a mitigation benefit or close WP-810/WP-749.

## Reproduce

On the exact clean source, check and apply `observer.patch`, build the normal
daemon and application-baseline runner, and retain their digests. The private
runner additionally requires a frozen source bundle, `source-freeze.json` and
a freshly authorized `lease.json`; absence of a lease stops remote work.

From this directory:

```sh
python3 -m unittest -v test_materialization.py test_run_retention.py
python3 analyze_materialization.py /path/to/private/cell
```

`--local-functional` explicitly skips host qualification for a functional
check. Its output says that it supports no performance conclusion. Both modes
retain the same completeness and arithmetic checks. The pinned patch has
literal blank context lines containing one space; preserve those bytes.
