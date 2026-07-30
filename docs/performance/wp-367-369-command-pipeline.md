# WP-367–369: bounded command pipeline and concurrent reads

Status: implementation evidence; PERF-008 parity remains a release gate.

## Product rule

Performance work may remove redundant computation, unnecessary serialization, and
avoidable storage scans. It may not weaken the application safety model.

The production path therefore retains:

- symbolic application operations and compiled command plans;
- fresh authorization and response-release checks;
- canonical conflict acquisition;
- transaction-current dependency validation;
- one admission-ordered authoritative writer;
- atomic mutation, outcome, event, provenance, audit, and commit records;
- exact idempotency replay and uncertainty recovery;
- Immediate redb durability for every acknowledged durable transition.

The coordinator still records a durable `Started` admission before evaluation and
a separate terminal transition. Fusing those transitions is not an optimization
covered by ADR-0060; it would change transaction ordering and requires explicit
human architecture review.

## Implemented pipeline

The command actor drains its bounded receiver into an ordered local deque. After
receiving the oldest groupable command it waits for at most 200 microseconds,
selects at most 64 commands, defers only read-only idempotency observations, and
never crosses catalog, policy, administration, readiness, fencing, shutdown, or
other non-deferrable barriers.

Coordinator admission is independently bounded by 128 messages and 32 MiB of
estimated retained preparation memory. Group dispatch reports a closed reason
(`full`, `barrier`, `window_elapsed`, or `receiver_closed`), selected count,
deferred count, and collection duration without command data.

Historical idempotency selection uses bounded grouped reads. Admission still
rechecks each selected identity in the authoritative write transaction. Replay
`Started` audit records share the grouped admission transaction.

At most eight fixed workers perform read-only snapshot materialization,
deterministic evaluation, and record preparation. Conflict capabilities are
acquired in admission order, remain move-only, and are returned to the one
writer through an ordinal reorder buffer. The writer commits only the earliest
contiguous compatible results.

Active plans and schemas are immutable, hash-bound `Arc` artifacts. Named RiffQL
queries retain their parsed document. Command inputs are normalized once on the
normal active path, protobuf sizing reuses shared messages, and redb no longer
repeats event hashing already proved by the sealed graph.

## Storage and publication

The server bridge uses shared read guards for redb MVCC read transactions and an
exclusive guard only for mutable administrative interfaces. Admission and
application transactions use shared access to the port bundle; redb's private
mutation gate remains the sole writer serialization point. Independent RiffQL,
entity, catalog, capability, projection, and idempotency reads can proceed
concurrently.

The blocking-port driver uses a bounded condition-variable queue with true
multi-consumer wakeup. Workers do not hold a receiver mutex while waiting for
the next job. The production driver retains 256 total admission permits and 32
blocking workers; a deterministic test proves two workers enter distinct jobs
before either job is allowed to complete.

First-commit publication is grouped. No-destination outbox readiness uses the
recovery-seeded transient undelivered index instead of scanning durable outbox
status once per committed command.

## Evidence and open gate

Semantic, architecture, recovery, crash/reopen, redaction, formatting, and
Clippy suites must pass before performance evidence is considered.

The same-run TicketDesk gate is:

```text
./benchmarks/run-app-baseline --full --assert-all-parity
```

It requires seed time and every scenario p50 to be at most 1.10 times
PostgreSQL. A miss is reported as a failed gate; it must not be converted into a
waiver or a weaker durability mode. The current implementation materially
improves grouping and CPU amplification, but the strict parity gate remains red
on the recorded development host. The final run seeded 15,160 commands in
3.185 seconds versus PostgreSQL's 0.687 seconds (4.63 times); representative
RiffDB/PostgreSQL p50 ratios ranged from 1.31 times for
`close_ticket_with_comment` to 7.53 times for `list_comments_for_ticket`.
WP-370 cannot start until the seed and every scenario are at or below 1.10
times.

Concurrent point-read evidence is produced by:

```text
./scripts/benchmark-concurrent-application --checked
```

It runs independent 1, 8, 32, and 64-client workloads against both live
PostgreSQL and live public RiffDB gRPC. On the development host, removing the
mutexed receiver and increasing the blocking worker set changed the 32-client
RiffDB sample from roughly 3.6k operations/s with 20ms p95 to roughly 11k
operations/s with 4.7ms p95. This closes the artificial receiver
serialization, but it does not satisfy PERF-008 parity with PostgreSQL; the
remaining public-path CPU and transport costs require further work.

The final checked sweep recorded:

| Clients | PostgreSQL ops/s | RiffDB ops/s | PostgreSQL p95 | RiffDB p95 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 27,310 | 2,921 | 0.039 ms | 0.441 ms |
| 8 | 128,218 | 11,103 | 0.088 ms | 1.037 ms |
| 32 | 164,558 | 10,822 | 0.327 ms | 5.036 ms |
| 64 | 236,105 | 3,480 | 0.490 ms | 22.815 ms |

PERF-007's bounded scaling check passes because eight-client throughput exceeds
1.5 times the single-client result. The 64-client result is nevertheless a
second saturation collapse and remains explicit evidence against declaring
WP-369 complete. The machine-readable source is
`target/concurrent-application/report-v1.json`.
