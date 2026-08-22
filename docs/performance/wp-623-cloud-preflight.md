# WP-623 current-HEAD cloud preflight

WP-623 remains open. A short, non-evidentiary full-scale preflight at revision
`a92254af` shows that the current unary artifact clears the mixed c32, tail,
and seed gates on both cloud profiles, but cannot clear the representative
unary-scenario gate. The required three-repetition 90-second qualification was
not started because a known failing preflight cannot become release evidence
by running longer.

## Fixed shape

N1 and E2 each ran sequentially isolated safe-application PostgreSQL and
public unary gRPC RiffDB phases. The workload used the full TicketDesk scale,
interactive weights, one persistent connection per client, standard
acknowledged RiffDB durability, PostgreSQL `fsync=on` and
`synchronous_commit=on`, 15 measured seconds after three warmup seconds, and
one repetition. Correctness reconciliation was clean in every cell.

The short duration and single repetition make these diagnostics ineligible for
`PERF-018` qualification.

## c32 result

| Host | RiffDB / safe-PG throughput | RiffDB / safe-PG p95 | RiffDB / safe-PG seed | CreateComment p50 ratio |
| --- | ---: | ---: | ---: | ---: |
| N1 | 0.944x | 1.241x | 3.76x | 1.28x |
| E2 | 0.935x | 1.120x | 2.47x | 1.19x |

The first three columns clear the current `PERF-008` gates: mixed throughput
at least 0.90x, p95 at most 1.25x, and seed at most 5.0x. This is a useful
directional improvement over the earlier cloud baseline, but it cannot waive
the separate unary-scenario requirement.

## c1 result

At c1, every representative generated scenario must be at most 1.10x the
same-run safe-application PostgreSQL latency. The point-read, list, detail, and
several command shapes miss:

| Scenario family | N1 ratio range | E2 ratio range |
| --- | ---: | ---: |
| point reads | 2.00x-2.13x | 1.61x |
| bounded lists | 2.22x-2.42x | 1.79x-2.09x |
| ticket detail | 1.56x | 1.31x |
| CreateComment | 1.22x | 1.06x |
| atomic multi-entity commands | 1.20x-1.40x | 1.05x-1.16x |

The failures are large enough that percentile quantization cannot change the
decision.

## Current read ledger

The diagnostic-only exact generated `GetTicket` run used 1,000 observations
after 64 warmups and enabled fixed-cardinality query-execute stages. The
synchronous benchmark bridge costs only 3.48 microseconds on N1 and 2.66
microseconds on E2; removing it is measurement correction, not product gain.

| Host | Async generated-call mean | Server-stage sum | Outside-service residual | Safe-PG GetTicket mean | 1.10x target |
| --- | ---: | ---: | ---: | ---: | ---: |
| N1 | 539.7 us | 124.5 us | 415.3 us | 284.3 us | 312.7 us |
| E2 | 480.6 us | 92.9 us | 387.7 us | 294.1 us | 323.5 us |

The diagnostic process and the mixed-load process produce slightly different
absolute means, but the decision is invariant: the outside-service component
alone exceeds the complete 1.10x budget on both hosts. Eliminating all server
query work could not make unary gRPC pass. Server-only read caching, further
entity-path work, or benchmark-driver changes are therefore not sized to the
remaining gate.

The query-execute windows also remain flat through the sample sequence. This
is a per-operation transport/orchestration floor, not renewed operation-count
growth.

## Receipts

- N1 c1: `3c106cff29a79e3b07a2b9fe87b4af6352f1b783a96fb750249645999da964d2`
- N1 c32: `eb5ee2ecba8bd98168d9cc41a6cf914fc80450064c2ea048926c0cb957178b3d`
- E2 c1: `0affc95c2815fa3315886d218bbd2e605695a585dc862111d250b8b1670b3b62`
- E2 c32: `e8bbc5618c592446c44b7639ff9b10fc4d8493ba0aba82e76a4879c8e949f97b`
- N1 exact-read diagnostic: `56013d9de3c2cacb1934029c7c3705f339b48eb9856f383fe1d922594cf440ce`
- E2 exact-read diagnostic: `529461e4c0133c81e0578fb8511784b455e374e25ac6120eb445990f8c9cec57`

Raw value-free reports remain under `/home/user/tmp/wp623-preflight/`.

## Decision boundary

WP-623, WP-578, and WP-579 remain blocked. The next performance design must
remove the outside-service scheduling/protocol floor and also reduce N1 unary
command latency; it must not reopen the writer, entity, durable-format, or seed
campaigns that already satisfy their real-world gates. Any new application
transport or session-level authentication proof scope requires a separately
accepted ADR before implementation.
