# WP-362 command-growth investigation

## Finding

The observed fall from roughly 180 to 5 application operations per second was
not a gRPC deadlock and was not primarily redb page growth. The redb
administration adapter performed an exhaustive, decoding scan of the complete
administration-audit table before every append. A public application request
normally writes a `Started` record and a terminal record, so retained request
history caused linear per-request work and quadratic seed time.

An exact-table mechanics benchmark did not reproduce the collapse. With the
production `Immediate` durability and two-phase commit shape, the final
4,096-command window retained more than half of the initial window throughput.
That isolated the semantic adapter’s audit validation from engine commit and
multi-table costs.

## Accepted optimization

Every startup and public audit read continues to validate and decode the entire
contiguous stream. Before a typed write, the adapter now verifies:

1. the transaction-current allocator is decodable;
2. table length equals the number of already allocated sequences;
3. the exact final key equals the previous allocated sequence; and
4. the decoded final record carries that same sequence.

This check preserves the startup proof inductively because production has no
raw audit mutation path: each accepted transition appends exactly one
allocator-owned sequence and atomically advances the allocator. Tail loss,
extra rows, malformed tail records, and allocator skew fail before mutation.
The full read validator still rejects corruption anywhere in retained history.

The change does not alter immediate durability, two-phase commit,
acknowledgement timing, the sole-writer coordinator, the atomic command record
graph, uncertainty fencing, replay, provenance, outcome, or sequence semantics.
It therefore requires no durability or scheduling ADR.

## Qualified gate

Run:

```text
./scripts/benchmark-command-growth --assert-perf-003
```

The wrapper runs semantic parity and recovery suites for memory storage, redb,
the commit coordinator, and the public server before issuing a report. The
benchmark will not assert PERF-003 without the wrapper evidence.

The primary size sweep uses the real service-audit repository with a
`Started`/`Failed` lifecycle per command. Each reported command includes two
independent immediate, two-phase durable commits. It reports typed-intent
preparation, repository time, retained size, file growth, and reopen cost.

A separate bounded mechanics comparison reports no-flush, immediate
one-phase, production immediate two-phase, and experimental group-of-16
results. The latter two alternatives are evidence only. Production durability
and scheduling remain unchanged pending a separately accepted ADR.

PERF-003 passes only when the final window:

- retains at least 50% of the initial window throughput; and
- completes at least 50 command lifecycles per second.

The checked JSONL evidence is stored in
`fixtures/performance/wp-362-command-growth.jsonl`.
