# WP-378 command CPU pipeline evidence

WP-378 removes serialized command-pipeline work without changing admission
order, authorization, transaction-current validation, sequence assignment,
durability, audit, provenance, or acknowledgement semantics.

## Changes

- Compatible command groups are built incrementally. The accumulator retains
  exact conflict keys plus read and write targets in hash sets and produces the
  same maximal FIFO partition as the former prefix-rescan algorithm.
- Checked command write plans and evaluated runtime results use immutable
  shared ownership after their identity and bounds are sealed. Cloning a later
  typestate no longer deep-clones the mutation/event graph.
- Durable record registries are validated once per process and reused through
  immutable static storage.
- Canonical record lengths and final canonical byte encodings are retained by
  their sealed semantic records. Sequence-free reservation is calculated
  structurally from those lengths; it no longer creates sizing-only entity,
  event, outcome, provenance, outbox, and commit protobuf graphs.
- Final entity, index, event, and declared-outcome protobuf construction reuses
  the sealed canonical bytes. The exact final envelope charge remains checked
  against the conservative pre-sequence reservation.
- A command's transaction-current entity versions and index generations are
  decoded and validated once in the authoritative write transaction. Staging
  reuses those observations and checks the mutation operation's returned
  prior-row presence. Any mismatch aborts the transaction fail-closed.
- Outcome rendering retains the exact validated contract bundle instead of
  cloning its complete schema and outcome catalog for every command.

## Safety argument

The reservation uses maximum-width future sequence and version values. Every
final record class and the aggregate are still compared with the reservation
before staging. Compatibility remains exact: conflict-key overlap,
read/write overlap in either FIFO direction, and write/write overlap split the
group. The writer remains sole and admission ordered.

The removed redb reads were not independent observations. The same candidate
had already read and decoded the exact row immediately before validation in
the same write transaction, with no intervening command. Earlier candidates
in a physical group are visible to that read. The insert return value supplies
an additional absent/present structural check, and dropping the transaction
rolls back a mismatch.

Cached bytes are derived only inside constructors that validate bounded
canonical records. Recovered, migrated, or external bytes still pass envelope,
CRC, schema, preflight, canonicality, and semantic validation before a sealed
record can exist.

## Measurements

The full TicketDesk workload contains 15,160 independently idempotent commands
and runs over the public application gRPC surface. On the reference development
host, the optimized pipeline completed a release seed in approximately
1.5 seconds (about 10,000 commands/second), while representative named reads
remained around 0.2–0.3 ms and unary commands around 0.4–0.6 ms.

Sampling before the changes identified registry reconstruction, protobuf
sizing/encoding, compatibility rescans, allocation/cloning, redb reads, and
canonical preflight among the coordinator's non-I/O costs. The full benchmark,
write-completion group histogram, exact reservation tests, storage backend
suites, and process restart test are the release evidence; an isolated storage
microbenchmark is not sufficient for WP-379 eligibility.

WP-378 does not itself declare application parity. WP-379 must run PostgreSQL
and RiffDB in the same invocation and retain every miss against `PERF-008`.
