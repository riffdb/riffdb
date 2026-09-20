# The columnar leg is unmeasured, and why

RiffDB's stated product thesis is that one database serves the transactional and
the analytical shape of an application — the role PostgreSQL and ClickHouse play
together. Every performance figure this project has banked covers the
transactional half. **No benchmark has ever activated the columnar engine.**
`columnar_activations` is zero in every run recorded here.

This note records why, and what closing it needs, so the gap is a known quantity
rather than an absence nobody has looked at.

## Why it is zero

Not because the engine is unreachable. The path exists and is production code:
`columnar_adapter.rs:1751 resolve_production_vector_projection` builds a
`ColumnarProjectionDefinition` from a contract bundle, reached through
`resolve_vector_registration` at `:1791`. The columnar engine backs **vector
projections**, which is how a contract asks for it.

It is zero because **no benchmark contract declares a vector field**. Two
contracts in the repository do, and both are compiler fixtures rather than
workloads:

- `fixtures/vector-exit/documents.riff` (37 lines) —
  `vector_field embedding(4, cosine, (title, body), staleness_slo 60, ...)`
- `fixtures/compiler/production-embedding/contract.riff` (25 lines)

The benchmark contracts — ticketdesk, and the perf-surface variants — declare
none, so the engine is never admitted, never activated, and never measured.
`docs/performance/deferred-dormant-path-optimizations.md` records the same
absence for projections, tokenized text and row policies; this is the fourth and
largest instance of it.

## What closing it requires

1. **A workload contract declaring a vector field.** The perf-surface harness
   already builds one contract variant per mechanism
   (`benchmarks/perf-surface/src/lib.rs`, `Mechanism::{Projection, TextKey,
   TokenizedText}`); a fourth variant is the natural shape, and the two fixture
   contracts show the declaration.
2. **An activation wait, not a sleep.** WP-777 keeps every admitted source
   `Cold` until a semantic projected query demands it, and the first query
   returns the typed `Building` result with **no rows**. A harness that measures
   without waiting measures a cold source returning nothing — the same class of
   error as measuring a projection whose apply had silently stopped. The wait
   must observe the `projection` health component becoming healthy, or poll the
   typed result until it stops being `Building`.
3. **Both shapes measured, not just ingest.** The thesis is that one database
   serves both, so the figure that matters is write throughput with a columnar
   source admitted **and** analytical query latency against it — not either
   alone.

## What it must not claim until then

No public performance claim about analytical or mixed workloads is supported by
anything banked here. ADR-0239 decision 4 already binds this: the baseline
covers the commit path and named-query point reads on ticketdesk, and says so
wherever it is quoted. Until a contract declares a vector field and a harness
waits for activation, "RiffDB replaces PostgreSQL and ClickHouse" is a design
intent with measurement behind only the first half.
