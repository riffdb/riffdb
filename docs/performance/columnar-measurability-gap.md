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

## Progress, 2026-09-20: the engine is now reachable from a benchmark

A `Vector` mechanism was added to the perf-surface harness, declaring
`vector_field embedding(4, cosine, (title, body), ...)` on `Document` with the
`embed ... from (model, version)` inputs its command needs. Measured on this
workstation, 200 documents at 8 clients, reopening the database so the startup
census sees the deployed contract:

| variant | `columnar_cold_sources` | `columnar_activations` | docs/s |
|---|---:|---:|---:|
| base | 0 | 0 | 2,273-2,764 |
| **vector** | **1** | 0 | 2,163-2,813 |

**A declared vector field registers a columnar source.** Activations remaining
at zero is correct rather than a failure: WP-777 keeps a source cold until a
projected query demands it, and this workload only writes. Declaring the field
costs nothing detectable on the write path — both ranges sit inside this host's
spread, which the cold-source design predicts.

Two observability traps cost time and are recorded so they do not again:

- **The columnar counters are emitted only in the startup census**, so a process
  can never report on a source its own run admitted by deploying a contract. The
  database must be reopened and the second process's census read.
- **That census goes to stderr, not stdout.** A probe reading the shutdown
  stdout sees nothing and looks exactly like an engine that was never reached.

Either mistake alone produces a confident, wrong conclusion that columnar is
unreachable — the same shape as reading an absence of signal as an absence of
behaviour.

## What still blocks the analytical half

Activation needs a projected query, and the harness cannot issue one yet. It
deploys a contract through `deploy_contract` only; a `nearest` binding lives in
a RiffQL **query module**, which is a separate deployment path
(`docs/riffql/LANGUAGE.md` §Nearest-neighbor bindings). Closing the gap needs:

1. **Query-module deployment.** The blocker is specific: the Rust client's
   generated surface has `deploy_contract` but **no `deploy_query_module`**
   (`riffdb-client-rust/src/generated/client.rs`), and that file is
   generator-owned. Two routes avoid editing it — call the RPC through raw
   tonic, as the harness already does for `DeployContractRequest`, or shell out
   to the CLI, which has the whole path: `riffdb query deploy --module-name X
   --module-version 1 <dir>`, `query run-named`, `query projected`, and
   `query inspect-vector`. The CLI route needs a credential file where the
   harness currently holds an in-process bootstrap token.
2. **The query itself**, verified to parse against the grammar on 2026-09-20:

   ```riffql
   query SimilarDocuments(
       $workspace_id: Document.workspace_id,
       $query_vec: Document.embedding,
       $k: Limit<64>,
   ) {
       source projected Document.embedding
       freshness available

       many results from Document
           where workspace_id == $workspace_id
           nearest(embedding, $query_vec, $k)
       return Found { results: results { title } }
       outcomes Found
   }
   ```

   The partition equality predicate is mandatory: a nearest query without it
   does not compile (`RDB-QP002`). Parsing is not compilation — this still has
   to bind against the deployed bundle's `source projected` declaration.
3. **An activation wait, not a sleep.** The first query returns `Building` with
   no rows, so the harness must poll until the typed result stops being
   `Building`, or observe the `projection` health component. A fixed sleep
   measures a cold source and reports it as a fast one.
4. **Query latency measured after activation**, alongside the write figure
   above. The thesis needs both shapes, not either alone.

## What it must not claim until then

No public performance claim about analytical or mixed workloads is supported by
anything banked here. ADR-0239 decision 4 already binds this: the baseline
covers the commit path and named-query point reads on ticketdesk, and says so
wherever it is quoted. Until a contract declares a vector field and a harness
waits for activation, "RiffDB replaces PostgreSQL and ClickHouse" is a design
intent with measurement behind only the first half.
