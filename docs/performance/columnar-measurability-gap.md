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

The `cold_sources` column is the finding; see below for why the throughput
column is not yet a result.

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

## Progress, 2026-09-20 continued: the query module deploys

`deploy_query_module` is now wired into the harness and **a `nearest` query
module deploys successfully against the vector contract**. The compiler accepts
a projected vector query over the perf-surface schema, which was the piece
previously recorded as a separate work package.

Getting there cost three wrong turns, each recorded because each looks like a
different failure than it is:

- **`RiffDbClient` cannot do this.** Its generated surface carries
  `deploy_contract` but not `deploy_query_module`, and its service clients are
  private fields in generator-owned code. The tonic client type is public, so
  the harness constructs its own; no generated code was edited.
- **`CallMetadata::apply` is crate-private**, so the bearer header is set
  directly and must match what `bearer` produces.
- **A missing `ContractSelector` surfaces as an HTTP/2 stream reset**, not a
  typed error. The server's wire validation returns `MissingRequiredField`,
  which resets the stream, so an absent selector looks exactly like a transport
  fault and points at the wrong layer entirely.

## The write-path figure is not yet settled

Two local runs disagree about what declaring a vector field costs:

| run | base docs/s | vector docs/s |
|---|---:|---:|
| columnar probe, 200 docs, 8 clients | 2,764 / 2,273 | 2,163 / 2,244 |
| full variant sweep, 200 docs, 8 clients | 2,862 | 1,445 |

The first says the cost is inside the spread; the second says roughly half. Both
are 200-document runs on a workstation whose base spread has been measured at
11 percent, which is wide enough that neither settles it and short enough that a
single slow start distorts the whole figure.

**Neither number is evidence.** The earlier entry above recorded "costs nothing
detectable on the write path" from the first pair; that claim is withdrawn until
it is taken on the bench host at a workload size where the spread is 2 to 4
percent rather than 11. It is recorded here rather than deleted because the
first reading was quoted once already, and a withdrawn number that quietly
disappears is worse than one that says why it went.

## What still blocks execution

Executing the deployed query is refused with `AuthorizationDenied`, and the
obvious cause has been ruled out. The runner capability now carries an
`ExecuteNamedQuery` permission with the correct contract lineage, the 32-byte
module hash the deployment returned, and the exact query name, under a global
tenant scope and an all-partitions partition scope. It is still refused.

So a projected vector query requires **something beyond the named-query
permission**, and identifying it is the next step. The candidates visible in
`CapabilityPermission` are `query_projection` (which takes a
`LineageScopedStableId`, so it needs a projection identifier the harness does
not currently obtain) and `inspect_vector_state`. The capability's
`max_scan_rows: 1` is also worth ruling out, though a scan bound should surface
as a limit rather than an authorization refusal.

This is a narrow, well-defined question rather than an open design problem: the
deployment works, the permission shape is right, and what remains is finding
which additional grant a projected source requires.

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
