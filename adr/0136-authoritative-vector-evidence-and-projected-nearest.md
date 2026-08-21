# ADR-0136: Authoritative Vector Evidence and Production Projected Nearest

- **Status:** Proposed
- **Direction approved:** No
- **Exact text accepted:** No
- **Decision deadline:** Before WP-596 writes vector evidence, changes contract
  or query IR, or makes `nearest` production-reachable
- **Requires:** ADR-0002, ADR-0005, ADR-0011, ADR-0038, ADR-0040,
  ADR-0041, ADR-0055, ADR-0082, ADR-0085, ADR-0086, ADR-0087,
  ADR-0091, ADR-0100, ADR-0107, ADR-0111, ADR-0112, ADR-0115,
  ADR-0119, ADR-0124, and ADR-0130
- **Defines or blocks:** WP-596 and WP-595

## Context

The vector feasibility work proves canonical vector values, exact and bounded
nearest-neighbor evaluation, compiler-owned K limits, a first-party per-tenant
ANN tier, and policy admission before ranking. Those pieces do not yet form a
safe production application surface.

Six decisions remain load-bearing:

1. a named query cannot identify the exact vector projection or its freshness
   policy;
2. model identity, model version, the last embedding write, and the newest
   source-field write are not authoritative durable state;
3. the contract does not identify the current model whose vectors may share a
   ranking space;
4. the exact relationship between current row policy and a historical
   projection snapshot is not frozen for vector ranking;
5. ADR-0086's replay age, byte, and sequence budgets have no vector-specific
   declaration or detachment behavior; and
6. the existing staleness/model DTOs and health enum have no complete symbolic,
   authorized, generated, storage-backed operation behind them.

An implementation package cannot choose these locally. They affect contract
source and IR, query source and IR, durable authoritative records, command
atomicity, changelog and backup contents, projection retention, authorization,
health, generated clients, and public failure semantics. Guessing at any one
would create exactly the kind of application-side vector-pipeline wiring that
ADR-0091 is meant to eliminate.

This record completes the production semantics while retaining ADR-0091's
application-owned embedding computation. RiffDB validates and stores evidence;
it never calls a model provider.

## Proposed Decision

### 1. One vector field defines one explicit, compiler-owned projection source

Every production-capable vector field declares all of the following in the
contract:

- one nonempty, bounded, case-sensitive model identity;
- one nonempty, bounded, case-sensitive current model version;
- the existing dimension, metric, source-field set, stale-entity count
  threshold, and optional ANN threshold/recall target; and
- positive replay ceilings for retained age, retained bytes, and sequence
  backlog, each no greater than a first-party hard maximum.

Illustrative source, whose exact token spelling and formatter output WP-596
freezes with source-span fixtures:

```riff
vector_field embedding(
    1536,
    cosine,
    (title, body),
    staleness_slo 60,
    model "text-embedding-3-small",
    current_version "2026-08-01",
    replay_age_seconds 86400,
    replay_bytes 1073741824,
    replay_backlog 100000,
    ann_threshold 256,
    recall_target_bps 9500,
)
```

The compiler derives one stable logical vector-projection identity from the
contract lineage plus the vector field's stable entity and field identities.
Its symbolic source name is the exact `Entity.field` path. There is no runtime
lookup for "a projection containing this field," no caller-selected provider,
and no second unnamed projection for the same vector field in v1.

A named RiffQL nearest query must declare that source and one freshness policy.
Illustrative syntax:

```riffql
query SimilarDocuments(...) {
    source projected Document.embedding
    freshness causal inherit_session_commit true max_wait_ms 500

    many results from Document
        where organization_id == $organization_id
        nearest(embedding, $query_vector, $k)

    return Found { results: results { document_id title } }
    outcomes Found
}
```

`source` and `freshness` are compiler input, never request parameters. Newly
compiled `nearest` without the exact projected source is rejected. A source
whose entity/field, partition route, metric, dimension, output envelope, or
provider descriptor does not match the binding is rejected with a source-span
diagnostic. Zero matches and multiple matches are compile errors rather than
implicit selection.

The first production form contains exactly one nearest collection binding and
only predicates and output fields rooted in that same entity. It may use the
already accepted scalar filters before ranking, but it cannot add authoritative
bindings, dependent relations, aggregates, a second projection, a cross-
partition source, or a cross-provider bridge. Those forms require an accepted
ADR-0130 bridge with an epoch-transfer proof; they never fall back to N+1 row
reads or mixed snapshots.

Preexisting named queries remain authoritative by default. A preexisting
nearest query whose IR has no source declaration remains decodable but is not
production-executable; it returns a typed `projected_source_required` outcome
and cannot be granted by a newly compiled application role.

### 2. Freshness preserves ADR-0086 exactly

The query declaration admits only ADR-0086's three closed policies:

- `Causal` inherits the generated client's scoped commit token by default and
  carries a compiler-bounded maximum wait;
- `Bounded` compares the service-observed time represented by the authoritative
  head with the service-observed time represented by the projection frontier;
  it does not substitute sequence distance for elapsed time; and
- `Available` may serve the current published generation but always returns
  its opaque frontier and head.

Commit and projection tokens remain opaque, incarnation-bound types. The
existing prototype `max_lag_sequences` DTO is not evidence for ADR-0086
`Bounded(max_lag)` and must not be exposed as that policy. WP-596 either
replaces it on the production path with the declared duration observation or
keeps it confined to explicitly non-production tests.

If the required head/frontier time observation is missing, from another
incarnation, or not trustworthy, `Bounded` returns a typed freshness-
unavailable outcome. It never estimates from process wall time, sequence rate,
or a caller timestamp. Causal wait, cancellation, rebuild, degradation,
retirement, and reset retain the existing typed ADR-0086 lifecycle outcomes.

One result set negotiates and binds one exact provider generation and servable
epoch under ADR-0130. Authorization revalidation does not renegotiate a nearby
epoch, and cursor continuation never advances to a different snapshot.

### 3. Vector evidence is authoritative and atomic with the entity mutation

WP-596 introduces a versioned authoritative `StoredVectorEvidenceV1` record,
keyed by database/history identity, partition, entity target, and vector-field
stable identity. It contains:

- the newest commit sequence that wrote any declared source field;
- the last commit sequence that wrote the vector value, or explicit absence;
- the exact model identity and model version attached to that vector write, or
  explicit absence;
- the contract lineage/version/bundle and vector-spec identity under which the
  evidence was written; and
- the provenance/command binding needed to prove that the evidence and entity
  post-image came from the same command transition.

The record does not duplicate the vector components or entity post-image.
Sequences are assigned only by the commit coordinator. On an entity create or
update, the compiler-derived mutation graph determines whether a declared
source field and/or vector field changed. Final apply writes the entity,
vector evidence, its exact count/index deltas, provenance, outcome, durable
events, and commit record atomically.

The state transitions are closed:

- a source-only write advances `newest_source_write` and preserves embedding
  evidence;
- an embedding-only write advances `embedding_write` and records the exact
  submitted model identity/version after contract validation;
- a command writing both uses the same commit sequence for both revisions;
- an absent embedding has no model metadata and is stale once source state
  exists; and
- entity deletion removes current evidence and its indexes through the
  ADR-0107/ADR-0100 authoritative tombstone class in the same command.

A vector value without matching evidence, evidence without a live matching
entity/vector value, mismatched partition or field identity, a future
sequence, or model metadata without an embedding is corrupt data. Startup,
backup verification, reimport, changelog apply, and recovery fail closed.

Durable bytes are validated once at startup or ingress into a process
generation and then carried as checked values. Vector reads do not re-decode,
re-hash, or re-prove the same durable evidence per operation. Current command
dependencies and policy are still revalidated per operation because those are
the facts being guaranteed.

### 4. Embedding commands prove the contract-current model

Applications still compute vectors outside RiffDB. A compiled embedding write
has a distinct compiler-sealed instruction that binds exactly one declared
vector field, vector value, model identity, and model version. Ordinary field
assignment cannot write a production vector field.

The request carries the model identity and version as bounded typed values so
an application cannot silently label an embedding without stating its source.
The runtime requires exact equality with the active contract's declared model
identity and current version before evaluation. Generated Rust, Go,
TypeScript, and Python methods expose one canonical embedding input and cannot
omit either value; convenience constructors generated for a specific field
fill the declared constants while retaining inspectable accessors. Handwritten
protobuf, numeric field identity, arbitrary vector writes, and generic map
packing remain outside the application surface.

The runtime cannot prove which external model produced the numbers. It can and
does prevent accidental dimension mismatch, non-finite values, missing model
evidence, or storage under a model identity/version different from the active
contract. Provider credentials, endpoints, retry policy, batching, and model
invocation remain application-owned and never enter RiffDB.

Changing the declared current model requires a contract successor. Existing
evidence remains valid historical state but becomes `outdated`; it is never
relabelled. Backfill is a normal idempotent compiled embedding command. A
contract deploy cannot claim that old vectors were recomputed.

### 5. A projection generation never mixes model spaces

Every vector projection generation binds one exact model identity and version.
Only entity vectors carrying that exact evidence enter its candidate set.
Outdated or missing embeddings are excluded before distance computation,
statistics, graph construction, and ranking. They cannot influence entry
points, centroids, recall measurements, result counts, timing class selection,
or cursors.

Current-model embeddings whose declared source fields changed afterward remain
eligible while the declared stale-entity count threshold is not breached; that
is the explicit quality tolerance accepted by ADR-0091. The response and
projection status report bounded stale/current-model observations without
revealing rows outside the principal's authority. Once the threshold is
strictly exceeded, projection health degrades and freshness-constrained reads
return the typed degraded/lagging outcome instead of silently serving a lower-
quality result.

A model-version successor builds a new generation. The old published
generation may continue serving only old query/module identities that name its
exact model and remain authorized. A newly compiled query cannot borrow the
old graph, compare vectors across the two spaces, or publish until its own
generation satisfies the declared lifecycle and quality gates.

Recall is measured against the exact eligible, policy-admitted population at
the same frontier and model identity/version. Excluding outdated, unauthorized,
deleted, or wrong-partition rows is part of defining that population, not an
ANN recall success.

### 6. Current row policy is loaded before vector work

Vector queries use ADR-0111 Amendment 1 without a vector exception. From one
immutable vector-projection snapshot, the service derives the complete bounded
candidate-key set. One authoritative read snapshot reloads every candidate's
current entity and compiler-bounded relationship evidence, evaluates the
current capability's exact V4 row policy, and returns one opaque admission
proof bound to the complete candidate set.

The vector engine verifies that proof and applies it before scalar predicates,
distance computation, ANN graph/statistics construction, ranking, K, count,
cursor, or result projection. A missing current row, incomplete candidate set,
missing relationship evidence, changed capability revision, stale policy
identity, or proof mismatch denies or fails closed before vector execution.
No principal facts or allow decisions are persisted in vector state.

The projection frontier controls result data freshness; the authoritative
policy snapshot controls present authority. This deliberate two-snapshot rule
cannot widen access: current denial removes a row before all result shaping,
and current allowance exposes only the projection snapshot explicitly selected
by the query's freshness contract. Capability/policy revalidation occurs again
before release. Revocation or narrowing never waits for projection catch-up.

The bounded policy-admission ceiling applies before vector work. Exceeding it
returns the established typed bounded-query refusal and never a partial top-K.
Hidden rows do not shape graph statistics or caller-visible counts, ranks,
cursors, work classes, or results.

### 7. Replay budgets detach safely and rebuild from authoritative state

The three replay limits are part of the compiler-sealed vector projection
descriptor and cannot be supplied or widened by a query, SDK, MCP request, or
runtime fallback. Deployment may refuse a projection whose declared limits
exceed operator policy; it cannot silently substitute different semantics.

The projection controller observes retained age, retained bytes attributable
to the consumer, and head-to-durable-frontier sequence backlog. Breaching any
one limit atomically records `RebuildRequired`, detaches that projection's old
frontier from the retention watermark, and begins a bounded snapshot rebuild
plus remaining-tail replay. Detachment never advances the projection frontier,
publishes a partial generation, deletes authoritative embeddings/evidence, or
acknowledges a query.

The rebuild uses a stable authoritative snapshot, recreates only the declared
partition/model population, then applies the retained tail idempotently. A new
generation publishes only after its complete snapshot and tail are visible and
its frontier is exact. Crash before/after detachment, snapshot completion,
tail application, and publication is recoverable without an overclaiming
frontier or an unbounded retention fence.

Budget state and the detachment/rebuild decision are durable control records.
Diagnostics expose only bounded reason codes, configured limits, safe progress,
and opaque frontiers. They do not reveal hidden entity counts or values.

### 8. Staleness and model status are symbolic, bounded, and authorized

The authoritative evidence plane maintains canonical per-partition counters
and ordered evidence indexes atomically with each transition:

- total live entities for the vector field;
- source-stale entities;
- entities per exact model identity/version; and
- the number of partitions whose stale count strictly exceeds the contract
  threshold.

Health reads these maintained observations; it never scans every entity or
reconstructs evidence per probe. `VectorStaleness` is `Unavailable` when the
observer/evidence/index state is missing or invalid, `Degraded` when at least
one relevant partition breaches its threshold or its projection is detached,
and healthy only when the complete observation is current at the reported
authoritative head. "No observer" is not healthy.

Public application inspection names the contract and symbolic `Entity.field`,
not entity-type IDs, field IDs, table names, or storage keys. One compiler-
derived `InspectVectorState` role grant binds the exact vector projection,
partition scope, visible fields, maximum page size, and whether whole-partition
counts are permitted. Whole-partition counts require whole-partition authority;
a row-policy-limited principal receives only a bounded policy-filtered page and
opaque continuation, never a total from which hidden rows could be inferred.

The stable application service owns one operation with typed variants for
stale entities and outdated-model entities. gRPC, CLI, MCP, and generated Rust,
Go, TypeScript, and Python bindings map that same service operation and error
envelope. MCP exposes it only for a bound application role. The operator health
surface may report aggregate component state under operator authority but
never grants application row access.

Pages are snapshot-bound, count/row/byte bounded, canonically ordered by entity
key, and revalidate exact current capability revision before every page.
Continuation after authority, contract, model, incarnation, or evidence-index
change returns a typed reset/authorization outcome rather than splicing
snapshots. Diagnostics use symbols and source spans and redact entity keys and
model strings unless the exact output grant permits them.

### 9. Authoritative lifecycle includes backup, export, reimport, and changelog

`StoredVectorEvidenceV1`, its exact count/index rows, and control identities are
part of backup-authoritative state. Checkpoint/backup verification covers them
as one unit with entity state and journal suffix. Application export includes
typed vector evidence only under the existing distinct export authority and
field/row policy. Reimport recreates equivalent current/stale/model state under
destination-assigned sequences; it cannot invent current evidence for an
outdated or absent source record.

The replication changelog gains versioned insert/replace and delete entries for
vector evidence and its authoritative indexes. Delete uses ADR-0107's accepted
transition tombstone semantics, not retention or overlay tombstones. Follower
apply, bootstrap, chain/checkpoint validation, backup restore, export/reimport,
and create-update-delete-recreate fixtures must agree before production writes
are enabled. Unknown evidence/table/entry identities make old binaries refuse
before mutation; they are never ignored as optional derived bytes.

The physical vector ANN graph remains rebuildable derived state and stays out
of backup/export/changelog identity. Replication transports authoritative
vectors and evidence, not graph bytes. A follower may serve a vector query only
at a locally proven projection and policy frontier satisfying the same rules;
otherwise it returns typed freshness/lifecycle refusal.

## Options Considered

1. **Infer model and staleness from current entity values.** Rejected: entity
   values do not retain per-source-field write order or the model that produced
   a vector, so inference would silently bless stale or mixed-model data.
2. **Store metadata as application-visible shadow fields.** Rejected: every
   application would recreate unsafe bookkeeping and could mutate or omit it.
3. **Extend `StoredEntityRecordV1` in place.** Rejected: the frozen entity
   record is a compatibility boundary and per-vector evidence has its own key,
   count, deletion, and inspection lifecycle.
4. **Persist vector evidence in a separate authoritative record.** Proposed:
   it preserves old entity bytes while making atomicity, backup, changelog,
   validation, and symbolic inspection explicit.
5. **Select a vector projection implicitly from the nearest field.** Rejected:
   zero/multiple providers, model generations, and freshness would become
   runtime guesses outside the plan identity.
6. **Post-filter ANN results by current row policy.** Rejected: hidden rows
   would shape graph statistics, distances, ranks, counts, cursors, and timing.
7. **Mix old and current model versions until backfill completes.** Rejected:
   distances across embedding spaces have no declared meaning.
8. **Let an unhealthy projection retain history indefinitely.** Rejected:
   one consumer could exhaust disk and turn quality lag into authoritative
   unavailability.
9. **Have RiffDB call the embedding provider.** Rejected by ADR-0091 Amendment
   1: provider execution is nondeterministic external application work.

## Consequences

- Vector writes gain one authoritative evidence transition and bounded index/
  counter maintenance in the originating command.
- A production nearest query becomes explicit, generated, freshness-aware, and
  incapable of choosing a provider or mixing model spaces.
- Contract successors changing the current model truthfully make old rows
  observable as outdated and require an idempotent application backfill.
- Policy admission may cost one bounded authoritative row/relationship read per
  candidate. This accepted cost is the existing ADR-0111 security boundary; a
  later pay-once cache must bind the exact authoritative snapshot, candidate
  set, capability revision, and policy identity.
- Replay detachment bounds retention growth at the cost of typed temporary
  rebuild unavailability.
- Duration-based embedding staleness remains deferred. The v1 embedding SLO is
  the already accepted strict stale-entity count threshold; ADR-0086 bounded
  *query freshness* remains duration-based and is not the same concept.
- Hybrid text/vector scoring, multi-vector fields, cross-projection joins,
  caller-selected providers, and database-managed model calls remain deferred.

## Compatibility

WP-596 introduces the next unused least-sufficient contract bundle, grammar,
executable IR, RiffQL language, query IR, query-module, generated-surface,
public-wire, durable-record/table, command-segment, changelog, backup, export,
and reimport identities required by the implemented semantics. V15 is the
expected contract bundle/grammar/executable-IR slot while V14 is current, but
the implementation must verify the registry at merge and use the next free
identity if concurrent accepted work advances it. No same-commit whitelist or
fence expansion may self-authorize an unaudited identity.

Older contract/query artifacts and entity records remain byte-identical and
readable under their declared windows. They cannot access the new production
surface without exact source/model/replay metadata. Newly written evidence
uses only the successor durable identities; old binaries detect the new
authoritative table/format through the durable-format manifest and refuse
before mutation.

The identity-rotation commit regenerates and audits every affected golden
literal, format registry, version-topology entry, lock, module/plan hash,
generated client, public schema, example, migration fixture, backup/export
fixture, and source-span snapshot. `application check`, deploy, role binding,
and generated clients must report the expected/active/lock identities together
when they disagree.

## Security

Default is deny. Callers cannot choose projection, provider, metric, model,
freshness class, replay budget, policy mode, candidate set, scan budget,
ranking precision, fallback, or evidence fields. Current row policy and current
capability revision are checked before vector work and again before release.
Wrong-model and incomplete-evidence writes are rejected before mutation.

Model identity/version and entity keys are application data, not safe generic
diagnostics. Logs, metrics, health, MCP text, and public errors carry symbols,
bounded counts, closed reason codes, and incident/trace IDs unless an exact
authorized response field permits values. Durable records never contain model
provider credentials or endpoints.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** an application can invoke only
  a compiler-fixed named nearest query or embedding command. Every vector write
  carries validated model evidence; every read has an exact source, freshness,
  partition, K, model space, policy proof, replay budget, and typed lifecycle.
  No public request can omit or weaken one of those guarantees.
- **Scale:** vector dimension, source fields, candidate admission, exact scan,
  K, ANN work, replay age/bytes/backlog, retained snapshots, evidence pages,
  response bytes, diagnostics, waits, and rebuild resources are bounded per
  declared partition. The authoritative evidence table and derived projection
  are partition-addressable and require neither whole-database memory nor
  co-location with future followers. Whole-partition counts use maintained
  counters rather than per-request full scans.

## Testing

- Grammar/IR/compiler span tests for missing/duplicate/oversized model and
  replay declarations, wrong vector source, missing freshness, mixed sources,
  invalid K, and unsupported joins.
- Frozen old/new bundle, query/module, wire, durable evidence, command segment,
  changelog, backup, export, reimport, and generated-client fixtures plus the
  complete ADR-0124 topology audit.
- Command tests for source-only, embedding-only, combined, absent embedding,
  wrong model, dimension/finiteness failure, idempotent replay, conflict,
  deletion, recreate, cancellation, crash, and lost response.
- Startup and deterministic-schedule invariants proving entity/evidence/index/
  counter atomicity and private/applied frontier equivalence.
- Projection tests for Causal/Bounded/Available, model-version rebuild, stale
  threshold at/equal/over, replay age/bytes/backlog detachment, crash at every
  detach/rebuild/publication boundary, and retention-watermark release.
- Adversarial row-policy tests prove denied rows affect no vector, graph,
  statistic, distance, rank, count, cursor, output, or public work class.
- Exact/ANN recall at the same model, policy-admitted population, partition,
  and frontier; no mixed-version population can enter the evaluator.
- gRPC/MCP/CLI and generated Rust/Go/TypeScript/Python parity for embedding
  writes, nearest results, freshness/lifecycle outcomes, health, and symbolic
  bounded inspection.
- Architecture checks prove no storage adapter serves source-less nearest, no
  application crate constructs numeric vector identities or generic writes,
  no evidence is re-proven per read, and no derived graph enters authoritative
  backup/changelog/export.

## Requirements and Work Packages

- **Requirements:** VEC-001 through VEC-012; API-001; SAFE-001, SAFE-002,
  SAFE-006, and SAFE-008
- **Defines or blocks:** WP-596
- **Final evidence:** WP-595 and WP-579

## Decision Deadline

Exact human acceptance is required before WP-596 changes contract or RiffQL
source, allocates successor IR/wire/durable identities, persists vector
evidence, enables embedding writes, exposes vector inspection, or routes a
production nearest query to a projection. Until then, the existing storage
adapters must continue failing source-less nearest closed.
