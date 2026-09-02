# ADR-0177: Compiler-Bounded High-Cardinality Atomic Collections

- **Status:** Accepted
- **Direction approved:** 2026-09-01
- **Exact text accepted:** 2026-09-01
- **Acceptance reference:** Maintainer approval in the current Codex session
- **Requires:** ADR-0002, ADR-0003, ADR-0013, ADR-0031, ADR-0055,
  ADR-0059, ADR-0107, ADR-0147, and ADR-0176
- **Amends:** ADR-0107 and ADR-0147 only where they retain the first-format
  256-element and 256-mutation-instance ceilings
- **Defines or blocks:** WP-745
- **Decision deadline:** Before accepting a collection command above either
  legacy cardinality ceiling or assigning its executable identity

The maintainer accepted the exact bounded successor below on 2026-09-01 after
reviewing adapter-side subdivision against the reference transactional store
semantics. The accepted direction is a generic compiler-owned capacity tier,
not an application-specific exception or a caller-selected transaction size.

## Context

ADR-0107 introduced one statically expanded, one-partition atomic collection
command and deliberately capped both submitted elements and possible mutation
instances at 256. ADR-0147 added correlated aggregate-byte proofs without
changing those count ceilings. ADR-0176 later allowed a compiler-proved root
input through 4 MiB while retaining the 16 MiB complete command graph.

A legal application operation can now fit every byte, partition, conflict,
index-work, input, and graph proof but still fail solely because it contains
more than 256 small rows. A representative store operation accepts one bounded
request containing as many as 1,000 total metric, parameter, and tag entities.
Its reference transactional implementation applies one such store call in one
database transaction. The framework already subdivides work larger than that
public store limit before invoking an adapter.

Subdividing the already bounded store operation again in an adapter would add
observable partial commits. A later duplicate, validation failure, conflict,
crash, or cancellation could leave earlier chunks committed, and retry would
no longer resolve one original command identity. An application-level staging
protocol would recreate transactions, visibility filtering, recovery, and
garbage collection outside RiffDB's compiler and commit coordinator.

The lower command runtime and storage boundary already admits at most 4,096
entity mutations, binding observations, dependencies, index deltas, and event
intents where their independent limits allow them. The missing capability is
therefore a least-sufficient collection-plan tier that uses those existing
bounds without weakening any of them.

## Decision

### 1. Add one fixed successor collection tier

One compiler-owned collection expansion may declare at most 1,024 submitted
elements. One complete command may have at most 4,096 statically possible
authoritative mutation instances.

The mutation-instance proof includes every non-read binding outside the
collection template once plus every non-read collection-local binding times the
declared maximum element count. A cascade binding retains its relationship
weight, and checked arithmetic is mandatory. ADR-0107's one-list rule,
ADR-0059's one mutation aggregate, and the exact one-partition proof remain
unchanged.

This ADR does not widen compiler-bounded cascade deletion. `DEL-004` continues
to cap the complete possible cascade graph at 256 rows. It also does not promise
that every source declaring 1,024 elements or 4,096 mutations compiles: any
independent byte, conflict, index, observation, event, or graph proof may impose
a lower maximum.

The limits are fixed first-party POC ceilings. They are not contract literals,
role grants, deployment settings, environment variables, transport negotiation,
or caller controls. A source list continues to carry its exact smaller maximum,
and generated interfaces enforce that exact maximum.

### 2. Retain every independent safety and locality bound

The following existing ceilings and semantics remain unchanged:

- one submitted list and no nested or general iteration;
- one exact partition and one compiler-proved mutation aggregate;
- 256 conflict-key derivations after canonical deduplication;
- 4,096 index-entry deltas, binding/root observations, dependencies, entity
  mutations, and event intents;
- 65,535 affected-prefix, validation-position, and correlated index-work units;
- 4 MiB decoded/canonical root command input and 8 MiB command framing;
- 1 MiB individual and nested canonical values;
- 16 MiB complete canonical input and authoritative write/event graph; and
- one idempotency identity, outcome, commit sequence, audit lifecycle,
  provenance record, and atomic complete-or-absent commit.

The compiler proves all maxima before deployment. Runtime and storage recheck
the concrete graph before effects and at authoritative admission. No layer may
page, truncate, stream, deduplicate, or partially apply a command to fit a
ceiling.

### 3. Use least-sufficient contract identity V23

A command whose submitted-element maximum and complete mutation-instance
maximum are both at most 256 retains its exact existing grammar, executable IR,
bundle, plan hash, lock, role, and generated artifact bytes.

A collection command exceeding either legacy maximum, while fitting the new
fixed tier and every independent proof, requires contract grammar, executable
IR, and bundle V23. V23 does not add source syntax or a new encoded collection
field. Its semantic identity authorizes the larger interpretation of the
already encoded exact list maximum and sealed binding plan.

The V23 writer recomputes the complete mutation-instance maximum from the
checked schema and plan. Decoding V23 performs the same recomputation. V1
through V22 decoding continues to reject an element or mutation-instance
maximum above 256, so malformed historical bytes cannot acquire successor
semantics. V23 participates in command-plan and bundle identity even when a
different feature would otherwise require an older version.

No canonical application value, command input, idempotency hash, entity/index
key, mutation, event, outcome, commit record, provenance record, durable
envelope, public Protobuf message, or driver protocol encoding changes.

### 4. Keep every application surface compiler-owned

Rust, Go, TypeScript, Python, CLI, MCP, local-driver, and remote-gRPC paths
continue to accept only the generated typed collection declared by the exact
command plan. They expose the source-declared maximum, not the 1,024 process
ceiling, and cannot choose V23, mutation capacity, transaction boundaries,
chunking, fallback, or retry subsets.

Cardinality overflow remains a typed, bounded, source-spanned compiler or
submitted-value error as appropriate. Diagnostics may identify the closed
resource, checked actual maximum, and fixed ceiling but never submitted values,
keys, records, or hidden plan state.

## Options Considered

1. **Adapter-side chunks of at most 256:** Rejected because one accepted store
   operation could become several visible commits with partial failure and
   non-equivalent retry semantics.
2. **Application-owned staging rows and a final marker:** Rejected because it
   recreates transactions, visibility, cleanup, and recovery outside compiled
   commands and still consumes more operations and durable state.
3. **Raise every command, conflict, byte, or transport ceiling:** Rejected
   because only collection cardinality is missing and the representative graph
   already fits the independently qualified limits.
4. **Use exactly 1,000 and 1,001 as framework-shaped process limits:** Rejected
   in favor of fixed generic binary capacity aligned with the existing 4,096
   lower mutation boundary. The application plan still proves its exact 1,000
   and 1,001 maxima.
5. **Least-sufficient V23 with 1,024 elements and 4,096 mutations:** Accepted
   because it preserves old identities and uses existing bounded runtime
   capacity without admitting arbitrary transactions.

## Consequences

- Small-row atomic collection commands can use the already supported lower
  mutation capacity without application-side partial commits.
- Worst-case deterministic evaluation and conflict holding time may increase;
  admission, byte, work, deadline, cancellation, and storage limits remain
  mandatory and receive boundary and stage-cost tests.
- A source may fit the element ceiling but fail another proof. Such refusal is
  correct and must identify the owning fixed resource rather than silently
  lowering the source maximum.
- Existing deployed collection commands remain byte-exact and do not acquire
  additional capacity unless recompiled from source that needs V23.

## Compatibility

The change is source-additive and uses least-sufficient V23 identities. V1
through V22 remain readable and byte-exact, and their collection semantics
retain the 256/256 ceilings. Old readers reject V23 before deployment. No
decoder retirement or data migration is authorized.

V23 reuses the existing encoded exact collection maximum and derives the exact
mutation count from the complete sealed plan. It changes no source spelling or
durable command data. Deployment requires ordinary exact contract, lock, role,
and generated-binding rotation.

## Security

Every count is derived with checked arithmetic from bounded schema and plan
data before deployment and revalidated on decode. Concrete submitted counts and
graphs are checked before proportional work or authoritative effects. Existing
byte, index, conflict, authorization, row-policy, cancellation, redaction,
audit, and uncertainty controls remain defense in depth.

The capability grants no new entity, field, partition, command, query, storage,
or transaction authority. A caller cannot request more capacity or transform
one command into multiple commits.

## Standing Design Tests

- **Interface safety:** Applications submit one generated typed value graph and
  cannot select the process ceiling, IR version, mutation count, split,
  transaction, retry subset, or fallback. The compiler and first-party runtime
  own all count, locality, authority, and atomicity proofs.
- **Scale:** Element count, mutation instances, conflicts, observations,
  dependencies, index work, events, input bytes, graph bytes, framing,
  execution deadline, diagnostics, and retained state remain independently
  finite. The successor uses existing single-node lower capacity and makes no
  distributed-transaction or billion-row claim.

## Testing

- Compiler source-span and semantic assertions at 256/257 and 1,024/1,025
  elements and at 256/257 and 4,096/4,097 complete mutation instances.
- Least-sufficient old/V23 plan-hash, bundle, round-trip, unsupported-reader,
  canonical regeneration, and byte-exact legacy fixtures.
- Runtime and storage exact/plus-one guards, one-partition refusal, independent
  conflict/index/byte/work refusal, cancellation, business failure,
  idempotency replay, and no-partial-effect assertions.
- Memory and redb process-level atomic commit/recovery coverage for one 1,000-
  element command plus one fixed root mutation.
- Generated Rust, Go, TypeScript, Python, CLI, MCP, local-driver, and remote
  parity with one value-free real adapter loopback at its legal 1,000-total
  store boundary.

## Requirements and Work Packages

- **Requirements:** `BLK-065` through `BLK-070`
- **Defines or blocks:** WP-745
- **Final evidence:** WP-745 and the value-free high-cardinality atomic-command
  receipt

## Decision Deadline

The exact boundary was accepted before implementation. Any element ceiling
above 1,024, mutation-instance ceiling above 4,096, widening of another fixed
resource, multiple collection inputs, cross-partition mutation, caller-selected
capacity, partial commit, or reinterpretation of V1 through V22 requires renewed
human review.
