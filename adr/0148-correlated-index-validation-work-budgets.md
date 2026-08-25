# ADR-0148: Correlated Index-Validation Work Budgets for Atomic Commands

- **Status:** Accepted
- **Direction approved:** 2026-08-25 (maintainer, in session)
- **Exact text accepted:** Yes, 2026-08-25 (maintainer, in session)
- **Decision deadline:** Before changing the accepted 4,096 affected-prefix or
  validation-target ceilings, compiler admission, coordinator derivation,
  storage bounds, or durable command-capsule formats
- **Requires:** ADR-0002, ADR-0003, ADR-0004, ADR-0011, ADR-0012, ADR-0013,
  ADR-0023, ADR-0031, ADR-0038, ADR-0055, ADR-0059, ADR-0107, ADR-0124,
  ADR-0129, and ADR-0147
- **Amends if accepted:** ADR-0004, ADR-0013, and ADR-0023's independent 4,096
  affected-prefix and combined validation-position ceilings; it does not
  change the 4,096 index-entry-delta, 256 mutation-instance, one-partition,
  16 MiB read-state, 15 MiB pre-commit-intent, or 16 MiB staged-write and
  durable-envelope ceilings
- **Defines or blocks:** Proposed WP-682 and WP-683, and resumption of external
  adapters whose bounded atomic commands maintain many compiler-declared
  indexes

## Context

ADR-0147 permits one atomic collection command to combine a useful element
cardinality with a large individual value bound by proving a smaller aggregate
canonical-element-byte budget. The first real consumer now compiles its
1-through-100 element input and complete command graph, but fails the older
index-validation count limits.

The generic shape is an atomic collection command that updates one indexed
state row and appends one indexed change row per element. One real schema has
eighteen compiler-declared state indexes and two change indexes. At the
100-element bound, the existing conservative estimator reports:

| Command shape | Index-entry deltas | Affected prefix epochs | Validation positions |
|---|---:|---:|---:|
| create indexed state rows | 1,800 | 12,318 | 12,418 |
| replace state rows and append change rows | 3,800 | 23,320 | 23,520 |

The 3,800 physical index-entry deltas fit the accepted 4,096 ceiling. The
larger figures arise because exact phantom protection advances the canonical
union of every affected whole-index and complete leading-component prefix.
Each affected prefix also occupies one transaction-current validation
position. These are real bounded costs, but the two 4,096 ceilings reject them
independently even when their combined work and byte charges remain within a
closed command budget.

Removing indexes would trade a compiler-planning limit for unindexed or
incorrectly paginated application reads. Splitting the command would change
atomicity, idempotency, provenance, events, changelog order, and observable
state. Raising one constant would hide the coupled costs and would encounter
the next 4,096 guard in the compiler, coordinator, storage API, or durable
capsule. A framework-specific exception would put an external authorization
model into RiffDB.

The current diagnostic also loses information already present in
`IrValidationError::LimitExceeded`. `RDB-C020` reaches the author as only a
static summary and generic help, while the exceeded resource, observed
compiler value, and maximum are discarded. Discovering 12,318 and 23,320
required inspecting the compiler under a debugger. Compiler-derived resource
counts are contract metadata, not application values or secrets, and can be
reported safely under a closed bounded vocabulary.

Finally, affected prefix epochs are durable command authority. A command with
more than 4,096 transitions cannot silently be written as the existing
`StoredCommandCapsuleV1` through `StoredCommandCapsuleV5` identities: their
structural readers freeze the old collection maximum. Reinterpreting an
existing identity would violate ADR-0124 even though Protobuf's repeated-field
wire shape could carry more entries.

## Decision

### 1. Replace the two independent ceilings with one correlated index-work proof

Every mutating command plan computes the following conservative successful-
execution maxima with checked arithmetic:

```text
D = index-entry deltas
A = distinct mutation-affected prefix epoch targets
V = binding observations
  + root-validation observations
  + influential range-validation observations
  + affected prefix epoch targets
  + exact unique-occupancy validations

W = D + A + V
```

Each term represents separately performed work. An affected prefix is charged
once in `A` for canonical target derivation and epoch advancement and once in
`V` for its transaction-current observation. That deliberate double charge is
not accidental estimator duplication.

The closed limits are:

| Resource | Maximum |
|---|---:|
| index-entry deltas `D` | 4,096 |
| affected prefix epochs `A` | 65,535 structural entries |
| validation positions `V` | 65,535 structural entries |
| correlated index work `W` | 65,535 units |
| affected targets plus current epoch observations | 16 MiB semantic bytes |

The existing command read-state, evaluated graph, pre-commit intent, staged
write set, record, envelope, key, value, mutation, event, and public request
byte limits remain independently mandatory. Passing `W` never reserves or
implies byte capacity. Passing a byte ceiling never grants more work units.
Configuration may lower but never raise these limits.

`D` remains independently capped at 4,096 because it bounds physical secondary-
index removals and additions and the existing index-entry segment authority.
The new structural maximum is not permission for a command to maintain 65,535
physical indexes. It permits a bounded number of small prefix-epoch and
validation records when a still-small physical mutation set affects many
declared leading prefixes.

For the two motivating neutral shapes, `W` is 26,536 and 50,640 respectively
before the additional safe reductions defined below. Both fit. A plan with the
same 23,320 affected prefixes but enough other reads, unique checks, or index
deltas to exceed 65,535 still fails compilation.

### 2. Make collection estimation correlation-aware without assuming runtime data

The estimator continues to reject before plan hashing and never executes an
application expression. It may reduce an independent-maximum estimate only for
correlations already proved by the checked plan:

1. a bulk command's repeated bindings share one exact structural partition, so
   the complete partition-route leading prefix is counted once per affected
   index rather than once per possible element;
2. the whole-index prefix is counted once per affected stable index identity;
3. unchanged leading components before the earliest possibly changed component
   retain their existing single-copy treatment for one replacement;
4. old and new prefixes are both charged from the earliest possibly changed
   component onward; and
5. no equality among caller values, entity keys, non-partition index fields, or
   separate collection elements is assumed.

For a collection without ADR-0147 `aggregate_bytes`, affected-target and epoch-
state bytes retain the current independent field-maximum proof. For an
aggregate-byte collection, the compiler may use the same accepted affine-proof
discipline to derive:

```text
fixed_index_state_bytes
  + aggregate_bytes * maximum_index_prefix_copy_coefficient
  <= 16 MiB
```

The coefficient is compiler-owned and counts every place aggregate-variable
element bytes can appear in affected prefix targets and current epoch
observations. Fixed framing, service-owned values, key components not sourced
from the budgeted element bytes, and independently bounded command inputs are
charged separately. The proof may be conservative and must fall back to the
independent-maximum calculation when provenance through an expression is not
exact. It never assumes compression, interning, equal values, runtime
deduplication, optional absence, or backend representation size.

Aggregate bytes can improve only the byte proof. They cannot reduce `D`, `A`,
`V`, or `W`, because a small value may still create one unit of each declared
operation.

These calculations add no caller syntax, request-time budget, backend knob, or
framework profile. The plan owns every field, index, bound, coefficient, and
result. Commands without collection expansion retain their ordinary estimator.

### 3. Derive and validate the concrete target set once per command attempt

After transaction-current entity validation, the coordinator derives the
bounded candidate index mutations and affected prefix targets once. It charges
candidate count and semantic bytes while deriving, canonically sorts once,
deduplicates once, and seals the result. Validation observations, epoch reads,
epoch advances, staged-write reservation, and durable capsule construction
must borrow that sealed canonical set; none may re-expand the mutation plan or
re-prove schema compatibility per target.

Plan/schema validation is paid once per resolved plan. Static budget validation
is paid once per plan construction or decode. Concrete target derivation is
paid once per command attempt after the transaction-current old values are
known. Storage boundary validation and durable codec validation remain
independent defense in depth, but operate linearly over the already bounded
canonical collections. No check is moved into a per-row query path, page fetch,
retry loop detached from a new attempt, or backend scan.

Runtime builders enforce `D`, `A`, `V`, `W`, and semantic bytes incrementally
before an allocation can exceed the declared maximum. They do not preallocate
65,535 maximum-sized keys. A checked plan reaching a runtime guard is an
internal integrity defect; it is never converted into partial success,
truncation, a smaller advertised collection limit, or a durable application
`ResourceLimit` outcome.

The transaction still reads every concrete affected epoch and stages every
concrete advance atomically with entity state, index entries, outcome, events,
provenance, changelog, and commit authority. This ADR adds no coarse
invalidation fallback, whole-index substitution, probabilistic summary,
deferred epoch repair, or post-commit projection. Those are different
concurrency semantics and require a separate accepted decision if ever needed.

### 4. Add least-sufficient durable command-capsule and segment successors

WP-683 adds a new durable command-capsule identity whose index-generation-
transition collection permits at most 65,535 entries and remains bounded by the
existing complete semantic and 16 MiB envelope charges. The current topology
suggests `StoredCommandCapsuleV6`, `StoredCommandSegmentBodyV5`, and
`StoredCommandSegmentV5`; WP-683 must audit the durable schema registry,
manifest, tag allocation, and version topology immediately before assigning
those names or numbers. An intervening identity causes administrative
renumbering, never reuse.

The successor capsule carries the same canonical command authority as the
current capsule: base command, durable event variants, ordered index-generation
transitions, canonical service values, and entity transitions. It changes only
the closed structural maximum for the ordered generation-transition
collection. Existing field numbers and existing messages are not edited or
reinterpreted.

The writer uses the least-sufficient durable identity. A command with at most
4,096 transitions continues to use the currently selected existing capsule and
segment identity. A command with 4,097 through 65,535 transitions uses the
successor capsule and therefore a successor segment body/wrapper. A segment
containing such a command is encoded wholly under the successor segment
identity; it does not smuggle a successor member into an older body.

All existing command capsules, segments, envelopes, hashes, checkpoints,
backups, and fixtures remain byte-exact and readable. New readers validate the
old 4,096 maximum under every old identity and the new 65,535 maximum only
under the successor. Old binaries refuse the unknown successor identity before
mutation. No startup rewrite is required because historical segments are
immutable and mixed historical identities are already an explicit read path.
The release durable-format manifest and topology record the new identity and
downgrade posture before it may become writable.

### 5. Preserve exact prefix-epoch and transaction semantics

The canonical affected set remains the exact union of the whole-index and every
complete leading-component prefix bucket for changed old and new index entries,
with identical targets deduplicated and each concrete bucket advanced exactly
once per command. Influential range dependencies remain separate. Unique
occupancy checks, covered-value changes, first epoch creation, staged commands,
same-transaction visibility, `u64::MAX` exhaustion, rollback, and sequence
assignment retain their accepted meanings.

The commit coordinator remains the only sequence owner and the only component
that applies authoritative mutations. All expanded epoch reads and writes occur
inside the same short authoritative transaction, before capacity reservation
and sequence assignment where currently required. Memory and redb must agree on
canonical order, duplicate collapse, exact-bound and plus-one behavior,
rollback, crash/reopen, and later-command observation in one staged batch.

The implementation must publish index-planning and transaction-stage benchmark
evidence for representative 1, 9, 19, and 100 element commands. The evidence is
not an alternate correctness gate, but it must identify prefix derivation,
canonicalization, epoch read, capacity reservation, epoch write, capsule
encoding, and commit costs separately. A hidden quadratic path, per-target plan
decode, or repeated canonical encoding blocks completion even if semantic tests
pass.

### 6. Make `RDB-C020` bound evidence precise and safe

Every `RDB-C020` caused by a known ceiling carries one bounded structured
observation:

```text
resource: closed compiler resource identity
actual: checked nonnegative integer
maximum: checked nonnegative integer
```

The public rendered summary names all three, for example:

```text
RDB-C020: command affected-prefix epochs is 23,320; maximum is 4,096
```

The resource comes from a closed compiler registry, not an arbitrary internal
string. At minimum it distinguishes key bytes, value bytes, declaration count,
index-entry deltas, affected-prefix epochs, validation positions, correlated
index work, affected-epoch semantic bytes, command-graph bytes, and aggregate
collection bytes. Unknown internal limit kinds map to a fixed redacted
`compiled_artifact` identity rather than exposing source paths or debug text.

The observation may use the existing bounded summary/help wire strings; this
ADR does not require a public Protobuf field. If an implementation adds typed
fields, that public schema change requires its own normal additive protocol
review and generated-fixture update. Source-spanned compiler diagnostics,
CLI/MCP/gRPC rendering, LSP output, and checked fixtures must preserve the same
resource, actual, and maximum. Arithmetic overflow reports the closed resource
and an explicit checked-overflow condition; it must not invent an `actual`
value.

These values are safe because they describe compiler-owned schema and plan
bounds. Diagnostics never include input values, key bytes, prefix bytes,
entity data, condition contexts, secrets, row counts, storage contents, or
backend timing. Strings and integers remain bounded before rendering.

### 7. Prove the capability with a framework-neutral index-rich corpus

Acceptance uses a neutral `IndexedMutation` contract, not an external framework
schema. It contains one partition-scoped indexed state entity, one indexed
change entity, a finite family of compiler-declared covering indexes, and one
1-through-100 atomic collection command with an ADR-0147 aggregate byte bound.

The corpus freezes plans at 4,096 and 4,097 affected prefixes; 65,535 and 65,536
work units; exact and plus-one affected-state bytes; index deltas at 4,096 and
4,097; and the representative 1, 9, 19, and 100 element commands. It proves
compiler diagnostics, plan and root hashes, memory/redb parity, deterministic
schedules, cancellation, overlapping commands, idempotent replay, unique and
business failures, complete-or-absent visibility, provenance, events,
changelog order, capsule least-sufficient selection, old-reader refusal, crash
recovery, and immutable publication loopback.

Static architecture checks reject external framework names, schemas, routes,
error mappings, handwritten transactions, raw storage access, unindexed query
fallbacks, split commands, partial results, coarse invalidation, and caller-
selected resource budgets. External protocol conformance remains solely in the
adapter repository.

## Options Considered

1. **Raise affected prefixes and validation targets independently:** rejected
   because it hides their coupled work, leaves adjacent runtime/durable guards
   inconsistent, and admits shapes whose total index work was never bounded.
2. **Raise every command collection to 65,535:** rejected because physical
   index deltas, mutations, events, and other collections retain different
   costs and accepted ceilings.
3. **Remove covering indexes:** rejected because it changes declared query
   capability and can make correct bounded pagination or replacement workloads
   unavailable.
4. **Split the atomic collection:** rejected because it changes atomicity,
   idempotency, ordering, provenance, events, and uncertainty recovery.
5. **Advance only a whole-index or partition epoch:** rejected for this ADR
   because it changes conflict ownership and range-validation semantics and can
   cause unrelated work to invalidate. A future coarse-invalidation design
   would need its own explicit availability and concurrency review.
6. **Write more transitions under an existing capsule identity:** rejected as
   an ADR-0124 compatibility violation; old readers freeze 4,096 under those
   identities.
7. **Report only the diagnostic code:** rejected because the compiler already
   knows safe actionable bound evidence and hiding it makes ordinary contract
   authoring require debugger access.
8. **Special-case an external authorization adapter:** rejected because the
   capability applies to any bounded index-rich atomic command and RiffDB owns
   no userland identity or authorization framework.

## Consequences

- Index-rich atomic commands can retain useful read indexes without weakening
  atomic writes or pretending prefix epochs are free.
- The maximum admitted CPU and transient collection work rises from an
  independent 4,096 target ceiling to a correlated 65,535-unit command budget;
  the existing byte and physical index-delta ceilings continue to constrain it.
- Some commands may atomically read and write tens of thousands of small epoch
  records. That cost is explicit, benchmarked, and bounded rather than hidden
  behind a constant increase.
- Durable history gains a least-sufficient successor identity; old history
  remains exact and no old decoder is retired.
- `RDB-C020` becomes actionable without exposing application data.
- No public application surface gains raw predicates, indexes, transactions,
  budget controls, partial writes, or framework-specific operations.

## Compatibility

Contract source syntax and generated application method signatures do not
change. Plan hashes may change only if WP-682 must carry a new compiler-derived
coefficient or budget identity; the topology audit must prefer constructor-
validated derived metadata when no runtime consumer needs serialized plan
state. If executable IR changes, it receives a least-sufficient successor and
all earlier artifacts remain byte-exact.

The durable capsule and segment successors are additive new identities. Old
identities keep their old 4,096 transition maximum. A database containing the
successor is not downgradable to a binary that does not list it as readable;
that binary must refuse before mutation. Backup, export, startup validation,
segment checkpoints, and recovery must all recognize the same declared reader
window.

## Security

All work and bytes remain statically and dynamically bounded. Checked
arithmetic precedes allocation. Larger prefix collections carry only canonical
index identities, partition keys, and prefix bytes already authorized by the
compiled command; they grant no new read or write authority. Diagnostics expose
only schema-derived resource counts and maxima and remain value-free. Atomicity,
authorization, redaction, idempotency, provenance, durability, and uncertainty
recovery are unchanged.

## Standing Design Tests

- **Interface safety:** application callers invoke one generated compiled
  command with typed values. They cannot select indexes, prefixes, validation
  mode, work budget, byte coefficient, durable identity, transaction, split,
  fallback, or partial result. Every accepted command follows the same service,
  authorization, deterministic runtime, and commit coordinator.
- **Scale:** physical index deltas remain at most 4,096; affected prefixes and
  validation positions are each structurally at most 65,535; their exact
  correlated work is at most 65,535; target/current state is at most 16 MiB;
  and all other command, transaction, record, and envelope ceilings remain.
  Plan proof is paid once, concrete derivation once per attempt, and no work is
  proportional to database history or matching query population.

## Testing

- Independent estimator tests for create, replace, delete, covered-only change,
  shared whole prefixes, shared partition prefixes, changed components,
  collection expansion, unique occupancy, and checked overflow.
- Source-span snapshots and semantic assertions for every precise `RDB-C020`
  index resource, exact boundary, and plus one.
- Property tests comparing the compiler maximum to concrete runtime derivation
  for reduced schemas and collection sizes.
- Memory/redb semantic conformance at all count and byte boundaries.
- Deterministic schedule and process crash/reopen tests around epoch reads,
  staged advances, reservation, sequence assignment, capsule sealing, and
  commit.
- Golden old and successor capsule/segment bytes, hashes, decoder matrices,
  old-reader refusal, manifest/topology checks, backup, and recovery fixtures.
- Neutral remote 100-element loopback plus cost-stage benchmark evidence and
  architecture negatives.

## Requirements and Work Packages

- **Carries forward:** BLK-001 through BLK-021, TXN-001, TXN-002, TXN-030,
  TXN-031, TXN-040 through TXN-044, STO-001, STO-002, ID-001, ID-004,
  SAFE-001, SAFE-004, SAFE-005, PERF-004, and PERF-005.
- **Future requirement registration if accepted:** BLK-022 through BLK-027
  will cover correlated index-work admission, correlation-safe byte proof,
  pay-once concrete derivation, durable successor compatibility, precise bound
  diagnostics, and neutral 100-element atomic conformance.
- **Proposed WP-682:** compiler estimator, correlated limits, precise
  diagnostics, SPEC/ADR reconciliation, fixtures, and documentation.
- **Proposed WP-683:** coordinator/storage bounds, durable capsule/segment
  successors, backend/crash/performance conformance, immutable development
  publication, and neutral remote acceptance.

Exact maintainer acceptance of this ADR is required before either work package
may change an accepted semantic, transaction, or durable boundary.

Direction, package boundaries, and this exact text were accepted on
2026-08-25.
