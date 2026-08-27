# ADR-0153: Compiler-Sealed Initialized State Transitions

- **Status:** Accepted
- **Direction approved:** 2026-08-26
- **Exact text accepted:** Yes, 2026-08-26
- **Accepted:** 2026-08-26
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for upstream commit `0d6c429e`
- **Decision deadline:** Before WP-697 adds a binding mode, initializer plan,
  grammar construct, or executable-command identity
- **Requires:** ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0012,
  ADR-0023, ADR-0055, ADR-0107, ADR-0124, ADR-0129, ADR-0147, and ADR-0149
- **Amends if accepted:** `BLK-009`'s prohibition on blind overwrite and
  generic upsert by admitting only the sealed initialized transition below
- **Defines or blocks:** WP-697 through WP-699

This record is authoritative for WP-697 through WP-699.

## Context

RiffDB commands currently bind one exact entity target in one of four modes:
`read` and `mutate` require a present row, `create` requires an absent row, and
`delete` requires a present row. A binding mismatch returns its declared
whole-command business outcome. This makes every mutation target and
transaction-current dependency statically visible, but it forces an
application that needs the same logical successor from either initial state to
coordinate several generated operations.

A real tuple-oriented storage consumer exposes the cost. Creating one new
logical row currently requires an existence query, a command that creates a
dormant row, another existence query, and a command that validates and replaces
that row while creating its changelog record. Together with an independently
ensured aggregate root and conservative rereads in the adapter, a one-row
logical write reached seven RiffDB operations. In a matched conformance run,
2,500 sequential one-row writes spent about 80 seconds in setup before reads.

The temporary dormant row is not application-visible because indexed reads
require its inactive state, but it is still authoritative work. It creates one
entity version and every secondary-index entry, then the following command
replaces the entity and rewrites each index whose state component changes. For
an entity with eighteen maintained access paths, one new logical row can cause
eighteen index insertions followed by thirty-six index delete/insert deltas,
before its changelog indexes. This is avoidable write amplification created by
a command-language capability gap.

The database already possesses the safety mechanisms needed for one atomic
choice: exact-key absence observations, transaction-current revalidation,
create and replace mutations, closed outcomes, bounded collection expansion,
correlated index-work accounting, idempotent admission, atomic events and
outcomes, and replay. What is missing is one compiler-declared binding that
turns either an absent target plus declared initial values or a present target
plus its exact preimage into one mutable working record.

This does not justify arbitrary branching, a SQL-style upsert, a caller-chosen
conflict target, last-write-wins, or a general transaction callback. The real
consumer needs one common checked instruction suffix after the row has been
initialized or loaded. Keeping that restriction avoids designing a control-flow
language from imagined consumers while still removing the redundant durable
transition.

Generated query page batching, iterator buffering, process-local caching of an
immutable aggregate root, and elimination of repeated adapter reads are
consumer work. Existing RiffQL already supports compiler-bounded `take $limit`.
This ADR adds no query, pagination, join, index-selection, or framework-specific
surface.

## Proposed Decision

### 1. Add one initialized mutable binding, not general branching

Contract grammar gains this source shape:

```riff
init_or_mutate Entity(key_expression, ...) as entity
  initialize {
    field_name: expression,
    ...
  }
```

`init_or_mutate` resolves one compiler-declared entity and complete primary key.
It has exactly two runtime states:

- when the transaction snapshot observes the target absent, runtime constructs
  a provisional record from the primary key and the declared initializer and
  the eventual mutation is one `Create`; or
- when the snapshot observes the target present, runtime ignores the
  initializer as a value source, materializes the exact current preimage, and
  the eventual mutation is one revision-checked `Replace`.

Both states continue through the same existing ordered requirements, set/embed/
workflow effects where valid, event constructions, invariants, and return
clause. Source cannot attach an absent arm, present arm, state-origin predicate,
conditional effect block, dynamic outcome, callback, retry rule, or alternate
target. The initializer does not create a value visible as `created`,
`present`, or another discriminator. Applications that need different
duplicate and success outcomes continue to use `create`; applications that
need missing outcomes continue to use `mutate`.

The name is deliberately not `upsert`. A SQL-style or storage-style upsert
usually selects conflict targets, overwrites without a declared current-state
model, or has backend-dependent affected-row behavior. None of those semantics
are admitted. `BLK-009` continues to prohibit blind overwrite and generic
upsert; its only amendment is this exact compiler-sealed binding.

### 2. Freeze initializer dataflow and definite assignment

The initializer is an ordered, unique-field construction over non-key entity
fields. Each initializer expression may use only constants, command inputs,
service-owned values, deterministic transaction context, and the current
bounded collection element. It cannot read the binding being initialized,
another entity binding, a query result, a projection, storage, a clock,
randomness, or a target selected at runtime.

The compiler type-checks and lowers every initializer expression once. It
rejects duplicate fields, key fields, unknown fields, type mismatch, secret-
flow violations, non-total expression work, and any dependency outside the
closed list above. The deterministic runtime evaluates the bounded initializer
once per binding instance before choosing its values for an absent target.
Because those expressions are state-independent, arithmetic and resource faults
do not become a row-existence oracle.

An initializer may be partial. Existing create-style definite-assignment
analysis is extended path-sensitively:

- on the absent path, a field is readable by a requirement or right-hand
  expression only after the key or initializer has assigned it;
- on the present path, every declared field is available from the preimage;
- a common instruction may assign additional fields on both paths; and
- every possible successful create postimage must contain exactly every
  required field before an event, invariant, outcome, or mutation reads the
  complete record.

The compiler rejects a plan whose common suffix is valid for only one initial
state. It does not insert a default, `NoValue`, zero, empty string, or stale
caller copy for an uninitialized field.

### 3. Preserve one snapshot dependency and one authoritative mutation

Snapshot materialization records exactly one `EntityObservation` for each
initialized mutable binding. Absence and presence use the existing canonical
target identity. A present observation carries its exact entity version and
record hash; an absent observation carries the exact absence dependency.

Evaluation produces at most one entity mutation for the binding. It never
creates a dormant row and replaces it within one intent, never emits both a
create and replace for one target, and never performs an internal command or
storage round trip. The mutation kind is derived solely from the revalidated
observation:

- `Absent -> Create(postimage)`; or
- `Present(version, preimage) -> Replace(version, postimage)`.

The coordinator revalidates the complete observation under the same acquired
target capability before accepting the evaluated graph. If another command
changes absence to presence, presence to absence, version, record hash,
relationship evidence, uniqueness evidence, or an influential index epoch,
the candidate is discarded and the whole command is reevaluated under the
existing bounded retry rules. Evaluation never changes an absent candidate into
a replace or a present candidate into a create after validation.

Requirements, invariants, row policy, relationships, uniqueness, secret flow,
events, provenance, outcome construction, and changelog emission operate on the
selected working record exactly as they do for existing create or mutate
bindings. A business rejection persists zero authoritative mutation. Idempotent
replay returns the exact persisted outcome without resolving the target again.

### 4. Authorize the complete possible operation before execution

An initialized mutable binding statically requires the union of create and
update entity/field authority. Command discovery, role derivation, capability
hashes, generated metadata, and deployment validation include both possible
operation classes. A role that grants only create or only update cannot invoke
the command and cannot learn which initial state would have been selected.

After snapshot selection, row policy evaluates the exact accepted operation:
create policy over the absent-path successor or update policy over the present
preimage and successor. Both policy families and every field they may inspect
remain compiler-declared. Denial releases no outcome, predecessor value,
existence bit, index statistic, or partial mutation.

Secret initializer sources and successor fields retain existing sticky
classification. Returning or emitting a secret still requires the exact
compiler-declared reveal and role authority; initialization does not imply
read, output, or disclosure authority.

### 5. Extend bounded collection commands without multiplying work

`init_or_mutate` is valid in an ordinary command or as an element-local binding
inside the existing one-list `bulk command`. Collection cardinality,
aggregate-element bytes, copy coefficient, one-partition routing, duplicate
element policy, distinct mutation targets, complete graph bytes, and every
existing 256-element structural maximum remain unchanged.

For each element, the compiler proves the exact primary key and the common
successor graph from the scalar input and current element. Repeated target keys
remain rejected before evaluation; a list cannot use multiple elements to
simulate sequential updates to one row. Up-front and element-local bindings
cannot alias the same target.

Static cost is the conservative maximum of the absent-create and present-
replace alternatives for one binding instance, not their sum. Runtime charges
only the selected mutation's physical work. Collection correlation may
multiply that maximum only through the already proved element count and
aggregate-byte constraints. Index-entry deltas remain capped at 4,096;
affected-prefix, validation-position, and correlated index-work ceilings remain
those accepted by ADR-0149. This decision does not raise a ceiling or exempt an
index-rich row.

Initializer validation, expression lowering, authority union, relationship and
uniqueness shape, index-delta alternatives, and byte-copy coefficients are paid
once per compiled plan. Input normalization is paid once per invocation and
initializer evaluation once per binding instance. None may repeat per field,
index, validation target, event, page, storage operation, retry substep, or
generated-language layer.

### 6. Use least-sufficient version successors

The new source construct and executable binding semantics require
`GRAMMAR_VERSION_V17`, `EXECUTABLE_IR_VERSION_V17`, and
`BUNDLE_FORMAT_VERSION_V17`. V17 adds one binding-mode tag and one canonical
initializer field-expression list in ascending `FieldId` order. It does not
change entity records, mutations, commit intents, command capsules, admission,
outcomes, events, indexes, changelog, provenance, public Protobuf, driver
protocol, or generated method signatures.

The V17 writer emits V17 only when a contract contains `init_or_mutate`.
Contracts using only older constructs retain their least-sufficient historical
writer identity and byte-exact plan/module/bundle hashes. V1 through V16 readers
and fixtures remain supported for their registered windows. An old reader or
runtime rejects V17 before activation with the existing typed refresh/upgrade
guidance; no older tag or reserved byte is reinterpreted.

The version-topology manifest must record sources, reader/writer windows,
activation compatibility, fixtures, upgrade guidance, and retirement posture
before V17 can become executable. No durable decoder may be retired by this
work.

### 7. Keep adjacent optimization work in its proper owner

RiffDB acceptance will include a framework-neutral lifecycle corpus and one
value-free external capability receipt proving that a consumer can replace an
absent bootstrap command plus a present-state mutation command with one atomic
generated command. External schemas, adapter code, route semantics, test-suite
names, and generated consumer profiles remain outside this repository.

The consumer remains responsible for:

- choosing finite named command variants for its business rules;
- batching ordinary named-query pages with its requested bounded `$limit`;
- buffering generated iterator pages without changing cursor semantics;
- removing redundant reads when a command outcome or known initializer already
  proves the state; and
- caching an immutable root only where its own lifecycle proves that safe.

RiffDB remains responsible for its per-operation service floor, pay-once
validation, index maintenance efficiency, and honest benchmark evidence. This
ADR neither claims PostgreSQL parity nor changes ADR-0142's performance gates.

## Options Considered

1. **Keep composing create and mutate commands:** rejected because it requires
   extra round trips and commits an avoidable intermediate authoritative row and
   index state.
2. **Add a general upsert:** rejected because conflict targets, overwrite
   posture, affected-row semantics, and current-state validation would become
   application- or backend-selectable.
3. **Add arbitrary command branches or match expressions:** rejected because
   the current real consumer needs one common suffix, while general control
   flow would multiply dataflow, authority, cost, outcome, and compatibility
   semantics without a second real shape.
4. **Hide an ensure/create inside the adapter or service:** rejected because
   its dependency and mutation graph would be absent from the compiled plan and
   other language bindings would not share the semantics.
5. **Add one initialized mutable binding:** proposed because both possible row
   states, all initial values, one target, one successor program, authority, and
   worst-case cost remain compiler-visible.

## Consequences

- A logical row can be initialized or updated in one idempotent atomic command
  without a dormant-row commit.
- Bulk commands can apply the same transition to up to their existing bound
  while retaining one whole-command outcome and all-or-nothing durability.
- The common-suffix restriction is less expressive than branching; consumers
  with state-specific effects must continue using finite named commands or
  return with a real second shape for review.
- V17 adds a release-significant command-plan identity and therefore incurs the
  accepted decoder, fixture, deployment, and retirement obligations now rather
  than after alpha.

## Compatibility

This Proposed ADR changes no current bytes or behavior. After acceptance,
contracts without `init_or_mutate` remain byte-identical and use their existing
least-sufficient grammar, executable-IR, and bundle versions. Contracts using
the construct compile only to V17 and rotate exact contract, command-plan,
bundle, application-lock, role, and generated-artifact identities through the
existing review workflow.

The generated operation's input and closed outcome schemas come solely from
the command declaration and need no conditional-state field. Rust, Go,
TypeScript, Python, CLI, MCP, gRPC, and driver transport therefore require no
new caller-programmable option. They must still reproduce the V17 operation
identity and authorization metadata exactly.

The selected `EntityMutation::Create` or `EntityMutation::Replace`, committed
entity record, indexes, events, outcome, provenance, admission, replay, and
recovery use existing durable encodings. If implementation proves that any of
those formats must change, WP-697 or WP-698 stops for separate ADR-0124
classification and exact human review.

## Security

The caller invokes one generated named command and supplies only its typed
values. It cannot select create versus update, observe the selected path,
provide an initializer program, choose a key/index/conflict target, change a
budget, bypass row policy, request last-write-wins, or opt out of idempotency,
atomicity, durability, provenance, and current-state validation.

Create and update authority are both required before storage access. Exact
selected row policy is enforced before a contribution reaches a mutation,
event, outcome, index, or work-class decision. Diagnostics may contain only a
safe source symbol and bounded actual/maximum numbers; they contain no key,
field value, preimage, initializer value, existence state, policy exclusion, or
index content.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications can declare only
  one exact-key initialized mutable binding and one common compiler-checked
  suffix. There is no public transaction, upsert option, state-origin flag,
  callback, dynamic branch, target selector, overwrite mode, or guarantee
  opt-out.
- **Scale:** each binding performs one exact-key observation and at most one
  entity mutation. Bulk multiplication uses the existing finite list, byte,
  target, index, validation, graph, and partition bounds. No scan,
  population-sized state, full-database rewrite, or per-row descriptor proof is
  introduced.

## Testing

- Parser/formatter/source-span snapshots for valid initialization and every
  forbidden field, dependency, alias, missing assignment, unsupported effect,
  and old-grammar case.
- V17 IR/bundle/plan/hash/lock golden fixtures plus V1-V16 byte-exact writer and
  reader-window regression.
- Pure-runtime differential tests for absent create and present replace through
  the same requirements, assignments, invariants, events, and outcome.
- Deterministic concurrent absent/absent, absent/present, update/update,
  delete/recreate, and retry-exhaustion schedules proving whole-command
  reevaluation and no stale mutation or outcome.
- Memory/redb crash arms around pending admission, snapshot, evaluation,
  staging, durability fence, publication, response loss, and replay.
- Bulk one, nine, nineteen, and one hundred element tests over all-absent,
  all-present, and mixed observations, including duplicates, maximum bytes,
  index-work limits, business rejection, cancellation, and complete-or-absent
  visibility.
- Authorization union, row-policy create/update selection, field authority,
  revocation, secret flow/reveal, redaction, and existence-nondisclosure tests.
- Generated Rust/Go/TypeScript/Python, CLI, MCP, local driver, and remote gRPC
  conformance with a generic lifecycle example and an external value-free
  capability/performance receipt.
- Architecture checks proving one binding implementation, one mutation per
  target, no internal service recursion, no dormant-row rewrite, no generic
  upsert/branching, no framework code, and no per-index repeated initializer or
  compatibility proof.

## Requirements and Work Packages

- **Future requirements after exact acceptance:** `BLK-036` through `BLK-045`
- **Language, compiler, and V17 compatibility:** WP-697
- **Runtime, coordinator, storage, and concurrency:** WP-698
- **Generated surfaces, recovery, performance, and external acceptance:**
  WP-699

## Decision Deadline

Exact human acceptance is required before WP-697 adds the source construct,
binding mode, initializer plan, or V17 identity. Any general conditional block,
state-origin value, caller-selected conflict target, last-write-wins behavior,
mutation/commit encoding change, cross-partition transition, or weakening of
create/update authority requires a separately accepted decision.
