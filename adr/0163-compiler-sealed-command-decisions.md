# ADR-0163: Compiler-Sealed Command Decisions with Exact No-Effect Arms

- **Status:** Accepted
- **Direction approved:** 2026-08-27
- **Exact text accepted:** Yes, 2026-08-27
- **Accepted:** 2026-08-27
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for upstream commit `60050745`
- **Decision deadline:** Before WP-716 adds conditional command syntax, a
  deferred initialized binding, branch-local effects, or a V18 executable
  identity
- **Requires:** ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0012,
  ADR-0023, ADR-0055, ADR-0107, ADR-0124, ADR-0129, ADR-0147, ADR-0149,
  and ADR-0153
- **Amends if accepted:** ADR-0153's common-suffix restriction only through
  the closed decision form below; its prohibition on general branching,
  state-origin exposure, generic upsert, callbacks, and caller-selected
  conflict behavior remains in force
- **Defines or blocks:** WP-716 through WP-718

This record is authoritative for WP-716 through WP-718.

## Context

ADR-0153 admitted one exact-key `init_or_mutate` binding because its first real
consumer needed the same checked successor program whether a row was absent or
present. It deliberately rejected conditional effect blocks until a real
second shape established their semantics.

That second shape now exists. A bounded tuple-oriented storage consumer accepts
a finite atomic mutation list whose policy is sealed by the named operation.
For each element, current state and the submitted transition can require one of
three results:

1. apply the state transition and create its corresponding changelog row;
2. accept the element with no entity, index, event, outbox, or changelog effect;
   or
3. reject the complete command with a typed duplicate, missing, or conflicting-
   condition outcome and zero application mutations.

Always sending an accepted no-effect element through `init_or_mutate` is wrong:
it replaces or creates the state row, increments its revision, rewrites indexes,
and can create a false changelog record. Reading first and omitting that element
is also wrong: another command can change the row between the read and the
atomic write. Splitting validation from mutation cannot preserve the external
interface's all-or-nothing list semantics.

The required capability is not an arbitrary transaction callback. Every target,
predicate, branch, effect, outcome, authority requirement, byte bound, and
worst-case cost is known when the named command compiles. Runtime selects one
of those finite arms from transaction-current values and commits the selected
graph through the existing coordinator.

## Proposed Decision

### 1. Add one closed decision form over a deferred initialized binding

Contract grammar gains this source shape, shown with representative names:

```riff
observe_or_initialize TupleState(store_id, tuple_key) as state
  initialize {
    active: false,
    revision: 0
  }

decide state {
  when state.active == mutation.expected_active => apply {
    set state.active = mutation.active
    set state.revision = state.revision + 1
    create TupleChange(store_id, mutation.change_id) as change
      else TupleChangeAlreadyExists {}
    set change.tuple_key = mutation.tuple_key
  }

  when state.active == mutation.requested_active => no_effect

  else => reject TupleStateConflict {
    tuple_key: mutation.tuple_key
  }
}
```

`observe_or_initialize` resolves one compiler-declared entity and complete
primary key. An absent target yields the same bounded provisional working record
and definite-assignment proof as ADR-0153 initialization; a present target yields
its exact immutable preimage. Unlike `init_or_mutate`, the binding is not itself
an application mutation. It must be consumed exactly once by the immediately
following `decide` block, and no expression can observe whether its working
record came from absence or presence.

A decision contains one through eight ordered `when` arms and exactly one
`else` arm. Every arm is exactly one of:

- `apply`, which finalizes the deferred binding as one create or revision-
  checked replace and may execute a finite compiler-declared branch-local
  binding/effect suffix;
- `no_effect`, which contributes no application entity mutation, index delta,
  event, outbox intent, workflow transition, or changelog row for that decision;
  or
- `reject Outcome { ... }`, which produces one existing compiler-declared
  whole-command business outcome and discards every provisional application
  effect from every element.

The first true `when` arm is selected; otherwise `else` is selected. Predicates
and outcome/effect expressions may use only constants, command inputs, service-
owned deterministic values, transaction context, the current bounded collection
element, and compiler-declared fields of bindings already in scope. They cannot
invoke a query, projection, filesystem, network, clock, randomness source,
callback, target selector, dynamic field, dynamic outcome, or another command.

Decisions cannot nest, jump, recur, return partial collection results, introduce
another list traversal, or flow into a second decision over the same binding.
This is a sealed state-transition choice, not a general `if`, `match`, stored
procedure, transaction closure, or user-programmable control-flow language.

### 2. Seal every alternative and its dataflow at compilation

The compiler resolves every arm, branch-local binding, target key, expression,
requirement, invariant, event, outcome, reveal, and effect before deployment.
Branch-local targets must remain complete, input/current-element-computable,
same-partition targets under the existing command rules. No branch may derive a
target from a record value learned only after selecting that branch.

Definite-assignment analysis runs separately for each arm. An `apply` arm must
produce one complete valid postimage for the deferred binding and every
branch-local create. A `no_effect` arm cannot read an uninitialized absent-path
field merely to construct a success value; the common success outcome may use
only values proven assigned on every successful arm. A `reject` arm may return
only fields authorized and definitely assigned on that arm. Secret flow and
explicit reveal remain unchanged.

All `when` expressions are total, deterministic, bounded Boolean expressions.
The compiler rejects an arm made unreachable by a preceding compile-time-
equivalent predicate, duplicate outcomes where the language requires unique
outcome construction, unsupported effects, aliasing targets, incomplete
postimages, and any source whose maximum branch graph cannot be proved. It does
not attempt a general theorem proof that runtime predicates are mutually
exclusive; ordered first-match semantics and the mandatory `else` make the
decision total without nondeterminism.

The selected arm is not exposed as a Boolean, ordinal, state-origin value,
diagnostic field, work-class label, generated option, or runtime selector.
Applications observe only the command's declared business outcome and ordinary
authorized state effects.

### 3. Revalidate the decision observation before any selected graph commits

Snapshot materialization records one exact absence or present-version/hash
observation for the deferred binding. Evaluation selects an arm from that
working record and constructs only that arm's candidate graph. The coordinator
acquires the compiler-enumerated target capabilities in canonical order and
revalidates the complete influential observation set before commit.

If concurrent work changes absence, version, record hash, relationship or
uniqueness evidence, capability/policy revision, or another selected-arm
dependency, the entire candidate graph is discarded and the complete command
is reevaluated under the existing bounded retry rules. The implementation may
not preserve a previous arm choice across reevaluation.

This rule applies equally to `no_effect`. An ignored element remains a
transaction-current decision dependency even though it contributes no
application mutation. In a mixed bulk command, stale ignored evidence cannot
commit beside other elements' effects. When every element selects `no_effect`,
RiffDB still persists the normal idempotent command outcome, provenance, audit,
and nonzero commit sequence required by `LOG-001`; it persists no application
entity, index, event, outbox, workflow, or changelog effect.

Selecting `reject` anywhere rejects the whole command. No earlier or later
element effect survives, and no partial success list is released. Idempotent
replay returns the exact persisted success or rejection outcome without
rereading state or reselecting arms. Existing uncertainty recovery proves the
one original atomic result.

### 4. Authorize the union and enforce the selected operation exactly

Command discovery, role derivation, capability hashes, application locks, and
deployment validation include the union of every operation, entity, field,
relationship, event, outcome, and reveal authority reachable from every arm.
That union is checked before application state is inspected. A principal cannot
learn which arm would have run by holding authority for only that arm.

After snapshot selection, row and field policy execute for the exact observed
state and selected operation. An absent-path decision first satisfies create
policy over the complete provisional record; an absent-path apply then uses
that record's complete successor. A present-path decision satisfies update
policy over the exact preimage and the selected successor; for `no_effect` or
`reject`, that policy successor is byte-equal to the unchanged preimage.
Branch-local creates use their ordinary create policy. Thus selecting
`no_effect` or `reject` cannot bypass policy merely because no application
mutation is emitted. A no-effect arm contributes no policy-filtered record,
statistic, event, or output. A rejection exposes only its declared authorized
outcome.

Policy denial, revocation, or release-time authorization drift returns the
existing safe typed failure and releases no branch identity, presence bit,
preimage, hidden value, partial effect, or data-dependent diagnostic.

### 5. Bound branch work conservatively and pay proofs once

The existing ordinary and one-list bulk ceilings remain unchanged: element
count, individual and aggregate bytes, copy coefficient, targets, graph bytes,
mutations, index-entry deltas, affected prefixes, validation positions,
correlated index work, partition routing, retries, outcomes, events, and
diagnostics all remain finite.

Static application-effect cost for one decision is the maximum of its arms,
not the sum, because exactly one arm executes. Target-capability acquisition,
snapshot evidence, authority union, and any work required independent of arm
selection are charged as their real bounded union. Runtime charges the selected
arm and must not reserve, construct, encode, index, journal, or copy an
unselected arm's application effects.

Grammar validation, expression lowering, authority union, branch target
enumeration, compatibility validation, relationship and uniqueness shape,
index alternatives, and byte coefficients are paid once per compiled plan.
Request normalization and arm predicates are paid once per decision instance.
No proof may repeat per field, index, validation target, storage operation,
retry substep, page, generated-language layer, or unselected branch effect.

### 6. Use least-sufficient V18 compiler identities

The new binding and decision semantics require `GRAMMAR_VERSION_V18`,
`EXECUTABLE_IR_VERSION_V18`, and `BUNDLE_FORMAT_VERSION_V18`. V18 canonically
encodes one deferred initialized binding, ordered decision arms, arm kind,
predicate program, branch-local binding/effect program, and rejection outcome.
Arm order is identity-bearing.

The V18 writer emits V18 only when a contract uses `observe_or_initialize` or
`decide`. Contracts using only older constructs retain their least-sufficient
historical source, plan, module, bundle, lock, role, and generated-artifact
identities byte-for-byte. Older readers reject V18 before activation with typed
refresh/upgrade guidance. The version-topology registry must record reader and
writer windows, fixtures, deployment rotation, and retirement posture before
V18 executes.

No entity, mutation, commit, outcome, event, changelog, index, provenance,
admission, storage-key, public Protobuf, driver-frame, or cursor encoding changes
under this decision. If implementation proves one is necessary, the responsible
work package stops for separate ADR-0124 classification and exact review.

### 7. Keep consumer policy outside RiffDB

RiffDB supplies a generic finite decision mechanism. External repositories own
their duplicate, missing, condition-equivalence, changelog payload, operation-
list, and conformance semantics. RiffDB source, fixtures, examples, generated
profiles, diagnostics, and documentation use framework-neutral entity and
operation names.

Acceptance may retain a value-free external receipt proving that one real
consumer replaced pre-read/omit logic with one generated atomic command. No
external schema, API route, adapter source, framework package, or special-
purpose branch enters this repository.

## Options Considered

1. **Always mutate ignored elements:** rejected because it changes revisions,
   indexes, and changelog state for an operation whose required semantics are
   no effect.
2. **Pre-read and omit ignored elements:** rejected because the decision is not
   transaction-current and races with concurrent writes.
3. **Split validation and mutation commands:** rejected because a typed error or
   concurrent state change can leave a partial list committed.
4. **Add a general `if`/`match` or transaction callback:** rejected because it
   permits unbounded control-flow, target, authority, cost, and compatibility
   shapes beyond the demonstrated requirement.
5. **Add a compiler-sealed decision over one deferred initialized target:**
   proposed because apply, no-effect, and rejection remain finite, atomic,
   statically authorized, and transaction-current.

## Consequences

- One bounded atomic command can classify every element from current state and
  produce exact apply, ignore, or whole-command rejection behavior.
- Ignored elements remain conflict dependencies without generating false state
  or changelog effects.
- Compiler, runtime, authorization, recovery, and compatibility complexity grow
  by one finite arm algebra and one new least-sufficient identity family.
- Arbitrary branching, nested decisions, dynamic targets, per-item partial
  outcomes, user callbacks, and general stored procedures remain deferred and
  prohibited.

## Compatibility

This Proposed ADR changes no current bytes or behavior. After acceptance, only
contracts using the new source forms rotate to V18 identities. Existing V1-V17
contracts and every durable application record remain byte-exact and readable
for their registered windows.

Generated method signatures continue to expose only the named command's typed
input and declared closed outcomes. No caller supplies an arm, predicate,
conflict policy, transaction, callback, target, or budget. Public transport and
durable storage schemas remain unchanged.

## Security

The union of every possible arm's authority is required before state access,
and exact selected policy runs before any effect or output. No-effect decisions
remain revalidated dependencies and cannot be used to bypass a conflict,
uniqueness, relationship, or policy check. Diagnostics reveal no selected arm,
row presence, preimage, hidden value, policy exclusion, target key, or secret.

Secret values remain sticky. A rejection or common success outcome that returns
a secret requires the same explicit source reveal, role grant, plan/module
binding, release-time reauthorization, and redaction as existing commands.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications declare only one
  finite ordered arm set over compiler-known expressions and targets in a named
  command. They cannot submit control flow, choose an arm, inspect state origin,
  select a transaction/conflict policy, weaken atomicity, or opt out of
  idempotency, durability, provenance, authorization, or current-state
  validation.
- **Scale:** arm count, nesting depth, bindings, targets, expressions, bytes,
  effects, indexes, retries, and collection elements are statically bounded.
  Runtime evaluates one selected arm per element with no scan, population-sized
  state, dynamic graph discovery, per-row task, or cross-partition transaction.

## Testing

- Parser, formatter, editor, and source-span snapshots for valid forms, one and
  eight arms, missing/duplicate `else`, nesting, aliasing, dynamic targets,
  forbidden dependencies, incomplete assignment, secret flow, and every
  unsupported branch-local effect.
- V18 IR/module/bundle/plan/hash/lock golden fixtures plus byte-exact V1-V17
  writer and reader-window regression.
- Pure-runtime differential tests for absent/present apply, absent/present no-
  effect, rejection, ordered predicates, initializer faults, and branch-local
  entity/event/changelog effects.
- Deterministic concurrent duplicate/duplicate, create/delete, update/update,
  ignore/update, ignore/delete, mixed-list, policy-revision, and retry-exhaustion
  schedules proving complete reevaluation and no stale arm choice.
- Memory/redb failpoints around admission, observation, decision, selected-graph
  construction, validation, staging, durability, publication, response loss,
  replay, and zero-application-effect committed outcomes.
- One, nine, nineteen, and one hundred element corpora covering all-apply, all-
  ignore, mixed apply/ignore, each typed rejection, duplicates, maximum bytes,
  maximum index work, cancellation, and complete-or-absent visibility.
- Authorization-union, selected create/update policy, branch-local create
  policy, revocation, reveal, redaction, and nondisclosure tests across Rust, Go,
  TypeScript, Python, CLI, MCP, local driver, and remote gRPC.
- Architecture checks for no general branch, callback, framework code, internal
  service recursion, unselected effect construction, repeated proof, or changed
  durable/public encoding.

## Requirements and Work Packages

- **Future requirements after exact acceptance:** `BLK-046` through `BLK-057`
- **Language, compiler, and V18 compatibility:** WP-716
- **Runtime, coordinator, policy, and recovery:** WP-717
- **Generated surfaces and generic/external acceptance:** WP-718

## Decision Deadline

Exact human acceptance is required before WP-716 adds conditional syntax,
deferred initialized bindings, branch-local effects, or V18 identities. Any
state-origin value, nested/general branch, dynamic target or outcome, per-item
partial commit, new durable mutation/commit encoding, cross-partition decision,
or weakening of union authorization and transaction-current revalidation
requires a separately accepted decision.
