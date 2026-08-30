# WP-721: can executable IR encode cross-aggregate conflict ownership?

ADR-0170 defers one question before any writer behaviour changes: whether the
executable IR can encode a command whose conflict ownership spans aggregates
without a version increment.

**It cannot. This needs executable IR V19, and it must use the ADR-0126
least-sufficient writer pattern so existing commands keep their plan hashes.**

## Why the current encoding cannot carry it

`LocalityPlan` is structurally single-aggregate, in three reinforcing ways.

It stores one aggregate and validates every conflict key against it
(`crates/riffdb-contract-ir/src/plan.rs`):

```rust
pub struct LocalityPlan {
    aggregate_id: AggregateTypeId,
    partition_schema: KeySchema,
    partition_expression: ExprId,
    conflict_keys: Vec<ConflictDerivationPlan>,
}
```

```rust
if partition_schema.purpose() != KeyPurpose::Partition(aggregate_id)
    || conflict_keys.iter().any(|key| key.schema.purpose() != KeyPurpose::Conflict(aggregate_id))
{
    return Err(IrValidationError::InvalidDependency {
        reason: "locality plan does not describe one complete aggregate partition",
    });
}
```

The aggregate identity is therefore carried twice: once in the field, and again
inside each key schema's `KeyPurpose`. A conflict key is not merely *associated
with* an aggregate — it is *typed by* one.

That shape is serialized, not derived
(`crates/riffdb-contract-ir/src/bundle.rs`):

```rust
fn encode_locality(writer: &mut Writer, locality: &crate::LocalityPlan) -> ... {
    writer.u32(locality.aggregate_id().get())?;
    encode_key_schema(writer, locality.partition_schema())?;
    ...
```

And that encoding feeds the command plan hash, whose domain payload is
`RIFFDB-COMMAND-PLAN\0 || ir_version || CommandId || CommandSemantics ||
ReferencedEnumClosure`. Widening the locality shape in place would change every
existing command's plan identity, invalidating every application lock in every
downstream repository.

## The shape that works

Follow ADR-0126, which added cascade at V13: "V1 through V12 readers and
least-sufficient writers remain active. Contracts without cascade continue to
emit the least sufficient older identity."

Applied here: a command whose create/mutate bindings all belong to one
aggregate continues to emit the current locality encoding **byte-for-byte** and
keeps its existing plan hash. Only a command that genuinely spans aggregates
emits the V19 form. No contract that exists today changes identity, and the
riffdb-openfga and riffdb-better-auth locks stay valid.

The V19 form needs conflict keys that carry their own aggregate rather than
inheriting one from the plan. The minimal change is to pair each
`ConflictDerivationPlan` with its owning `AggregateTypeId` and relax the
`KeyPurpose::Conflict` equality check to membership in the command's aggregate
set. The partition schema stays single — ADR-0170 keeps one partition route per
command, so `partition_schema` and `partition_expression` are unchanged.

ADR-0126 closes with the clause that governs this work:

> If implementation proves that a wire field, durable envelope/tag, changelog
> class, transaction ordering rule, or conflict-ownership rule must change,
> work stops for a separately accepted [record].

WP-721 changes the conflict-ownership rule, so that clause applies directly.
ADR-0170 is that separately accepted record.

## The compiler change is small

`crates/riffdb-contract-compiler/src/locality.rs` already performs two
independent checks over a command's bindings:

- `mutation_aggregate` — every create/mutate/delete binding is the same
  aggregate. **This is what ADR-0170 relaxes.**
- `partition_fingerprint` — every binding, read or write, derives the same
  partition expression. **This is what ADR-0170 keeps**, and it is already the
  exact proof the record requires.

The second check runs over all bindings today, including reads, which is why a
command may already read another aggregate in its partition. Removing the first
check while retaining the second is close to the whole front-end change.

Verified against the compiler rather than inferred from it. A command that
`read`s `Product` in `ProductData` and `mutate`s `Order` in `OrderData`, both
`partition_by tenant_id`, **compiles today**. Re-key `Product` to
`partition_by region_id` so the two bindings derive different routes, change
nothing else, and the same command is rejected with `RDB-C017`. The proof
ADR-0170 requires is therefore already implemented and already running over the
bindings it needs to cover; what the record relaxes is only the separate
same-aggregate test applied to mutating bindings.

`command_lowering.rs` also raises `RDB-C017`, but only for event partition
proofs. That site is unrelated and stays.

## What must not be done first

Relaxing `locality.rs` ahead of V19 makes things worse, not better. The
`mutation_aggregate` check is what currently stops a cross-aggregate command
from reaching `LocalityPlan::new`, which would then reject it anyway — its
conflict keys carry a `KeyPurpose::Conflict` for an aggregate that is not the
plan's. The result is the same rejection reported further from the source and
with a worse diagnostic. The encoding leads; the front end follows.

## Where the real work is

Not the compiler, and not the encoding. Two things:

**Lease acquisition.** The writer must take a lease set spanning aggregates
under a canonical total order over `(aggregate, conflict key)`, or concurrent
commands acquiring overlapping sets in different orders will deadlock. The
ordering is mechanical; proving it is the deliverable, and it needs a test that
fails under an arbitrary order rather than one that merely passes under the
canonical one.

**Crash atomicity.** No test in this repository covers a command writing two
aggregates, because none can be expressed. A restart arm proving such a command
is all-or-nothing is new coverage, not an extension of existing coverage.

## What this does not settle

Whether the relaxation should ship at all if aggregates later become the unit
of physical placement. ADR-0170 records that as the accepted trade; this note
only establishes that the format can express it and at what version.
