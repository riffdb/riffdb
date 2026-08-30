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

The V19 form needs **no new bytes at all**. This note originally proposed
pairing each `ConflictDerivationPlan` with its owning `AggregateTypeId`;
implementing it showed that pairing already exists. A conflict key's schema is
`KeyPurpose::Conflict(owner)`, `encode_key_schema` writes that owner as a u32,
and `ConflictKeyBuilder::new(aggregate_type)` additionally namespaces the
encoded key bytes by aggregate. Per-key ownership was always encoded; only
`LocalityPlan::new` insisted every key name the same aggregate.

So V19 widens an accepted value space rather than a layout. It still earns a
version, because a V18 reader must refuse a cross-aggregate plan by version
rather than by an opaque locality validation failure. The partition schema
stays single — ADR-0170 keeps one partition route per command, so
`partition_schema` and `partition_expression` are unchanged.

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

## Where the work actually was

This note first predicted the work would be lease acquisition and crash
atomicity, and that the compiler and encoding were the easy parts. The first
half of that was wrong.

**Lease acquisition was already done.** `lower_conflict_keys` in
`crates/riffdb-commit/src/command_admission.rs` takes the declared conflict
keys as a set, sorts them, and dedups before any lease is taken. Because
`ConflictKeyBuilder` prefixes each key with its aggregate, that sort *is* a
canonical total order over `(aggregate, key)`. The writer never assumed one
aggregate; only the compiler and the IR validator did.

**The work was three validation gates and the version ladder**, all of which
independently re-derived the single-aggregate rule:

1. `LocalityPlan::new` required every conflict key's purpose to equal the
   plan's aggregate. Relaxed to "is a conflict key"; the union is read off the
   keys.
2. The binding validator required every mutable binding's aggregate to equal
   `locality.aggregate_id`. Relaxed to membership in the ownership set, so a
   binding with no conflict key — and therefore no lease — is still rejected.
3. The conflict-coverage validator compared every derivation against *one*
   aggregate's template. Each now resolves its binding's own aggregate and
   root. The partition check immediately above it already resolved per-binding
   aggregates, which is what proves they share the route.

`lower_locality` in the compiler had the same shape: it substituted every
binding's arguments into the anchor aggregate's conflict expressions, which for
a foreign binding builds a key for a row that does not exist.

**Crash atomicity is still absent.** No arm covers a command writing two
aggregates across a restart. That was true before this work and remains true.

## Status

Delivered: the compiler proof, union conflict ownership, executable IR V19 and
its version ladder, and coverage in
`crates/riffdb-contract-compiler/tests/cross_aggregate_writes.rs`. The
riffdb-openfga and riffdb-better-auth application locks were verified
byte-identical after the change, which is the compatibility claim tested rather
than argued.

Outstanding: a deadlock arm that fails under an arbitrary acquisition order,
and a crash arm proving a command writing two aggregates is atomic across a
restart. The lease order is believed correct by construction and by reading
`lower_conflict_keys`; that is not the same as tested.

## What this does not settle

Whether the relaxation should ship at all if aggregates later become the unit
of physical placement. ADR-0170 records that as the accepted trade; this note
only establishes that the format can express it and at what version.
