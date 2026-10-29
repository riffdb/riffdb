# Command and invariant cookbook

These patterns preserve RiffDB's application safety rule: writes are
accepted only through compiled commands whose complete data dependencies,
conflicts, invariants, outcomes, and partition route are known before runtime.

A contract does not pick a grammar version. The compiler assigns the least
sufficient one for the constructs the contract actually uses, so a contract
built only from the patterns below keeps the V1 identity, and one that reaches
for a newer construct — partition-local cross-aggregate writes, high-cardinality
atomic collections — selects the version that admits it. The current maximum is
V26. `SPEC.md` §7 defines the base surface and its changelog records what each
later version added.

## Create an aggregate root

```riff
command CreateCollection {
  input idempotency_key: string<128>
  input collection_id: uuid
  input name: string<96>
  idempotency_key idempotency_key
  create Collection(collection_id) as collection
    else CollectionExists { collection_id: collection_id }
  set collection.name = name
  set collection.created_at = tx.time
  return Created { collection: collection }
}
```

The idempotency input is caller-stable across retries. `create ... else`
declares duplicate handling as a typed business outcome. Every required
non-key field is assigned exactly once.

## Create a related child safely

```riff
command AddArtifact {
  input idempotency_key: string<128>
  input collection_id: uuid
  input artifact_id: uuid
  input title: string<128>
  idempotency_key idempotency_key
  read Collection(collection_id) as collection
    else CollectionMissing { collection_id: collection_id }
  create Artifact(collection_id, artifact_id) as artifact
    else ArtifactExists { artifact_id: artifact_id }
  set artifact.title = title
  set artifact.created_at = tx.time
  return Created { artifact: artifact }
}
```

The dominating exact read proves the required relationship target exists in
the same partition. Do not replace it with an application preflight read:
check-then-write races are exactly what the compiled command prevents.

## Enforce state rules in the command

```riff
require StillOpen: collection.closed == false
  else CollectionClosed { collection_id: collection_id }
```

Place a requirement after the binding that supplies its fields. The condition
is evaluated transaction-current inside the command. The rejection is a
declared typed outcome, not an exception inferred by client code.

Entity or aggregate invariants use:

```riff
invariant NonnegativeQuantity: quantity >= 0
```

An invariant is part of compilation and mutation validation. Do not reproduce
it with a prior RiffQL read or a generated-client branch.

## Change a relationship

Read the complete new target key before mutating relationship fields:

```riff
read Shelf(collection_id, new_shelf_id) as shelf
  else ShelfMissing { shelf_id: new_shelf_id }
mutate Artifact(collection_id, artifact_id) as artifact
  else ArtifactMissing { artifact_id: artifact_id }
overwrite artifact.shelf_id = new_shelf_id
```

The compiler rejects a relationship-changing command without the dominating
target read. Required references use required fields; optional relationships
must use a contract-supported optional shape.

## Declare the evidence for each write

`set` and `overwrite` are checked per assignment. Reading another stored field,
incrementing revision, or taking a decision branch does not authorize an
unrelated replacement `set`.

| Evidence | What it establishes | What it does not establish |
|---|---|---|
| Initialization | The binding is created without a prior value | Freshness of caller-supplied values |
| State-derived update | The target is changed by a same-field arithmetic delta | Freshness or business relevance of that delta |
| Named business check | A named requirement dominates and reads the target binding | That the author's chosen predicate captures stale caller intent |
| Caller-version fence | The expected root revision matches and advances atomically | That every other command maintains the same revision protocol |
| Explicit overwrite | The author declares replacement without qualifying evidence | A precondition on the replaced value |

For arithmetic, this POC recognizes only `target + delta`, `delta + target`,
and `target - delta`, using existing checked numeric types. Delta may read
inputs, constants, and keys but no stored state. `old * 0 + replacement` and
reading a different target field do not qualify.

Associate a business transition explicitly:

```riff
mutate Slot(tenant_id, slot_id) as slot else SlotMissing {}
require is_free: slot.free else SlotTaken {}
set slot.free = false checked_by is_free
set slot.held_by = holder checked_by is_free
```

Removing `is_free` makes these associations fail compilation. The compiler
checks the requirement's scope, execution order, and stored-state dependency.
The author asserts its relevance: even a weak predicate must never be read as
a proof of caller freshness. An invalid association fails even if another
proof would admit the write. Use one name after the value and before any
`reveals` annotation.

Use an expected revision for replacement edits:

```riff
mutate Parent(tenant_id, parent_id) as parent else ParentMissing {}
mutate Item(tenant_id, parent_id, item_id) as item else ItemMissing {}
require current: parent.revision == expected_revision else Stale {}
set parent.revision = parent.revision + 1
set item.value = value
```

The same fence covers replacement fields on `parent` itself. Merely incrementing
`parent.revision` does not. The comparison and increment must be unconditional;
revision and input must both be `i64` or both `u64`, and the increment is exactly
one, with either operand order. Children must belong to the same aggregate and
match every root-key argument. Overflow refuses the command. Relevant writers
must advance this revision, and callers must send the revision they observed;
the compiler does not enforce that protocol across all commands.

A deliberate replacement without these proofs uses `overwrite`. It can coexist
with a revision increment or an independent business check. Creation uses
`set`; initialized bindings are checked for their existing-row case. An
invariant, an unassociated requirement, and a decision predicate do not count
as per-write evidence. Current decision requirements execute after arm effects,
so they cannot be associated as dominating checks. Bulk outer requirements
execute after element effects; they cannot justify those element assignments.

`riffdb contract explain` reports every assignment occurrence and its evidence,
including decision-arm locations and requirement/fence references. Multiple
proofs are preserved. `RDB-C048` identifies a `set` without evidence, `RDB-C049`
a contradictory `overwrite`, and `RDB-C050` an invalid business association.

Newly compiled assignment-bearing plans use grammar, IR, and bundle V25. Their
hashes and application locks change. Review each affected contract, then use
the normal lock-writing workflow. Existing deployed artifacts retain their
behavior; explain labels their classification as legacy dependency evidence.
They acquire the new checks only when their source is migrated and recompiled.

Mutually exclusive `decide` arms (`when ... => { ... }` and `else => { ... }`)
may also declare branch-local workflow `transition` and `lease` instructions on
`mutate`, `init_or_mutate`, or `observe_or_initialize` bindings, using the same
expected revision, allowed source state, lease holder/epoch fences, and
zero-mutation declared rejection outcomes as top-level instructions.

## Unique values

Declare uniqueness on the entity:

```riff
unique collection_name (collection_id, name)
```

The complete partition prefix must lead the unique key. Commands that create or
change the tuple receive compiler-derived transaction-current occupancy and
conflict checks. A `query` followed by a write is not a uniqueness mechanism.

## Safe retry rule

Reuse the exact command name, input, role, contract identity, and idempotency
key until RiffDB returns the stored terminal outcome. Never generate a new key
to escape an uncertain response. Generated command helpers own this behavior.

## Patterns intentionally unavailable

- raw insert, update, delete, or SQL;
- arbitrary transaction callbacks;
- network, filesystem, wall-clock, or process-global access in commands;
- client-side check-then-write integrity;
- cross-partition mutation;
- undeclared business exceptions;
- unbounded loops, recursion, scans, or collections; and
- manually supplied conflict keys, plans, IDs, field masks, or capability
  grants.

An unavailable pattern is not an invitation to work around the compiler. Model
the invariant, relationship, aggregate, command, or declared outcome so RiffDB
can prove it.

## Allocate an unspecified free slot

Use [atomic indexed selection](ATOMIC-INDEXED-SELECTION.md) when an equality
prefix fixes the partition and every conflict key. `mutate first` selects one
row and carries its index generation through commit validation. Keep the
availability requirement and explicit write associations: selection itself does
not authorize replacement. Empty results are durable and replay under the same
request identity; a new attempt after capacity changes needs a new identity.
