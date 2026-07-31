# Contract-First State

In RiffDB, a contract is the source of executable operational semantics rather
than only a record schema. It defines what state means, which operations may
change it, what each operation can observe, and which results callers must
handle.

## Source, IR, and plan

The compiler boundary has three layers:

1. The source AST preserves author syntax and spans for diagnostics.
2. Typed IR contains resolved symbols, types, bounds, and canonical identities.
3. An executable command plan contains only operations the deterministic
   runtime and commit evaluator can execute.

Applications interact with source names and generated types. Numeric IDs,
storage keys, masks, dependency encodings, and plan hashes are compiler-owned.
The exact application lock records the derived result for review and
reproduction.

## Closed mutation surface

Every application mutation names a compiled command. A command declares:

- its input types and locality;
- the aggregate that owns each mutation;
- observations and predicate dependencies;
- preconditions, invariants, and commit checks;
- entity and index changes;
- durable events and outbox intents; and
- every business outcome.

A business rejection is a declared outcome, not an unstructured exception.
Invalid input, authorization failure, resource exhaustion, and unavailable
service state remain typed public errors.

## Static intent, current truth

Compilation makes the dependency shape explicit; execution still checks the
current database. The runtime evaluates against a snapshot and returns a
`CommitIntent`. The commit coordinator alone enters the authoritative write
transaction, revalidates dependencies and commit checks, assigns a sequence,
and applies the atomic command graph.

This separation prevents a long-lived storage write transaction from being
held while application expressions execute.

## Continue reading

- [Contract language overview](../contracts/)
- [Command and invariant cookbook](../contracts/COMMAND-INVARIANT-COOKBOOK.md)
- [Command lifecycle](COMMAND-LIFECYCLE.md)
- [Application source and exact lock](../getting-started/APPLICATION-MANIFEST.md)
