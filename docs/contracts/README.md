# RiffDB contract language v1

The [complete grammar-version-1 parser reference](LANGUAGE.md) is generated
directly from the parser grammar. The crate-local and public copies are checked
byte-for-byte in CI.

A contract is compiled before deployment. Parsing alone grants nothing and
does not create an executable plan. Compilation must prove:

- every symbol and expression has one exact static type;
- every entity belongs to one aggregate and has a complete key;
- every command is confined to one statically derivable partition;
- mutating commands have one direct bounded-string idempotency input;
- relationship and uniqueness changes are validated transaction-current;
- required fields are definitely initialized and each mutation target is
  legal;
- every declared outcome and emitted event has one consistent typed shape; and
- all source, nesting, collection, schema, key, row, and plan bounds hold.

If any proof is missing, compilation fails with a stable source-spanned
diagnostic. There is no fallback to a callback, SQL statement, unrestricted
transaction, cross-partition write, or runtime-only integrity convention.

## Initialized exact-key transitions

Use `init_or_mutate` when one named command must apply the same checked effects
to an exact entity key whether the row is absent or present:

```riff
init_or_mutate Counter(organization_id, counter_id) as counter initialize {
    enabled: false,
    revision: 0,
}
set counter.enabled = true
set counter.revision = counter.revision + 1
```

For an absent row, RiffDB constructs a provisional record from the declared key
and initializer, runs the common command suffix, and stages exactly one
`Create`. For a present row, it ignores initializer values, runs that same
suffix against the transaction-current preimage, and stages exactly one
revision-checked `Replace`. Revalidation discards and reevaluates a candidate
if concurrent work changes which state was observed. The command has one
ordinary closed outcome surface; callers cannot choose the state path,
conflict behavior, or overwrite policy.

Initializer expressions may use constants, command inputs, service-owned
values, deterministic transaction context, and the current element of a
bounded bulk command. They cannot read entity state, query results, storage,
the operating-system clock, or randomness. An initializer may be partial or
empty, but the compiler proves that every absent-path read is dominated by an
initial value or common assignment and that every successful create postimage
is complete.

The compiler requires the union of create and update authority and both row
policy operation families before the command can be invoked. Static index and
graph accounting charges the maximum of the possible create and replace work,
never their sum; runtime charges only the selected mutation. Collection count,
aggregate-byte, one-partition, duplicate-target, mutation, and index-work
ceilings remain unchanged. This is a sealed exact-key transition, not a generic
upsert: there are no caller-selected conflict targets, conditional arms,
callbacks, raw predicates, or last-write-wins behavior.

## Compiler-sealed decisions

Use `observe_or_initialize` plus its immediately following `decide` when the
same named command must atomically choose between applying a finite graph,
accepting an exact no-effect transition, or rejecting the whole command:

```riff
observe_or_initialize TupleState(organization_id, tuple_key) as state
  initialize { active: false, revision: 0 }
decide state {
  when state.active == expected_active => apply {
    set state.active = requested_active
    set state.revision = state.revision + 1
  }
  when state.active == requested_active => no_effect
  else => reject StateConflict { tuple_key: tuple_key }
}
```

Arms are ordered, the first true `when` wins, and the mandatory `else` makes
selection total. An `apply` finalizes the deferred row as exactly one create or
revision-checked replace and executes only that arm's compiler-known suffix.
`no_effect` emits no entity, index, event, outbox, workflow, or changelog
effect. `reject` discards every provisional effect from the ordinary or bulk
command and persists its declared whole-command outcome.

The caller cannot select or observe an arm or absence/presence origin. RiffDB
authorizes the union of all arms before reading state, then enforces the exact
selected create/update row policy transaction-current. One through eight
`when` arms are allowed; nested decisions, callbacks, dynamic targets,
caller-selected predicates, and general branching are not. Bulk decisions use
the existing element, aggregate-byte, graph, index-work, retry, and
single-partition limits. Static effect cost is the maximum arm plus real union
overhead, while runtime constructs and charges only the selected graph.

## Source bounds

| Input | Inclusive limit |
|---|---:|
| Contract source | 1,048,576 UTF-8 bytes |
| Identifier | 256 bytes |
| Tokens | 131,072 |
| AST nodes | 131,072 |
| Nesting | 32 |
| Items in one declaration | 4,096 |
| Items in one local list | 1,024 |
| Diagnostics | 32 |

The authoritative constants live in the parser and compiler. The generated
language reference is checked against the parser grammar in CI.

## Command index-work bounds

Mutating plans prove secondary-index work before plan hashing. The compiler
keeps physical index-entry removals and additions at 4,096, permits at most
65,535 exact affected prefix epochs and complete validation positions, and
also requires their correlated charge to fit:

```text
index-entry deltas + affected prefix epochs + validation positions <= 65,535
```

Affected targets and their current epoch observations must independently fit
the 16 MiB command read-state ceiling. A bounded collection shares its proved
partition prefix across elements, but the compiler assumes no equality among
caller values, entity keys, other index fields, or different elements. When a
collection declares `aggregate_bytes`, the same checked aggregate bound may
limit copies of element-sourced bytes in index prefixes; count and work charges
are never reduced by byte correlation.

An exceeded known ceiling is reported as `RDB-C020` with a closed resource
identity and the checked `actual` and `maximum` integers. These diagnostics
contain compiler-owned plan metadata only—never submitted values, keys, stored
rows, or backend details. There is no application-selectable work budget,
runtime fallback, partial command, or automatic command split.

## Author workflow

```text
edit riffdb/contract.riff
riffdb application check
riffdb application lock --write
riffdb application lock --check
```

Authors use names. Stable IDs, hashes, command plans, capability requirements,
field visibility, and generated transport code belong to the compiler-owned
lock and generated tree.

See [First application authoring](AUTHORING.md) for reserved identifiers,
namespace rules, supported comments, natural CLI/MCP JSON values, and installed
deployment inputs.

See [the command and invariant cookbook](COMMAND-INVARIANT-COOKBOOK.md) for
safe patterns and [the Museum example](examples/museum/) for a complete
multi-entity application.
