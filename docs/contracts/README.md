# RiffDB contract language

The [complete parser reference](LANGUAGE.md) is generated
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

## Common contract and query patterns

Most applications need the same nine building blocks. Each example below is
complete enough to copy directly into `riffdb/contract.riff` or `riffdb/queries/*.riffq`.

### 1. Contract skeleton, entities, and optional fields

Declare your contract header, inline-typed primary keys (`key (org_id: string<64>, ...)`),
scalar types, bounded strings, nullable fields (`optional<T>`), and deletion policy
(`delete_policy no_inbound` when no inbound references target the entity):

```riff
contract TodoApp version 1 {
  entity Todo {
    key (org_id: string<64>, todo_id: string<64>)
    field title: string<200>
    field notes: optional<string<2000>>
    field done: bool
    field updated_at: timestamp
    field revision: i64

    index by_updated (org_id, updated_at, todo_id)
    index by_done_updated (org_id, done, updated_at, todo_id)
    delete_policy no_inbound
  }

  entity TodoPatch {
    key (org_id: string<64>, todo_id: string<64>)
    field expected_revision: i64
    field title: optional<string<200>>
    field done: optional<bool>
  }

  aggregate TodoAggregate {
    root Todo
    child TodoPatch
    partition_by org_id
    conflict_key (org_id, todo_id)
  }
}
```

- Primary key fields are declared inline inside `key (field_name: type, ...)`—do
  not repeat key fields as separate `field` declarations.
- Every entity belongs to an `aggregate` whose `partition_by` prefix leads its
  `key (...)` and `conflict_key (...)`.
- Use `optional<T>` for nullable entity fields and omissible command inputs,
  and `null` for the absent literal.
- `TodoPatch` declares the typed element shape for bulk collection commands
  (`list<TodoPatch, 1..64>` in section 6 below).

### 2. Create a record

Every mutating command declares a caller-stable `idempotency_key` and assigns
every required non-key field on `create` (optional fields can be set from an
`optional<T>` input or assigned `null`). Service-supplied timestamps and IDs use
`service <name>: transaction_time` or `service <name>: uuid_v7`:

```riff
  command CreateTodo {
    input idempotency_key: string<128>
    input org_id: string<64>
    input todo_id: string<64>
    input title: string<200>
    input notes: optional<string<2000>>
    service now: transaction_time
    idempotency_key idempotency_key

    create Todo(org_id, todo_id) as todo
      else TodoAlreadyExists { todo_id: todo_id }
    set todo.title = title
    set todo.notes = notes
    set todo.done = false
    set todo.updated_at = now
    set todo.revision = 1
    return Created { todo_id: todo.todo_id, revision: todo.revision }
  }
```

### 3. Update with a revision check (`CallerVersionFence`)

To prevent blind overwrites (`RDB-C048`), pair an aggregate-root revision
equality check (`require <name>: <root>.revision == <expected>`) with an atomic
increment (`set <root>.revision = <root>.revision + 1`). Once both are present,
any `set` on that root or its same-aggregate children is automatically guarded:

```riff
  command RenameTodo {
    input idempotency_key: string<128>
    input org_id: string<64>
    input todo_id: string<64>
    input expected_revision: i64
    input title: string<200>
    service now: transaction_time
    idempotency_key idempotency_key

    mutate Todo(org_id, todo_id) as todo
      else TodoNotFound { todo_id: todo_id }
    require fresh_rev: todo.revision == expected_revision
      else RevisionConflict { current_revision: todo.revision }

    set todo.title = title
    set todo.updated_at = now
    set todo.revision = todo.revision + 1
    return Updated { revision: todo.revision }
  }
```

### 4. Partial update with `??` or `coalesce(...)`

You do not need separate commands for every combination of updated fields (for
example, `SetTodoDone` next to `UpdateTodo`). Declare optional inputs with
`optional<T>` and preserve unspecified fields with `input_field ?? record.field`
(or `coalesce(input_field, record.field)`):

```riff
  command UpdateTodo {
    input idempotency_key: string<128>
    input org_id: string<64>
    input todo_id: string<64>
    input expected_revision: i64
    input title: optional<string<200>>
    input notes: optional<string<2000>>
    input done: optional<bool>
    service now: transaction_time
    idempotency_key idempotency_key

    mutate Todo(org_id, todo_id) as todo
      else TodoNotFound { todo_id: todo_id }
    require fresh_rev: todo.revision == expected_revision
      else RevisionConflict { current_revision: todo.revision }

    set todo.title = title ?? todo.title
    set todo.notes = coalesce(notes, todo.notes)
    set todo.done = done ?? todo.done
    set todo.updated_at = now
    set todo.revision = todo.revision + 1
    return Updated {
      todo_id: todo.todo_id,
      title: todo.title,
      done: todo.done,
      revision: todo.revision
    }
  }

  command ClearTodoNotes {
    input idempotency_key: string<128>
    input org_id: string<64>
    input todo_id: string<64>
    input expected_revision: i64
    service now: transaction_time
    idempotency_key idempotency_key

    mutate Todo(org_id, todo_id) as todo
      else TodoNotFound { todo_id: todo_id }
    require fresh_rev: todo.revision == expected_revision
      else RevisionConflict { current_revision: todo.revision }

    set todo.notes = null
    set todo.updated_at = now
    set todo.revision = todo.revision + 1
    return Cleared { todo_id: todo.todo_id, revision: todo.revision }
  }
```

- `a ?? b` (and `coalesce(a, b, ...)`) returns `a` whenever `a` is non-null—even
  when `a` is `false`, `0`, or `""`—and falls back to `b` only when `a` is
  `null`. Because `null` means "keep existing value" in a `??` expression, use
  an explicit `set todo.notes = null` command (`ClearTodoNotes` above) when a
  nullable field must be cleared back to `null`.
- Generated TypeScript, Go, and Python SDKs emit `optional<T>` command inputs as
  omissible fields (`title?: string | null` in TypeScript, `*string` in Go, and
  `title: str | None = None` in Python), so callers pass only the fields they
  want to change.

### 5. Delete a record

Declare `delete_policy no_inbound` on an entity with no inbound references (or
`restrict` / `cascade` for referenced entities; see [Bounded Delete Commands](BOUNDED-COMMANDS.md)).
In a unary delete command, the `delete` binding itself exposes the transaction-current
preimage for `require` checks and `return` fields:

```riff
  command DeleteTodo {
    input idempotency_key: string<128>
    input org_id: string<64>
    input todo_id: string<64>
    input expected_revision: i64
    idempotency_key idempotency_key

    delete Todo(org_id, todo_id) as todo
      else TodoNotFound { todo_id: todo_id }
    require fresh_rev: todo.revision == expected_revision
      else RevisionConflict { current_revision: todo.revision }
    return Deleted { todo_id: todo.todo_id }
  }
```

### 6. Bulk command with per-item revision checks

A `bulk command` iterates over one bounded `list<Element, MIN..MAX>` input and
commits all element mutations atomically in one partition. When each element
carries multiple fields, declare a companion child entity (`TodoPatch` in
section 1 above) on the same aggregate and reference it as `list<TodoPatch, 1..64>`.
Inside the `for` loop, an iteration-local revision check on the aggregate root
(`require <name>: <root>.revision == item.expected_revision`) paired with
`set <root>.revision = <root>.revision + 1` in the same loop body is recognized
automatically as a `CallerVersionFence` for that iteration's root and child
mutations—no `checked_by` is needed on every `set` line:

```riff
  bulk command BulkUpdateTodos {
    input idempotency_key: string<128>
    input org_id: string<64>
    input items: list<TodoPatch, 1..64>
    service now: transaction_time
    idempotency_key idempotency_key

    for item in items {
      mutate Todo(org_id, item.todo_id) as todo
        else TodoNotFound { todo_id: item.todo_id }
      require item_rev: todo.revision == item.expected_revision
        else RevisionConflict { todo_id: item.todo_id, current_revision: todo.revision }

      set todo.title = item.title ?? todo.title
      set todo.done = item.done ?? todo.done
      set todo.updated_at = now
      set todo.revision = todo.revision + 1
    }

    return Completed {}
  }
```

When a write is authorized by a business precondition rather than an aggregate-root
revision fence (for example, `require not_done: todo.done == false ...`), attach
`checked_by <require_name>` after the assigned value:

```riff
      require not_done: todo.done == false
        else AlreadyDone { todo_id: item.todo_id }
      set todo.done = true checked_by not_done
```

Or use `overwrite todo.done = item.done` when an unconditional last-write-wins
replacement is intentional.

### 7. Matching `.riffq` queries and automatic SDK chunking/pagination

Each query lives in its own `.riffq` file under `riffdb/queries/`, has an
explicit `take` bound on `many` queries, and uses a declared contract index:

```riffql
// riffdb/queries/get_todo.riffq
query GetTodo($org_id: Todo.org_id, $todo_id: Todo.todo_id) {
  one todo from Todo
    where org_id == $org_id && todo_id == $todo_id
    else NotFound
  return Found {
    todo: todo { todo_id title notes done updated_at revision }
  }
  outcomes Found | NotFound
}

// riffdb/queries/list_todos.riffq
query ListTodos(
  $org_id: Todo.org_id,
  $done: Todo.done?,
  $limit: Limit<200> = 50,
  $after: Cursor?
) {
  many todos from Todo
    where org_id == $org_id
      && when $done { done == $done }
    order by updated_at desc, todo_id desc
    take $limit after $after
  return Found {
    todos: todos { todo_id title done updated_at revision }
  }
  outcomes Found
}

// riffdb/queries/todos_by_ids.riffq
query TodosByIds(
  $org_id: Todo.org_id,
  $todo_ids: Set<Todo.todo_id>
) {
  many todos from Todo
    where org_id == $org_id && todo_id in $todo_ids
    order by todo_id asc
    take 200
  return Found {
    todos: todos { todo_id title done updated_at revision }
  }
  outcomes Found
}
```

Because every database query is strictly bounded (`Set<T>` defaults to `200`
items, or `Set<T, MAX>` declares an explicit bound from `1` through `65,535` up
to the query's `take` ceiling), and primary-key membership queries
(`org_id == $org_id && todo_id in $todo_ids order by todo_id asc`) execute via
bounded primary-key point reads in one snapshot without requiring an extra
secondary index (unless a covering index is declared to avoid entity reads),
generated TypeScript, Go, and Python SDKs automatically emit:

- Per-query bound constants (`TODOS_BY_IDS_TODO_IDS_MAX_ITEMS = 200` in
  TypeScript/Python, `TodosByIdsTodoIdsMaxItems = 200` in Go).
- `<query>All` and `<query>Pages` helpers (`client.todosByIdsAll(...)`,
  `client.listTodosPages(...)`) that enforce an upfront total item bound
  (`items.length <= maxPages * MAX_ITEMS`), deduplicate and canonically sort
  `Set<T>` inputs in the query's `order by` direction before slicing into chunks,
  and thread `readAfterCommit` across chunks/pages as a monotonic freshness
  floor under a bounded `maxPages` cap (default `64`; declare `Set<T, MAX>` when
  a single server snapshot is required across more than `200` items).

> **Version note (`0.2.2+`):** Indexless primary-key `in $ids` queries (`order by <key> asc`) and referencing `item.<field>` inside bulk-command failure/rejection outcomes (`else TodoNotFound { todo_id: item.todo_id }`) require `riffdb >= 0.2.2`. If either fails to compile, check `riffdb --version` and `which riffdb` to ensure an older `0.2.1` binary in `~/.local/bin` or `PATH` is not shadowing your toolchain.

## Mental model for strictness

RiffDB rejects patterns that would hide unbounded scans, cross-partition locks,
or silent lost updates. This quick reference summarizes the four rules authors
encounter most often:

| Construct | What it proves | Rule of thumb |
| --- | --- | --- |
| `partition_by` vs. `conflict_key` on `aggregate` | `partition_by` is the routing and single-command locality boundary (every entity key and command in the aggregate begins with `partition_by`). `conflict_key` is the optimistic-concurrency unit within that partition (`conflict_key` starts with `partition_by` and typically matches the root entity's `key (...)`). | Use the tenant/workspace/store column as `partition_by` (`partition_by org_id`) and the full aggregate root key as `conflict_key (org_id, todo_id)` so concurrent writes to different roots in the same org do not contend. |
| `set` + `CallerVersionFence` vs. `checked_by` vs. `overwrite` (`RDB-C048`) | Every mutation of an existing row must state how concurrent updates are guarded. | 1. **Revision fence (`CallerVersionFence`):** `require <name>: <root>.revision == <expected>` + `set <root>.revision = <root>.revision + 1` automatically guards all `set` statements on that root and its children.<br>2. **Precondition guard (`checked_by`):** `set todo.done = true checked_by not_done` links a write to a named `require not_done: ...` check.<br>3. **Unconditional last-write-wins (`overwrite`):** `overwrite todo.title = title` opts into blind replacement explicitly. |
| `delete_policy` on `entity` | Proves that deleting a row cannot leave dangling foreign-key references (`reference <name> (<fields>) -> <Target>(<keys>)`). | - `delete_policy no_inbound`: leaf entity with no inbound `reference` declarations.<br>- `delete_policy restrict`: deletion requires `else restricted <Outcome> { ... }` on `delete` and aborts if any referencing child exists.<br>- `delete_policy cascade (max_dependents: N)`: deletion requires `else cascade_limit <Outcome> { ... }` and atomically deletes up to `N` referencing rows. |
| `for item in items` in `bulk command` | Bounds per-command write amplification while allowing batch mutations over `list<Element, MIN..MAX>`. | Inside `for item in items { ... }`, `item.<field>` is in scope for entity key arguments, `require` conditions, per-item `else` outcomes, and `set` / `overwrite` values. Outside the loop, `return <Outcome> { ... }` summarizes the batch using top-level command inputs or service values. |

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

Use `observe_or_initialize` (or a `mutate` or `init_or_mutate` binding) plus an
immediately following `decide` when the same named command must atomically
choose between applying a finite graph, accepting an exact no-effect
transition, or rejecting the whole command:

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
revision-checked replace and executes only that arm's compiler-known suffix
(including any branch-local workflow `transition` or `lease` instructions).
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
