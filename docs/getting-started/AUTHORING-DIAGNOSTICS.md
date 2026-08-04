# Authoring diagnostics

Every local application compiler path returns the same bounded
`riffdb-authoring-diagnostic/v1` semantics. Human CLI output and `--output json`
are renderings of one checked value; builder MCP uses the JSON form unchanged.

Each diagnostic contains:

- a stable stage and code;
- a workspace-relative source path and half-open UTF-8 byte span when the
  originating compiler has one;
- a bounded symbolic path, never a submitted runtime value;
- one closed cause and closed corrective-action codes;
- what happened to local files; and
- whether to correct source, review and write a lock, retry the same inputs, or
  contact an operator.

Example:

```text
RDB-QP003 [query_plan] query has no bounded index access plan
  --> riffdb/queries/list_items.riffq:104..179
  symbol: ListItems.Item
  cause: missing_index
  fixes: add_index
  files: no_files_changed; retry: correct_source
```

The byte range points into the caller-owned local file. RiffDB does not copy
source snippets, literals, command inputs, credentials, application values,
hidden schema, engine errors, or arbitrary library prose into the diagnostic,
logs, evidence, or MCP output.

The main correction codes are deliberately executable concepts:

- `use_language_reference`: correct syntax using the bundled grammar.
- `correct_symbol` / `correct_type`: resolve the named contract operation or
  make exact types agree.
- `supply_partition_route`: add the complete same-partition route.
- `model_one_mutation_aggregate`: place every entity changed atomically under
  one declared aggregate root, or split the workflow into independently
  idempotent commands.
- `prove_relationship_target`: read the complete relationship target before
  the mutable binding, declare its missing-target outcome, and reuse the same
  target-key input expressions when storing the relationship fields.
- `add_index`: add the bounded index identified by query explain.
- `add_bound`: declare explicit positive cardinality/work bounds.
- `reduce_input`: reduce a complete worst-case input or query-work bound. For
  `RDB-AR007`, replace unconstrained `Limit` parameters on a multi-collection
  page with fixed `take` limits whose aggregate index-scan bound is at most 500.
- `narrow_role`: remove or correct unsafe symbolic authority.
- `write_lock`: review the symbolic/authority diff, then run
  `riffdb application lock --write`.
- `generate_locked`: restore compiler-owned output with
  `riffdb application generate --locked`.

`RDB-C017` deliberately reports both `supply_partition_route` and
`model_one_mutation_aggregate`. It covers two unsafe shapes: bindings whose
route expressions are not provably identical, and one command that creates or
mutates records owned by different aggregates. A shared route does not make
independent aggregate writes atomic. Start from the command's complete mutation
set, choose one business root, make every written entity a root or child of
that aggregate, and put the route first in every key. If that ownership would
be false, keep the roots independent and use separate idempotent commands.

`RDB-C024` rejects a relationship change whose exact target proof is missing or
has expression drift. The complete target read must appear before the relevant
`create` or `mutate`, must declare what happens when the target is absent, and
must use the same key input expressions as the stored relationship fields. For
example, after `read Author(site_id, author_id) as author`, store
`post.author_id = author_id`. In grammar v1, storing
`post.author_id = author.author_id` is value-equivalent after the read but is
not the same structural expression and therefore does not satisfy the proof.
The database does not infer that equivalence or weaken the declared
relationship.

`application check` always reports `no_files_changed`. A staged write failure
reports whether staging was discarded, the previous generation remains
accepted, or generated files may be partial while the exact lock was not
published. No diagnostic turns partial output into an accepted application.

During iterative source editing, `application check --source-only` compiles the
complete author-owned contract, queries, and roles without comparing the old
compiler-owned generation. Success explicitly says no lock or generated
artifact was checked. This mode never writes, deploys, or grants authority.

When the default lock exists, exact `application check` verifies the source, pinned
contract bundle, exact lock, and every generated artifact. It cannot report a
source-only success over a stale lock. With no lock present, success explicitly
says that only symbolic sources compiled and that no lock or generated artifact
was checked.

Source, lock, or generated-artifact drift is always the structured
`RDB-AL008` diagnostic with `identity_drift` and `write_lock`. This includes an
interrupted or substituted generated file; the CLI, JSON output, and builder
MCP never replace it with unstructured process text. Review the symbolic diff
before writing a new lock. If source was not intentionally changed, restore it
and regenerate from the existing exact lock instead.

`RDB-AR003` describes a disagreement between a role's exact manifest contract
and the contract bundle used to derive its grant; it is not a report about an
older capability still stored by the server. With lock V3, role operations use
the pinned parent-aware bundle, so an exact `application check` and an
`RDB-AR003` from the same unchanged workspace would be an implementation defect,
not a reason to rewrite the same lock repeatedly. A deliberately widened role
instead produces a new role definition hash and requires
`--replace-role-credential`; replacement diagnostics include the retained and
locked role hashes in JSON when both are known.

Use JSON when an agent or editor needs deterministic fields:

```bash
riffdb --output json application check
```

Malformed, unknown, oversized, or path-escaping diagnostic inputs fail closed;
they cannot introduce a new fix, retry instruction, source path, or prose field.
