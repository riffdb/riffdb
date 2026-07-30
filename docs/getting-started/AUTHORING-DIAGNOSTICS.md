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

`application check` always reports `no_files_changed`. A staged write failure
reports whether staging was discarded, the previous generation remains
accepted, or generated files may be partial while the exact lock was not
published. No diagnostic turns partial output into an accepted application.

Source, lock, or generated-artifact drift is always the structured
`RDB-AL008` diagnostic with `identity_drift` and `write_lock`. This includes an
interrupted or substituted generated file; the CLI, JSON output, and builder
MCP never replace it with unstructured process text. Review the symbolic diff
before writing a new lock. If source was not intentionally changed, restore it
and regenerate from the existing exact lock instead.

Use JSON when an agent or editor needs deterministic fields:

```bash
riffdb --output json application check
```

Malformed, unknown, oversized, or path-escaping diagnostic inputs fail closed;
they cannot introduce a new fix, retry instruction, source path, or prose field.
