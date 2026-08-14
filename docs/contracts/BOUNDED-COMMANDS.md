# Bounded Collection Commands

> Alpha surface: grammar, executable IR v5, atomic runtime execution, and generated Rust, Go,
> TypeScript, Python, and MCP bindings are implemented. Collection commands use the ordinary
> symbolic command service; they do not introduce a generic transaction API.

RiffDB collection writes are compiled commands, not caller-defined transactions. A `bulk command`
may expand exactly one bounded list, once, with no nesting or data-dependent iteration. The
compiler proves the complete partition, aggregate, conflict, read, write, index, and capacity
sets before deployment. The list maximum and total aggregate instances cannot exceed 256, and
the complete input and mutation graph remains subject to the 16 MiB command bound.

```riff
entity Row {
    key (organization_id: uuid, row_id: uuid)
    delete_policy no_inbound
}

aggregate Rows {
    root Row
    partition_by organization_id
    conflict_key (organization_id, row_id)
}

bulk command DeleteRows {
    input request_id: uuid
    input organization_id: uuid
    input row_ids: list<uuid, 1..64>
    idempotency_key request_id

    for row_id in row_ids {
        delete Row(organization_id, row_id) as row else Missing {}
    }

    return Deleted {}
}
```

Every accepted delete names a policy on the entity:

- `delete_policy no_inbound` is legal only when no declared relationship targets the entity.
- `delete_policy restrict Child.by_parent` requires every inbound relationship to be covered by
  that exact, canonical, partition-local reverse-index prefix. The command plan records a
  transaction-current empty-prefix check; a runtime scan or unindexed relationship discovery is
  never substituted.

An indexed-restrict delete must name the business result separately from a missing row:

```riff
delete Parent(tenant_id, parent_id) as parent
    else Missing {}
    restrict Referenced {}
```

If the reverse index is nonempty, RiffDB persists `Referenced` as the command's typed zero-mutation
outcome. If a reference races with the delete, transaction-current validation reevaluates the
whole bounded command and selects `Referenced`; it does not spin on an infrastructure retry.
Grammar/IR v6 permits one indexed-restrict delete template per command.

Cascade, set-null, orphaning, cross-partition deletion, physical history removal, nested loops,
and caller-supplied callbacks are not part of this surface. A bounded delete may target an entity
with declared unique keys. RiffDB derives and removes each exact old index entry from the checked
predecessor inside the authoritative write transaction; application input does not supply or
guess a release conflict for a value visible only in that predecessor. Ordered execution is
intentional: a create submitted before the delete still receives the typed unique conflict, while
a create admitted after the committed delete may reuse the released value. `RDB-C044`, the former
feature seal for this audited path, is retired; unsafe deletion policy remains the source-spanned
`RDB-C045` refusal. Ordinary commands do not gain delete syntax—the accepted first delete format
remains a compiler-bounded collection command.

Duplicate collection keys reject the whole command. All expanded effects, the typed outcome,
events, provenance, idempotency record, and commit record are one atomic command result. A crash
or retry can expose only the complete persisted result or complete absence; an element-level
partial result is not part of the protocol.

## Client and CLI preflight

Generated Rust, Go, TypeScript, and Python methods carry the compiler's exact inclusive list
minimum and maximum. They reject an out-of-range collection before invoking the transport. The
generated MCP JSON Schema carries the same `minItems` and `maxItems`. Canonical command encoding
also enforces the fixed document-byte ceiling before a request is sent; no client silently splits
one atomic collection command into smaller writes.

The CLI accepts inline JSON, `@file`, a file path, or `-` for stdin. Nested collection elements are
ordinary symbolic JSON records; scalar tags remain explicit where JSON has no lossless native
form:

```json
{
  "request_id": {"$uuid":"018f0f8b-7c6d-7e31-8a4f-000000000001"},
  "tuples": [{
    "store_id": {"$uuid":"018f0f8b-7c6d-7e31-8a4f-000000000002"},
    "tuple_id": {"$uuid":"018f0f8b-7c6d-7e31-8a4f-000000000003"},
    "object": "document:roadmap",
    "relation": "viewer",
    "subject": "user:agent"
  }]
}
```

Pass the exact application source to make the CLI load its checked lock and reject the compiled
collection count before connecting:

```bash
riffdb command run WriteTuples \
  --application riffdb.application.json \
  --input @write-tuples.json
```

Without `--application`, local byte and structural bounds still apply and the server remains the
authoritative fail-closed validator, but the ad-hoc CLI cannot know a command-specific list range.
