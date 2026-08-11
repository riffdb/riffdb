# Bounded Collection Commands

> Alpha surface: grammar, executable IR v5, and atomic runtime execution are implemented.
> Collection commands use the ordinary symbolic command service. Generated Rust, TypeScript,
> and Python collection facades remain unavailable until the binding rollout is complete.

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

Cascade, set-null, orphaning, cross-partition deletion, physical history removal, nested loops,
and caller-supplied callbacks are not part of this surface. Deleting an entity with a declared
unique key is also rejected in the current compiler because its release conflict is not yet
input-computable. Use a new command/contract shape; do not work around the rejection with a raw
storage operation.

Duplicate collection keys reject the whole command. All expanded effects, the typed outcome,
events, provenance, idempotency record, and commit record are one atomic command result. A crash
or retry can expose only the complete persisted result or complete absence; an element-level
partial result is not part of the protocol.
