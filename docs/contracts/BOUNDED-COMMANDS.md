# Bounded Delete Commands

> Alpha surface: grammar, executable IR v16, atomic runtime execution, and generated Rust, Go,
> TypeScript, Python, and MCP bindings are implemented. Collection commands use the ordinary
> symbolic command service; they do not introduce a generic transaction API. Compiler-bounded
> one-hop cascade grammar, executable IR V13, and the command/runtime/storage path are implemented.
> Generated Rust, Go, TypeScript, and Python cascade facades expose the declared typed success and
> overflow outcomes; generated MCP deliberately omits secret-bearing query output.

## Atomic unary delete and preimage return

An ordinary idempotent `command` may delete exactly one complete-primary-key,
partition-local entity whose structural policy is `no_inbound`. The delete binding denotes the
immutable transaction-current preimage: requirements, explicit events, and the declared outcome
all read the same record and version carried as the delete mutation's prior image.

```riff
command ConsumeToken {
    input request_id: uuid
    input organization_id: uuid
    input token_id: uuid
    idempotency_key request_id

    delete OneTimeToken(organization_id, token_id)
        as token
        else TokenMissing {}

    return TokenConsumed {
        identifier: token.identifier,
        value: token.value reveals token.value
    }
}
```

Absence selects `TokenMissing` with no mutation. If another command updates or deletes the row
after evaluation, transaction-current validation discards the entire evaluated graph and performs
bounded whole-command reevaluation. Therefore an update winner supplies the new preimage, while a
delete winner supplies the missing outcome; a stale preimage is never persisted or released.
Idempotent replay returns the already-persisted consumed or missing outcome without another read or
delete attempt.

All returned, required, and event-bound preimage fields are compiler-declared access dependencies.
Complete-record use remains explicit and schema-bounded; forward-compatible unknown stored fields
remain in the mutation's prior-image evidence but do not become public output. Secret fields require
an explicit `reveals` declaration and independent release-time field authority—delete permission
does not grant reveal permission. Sets, a second alias for the target, multiple ordinary deletes,
`restrict`, `cascade`, and caller-selected targets receive source diagnostics. Those broader
deletion policies remain available only through the compiler-bounded collection forms below.

Generated Rust, Go, TypeScript, and Python clients expose the same closed consumed/missing outcome
union and publish exact symbolic metadata for every secret outcome field. Generated MCP command
schemas carry that same value-free metadata. TypeScript also generates a structured-redaction
helper, while Rust, Go, and Python redact the default diagnostic representation. MCP never gains a
raw delete or transaction method.
CLI and transport uncertainty recovery retain the original idempotency identity, so a response lost
after commit resolves to the persisted outcome rather than executing another delete.

The value-free external acceptance receipt at
`fixtures/unary-delete-preimage/external-consumer-receipt-v1.json` binds the generic consume plans
and generated artifacts to a Rust, Go, TypeScript, and Python loopback over one real TLS service.
The harness creates consumer workspaces outside the repository; no framework schema, adapter,
route, generated profile, or authentication policy is retained by RiffDB.

## Compiler-bounded collection commands

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

Set-null, orphaning, cross-partition deletion, physical history removal, nested loops, and
caller-supplied callbacks are not part of this surface. A bounded delete may target an entity
with declared unique keys. RiffDB derives and removes each exact old index entry from the checked
predecessor inside the authoritative write transaction; application input does not supply or
guess a release conflict for a value visible only in that predecessor. Ordered execution is
intentional: a create submitted before the delete still receives the typed unique conflict, while
a create admitted after the committed delete may reuse the released value. `RDB-C044`, the former
feature seal for this audited path, is retired; unsafe deletion policy remains the source-spanned
`RDB-C045` refusal. Ordinary commands support only the single exact-key `no_inbound` form described
above; indexed restrict and cascade deletion remain compiler-bounded collection commands.

## Compiler-bounded one-hop cascade (V13)

An exhaustive `cascade` policy may name direct inbound relationships whose source rows share the
target aggregate, partition derivation, and aggregate conflict key. Every source entity must have
`delete_policy no_inbound`; recursive and multi-level deletion remain unavailable.

```riff
entity User {
    key (organization_id: uuid, user_id: uuid)
    delete_policy cascade {
        relationship Account.account_user using Account.by_user maximum 32
        relationship Session.session_user using Session.by_user maximum 32
    }
}

bulk command DeleteUsers {
    input request_id: uuid
    input organization_id: uuid
    input user_ids: list<uuid, 1..3>
    idempotency_key request_id

    for user_id in user_ids {
        delete User(organization_id, user_id) as user
            else UserMissing {}
            cascade CascadeLimitExceeded {}
    }

    return UserDeleted {}
}
```

The compiler requires every direct inbound relationship exactly once, an exact canonical reverse
index prefix, a positive maximum, at most 32 entries, and at most one cascade delete template per
command. It proves `root maximum * (1 + sum(relationship maxima)) <= 256` and charges the complete
possible predecessor graph against 16 MiB. Callers provide only root keys and ordinary inputs;
they cannot choose relationships, indexes, maxima, child keys, or traversal depth.

Cascade contracts emit the least-sufficient bundle, grammar, and executable IR identity V13.
Contracts without cascade continue to emit V1 through V12 as appropriate. This compiler package
is deployable by a V13-capable runtime. After acquiring the command's canonical aggregate conflict,
the coordinator reads each named reverse-index prefix only through its compiler-declared
`maximum + 1` bound. If any prefix returns the extra row, it durably records the declared cascade
outcome with no entity mutation. The root and every present candidate in that bounded
maximum-plus-one set are nevertheless materialized and checked against the transaction-current
capability revision and row policy before the outcome is released. An unauthorized caller therefore
cannot distinguish an absent, within-limit, or over-limit protected relationship by observing the
cascade result. Otherwise the runtime rereads the roots, discovered predecessors, and range epochs
in one consistent view and evaluates one children-before-parent graph.

The persisted entity mutations remain canonically ordered in the existing command envelope. This
is a physical encoding detail: the validated semantic graph proves every child precedes its parent
before the commit coordinator derives the ordinary tombstones, index removals, provenance,
changelog, and commit records. Crash recovery, idempotent replay, backup/restore, and follower
application therefore use the same paths as any other bounded collection delete.

Duplicate collection keys reject the whole command. All expanded effects, the typed outcome,
events, provenance, idempotency record, and commit record are one atomic command result. A crash
or retry can expose only the complete persisted result or complete absence; an element-level
partial result is not part of the protocol.

External adapters can exercise this surface through generated Rust, Go, TypeScript, and Python
clients while retaining their own identity, session, and application models. RiffDB supplies only
the generic compiled data command, authorization, and durability guarantees.

## Correlated aggregate element bytes (V16)

When a list's individual element maximum multiplied by its cardinality would reject a useful
atomic command, the expanded input may declare one smaller aggregate bound:

```riff
bulk command WritePolicyMutations {
    input request_id: uuid
    input mutations: list<PolicyMutation, 1..100>
        aggregate_bytes <= 900000
    idempotency_key request_id

    for mutation in mutations {
        create PolicyMutation(mutation.organization_id, mutation.mutation_id) as stored
            else Exists {}
        set stored.context = mutation.context
    }

    return Written {}
}
```

The number is compiler-owned, positive, and applies only to the single list expanded by the bulk
command. It is the sum of the complete ADR-0011 canonical documents for the submitted elements;
it is not JSON, Protobuf, compressed, or in-memory size. Every element still obeys its individual
type and byte bounds, and the list still obeys its count bound.

The compiler derives how many times element-variable bytes can appear in the canonical command
input, entity mutations, and events. It proves both the unchanged 1 MiB public request envelope
and `fixed bytes + aggregate bytes * derived copy coefficient <= 16 MiB`. Unsupported flows,
arithmetic overflow, or either excessive result fails at the source clause. The coefficient is
stored in executable IR V16 and cannot be selected by an application or caller.

Generated Rust, Go, TypeScript, and Python methods compute the exact canonical sum before opening
the transport. MCP publishes `x-riffdb-aggregateCanonicalElementBytes` on the list schema. The
service recomputes the sum authoritatively before effects, and the deterministic runtime retains
the complete graph check. No layer splits, truncates, retries a subset, or returns partial element
outcomes.

## Client and CLI preflight

Generated Rust, Go, TypeScript, and Python methods carry the compiler's exact inclusive list
minimum and maximum. They reject an out-of-range collection before invoking the transport. The
generated MCP JSON Schema carries the same `minItems`, `maxItems`, and any compiled aggregate
canonical-element-byte extension. Canonical command encoding
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
