# Contract and application authoring reference

RiffDB contract and RiffQL source accept `//` line comments. Block comments are
not part of grammar version 1.

The following lowercase words are reserved by the contract lexer and cannot be
used as field or declaration identifiers:

```text
active aggregate allow approval as attempts binary_utf8_v1 bool bytes capability
child claim command conflict_key contract count create date decimal
default delete duration_seconds else emit entity enum event
exhausted exists expire expired expires_at fact false fence fencing_token field
from frontier i64 idempotency_key illegal import in include index input invalid
invariant is key lease list measure module money mutate not null on optional
owner partition_by policy presence principal projection query read reference
release renew require return revision root row service set source stale
state state_machine string sum text_key timestamp to
transaction_time transactionally_ordered transition true u64 unavailable
unicode_fold_v1 unique update uuid uuid_v7 vector_field version when where workflow
```

For example, use `origin`, `origin_label`, or `source_label` instead of the
reserved field name `source`.

The vector search words `cosine`, `euclidean`, `dot_product`,
`staleness_slo`, `ann_threshold`, and `recall_target_bps` are contextual, not
reserved: they are meaningful only inside a `vector_field` declaration and
remain usable as ordinary field and declaration identifiers everywhere else.
The same is true of `nearest` in RiffQL — a contract field named `nearest`
stays queryable.

Names are unique in their semantic namespace, not globally across an entire
contract. Enum variants belong to their enum, command outcomes belong to their
command, and indexes belong to their owning entity. A duplicate diagnostic
identifies the current declaration and includes the related span of the first
declaration.

## Command declaration order and relationship proofs

Grammar-v1 command clauses follow one visible order: inputs and service values,
the idempotency declaration, every `read`/`mutate`/`create` binding,
requirements, effects such as `set` and `emit`, then `return`. Bind everything
a requirement can observe before writing the requirement:

```riff
command AddComment {
    input request_key: string<128>
    input site_id: uuid
    input post_id: uuid
    input comment_id: uuid
    service created_at: transaction_time
    idempotency_key request_key

    read Post(site_id, post_id) as post else PostMissing {}
    create Comment(site_id, post_id, comment_id) as comment
        else CommentExists {}
    require PublishedPost: post.state == PostState.Published
        else PostNotPublished {}
    set comment.created_at = created_at
    return Created { comment: comment }
}
```

Every required relationship affected by a create or mutation needs a
dominating exact read of that relationship's complete target key. Reading one
aggregate root does not prove a separate author, route, project, or other
target exists. This is intentional: the compiled command proves all reference
integrity in the same transaction instead of relying on a racy application
preflight.

## Operational query indexes

Indexes used by null/existence and text-prefix RiffQL predicates declare their
physical semantics in the contract:

```riff
entity Document {
    key (organization_id: uuid, document_id: uuid)
    field deleted_at: optional<timestamp>
    field title: string<200>

    index by_deleted (organization_id, deleted_at, document_id)
        presence(deleted_at)
    index by_title (organization_id, title, document_id)
        text_key(title, binary_utf8_v1)
}
```

`presence` is valid only for an optional authoritative key scalar and preserves
missing, explicit null, and non-null as distinct index states. `text_key` is
valid only for a bounded string field. `binary_utf8_v1` compares exact UTF-8
bytes; it is case-sensitive and performs no Unicode normalization. The
`unicode_fold_v1` profile is reserved but is not executable in the current
build. Adding or changing either encoding changes durable index identity and
requires the contract migration/rebuild path; RiffDB never silently changes a
text profile during a software upgrade.

An optional field is not an ordinary index key. If a query filters or orders
on an optional field, declare `presence(field)` as shown above so missing,
explicit-null, and present values remain distinct and the compiler can prove
the access shape.

## Explicit covering indexes

A new index identity may declare bounded authoritative fields that are stored
atomically with each index entry:

```riff
index by_board_project_status
    (organization_id, project_id, status, ticket_id)
    cover (title, reporter_id, assignee_id)
```

The compiler may use this index for a named query only when its key plus cover
contains every predicate, ordering, and selected field and the complete result
has one statically bounded positional layout. The application still names only
the query; it cannot select the index, request a partial cover, provide field
ordinals, or disable validation. Covered bytes are derived from the same
authoritative post-image as the index key and commit atomically with it.

`cover (...)` is intentionally explicit and ordered. Adding it to an existing
index is not an in-place optimization: add a new index identity and use the
normal migration/rebuild path. Missing, stale, malformed, duplicate, or
non-derivable covered values fail closed; the executor never falls back to
entity reads after selecting a covered plan. Unbounded, secret, unsupported,
or policy-incompatible layouts remain on the ordinary checked result path.

## Vector fields (alpha)

An entity may declare one or more fixed-dimension embedding fields:

```riff
entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: string<65536>
    vector_field embedding(1536, cosine, (title, body), staleness_slo 60,
        model "text-embedding-3-small", current_version "2026-08-21",
        replay_age_seconds 86400, replay_bytes 1073741824,
        replay_backlog 100000,
        ann_threshold 256, recall_target_bps 9500)
}
```

The declaration carries the search configuration into the compiled contract
bundle, where it is part of the contract's durable identity:

- The dimension is a positive integer of at most 4,096.
- The metric is exactly one of `cosine`, `euclidean`, or `dot_product`.
- The source fields name 1 to 1,024 distinct existing fields on the same
  entity — the fields whose edits make a stored embedding stale. A repeated
  name or the vector field itself is a compile error at its span.
- `staleness_slo` is the positive v1 stale-entity count threshold. A staleness
  observer breaches the SLO only when `stale_count > staleness_slo`; equality
  does not breach it. Duration-based or clock-based staleness is a named future
  amendment, not a v1 interpretation of this declaration.
- The production clause is atomic: `model`, `current_version`,
  `replay_age_seconds`, `replay_bytes`, and `replay_backlog` must all appear in
  that order or all be absent. Model strings are nonempty and at most 256
  bytes. Replay limits are positive and compiler-bounded (one year, one TiB,
  and 100,000,000 sequences respectively). They are projection retention
  ceilings, not request parameters.
- Omitting the production clause preserves legacy exact-vector bundle bytes,
  but that field cannot back a production `nearest` query. A production clause
  selects contract IR V15; earlier vector and ANN records remain byte-frozen.
- The optional ANN clause is atomic: `ann_threshold` and
  `recall_target_bps` must appear together in that order. The threshold is
  1 to 65,536 rows per organization. Search remains exact at or below the
  threshold and engages the approximate graph only above it.
- `recall_target_bps` is 1 to 10,000 basis points. For example, `9500`
  declares recall@K of at least 0.95 against exact scan at the same projection
  frontier. Application query inputs cannot lower or omit this projection-owned
  target.

The approximate graph is derived, bounded by the query's organization
partition and scan budget, and rebuilt deterministically from the visible
snapshot. Graph entry points and links are computed only after scalar filters
and principal-policy admission, so another organization or a denied row cannot
shape traversal or reported graph statistics. Exact KNN remains the reference
path.

Production vector fields are written only through the compiler-sealed `embed`
effect; generic `set` is rejected at the target field:

```riff
command SetDocumentEmbedding {
    input request_id: string<128>
    input org_id: uuid
    input doc_id: uuid
    input embedding: vector<1536>
    input submitted_model: string<256>
    input submitted_version: string<256>

    idempotency_key request_id
    mutate Document(org_id, doc_id) as document else Missing {}

    embed document.embedding = embedding
        from (submitted_model, submitted_version)

    return Embedded { document: document }
}
```

The vector dimension is part of the input type. Model identity and version are
distinct direct command inputs and must exactly match the field's compiled
production declaration; a mismatch is rejected before admission with the
symbolic input path. The entity post-image and its authoritative embedding
evidence (model identity, model version, and commit sequence) are derived from
the same checked command and persist atomically with its outcome, provenance,
and idempotency record. An application cannot submit evidence separately or
use generic field mutation to bypass it.

Typed vector values cross the native, Protobuf, gRPC, hosted MCP, CLI, and
generated Rust, Go, TypeScript, and Python conversion boundaries with
finite-component and exact-dimension checks. Each generated embedding command
also has a field-specific constructor that fills the contract-sealed model
identity and version; generated constants and accessors keep that evidence
inspectable without asking application code to duplicate it. Direct input
construction remains checked by the same server authority. The production
staleness enumeration/health observer and `nearest()` storage path are still
absent — see [Known Limitations](../known-limitations.md).

## Revision-checked workflow transitions

A workflow declaration closes the legal state graph for one aggregate-owned
entity. A transition effect must name a mutable binding and an exact revision
supplied by the caller:

```riff
workflow WorkLifecycle {
    entity WorkItem
    state state
    transition Start from (Queued) to Running
}

command StartWork {
    input request_key: string<128>
    input organization_id: uuid
    input work_id: uuid
    input expected_revision: u64
    idempotency_key request_key

    mutate WorkItem(organization_id, work_id) as work else Missing {}
    transition Start on work revision expected_revision
        stale StaleRevision {}
        illegal IllegalState {}
    return Started { work: work }
}
```

RiffDB compares `expected_revision` with the observed entity revision before it
checks the source state. A mismatch returns the declared `StaleRevision`
outcome; a matching revision in any state outside `Start`'s source set returns
`IllegalState`. Both are ordinary idempotent zero-mutation command outcomes.
The legal path assigns the declared destination and retains the exact entity
version as a transaction-current read dependency, so a concurrent replacement
cannot turn the checked result into a lost update at commit.

There is no unconditional workflow update, automatic substitution of a newer
revision, generic compare-and-swap, or caller-supplied transition name. The
current executable surface covers revision-checked declared transitions plus
sealed service-owned values:

```riff
command StartWork {
    input organization_id: uuid
    input work_id: uuid
    input expected_revision: u64
    service execution_id: uuid_v7
    service started_at: transaction_time
    idempotency_key request_key

    mutate WorkItem(organization_id, work_id) as work else Missing {}
    transition Start on work revision expected_revision
        stale StaleRevision {}
        illegal IllegalState {}
    set work.execution_id = execution_id
    set work.started_at = started_at
    return Started { work: work }
}
```

`transaction_time` is the logical time sampled for durable admission;
`uuid_v7` is generated by the service before deterministic evaluation. Neither
value is a caller input, and the runtime receives the values without clock,
randomness, filesystem, or network authority. RiffDB stores the canonical
service-value record with pending and terminal command evidence and retains it
inside the authoritative command segment. A retry, uncertain-outcome recovery,
or restart therefore returns and reconstructs the originally sealed values
rather than sampling replacements. Service values consequently require a
durably idempotent mutation command; unjournaled read-only commands cannot
declare them.

Lease declarations remain compiler-visible foundations only. Generated
claim/renew/release/expire operations are not yet public alpha features.

## CLI application JSON

The application CLI accepts inline JSON, `@path`, a legacy bare path, or `-`
for stdin:

```bash
riffdb command run CreateItem --input \
  '{"item_id":"01900000-0000-7000-8000-000000000001","priority":2,"mode":"Linear"}'

riffdb command run CreateItem --input @fixtures/item.json
riffdb command run CreateItem --input fixtures/item.json
riffdb command run CreateItem --input - < fixtures/item.json
```

Natural schema-directed values are:

| Contract type | JSON |
|---|---|
| `bool` | `true` |
| `i64`, `u64` | `2` |
| `uuid` | `"01900000-0000-7000-8000-000000000001"` |
| enum | `"Linear"` |
| bounded `string` | `"text"` |
| optional absence | `null` |
| list | `[...]` |

The explicit `$i64`, `$u64`, `$uuid`, `$enum`, `$decimal`, `$money`, `$bytes`,
`$date`, and `$timestamp` objects remain compatibility forms for lossless
transport or fixture work. Public type failures name the operation and symbolic
path when that context is available. They never release numeric field IDs
through the application error envelope; numeric IDs remain confined to the
explicitly low-level kernel interface.

## Successor contract evolution

Every changed deployment must declare a contract version greater than the
active version and must compare against the exact expected active version.
Version numbers alone do not establish compatibility; the compiler compares
the checked successor with the active bundle.

For application source trees, `application lock --write` obtains that exact
parent through the authorized read-only candidate preview. The resulting lock
pins both the parent identity and `generated/riffdb.contract.bundle`. Never
manufacture a successor lock with an offline genesis compile or copy a hash
from an error.

The pre-alpha compatibility classes are:

| Successor change | Classification |
|---|---|
| Add an enum with its complete initial variants | Compatible |
| Add an entity with its complete initial key, fields, indexes, and local invariants | Compatible |
| Add an aggregate whose root and children are all new in this successor | Compatible |
| Add a relationship or unique constraint confined to new entities | Compatible |
| Append a fresh variant to an existing enum | Requires explicit version |
| Add an optional field to an existing entity | Compatible |
| Add a required field, index, invariant, relationship, or unique constraint to an existing entity | Requires migration |
| Add a projection over existing authoritative state | Requires migration |
| Change or remove an existing declaration, type, key, variant, constraint, aggregate membership, partition rule, or conflict rule | Incompatible |

The complete initial definition of a newly added entity or aggregate is
treated as one addition. A new aggregate cannot take ownership of an existing
entity. These restrictions ensure that a compatible successor does not scan,
rewrite, or reinterpret an existing durable row.

Use the application workflow when deploying a locked source tree:

```bash
riffdb application check
riffdb application lock --write
riffdb application generate --locked
riffdb application deploy
```

For a direct contract deployment, validate before deploying and supply the
active version expected by that CLI version:

```bash
riffdb --config "$HOME/.config/riffdb/client-ea.toml" \
  contract validate riffdb/contract.riff
riffdb --config "$HOME/.config/riffdb/client-ea.toml" \
  contract deploy --expected-version 3 riffdb/contract.riff
```

An invalid source returns bounded syntax or semantic diagnostics. A checked
but incompatible successor returns `incompatible_candidate` with its parent,
overall compatibility class, and stable compatibility-code counts. Neither
result activates the candidate. Do not work around incompatibility by
reusing stable IDs or editing the active database file.

A valid successor that needs existing state to be checked or transformed
returns `migration_required`. Use Application Source V3, a parent-specific
`.riffm` proof, Application Lock V4, and the read-only plan described in
[Contract Migrations](MIGRATIONS.md). Internal redb execution and crash recovery
are implemented, but no supported public check/apply/status interface can
activate that successor until WP-409.

`contract deploy` is a mutating operator command, not a compatibility probe.
There is no contract rollback RPC. Use `contract validate` for source-only
validation or `application lock --write` for an exact read-only successor
preview, then review the lock before `application deploy`. If a direct deploy
activates an unwanted successor, stop the service and follow the documented
offline backup/restore or disposable pre-alpha reset procedure; do not deploy
another probe hoping to undo it.
