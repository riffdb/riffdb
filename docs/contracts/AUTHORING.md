# Contract and application authoring reference

RiffDB contract and RiffQL source accept `//` line comments. Block comments are
not part of grammar version 1.

The following lowercase words are reserved by the contract lexer and cannot be
used as field or declaration identifiers:

```text
aggregate approval as bool bytes capability child command conflict_key
contract count create date decimal default else emit entity enum event false
field frontier i64 idempotency_key import include index input invariant key
list measure module money mutate null optional partition_by projection query
read reference require return root set source state state_machine string sum
timestamp transactionally_ordered transition true u64 unique uuid version where
```

For example, use `origin`, `origin_label`, or `source_label` instead of the
reserved field name `source`.

Names are unique in their semantic namespace, not globally across an entire
contract. Enum variants belong to their enum, command outcomes belong to their
command, and indexes belong to their owning entity. A duplicate diagnostic
identifies the current declaration and includes the related span of the first
declaration.

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
