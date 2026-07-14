# RiffDB Contract IR Format v1

Status: **Accepted**

This generated review artifact is derived from the production tag and ordered-layout registries. It does not accept or freeze the durable format.

## Scalar Framing

- Unsigned integers are big-endian `u8`, `u32`, or `u64`; signed integers are two's-complement big-endian.
- Boolean is `0x00` or `0x01`.
- Bytes and UTF-8 strings are `u32 byte_length || exact_bytes`.
- Optional values are `Boolean present || payload when present`.
- Collections are `u32 count || elements`; checked decoders validate bounds before allocation.
- Stable semantic IDs are nonzero. `ExprId`, `BindingId`, and `RootValidationReadId` are dense zero-based plan-local `u32` values.
- Full input consumption and canonical re-encoding are mandatory.

### Canonical Value Reference

Expression constants use exactly `u32 canonical_document_byte_length || canonical_document_bytes`. `canonical_document_bytes` is one complete [ADR-0011 canonical value encoding v1](../../adr/0011-canonical-values-keys-and-hashing.md#canonical-value-encoding-v1), owned by `riffdb-types::encode_canonical_value`: its first byte is `riffdb_types::CANONICAL_VALUE_VERSION` (`0x01`), its second byte is the ADR-0011 value tag, and recursive list/record children are complete version-and-tag-prefixed documents. The outer IR length is not part of the inner canonical document. Empty, truncated, trailing, unsupported-version, and unknown-tag documents reject.

## Closed Tags

### Stable ID namespace

| Tag | Variant |
|---:|---|
| `0x01` | entity |
| `0x02` | event |
| `0x03` | enum |
| `0x04` | aggregate |
| `0x05` | command |
| `0x06` | projection |
| `0x07` | index |
| `0x08` | invariant |
| `0x09` | field |
| `0x0a` | outcome |
| `0x0b` | enum variant |

### Lineage entry state

| Tag | Variant |
|---:|---|
| `0x01` | active |
| `0x02` | tombstone |

### Record reference

| Tag | Variant |
|---:|---|
| `0x01` | entity |
| `0x02` | event |
| `0x03` | command input |
| `0x04` | command outcome |
| `0x05` | projection result |

### Value type

| Tag | Variant |
|---:|---|
| `0x01` | bool |
| `0x02` | i64 |
| `0x03` | u64 |
| `0x04` | decimal |
| `0x05` | money |
| `0x06` | string |
| `0x07` | bytes |
| `0x08` | timestamp |
| `0x09` | date |
| `0x0a` | uuid |
| `0x0b` | enum |
| `0x0c` | optional |
| `0x0d` | list |
| `0x0e` | record |

### Expression

| Tag | Variant |
|---:|---|
| `0x01` | constant |
| `0x02` | input field |
| `0x03` | complete binding |
| `0x04` | bound field |
| `0x05` | schema field |
| `0x06` | source-event field |
| `0x07` | tx.time |
| `0x08` | tx.date |
| `0x09` | unary |
| `0x0a` | binary |
| `0x0b` | root-validation field |

### Unary operator

| Tag | Variant |
|---:|---|
| `0x01` | not |
| `0x02` | negate |

### Binary operator

| Tag | Variant |
|---:|---|
| `0x01` | multiply |
| `0x02` | divide |
| `0x03` | add |
| `0x04` | subtract |
| `0x05` | equal |
| `0x06` | not-equal |
| `0x07` | less |
| `0x08` | less-equal |
| `0x09` | greater |
| `0x0a` | greater-equal |
| `0x0b` | and |
| `0x0c` | or |

### Key purpose

| Tag | Variant |
|---:|---|
| `0x01` | entity |
| `0x02` | partition |
| `0x03` | conflict |
| `0x04` | index |

### Binding mode

| Tag | Variant |
|---:|---|
| `0x01` | read |
| `0x02` | mutate |
| `0x03` | create |

### Instruction

| Tag | Variant |
|---:|---|
| `0x01` | require |
| `0x02` | set field |
| `0x03` | emit event |
| `0x04` | return |

### Execution class

| Tag | Variant |
|---:|---|
| `0x01` | read-only |
| `0x02` | idempotent mutation |

### Retry policy

| Tag | Variant |
|---:|---|
| `0x01` | bounded full reevaluation |

### Capability requirement

| Tag | Variant |
|---:|---|
| `0x01` | invoke command |

### Projection aggregation

| Tag | Variant |
|---:|---|
| `0x01` | count |
| `0x02` | sum |

### Projection frontier

| Tag | Variant |
|---:|---|
| `0x01` | transactionally ordered |

### Compatibility class

| Tag | Variant |
|---:|---|
| `0x01` | compatible |
| `0x02` | explicit version |
| `0x03` | incompatible |

### Schema artifact

| Tag | Variant |
|---:|---|
| `0x01` | entity record |
| `0x02` | durable event payload |
| `0x03` | command input |
| `0x04` | command outcome union |
| `0x05` | projection result row |

### Record allocation owner

| Tag | Variant |
|---:|---|
| `0x01` | entity |
| `0x02` | event |
| `0x03` | command input |
| `0x04` | command outcome |
| `0x05` | projection result |

### Invariant identity owner

| Tag | Variant |
|---:|---|
| `0x01` | entity |
| `0x02` | aggregate |

### Index identity owner

| Tag | Variant |
|---:|---|
| `0x01` | entity |

### Outcome allocation owner

| Tag | Variant |
|---:|---|
| `0x01` | command |

### Enum-variant allocation owner

| Tag | Variant |
|---:|---|
| `0x01` | enum |

## Exact Tagged Variant Payloads

Each row lists all bytes immediately following the tag, in byte order. `empty` means the tag has no payload bytes.

### RecordTypeRef

| Tag | Variant | Ordered payload after tag |
|---:|---|---|
| `0x01` | entity | `entity_type`: EntityTypeId as u32 |
| `0x02` | event | `event_type`: EventTypeId as u32 |
| `0x03` | command input | `command_id`: CommandId as u32 |
| `0x04` | command outcome | `command_id`: CommandId as u32; `outcome_id`: OutcomeId as u32 |
| `0x05` | projection result | `projection_id`: ProjectionId as u32 |

### ValueType

| Tag | Variant | Ordered payload after tag |
|---:|---|---|
| `0x01` | bool | empty |
| `0x02` | i64 | empty |
| `0x03` | u64 | empty |
| `0x04` | decimal | `precision`: u8 in 1..=38; `scale`: u8 in 0..=precision |
| `0x05` | money | `currency`: exactly 3 uppercase ASCII bytes |
| `0x06` | string | `maximum_utf8_bytes`: u32 |
| `0x07` | bytes | `maximum_bytes`: u32 |
| `0x08` | timestamp | empty |
| `0x09` | date | empty |
| `0x0a` | uuid | empty |
| `0x0b` | enum | `enum_type`: EnumTypeId as u32 |
| `0x0c` | optional | `inner_type`: recursive ValueType |
| `0x0d` | list | `element_type`: recursive ValueType; `maximum_entries`: u32 |
| `0x0e` | record | `record_type`: RecordTypeRef tag plus exact selected payload |

### KeyPurpose

| Tag | Variant | Ordered payload after tag |
|---:|---|---|
| `0x01` | entity | `entity_type`: EntityTypeId as u32 |
| `0x02` | partition | `aggregate_id`: AggregateTypeId as u32 |
| `0x03` | conflict | `aggregate_id`: AggregateTypeId as u32 |
| `0x04` | index | `index_id`: IndexId as u32; `entity_type`: EntityTypeId as u32 |

### TypedExpression

| Tag | Variant | Ordered payload after tag |
|---:|---|---|
| `0x01` | constant | `result_type`: ValueType tag plus exact selected payload; `canonical_value`: u32 byte length + one complete ADR-0011 canonical-value v1 document |
| `0x02` | input field | `result_type`: ValueType tag plus exact selected payload; `field`: FieldId as u32 |
| `0x03` | complete binding | `result_type`: ValueType tag plus exact selected payload; `binding`: BindingId as u32 |
| `0x04` | bound field | `result_type`: ValueType tag plus exact selected payload; `binding`: BindingId as u32; `field`: FieldId as u32 |
| `0x05` | schema field | `result_type`: ValueType tag plus exact selected payload; `entity_type`: EntityTypeId as u32; `field`: FieldId as u32 |
| `0x06` | source-event field | `result_type`: ValueType tag plus exact selected payload; `field`: FieldId as u32 |
| `0x07` | tx.time | `result_type`: ValueType tag plus exact selected payload |
| `0x08` | tx.date | `result_type`: ValueType tag plus exact selected payload |
| `0x09` | unary | `result_type`: ValueType tag plus exact selected payload; `operator`: Unary operator tag as u8; `operand`: ExprId as u32 |
| `0x0a` | binary | `result_type`: ValueType tag plus exact selected payload; `operator`: Binary operator tag as u8; `left`: ExprId as u32; `right`: ExprId as u32 |
| `0x0b` | root-validation field | `result_type`: ValueType tag plus exact selected payload; `read`: RootValidationReadId as u32; `field`: FieldId as u32 |

### Instruction

| Tag | Variant | Ordered payload after tag |
|---:|---|---|
| `0x01` | require | `requirement_index`: u32; `predicate`: ExprId as u32; `reject`: OutcomeConstruction |
| `0x02` | set field | `binding`: BindingId as u32; `field`: FieldId as u32; `value`: ExprId as u32 |
| `0x03` | emit event | `event`: EventConstruction |
| `0x04` | return | `outcome`: OutcomeConstruction |

### CapabilityRequirement

| Tag | Variant | Ordered payload after tag |
|---:|---|---|
| `0x01` | invoke command | `lineage`: string; `command_id`: CommandId as u32 |

### ProjectionAggregation

| Tag | Variant | Ordered payload after tag |
|---:|---|---|
| `0x01` | count | `expression_present`: Boolean = 0x00 |
| `0x02` | sum | `expression_present`: Boolean = 0x01; `expression`: ExprId as u32 |

### SchemaArtifactKey

| Tag | Variant | Ordered payload after tag |
|---:|---|---|
| `0x01` | entity record | `entity_type`: EntityTypeId as u32 |
| `0x02` | durable event payload | `event_type`: EventTypeId as u32 |
| `0x03` | command input | `command_id`: CommandId as u32 |
| `0x04` | command outcome union | `command_id`: CommandId as u32 |
| `0x05` | projection result row | `projection_id`: ProjectionId as u32 |

## Lineage Owner Matrix

Owner kind `0x00` and count `0` encode no owner. Index and invariant allocation states are global even though their identity keys carry the contextual owner shown. Scoped field, outcome, and enum-variant allocation paths equal their identity owner paths.

| Namespace | Allocation owner kind/count | Identity owner | Identity owner kind/count |
|---|---:|---|---:|
| `0x01` entity | `0x00` / 0 | none | `0x00` / 0 |
| `0x02` event | `0x00` / 0 | none | `0x00` / 0 |
| `0x03` enum | `0x00` / 0 | none | `0x00` / 0 |
| `0x04` aggregate | `0x00` / 0 | none | `0x00` / 0 |
| `0x05` command | `0x00` / 0 | none | `0x00` / 0 |
| `0x06` projection | `0x00` / 0 | none | `0x00` / 0 |
| `0x07` index | `0x00` / 0 | entity | `0x01` / 1 |
| `0x08` invariant | `0x00` / 0 | entity | `0x01` / 1 |
| `0x08` invariant | `0x00` / 0 | aggregate | `0x02` / 1 |
| `0x09` field | `0x01` / 1 | entity | `0x01` / 1 |
| `0x09` field | `0x02` / 1 | event | `0x02` / 1 |
| `0x09` field | `0x03` / 1 | command input | `0x03` / 1 |
| `0x09` field | `0x04` / 2 | command outcome | `0x04` / 2 |
| `0x09` field | `0x05` / 1 | projection result | `0x05` / 1 |
| `0x0a` outcome | `0x01` / 1 | command | `0x01` / 1 |
| `0x0b` enum variant | `0x01` / 1 | enum | `0x01` / 1 |

## Compatibility Code Registry

Each compatibility entry encodes its exact eight-byte ASCII code through normal string framing, followed by the affected-path string. The code fixes the entry class; the report overall is the maximum class.

| Code | Required class tag | Meaning |
|---|---:|---|
| `RDB-K001` | `0x01` | no semantic change |
| `RDB-K010` | `0x01` | added command |
| `RDB-K011` | `0x01` | added event |
| `RDB-K012` | `0x01` | added projection |
| `RDB-K013` | `0x01` | added optional field |
| `RDB-K020` | `0x02` | added outcome |
| `RDB-K021` | `0x02` | added optional outcome field |
| `RDB-K100` | `0x03` | removed identity |
| `RDB-K101` | `0x03` | tombstone resurrection |
| `RDB-K102` | `0x03` | stable ID reuse |
| `RDB-K103` | `0x03` | type change |
| `RDB-K104` | `0x03` | key-layout change |
| `RDB-K105` | `0x03` | idempotency change |
| `RDB-K106` | `0x03` | partition/conflict change |
| `RDB-K107` | `0x03` | invariant change |
| `RDB-K108` | `0x03` | outcome change |
| `RDB-K109` | `0x03` | event change |
| `RDB-K110` | `0x03` | existing plan change |
| `RDB-K111` | `0x03` | unsupported addition |
| `RDB-K112` | `0x03` | executable IR version change |

## Stable Affected-Path Grammar and Order

Every path is either `contract` or starts with one root stable ID: `aggregate:<id>`, `command:<id>`, `entity:<id>`, `enum:<id>`, `event:<id>`, or `projection:<id>`. Allowed descendants are `aggregate:<id>/invariant:<id>`; `entity:<id>/{field|index|invariant}:<id>`; `enum:<id>/variant:<id>`; `{event|projection}:<id>/field:<id>`; `command:<id>/input/field:<id>`; and `command:<id>/outcome:<id>[/field:<id>]`. Every `<id>` is a one-based `u32` in shortest decimal spelling, with no leading zero. No other root, literal, descendant, or empty segment is valid.

Compatibility entries order first by the exact eight-byte code, then by structured affected path. Path segments compare their listed ASCII kind/literal, then numeric stable ID; a path prefix precedes its descendants. Numeric comparison therefore places ID 2 before IDs 10 and 11 at every nesting level. Duplicate code/path pairs reject.

## Ordered Nested Layouts

Fields below are listed in exact byte order. A collection field includes its count immediately before its listed elements.

### ContractBundle

| # | Field | Encoding |
|---:|---|---|
| 1 | `magic` | ASCII `RIFFDB-BUNDLE\0` |
| 2 | `bundle_format_version` | u32 = 1 |
| 3 | `grammar_version` | u32 = 1 |
| 4 | `executable_ir_version` | u32 = 1 |
| 5 | `compiler_version` | nonempty ASCII compiler semantic-version identity string, <=64 bytes |
| 6 | `contract_lineage` | string |
| 7 | `contract_version` | u64 |
| 8 | `parent` | optional ParentBundleRef |
| 9 | `source_hash` | 32 bytes |
| 10 | `plan_root_hash` | 32 bytes |
| 11 | `ledger` | LineageLedgerV1 |
| 12 | `schema` | StructuralSchema |
| 13 | `commands` | u32 count + CommandBundleEntry[] |
| 14 | `projections` | u32 count + ProjectionBundleEntry[] |
| 15 | `schema_artifacts` | u32 count + GeneratedSchemaArtifact[] |
| 16 | `mcp_names` | McpCommandNameRegistryV1 |
| 17 | `compatibility` | CompatibilityReport |

### ParentBundleRef

| # | Field | Encoding |
|---:|---|---|
| 1 | `contract_version` | u64 |
| 2 | `bundle_hash` | 32 bytes |

### LineageLedgerV1

| # | Field | Encoding |
|---:|---|---|
| 1 | `version` | u32 = 1 |
| 2 | `allocations` | u32 count + LineageAllocation[] |

### LineageAllocation

| # | Field | Encoding |
|---:|---|---|
| 1 | `namespace_tag` | Stable ID namespace tag |
| 2 | `owner_kind` | contextual owner tag |
| 3 | `owner_ids` | u8 count + u32[] |
| 4 | `max_allocated` | u32 |
| 5 | `entries` | u32 count + LineageEntry[] |

### LineageEntry

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | u32 |
| 2 | `identity_owner_kind` | contextual owner tag |
| 3 | `identity_owner_ids` | u8 count + u32[] |
| 4 | `name` | string |
| 5 | `state` | Lineage entry state tag |

### StructuralSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `entities` | u32 count + EntitySchema[] |
| 2 | `events` | u32 count + EventSchema[] |
| 3 | `enums` | u32 count + EnumSchema[] |
| 4 | `aggregates` | u32 count + AggregateSchema[] |

### EntitySchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | u32 |
| 2 | `name` | string |
| 3 | `record` | RecordSchema |
| 4 | `primary_key_fields` | u32 count + FieldId[] |
| 5 | `primary_key` | KeySchema |
| 6 | `invariants` | u32 count + InvariantPlan[] |
| 7 | `indexes` | u32 count + IndexSchema[] |

### EventSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | u32 |
| 2 | `name` | string |
| 3 | `payload` | RecordSchema |

### EnumSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | u32 |
| 2 | `name` | string |
| 3 | `variants` | u32 count + EnumVariantSchema[] |

### EnumVariantSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | u32 |
| 2 | `name` | string |

### AggregateSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | u32 |
| 2 | `name` | string |
| 3 | `root` | EntityTypeId |
| 4 | `children` | u32 count + EntityTypeId[] |
| 5 | `keys` | AggregateKeyPlan |
| 6 | `invariants` | u32 count + InvariantPlan[] |

### AggregateKeyPlan

| # | Field | Encoding |
|---:|---|---|
| 1 | `expressions` | ExpressionArena |
| 2 | `partition_expression` | ExprId |
| 3 | `conflict_expressions` | u32 count + ExprId[] |
| 4 | `partition_schema` | KeySchema |
| 5 | `conflict_schema` | KeySchema |

### InvariantPlan

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | InvariantId |
| 2 | `name` | string |
| 3 | `expressions` | ExpressionArena |
| 4 | `predicate` | ExprId |

### IndexSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | IndexId |
| 2 | `name` | string |
| 3 | `fields` | u32 count + FieldId[] |
| 4 | `key_schema` | KeySchema |

### RecordSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `owner` | RecordTypeRef |
| 2 | `fields` | u32 count + FieldSchema[] |

### FieldSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | FieldId |
| 2 | `name` | string |
| 3 | `value_type` | ValueType |

### KeySchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `codec_version` | u32 = 1 |
| 2 | `purpose` | KeyPurpose tag plus exact selected payload |
| 3 | `components` | u32 count + KeyComponentSchema[] |
| 4 | `maximum_encoded_bytes` | u32 |
| 5 | `entity_key_schema` | optional nested KeySchema |

### KeyComponentSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `value_type` | ValueType |
| 2 | `enum_variants` | u32 count + EnumVariantId[] |
| 3 | `maximum_payload_bytes` | u32 |

### ExpressionArena

| # | Field | Encoding |
|---:|---|---|
| 1 | `nodes` | u32 count + TypedExpression[] |

### CommandBundleEntry

| # | Field | Encoding |
|---:|---|---|
| 1 | `command_id` | CommandId |
| 2 | `name` | string |
| 3 | `contract_version` | u64 |
| 4 | `plan_hash` | 32 bytes |
| 5 | `semantics` | CommandSemantics with display names |

### CommandSemantics

| # | Field | Encoding |
|---:|---|---|
| 1 | `input` | RecordSchema |
| 2 | `outcomes` | u32 count + OutcomeSchema[] |
| 3 | `success_outcome` | OutcomeId |
| 4 | `idempotency_input` | optional FieldId |
| 5 | `input_schema_hash` | 32 bytes |
| 6 | `output_schema_hash` | 32 bytes |
| 7 | `expressions` | ExpressionArena |
| 8 | `bindings` | u32 count + BindingPlan[] |
| 9 | `root_validation_reads` | u32 count + RootValidationReadPlan[] |
| 10 | `locality` | LocalityPlan |
| 11 | `commit_checks` | u32 count + CommitCheckPlan[] |
| 12 | `instructions` | u32 count + Instruction[] |
| 13 | `execution_class` | Execution class tag |
| 14 | `retry_policy` | Retry policy tag |
| 15 | `required_capability` | CapabilityRequirement tag plus exact selected payload |
| 16 | `entity_closure` | u32 count + EntitySchema[] |
| 17 | `aggregate_closure` | AggregateSchema |
| 18 | `event_closure` | u32 count + EventSchema[] |

### OutcomeSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | OutcomeId |
| 2 | `name` | string |
| 3 | `payload` | RecordSchema |

### BindingPlan

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | BindingId |
| 2 | `name` | string |
| 3 | `mode` | Binding mode tag |
| 4 | `entity_type` | EntityTypeId |
| 5 | `key_schema` | KeySchema |
| 6 | `key_expressions` | u32 count + ExprId[] |
| 7 | `accessed_fields` | u32 count + FieldId[] |
| 8 | `complete_record_access` | Boolean |
| 9 | `failure` | OutcomeConstruction |

### RootValidationReadPlan

| # | Field | Encoding |
|---:|---|---|
| 1 | `id` | RootValidationReadId |
| 2 | `source_binding` | BindingId |
| 3 | `root_entity` | EntityTypeId |
| 4 | `key_schema` | KeySchema |
| 5 | `key_expressions` | u32 count + ExprId[] |
| 6 | `accessed_fields` | u32 count + FieldId[] |

### LocalityPlan

| # | Field | Encoding |
|---:|---|---|
| 1 | `aggregate_id` | AggregateTypeId |
| 2 | `partition_schema` | KeySchema |
| 3 | `partition_expression` | ExprId |
| 4 | `conflict_derivations` | u32 count + ConflictDerivationPlan[] |

### ConflictDerivationPlan

| # | Field | Encoding |
|---:|---|---|
| 1 | `schema` | KeySchema |
| 2 | `expressions` | u32 count + ExprId[] |

### CommitCheckPlan

| # | Field | Encoding |
|---:|---|---|
| 1 | `invariant_id` | InvariantId |
| 2 | `predicate` | ExprId |
| 3 | `source_bindings` | u32 count + BindingId[] |
| 4 | `root_validation_reads` | u32 count + RootValidationReadId[] |

### ObjectConstruction

| # | Field | Encoding |
|---:|---|---|
| 1 | `record` | RecordTypeRef |
| 2 | `fields` | u32 count + (FieldId, ExprId)[] |

### OutcomeConstruction

| # | Field | Encoding |
|---:|---|---|
| 1 | `outcome_id` | OutcomeId |
| 2 | `payload` | ObjectConstruction |

### EventConstruction

| # | Field | Encoding |
|---:|---|---|
| 1 | `event_type` | EventTypeId |
| 2 | `payload` | ObjectConstruction |

### ProjectionBundleEntry

| # | Field | Encoding |
|---:|---|---|
| 1 | `projection_id` | ProjectionId |
| 2 | `name` | string |
| 3 | `plan_hash` | 32 bytes |
| 4 | `semantics` | ProjectionSemantics |

### ProjectionSemantics

| # | Field | Encoding |
|---:|---|---|
| 1 | `source_event` | EventTypeId |
| 2 | `expressions` | ExpressionArena |
| 3 | `filter` | optional ExprId |
| 4 | `key_expressions` | u32 count + ExprId[] |
| 5 | `measures` | u32 count + ProjectionMeasurePlan[] |
| 6 | `frontier` | Projection frontier tag |
| 7 | `group_schema` | ProjectionGroupSchema |

### ProjectionMeasurePlan

| # | Field | Encoding |
|---:|---|---|
| 1 | `field` | FieldSchema |
| 2 | `aggregation` | ProjectionAggregation tag plus exact selected payload |

### ProjectionGroupSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `projection_id` | ProjectionId |
| 2 | `codec_version` | u32 = 1 |
| 3 | `components` | u32 count + ProjectionGroupComponentSchema[] |
| 4 | `measures` | RecordSchema |
| 5 | `maximum_complete_key_bytes` | u32 |
| 6 | `maximum_stored_state_bytes` | u32 |

### ProjectionGroupComponentSchema

| # | Field | Encoding |
|---:|---|---|
| 1 | `value_type` | ValueType |
| 2 | `enum_variants` | u32 count + EnumVariantId[] |
| 3 | `maximum_framed_bytes` | u32 |

### GeneratedSchemaArtifact

| # | Field | Encoding |
|---:|---|---|
| 1 | `key` | SchemaArtifactKey tag plus exact selected payload |
| 2 | `canonical_json` | bytes |
| 3 | `schema_hash` | 32 bytes |

### McpCommandNameRegistryV1

| # | Field | Encoding |
|---:|---|---|
| 1 | `version` | u32 = 1 |
| 2 | `lineage` | string |
| 3 | `source_contract_name` | string |
| 4 | `entries` | u32 count + McpCommandNameEntryV1[] |

### McpCommandNameEntryV1

| # | Field | Encoding |
|---:|---|---|
| 1 | `command_id` | CommandId |
| 2 | `source_command_name` | string |
| 3 | `tool_name` | string |

### CompatibilityReport

| # | Field | Encoding |
|---:|---|---|
| 1 | `overall` | Compatibility class tag |
| 2 | `entries` | u32 count + CompatibilityEntry[] |

### CompatibilityEntry

| # | Field | Encoding |
|---:|---|---|
| 1 | `code` | string containing one exact closed CompatibilityCode |
| 2 | `affected_path` | canonical StableAffectedPath string |

## Canonical Ordering and Bounds

- Stable-ID declarations and field registries are increasing and duplicate-free. Source identifiers and contract lineage names are at most 256 ASCII bytes; `compiler_version` is nonempty ASCII at most 64 bytes.
- Expression arenas are forward-only in dense `ExprId` order; unreachable nodes reject; expression and `ValueType` nesting depths are each at most 32.
- Root-validation reads are dense and ordered by the lowest source child `BindingId` in each structurally identical root-key derivation group.
- Commit checks order by invariant ID, source subjects, then root-validation subjects.
- Complete bundle size is at most 15 MiB; one generated JSON Schema artifact is at most 1 MiB and its exact byte size is checked before allocating the JSON tree; the artifact inventory is at most 20,480.
- Lineage entries and allocation states are each at most 262,144; total expression nodes are at most 131,072; declaration and command-item collections are at most 4,096; object/tuple constructions are at most 1,024 fields.
- `ValueType::string` and `ValueType::bytes` bounds are each 1..=1,048,576 bytes; `ValueType::list` bounds are 1..=65,535 entries. Irrespective of ADR-0011's general canonical-value collection limit, every list or record nested anywhere inside an executable-IR `Constant` is limited to 1,024 entries before allocation.
- Only currently required empty lineage allocation states are encoded; nonempty historical states persist.
- Repeated maps and sets are encoded in their declared canonical order. Stored hashes, schema artifacts, registry contents, and computed maxima are revalidated on decode.

## Root Validation Boundary

For a mutable child whose aggregate invariant needs the root and has no exact source root binding, the compiler emits one `RootValidationReadPlan`. The table occurs immediately after source bindings. Reads group only structurally identical checked root-key derivations and carry the lowest requiring source binding, exact root key schema, key expressions, and duplicate-free influential root fields. Empty fields are valid for a constant invariant because root presence and invariant application still matter. `RootValidationField` (`0x0b`) is legal only in commit-check predicates. Commit checks encode source and root subjects separately. Missing internal roots are integrity faults, never business outcomes.

## Typed Hash Payloads

The sequences below are canonical payloads supplied to the accepted [ADR-0011 typed SHA-256 frame](../../adr/0011-canonical-values-keys-and-hashing.md): `RIFFDB-HASH\0 || 0x01 || u16 domain_byte_length || domain_bytes || u64 payload_byte_length || payload_bytes`. They are not complete SHA-256 preimages by themselves. Every ordered durable layout above includes its listed name strings unconditionally. Name omission applies only to the hash payload helpers described here.

- Command domain payload: `RIFFDB-COMMAND-PLAN\0 || ir_version || CommandId || CommandSemantics || ReferencedEnumClosure`. It omits the command name, binding aliases, and declaration display names for entity, aggregate, invariant, index, event, and enum closures. Input, outcome-payload, nested record field, enum-variant, and outcome names remain encoded.
- Projection domain payload: `RIFFDB-PROJECTION-PLAN\0 || ir_version || ProjectionId || source EventSchema || ProjectionSemantics || ReferencedEnumClosure`. It omits the projection name, source-event declaration name, and enum declaration names; source payload, projection-group, measure field, and enum-variant names remain encoded.
- `ReferencedEnumClosure` contains every enum reached through the encoded command/projection value types or their referenced entity/event records, sorted by `EnumTypeId`; each entry encodes its stable `EnumTypeId` and complete variants in `EnumVariantId` order but omits the display-only enum declaration name. An unrelated enum is absent.
- Schema domain payload: `RIFFDB-SCHEMA-IR\0 || ir_version || StructuralSchema`; schema declaration, field, and enum-variant names are all encoded.
- Contract-root domain payload: `RIFFDB-CONTRACT-PLAN-ROOT\0 || ir_version || schema_hash || ordered (CommandId, PlanHash) pairs || ordered (ProjectionId, ProjectionPlanHash) pairs`.
- `ContractBundleHash` uses the bundle hash domain over complete `ContractBundle` bytes and is not embedded in its own payload.

### Hash-Only Ordered Layouts

#### ReferencedEnumClosure

| # | Field | Encoding |
|---:|---|---|
| 1 | `enums` | u32 count + entries in EnumTypeId order |
| 2 | `enum_id` | EnumTypeId |
| 3 | `variants` | u32 count + (EnumVariantId, string name)[] in EnumVariantId order |

## Projection Framing Review

Projection group codec version is `1`. The stored-state calculation includes exactly one 32-byte v1 stored-record framing reserve under accepted ADR-0017. It is sizing headroom, not persisted padding or an extension field. WP-065 must prove the reviewed generated `StoredProjectionStateV1` payload fits the computed maximum and the real `StoredEnvelope` fits the 16 MiB ceiling before any projection row is persisted; otherwise implementation stops for human review before changing this format.
