//! Byte-level conformance witnesses for the private contract-bundle codec.

use super::*;
use crate::format_registry::{FORMAT_LAYOUTS, HASH_ONLY_LAYOUTS, TAGGED_UNION_LAYOUTS};

fn encode_with(operation: impl FnOnce(&mut Writer) -> Result<(), IrValidationError>) -> Vec<u8> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    operation(&mut writer).expect("sentinel encodes");
    writer.finish()
}

fn assert_reader_finished(reader: Reader<'_>) {
    reader.finish().expect("sentinel fully consumed");
}

fn lowercase_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("string formatting cannot fail");
    }
    output
}

fn assert_record_ref_rejects_malformed(canonical: &[u8]) {
    for length in 0..canonical.len() {
        assert!(
            decode_record_ref(&mut Reader::new(&canonical[..length])).is_err(),
            "record reference accepted truncated prefix of length {length}"
        );
    }
    let mut trailing = canonical.to_vec();
    trailing.push(0xff);
    let mut reader = Reader::new(&trailing);
    decode_record_ref(&mut reader).expect("canonical prefix decodes");
    assert!(reader.finish().is_err());
}

fn assert_value_type_rejects_malformed(canonical: &[u8]) {
    for length in 0..canonical.len() {
        assert!(
            decode_value_type(&mut Reader::new(&canonical[..length]), 0).is_err(),
            "value type accepted truncated prefix of length {length}"
        );
    }
    let mut trailing = canonical.to_vec();
    trailing.push(0xff);
    let mut reader = Reader::new(&trailing);
    decode_value_type(&mut reader, 0).expect("canonical prefix decodes");
    assert!(reader.finish().is_err());
}

fn enum_index_schema() -> SchemaIr {
    let entity_id = EntityTypeId::first();
    let aggregate_id = AggregateTypeId::first();
    let enum_id = EnumTypeId::new(2).expect("enum ID");
    let id_field = FieldId::first();
    let state_field = FieldId::new(2).expect("field ID");
    let enum_variants = vec![
        EnumVariantId::first(),
        EnumVariantId::new(10).expect("variant ID"),
    ];
    let scalar = KeyComponentSchema::new(ValueType::u64(), vec![]).expect("component");
    let entity_key =
        KeySchema::new(KeyPurpose::Entity(entity_id), vec![scalar.clone()]).expect("entity key");
    let index_id = IndexId::first();
    let index_component =
        KeyComponentSchema::new(ValueType::enumeration(enum_id), enum_variants.clone())
            .expect("enum component");
    let index_key = KeySchema::index(
        index_id,
        entity_id,
        vec![index_component],
        entity_key.clone(),
    )
    .expect("index key");
    let entity = EntitySchema::new(
        entity_id,
        "Root",
        RecordSchema::new(
            RecordTypeRef::Entity(entity_id),
            vec![
                FieldSchema::new(id_field, "id", ValueType::u64()).expect("field"),
                FieldSchema::new(state_field, "state", ValueType::enumeration(enum_id))
                    .expect("field"),
            ],
        )
        .expect("record"),
        vec![id_field],
        entity_key,
        vec![],
        vec![IndexSchema::new(index_id, "by_state", vec![state_field], index_key).expect("index")],
    )
    .expect("entity");
    let key_expressions = ExpressionArena::new(vec![(
        ExpressionKind::SchemaField {
            entity_type: entity_id,
            field: id_field,
        },
        ValueType::u64(),
    )])
    .expect("key expressions");
    let aggregate = AggregateSchema::new(
        aggregate_id,
        "Aggregate",
        entity_id,
        vec![],
        AggregateKeyPlan::new(
            key_expressions,
            ExprId::new(0),
            vec![ExprId::new(0)],
            KeySchema::new(KeyPurpose::Partition(aggregate_id), vec![scalar.clone()])
                .expect("partition key"),
            KeySchema::new(KeyPurpose::Conflict(aggregate_id), vec![scalar]).expect("conflict key"),
        )
        .expect("aggregate keys"),
        vec![],
    )
    .expect("aggregate");
    let enumeration = EnumSchema::new(
        enum_id,
        "State",
        vec![
            EnumVariantSchema::new(enum_variants[0], "Open").expect("variant"),
            EnumVariantSchema::new(enum_variants[1], "Closed").expect("variant"),
        ],
    )
    .expect("enum");
    SchemaIr::new(vec![entity], vec![], vec![enumeration], vec![aggregate]).expect("schema")
}

fn projection_fixture() -> (ProjectionPlan, SchemaIr) {
    let event_id = EventTypeId::first();
    let projection_id = ProjectionId::first();
    let group_field = FieldId::first();
    let amount_field = FieldId::new(2).expect("field ID");
    let event = EventSchema::new(
        event_id,
        "Applied",
        RecordSchema::new(
            RecordTypeRef::Event(event_id),
            vec![
                FieldSchema::new(group_field, "group", ValueType::bool()).expect("field"),
                FieldSchema::new(amount_field, "amount", ValueType::i64()).expect("field"),
            ],
        )
        .expect("event record"),
    )
    .expect("event");
    let schema = SchemaIr::new(vec![], vec![event], vec![], vec![]).expect("schema");
    let expressions = ExpressionArena::new(vec![
        (
            ExpressionKind::SourceEventField(group_field),
            ValueType::bool(),
        ),
        (
            ExpressionKind::SourceEventField(amount_field),
            ValueType::i64(),
        ),
    ])
    .expect("expressions");
    let measure_field =
        FieldSchema::new(FieldId::first(), "total", ValueType::i64()).expect("measure field");
    let group_schema = ProjectionGroupSchema::new(
        projection_id,
        vec![
            ProjectionGroupComponentSchema::new(ValueType::bool(), vec![])
                .expect("group component"),
        ],
        RecordSchema::new(
            RecordTypeRef::ProjectionResult(projection_id),
            vec![measure_field.clone()],
        )
        .expect("measure record"),
    )
    .expect("group schema");
    let plan = ProjectionPlan::new(
        projection_id,
        "Totals",
        event_id,
        expressions,
        None,
        vec![ExprId::new(0)],
        vec![ProjectionMeasurePlan::sum(measure_field, ExprId::new(1)).expect("sum")],
        ProjectionFrontierPolicy::TransactionallyOrdered,
        group_schema,
        &schema,
    )
    .expect("projection");
    (plan, schema)
}

#[test]
fn ordered_layout_registry_has_one_closed_witness_slot_per_layout() {
    let expected = vec![
        (
            "ContractBundle",
            vec![
                "magic",
                "bundle_format_version",
                "grammar_version",
                "executable_ir_version",
                "compiler_version",
                "contract_lineage",
                "contract_version",
                "parent",
                "source_hash",
                "plan_root_hash",
                "ledger",
                "schema",
                "workflows",
                "row_policies",
                "commands",
                "projections",
                "schema_artifacts",
                "mcp_names",
                "compatibility",
            ],
        ),
        ("ParentBundleRef", vec!["contract_version", "bundle_hash"]),
        ("LineageLedgerV1", vec!["version", "allocations", "aliases"]),
        (
            "LineageAllocation",
            vec![
                "namespace_tag",
                "owner_kind",
                "owner_ids",
                "max_allocated",
                "entries",
            ],
        ),
        (
            "LineageEntry",
            vec![
                "id",
                "identity_owner_kind",
                "identity_owner_ids",
                "name",
                "state",
            ],
        ),
        (
            "LineageAlias",
            vec![
                "namespace_tag",
                "identity_owner_kind",
                "identity_owner_ids",
                "name",
                "id",
            ],
        ),
        (
            "StructuralSchema",
            vec![
                "entities",
                "events",
                "enums",
                "aggregates",
                "relationships",
                "unique_keys",
                "delete_policies",
                "vector_field_specs",
                "secret_field_specs",
            ],
        ),
        (
            "RelationshipSchema",
            vec![
                "name",
                "source_entity",
                "source_fields",
                "target_entity",
                "target_fields",
            ],
        ),
        (
            "UniqueKeySchema",
            vec!["name", "source_entity", "index_id", "fields"],
        ),
        (
            "DeletePolicySchemaV1",
            vec!["target_entity", "mode", "restrict_payload"],
        ),
        (
            "VectorFieldSpecV1",
            vec![
                "entity",
                "field",
                "metric",
                "source_fields",
                "staleness_slo_secs",
            ],
        ),
        ("SecretFieldSpecV1", vec!["entity", "field"]),
        (
            "EntitySchema",
            vec![
                "id",
                "name",
                "record",
                "primary_key_fields",
                "primary_key",
                "invariants",
                "indexes",
            ],
        ),
        ("EventSchema", vec!["id", "name", "payload", "partition"]),
        ("EventPartitionSchema", vec!["fields", "key_schema"]),
        ("EnumSchema", vec!["id", "name", "variants"]),
        ("EnumVariantSchema", vec!["id", "name"]),
        (
            "AggregateSchema",
            vec!["id", "name", "root", "children", "keys", "invariants"],
        ),
        (
            "AggregateKeyPlan",
            vec![
                "expressions",
                "partition_expression",
                "conflict_expressions",
                "partition_schema",
                "conflict_schema",
            ],
        ),
        (
            "InvariantPlan",
            vec!["id", "name", "expressions", "predicate"],
        ),
        ("IndexSchema", vec!["id", "name", "fields", "key_schema"]),
        ("RecordSchema", vec!["owner", "fields"]),
        ("FieldSchema", vec!["id", "name", "value_type"]),
        (
            "KeySchema",
            vec![
                "codec_version",
                "purpose",
                "components",
                "maximum_encoded_bytes",
                "entity_key_schema",
            ],
        ),
        (
            "KeyComponentSchema",
            vec!["value_type", "enum_variants", "maximum_payload_bytes"],
        ),
        ("ExpressionArena", vec!["nodes"]),
        (
            "WorkflowSchema",
            vec![
                "name",
                "entity",
                "state_field",
                "state_enum",
                "initial_state",
                "transitions",
                "lease",
            ],
        ),
        (
            "WorkflowTransitionSchema",
            vec!["name", "source_states", "destination"],
        ),
        (
            "WorkflowLeaseSchema",
            vec![
                "name",
                "owner_field",
                "expiry_field",
                "fencing_token_field",
                "attempt_field",
                "minimum_duration_seconds",
                "maximum_duration_seconds",
            ],
        ),
        (
            "WorkflowLeaseFields",
            vec![
                "owner_field",
                "expiry_field",
                "fencing_token_field",
                "attempt_field",
                "minimum_duration_seconds",
                "maximum_duration_seconds",
            ],
        ),
        (
            "CommandBundleEntry",
            vec![
                "command_id",
                "name",
                "contract_version",
                "plan_hash",
                "semantics",
            ],
        ),
        (
            "CommandSemantics",
            vec![
                "input",
                "service_values",
                "outcomes",
                "success_outcome",
                "idempotency_input",
                "input_schema_hash",
                "output_schema_hash",
                "collection_expansion",
                "expressions",
                "bindings",
                "root_validation_reads",
                "relationship_checks",
                "delete_checks",
                "locality",
                "commit_checks",
                "instructions",
                "invocation_class",
                "execution_class",
                "retry_policy",
                "required_capability",
                "entity_closure",
                "aggregate_closure",
                "event_closure",
            ],
        ),
        (
            "CollectionExpansionPlanV1",
            vec![
                "input_field",
                "minimum_elements",
                "maximum_elements",
                "element_type",
                "first_binding",
                "binding_count",
                "first_instruction",
                "instruction_count",
                "duplicate_policy",
            ],
        ),
        ("OutcomeSchema", vec!["id", "name", "payload"]),
        (
            "BindingPlan",
            vec![
                "id",
                "name",
                "mode",
                "entity_type",
                "key_schema",
                "key_expressions",
                "accessed_fields",
                "complete_record_access",
                "failure",
                "restriction_failure",
            ],
        ),
        (
            "RootValidationReadPlan",
            vec![
                "id",
                "source_binding",
                "root_entity",
                "key_schema",
                "key_expressions",
                "accessed_fields",
            ],
        ),
        (
            "RelationshipCheckPlan",
            vec!["relationship_name", "source_binding", "target_binding"],
        ),
        (
            "DeleteCheckPlanV1",
            vec![
                "binding",
                "mode",
                "restrict_source_entity",
                "restrict_index",
            ],
        ),
        (
            "LocalityPlan",
            vec![
                "aggregate_id",
                "partition_schema",
                "partition_expression",
                "conflict_derivations",
            ],
        ),
        ("ConflictDerivationPlan", vec!["schema", "expressions"]),
        (
            "CommitCheckPlan",
            vec![
                "invariant_id",
                "predicate",
                "source_bindings",
                "root_validation_reads",
            ],
        ),
        ("ObjectConstruction", vec!["record", "fields"]),
        ("OutcomeConstruction", vec!["outcome_id", "payload"]),
        ("EventConstruction", vec!["event_type", "payload"]),
        (
            "ProjectionBundleEntry",
            vec!["projection_id", "name", "plan_hash", "semantics"],
        ),
        (
            "ProjectionSemantics",
            vec![
                "source_event",
                "expressions",
                "filter",
                "key_expressions",
                "measures",
                "frontier",
                "group_schema",
            ],
        ),
        ("ProjectionMeasurePlan", vec!["field", "aggregation"]),
        (
            "ProjectionGroupSchema",
            vec![
                "projection_id",
                "codec_version",
                "components",
                "measures",
                "maximum_complete_key_bytes",
                "maximum_stored_state_bytes",
            ],
        ),
        (
            "ProjectionGroupComponentSchema",
            vec!["value_type", "enum_variants", "maximum_framed_bytes"],
        ),
        ("RowPolicyCatalogV1", vec!["version", "facts", "policies"]),
        ("PrincipalFactSchemaV1", vec!["name", "value_type"]),
        ("RowPolicyPlanV1", vec!["name", "entity", "rules"]),
        ("RowPolicyRuleV1", vec!["operation", "root", "nodes"]),
        ("RowPolicyExpressionNodeV1", vec!["tag", "payload"]),
        ("RowPolicyOperandV1", vec!["source", "value_type"]),
        (
            "GeneratedSchemaArtifact",
            vec!["key", "canonical_json", "schema_hash"],
        ),
        (
            "McpCommandNameRegistryV2",
            vec!["version", "lineage", "source_contract_name", "entries"],
        ),
        (
            "McpCommandNameEntryV2",
            vec!["command_id", "source_command_name", "tool_name"],
        ),
        ("CompatibilityReport", vec!["overall", "entries"]),
        ("CompatibilityEntry", vec!["code", "affected_path"]),
    ];
    assert_eq!(
        FORMAT_LAYOUTS
            .iter()
            .map(|layout| {
                (
                    layout.name,
                    layout
                        .fields
                        .iter()
                        .map(|field| field.name)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        HASH_ONLY_LAYOUTS
            .iter()
            .map(|layout| {
                (
                    layout.name,
                    layout
                        .fields
                        .iter()
                        .map(|field| field.name)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
        [(
            "ReferencedEnumClosure",
            vec!["enums", "enum_id", "variants"]
        )]
    );

    // This is deliberately closed: adding a layout requires naming the fixture
    // family that exercises its encoder and decoder, not just documenting it.
    let witness_layouts = [
        ("ContractBundle", "contract bundle fixture"),
        ("ParentBundleRef", "successor bundle fixture"),
        ("LineageLedgerV1", "lineage ledger fixture"),
        ("LineageAllocation", "lineage ledger fixture"),
        ("LineageEntry", "lineage ledger fixture"),
        ("LineageAlias", "lineage rename fixture"),
        ("StructuralSchema", "structural schema fixture"),
        ("RelationshipSchema", "relationship compiler fixture"),
        ("UniqueKeySchema", "uniqueness compiler fixture"),
        ("DeletePolicySchemaV1", "delete-policy schema fixture"),
        ("VectorFieldSpecV1", "vector contract front-door fixture"),
        ("SecretFieldSpecV1", "secret contract front-door fixture"),
        ("EntitySchema", "command schema closure fixture"),
        ("EventSchema", "projection source fixture"),
        ("EventPartitionSchema", "partitioned event fixture"),
        ("EnumSchema", "enum closure fixture"),
        ("EnumVariantSchema", "enum closure fixture"),
        ("AggregateSchema", "command schema closure fixture"),
        ("AggregateKeyPlan", "command schema closure fixture"),
        ("InvariantPlan", "command schema closure fixture"),
        ("IndexSchema", "structural schema fixture"),
        ("RecordSchema", "structural schema fixture"),
        ("FieldSchema", "structural schema fixture"),
        ("KeySchema", "key-purpose fixture"),
        ("KeyComponentSchema", "key-purpose fixture"),
        ("ExpressionArena", "expression fixture"),
        ("WorkflowSchema", "workflow bundle fixture"),
        ("WorkflowTransitionSchema", "workflow bundle fixture"),
        ("WorkflowLeaseSchema", "workflow bundle fixture"),
        ("WorkflowLeaseFields", "workflow lease instruction fixture"),
        ("CommandBundleEntry", "root-validation command fixture"),
        ("CommandSemantics", "root-validation command fixture"),
        ("CollectionExpansionPlanV1", "collection command fixture"),
        ("OutcomeSchema", "root-validation command fixture"),
        ("BindingPlan", "root-validation command fixture"),
        ("RootValidationReadPlan", "root-validation command fixture"),
        ("RelationshipCheckPlan", "relationship compiler fixture"),
        ("DeleteCheckPlanV1", "checked-delete command fixture"),
        ("LocalityPlan", "root-validation command fixture"),
        ("ConflictDerivationPlan", "root-validation command fixture"),
        ("CommitCheckPlan", "root-validation command fixture"),
        ("ObjectConstruction", "instruction fixture"),
        ("OutcomeConstruction", "instruction fixture"),
        ("EventConstruction", "instruction fixture"),
        ("ProjectionBundleEntry", "projection fixture"),
        ("ProjectionSemantics", "projection fixture"),
        ("ProjectionMeasurePlan", "projection fixture"),
        ("ProjectionGroupSchema", "projection fixture"),
        ("ProjectionGroupComponentSchema", "projection fixture"),
        ("RowPolicyCatalogV1", "row-policy catalog fixture"),
        ("PrincipalFactSchemaV1", "row-policy catalog fixture"),
        ("RowPolicyPlanV1", "row-policy catalog fixture"),
        ("RowPolicyRuleV1", "row-policy catalog fixture"),
        ("RowPolicyExpressionNodeV1", "row-policy catalog fixture"),
        ("RowPolicyOperandV1", "row-policy catalog fixture"),
        ("GeneratedSchemaArtifact", "contract bundle fixture"),
        ("McpCommandNameRegistryV2", "MCP registry fixture"),
        ("McpCommandNameEntryV2", "MCP registry fixture"),
        ("CompatibilityReport", "compatibility fixture"),
        ("CompatibilityEntry", "compatibility fixture"),
    ];
    assert_eq!(
        witness_layouts
            .iter()
            .map(|(layout, _family)| *layout)
            .collect::<Vec<_>>(),
        FORMAT_LAYOUTS
            .iter()
            .map(|layout| layout.name)
            .collect::<Vec<_>>(),
        "every registered durable layout must stay assigned to a witness family"
    );
}

#[test]
fn tagged_union_registry_is_closed_in_tag_order() {
    for union in TAGGED_UNION_LAYOUTS {
        assert_eq!(
            union
                .tags
                .tags
                .iter()
                .map(|tag| (tag.value, tag.name))
                .collect::<Vec<_>>(),
            union
                .variants
                .iter()
                .map(|variant| (variant.tag, variant.name))
                .collect::<Vec<_>>(),
            "{} must have exactly one payload layout for every registered tag",
            union.name
        );
    }
    let actual = TAGGED_UNION_LAYOUTS
        .iter()
        .map(|union| {
            (
                union.name,
                union
                    .variants
                    .iter()
                    .map(|variant| {
                        (
                            variant.tag,
                            variant.name,
                            variant
                                .fields
                                .iter()
                                .map(|field| field.name)
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (
                "RecordTypeRef",
                vec![
                    (0x01, "entity", vec!["entity_type"]),
                    (0x02, "event", vec!["event_type"]),
                    (0x03, "command input", vec!["command_id"]),
                    (0x04, "command outcome", vec!["command_id", "outcome_id"],),
                    (0x05, "projection result", vec!["projection_id"]),
                ],
            ),
            (
                "ValueType",
                vec![
                    (0x01, "bool", vec![]),
                    (0x02, "i64", vec![]),
                    (0x03, "u64", vec![]),
                    (0x04, "decimal", vec!["precision", "scale"]),
                    (0x05, "money", vec!["currency"]),
                    (0x06, "string", vec!["maximum_utf8_bytes"]),
                    (0x07, "bytes", vec!["maximum_bytes"]),
                    (0x08, "timestamp", vec![]),
                    (0x09, "date", vec![]),
                    (0x0a, "uuid", vec![]),
                    (0x0b, "enum", vec!["enum_type"]),
                    (0x0c, "optional", vec!["inner_type"]),
                    (0x0d, "list", vec!["element_type", "maximum_entries"]),
                    (0x0e, "record", vec!["record_type"]),
                    (0x0f, "vector", vec!["dimension"]),
                ],
            ),
            (
                "KeyPurpose",
                vec![
                    (0x01, "entity", vec!["entity_type"]),
                    (0x02, "partition", vec!["aggregate_id"]),
                    (0x03, "conflict", vec!["aggregate_id"]),
                    (0x04, "index", vec!["index_id", "entity_type"]),
                ],
            ),
            (
                "TypedExpression",
                vec![
                    (0x01, "constant", vec!["result_type", "canonical_value"]),
                    (0x02, "input field", vec!["result_type", "field"]),
                    (0x03, "complete binding", vec!["result_type", "binding"],),
                    (0x04, "bound field", vec!["result_type", "binding", "field"],),
                    (
                        0x05,
                        "schema field",
                        vec!["result_type", "entity_type", "field"],
                    ),
                    (0x06, "source-event field", vec!["result_type", "field"],),
                    (0x07, "tx.time", vec!["result_type"]),
                    (0x08, "tx.date", vec!["result_type"]),
                    (0x09, "unary", vec!["result_type", "operator", "operand"],),
                    (
                        0x0a,
                        "binary",
                        vec!["result_type", "operator", "left", "right"],
                    ),
                    (
                        0x0b,
                        "root-validation field",
                        vec!["result_type", "read", "field"],
                    ),
                    (
                        0x0c,
                        "service-owned command value",
                        vec!["result_type", "field"],
                    ),
                    (0x0d, "collection element", vec!["result_type"]),
                    (
                        0x0e,
                        "collection element field",
                        vec!["result_type", "field"],
                    ),
                ],
            ),
            (
                "Instruction",
                vec![
                    (
                        0x01,
                        "require",
                        vec!["requirement_index", "predicate", "reject"],
                    ),
                    (0x02, "set field", vec!["binding", "field", "value"]),
                    (0x03, "emit event", vec!["event"]),
                    (0x04, "return", vec!["outcome"]),
                    (
                        0x05,
                        "workflow transition",
                        vec![
                            "binding",
                            "state_field",
                            "source_states",
                            "destination",
                            "expected_revision",
                            "stale",
                            "illegal",
                        ],
                    ),
                    (
                        0x06,
                        "workflow lease",
                        vec!["binding", "fields", "operation"]
                    ),
                ],
            ),
            (
                "WorkflowLeaseOperation",
                vec![
                    (
                        0x01,
                        "claim",
                        vec![
                            "owner",
                            "duration_seconds",
                            "expected_revision",
                            "stale",
                            "unavailable",
                            "invalid",
                            "exhausted"
                        ]
                    ),
                    (
                        0x02,
                        "renew",
                        vec![
                            "owner",
                            "fencing_token",
                            "duration_seconds",
                            "expected_revision",
                            "stale",
                            "invalid",
                            "expired",
                            "exhausted"
                        ]
                    ),
                    (
                        0x03,
                        "release",
                        vec![
                            "owner",
                            "fencing_token",
                            "expected_revision",
                            "stale",
                            "invalid"
                        ]
                    ),
                    (0x04, "expire", vec!["expected_revision", "stale", "active"]),
                    (
                        0x05,
                        "fence",
                        vec![
                            "owner",
                            "fencing_token",
                            "expected_revision",
                            "stale",
                            "invalid",
                            "expired"
                        ]
                    ),
                ],
            ),
            (
                "CapabilityRequirement",
                vec![(0x01, "invoke command", vec!["lineage", "command_id"])],
            ),
            (
                "ProjectionAggregation",
                vec![
                    (0x01, "count", vec!["expression_present"]),
                    (0x02, "sum", vec!["expression_present", "expression"],),
                ],
            ),
            (
                "SchemaArtifactKey",
                vec![
                    (0x01, "entity record", vec!["entity_type"]),
                    (0x02, "durable event payload", vec!["event_type"]),
                    (0x03, "command input", vec!["command_id"]),
                    (0x04, "command outcome union", vec!["command_id"]),
                    (0x05, "projection result row", vec!["projection_id"]),
                ],
            ),
        ]
    );
}

#[test]
fn checked_bundle_fixtures_exercise_the_registered_layout_families() {
    const FIXTURES: &[(&str, &[u8])] = &[
        (
            "canonical budget",
            include_bytes!("../../../../fixtures/compiler/bundle.bin"),
        ),
        (
            "constant root validation v1",
            include_bytes!(
                "../../../../fixtures/compiler/root-validation/constant-invariant-bundle-v1.bin"
            ),
        ),
        (
            "constant root validation successor",
            include_bytes!(
                "../../../../fixtures/compiler/root-validation/constant-invariant-bundle-v2.bin"
            ),
        ),
        (
            "field-dependent root validation v1",
            include_bytes!(
                "../../../../fixtures/compiler/root-validation/field-dependent-bundle-v1.bin"
            ),
        ),
        (
            "field-dependent root validation successor",
            include_bytes!(
                "../../../../fixtures/compiler/root-validation/field-dependent-bundle-v2.bin"
            ),
        ),
    ];

    let decoded = FIXTURES
        .iter()
        .map(|(name, bytes)| {
            let bundle = ContractBundle::decode(bytes)
                .unwrap_or_else(|error| panic!("{name} fixture decodes: {error}"));
            assert_eq!(
                bundle.canonical_bytes(),
                *bytes,
                "{name} re-encodes exactly"
            );
            bundle
        })
        .collect::<Vec<_>>();

    let budget = &decoded[0];
    assert!(!budget.schema().events().is_empty());
    assert!(!budget.projections().is_empty());
    assert!(!budget.schema_artifacts().is_empty());
    assert!(!budget.mcp_command_names().entries().is_empty());

    assert!(decoded[2].parent().is_some());
    assert!(decoded[4].parent().is_some());
    assert!(
        decoded[1..]
            .iter()
            .all(|bundle| !bundle.commands().is_empty())
    );
    assert!(decoded[3].commands()[0].root_validation_reads().len() == 1);
}

#[test]
fn checked_hash_preimage_vectors_freeze_all_four_domains() {
    let enum_schema = enum_index_schema();
    let enum_ids = BTreeSet::from([EnumTypeId::new(2).expect("enum ID")]);
    let enum_preimage = encode_with(|writer| encode_enum_closure(writer, &enum_ids, &enum_schema));
    let expected_enum_preimage = vec![
        0, 0, 0, 1, // enum count
        0, 0, 0, 2, // EnumTypeId
        0, 0, 0, 2, // variant count
        0, 0, 0, 1, // EnumVariantId
        0, 0, 0, 4, b'O', b'p', b'e', b'n', // name
        0, 0, 0, 10, // EnumVariantId
        0, 0, 0, 6, b'C', b'l', b'o', b's', b'e', b'd', // name
    ];
    assert_eq!(enum_preimage, expected_enum_preimage);
    let schema_bytes = encode_with(|writer| encode_schema(writer, &enum_schema));
    let mut reader = Reader::new(&schema_bytes);
    assert_eq!(
        decode_schema(&mut reader).expect("schema decode"),
        enum_schema
    );
    assert_reader_finished(reader);

    let (command, command_schema) = crate::plan::tests::minimal_mutation();
    let command_semantics =
        encode_with(|writer| encode_command_semantics(writer, &command, &command_schema, false));
    let command_enums =
        collect_command_enum_closure(&command, &command_schema).expect("command enum closure");
    let command_enum_bytes =
        encode_with(|writer| encode_enum_closure(writer, &command_enums, &command_schema));
    let mut command_preimage = COMMAND_PLAN_MAGIC.to_vec();
    command_preimage.extend_from_slice(&EXECUTABLE_IR_VERSION_V1.to_be_bytes());
    command_preimage.extend_from_slice(&command.command_id().get().to_be_bytes());
    command_preimage.extend_from_slice(&command_semantics);
    command_preimage.extend_from_slice(&command_enum_bytes);
    assert_eq!(hash_plan(&command_preimage), command.plan_hash());
    assert_eq!(
        compute_command_plan_hash(&command, &command_schema).expect("command hash"),
        command.plan_hash()
    );

    let (projection, projection_schema) = projection_fixture();
    let source = projection_schema
        .event(projection.source_event())
        .expect("projection source");
    let source_bytes = encode_with(|writer| encode_event_schema(writer, source, false));
    let projection_semantics =
        encode_with(|writer| encode_projection_semantics(writer, &projection));
    let projection_enums = collect_projection_enum_closure(&projection, source, &projection_schema)
        .expect("projection enum closure");
    let projection_enum_bytes =
        encode_with(|writer| encode_enum_closure(writer, &projection_enums, &projection_schema));
    let mut projection_preimage = PROJECTION_PLAN_MAGIC.to_vec();
    projection_preimage.extend_from_slice(&EXECUTABLE_IR_VERSION_V1.to_be_bytes());
    projection_preimage.extend_from_slice(&projection.projection_id().get().to_be_bytes());
    projection_preimage.extend_from_slice(&source_bytes);
    projection_preimage.extend_from_slice(&projection_semantics);
    projection_preimage.extend_from_slice(&projection_enum_bytes);
    assert_eq!(
        hash_projection_plan(&projection_preimage),
        projection.plan_hash()
    );
    assert_eq!(
        compute_projection_plan_hash(&projection, &projection_schema).expect("projection hash"),
        projection.plan_hash()
    );
    let projection_bytes =
        encode_with(|writer| encode_projection_bundle_entry(writer, &projection));
    let mut reader = Reader::new(&projection_bytes);
    assert_eq!(
        decode_projection(&mut reader, &projection_schema).expect("projection decode"),
        projection
    );
    assert_reader_finished(reader);

    let structural = encode_structural_schema(&command_schema).expect("structural schema");
    let mut schema_preimage = SCHEMA_IR_MAGIC.to_vec();
    schema_preimage.extend_from_slice(&EXECUTABLE_IR_VERSION_V1.to_be_bytes());
    schema_preimage.extend_from_slice(&structural);
    let structural_hash = hash_schema(&schema_preimage);
    let mut root_preimage = ROOT_PLAN_MAGIC.to_vec();
    root_preimage.extend_from_slice(&EXECUTABLE_IR_VERSION_V1.to_be_bytes());
    root_preimage.extend_from_slice(structural_hash.as_bytes());
    root_preimage.extend_from_slice(&1u32.to_be_bytes());
    root_preimage.extend_from_slice(&command.command_id().get().to_be_bytes());
    root_preimage.extend_from_slice(command.plan_hash().as_bytes());
    root_preimage.extend_from_slice(&0u32.to_be_bytes());
    assert_eq!(
        hash_contract_plan_root(&root_preimage),
        compute_plan_root_hash(&command_schema, std::slice::from_ref(&command), &[])
            .expect("root hash")
    );

    assert_eq!(command_preimage.len(), 604);
    assert_eq!(
        lowercase_hex(hash_plan(&command_preimage).as_bytes()),
        "9a0abddc786b5f5c37fe875c6bcfa285b63c52d60ad67300519e5df40f928a6a"
    );
    assert_eq!(projection_preimage.len(), 183);
    assert_eq!(
        lowercase_hex(hash_projection_plan(&projection_preimage).as_bytes()),
        "6ba9577f299613359a8146c21f312dc8a26a0add44ad13eb7a2970c154b9c2cc"
    );
    assert_eq!(root_preimage.len(), 106);
    assert_eq!(
        lowercase_hex(hash_contract_plan_root(&root_preimage).as_bytes()),
        "c458673319642adf3e3761c142929a8730f644b87f0d4348178c1ce8b00c1b97"
    );
}

#[test]
fn record_reference_family_has_exact_bytes_and_round_trips() {
    let entity = EntityTypeId::new(0x0102_0304).expect("entity ID");
    let command = CommandId::new(0x1112_1314).expect("command ID");
    let outcome = OutcomeId::new(0x2122_2324).expect("outcome ID");
    let vectors = [
        (
            RecordTypeRef::Entity(entity),
            vec![0x01, 0x01, 0x02, 0x03, 0x04],
        ),
        (
            RecordTypeRef::Event(EventTypeId::new(0x0506_0708).expect("event ID")),
            vec![0x02, 0x05, 0x06, 0x07, 0x08],
        ),
        (
            RecordTypeRef::CommandInput(command),
            vec![0x03, 0x11, 0x12, 0x13, 0x14],
        ),
        (
            RecordTypeRef::CommandOutcome {
                command_id: command,
                outcome_id: outcome,
            },
            vec![0x04, 0x11, 0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x24],
        ),
        (
            RecordTypeRef::ProjectionResult(ProjectionId::new(0x3132_3334).expect("projection ID")),
            vec![0x05, 0x31, 0x32, 0x33, 0x34],
        ),
    ];

    for (value, expected) in vectors {
        assert_eq!(
            encode_with(|writer| encode_record_ref(writer, &value)),
            expected
        );
        let mut reader = Reader::new(&expected);
        let decoded = decode_record_ref(&mut reader).expect("decode");
        assert_eq!(decoded, value);
        assert_reader_finished(reader);
        assert_eq!(
            encode_with(|writer| encode_record_ref(writer, &decoded)),
            expected
        );
        assert_record_ref_rejects_malformed(&expected);
    }
    assert!(matches!(
        decode_record_ref(&mut Reader::new(&[0xff])),
        Err(IrValidationError::UnknownTag { .. })
    ));
}

#[test]
fn value_type_family_has_exact_bytes_and_round_trips() {
    let currency = CurrencyCode::new("USD").expect("currency");
    let vectors = vec![
        (ValueType::bool(), vec![0x01]),
        (ValueType::i64(), vec![0x02]),
        (ValueType::u64(), vec![0x03]),
        (
            ValueType::decimal(DecimalSpec::new(5, 2).expect("decimal")),
            vec![0x04, 0x05, 0x02],
        ),
        (ValueType::money(currency), vec![0x05, b'U', b'S', b'D']),
        (
            ValueType::string(0x0001_0203).expect("string"),
            vec![0x06, 0x00, 0x01, 0x02, 0x03],
        ),
        (
            ValueType::bytes(0x0005_0607).expect("bytes"),
            vec![0x07, 0x00, 0x05, 0x06, 0x07],
        ),
        (ValueType::timestamp(), vec![0x08]),
        (ValueType::date(), vec![0x09]),
        (ValueType::uuid(), vec![0x0a]),
        (
            ValueType::enumeration(EnumTypeId::new(0x1112_1314).expect("enum ID")),
            vec![0x0b, 0x11, 0x12, 0x13, 0x14],
        ),
        (
            ValueType::optional(ValueType::bool()).expect("optional"),
            vec![0x0c, 0x01],
        ),
        (
            ValueType::list(ValueType::u64(), 0x0102).expect("list"),
            vec![0x0d, 0x03, 0x00, 0x00, 0x01, 0x02],
        ),
        (
            ValueType::record(RecordTypeRef::Entity(
                EntityTypeId::new(0x2122_2324).expect("entity ID"),
            )),
            vec![0x0e, 0x01, 0x21, 0x22, 0x23, 0x24],
        ),
    ];

    for (value, expected) in vectors {
        assert_eq!(
            encode_with(|writer| encode_value_type(writer, &value)),
            expected
        );
        let mut reader = Reader::new(&expected);
        let decoded = decode_value_type(&mut reader, 0).expect("decode");
        assert_eq!(decoded, value);
        assert_reader_finished(reader);
        assert_eq!(
            encode_with(|writer| encode_value_type(writer, &decoded)),
            expected
        );
        assert_value_type_rejects_malformed(&expected);
    }
    assert!(matches!(
        decode_value_type(&mut Reader::new(&[0xff]), 0),
        Err(IrValidationError::UnknownTag { .. })
    ));
}

#[test]
fn expression_family_has_one_exact_vector_for_every_tag() {
    let field = FieldId::first();
    let entity = EntityTypeId::first();
    let arena = ExpressionArena::new(vec![
        (
            ExpressionKind::Constant(riffdb_types::CanonicalValue::Bool(true)),
            ValueType::bool(),
        ),
        (ExpressionKind::InputField(field), ValueType::bool()),
        (
            ExpressionKind::CompleteBinding(BindingId::new(0)),
            ValueType::record(RecordTypeRef::Entity(entity)),
        ),
        (
            ExpressionKind::BoundField {
                binding: BindingId::new(0),
                field,
            },
            ValueType::bool(),
        ),
        (
            ExpressionKind::SchemaField {
                entity_type: entity,
                field,
            },
            ValueType::bool(),
        ),
        (ExpressionKind::SourceEventField(field), ValueType::bool()),
        (ExpressionKind::TransactionTime, ValueType::timestamp()),
        (ExpressionKind::TransactionDate, ValueType::date()),
        (
            ExpressionKind::Unary {
                operator: UnaryOperator::Not,
                operand: ExprId::new(0),
            },
            ValueType::bool(),
        ),
        (
            ExpressionKind::Binary {
                operator: BinaryOperator::Equal,
                left: ExprId::new(0),
                right: ExprId::new(1),
            },
            ValueType::bool(),
        ),
        (
            ExpressionKind::RootValidationField {
                read: crate::RootValidationReadId::new(0),
                field,
            },
            ValueType::bool(),
        ),
    ])
    .expect("arena");
    let expected = vec![
        0, 0, 0, 11, // node count
        0x01, 0x01, 0, 0, 0, 3, 0x01, 0x01, 0x01, // constant true
        0x02, 0x01, 0, 0, 0, 1, // input field
        0x03, 0x0e, 0x01, 0, 0, 0, 1, 0, 0, 0, 0, // complete binding
        0x04, 0x01, 0, 0, 0, 0, 0, 0, 0, 1, // bound field
        0x05, 0x01, 0, 0, 0, 1, 0, 0, 0, 1, // schema field
        0x06, 0x01, 0, 0, 0, 1, // source-event field
        0x07, 0x08, // tx.time
        0x08, 0x09, // tx.date
        0x09, 0x01, 0x01, 0, 0, 0, 0, // unary not
        0x0a, 0x01, 0x05, 0, 0, 0, 0, 0, 0, 0, 1, // binary equal
        0x0b, 0x01, 0, 0, 0, 0, 0, 0, 0, 1, // root-validation field
    ];
    assert_eq!(
        encode_with(|writer| encode_expression_arena(writer, &arena)),
        expected
    );
    let mut reader = Reader::new(&expected);
    let decoded = decode_expression_arena(&mut reader).expect("decode");
    assert_eq!(decoded, arena);
    assert_reader_finished(reader);
    assert_eq!(
        encode_with(|writer| encode_expression_arena(writer, &decoded)),
        expected
    );
    for length in 0..expected.len() {
        assert!(decode_expression_arena(&mut Reader::new(&expected[..length])).is_err());
    }
    let mut trailing = expected.clone();
    trailing.push(0xff);
    let mut reader = Reader::new(&trailing);
    decode_expression_arena(&mut reader).expect("canonical prefix decodes");
    assert!(reader.finish().is_err());
    assert!(matches!(
        decode_expression_arena(&mut Reader::new(&[0, 0, 0, 1, 0xff, 0x01])),
        Err(IrValidationError::UnknownTag { .. })
    ));
}

#[test]
fn key_purpose_family_has_one_exact_vector_for_every_tag() {
    fn expected_scalar_key(
        purpose: &[u8],
        schema: &KeySchema,
        nested_entity: Option<&[u8]>,
    ) -> Vec<u8> {
        let mut expected = crate::KEY_CODEC_VERSION_V1.to_be_bytes().to_vec();
        expected.extend_from_slice(purpose);
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.push(0x03); // ValueType::U64
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&8u32.to_be_bytes());
        expected.extend_from_slice(&(schema.maximum_encoded_bytes() as u32).to_be_bytes());
        expected.push(u8::from(nested_entity.is_some()));
        if let Some(nested) = nested_entity {
            expected.extend_from_slice(nested);
        }
        expected
    }

    let component = KeyComponentSchema::new(ValueType::u64(), vec![]).expect("component");
    assert_eq!(component.maximum_payload_bytes(), 8);
    let entity_id = EntityTypeId::new(0x0102_0304).expect("entity ID");
    let aggregate_id = AggregateTypeId::new(0x1112_1314).expect("aggregate ID");
    let entity =
        KeySchema::new(KeyPurpose::Entity(entity_id), vec![component.clone()]).expect("entity key");
    let entity_expected = expected_scalar_key(&[0x01, 1, 2, 3, 4], &entity, None);
    let partition = KeySchema::new(KeyPurpose::Partition(aggregate_id), vec![component.clone()])
        .expect("partition key");
    let conflict = KeySchema::new(KeyPurpose::Conflict(aggregate_id), vec![component.clone()])
        .expect("conflict key");
    let index = KeySchema::index(
        IndexId::new(0x2122_2324).expect("index ID"),
        entity_id,
        vec![component],
        entity.clone(),
    )
    .expect("index key");
    let vectors = vec![
        (entity, entity_expected.clone()),
        (
            partition.clone(),
            expected_scalar_key(&[0x02, 0x11, 0x12, 0x13, 0x14], &partition, None),
        ),
        (
            conflict.clone(),
            expected_scalar_key(&[0x03, 0x11, 0x12, 0x13, 0x14], &conflict, None),
        ),
        (
            index.clone(),
            expected_scalar_key(
                &[0x04, 0x21, 0x22, 0x23, 0x24, 1, 2, 3, 4],
                &index,
                Some(&entity_expected),
            ),
        ),
    ];

    for (schema, expected) in vectors {
        assert_eq!(
            encode_with(|writer| encode_key_schema(writer, &schema)),
            expected
        );
        let mut reader = Reader::new(&expected);
        let decoded = decode_key_schema(&mut reader, 0).expect("decode");
        assert_eq!(decoded, schema);
        assert_reader_finished(reader);
        assert_eq!(
            encode_with(|writer| encode_key_schema(writer, &decoded)),
            expected
        );
        for length in 0..expected.len() {
            assert!(decode_key_schema(&mut Reader::new(&expected[..length]), 0).is_err());
        }
        let mut trailing = expected.clone();
        trailing.push(0xff);
        let mut reader = Reader::new(&trailing);
        decode_key_schema(&mut reader, 0).expect("canonical prefix decodes");
        assert!(reader.finish().is_err());
    }
    assert!(matches!(
        decode_key_schema(&mut Reader::new(&[0, 0, 0, 1, 0xff]), 0),
        Err(IrValidationError::UnknownTag { .. })
    ));
}

#[test]
fn capability_projection_aggregation_and_artifact_key_vectors_are_closed() {
    let command = CommandId::new(0x1112_1314).expect("command ID");
    let lineage = ContractLineage::new("LegalSpend").expect("lineage");
    let capability_bytes = [
        0x01, // InvokeCommand
        0, 0, 0, 10, b'L', b'e', b'g', b'a', b'l', b'S', b'p', b'e', b'n', b'd', // lineage
        0x11, 0x12, 0x13, 0x14, // CommandId
    ];
    let mut reader = Reader::new(&capability_bytes);
    assert_eq!(
        decode_capability_requirement(&mut reader).expect("capability decode"),
        CapabilityRequirement::InvokeCommand {
            lineage,
            command_id: command,
        }
    );
    assert_reader_finished(reader);
    for length in 0..capability_bytes.len() {
        assert!(
            decode_capability_requirement(&mut Reader::new(&capability_bytes[..length])).is_err(),
            "capability accepted truncated prefix of length {length}"
        );
    }
    let mut trailing = capability_bytes.to_vec();
    trailing.push(0xff);
    let mut reader = Reader::new(&trailing);
    decode_capability_requirement(&mut reader).expect("canonical prefix decodes");
    assert!(reader.finish().is_err());
    assert!(matches!(
        decode_capability_requirement(&mut Reader::new(&[0xff])),
        Err(IrValidationError::UnknownTag { .. })
    ));
    assert!(
        decode_capability_requirement(&mut Reader::new(&[
            0x01, 0, 0, 0, 0, 0x11, 0x12, 0x13, 0x14,
        ]))
        .is_err()
    );

    let count = ProjectionMeasurePlan::count(
        FieldSchema::new(FieldId::first(), "count", ValueType::u64()).expect("field"),
    )
    .expect("count");
    let count_expected = vec![
        0, 0, 0, 1, // FieldId
        0, 0, 0, 5, b'c', b'o', b'u', b'n', b't', // name
        0x03, // ValueType::U64
        0x01, // count
        0x00, // no expression
    ];
    let sum = ProjectionMeasurePlan::sum(
        FieldSchema::new(FieldId::new(2).expect("field ID"), "sum", ValueType::i64())
            .expect("field"),
        ExprId::new(7),
    )
    .expect("sum");
    let sum_expected = vec![
        0, 0, 0, 2, // FieldId
        0, 0, 0, 3, b's', b'u', b'm', // name
        0x02, // ValueType::I64
        0x02, // sum
        0x01, // expression present
        0, 0, 0, 7, // ExprId
    ];
    for (measure, expected) in [(count, count_expected), (sum, sum_expected)] {
        assert_eq!(
            encode_with(|writer| encode_projection_measure(writer, &measure)),
            expected
        );
        let mut reader = Reader::new(&expected);
        let decoded = decode_projection_measure(&mut reader).expect("measure decode");
        assert_eq!(decoded, measure);
        assert_reader_finished(reader);
        assert_eq!(
            encode_with(|writer| encode_projection_measure(writer, &decoded)),
            expected
        );
        let mut trailing = expected.clone();
        trailing.push(0xff);
        let mut reader = Reader::new(&trailing);
        decode_projection_measure(&mut reader).expect("canonical prefix decodes");
        assert!(reader.finish().is_err());
    }
    assert!(matches!(
        decode_projection_aggregation(0xff),
        Err(IrValidationError::UnknownTag { .. })
    ));

    let artifact_vectors = [
        (
            crate::SchemaArtifactKey::Entity(EntityTypeId::new(0x0102_0304).expect("entity ID")),
            [0x01, 0x01, 0x02, 0x03, 0x04],
        ),
        (
            crate::SchemaArtifactKey::Event(EventTypeId::new(0x0506_0708).expect("event ID")),
            [0x02, 0x05, 0x06, 0x07, 0x08],
        ),
        (
            crate::SchemaArtifactKey::CommandInput(command),
            [0x03, 0x11, 0x12, 0x13, 0x14],
        ),
        (
            crate::SchemaArtifactKey::CommandOutcomeUnion(command),
            [0x04, 0x11, 0x12, 0x13, 0x14],
        ),
        (
            crate::SchemaArtifactKey::ProjectionResult(
                ProjectionId::new(0x2122_2324).expect("projection ID"),
            ),
            [0x05, 0x21, 0x22, 0x23, 0x24],
        ),
    ];
    for (key, expected) in artifact_vectors {
        assert_eq!(key.to_bytes(), expected);
        assert_eq!(key.tag(), expected[0]);
        assert_eq!(key.stable_id().to_be_bytes(), expected[1..]);
    }
}

#[test]
fn instruction_family_has_one_checked_exact_vector_for_every_tag() {
    let budget = ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin"))
        .expect("budget fixture");
    let create = &budget.commands()[0];
    let allocate = &budget.commands()[1];
    let cases = [
        (
            "require",
            0x01,
            "01000000000000000600000003040000000100000003000000010000000100000007",
            &create.instructions()[0],
            create,
        ),
        (
            "set field",
            0x02,
            "02000000000000000300000008",
            &create.instructions()[1],
            create,
        ),
        (
            "emit event",
            0x03,
            "03000000010200000001000000040000000100000017000000020000001600000003000000150000000400000014",
            &allocate.instructions()[4],
            allocate,
        ),
        (
            "return",
            0x04,
            "040000000104000000010000000100000001000000010000000b",
            &create.instructions()[4],
            create,
        ),
    ];

    for (name, tag, expected_hex, instruction, command) in cases {
        let bytes = encode_with(|writer| encode_instruction(writer, instruction));
        assert_eq!(lowercase_hex(&bytes), expected_hex);
        assert_eq!(bytes[0], tag);
        let mut reader = Reader::new(&bytes);
        let decoded = decode_instruction(
            &mut reader,
            command.outcomes(),
            budget.schema(),
            command.expressions(),
        )
        .expect("instruction decode");
        assert_eq!(&decoded, instruction);
        assert_reader_finished(reader);
        assert_eq!(
            encode_with(|writer| encode_instruction(writer, &decoded)),
            bytes
        );
        for length in 0..bytes.len() {
            assert!(
                decode_instruction(
                    &mut Reader::new(&bytes[..length]),
                    command.outcomes(),
                    budget.schema(),
                    command.expressions(),
                )
                .is_err(),
                "{name} accepted truncated prefix of length {length}"
            );
        }
        let mut trailing = bytes;
        trailing.push(0xff);
        let mut reader = Reader::new(&trailing);
        decode_instruction(
            &mut reader,
            command.outcomes(),
            budget.schema(),
            command.expressions(),
        )
        .expect("canonical prefix decodes");
        assert!(reader.finish().is_err());
    }

    let empty_schema = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
    assert!(matches!(
        decode_instruction(
            &mut Reader::new(&[0xff]),
            &[],
            &empty_schema,
            &ExpressionArena::empty(),
        ),
        Err(IrValidationError::UnknownTag { .. })
    ));
}

#[test]
fn expression_and_plan_codec_families_round_trip_typed_sentinels() {
    let arena = ExpressionArena::new(vec![(
        ExpressionKind::Constant(riffdb_types::CanonicalValue::Bool(true)),
        ValueType::bool(),
    )])
    .expect("arena");
    let bytes = encode_with(|writer| encode_expression_arena(writer, &arena));
    let mut reader = Reader::new(&bytes);
    assert_eq!(decode_expression_arena(&mut reader).expect("decode"), arena);
    assert_reader_finished(reader);

    let (plan, schema) = crate::plan::tests::root_validation_mutation(true);
    let bytes = encode_with(|writer| encode_command_bundle_entry(writer, &plan, &schema));
    let mut reader = Reader::new(&bytes);
    assert_eq!(
        decode_command(&mut reader, plan.required_capability().lineage(), &schema,)
            .expect("decode"),
        plan
    );
    assert_reader_finished(reader);

    let lineage = plan.required_capability().lineage();
    let mut framed_lineage = (lineage.as_bytes().len() as u32).to_be_bytes().to_vec();
    framed_lineage.extend_from_slice(lineage.as_bytes());
    let positions = bytes
        .windows(framed_lineage.len())
        .enumerate()
        .filter_map(|(index, window)| (window == framed_lineage).then_some(index))
        .collect::<Vec<_>>();
    assert_eq!(positions.len(), 1, "capability lineage must occur once");
    let lineage_offset = positions[0] + 4;

    let mut wrong_lineage = bytes.clone();
    wrong_lineage[lineage_offset] ^= 0x20;
    assert!(
        decode_command(
            &mut Reader::new(&wrong_lineage),
            plan.required_capability().lineage(),
            &schema,
        )
        .is_err()
    );

    let mut wrong_command = bytes;
    let command_offset = lineage_offset + lineage.as_bytes().len();
    wrong_command[command_offset + 3] ^= 0x01;
    assert!(
        decode_command(
            &mut Reader::new(&wrong_command),
            plan.required_capability().lineage(),
            &schema,
        )
        .is_err()
    );
}

#[test]
fn ledger_schema_registry_and_compatibility_families_round_trip() {
    let identity = StableIdentity::new(
        StableIdNamespace::new(StableIdNamespaceTag::Command, 0, vec![]).expect("namespace"),
        "Apply",
    )
    .expect("identity");
    let ledger = LineageLedgerV1::genesis(vec![identity]).expect("ledger");
    let bytes = encode_with(|writer| encode_ledger(writer, &ledger));
    let mut reader = Reader::new(&bytes);
    assert_eq!(decode_ledger(&mut reader).expect("decode"), ledger);
    assert_reader_finished(reader);

    let (_, schema) = crate::plan::tests::minimal_mutation();
    let bytes = encode_with(|writer| encode_schema(writer, &schema));
    let mut reader = Reader::new(&bytes);
    assert_eq!(decode_schema(&mut reader).expect("decode"), schema);
    assert_reader_finished(reader);

    let lineage = ContractLineage::new("LegalSpend").expect("lineage");
    let registry = McpCommandNameRegistryV2::new(
        lineage,
        "LegalSpend",
        vec![
            McpCommandNameEntryV2::new(
                CommandId::first(),
                "LegalSpend",
                "Apply",
                "riffdb_cmd_legalspend_apply",
            )
            .expect("entry"),
        ],
    )
    .expect("registry");
    let bytes = encode_with(|writer| encode_mcp_registry(writer, &registry));
    let mut reader = Reader::new(&bytes);
    assert_eq!(decode_mcp_registry(&mut reader).expect("decode"), registry);
    assert_reader_finished(reader);

    let report = CompatibilityReport::successor(vec![
        CompatibilityEntry::new(CompatibilityCode::AddedCommand, "command:1").expect("entry"),
    ])
    .expect("report");
    let bytes = encode_with(|writer| encode_compatibility(writer, &report));
    let mut reader = Reader::new(&bytes);
    assert_eq!(
        decode_compatibility(&mut reader, true).expect("decode"),
        report
    );
    assert_reader_finished(reader);
}

/// The optional schema extensions share one descending u32 marker namespace
/// consumed one value at a time by concurrent work. The witness registry
/// catches a FORGOTTEN registration; this catches the COLLIDING one — two
/// branches each grabbing the same next-free marker merge cleanly
/// everywhere else and would misparse each other's bundles.
#[test]
fn schema_extension_markers_are_unique_and_strictly_descending() {
    let markers = [
        super::RELATIONSHIP_SCHEMA_EXTENSION,
        super::UNIQUE_KEY_SCHEMA_EXTENSION,
        super::DELETE_POLICY_SCHEMA_EXTENSION,
        super::VECTOR_FIELD_SPEC_SCHEMA_EXTENSION,
        super::INDEX_FIELD_ENCODING_EXTENSION,
        super::SECRET_FIELD_SPEC_SCHEMA_EXTENSION,
    ];
    for pair in markers.windows(2) {
        assert!(
            pair[0] > pair[1],
            "extension markers must stay unique and strictly descending: {:#010x} !> {:#010x}",
            pair[0],
            pair[1]
        );
    }
    // The eight-byte magics read at per-event positions, never at the
    // schema tail, so full-value distinctness is the hard invariant. Their
    // HIGH words additionally consume slots from the shared descending
    // namespace (the legacy partition magic took 0xffff_fffc's word by
    // design; the V7 event-anchor magic took 0xffff_fff9's, which is why
    // the secret extension moved to 0xffff_fff8): every future u32 marker
    // must skip any high word already claimed by a u64 magic.
    let magic_high_words = [
        (super::EVENT_PARTITION_SCHEMA_EXTENSION >> 32) as u32,
        (super::EVENT_POLICY_ANCHOR_SCHEMA_EXTENSION >> 32) as u32,
    ];
    for magic in [
        super::EVENT_PARTITION_SCHEMA_EXTENSION,
        super::EVENT_POLICY_ANCHOR_SCHEMA_EXTENSION,
    ] {
        for marker in markers {
            assert_ne!(u64::from(marker), magic);
        }
    }
    // The legacy fffc reuse predates the rule and is positionally safe;
    // everything after the vector marker must respect it.
    for marker in markers {
        if marker < super::VECTOR_FIELD_SPEC_SCHEMA_EXTENSION {
            assert!(
                !magic_high_words.contains(&marker),
                "u32 marker {marker:#010x} collides with a u64 magic high word"
            );
        }
    }
}
