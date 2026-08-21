//! Declarative registries for the accepted executable-IR and JSON Schema v1 formats.

use std::fmt::Write as _;

/// One named byte tag in a closed registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FormatTag {
    /// Human-readable stable variant label.
    pub(crate) name: &'static str,
    /// Immutable encoded byte.
    pub(crate) value: u8,
}

/// One closed byte-tag registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TagRegistry {
    /// Stable registry label used in generated documentation.
    pub(crate) name: &'static str,
    /// Complete variants in ascending byte order.
    pub(crate) tags: &'static [FormatTag],
}

macro_rules! tag_registry {
    ($module:ident, $label:literal, {$($constant:ident = $value:literal => $name:literal),+ $(,)?}) => {
        /// Closed tag constants and exhaustive registry entries.
        pub(crate) mod $module {
            use super::{FormatTag, TagRegistry};
            $(
                #[doc = concat!("Encoded tag for `", $name, "`.")]
                pub(crate) const $constant: u8 = $value;
            )+
            /// Complete registry entries in ascending encoded order.
            pub(crate) const TAGS: &[FormatTag] = &[
                $(FormatTag { name: $name, value: $constant }),+
            ];
            /// Registry metadata.
            pub(crate) const REGISTRY: TagRegistry = TagRegistry { name: $label, tags: TAGS };
        }
    };
}

tag_registry!(stable_id_namespace, "Stable ID namespace", {
    ENTITY = 0x01 => "entity",
    EVENT = 0x02 => "event",
    ENUM = 0x03 => "enum",
    AGGREGATE = 0x04 => "aggregate",
    COMMAND = 0x05 => "command",
    PROJECTION = 0x06 => "projection",
    INDEX = 0x07 => "index",
    INVARIANT = 0x08 => "invariant",
    FIELD = 0x09 => "field",
    OUTCOME = 0x0a => "outcome",
    ENUM_VARIANT = 0x0b => "enum variant",
});
tag_registry!(lineage_entry_state, "Lineage entry state", {
    ACTIVE = 0x01 => "active",
    TOMBSTONE = 0x02 => "tombstone",
});
tag_registry!(record_reference, "Record reference", {
    ENTITY = 0x01 => "entity",
    EVENT = 0x02 => "event",
    COMMAND_INPUT = 0x03 => "command input",
    COMMAND_OUTCOME = 0x04 => "command outcome",
    PROJECTION_RESULT = 0x05 => "projection result",
});
tag_registry!(value_type, "Value type", {
    BOOL = 0x01 => "bool",
    I64 = 0x02 => "i64",
    U64 = 0x03 => "u64",
    DECIMAL = 0x04 => "decimal",
    MONEY = 0x05 => "money",
    STRING = 0x06 => "string",
    BYTES = 0x07 => "bytes",
    TIMESTAMP = 0x08 => "timestamp",
    DATE = 0x09 => "date",
    UUID = 0x0a => "uuid",
    ENUM = 0x0b => "enum",
    OPTIONAL = 0x0c => "optional",
    LIST = 0x0d => "list",
    RECORD = 0x0e => "record",
    VECTOR = 0x0f => "vector",
});
tag_registry!(expression, "Expression", {
    CONSTANT = 0x01 => "constant",
    INPUT_FIELD = 0x02 => "input field",
    COMPLETE_BINDING = 0x03 => "complete binding",
    BOUND_FIELD = 0x04 => "bound field",
    SCHEMA_FIELD = 0x05 => "schema field",
    SOURCE_EVENT_FIELD = 0x06 => "source-event field",
    TRANSACTION_TIME = 0x07 => "tx.time",
    TRANSACTION_DATE = 0x08 => "tx.date",
    UNARY = 0x09 => "unary",
    BINARY = 0x0a => "binary",
    ROOT_VALIDATION_FIELD = 0x0b => "root-validation field",
    SERVICE_VALUE = 0x0c => "service-owned command value",
    COLLECTION_ELEMENT = 0x0d => "collection element",
    COLLECTION_ELEMENT_FIELD = 0x0e => "collection element field",
});
tag_registry!(unary_operator, "Unary operator", {
    NOT = 0x01 => "not",
    NEGATE = 0x02 => "negate",
});
tag_registry!(binary_operator, "Binary operator", {
    MULTIPLY = 0x01 => "multiply",
    DIVIDE = 0x02 => "divide",
    ADD = 0x03 => "add",
    SUBTRACT = 0x04 => "subtract",
    EQUAL = 0x05 => "equal",
    NOT_EQUAL = 0x06 => "not-equal",
    LESS = 0x07 => "less",
    LESS_EQUAL = 0x08 => "less-equal",
    GREATER = 0x09 => "greater",
    GREATER_EQUAL = 0x0a => "greater-equal",
    AND = 0x0b => "and",
    OR = 0x0c => "or",
});
tag_registry!(key_purpose, "Key purpose", {
    ENTITY = 0x01 => "entity",
    PARTITION = 0x02 => "partition",
    CONFLICT = 0x03 => "conflict",
    INDEX = 0x04 => "index",
});
tag_registry!(binding_mode, "Binding mode", {
    READ = 0x01 => "read",
    MUTATE = 0x02 => "mutate",
    CREATE = 0x03 => "create",
    DELETE = 0x04 => "delete",
});
tag_registry!(delete_policy_mode, "Delete policy mode", {
    NO_INBOUND = 0x01 => "no inbound relationship",
    RESTRICT = 0x02 => "indexed restrict",
    CASCADE = 0x03 => "bounded one-hop cascade",
});
tag_registry!(delete_check_mode, "Delete check mode", {
    NO_INBOUND = 0x01 => "no inbound relationship",
    RESTRICT = 0x02 => "transaction-current indexed restrict",
    CASCADE = 0x03 => "transaction-current bounded one-hop cascade",
});
tag_registry!(instruction, "Instruction", {
    REQUIRE = 0x01 => "require",
    SET_FIELD = 0x02 => "set field",
    EMIT_EVENT = 0x03 => "emit event",
    RETURN = 0x04 => "return",
    WORKFLOW_TRANSITION = 0x05 => "workflow transition",
    WORKFLOW_LEASE = 0x06 => "workflow lease",
});
tag_registry!(workflow_lease_operation, "Workflow lease operation", {
    CLAIM = 0x01 => "claim",
    RENEW = 0x02 => "renew",
    RELEASE = 0x03 => "release",
    EXPIRE = 0x04 => "expire",
    FENCE = 0x05 => "fence",
});
tag_registry!(service_value_kind, "Service-owned command value", {
    UUID_V7 = 0x01 => "uuid v7",
    TRANSACTION_TIME = 0x02 => "transaction time",
});
tag_registry!(execution_class, "Execution class", {
    READ_ONLY = 0x01 => "read-only",
    IDEMPOTENT_MUTATION = 0x02 => "idempotent mutation",
});
tag_registry!(command_invocation_class, "Command invocation class", {
    APPLICATION = 0x01 => "application",
    REIMPORT = 0x02 => "operator-only reimport",
});
tag_registry!(secret_reveal_destination, "Secret reveal destination", {
    ENTITY_FIELD = 0x01 => "entity field",
    EVENT_FIELD = 0x02 => "event field",
    OUTCOME_FIELD = 0x03 => "outcome field",
});
tag_registry!(retry_policy, "Retry policy", {
    BOUNDED_FULL_REEVALUATION = 0x01 => "bounded full reevaluation",
});
tag_registry!(capability_requirement, "Capability requirement", {
    INVOKE_COMMAND = 0x01 => "invoke command",
});
tag_registry!(projection_aggregation, "Projection aggregation", {
    COUNT = 0x01 => "count",
    SUM = 0x02 => "sum",
});
tag_registry!(projection_frontier, "Projection frontier", {
    TRANSACTIONALLY_ORDERED = 0x01 => "transactionally ordered",
});
tag_registry!(compatibility_class, "Compatibility class", {
    COMPATIBLE = 0x01 => "compatible",
    REQUIRES_EXPLICIT_VERSION = 0x02 => "explicit version",
    INCOMPATIBLE = 0x03 => "incompatible",
    REQUIRES_MIGRATION = 0x04 => "migration required",
});
tag_registry!(migration_step, "Migration step", {
    RENAME_IDENTITY = 0x01 => "rename identity",
    RETIRE_IDENTITY = 0x02 => "retire identity",
    SET_FIELD = 0x03 => "set field",
    REPLACE_FIELD = 0x04 => "replace field",
    REQUIRE_ENTITY = 0x05 => "require entity",
    REKEY_ENTITY = 0x06 => "rekey entity",
    MAP_ENUM = 0x07 => "map enum",
    REBUILD_INDEX = 0x08 => "rebuild index",
    VALIDATE_RELATIONSHIP = 0x09 => "validate relationship",
    VALIDATE_UNIQUE = 0x0a => "validate unique",
    VALIDATE_INVARIANT = 0x0b => "validate invariant",
    REBUILD_PROJECTION = 0x0c => "rebuild projection",
    ACKNOWLEDGE_REPARTITION = 0x0d => "acknowledge repartition",
    ACKNOWLEDGE_AGGREGATE = 0x0e => "acknowledge aggregate",
    ACKNOWLEDGE_CONFLICT = 0x0f => "acknowledge conflict",
});
tag_registry!(migration_conversion, "Migration conversion", {
    IDENTITY = 0x01 => "identity",
    WRAP_OPTIONAL = 0x02 => "wrap optional",
    ASSERT_UNWRAP_OPTIONAL = 0x03 => "assert unwrap optional",
    CHECKED_I64_TO_U64 = 0x04 => "checked i64 to u64",
    CHECKED_U64_TO_I64 = 0x05 => "checked u64 to i64",
    EXACT_DECIMAL = 0x06 => "exact decimal",
    ASSERT_BOUNDED_NARROW = 0x07 => "assert bounded narrow",
    LIST_ELEMENTS = 0x08 => "list elements",
    UUID_TO_STRING = 0x09 => "UUID to string",
    STRING_TO_UUID = 0x0a => "string to UUID",
});
tag_registry!(schema_artifact, "Schema artifact", {
    ENTITY = 0x01 => "entity record",
    EVENT = 0x02 => "durable event payload",
    COMMAND_INPUT = 0x03 => "command input",
    COMMAND_OUTCOME_UNION = 0x04 => "command outcome union",
    PROJECTION_RESULT = 0x05 => "projection result row",
});
tag_registry!(record_owner, "Record allocation owner", {
    ENTITY = 0x01 => "entity",
    EVENT = 0x02 => "event",
    COMMAND_INPUT = 0x03 => "command input",
    COMMAND_OUTCOME = 0x04 => "command outcome",
    PROJECTION_RESULT = 0x05 => "projection result",
    COMMAND_SERVICE_VALUE = 0x06 => "command service value",
});
tag_registry!(invariant_owner, "Invariant identity owner", {
    ENTITY = 0x01 => "entity",
    AGGREGATE = 0x02 => "aggregate",
});
tag_registry!(index_owner, "Index identity owner", {
    ENTITY = 0x01 => "entity",
});
tag_registry!(outcome_owner, "Outcome allocation owner", {
    COMMAND = 0x01 => "command",
});
tag_registry!(enum_variant_owner, "Enum-variant allocation owner", {
    ENUM = 0x01 => "enum",
});

/// Every closed encoded byte registry in canonical documentation order.
pub(crate) const TAG_REGISTRIES: &[TagRegistry] = &[
    stable_id_namespace::REGISTRY,
    lineage_entry_state::REGISTRY,
    record_reference::REGISTRY,
    value_type::REGISTRY,
    expression::REGISTRY,
    unary_operator::REGISTRY,
    binary_operator::REGISTRY,
    key_purpose::REGISTRY,
    binding_mode::REGISTRY,
    delete_policy_mode::REGISTRY,
    delete_check_mode::REGISTRY,
    instruction::REGISTRY,
    service_value_kind::REGISTRY,
    execution_class::REGISTRY,
    command_invocation_class::REGISTRY,
    secret_reveal_destination::REGISTRY,
    retry_policy::REGISTRY,
    capability_requirement::REGISTRY,
    projection_aggregation::REGISTRY,
    projection_frontier::REGISTRY,
    compatibility_class::REGISTRY,
    migration_step::REGISTRY,
    migration_conversion::REGISTRY,
    schema_artifact::REGISTRY,
    record_owner::REGISTRY,
    invariant_owner::REGISTRY,
    index_owner::REGISTRY,
    outcome_owner::REGISTRY,
    enum_variant_owner::REGISTRY,
];

/// One field in a canonical nested encoding layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LayoutField {
    /// Stable field label.
    pub(crate) name: &'static str,
    /// Exact scalar, optional, collection, or nested-layout encoding.
    pub(crate) encoding: &'static str,
}

/// One complete ordered canonical encoding layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FormatLayout {
    /// Stable layout name.
    pub(crate) name: &'static str,
    /// Fields in byte order.
    pub(crate) fields: &'static [LayoutField],
}

/// One exact payload selected by a closed union tag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TaggedVariantLayout {
    /// Immutable encoded tag.
    pub(crate) tag: u8,
    /// Stable variant label, equal to the tag-registry label.
    pub(crate) name: &'static str,
    /// Payload fields immediately following the tag, in exact byte order.
    pub(crate) fields: &'static [LayoutField],
}

/// One complete tag-to-payload mapping for a parameterized closed union.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TaggedUnionLayout {
    /// Stable encoded union name.
    pub(crate) name: &'static str,
    /// Closed tag registry selected by this union.
    pub(crate) tags: TagRegistry,
    /// One payload layout for every tag, in tag order.
    pub(crate) variants: &'static [TaggedVariantLayout],
}

macro_rules! layout {
    ($constant:ident, $name:literal, {$($field:literal => $encoding:literal),+ $(,)?}) => {
        /// Ordered canonical layout.
        pub(crate) const $constant: FormatLayout = FormatLayout {
            name: $name,
            fields: &[$(LayoutField { name: $field, encoding: $encoding }),+],
        };
    };
}

macro_rules! tagged_variant {
    ($tag:expr, $name:literal, {$($field:literal => $encoding:literal),* $(,)?}) => {
        TaggedVariantLayout {
            tag: $tag,
            name: $name,
            fields: &[$(LayoutField { name: $field, encoding: $encoding }),*],
        }
    };
}

/// Exact `RecordTypeRef` payloads after the record-reference tag.
pub(crate) const RECORD_REFERENCE_VARIANTS: &[TaggedVariantLayout] = &[
    tagged_variant!(record_reference::ENTITY, "entity", {
        "entity_type" => "EntityTypeId as u32",
    }),
    tagged_variant!(record_reference::EVENT, "event", {
        "event_type" => "EventTypeId as u32",
    }),
    tagged_variant!(record_reference::COMMAND_INPUT, "command input", {
        "command_id" => "CommandId as u32",
    }),
    tagged_variant!(record_reference::COMMAND_OUTCOME, "command outcome", {
        "command_id" => "CommandId as u32",
        "outcome_id" => "OutcomeId as u32",
    }),
    tagged_variant!(record_reference::PROJECTION_RESULT, "projection result", {
        "projection_id" => "ProjectionId as u32",
    }),
];

/// Exact `ValueType` payloads after the value-type tag.
pub(crate) const VALUE_TYPE_VARIANTS: &[TaggedVariantLayout] = &[
    tagged_variant!(value_type::BOOL, "bool", {}),
    tagged_variant!(value_type::I64, "i64", {}),
    tagged_variant!(value_type::U64, "u64", {}),
    tagged_variant!(value_type::DECIMAL, "decimal", {
        "precision" => "u8 in 1..=38",
        "scale" => "u8 in 0..=precision",
    }),
    tagged_variant!(value_type::MONEY, "money", {
        "currency" => "exactly 3 uppercase ASCII bytes",
    }),
    tagged_variant!(value_type::STRING, "string", {
        "maximum_utf8_bytes" => "u32",
    }),
    tagged_variant!(value_type::BYTES, "bytes", {
        "maximum_bytes" => "u32",
    }),
    tagged_variant!(value_type::TIMESTAMP, "timestamp", {}),
    tagged_variant!(value_type::DATE, "date", {}),
    tagged_variant!(value_type::UUID, "uuid", {}),
    tagged_variant!(value_type::ENUM, "enum", {
        "enum_type" => "EnumTypeId as u32",
    }),
    tagged_variant!(value_type::OPTIONAL, "optional", {
        "inner_type" => "recursive ValueType",
    }),
    tagged_variant!(value_type::LIST, "list", {
        "element_type" => "recursive ValueType",
        "maximum_entries" => "u32",
    }),
    tagged_variant!(value_type::RECORD, "record", {
        "record_type" => "RecordTypeRef tag plus exact selected payload",
    }),
    tagged_variant!(value_type::VECTOR, "vector", {
        "dimension" => "u32 in 1..=4096",
    }),
];

/// Exact `KeyPurpose` payloads after the key-purpose tag.
pub(crate) const KEY_PURPOSE_VARIANTS: &[TaggedVariantLayout] = &[
    tagged_variant!(key_purpose::ENTITY, "entity", {
        "entity_type" => "EntityTypeId as u32",
    }),
    tagged_variant!(key_purpose::PARTITION, "partition", {
        "aggregate_id" => "AggregateTypeId as u32",
    }),
    tagged_variant!(key_purpose::CONFLICT, "conflict", {
        "aggregate_id" => "AggregateTypeId as u32",
    }),
    tagged_variant!(key_purpose::INDEX, "index", {
        "index_id" => "IndexId as u32",
        "entity_type" => "EntityTypeId as u32",
    }),
];

/// Exact `TypedExpression` payloads after the expression tag.
pub(crate) const EXPRESSION_VARIANTS: &[TaggedVariantLayout] = &[
    tagged_variant!(expression::CONSTANT, "constant", {
        "result_type" => "ValueType tag plus exact selected payload",
        "canonical_value" => "u32 byte length + one complete ADR-0011 canonical-value v1 document",
    }),
    tagged_variant!(expression::INPUT_FIELD, "input field", {
        "result_type" => "ValueType tag plus exact selected payload",
        "field" => "FieldId as u32",
    }),
    tagged_variant!(expression::COMPLETE_BINDING, "complete binding", {
        "result_type" => "ValueType tag plus exact selected payload",
        "binding" => "BindingId as u32",
    }),
    tagged_variant!(expression::BOUND_FIELD, "bound field", {
        "result_type" => "ValueType tag plus exact selected payload",
        "binding" => "BindingId as u32",
        "field" => "FieldId as u32",
    }),
    tagged_variant!(expression::SCHEMA_FIELD, "schema field", {
        "result_type" => "ValueType tag plus exact selected payload",
        "entity_type" => "EntityTypeId as u32",
        "field" => "FieldId as u32",
    }),
    tagged_variant!(expression::SOURCE_EVENT_FIELD, "source-event field", {
        "result_type" => "ValueType tag plus exact selected payload",
        "field" => "FieldId as u32",
    }),
    tagged_variant!(expression::TRANSACTION_TIME, "tx.time", {
        "result_type" => "ValueType tag plus exact selected payload",
    }),
    tagged_variant!(expression::TRANSACTION_DATE, "tx.date", {
        "result_type" => "ValueType tag plus exact selected payload",
    }),
    tagged_variant!(expression::UNARY, "unary", {
        "result_type" => "ValueType tag plus exact selected payload",
        "operator" => "Unary operator tag as u8",
        "operand" => "ExprId as u32",
    }),
    tagged_variant!(expression::BINARY, "binary", {
        "result_type" => "ValueType tag plus exact selected payload",
        "operator" => "Binary operator tag as u8",
        "left" => "ExprId as u32",
        "right" => "ExprId as u32",
    }),
    tagged_variant!(expression::ROOT_VALIDATION_FIELD, "root-validation field", {
        "result_type" => "ValueType tag plus exact selected payload",
        "read" => "RootValidationReadId as u32",
        "field" => "FieldId as u32",
    }),
    tagged_variant!(expression::SERVICE_VALUE, "service-owned command value", {
        "result_type" => "ValueType tag plus exact selected payload",
        "field" => "FieldId as u32",
    }),
    tagged_variant!(expression::COLLECTION_ELEMENT, "collection element", {
        "result_type" => "ValueType tag plus exact selected payload",
    }),
    tagged_variant!(expression::COLLECTION_ELEMENT_FIELD, "collection element field", {
        "result_type" => "ValueType tag plus exact selected payload",
        "field" => "FieldId as u32",
    }),
];

/// Exact `Instruction` payloads after the instruction tag.
pub(crate) const INSTRUCTION_VARIANTS: &[TaggedVariantLayout] = &[
    tagged_variant!(instruction::REQUIRE, "require", {
        "requirement_index" => "u32",
        "predicate" => "ExprId as u32",
        "reject" => "OutcomeConstruction",
    }),
    tagged_variant!(instruction::SET_FIELD, "set field", {
        "binding" => "BindingId as u32",
        "field" => "FieldId as u32",
        "value" => "ExprId as u32",
    }),
    tagged_variant!(instruction::EMIT_EVENT, "emit event", {
        "event" => "EventConstruction",
    }),
    tagged_variant!(instruction::RETURN, "return", {
        "outcome" => "OutcomeConstruction",
    }),
    tagged_variant!(instruction::WORKFLOW_TRANSITION, "workflow transition", {
        "binding" => "BindingId as u32",
        "state_field" => "FieldId as u32",
        "source_states" => "u32 count + EnumVariantId[]",
        "destination" => "EnumVariantId as u32",
        "expected_revision" => "ExprId as u32",
        "stale" => "OutcomeConstruction",
        "illegal" => "OutcomeConstruction",
    }),
    tagged_variant!(instruction::WORKFLOW_LEASE, "workflow lease", {
        "binding" => "BindingId as u32",
        "fields" => "WorkflowLeaseFields",
        "operation" => "tagged WorkflowLeaseOperation",
    }),
];

/// Exact `WorkflowLeaseOperation` payloads after the operation tag.
pub(crate) const WORKFLOW_LEASE_OPERATION_VARIANTS: &[TaggedVariantLayout] = &[
    tagged_variant!(workflow_lease_operation::CLAIM, "claim", {
        "owner" => "ExprId", "duration_seconds" => "ExprId", "expected_revision" => "ExprId",
        "stale" => "OutcomeConstruction", "unavailable" => "OutcomeConstruction", "invalid" => "OutcomeConstruction", "exhausted" => "OutcomeConstruction",
    }),
    tagged_variant!(workflow_lease_operation::RENEW, "renew", {
        "owner" => "ExprId", "fencing_token" => "ExprId", "duration_seconds" => "ExprId", "expected_revision" => "ExprId",
        "stale" => "OutcomeConstruction", "invalid" => "OutcomeConstruction", "expired" => "OutcomeConstruction", "exhausted" => "OutcomeConstruction",
    }),
    tagged_variant!(workflow_lease_operation::RELEASE, "release", {
        "owner" => "ExprId", "fencing_token" => "ExprId", "expected_revision" => "ExprId", "stale" => "OutcomeConstruction", "invalid" => "OutcomeConstruction",
    }),
    tagged_variant!(workflow_lease_operation::EXPIRE, "expire", {
        "expected_revision" => "ExprId", "stale" => "OutcomeConstruction", "active" => "OutcomeConstruction",
    }),
    tagged_variant!(workflow_lease_operation::FENCE, "fence", {
        "owner" => "ExprId", "fencing_token" => "ExprId", "expected_revision" => "ExprId", "stale" => "OutcomeConstruction", "invalid" => "OutcomeConstruction", "expired" => "OutcomeConstruction",
    }),
];

/// Exact capability payloads after the capability-requirement tag.
pub(crate) const CAPABILITY_REQUIREMENT_VARIANTS: &[TaggedVariantLayout] = &[tagged_variant!(
    capability_requirement::INVOKE_COMMAND,
    "invoke command",
    {
        "lineage" => "string",
        "command_id" => "CommandId as u32"
    }
)];

/// Exact projection-measure payloads after the aggregation tag.
pub(crate) const PROJECTION_AGGREGATION_VARIANTS: &[TaggedVariantLayout] = &[
    tagged_variant!(projection_aggregation::COUNT, "count", {
        "expression_present" => "Boolean = 0x00",
    }),
    tagged_variant!(projection_aggregation::SUM, "sum", {
        "expression_present" => "Boolean = 0x01",
        "expression" => "ExprId as u32",
    }),
];

/// Exact schema-artifact key payloads after the artifact tag.
pub(crate) const SCHEMA_ARTIFACT_KEY_VARIANTS: &[TaggedVariantLayout] = &[
    tagged_variant!(schema_artifact::ENTITY, "entity record", {
        "entity_type" => "EntityTypeId as u32",
    }),
    tagged_variant!(schema_artifact::EVENT, "durable event payload", {
        "event_type" => "EventTypeId as u32",
    }),
    tagged_variant!(schema_artifact::COMMAND_INPUT, "command input", {
        "command_id" => "CommandId as u32",
    }),
    tagged_variant!(schema_artifact::COMMAND_OUTCOME_UNION, "command outcome union", {
        "command_id" => "CommandId as u32",
    }),
    tagged_variant!(schema_artifact::PROJECTION_RESULT, "projection result row", {
        "projection_id" => "ProjectionId as u32",
    }),
];

/// Every parameterized tagged union in canonical documentation order.
pub(crate) const TAGGED_UNION_LAYOUTS: &[TaggedUnionLayout] = &[
    TaggedUnionLayout {
        name: "RecordTypeRef",
        tags: record_reference::REGISTRY,
        variants: RECORD_REFERENCE_VARIANTS,
    },
    TaggedUnionLayout {
        name: "ValueType",
        tags: value_type::REGISTRY,
        variants: VALUE_TYPE_VARIANTS,
    },
    TaggedUnionLayout {
        name: "KeyPurpose",
        tags: key_purpose::REGISTRY,
        variants: KEY_PURPOSE_VARIANTS,
    },
    TaggedUnionLayout {
        name: "TypedExpression",
        tags: expression::REGISTRY,
        variants: EXPRESSION_VARIANTS,
    },
    TaggedUnionLayout {
        name: "Instruction",
        tags: instruction::REGISTRY,
        variants: INSTRUCTION_VARIANTS,
    },
    TaggedUnionLayout {
        name: "WorkflowLeaseOperation",
        tags: workflow_lease_operation::REGISTRY,
        variants: WORKFLOW_LEASE_OPERATION_VARIANTS,
    },
    TaggedUnionLayout {
        name: "CapabilityRequirement",
        tags: capability_requirement::REGISTRY,
        variants: CAPABILITY_REQUIREMENT_VARIANTS,
    },
    TaggedUnionLayout {
        name: "ProjectionAggregation",
        tags: projection_aggregation::REGISTRY,
        variants: PROJECTION_AGGREGATION_VARIANTS,
    },
    TaggedUnionLayout {
        name: "SchemaArtifactKey",
        tags: schema_artifact::REGISTRY,
        variants: SCHEMA_ARTIFACT_KEY_VARIANTS,
    },
];

/// One exact lineage allocation/identity owner-path rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LineageOwnerRule {
    /// Stable namespace tag.
    pub(crate) namespace_tag: u8,
    /// Human-readable namespace variant.
    pub(crate) namespace: &'static str,
    /// Allocation-state owner tag (`0x00` means no owner).
    pub(crate) allocation_owner_kind: u8,
    /// Exact allocation-state owner-ID count.
    pub(crate) allocation_owner_count: u8,
    /// Identity owner tag (`0x00` means no owner).
    pub(crate) identity_owner_kind: u8,
    /// Exact identity owner-ID count.
    pub(crate) identity_owner_count: u8,
    /// Contextual identity-owner meaning.
    pub(crate) identity_owner: &'static str,
}

/// Complete namespace-to-allocation/identity owner matrix.
pub(crate) const LINEAGE_OWNER_RULES: &[LineageOwnerRule] = &[
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::ENTITY,
        namespace: "entity",
        allocation_owner_kind: 0,
        allocation_owner_count: 0,
        identity_owner_kind: 0,
        identity_owner_count: 0,
        identity_owner: "none",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::EVENT,
        namespace: "event",
        allocation_owner_kind: 0,
        allocation_owner_count: 0,
        identity_owner_kind: 0,
        identity_owner_count: 0,
        identity_owner: "none",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::ENUM,
        namespace: "enum",
        allocation_owner_kind: 0,
        allocation_owner_count: 0,
        identity_owner_kind: 0,
        identity_owner_count: 0,
        identity_owner: "none",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::AGGREGATE,
        namespace: "aggregate",
        allocation_owner_kind: 0,
        allocation_owner_count: 0,
        identity_owner_kind: 0,
        identity_owner_count: 0,
        identity_owner: "none",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::COMMAND,
        namespace: "command",
        allocation_owner_kind: 0,
        allocation_owner_count: 0,
        identity_owner_kind: 0,
        identity_owner_count: 0,
        identity_owner: "none",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::PROJECTION,
        namespace: "projection",
        allocation_owner_kind: 0,
        allocation_owner_count: 0,
        identity_owner_kind: 0,
        identity_owner_count: 0,
        identity_owner: "none",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::INDEX,
        namespace: "index",
        allocation_owner_kind: 0,
        allocation_owner_count: 0,
        identity_owner_kind: index_owner::ENTITY,
        identity_owner_count: 1,
        identity_owner: "entity",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::INVARIANT,
        namespace: "invariant",
        allocation_owner_kind: 0,
        allocation_owner_count: 0,
        identity_owner_kind: invariant_owner::ENTITY,
        identity_owner_count: 1,
        identity_owner: "entity",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::INVARIANT,
        namespace: "invariant",
        allocation_owner_kind: 0,
        allocation_owner_count: 0,
        identity_owner_kind: invariant_owner::AGGREGATE,
        identity_owner_count: 1,
        identity_owner: "aggregate",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::FIELD,
        namespace: "field",
        allocation_owner_kind: record_owner::ENTITY,
        allocation_owner_count: 1,
        identity_owner_kind: record_owner::ENTITY,
        identity_owner_count: 1,
        identity_owner: "entity",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::FIELD,
        namespace: "field",
        allocation_owner_kind: record_owner::EVENT,
        allocation_owner_count: 1,
        identity_owner_kind: record_owner::EVENT,
        identity_owner_count: 1,
        identity_owner: "event",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::FIELD,
        namespace: "field",
        allocation_owner_kind: record_owner::COMMAND_INPUT,
        allocation_owner_count: 1,
        identity_owner_kind: record_owner::COMMAND_INPUT,
        identity_owner_count: 1,
        identity_owner: "command input",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::FIELD,
        namespace: "field",
        allocation_owner_kind: record_owner::COMMAND_SERVICE_VALUE,
        allocation_owner_count: 1,
        identity_owner_kind: record_owner::COMMAND_SERVICE_VALUE,
        identity_owner_count: 1,
        identity_owner: "command service value",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::FIELD,
        namespace: "field",
        allocation_owner_kind: record_owner::COMMAND_OUTCOME,
        allocation_owner_count: 2,
        identity_owner_kind: record_owner::COMMAND_OUTCOME,
        identity_owner_count: 2,
        identity_owner: "command outcome",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::FIELD,
        namespace: "field",
        allocation_owner_kind: record_owner::PROJECTION_RESULT,
        allocation_owner_count: 1,
        identity_owner_kind: record_owner::PROJECTION_RESULT,
        identity_owner_count: 1,
        identity_owner: "projection result",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::OUTCOME,
        namespace: "outcome",
        allocation_owner_kind: outcome_owner::COMMAND,
        allocation_owner_count: 1,
        identity_owner_kind: outcome_owner::COMMAND,
        identity_owner_count: 1,
        identity_owner: "command",
    },
    LineageOwnerRule {
        namespace_tag: stable_id_namespace::ENUM_VARIANT,
        namespace: "enum variant",
        allocation_owner_kind: enum_variant_owner::ENUM,
        allocation_owner_count: 1,
        identity_owner_kind: enum_variant_owner::ENUM,
        identity_owner_count: 1,
        identity_owner: "enum",
    },
];

/// One closed compatibility-code encoding and required class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CompatibilityCodeFormat {
    /// Exact encoded ASCII code.
    pub(crate) code: &'static str,
    /// Required compatibility-class tag.
    pub(crate) class_tag: u8,
    /// Stable human-readable meaning.
    pub(crate) meaning: &'static str,
}

/// Complete compatibility-code registry in ASCII code order.
pub(crate) const COMPATIBILITY_CODES: &[CompatibilityCodeFormat] = &[
    CompatibilityCodeFormat {
        code: "RDB-K001",
        class_tag: compatibility_class::COMPATIBLE,
        meaning: "no semantic change",
    },
    CompatibilityCodeFormat {
        code: "RDB-K010",
        class_tag: compatibility_class::COMPATIBLE,
        meaning: "added command",
    },
    CompatibilityCodeFormat {
        code: "RDB-K011",
        class_tag: compatibility_class::COMPATIBLE,
        meaning: "added event",
    },
    CompatibilityCodeFormat {
        code: "RDB-K012",
        class_tag: compatibility_class::COMPATIBLE,
        meaning: "added projection",
    },
    CompatibilityCodeFormat {
        code: "RDB-K013",
        class_tag: compatibility_class::COMPATIBLE,
        meaning: "added optional field",
    },
    CompatibilityCodeFormat {
        code: "RDB-K014",
        class_tag: compatibility_class::COMPATIBLE,
        meaning: "added enum",
    },
    CompatibilityCodeFormat {
        code: "RDB-K015",
        class_tag: compatibility_class::COMPATIBLE,
        meaning: "added entity",
    },
    CompatibilityCodeFormat {
        code: "RDB-K016",
        class_tag: compatibility_class::COMPATIBLE,
        meaning: "added aggregate containing only newly added entities",
    },
    CompatibilityCodeFormat {
        code: "RDB-K020",
        class_tag: compatibility_class::REQUIRES_EXPLICIT_VERSION,
        meaning: "added outcome",
    },
    CompatibilityCodeFormat {
        code: "RDB-K021",
        class_tag: compatibility_class::REQUIRES_EXPLICIT_VERSION,
        meaning: "added optional outcome field",
    },
    CompatibilityCodeFormat {
        code: "RDB-K022",
        class_tag: compatibility_class::REQUIRES_EXPLICIT_VERSION,
        meaning: "added enum variant",
    },
    CompatibilityCodeFormat {
        code: "RDB-K023",
        class_tag: compatibility_class::REQUIRES_EXPLICIT_VERSION,
        meaning: "added checked deletion policy",
    },
    CompatibilityCodeFormat {
        code: "RDB-K030",
        class_tag: compatibility_class::REQUIRES_MIGRATION,
        meaning: "added required field",
    },
    CompatibilityCodeFormat {
        code: "RDB-K031",
        class_tag: compatibility_class::REQUIRES_MIGRATION,
        meaning: "added index over existing state",
    },
    CompatibilityCodeFormat {
        code: "RDB-K032",
        class_tag: compatibility_class::REQUIRES_MIGRATION,
        meaning: "added relationship over existing state",
    },
    CompatibilityCodeFormat {
        code: "RDB-K033",
        class_tag: compatibility_class::REQUIRES_MIGRATION,
        meaning: "added uniqueness rule over existing state",
    },
    CompatibilityCodeFormat {
        code: "RDB-K034",
        class_tag: compatibility_class::REQUIRES_MIGRATION,
        meaning: "added invariant over existing state",
    },
    CompatibilityCodeFormat {
        code: "RDB-K035",
        class_tag: compatibility_class::REQUIRES_MIGRATION,
        meaning: "added projection requiring historical backfill",
    },
    CompatibilityCodeFormat {
        code: "RDB-K036",
        class_tag: compatibility_class::REQUIRES_MIGRATION,
        meaning: "semantic rename retaining stable ID",
    },
    CompatibilityCodeFormat {
        code: "RDB-K100",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "removed identity",
    },
    CompatibilityCodeFormat {
        code: "RDB-K101",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "tombstone resurrection",
    },
    CompatibilityCodeFormat {
        code: "RDB-K102",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "stable ID reuse",
    },
    CompatibilityCodeFormat {
        code: "RDB-K103",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "type change",
    },
    CompatibilityCodeFormat {
        code: "RDB-K104",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "key-layout change",
    },
    CompatibilityCodeFormat {
        code: "RDB-K105",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "idempotency change",
    },
    CompatibilityCodeFormat {
        code: "RDB-K106",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "partition/conflict change",
    },
    CompatibilityCodeFormat {
        code: "RDB-K107",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "invariant change",
    },
    CompatibilityCodeFormat {
        code: "RDB-K108",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "outcome change",
    },
    CompatibilityCodeFormat {
        code: "RDB-K109",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "event change",
    },
    CompatibilityCodeFormat {
        code: "RDB-K110",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "existing plan change",
    },
    CompatibilityCodeFormat {
        code: "RDB-K111",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "unsupported addition",
    },
    CompatibilityCodeFormat {
        code: "RDB-K112",
        class_tag: compatibility_class::INCOMPATIBLE,
        meaning: "executable IR version change",
    },
];

layout!(BUNDLE_LAYOUT, "ContractBundle", {
    "magic" => "ASCII `RIFFDB-BUNDLE\\0`",
    "bundle_format_version" => "u32 = 1, 2, 3, 4, 5, or 6",
    "grammar_version" => "u32 = 1, 2, 3, 4, 5, or 6; must equal the bundle version",
    "executable_ir_version" => "u32 = 1, 2, 3, 4, 5, or 6; must equal the bundle version",
    "compiler_version" => "nonempty ASCII compiler semantic-version identity string, <=64 bytes",
    "contract_lineage" => "string",
    "contract_version" => "u64",
    "parent" => "optional ParentBundleRef",
    "source_hash" => "32 bytes",
    "plan_root_hash" => "32 bytes",
    "ledger" => "LineageLedgerV1",
    "schema" => "StructuralSchema",
    "workflows" => "IR v2+: u32 count + WorkflowSchema[]; omitted in v1",
    "row_policies" => "IR v4+: RowPolicyCatalogV1; omitted in v1-v3",
    "commands" => "u32 count + CommandBundleEntry[]",
    "projections" => "u32 count + ProjectionBundleEntry[]",
    "schema_artifacts" => "u32 count + GeneratedSchemaArtifact[]",
    "mcp_names" => "McpCommandNameRegistryV2",
    "compatibility" => "CompatibilityReport",
});
layout!(PARENT_LAYOUT, "ParentBundleRef", {
    "contract_version" => "u64",
    "bundle_hash" => "32 bytes",
});
layout!(LEDGER_LAYOUT, "LineageLedgerV1", {
    "version" => "u32 = 1 or 2",
    "allocations" => "u32 count + LineageAllocation[]",
    "aliases" => "ledger v2 only: u32 count + LineageAlias[]",
});
layout!(ALLOCATION_LAYOUT, "LineageAllocation", {
    "namespace_tag" => "Stable ID namespace tag",
    "owner_kind" => "contextual owner tag",
    "owner_ids" => "u8 count + u32[]",
    "max_allocated" => "u32",
    "entries" => "u32 count + LineageEntry[]",
});
layout!(LEDGER_ENTRY_LAYOUT, "LineageEntry", {
    "id" => "u32",
    "identity_owner_kind" => "contextual owner tag",
    "identity_owner_ids" => "u8 count + u32[]",
    "name" => "string",
    "state" => "Lineage entry state tag",
});
layout!(LEDGER_ALIAS_LAYOUT, "LineageAlias", {
    "namespace_tag" => "Stable ID namespace tag",
    "identity_owner_kind" => "contextual owner tag",
    "identity_owner_ids" => "u8 count + u32[]",
    "name" => "string",
    "id" => "u32 existing allocation ID",
});
layout!(SCHEMA_LAYOUT, "StructuralSchema", {
    "entities" => "u32 count + EntitySchema[]",
    "events" => "u32 count + EventSchema[]",
    "enums" => "u32 count + EnumSchema[]",
    "aggregates" => "u32 count + AggregateSchema[]",
    "relationships" => "optional u32 marker 0xfffffffe + u32 count + RelationshipSchema[]; omitted when empty",
    "unique_keys" => "optional u32 marker 0xfffffffd + u32 count + UniqueKeySchema[]; omitted when empty",
    "delete_policies" => "IR v5+: optional u32 marker 0xfffffffc + u32 count + DeletePolicySchemaV1[]; omitted when empty",
    "vector_field_specs" => "IR v6+: optional u32 marker 0xfffffffb + u32 count + VectorFieldSpecV1[]; omitted when empty",
    "secret_field_specs" => "IR v8+: optional u32 marker 0xfffffff8 + u32 count + SecretFieldSpecV1[]; omitted when empty",
    "vector_ann_specs" => "IR v12+: optional u32 marker 0xfffffff7 + u32 count + VectorAnnSpecV1[]; omitted when empty",
});
layout!(RELATIONSHIP_LAYOUT, "RelationshipSchema", {
    "name" => "string",
    "source_entity" => "EntityTypeId",
    "source_fields" => "u32 count + FieldId[]",
    "target_entity" => "EntityTypeId",
    "target_fields" => "u32 count + FieldId[]",
});
layout!(UNIQUE_KEY_LAYOUT, "UniqueKeySchema", {
    "name" => "string",
    "source_entity" => "EntityTypeId",
    "index_id" => "IndexId",
    "fields" => "u32 count + FieldId[]",
});
layout!(DELETE_POLICY_LAYOUT, "DeletePolicySchemaV1", {
    "target_entity" => "EntityTypeId",
    "mode" => "delete policy mode tag",
    "restrict_payload" => "for restrict only: source EntityTypeId + reverse IndexId",
    "cascade_payload" => "for cascade only: u32 count + canonical (source EntityTypeId, relationship string, reverse IndexId, u32 maximum)[]",
});
layout!(VECTOR_FIELD_SPEC_LAYOUT, "VectorFieldSpecV1", {
    "entity" => "EntityTypeId",
    "field" => "FieldId",
    "metric" => "distance metric tag (0x01 cosine, 0x02 euclidean, 0x03 dot_product)",
    "source_fields" => "u32 count + FieldId[]",
    "stale_entity_count_threshold" => "u64 declared stale-entity count threshold",
});
layout!(SECRET_FIELD_SPEC_LAYOUT, "SecretFieldSpecV1", {
    "entity" => "EntityTypeId",
    "field" => "FieldId",
});
layout!(VECTOR_ANN_SPEC_LAYOUT, "VectorAnnSpecV1", {
    "entity" => "EntityTypeId",
    "field" => "FieldId",
    "row_threshold" => "u32 rows per organization (1..=65536)",
    "recall_target_bps" => "u32 basis points (1..=10000)",
});
layout!(VECTOR_PRODUCTION_SPEC_LAYOUT, "VectorProductionSpecV1", {
    "entity" => "EntityTypeId",
    "field" => "FieldId",
    "model_identity" => "string (1..=256 bytes)",
    "current_model_version" => "string (1..=256 bytes)",
    "replay_age_seconds" => "u64 (1..=31536000)",
    "replay_bytes" => "u64 (1..=1099511627776)",
    "replay_backlog" => "u64 (1..=100000000)",
});
layout!(ENTITY_LAYOUT, "EntitySchema", {
    "id" => "u32",
    "name" => "string",
    "record" => "RecordSchema",
    "primary_key_fields" => "u32 count + FieldId[]",
    "primary_key" => "KeySchema",
    "invariants" => "u32 count + InvariantPlan[]",
    "indexes" => "u32 count + IndexSchema[]",
});
layout!(EVENT_LAYOUT, "EventSchema", {
    "id" => "u32",
    "name" => "string",
    "payload" => "RecordSchema",
    "partition" => "optional u64 magic 0xfffffffcffffffff + EventPartitionSchema; omitted when absent",
});
layout!(EVENT_PARTITION_LAYOUT, "EventPartitionSchema", {
    "fields" => "u32 count + FieldId[]",
    "key_schema" => "aggregate-namespaced partition KeySchema",
});
layout!(ENUM_LAYOUT, "EnumSchema", {
    "id" => "u32",
    "name" => "string",
    "variants" => "u32 count + EnumVariantSchema[]",
});
layout!(ENUM_VARIANT_LAYOUT, "EnumVariantSchema", {
    "id" => "u32",
    "name" => "string",
});
layout!(AGGREGATE_LAYOUT, "AggregateSchema", {
    "id" => "u32",
    "name" => "string",
    "root" => "EntityTypeId",
    "children" => "u32 count + EntityTypeId[]",
    "keys" => "AggregateKeyPlan",
    "invariants" => "u32 count + InvariantPlan[]",
});
layout!(AGGREGATE_KEYS_LAYOUT, "AggregateKeyPlan", {
    "expressions" => "ExpressionArena",
    "partition_expression" => "ExprId",
    "conflict_expressions" => "u32 count + ExprId[]",
    "partition_schema" => "KeySchema",
    "conflict_schema" => "KeySchema",
});
layout!(INVARIANT_LAYOUT, "InvariantPlan", {
    "id" => "InvariantId",
    "name" => "string",
    "expressions" => "ExpressionArena",
    "predicate" => "ExprId",
});
layout!(INDEX_LAYOUT, "IndexSchema", {
    "id" => "IndexId",
    "name" => "string",
    "fields" => "u32 count + FieldId[]",
    "key_schema" => "KeySchema",
    "cover_fields" => "IR v14+: extension marker + u32 count + FieldId[]; omitted in v1-v13",
});
layout!(RECORD_LAYOUT, "RecordSchema", {
    "owner" => "RecordTypeRef",
    "fields" => "u32 count + FieldSchema[]",
});
layout!(FIELD_LAYOUT, "FieldSchema", {
    "id" => "FieldId",
    "name" => "string",
    "value_type" => "ValueType",
});
layout!(KEY_SCHEMA_LAYOUT, "KeySchema", {
    "codec_version" => "u32 = 1",
    "purpose" => "KeyPurpose tag plus exact selected payload",
    "components" => "u32 count + KeyComponentSchema[]",
    "maximum_encoded_bytes" => "u32",
    "entity_key_schema" => "optional nested KeySchema",
});
layout!(KEY_COMPONENT_LAYOUT, "KeyComponentSchema", {
    "value_type" => "ValueType",
    "enum_variants" => "u32 count + EnumVariantId[]",
    "maximum_payload_bytes" => "u32",
});
layout!(EXPRESSION_ARENA_LAYOUT, "ExpressionArena", {
    "nodes" => "u32 count + TypedExpression[]",
});
layout!(WORKFLOW_LAYOUT, "WorkflowSchema", {
    "name" => "string",
    "entity" => "EntityTypeId",
    "state_field" => "FieldId",
    "state_enum" => "EnumTypeId",
    "initial_state" => "IR v9+: optional EnumVariantId; omitted in v2-v8",
    "transitions" => "u32 count + WorkflowTransitionSchema[]",
    "lease" => "optional WorkflowLeaseSchema",
});
layout!(WORKFLOW_TRANSITION_LAYOUT, "WorkflowTransitionSchema", {
    "name" => "string",
    "source_states" => "nonempty u32 count + canonical EnumVariantId[]",
    "destination" => "EnumVariantId",
});
layout!(WORKFLOW_LEASE_LAYOUT, "WorkflowLeaseSchema", {
    "name" => "string",
    "owner_field" => "FieldId of optional UUID",
    "expiry_field" => "FieldId of optional timestamp",
    "fencing_token_field" => "FieldId of u64",
    "attempt_field" => "optional FieldId of u64",
    "minimum_duration_seconds" => "nonzero u64",
    "maximum_duration_seconds" => "u64 <= 86400 and >= minimum",
});
layout!(WORKFLOW_LEASE_FIELDS_LAYOUT, "WorkflowLeaseFields", {
    "owner_field" => "FieldId of optional UUID",
    "expiry_field" => "FieldId of optional timestamp",
    "fencing_token_field" => "FieldId of u64",
    "attempt_field" => "optional FieldId of u64",
    "minimum_duration_seconds" => "nonzero u64",
    "maximum_duration_seconds" => "u64 <= 86400 and >= minimum",
});
layout!(COMMAND_ENTRY_LAYOUT, "CommandBundleEntry", {
    "command_id" => "CommandId",
    "name" => "string",
    "contract_version" => "u64",
    "plan_hash" => "32 bytes",
    "semantics" => "CommandSemantics with display names",
});
layout!(COMMAND_SEMANTICS_LAYOUT, "CommandSemantics", {
    "input" => "RecordSchema",
    "service_values" => "IR v2+: u32 count + (FieldId, optional display name, ValueType, ServiceValueKind tag)[]; omitted in v1",
    "outcomes" => "u32 count + OutcomeSchema[]",
    "success_outcome" => "OutcomeId",
    "idempotency_input" => "optional FieldId",
    "input_schema_hash" => "32 bytes",
    "output_schema_hash" => "32 bytes",
    "collection_expansion" => "IR v5+: optional CollectionExpansionPlanV1; omitted in v1-v4",
    "expressions" => "ExpressionArena",
    "bindings" => "u32 count + BindingPlan[]",
    "root_validation_reads" => "u32 count + RootValidationReadPlan[]",
    "relationship_checks" => "u32 count + RelationshipCheckPlan[] when StructuralSchema declares any relationship; otherwise omitted",
    "delete_checks" => "IR v5+: u32 count + DeleteCheckPlanV1[]; omitted in v1-v4",
    "locality" => "LocalityPlan",
    "commit_checks" => "u32 count + CommitCheckPlan[]",
    "instructions" => "u32 count + Instruction[]",
    "secret_reveals" => "IR v11+: u32 count + SecretRevealSpecV1[]; omitted in v1-v10",
    "invocation_class" => "IR v10+: Command invocation class tag; application in v1-v9",
    "execution_class" => "Execution class tag",
    "retry_policy" => "Retry policy tag",
    "required_capability" => "CapabilityRequirement tag plus exact selected payload",
    "entity_closure" => "u32 count + EntitySchema[]",
    "aggregate_closure" => "AggregateSchema",
    "event_closure" => "u32 count + EventSchema[]",
});
layout!(SECRET_REVEAL_SPEC_LAYOUT, "SecretRevealSpecV1", {
    "source_binding" => "BindingId",
    "source_field" => "FieldId",
    "expression" => "ExprId",
    "destination" => "Secret reveal destination tag plus two u32 IDs",
});
layout!(COLLECTION_EXPANSION_LAYOUT, "CollectionExpansionPlanV1", {
    "input_field" => "FieldId of one bounded list command input",
    "minimum_elements" => "u32 in 1..=maximum_elements",
    "maximum_elements" => "u32 <= 256 and equal to the input list maximum",
    "element_type" => "exact ValueType of the list element",
    "first_binding" => "dense BindingId",
    "binding_count" => "nonzero u32 consecutive template bindings",
    "first_instruction" => "dense zero-based u32 instruction position",
    "instruction_count" => "u32 consecutive template instructions; zero is valid for delete-only expansion",
    "duplicate_policy" => "u8 = 0x01 (reject)",
});
layout!(RELATIONSHIP_CHECK_LAYOUT, "RelationshipCheckPlan", {
    "relationship_name" => "string",
    "source_binding" => "BindingId",
    "target_binding" => "BindingId",
});
layout!(DELETE_CHECK_LAYOUT, "DeleteCheckPlanV1", {
    "binding" => "BindingId of one delete binding",
    "mode" => "Delete check mode tag",
    "restrict_source_entity" => "EntityTypeId only for indexed restrict",
    "restrict_index" => "IndexId only for indexed restrict",
    "cascade_payload" => "for cascade only: u32 count + canonical (source EntityTypeId, relationship string, reverse IndexId, u32 maximum)[]",
});
layout!(OUTCOME_SCHEMA_LAYOUT, "OutcomeSchema", {
    "id" => "OutcomeId",
    "name" => "string",
    "payload" => "RecordSchema",
});
layout!(BINDING_LAYOUT, "BindingPlan", {
    "id" => "BindingId",
    "name" => "string",
    "mode" => "Binding mode tag",
    "entity_type" => "EntityTypeId",
    "key_schema" => "KeySchema",
    "key_expressions" => "u32 count + ExprId[]",
    "accessed_fields" => "u32 count + FieldId[]",
    "complete_record_access" => "Boolean",
    "failure" => "OutcomeConstruction",
    "restriction_failure" => "IR v6+: Boolean + optional OutcomeConstruction; omitted in v1-v5",
    "cascade_failure" => "IR v13+: Boolean + optional OutcomeConstruction; omitted in v1-v12",
});
layout!(ROOT_READ_LAYOUT, "RootValidationReadPlan", {
    "id" => "RootValidationReadId",
    "source_binding" => "BindingId",
    "root_entity" => "EntityTypeId",
    "key_schema" => "KeySchema",
    "key_expressions" => "u32 count + ExprId[]",
    "accessed_fields" => "u32 count + FieldId[]",
});
layout!(LOCALITY_LAYOUT, "LocalityPlan", {
    "aggregate_id" => "AggregateTypeId",
    "partition_schema" => "KeySchema",
    "partition_expression" => "ExprId",
    "conflict_derivations" => "u32 count + ConflictDerivationPlan[]",
});
layout!(CONFLICT_LAYOUT, "ConflictDerivationPlan", {
    "schema" => "KeySchema",
    "expressions" => "u32 count + ExprId[]",
});
layout!(COMMIT_CHECK_LAYOUT, "CommitCheckPlan", {
    "invariant_id" => "InvariantId",
    "predicate" => "ExprId",
    "source_bindings" => "u32 count + BindingId[]",
    "root_validation_reads" => "u32 count + RootValidationReadId[]",
});
layout!(OBJECT_LAYOUT, "ObjectConstruction", {
    "record" => "RecordTypeRef",
    "fields" => "u32 count + (FieldId, ExprId)[]",
});
layout!(OUTCOME_CONSTRUCTION_LAYOUT, "OutcomeConstruction", {
    "outcome_id" => "OutcomeId",
    "payload" => "ObjectConstruction",
});
layout!(EVENT_CONSTRUCTION_LAYOUT, "EventConstruction", {
    "event_type" => "EventTypeId",
    "payload" => "ObjectConstruction",
});
layout!(PROJECTION_ENTRY_LAYOUT, "ProjectionBundleEntry", {
    "projection_id" => "ProjectionId",
    "name" => "string",
    "plan_hash" => "32 bytes",
    "semantics" => "ProjectionSemantics",
});
layout!(PROJECTION_SEMANTICS_LAYOUT, "ProjectionSemantics", {
    "source_event" => "EventTypeId",
    "expressions" => "ExpressionArena",
    "filter" => "optional ExprId",
    "key_expressions" => "u32 count + ExprId[]",
    "measures" => "u32 count + ProjectionMeasurePlan[]",
    "frontier" => "Projection frontier tag",
    "group_schema" => "ProjectionGroupSchema",
});
layout!(PROJECTION_MEASURE_LAYOUT, "ProjectionMeasurePlan", {
    "field" => "FieldSchema",
    "aggregation" => "ProjectionAggregation tag plus exact selected payload",
});
layout!(PROJECTION_GROUP_LAYOUT, "ProjectionGroupSchema", {
    "projection_id" => "ProjectionId",
    "codec_version" => "u32 = 1",
    "components" => "u32 count + ProjectionGroupComponentSchema[]",
    "measures" => "RecordSchema",
    "maximum_complete_key_bytes" => "u32",
    "maximum_stored_state_bytes" => "u32",
});
layout!(PROJECTION_COMPONENT_LAYOUT, "ProjectionGroupComponentSchema", {
    "value_type" => "ValueType",
    "enum_variants" => "u32 count + EnumVariantId[]",
    "maximum_framed_bytes" => "u32",
});
layout!(ROW_POLICY_CATALOG_LAYOUT, "RowPolicyCatalogV1", {
    "version" => "u32 = 1",
    "facts" => "u32 count + PrincipalFactSchemaV1[] in symbolic-name order",
    "policies" => "u32 count + RowPolicyPlanV1[] in symbolic-name order",
});
layout!(PRINCIPAL_FACT_SCHEMA_LAYOUT, "PrincipalFactSchemaV1", {
    "name" => "string",
    "value_type" => "scalar ValueType or list<scalar, maximum <= 64>",
});
layout!(ROW_POLICY_PLAN_LAYOUT, "RowPolicyPlanV1", {
    "name" => "string",
    "entity" => "EntityTypeId",
    "rules" => "u32 count + RowPolicyRuleV1[] in operation-tag order",
});
layout!(ROW_POLICY_RULE_LAYOUT, "RowPolicyRuleV1", {
    "operation" => "row-policy operation tag",
    "root" => "u32 topologically ordered node index",
    "nodes" => "u32 count + RowPolicyExpressionNodeV1[]",
});
layout!(ROW_POLICY_NODE_LAYOUT, "RowPolicyExpressionNodeV1", {
    "tag" => "closed row-policy expression tag",
    "payload" => "exact selected operand, node references, or indexed-exists payload",
});
layout!(ROW_POLICY_OPERAND_LAYOUT, "RowPolicyOperandV1", {
    "source" => "closed row-field/principal-id/principal-kind/principal-fact/constant tag and payload",
    "value_type" => "ValueType",
});
layout!(SCHEMA_ARTIFACT_LAYOUT, "GeneratedSchemaArtifact", {
    "key" => "SchemaArtifactKey tag plus exact selected payload",
    "canonical_json" => "bytes",
    "schema_hash" => "32 bytes",
});
layout!(MCP_REGISTRY_LAYOUT, "McpCommandNameRegistryV2", {
    "version" => "u32 = 1",
    "lineage" => "string",
    "source_contract_name" => "string",
    "entries" => "u32 count + McpCommandNameEntryV2[]",
});
layout!(MCP_ENTRY_LAYOUT, "McpCommandNameEntryV2", {
    "command_id" => "CommandId",
    "source_command_name" => "string",
    "tool_name" => "string",
});
layout!(COMPATIBILITY_LAYOUT, "CompatibilityReport", {
    "overall" => "Compatibility class tag",
    "entries" => "u32 count + CompatibilityEntry[]",
});
layout!(COMPATIBILITY_ENTRY_LAYOUT, "CompatibilityEntry", {
    "code" => "string containing one exact closed CompatibilityCode",
    "affected_path" => "canonical StableAffectedPath string",
});
layout!(HASH_ENUM_CLOSURE_LAYOUT, "ReferencedEnumClosure", {
    "enums" => "u32 count + entries in EnumTypeId order",
    "enum_id" => "EnumTypeId",
    "variants" => "u32 count + (EnumVariantId, string name)[] in EnumVariantId order",
});

/// Hash-only ordered layouts that are not embedded as durable bundle records.
pub(crate) const HASH_ONLY_LAYOUTS: &[FormatLayout] = &[HASH_ENUM_CLOSURE_LAYOUT];

/// Every canonical nested layout in top-down traversal order.
pub(crate) const FORMAT_LAYOUTS: &[FormatLayout] = &[
    BUNDLE_LAYOUT,
    PARENT_LAYOUT,
    LEDGER_LAYOUT,
    ALLOCATION_LAYOUT,
    LEDGER_ENTRY_LAYOUT,
    LEDGER_ALIAS_LAYOUT,
    SCHEMA_LAYOUT,
    RELATIONSHIP_LAYOUT,
    UNIQUE_KEY_LAYOUT,
    DELETE_POLICY_LAYOUT,
    VECTOR_FIELD_SPEC_LAYOUT,
    SECRET_FIELD_SPEC_LAYOUT,
    VECTOR_ANN_SPEC_LAYOUT,
    VECTOR_PRODUCTION_SPEC_LAYOUT,
    ENTITY_LAYOUT,
    EVENT_LAYOUT,
    EVENT_PARTITION_LAYOUT,
    ENUM_LAYOUT,
    ENUM_VARIANT_LAYOUT,
    AGGREGATE_LAYOUT,
    AGGREGATE_KEYS_LAYOUT,
    INVARIANT_LAYOUT,
    INDEX_LAYOUT,
    RECORD_LAYOUT,
    FIELD_LAYOUT,
    KEY_SCHEMA_LAYOUT,
    KEY_COMPONENT_LAYOUT,
    EXPRESSION_ARENA_LAYOUT,
    WORKFLOW_LAYOUT,
    WORKFLOW_TRANSITION_LAYOUT,
    WORKFLOW_LEASE_LAYOUT,
    WORKFLOW_LEASE_FIELDS_LAYOUT,
    COMMAND_ENTRY_LAYOUT,
    COMMAND_SEMANTICS_LAYOUT,
    SECRET_REVEAL_SPEC_LAYOUT,
    COLLECTION_EXPANSION_LAYOUT,
    OUTCOME_SCHEMA_LAYOUT,
    BINDING_LAYOUT,
    ROOT_READ_LAYOUT,
    RELATIONSHIP_CHECK_LAYOUT,
    DELETE_CHECK_LAYOUT,
    LOCALITY_LAYOUT,
    CONFLICT_LAYOUT,
    COMMIT_CHECK_LAYOUT,
    OBJECT_LAYOUT,
    OUTCOME_CONSTRUCTION_LAYOUT,
    EVENT_CONSTRUCTION_LAYOUT,
    PROJECTION_ENTRY_LAYOUT,
    PROJECTION_SEMANTICS_LAYOUT,
    PROJECTION_MEASURE_LAYOUT,
    PROJECTION_GROUP_LAYOUT,
    PROJECTION_COMPONENT_LAYOUT,
    ROW_POLICY_CATALOG_LAYOUT,
    PRINCIPAL_FACT_SCHEMA_LAYOUT,
    ROW_POLICY_PLAN_LAYOUT,
    ROW_POLICY_RULE_LAYOUT,
    ROW_POLICY_NODE_LAYOUT,
    ROW_POLICY_OPERAND_LAYOUT,
    SCHEMA_ARTIFACT_LAYOUT,
    MCP_REGISTRY_LAYOUT,
    MCP_ENTRY_LAYOUT,
    COMPATIBILITY_LAYOUT,
    COMPATIBILITY_ENTRY_LAYOUT,
];

/// Exact JSON Schema extension keyword for decimal precision.
pub(crate) const JSON_DECIMAL_PRECISION: &str = "x-riffdb-decimalPrecision";
/// Exact JSON Schema extension keyword for decimal scale.
pub(crate) const JSON_DECIMAL_SCALE: &str = "x-riffdb-decimalScale";
/// Exact JSON Schema extension keyword for integer wire identity.
pub(crate) const JSON_INTEGER_TYPE: &str = "x-riffdb-integerType";
/// Exact JSON Schema extension keyword for decoded byte bounds.
pub(crate) const JSON_MAX_DECODED_BYTES: &str = "x-riffdb-maxDecodedBytes";
/// Exact JSON Schema extension keyword for UTF-8 byte bounds.
pub(crate) const JSON_MAX_UTF8_BYTES: &str = "x-riffdb-maxUtf8Bytes";
/// Exact JSON Schema extension keyword for UTF-8 byte minimums.
pub(crate) const JSON_MIN_UTF8_BYTES: &str = "x-riffdb-minUtf8Bytes";
/// Exact JSON Schema extension keyword for fixed money currency.
pub(crate) const JSON_MONEY_CURRENCY: &str = "x-riffdb-moneyCurrency";
/// Exact signed-integer string pattern used by timestamp seconds.
pub(crate) const JSON_I64_STRING_PATTERN: &str = "^-?(0|[1-9][0-9]*)$";
/// Exact lowercase canonical UUID string pattern.
pub(crate) const JSON_UUID_PATTERN: &str =
    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";
/// Fixed money decimal precision in generated JSON Schema v1.
pub(crate) const JSON_MONEY_PRECISION: u8 = 38;
/// Fixed money decimal scale in generated JSON Schema v1.
pub(crate) const JSON_MONEY_SCALE: u8 = 2;

/// Builds the exact JSON Schema regex for one valid fixed-scale decimal type.
#[must_use]
pub(crate) fn json_decimal_pattern(precision: u8, scale: u8) -> Option<String> {
    if precision == 0 || precision > 38 || scale > precision {
        return None;
    }
    Some(if scale == 0 {
        format!("^-?(0|[1-9][0-9]{{0,{}}})$", precision - 1)
    } else if scale == precision {
        format!("^-?0\\.[0-9]{{{scale}}}$")
    } else {
        format!(
            "^-?(0|[1-9][0-9]{{0,{}}})\\.[0-9]{{{scale}}}$",
            precision - scale - 1
        )
    })
}

/// Closed JSON Schema keyword inventory in ASCII order.
#[cfg(test)]
pub(crate) const JSON_SCHEMA_KEYWORDS: &[&str] = &[
    "$schema",
    "additionalProperties",
    "const",
    "contentEncoding",
    "default",
    "enum",
    "items",
    "maxItems",
    "maximum",
    "minItems",
    "minLength",
    "minimum",
    "oneOf",
    "pattern",
    "prefixItems",
    "properties",
    "required",
    "type",
    JSON_DECIMAL_PRECISION,
    JSON_DECIMAL_SCALE,
    JSON_INTEGER_TYPE,
    JSON_MAX_DECODED_BYTES,
    JSON_MAX_UTF8_BYTES,
    JSON_MIN_UTF8_BYTES,
    JSON_MONEY_CURRENCY,
];

/// One closed generated-schema keyword with its exact JSON value shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JsonSchemaKeywordFormat {
    /// Exact ASCII object key.
    pub(crate) keyword: &'static str,
    /// Exact JSON value type admitted by generated schema v1.
    pub(crate) json_type: &'static str,
    /// Closed emitted meaning and parameter rule.
    pub(crate) semantics: &'static str,
}

/// Complete generated-schema keyword registry in raw ASCII key order.
pub(crate) const JSON_SCHEMA_KEYWORD_FORMATS: &[JsonSchemaKeywordFormat] = &[
    JsonSchemaKeywordFormat {
        keyword: "$schema",
        json_type: "string",
        semantics: "exact draft 2020-12 dialect URI",
    },
    JsonSchemaKeywordFormat {
        keyword: "additionalProperties",
        json_type: "boolean",
        semantics: "always false on emitted closed objects",
    },
    JsonSchemaKeywordFormat {
        keyword: "const",
        json_type: "string",
        semantics: "exact outcome discriminator name",
    },
    JsonSchemaKeywordFormat {
        keyword: "contentEncoding",
        json_type: "string",
        semantics: "exactly base64; decoders use strict base64",
    },
    JsonSchemaKeywordFormat {
        keyword: "default",
        json_type: "null",
        semantics: "only direct optional command-input fields",
    },
    JsonSchemaKeywordFormat {
        keyword: "enum",
        json_type: "array<string>",
        semantics: "variant names in EnumVariantId order",
    },
    JsonSchemaKeywordFormat {
        keyword: "items",
        json_type: "schema object or boolean",
        semantics: "list element schema, or false for a closed projection tuple",
    },
    JsonSchemaKeywordFormat {
        keyword: "maxItems",
        json_type: "integer",
        semantics: "nonnegative exact collection upper bound",
    },
    JsonSchemaKeywordFormat {
        keyword: "maximum",
        json_type: "integer",
        semantics: "inclusive canonical integer maximum",
    },
    JsonSchemaKeywordFormat {
        keyword: "minItems",
        json_type: "integer",
        semantics: "projection tuple arity, equal to maxItems",
    },
    JsonSchemaKeywordFormat {
        keyword: "minLength",
        json_type: "integer",
        semantics: "exactly 1 for a direct idempotency string",
    },
    JsonSchemaKeywordFormat {
        keyword: "minimum",
        json_type: "integer",
        semantics: "inclusive canonical integer minimum",
    },
    JsonSchemaKeywordFormat {
        keyword: "oneOf",
        json_type: "array<schema object>",
        semantics: "ordered optional variants or OutcomeId-ordered outcome variants",
    },
    JsonSchemaKeywordFormat {
        keyword: "pattern",
        json_type: "string",
        semantics: "exact registered decimal, integer-string, or UUID pattern",
    },
    JsonSchemaKeywordFormat {
        keyword: "prefixItems",
        json_type: "array<schema object>",
        semantics: "projection group components in declared group order",
    },
    JsonSchemaKeywordFormat {
        keyword: "properties",
        json_type: "object<string,schema object>",
        semantics: "property keys in raw ASCII order",
    },
    JsonSchemaKeywordFormat {
        keyword: "required",
        json_type: "array<string>",
        semantics: "semantic field order defined by the enclosing shape",
    },
    JsonSchemaKeywordFormat {
        keyword: "type",
        json_type: "string",
        semantics: "one of array, boolean, integer, null, object, or string",
    },
    JsonSchemaKeywordFormat {
        keyword: JSON_DECIMAL_PRECISION,
        json_type: "integer",
        semantics: "decimal precision in 1..=38",
    },
    JsonSchemaKeywordFormat {
        keyword: JSON_DECIMAL_SCALE,
        json_type: "integer",
        semantics: "decimal scale in 0..=precision",
    },
    JsonSchemaKeywordFormat {
        keyword: JSON_INTEGER_TYPE,
        json_type: "string",
        semantics: "exactly i64 for timestamp seconds",
    },
    JsonSchemaKeywordFormat {
        keyword: JSON_MAX_DECODED_BYTES,
        json_type: "integer",
        semantics: "nonzero ValueType bytes bound, at most 1,048,576",
    },
    JsonSchemaKeywordFormat {
        keyword: JSON_MAX_UTF8_BYTES,
        json_type: "integer",
        semantics: "nonzero ValueType string bound, at most 1,048,576",
    },
    JsonSchemaKeywordFormat {
        keyword: JSON_MIN_UTF8_BYTES,
        json_type: "integer",
        semantics: "exactly 1 for a direct idempotency string",
    },
    JsonSchemaKeywordFormat {
        keyword: JSON_MONEY_CURRENCY,
        json_type: "string",
        semantics: "exact three-byte uppercase ASCII currency",
    },
];

/// One exact, parameterized JSON Schema construction selected by `ValueType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JsonSchemaValueConstruction {
    /// Exact Boolean object.
    Boolean,
    /// Exact bounded JSON integer object.
    Integer {
        /// Inclusive decimal minimum.
        minimum: &'static str,
        /// Inclusive decimal maximum.
        maximum: &'static str,
    },
    /// Parameterized fixed-scale decimal string.
    Decimal,
    /// Fixed `<38,2>` decimal string plus currency metadata.
    Money,
    /// Bounded UTF-8 string.
    String,
    /// Base64 string with decoded-byte bound.
    Bytes,
    /// Closed seconds/nanoseconds object.
    Timestamp,
    /// Lowercase hyphenated UUID string.
    Uuid,
    /// Closed declared-enum string set.
    Enum,
    /// Inner schema followed by null.
    Optional,
    /// Bounded homogeneous array.
    List,
    /// Inline closed entity/event record.
    Record,
    /// Fixed-dimension f32 vector as base64 bytes.
    Vector,
}

/// One exact, parameterized JSON Schema construction selected by `ValueType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JsonSchemaValueTemplate {
    /// Immutable `ValueType` tag.
    pub(crate) tag: u8,
    /// Stable value-type label.
    pub(crate) name: &'static str,
    /// Production emitter construction selected by this registry entry.
    pub(crate) construction: JsonSchemaValueConstruction,
    /// Exact canonical JSON construction with angle-bracketed semantic parameters.
    pub(crate) canonical_template: &'static str,
    /// Additional semantic ordering or validation rule.
    pub(crate) rule: &'static str,
}

/// Exact JSON Schema construction selected by a generated artifact shape.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum JsonSchemaShapeConstruction {
    /// Root entity/event output record.
    OutputRecord,
    /// Root command-input record.
    CommandInput,
    /// Root command-outcome discriminated union.
    CommandOutcomeUnion,
    /// Closed projection grouping tuple nested in a result row.
    ProjectionTupleKey,
    /// Root projection result row.
    ProjectionResultRow,
}

impl JsonSchemaShapeConstruction {
    /// Complete closed construction registry in generated-shape order.
    #[cfg(test)]
    pub(crate) const ALL: [Self; 5] = [
        Self::OutputRecord,
        Self::CommandInput,
        Self::CommandOutcomeUnion,
        Self::ProjectionTupleKey,
        Self::ProjectionResultRow,
    ];
}

/// Exact JSON Schema construction selected by a generated artifact shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JsonSchemaShapeTemplate {
    /// Closed production construction selected by this entry.
    pub(crate) construction: JsonSchemaShapeConstruction,
    /// Stable shape label.
    pub(crate) name: &'static str,
    /// Exact canonical JSON construction with angle-bracketed semantic parameters.
    pub(crate) canonical_template: &'static str,
    /// Exact required/default/ordering rule.
    pub(crate) rule: &'static str,
}

/// Every `ValueType` JSON Schema construction in tag order.
pub(crate) const JSON_SCHEMA_VALUE_TEMPLATES: &[JsonSchemaValueTemplate] = &[
    JsonSchemaValueTemplate {
        tag: value_type::BOOL,
        name: "bool",
        construction: JsonSchemaValueConstruction::Boolean,
        canonical_template: r#"{"type":"boolean"}"#,
        rule: "no parameters",
    },
    JsonSchemaValueTemplate {
        tag: value_type::I64,
        name: "i64",
        construction: JsonSchemaValueConstruction::Integer {
            minimum: "-9223372036854775808",
            maximum: "9223372036854775807",
        },
        canonical_template: r#"{"maximum":9223372036854775807,"minimum":-9223372036854775808,"type":"integer"}"#,
        rule: "bounds are inclusive JSON integers",
    },
    JsonSchemaValueTemplate {
        tag: value_type::U64,
        name: "u64",
        construction: JsonSchemaValueConstruction::Integer {
            minimum: "0",
            maximum: "18446744073709551615",
        },
        canonical_template: r#"{"maximum":18446744073709551615,"minimum":0,"type":"integer"}"#,
        rule: "bounds are inclusive JSON integers",
    },
    JsonSchemaValueTemplate {
        tag: value_type::DECIMAL,
        name: "decimal",
        construction: JsonSchemaValueConstruction::Decimal,
        canonical_template: r#"{"pattern":"<decimal(P,S)>","type":"string","x-riffdb-decimalPrecision":<P>,"x-riffdb-decimalScale":<S>}"#,
        rule: "P=1..38, S=0..P; S=0 pattern ^-?(0|[1-9][0-9]{0,P-1})$; S=P pattern ^-?0\\.[0-9]{S}$; otherwise ^-?(0|[1-9][0-9]{0,P-S-1})\\.[0-9]{S}$",
    },
    JsonSchemaValueTemplate {
        tag: value_type::MONEY,
        name: "money",
        construction: JsonSchemaValueConstruction::Money,
        canonical_template: r#"{"pattern":"^-?(0|[1-9][0-9]{0,35})\\.[0-9]{2}$","type":"string","x-riffdb-decimalPrecision":38,"x-riffdb-decimalScale":2,"x-riffdb-moneyCurrency":"<3 uppercase ASCII currency>"}"#,
        rule: "precision and scale are exactly 38 and 2; currency is the ValueType currency",
    },
    JsonSchemaValueTemplate {
        tag: value_type::STRING,
        name: "string",
        construction: JsonSchemaValueConstruction::String,
        canonical_template: r#"{"type":"string","x-riffdb-maxUtf8Bytes":<maximum_utf8_bytes>}"#,
        rule: "maximum is the nonzero bounded ValueType byte limit",
    },
    JsonSchemaValueTemplate {
        tag: value_type::BYTES,
        name: "bytes",
        construction: JsonSchemaValueConstruction::Bytes,
        canonical_template: r#"{"contentEncoding":"base64","type":"string","x-riffdb-maxDecodedBytes":<maximum_bytes>}"#,
        rule: "maximum applies after strict padded RFC 4648 base64 decoding",
    },
    JsonSchemaValueTemplate {
        tag: value_type::TIMESTAMP,
        name: "timestamp",
        construction: JsonSchemaValueConstruction::Timestamp,
        canonical_template: r#"{"additionalProperties":false,"properties":{"nanos":{"maximum":999999999,"minimum":0,"type":"integer"},"seconds":{"pattern":"^-?(0|[1-9][0-9]*)$","type":"string","x-riffdb-integerType":"i64"}},"required":["seconds","nanos"],"type":"object"}"#,
        rule: "properties use ASCII key order; required preserves semantic seconds,nanos order",
    },
    JsonSchemaValueTemplate {
        tag: value_type::DATE,
        name: "date",
        construction: JsonSchemaValueConstruction::Integer {
            minimum: "-2147483648",
            maximum: "2147483647",
        },
        canonical_template: r#"{"maximum":2147483647,"minimum":-2147483648,"type":"integer"}"#,
        rule: "signed i32 days since Unix epoch",
    },
    JsonSchemaValueTemplate {
        tag: value_type::UUID,
        name: "uuid",
        construction: JsonSchemaValueConstruction::Uuid,
        canonical_template: r#"{"pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$","type":"string"}"#,
        rule: "lowercase hyphenated network-order UUID bytes",
    },
    JsonSchemaValueTemplate {
        tag: value_type::ENUM,
        name: "enum",
        construction: JsonSchemaValueConstruction::Enum,
        canonical_template: r#"{"enum":["<variant name in EnumVariantId order>"...],"type":"string"}"#,
        rule: "the referenced enum must exist; names are exact source names",
    },
    JsonSchemaValueTemplate {
        tag: value_type::OPTIONAL,
        name: "optional",
        construction: JsonSchemaValueConstruction::Optional,
        canonical_template: r#"{"oneOf":[<inner ValueType schema>,{"type":"null"}]}"#,
        rule: "inner schema is first and null is second",
    },
    JsonSchemaValueTemplate {
        tag: value_type::LIST,
        name: "list",
        construction: JsonSchemaValueConstruction::List,
        canonical_template: r#"{"items":<element ValueType schema>,"maxItems":<maximum_entries>,"type":"array"}"#,
        rule: "maximum is the bounded ValueType list limit",
    },
    JsonSchemaValueTemplate {
        tag: value_type::RECORD,
        name: "record",
        construction: JsonSchemaValueConstruction::Record,
        canonical_template: r#"{"additionalProperties":false,"properties":{<ASCII-name-ordered field schemas>},"required":["<all field names in FieldId order>"...],"type":"object"}"#,
        rule: "only entity and event references are legal inline; the inline record omits $schema",
    },
    JsonSchemaValueTemplate {
        tag: value_type::VECTOR,
        name: "vector",
        construction: JsonSchemaValueConstruction::Vector,
        canonical_template: r#"{"description":"f32 vector encoded as big-endian bytes: 4-byte dimension followed by dimension * 4 bytes of f32 components","format":"byte","type":"string"}"#,
        rule: "dimension is validated against the contract-declared vector field dimension",
    },
];

/// Resolves the one production JSON Schema construction for a `ValueType` tag.
#[must_use]
pub(crate) fn json_schema_value_template(tag: u8) -> Option<&'static JsonSchemaValueTemplate> {
    JSON_SCHEMA_VALUE_TEMPLATES
        .iter()
        .find(|template| template.tag == tag)
}

/// Every record, input, outcome, and projection JSON Schema construction.
pub(crate) const JSON_SCHEMA_SHAPE_TEMPLATES: &[JsonSchemaShapeTemplate] = &[
    JsonSchemaShapeTemplate {
        construction: JsonSchemaShapeConstruction::OutputRecord,
        name: "entity or event output record",
        canonical_template: r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{<ASCII-name-ordered field schemas>},"required":["<every field name in FieldId order>"...],"type":"object"}"#,
        rule: "every field is required, including fields whose ValueType is optional",
    },
    JsonSchemaShapeTemplate {
        construction: JsonSchemaShapeConstruction::CommandInput,
        name: "command input record",
        canonical_template: r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{<ASCII-name-ordered field schemas>},"required":["<nonoptional field names in FieldId order>"...],"type":"object"}"#,
        rule: "optional fields add default:null to their type-schema object and are omitted from required; the direct idempotency string is nonoptional, bounded 1..=128 bytes, and additionally adds minLength:1 and x-riffdb-minUtf8Bytes:1",
    },
    JsonSchemaShapeTemplate {
        construction: JsonSchemaShapeConstruction::CommandOutcomeUnion,
        name: "command outcome union",
        canonical_template: r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","oneOf":[{"additionalProperties":false,"properties":{<ASCII-name-ordered payload schemas plus "type":{"const":"<outcome name>"}>},"required":["type","<payload field names in FieldId order>"...],"type":"object"}<in OutcomeId order>...]}"#,
        rule: "the discriminator property is exactly type with the exact outcome name; type is first in required even when ASCII property order places it elsewhere; a source outcome payload field named type is invalid",
    },
    JsonSchemaShapeTemplate {
        construction: JsonSchemaShapeConstruction::ProjectionTupleKey,
        name: "projection tuple key",
        canonical_template: r#"{"items":false,"maxItems":<component_count>,"minItems":<component_count>,"prefixItems":[<component ValueType schemas in group order>...],"type":"array"}"#,
        rule: "items is Boolean false; the nonzero component count is at most 1,024 and minItems equals maxItems exactly",
    },
    JsonSchemaShapeTemplate {
        construction: JsonSchemaShapeConstruction::ProjectionResultRow,
        name: "projection result row",
        canonical_template: r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"key":<projection tuple key>,"measures":<inline output record>},"required":["key","measures"],"type":"object"}"#,
        rule: "key and measures are the only properties; measures omits its own $schema",
    },
];

/// Resolves the one production template for a generated artifact shape.
#[must_use]
pub(crate) fn json_schema_shape_template(
    construction: JsonSchemaShapeConstruction,
) -> Option<&'static JsonSchemaShapeTemplate> {
    JSON_SCHEMA_SHAPE_TEMPLATES
        .iter()
        .find(|template| template.construction == construction)
}

/// Renders the complete accepted executable-IR format review artifact.
#[must_use]
pub fn render_format_markdown() -> String {
    let mut output = String::new();
    output.push_str("# RiffDB Contract IR Formats v1 and v2\n\nStatus: **Accepted**\n\n");
    output.push_str("This generated review artifact is derived from the production tag and ordered-layout registries. It does not accept or freeze the durable format.\n\n");
    output.push_str("## Scalar Framing\n\n- Unsigned integers are big-endian `u8`, `u32`, or `u64`; signed integers are two's-complement big-endian.\n- Boolean is `0x00` or `0x01`.\n- Bytes and UTF-8 strings are `u32 byte_length || exact_bytes`.\n- Optional values are `Boolean present || payload when present`.\n- Collections are `u32 count || elements`; checked decoders validate bounds before allocation.\n- Stable semantic IDs are nonzero. `ExprId`, `BindingId`, and `RootValidationReadId` are dense zero-based plan-local `u32` values.\n- Full input consumption and canonical re-encoding are mandatory.\n\n### Canonical Value Reference\n\nExpression constants use exactly `u32 canonical_document_byte_length || canonical_document_bytes`. `canonical_document_bytes` is one complete [ADR-0011 canonical value encoding v1](../../adr/0011-canonical-values-keys-and-hashing.md#canonical-value-encoding-v1), owned by `riffdb-types::encode_canonical_value`: its first byte is `riffdb_types::CANONICAL_VALUE_VERSION` (`0x01`), its second byte is the ADR-0011 value tag, and recursive list/record children are complete version-and-tag-prefixed documents. The outer IR length is not part of the inner canonical document. Empty, truncated, trailing, unsupported-version, and unknown-tag documents reject.\n\n");
    output.push_str("## Closed Tags\n\n");
    for registry in TAG_REGISTRIES {
        let _ = writeln!(
            output,
            "### {}\n\n| Tag | Variant |\n|---:|---|",
            registry.name
        );
        for tag in registry.tags {
            let _ = writeln!(output, "| `0x{:02x}` | {} |", tag.value, tag.name);
        }
        output.push('\n');
    }
    output.push_str("## Exact Tagged Variant Payloads\n\nEach row lists all bytes immediately following the tag, in byte order. `empty` means the tag has no payload bytes.\n\n");
    for union in TAGGED_UNION_LAYOUTS {
        let _ = writeln!(
            output,
            "### {}\n\n| Tag | Variant | Ordered payload after tag |\n|---:|---|---|",
            union.name
        );
        for variant in union.variants {
            let _ = write!(output, "| `0x{:02x}` | {} | ", variant.tag, variant.name);
            if variant.fields.is_empty() {
                output.push_str("empty");
            } else {
                for (index, field) in variant.fields.iter().enumerate() {
                    if index != 0 {
                        output.push_str("; ");
                    }
                    let _ = write!(output, "`{}`: {}", field.name, field.encoding);
                }
            }
            output.push_str(" |\n");
        }
        output.push('\n');
    }
    output.push_str("## Lineage Owner Matrix\n\nOwner kind `0x00` and count `0` encode no owner. Index and invariant allocation states are global even though their identity keys carry the contextual owner shown. Scoped field, outcome, and enum-variant allocation paths equal their identity owner paths.\n\n| Namespace | Allocation owner kind/count | Identity owner | Identity owner kind/count |\n|---|---:|---|---:|\n");
    for rule in LINEAGE_OWNER_RULES {
        let _ = writeln!(
            output,
            "| `0x{:02x}` {} | `0x{:02x}` / {} | {} | `0x{:02x}` / {} |",
            rule.namespace_tag,
            rule.namespace,
            rule.allocation_owner_kind,
            rule.allocation_owner_count,
            rule.identity_owner,
            rule.identity_owner_kind,
            rule.identity_owner_count
        );
    }
    output.push_str("\n## Compatibility Code Registry\n\nEach compatibility entry encodes its exact eight-byte ASCII code through normal string framing, followed by the affected-path string. The code fixes the entry class; the report overall is the maximum class.\n\n| Code | Required class tag | Meaning |\n|---|---:|---|\n");
    for entry in COMPATIBILITY_CODES {
        let _ = writeln!(
            output,
            "| `{}` | `0x{:02x}` | {} |",
            entry.code, entry.class_tag, entry.meaning
        );
    }
    output.push_str(
        "\n## Stable Affected-Path Grammar and Order\n\nEvery path is either `contract` or starts with one root stable ID: `aggregate:<id>`, `command:<id>`, `entity:<id>`, `enum:<id>`, `event:<id>`, or `projection:<id>`. Allowed descendants are `aggregate:<id>/invariant:<id>`; `entity:<id>/{field|index|invariant}:<id>`; `enum:<id>/variant:<id>`; `{event|projection}:<id>/field:<id>`; `command:<id>/input/field:<id>`; and `command:<id>/outcome:<id>[/field:<id>]`. Every `<id>` is a one-based `u32` in shortest decimal spelling, with no leading zero. No other root, literal, descendant, or empty segment is valid.\n\nCompatibility entries order first by the exact eight-byte code, then by structured affected path. Path segments compare their listed ASCII kind/literal, then numeric stable ID; a path prefix precedes its descendants. Numeric comparison therefore places ID 2 before IDs 10 and 11 at every nesting level. Duplicate code/path pairs reject.\n\n",
    );
    output.push_str("## Ordered Nested Layouts\n\nFields below are listed in exact byte order. A collection field includes its count immediately before its listed elements.\n\n");
    for layout in FORMAT_LAYOUTS {
        let _ = writeln!(
            output,
            "### {}\n\n| # | Field | Encoding |\n|---:|---|---|",
            layout.name
        );
        for (index, field) in layout.fields.iter().enumerate() {
            let _ = writeln!(
                output,
                "| {} | `{}` | {} |",
                index + 1,
                field.name,
                field.encoding
            );
        }
        output.push('\n');
    }
    output.push_str("## Canonical Ordering and Bounds\n\n- Stable-ID declarations and field registries are increasing and duplicate-free. Source identifiers and contract lineage names are at most 256 ASCII bytes; `compiler_version` is nonempty ASCII at most 64 bytes.\n- Expression arenas are forward-only in dense `ExprId` order; unreachable nodes reject; expression and `ValueType` nesting depths are each at most 32.\n- Root-validation reads are dense and ordered by the lowest source child `BindingId` in each structurally identical root-key derivation group.\n- Commit checks order by invariant ID, source subjects, then root-validation subjects.\n- Complete bundle size is at most 15 MiB; one generated JSON Schema artifact is at most 1 MiB and its exact byte size is checked before allocating the JSON tree; the artifact inventory is at most 20,480.\n- Lineage entries and allocation states are each at most 262,144; total expression nodes are at most 131,072; declaration and command-item collections are at most 4,096; object/tuple constructions are at most 1,024 fields.\n- `ValueType::string` and `ValueType::bytes` bounds are each 1..=1,048,576 bytes; `ValueType::list` bounds are 1..=65,535 entries. Irrespective of ADR-0011's general canonical-value collection limit, every list or record nested anywhere inside an executable-IR `Constant` is limited to 1,024 entries before allocation.\n- Only currently required empty lineage allocation states are encoded; nonempty historical states persist.\n- Repeated maps and sets are encoded in their declared canonical order. Stored hashes, schema artifacts, registry contents, and computed maxima are revalidated on decode.\n\n");
    output.push_str("## Root Validation Boundary\n\nFor a mutable child whose aggregate invariant needs the root and has no exact source root binding, the compiler emits one `RootValidationReadPlan`. The table occurs immediately after source bindings. Reads group only structurally identical checked root-key derivations and carry the lowest requiring source binding, exact root key schema, key expressions, and duplicate-free influential root fields. Empty fields are valid for a constant invariant because root presence and invariant application still matter. `RootValidationField` (`0x0b`) is legal only in commit-check predicates. Commit checks encode source and root subjects separately. Missing internal roots are integrity faults, never business outcomes.\n\n");
    output.push_str("## Typed Hash Payloads\n\nThe sequences below are canonical payloads supplied to the accepted [ADR-0011 typed SHA-256 frame](../../adr/0011-canonical-values-keys-and-hashing.md): `RIFFDB-HASH\\0 || 0x01 || u16 domain_byte_length || domain_bytes || u64 payload_byte_length || payload_bytes`. They are not complete SHA-256 preimages by themselves. Every ordered durable layout above includes its listed name strings unconditionally. Name omission applies only to the hash payload helpers described here.\n\n- Command domain payload: `RIFFDB-COMMAND-PLAN\\0 || ir_version || CommandId || CommandSemantics || ReferencedEnumClosure`. It omits the command name, binding aliases, and declaration display names for entity, aggregate, invariant, index, event, and enum closures. Input, outcome-payload, nested record field, enum-variant, and outcome names remain encoded.\n- Projection domain payload: `RIFFDB-PROJECTION-PLAN\\0 || ir_version || ProjectionId || source EventSchema || ProjectionSemantics || ReferencedEnumClosure`. It omits the projection name, source-event declaration name, and enum declaration names; source payload, projection-group, measure field, and enum-variant names remain encoded.\n- `ReferencedEnumClosure` contains every enum reached through the encoded command/projection value types or their referenced entity/event records, sorted by `EnumTypeId`; each entry encodes its stable `EnumTypeId` and complete variants in `EnumVariantId` order but omits the display-only enum declaration name. An unrelated enum is absent.\n- Schema domain payload: `RIFFDB-SCHEMA-IR\\0 || ir_version || StructuralSchema`; schema declaration, field, and enum-variant names are all encoded.\n- Contract-root domain payload: `RIFFDB-CONTRACT-PLAN-ROOT\\0 || ir_version || schema_hash || ordered (CommandId, PlanHash) pairs || ordered (ProjectionId, ProjectionPlanHash) pairs`.\n- `ContractBundleHash` uses the bundle hash domain over complete `ContractBundle` bytes and is not embedded in its own payload.\n\n### Hash-Only Ordered Layouts\n\n");
    for layout in HASH_ONLY_LAYOUTS {
        let _ = writeln!(
            output,
            "#### {}\n\n| # | Field | Encoding |\n|---:|---|---|",
            layout.name
        );
        for (index, field) in layout.fields.iter().enumerate() {
            let _ = writeln!(
                output,
                "| {} | `{}` | {} |",
                index + 1,
                field.name,
                field.encoding
            );
        }
        output.push('\n');
    }
    output.push_str("## Projection Framing Review\n\nProjection group codec version is `1`. The stored-state calculation includes exactly one 32-byte v1 stored-record framing reserve under accepted ADR-0017. It is sizing headroom, not persisted padding or an extension field. WP-065 must prove the reviewed generated `StoredProjectionStateV1` payload fits the computed maximum and the real `StoredEnvelope` fits the 16 MiB ceiling before any projection row is persisted; otherwise implementation stops for human review before changing this format.\n");
    output
}

/// Renders the complete accepted generated-JSON-Schema review artifact.
#[must_use]
pub fn render_json_schema_format_markdown() -> String {
    let mut output = String::new();
    output.push_str("# RiffDB Generated JSON Schema v1\n\nStatus: **Accepted**\n\nThis generated review artifact is derived from the production schema-artifact and keyword registries. Artifacts use draft 2020-12 and are hashed as exact canonical UTF-8 bytes.\n\n## Canonical JSON\n\n- Root `$schema` is exactly `https://json-schema.org/draft/2020-12/schema`.\n- Object keys are ordered by raw ASCII bytes with no insignificant whitespace.\n- Semantic arrays preserve their specified order.\n- Integers use shortest base-10 spelling; Boolean and null are lowercase.\n- String escaping uses JSON short escapes where defined, lowercase `\\u00xx` for other control bytes, and leaves `/` and non-control Unicode unescaped.\n- Schema v1 is fully inline and emits no unregistered keyword.\n- One artifact is at most 1,048,576 UTF-8 bytes. An exact checked-arithmetic size traversal rejects a larger expansion before its JSON tree or output string is materialized; serialization must reproduce the preflight byte count exactly.\n\n## Artifact Keys\n\nThe five-byte key is `u8 tag || u32 stable_id`. Artifacts order by this key and encode `key || u32 JSON_byte_length || JSON_bytes || 32-byte SchemaHash`.\n\n| Tag | Artifact |\n|---:|---|\n");
    for tag in schema_artifact::TAGS {
        let _ = writeln!(output, "| `0x{:02x}` | {} |", tag.value, tag.name);
    }
    output.push_str("\n## Exact ValueType Construction Registry\n\nTemplates are canonical no-whitespace JSON with angle-bracketed semantic parameters. Object keys appear in exact emitted ASCII order; the accompanying rule fixes parameter derivation and semantic array order.\n\n");
    for template in JSON_SCHEMA_VALUE_TEMPLATES {
        let _ = writeln!(
            output,
            "### `0x{:02x}` {}\n\n```text\n{}\n```\n\nRule: {}.\n",
            template.tag, template.name, template.canonical_template, template.rule
        );
    }
    output.push_str("## Exact Record, Outcome, and Projection Shapes\n\n");
    for template in JSON_SCHEMA_SHAPE_TEMPLATES {
        let _ = writeln!(
            output,
            "### {}\n\n```text\n{}\n```\n\nRule: {}.\n",
            template.name, template.canonical_template, template.rule
        );
    }
    output.push_str("## Closed Keyword Type and Semantics Registry\n\n| Keyword | JSON value type | Emitted semantics |\n|---|---|---|\n");
    for keyword in JSON_SCHEMA_KEYWORD_FORMATS {
        let _ = writeln!(
            output,
            "| `{}` | `{}` | {} |",
            keyword.keyword, keyword.json_type, keyword.semantics
        );
    }
    output.push_str("\nNo other keyword or alternate object construction is valid in v1.\n");
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BinaryOperator, BindingMode, CompatibilityClass, CompatibilityCode, ExecutionClass,
        ExpressionKind, KeyPurpose, LineageEntryState, ProjectionAggregation,
        ProjectionFrontierPolicy, RecordTypeRef, RetryPolicy, SchemaArtifactKey, StableIdNamespace,
        StableIdNamespaceTag, UnaryOperator, ValueTypeTag,
    };
    use riffdb_types::{
        AggregateTypeId, CanonicalValue, CommandId, EntityTypeId, EventTypeId, FieldId, IndexId,
        ProjectionId,
    };

    #[test]
    fn registries_are_exhaustive_canonical_and_match_public_discriminants() {
        for registry in TAG_REGISTRIES {
            assert!(!registry.tags.is_empty(), "{}", registry.name);
            assert!(registry.tags.iter().all(|tag| tag.value != 0));
            assert!(
                registry
                    .tags
                    .windows(2)
                    .all(|pair| pair[0].value < pair[1].value)
            );
        }
        assert!(
            TAG_REGISTRIES
                .windows(2)
                .all(|pair| pair[0].name != pair[1].name)
        );
        let registry_values = |registry: TagRegistry| {
            registry
                .tags
                .iter()
                .map(|tag| tag.value)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            [
                StableIdNamespaceTag::Entity,
                StableIdNamespaceTag::Event,
                StableIdNamespaceTag::Enum,
                StableIdNamespaceTag::Aggregate,
                StableIdNamespaceTag::Command,
                StableIdNamespaceTag::Projection,
                StableIdNamespaceTag::Index,
                StableIdNamespaceTag::Invariant,
                StableIdNamespaceTag::Field,
                StableIdNamespaceTag::Outcome,
                StableIdNamespaceTag::EnumVariant,
            ]
            .map(|value| value as u8),
            stable_id_namespace::TAGS
                .iter()
                .map(|tag| tag.value)
                .collect::<Vec<_>>()
                .as_slice()
        );
        assert_eq!(
            [LineageEntryState::Active, LineageEntryState::Tombstone].map(|value| value as u8),
            registry_values(lineage_entry_state::REGISTRY).as_slice()
        );
        assert_eq!(
            [
                ValueTypeTag::Bool,
                ValueTypeTag::I64,
                ValueTypeTag::U64,
                ValueTypeTag::Decimal,
                ValueTypeTag::Money,
                ValueTypeTag::String,
                ValueTypeTag::Bytes,
                ValueTypeTag::Timestamp,
                ValueTypeTag::Date,
                ValueTypeTag::Uuid,
                ValueTypeTag::Enum,
                ValueTypeTag::Optional,
                ValueTypeTag::List,
                ValueTypeTag::Record,
                ValueTypeTag::Vector,
            ]
            .map(|value| value as u8),
            registry_values(value_type::REGISTRY).as_slice()
        );
        assert_eq!(
            [UnaryOperator::Not, UnaryOperator::Negate].map(|value| value as u8),
            registry_values(unary_operator::REGISTRY).as_slice()
        );
        assert_eq!(
            [
                BinaryOperator::Multiply,
                BinaryOperator::Divide,
                BinaryOperator::Add,
                BinaryOperator::Subtract,
                BinaryOperator::Equal,
                BinaryOperator::NotEqual,
                BinaryOperator::Less,
                BinaryOperator::LessEqual,
                BinaryOperator::Greater,
                BinaryOperator::GreaterEqual,
                BinaryOperator::And,
                BinaryOperator::Or,
            ]
            .map(|value| value as u8),
            registry_values(binary_operator::REGISTRY).as_slice()
        );
        assert_eq!(
            [
                BindingMode::Read,
                BindingMode::Mutate,
                BindingMode::Create,
                BindingMode::Delete,
            ]
            .map(|value| value as u8),
            registry_values(binding_mode::REGISTRY).as_slice()
        );
        assert_eq!(
            [ExecutionClass::ReadOnly, ExecutionClass::IdempotentMutation].map(|value| value as u8),
            registry_values(execution_class::REGISTRY).as_slice()
        );
        assert_eq!(
            [RetryPolicy::BoundedFullReevaluation].map(|value| value as u8),
            registry_values(retry_policy::REGISTRY).as_slice()
        );
        assert_eq!(
            [ProjectionAggregation::Count, ProjectionAggregation::Sum].map(|value| value as u8),
            registry_values(projection_aggregation::REGISTRY).as_slice()
        );
        assert_eq!(
            [ProjectionFrontierPolicy::TransactionallyOrdered].map(|value| value as u8),
            registry_values(projection_frontier::REGISTRY).as_slice()
        );
        assert_eq!(
            [
                CompatibilityClass::Compatible,
                CompatibilityClass::RequiresExplicitVersion,
                CompatibilityClass::Incompatible,
                CompatibilityClass::RequiresMigration,
            ]
            .map(|value| value as u8),
            registry_values(compatibility_class::REGISTRY).as_slice()
        );

        let entity = EntityTypeId::first();
        let command = CommandId::first();
        let projection = ProjectionId::first();
        assert_eq!(
            [
                RecordTypeRef::Entity(entity),
                RecordTypeRef::Event(EventTypeId::first()),
                RecordTypeRef::CommandInput(command),
                RecordTypeRef::CommandOutcome {
                    command_id: command,
                    outcome_id: riffdb_types::OutcomeId::first(),
                },
                RecordTypeRef::ProjectionResult(projection),
            ]
            .map(|value| value.tag()),
            registry_values(record_reference::REGISTRY).as_slice()
        );
        assert_eq!(
            [
                KeyPurpose::Entity(entity),
                KeyPurpose::Partition(AggregateTypeId::first()),
                KeyPurpose::Conflict(AggregateTypeId::first()),
                KeyPurpose::Index {
                    index_id: IndexId::first(),
                    entity_type: entity,
                },
            ]
            .map(|value| value.tag()),
            registry_values(key_purpose::REGISTRY).as_slice()
        );
        assert_eq!(
            [
                SchemaArtifactKey::Entity(entity),
                SchemaArtifactKey::Event(EventTypeId::first()),
                SchemaArtifactKey::CommandInput(command),
                SchemaArtifactKey::CommandOutcomeUnion(command),
                SchemaArtifactKey::ProjectionResult(projection),
            ]
            .map(SchemaArtifactKey::tag),
            registry_values(schema_artifact::REGISTRY).as_slice()
        );
        let field = FieldId::first();
        let binding = crate::BindingId::new(0);
        let read = crate::RootValidationReadId::new(0);
        assert_eq!(
            [
                ExpressionKind::Constant(CanonicalValue::Bool(true)),
                ExpressionKind::InputField(field),
                ExpressionKind::CompleteBinding(binding),
                ExpressionKind::BoundField { binding, field },
                ExpressionKind::SchemaField {
                    entity_type: entity,
                    field,
                },
                ExpressionKind::SourceEventField(field),
                ExpressionKind::TransactionTime,
                ExpressionKind::TransactionDate,
                ExpressionKind::Unary {
                    operator: UnaryOperator::Not,
                    operand: crate::ExprId::new(0),
                },
                ExpressionKind::Binary {
                    operator: BinaryOperator::And,
                    left: crate::ExprId::new(0),
                    right: crate::ExprId::new(0),
                },
                ExpressionKind::RootValidationField { read, field },
                ExpressionKind::ServiceValue(field),
                ExpressionKind::CollectionElement,
                ExpressionKind::CollectionElementField(field),
            ]
            .map(|value| value.tag()),
            registry_values(expression::REGISTRY).as_slice()
        );
    }

    #[test]
    fn ordered_layout_registry_is_complete_and_canonical() {
        assert_eq!(FORMAT_LAYOUTS.first(), Some(&BUNDLE_LAYOUT));
        // MERGE TRIPWIRE — keep this a bare literal, never derive it from the
        // list. Two branches that each add one layout both rewrite the same
        // count (e.g. 58 -> 59); git merges the identical edit cleanly while
        // the list gains BOTH entries, and this assertion is what reds the
        // semantically-wrong clean merge. Deriving the count from the list
        // (or a witness list both sides also append to) would make that
        // merge pass silently. Re-run this test after any merge touching the
        // registry.
        assert_eq!(FORMAT_LAYOUTS.len(), 63);
        for layout in FORMAT_LAYOUTS {
            assert!(!layout.fields.is_empty(), "{}", layout.name);
            assert!(
                layout
                    .fields
                    .iter()
                    .all(|field| !field.name.is_empty() && !field.encoding.is_empty())
            );
        }
        let mut names = FORMAT_LAYOUTS
            .iter()
            .map(|layout| layout.name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert!(names.windows(2).all(|pair| pair[0] != pair[1]));
        assert_eq!(HASH_ONLY_LAYOUTS, &[HASH_ENUM_CLOSURE_LAYOUT]);
        assert!(HASH_ONLY_LAYOUTS.iter().all(|layout| {
            !layout.name.is_empty()
                && !layout.fields.is_empty()
                && layout
                    .fields
                    .iter()
                    .all(|field| !field.name.is_empty() && !field.encoding.is_empty())
        }));
    }

    #[test]
    fn every_parameterized_tag_has_one_exact_payload_layout() {
        for union in TAGGED_UNION_LAYOUTS {
            assert_eq!(
                union.variants.len(),
                union.tags.tags.len(),
                "{}",
                union.name
            );
            for (variant, tag) in union.variants.iter().zip(union.tags.tags) {
                assert_eq!(
                    variant.tag,
                    tag.value,
                    "{}::{name}",
                    union.name,
                    name = tag.name
                );
                assert_eq!(
                    variant.name, tag.name,
                    "{} tag 0x{:02x}",
                    union.name, tag.value
                );
                assert!(
                    variant
                        .fields
                        .iter()
                        .all(|field| !field.name.is_empty() && !field.encoding.is_empty()),
                    "{}::{}",
                    union.name,
                    variant.name
                );
                let mut names = variant
                    .fields
                    .iter()
                    .map(|field| field.name)
                    .collect::<Vec<_>>();
                names.sort_unstable();
                assert!(
                    names.windows(2).all(|pair| pair[0] != pair[1]),
                    "{}::{}",
                    union.name,
                    variant.name
                );
            }
        }
    }

    #[test]
    fn json_schema_registry_is_exhaustive_and_canonical() {
        assert_eq!(
            JSON_SCHEMA_VALUE_TEMPLATES
                .iter()
                .map(|template| template.tag)
                .collect::<Vec<_>>(),
            value_type::TAGS
                .iter()
                .map(|tag| tag.value)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            JSON_SCHEMA_VALUE_TEMPLATES
                .iter()
                .map(|template| template.name)
                .collect::<Vec<_>>(),
            value_type::TAGS
                .iter()
                .map(|tag| tag.name)
                .collect::<Vec<_>>()
        );
        assert!(JSON_SCHEMA_VALUE_TEMPLATES.iter().all(|template| {
            !template.canonical_template.is_empty() && !template.rule.is_empty()
        }));
        assert!(JSON_SCHEMA_SHAPE_TEMPLATES.iter().all(|template| {
            !template.name.is_empty()
                && !template.canonical_template.is_empty()
                && !template.rule.is_empty()
        }));
        assert_eq!(
            JSON_SCHEMA_SHAPE_TEMPLATES
                .iter()
                .map(|template| template.construction)
                .collect::<Vec<_>>(),
            JsonSchemaShapeConstruction::ALL
        );
        assert!(
            JsonSchemaShapeConstruction::ALL
                .iter()
                .all(|construction| json_schema_shape_template(*construction).is_some())
        );
        assert!(
            JSON_SCHEMA_KEYWORDS
                .windows(2)
                .all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
        );
        assert_eq!(
            JSON_SCHEMA_KEYWORD_FORMATS
                .iter()
                .map(|entry| entry.keyword)
                .collect::<Vec<_>>(),
            JSON_SCHEMA_KEYWORDS
        );
        assert!(
            JSON_SCHEMA_KEYWORD_FORMATS
                .iter()
                .all(|entry| !entry.json_type.is_empty() && !entry.semantics.is_empty())
        );
    }

    #[test]
    fn decimal_schema_pattern_registry_covers_every_scale_shape() {
        assert_eq!(
            json_decimal_pattern(3, 0).as_deref(),
            Some("^-?(0|[1-9][0-9]{0,2})$")
        );
        assert_eq!(
            json_decimal_pattern(3, 3).as_deref(),
            Some("^-?0\\.[0-9]{3}$")
        );
        assert_eq!(
            json_decimal_pattern(5, 2).as_deref(),
            Some("^-?(0|[1-9][0-9]{0,2})\\.[0-9]{2}$")
        );
        assert_eq!(json_decimal_pattern(0, 0), None);
        assert_eq!(json_decimal_pattern(39, 2), None);
        assert_eq!(json_decimal_pattern(3, 4), None);
    }

    #[test]
    fn lineage_owner_matrix_matches_checked_namespace_construction() {
        let namespace_tags = [
            StableIdNamespaceTag::Entity,
            StableIdNamespaceTag::Event,
            StableIdNamespaceTag::Enum,
            StableIdNamespaceTag::Aggregate,
            StableIdNamespaceTag::Command,
            StableIdNamespaceTag::Projection,
            StableIdNamespaceTag::Index,
            StableIdNamespaceTag::Invariant,
            StableIdNamespaceTag::Field,
            StableIdNamespaceTag::Outcome,
            StableIdNamespaceTag::EnumVariant,
        ];
        for rule in LINEAGE_OWNER_RULES {
            let tag = namespace_tags
                .iter()
                .copied()
                .find(|tag| *tag as u8 == rule.namespace_tag)
                .expect("registered namespace");
            let identity_owners = (1..=rule.identity_owner_count)
                .map(u32::from)
                .collect::<Vec<_>>();
            let namespace =
                StableIdNamespace::new(tag, rule.identity_owner_kind, identity_owners.clone())
                    .expect("registered identity owner path");
            assert_eq!(namespace.owner_ids(), identity_owners);
            let allocation = namespace.allocation_namespace();
            assert_eq!(allocation.owner_kind(), rule.allocation_owner_kind);
            assert_eq!(
                allocation.owner_ids().len(),
                rule.allocation_owner_count as usize
            );
        }
    }

    #[test]
    fn compatibility_code_registry_drives_public_codes_and_classes() {
        assert_eq!(CompatibilityCode::ALL.len(), COMPATIBILITY_CODES.len());
        for (code, format) in CompatibilityCode::ALL.into_iter().zip(COMPATIBILITY_CODES) {
            assert_eq!(code.as_str(), format.code);
            assert_eq!(code.class() as u8, format.class_tag);
            assert_eq!(CompatibilityCode::from_code(format.code), Some(code));
        }
        assert!(CompatibilityCode::from_code("RDB-K999").is_none());
        assert!(
            COMPATIBILITY_CODES
                .windows(2)
                .all(|pair| pair[0].code < pair[1].code)
        );
    }

    #[test]
    fn generated_review_documents_are_byte_exact() {
        assert_eq!(render_format_markdown(), include_str!("../FORMAT.md"));
        assert_eq!(
            render_json_schema_format_markdown(),
            include_str!("../JSON_SCHEMA_FORMAT.md")
        );
    }
}
