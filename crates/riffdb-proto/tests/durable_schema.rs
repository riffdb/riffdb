//! Descriptor and structural-wire compatibility tests for ADR-0022.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use prost_types::{
    DescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};
use riffdb_proto::STORAGE_FILE_DESCRIPTOR_SET;
use riffdb_proto::durable::{
    CURRENT_RECORD_SCHEMA_COUNT, CURRENT_RECORD_SCHEMAS, READABLE_RECORD_SCHEMA_COUNT,
    READABLE_RECORD_SCHEMAS, WRITABLE_RECORD_SCHEMA_COUNT, WRITABLE_RECORD_SCHEMAS,
    current_record_registry, current_record_schema, readable_record_registry,
    readable_record_schema, record_registry_digest, writable_record_schema,
};
use riffdb_proto::envelope::{
    EnvelopeError, MAX_STORED_ENVELOPE_BYTES, PayloadValidationError, encode, encode_v1,
};
use riffdb_proto::storage::v1::{
    StoredContractBundleV1, StoredEnvelope, StoredStorageFormatVersionV1,
};
use riffdb_types::hash_schema;

const LEGACY_RECORDS: &[(&str, &str)] = &[
    ("StoredStorageFormatVersionV1", "metadata.proto"),
    ("StoredDatabaseIdentityV1", "metadata.proto"),
    ("StoredApplicationSequenceAllocatorV1", "metadata.proto"),
    ("StoredAdministrationSequenceAllocatorV1", "metadata.proto"),
    ("StoredContractBundleV1", "catalog.proto"),
    ("ActiveCatalogPointerV1", "catalog.proto"),
    ("StoredCatalogAdministrationV1", "catalog.proto"),
    ("StoredEntityRecordV1", "application.proto"),
    ("StoredIndexEntryV1", "application.proto"),
    ("StoredIndexEpochV1", "application.proto"),
    ("StoredPendingAdmissionV1", "application.proto"),
    ("StoredExecutionFailedV1", "application.proto"),
    ("StoredOutcomeV1", "application.proto"),
    ("StoredDurableEventV1", "application.proto"),
    ("StoredOutboxIntentV1", "outbox.proto"),
    ("StoredProvenanceRecordV1", "application.proto"),
    ("StoredCommitRecordV1", "application.proto"),
    ("CapabilityRecordV1", "capability.proto"),
    ("CapabilityTokenLookupV1", "capability.proto"),
    ("CapabilityBootstrapMarkerV1", "capability.proto"),
    ("CapabilityAdministrationAuditV1", "capability.proto"),
    ("ServiceAuditRecordV1", "audit.proto"),
    ("StoredOutboxStatusV1", "outbox.proto"),
    ("StoredProjectionStateV1", "projection.proto"),
    ("StoredProjectionApplyV1", "projection.proto"),
    ("StoredProjectionControlV1", "projection.proto"),
];

const INDEX_V2_RECORD: (&str, &str) = ("StoredIndexEntryV2", "index_v2.proto");
const QUERY_MODULE_RECORDS: &[(&str, &str)] = &[
    ("StoredQueryModuleV1", "catalog.proto"),
    ("ActiveQueryModulePointerV1", "catalog.proto"),
    ("StoredQueryModuleAdministrationV1", "catalog.proto"),
];
const REACTIVE_CONSUMER_RECORDS: &[(&str, &str)] = &[
    ("StoredReactiveModuleV1", "reactive_module_v1.proto"),
    (
        "StoredReactiveModuleAdministrationV1",
        "reactive_module_v1.proto",
    ),
    ("StoredEventConsumerV1", "consumer_v1.proto"),
    ("StoredEventConsumerDeliveryV1", "consumer_v1.proto"),
];

const DURABLE_WIRE_VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-wire-vectors.txt"
));
const LEGACY_REGISTRY_FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-registry.txt"
));
const READABLE_REGISTRY_FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-readable-registry.txt"
));
const WRITABLE_REGISTRY_FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-writable-registry.txt"
));

fn decode_lower_hex(value: &str) -> Vec<u8> {
    assert!(value.len().is_multiple_of(2), "hex length must be even");
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        "fixture hex must be lowercase"
    );
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("ASCII fixture hex");
            u8::from_str_radix(text, 16).expect("validated fixture hex")
        })
        .collect()
}

fn descriptors() -> FileDescriptorSet {
    FileDescriptorSet::decode(STORAGE_FILE_DESCRIPTOR_SET).expect("checked storage descriptors")
}

fn message_map(descriptors: &FileDescriptorSet) -> BTreeMap<String, &DescriptorProto> {
    descriptors
        .file
        .iter()
        .flat_map(|file| {
            file.message_type
                .iter()
                .map(move |message| (format!("{}.{}", file.package(), message.name()), message))
        })
        .collect()
}

#[test]
// req: STO-020, STO-023
fn clean_close_lifecycle_descriptor_is_exact() {
    let descriptors = descriptors();
    let source = descriptors
        .file
        .iter()
        .find(|file| file.name() == "riffdb/storage/v1/clean_close_lifecycle_v1.proto")
        .expect("ADR-0157 clean-close lifecycle source descriptor");
    assert!(source.dependency.is_empty());
    assert_eq!(source.package(), "riffdb.storage.v1");

    let lifecycle_state = source
        .enum_type
        .iter()
        .find(|enumeration| enumeration.name() == "StoredCleanCloseLifecycleStateV1")
        .expect("ADR-0157 lifecycle state enum");
    assert_eq!(
        lifecycle_state
            .value
            .iter()
            .map(|value| (value.name(), value.number()))
            .collect::<Vec<_>>(),
        vec![
            ("STORED_CLEAN_CLOSE_LIFECYCLE_STATE_V1_UNSPECIFIED", 0),
            ("STORED_CLEAN_CLOSE_LIFECYCLE_STATE_V1_DIRTY", 1),
            ("STORED_CLEAN_CLOSE_LIFECYCLE_STATE_V1_CLEAN", 2),
        ]
    );

    let lifecycle = source
        .message_type
        .iter()
        .find(|message| message.name() == "StoredCleanCloseLifecycleV1")
        .expect("ADR-0157 lifecycle record");
    assert_eq!(
        lifecycle
            .field
            .iter()
            .map(|field| {
                (
                    field.name(),
                    field.number(),
                    field.r#type(),
                    field.type_name(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            ("database_id", 1, Type::Bytes, ""),
            ("history_incarnation", 2, Type::Fixed64, ""),
            ("record_registry_digest", 3, Type::Bytes, ""),
            ("lifecycle_generation", 4, Type::Fixed64, ""),
            (
                "state",
                5,
                Type::Enum,
                ".riffdb.storage.v1.StoredCleanCloseLifecycleStateV1",
            ),
            ("clean_state_binding_hash", 6, Type::Bytes, ""),
            ("lifecycle_hash", 7, Type::Bytes, ""),
        ]
    );
    assert!(lifecycle.oneof_decl.is_empty());
    for field in &lifecycle.field {
        assert_eq!(field.label(), Label::Optional);
        assert!(!field.proto3_optional());
        assert!(field.oneof_index.is_none());
    }
}

fn descriptor_closure(descriptors: &FileDescriptorSet, root: &str) -> FileDescriptorSet {
    let files = descriptors
        .file
        .iter()
        .map(|file| (file.name(), file))
        .collect::<BTreeMap<_, _>>();
    let mut pending = vec![root.to_owned()];
    let mut names = BTreeSet::new();
    while let Some(name) = pending.pop() {
        if !names.insert(name.clone()) {
            continue;
        }
        let file = files[name.as_str()];
        pending.extend(file.dependency.iter().cloned());
    }
    FileDescriptorSet {
        file: names
            .iter()
            .map(|name| files[name.as_str()].clone())
            .collect(),
    }
}

#[test]
fn storage_source_import_and_type_inventory_is_exact() {
    let descriptors = descriptors();
    let imports = descriptors
        .file
        .iter()
        .map(|file| {
            (
                file.name().to_owned(),
                file.dependency
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        imports,
        BTreeMap::from([
            (
                "riffdb/storage/v1/promotion_administration_v1.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/common.proto",
                    "riffdb/storage/v1/audit.proto",
                    "riffdb/storage/v1/primary_fence_v1.proto",
                    "riffdb/storage/v1/changelog_history_state_v3.proto",
                ],
            ),
            (
                "riffdb/storage/v1/replication_source_hold_v2.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/replication_source_hold_v1.proto",
                    "riffdb/storage/v1/changelog_history_state_v3.proto"
                ],
            ),
            (
                "riffdb/storage/v1/replication_source_hold_v1.proto".to_owned(),
                vec!["riffdb/storage/v1/changelog_history_state_v3.proto"],
            ),
            (
                "riffdb/storage/v1/application.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            (
                "riffdb/storage/v1/audit.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            (
                "riffdb/storage/v1/capability.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            (
                "riffdb/storage/v1/capability_export.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/capability.proto",
                    "riffdb/storage/v1/capability_installation.proto",
                    "riffdb/storage/v1/capability_migration.proto",
                    "riffdb/storage/v1/capability_row_policy.proto",
                ],
            ),
            (
                "riffdb/storage/v1/capability_secret.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/capability.proto",
                    "riffdb/storage/v1/capability_export.proto",
                    "riffdb/storage/v1/capability_installation.proto",
                    "riffdb/storage/v1/capability_migration.proto",
                    "riffdb/storage/v1/capability_row_policy.proto",
                ],
            ),
            (
                "riffdb/storage/v1/capability_reimport.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/capability.proto",
                    "riffdb/storage/v1/capability_export.proto",
                    "riffdb/storage/v1/capability_installation.proto",
                    "riffdb/storage/v1/capability_migration.proto",
                    "riffdb/storage/v1/capability_row_policy.proto",
                    "riffdb/storage/v1/capability_secret.proto",
                ],
            ),
            (
                "riffdb/storage/v1/capability_vector_inspection.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/capability.proto",
                    "riffdb/storage/v1/capability_export.proto",
                    "riffdb/storage/v1/capability_installation.proto",
                    "riffdb/storage/v1/capability_migration.proto",
                    "riffdb/storage/v1/capability_reimport.proto",
                    "riffdb/storage/v1/capability_row_policy.proto",
                    "riffdb/storage/v1/capability_secret.proto",
                ],
            ),
            (
                "riffdb/storage/v1/capability_installation.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/capability.proto",
                    "riffdb/storage/v1/capability_migration.proto",
                ],
            ),
            (
                "riffdb/storage/v1/capability_migration.proto".to_owned(),
                vec!["riffdb/storage/v1/capability.proto"],
            ),
            (
                "riffdb/storage/v1/capability_row_policy.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/capability.proto",
                    "riffdb/storage/v1/capability_installation.proto",
                    "riffdb/storage/v1/capability_migration.proto",
                ],
            ),
            (
                "riffdb/storage/v1/catalog.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            (
                "riffdb/storage/v1/changelog_transaction_allocator_v3.proto".to_owned(),
                vec![],
            ),
            (
                "riffdb/storage/v1/authoritative_state_catalog_v1.proto".to_owned(),
                vec![]
            ),
            (
                "riffdb/storage/v1/authoritative_state_catalog_v2.proto".to_owned(),
                vec![]
            ),
            (
                "riffdb/storage/v1/primary_fence_v1.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/common.proto",
                    "riffdb/storage/v1/changelog_history_state_v3.proto",
                    "riffdb/storage/v1/service_audit_v3.proto"
                ]
            ),
            (
                "riffdb/storage/v1/leadership_epoch_v1.proto".to_owned(),
                vec![]
            ),
            (
                "riffdb/storage/v1/changelog_history_state_v3.proto".to_owned(),
                vec![]
            ),
            (
                "riffdb/storage/v1/replication_follower_state_v3.proto".to_owned(),
                vec!["riffdb/storage/v1/changelog_history_state_v3.proto"]
            ),
            (
                "riffdb/storage/v1/clean_close_lifecycle_v1.proto".to_owned(),
                vec![],
            ),
            (
                "riffdb/storage/v1/columnar_projection_control_v1.proto".to_owned(),
                vec![],
            ),
            (
                "riffdb/storage/v1/command_capsule_v1.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/audit.proto",
                    "riffdb/storage/v1/common.proto",
                    "riffdb/storage/v1/contextual_causation_v2.proto",
                    "riffdb/storage/v1/entity_references_v3.proto",
                    "riffdb/storage/v1/event_references_v2.proto",
                    "riffdb/storage/v1/service_audit_v2.proto",
                ],
            ),
            (
                "riffdb/storage/v1/command_prefix_authority_v7.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/command_capsule_v1.proto",
                    "riffdb/storage/v1/command_segment_v1.proto",
                    "riffdb/storage/v1/entity_transitions_v4.proto",
                    "riffdb/storage/v1/event_policy_command_authority_v5.proto",
                ],
            ),
            (
                "riffdb/storage/v1/command_segment_v1.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/command_capsule_v1.proto",
                    "riffdb/storage/v1/index_generation_v2.proto",
                ],
            ),
            ("riffdb/storage/v1/common.proto".to_owned(), vec![]),
            (
                "riffdb/storage/v1/consumer_v1.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            (
                "riffdb/storage/v1/contextual_causation_v2.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/common.proto",
                ],
            ),
            (
                "riffdb/storage/v1/entity_references_v3.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/common.proto",
                    "riffdb/storage/v1/event_references_v2.proto",
                ],
            ),
            (
                "riffdb/storage/v1/entity_transitions_v4.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/command_segment_v1.proto",
                    "riffdb/storage/v1/validated_prefix_checkpoint_v1.proto",
                    "riffdb/storage/v1/workflow_service_values_v3.proto",
                ],
            ),
            ("riffdb/storage/v1/envelope.proto".to_owned(), vec![]),
            (
                "riffdb/storage/v1/event_policy_anchor.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/common.proto",
                ],
            ),
            (
                "riffdb/storage/v1/event_policy_command_authority_v5.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/command_capsule_v1.proto",
                    "riffdb/storage/v1/command_segment_v1.proto",
                    "riffdb/storage/v1/entity_transitions_v4.proto",
                    "riffdb/storage/v1/event_policy_anchor.proto",
                ],
            ),
            (
                "riffdb/storage/v1/correlated_index_work_command_authority_v6.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/command_capsule_v1.proto",
                    "riffdb/storage/v1/command_segment_v1.proto",
                    "riffdb/storage/v1/entity_transitions_v4.proto",
                    "riffdb/storage/v1/event_policy_command_authority_v5.proto",
                ],
            ),
            (
                "riffdb/storage/v1/event_references_v2.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/common.proto",
                ],
            ),
            (
                "riffdb/storage/v1/event_route_v1.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            ("riffdb/storage/v1/export_v1.proto".to_owned(), vec![]),
            (
                "riffdb/storage/v1/export_ledger_v1.proto".to_owned(),
                vec![]
            ),
            (
                "riffdb/storage/v1/history_incarnation_v1.proto".to_owned(),
                vec![],
            ),
            (
                "riffdb/storage/v1/index_generation_v2.proto".to_owned(),
                vec!["riffdb/storage/v1/application.proto"],
            ),
            (
                "riffdb/storage/v1/index_v2.proto".to_owned(),
                vec!["riffdb/storage/v1/application.proto"],
            ),
            ("riffdb/storage/v1/installation_v1.proto".to_owned(), vec![],),
            (
                "riffdb/storage/v1/metadata.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            (
                "riffdb/storage/v1/migration_v1.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/common.proto",
                ],
            ),
            (
                "riffdb/storage/v1/outbox.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/common.proto",
                ],
            ),
            (
                "riffdb/storage/v1/projection.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            (
                "riffdb/storage/v1/reactive_module_v1.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            ("riffdb/storage/v1/registry_v2.proto".to_owned(), vec![]),
            (
                "riffdb/storage/v1/retention_watermark_v1.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            (
                "riffdb/storage/v1/service_audit_request_index_v1.proto".to_owned(),
                vec![],
            ),
            (
                "riffdb/storage/v1/service_audit_v2.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/audit.proto",
                    "riffdb/storage/v1/common.proto",
                ],
            ),
            (
                "riffdb/storage/v1/replication_administration_v1.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/common.proto",
                    "riffdb/storage/v1/changelog_history_state_v3.proto",
                    "riffdb/storage/v1/replication_source_hold_v1.proto",
                    "riffdb/storage/v1/replication_source_hold_v2.proto",
                    "riffdb/storage/v1/service_audit_v3.proto"
                ],
            ),
            (
                "riffdb/storage/v1/service_audit_v3.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/audit.proto",
                    "riffdb/storage/v1/common.proto",
                    "riffdb/storage/v1/service_audit_v2.proto",
                ],
            ),
            (
                "riffdb/storage/v1/validated_prefix_checkpoint_v1.proto".to_owned(),
                vec![],
            ),
            (
                "riffdb/storage/v1/vector_evidence_v1.proto".to_owned(),
                vec!["riffdb/storage/v1/application.proto"],
            ),
            (
                "riffdb/storage/v1/vector_evidence_index_v1.proto".to_owned(),
                vec!["riffdb/storage/v1/vector_evidence_v1.proto"],
            ),
            (
                "riffdb/storage/v1/vector_health_observation_v1.proto".to_owned(),
                vec![],
            ),
            (
                "riffdb/storage/v1/vector_observation_v1.proto".to_owned(),
                vec![],
            ),
            (
                "riffdb/storage/v1/vector_projection_control_v1.proto".to_owned(),
                vec![],
            ),
            (
                "riffdb/storage/v1/workflow_service_values_v3.proto".to_owned(),
                vec![
                    "riffdb/storage/v1/application.proto",
                    "riffdb/storage/v1/command_segment_v1.proto",
                    "riffdb/storage/v1/contextual_causation_v2.proto",
                ],
            ),
        ])
    );
    assert!(descriptors.file.iter().all(|file| {
        file.package() == "riffdb.storage.v1"
            && file.source_code_info.is_none()
            && file.dependency.iter().all(|dependency| {
                !dependency.starts_with("riffdb/v1/") && !dependency.starts_with("google/protobuf/")
            })
    }));
    assert_eq!(
        descriptors
            .file
            .iter()
            .map(|file| file.message_type.len())
            .sum::<usize>(),
        209,
        "exact top-level semantic messages, StoredEnvelope and registry support"
    );
    assert_eq!(
        descriptors
            .file
            .iter()
            .map(|file| file.enum_type.len())
            .sum::<usize>(),
        30
    );
    assert!(
        descriptors
            .file
            .iter()
            .flat_map(|file| &file.message_type)
            .all(|message| message.reserved_name.is_empty())
    );
    assert!(
        descriptors
            .file
            .iter()
            .flat_map(|file| &file.enum_type)
            .all(|enumeration| enumeration.reserved_name.is_empty())
    );
}

#[test]
fn closed_registry_order_and_schema_hashes_are_exactly_derived() {
    assert_eq!(CURRENT_RECORD_SCHEMA_COUNT, 92);
    assert_eq!(READABLE_RECORD_SCHEMA_COUNT, 115);
    assert_eq!(WRITABLE_RECORD_SCHEMA_COUNT, 92);
    assert_eq!(
        CURRENT_RECORD_SCHEMAS
            .iter()
            .map(|schema| (schema.record_type(), schema.schema_hash()))
            .collect::<Vec<_>>(),
        WRITABLE_RECORD_SCHEMAS
            .iter()
            .map(|schema| (schema.record_type(), schema.schema_hash()))
            .collect::<Vec<_>>()
    );
    let descriptors = descriptors();
    let legacy_names = LEGACY_RECORDS
        .iter()
        .map(|(name, _)| format!("riffdb.storage.v1.{name}"))
        .collect::<Vec<_>>();
    let mut readable_names = legacy_names.clone();
    readable_names.extend(
        QUERY_MODULE_RECORDS
            .iter()
            .map(|(name, _)| format!("riffdb.storage.v1.{name}")),
    );
    readable_names.push(format!("riffdb.storage.v1.{}", INDEX_V2_RECORD.0));
    readable_names.push("riffdb.storage.v1.StoredRecordRegistryV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommitRecordV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredOutboxIntentV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredIndexGenerationV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredHistoryIncarnationV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredServiceAuditRequestIndexV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredEventRouteV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommitRecordV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredContractMigrationJournalV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredContractMigrationRecordV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredContractWriteRetirementV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredRetiredEntityRecordV1".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityRecordV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredValidatedPrefixCheckpointV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredRetentionWatermarkV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredRetentionHoldsV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredHistoryTombstoneV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredRetentionAdministrationV1".to_owned());
    readable_names.extend(
        REACTIVE_CONSUMER_RECORDS
            .iter()
            .map(|(name, _)| format!("riffdb.storage.v1.{name}")),
    );
    readable_names.push("riffdb.storage.v1.ServiceAuditRecordV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredPendingAdmissionV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredExecutionFailedV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredOutcomeV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredProvenanceRecordV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandCapsuleV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandLocatorV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandAuditLocatorV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandCapsuleV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandSegmentV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandDerivedIndexCheckpointV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredPendingAdmissionV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredExecutionFailedV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredOutcomeV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandCapsuleV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandSegmentV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredApplicationInstallationCampaignV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredEntityChainHeadV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredChangelogV2RotationReceiptV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandCapsuleV4".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandSegmentV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredValidatedPrefixCheckpointV2".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityRecordV3".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityRecordV4".to_owned());
    readable_names.push("riffdb.storage.v1.StoredDurableEventV2".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityRecordV5".to_owned());
    readable_names.push("riffdb.storage.v1.StoredApplicationExportOperationV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandCapsuleV5".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandSegmentV4".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityRecordV6".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityRecordV7".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityRecordV8".to_owned());
    readable_names.push("riffdb.storage.v1.StoredVectorEvidenceV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredVectorObservationV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredVectorEvidenceIndexV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredVectorHealthObservationV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredVectorProjectionControlV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandCapsuleV6".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandSegmentV5".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCleanCloseLifecycleV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredColumnarProjectionControlV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredChangelogTransactionAllocatorV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredAuthoritativeStateCatalogV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredLeadershipEpochV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredChangelogHistoryStateV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredReplicationFollowerStateV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredReplicationSourceHoldV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandCapsuleV7".to_owned());
    readable_names.push("riffdb.storage.v1.StoredCommandSegmentV6".to_owned());
    readable_names.push("riffdb.storage.v1.StoredReplicationSourceHoldV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredApplicationExportOperationV2".to_owned());
    readable_names.push("riffdb.storage.v1.StoredApplicationExportPageCommitmentV1".to_owned());
    readable_names.push("riffdb.storage.v1.ServiceAuditRecordV3".to_owned());
    readable_names.push("riffdb.storage.v1.StoredReplicationAdministrationV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredAuthoritativeStateCatalogV2".to_owned());
    readable_names.push("riffdb.storage.v1.ReplicationPrimaryAdmissionV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredPrimaryFenceAdministrationV1".to_owned());
    readable_names.push("riffdb.storage.v1.StoredPromotionAdministrationV1".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityRecordV1".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityRecordV1".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityTokenLookupV1".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityBootstrapMarkerV1".to_owned());
    readable_names.push("riffdb.storage.v1.CapabilityAdministrationAuditV1".to_owned());
    let mut writable_names = legacy_names.clone();
    writable_names[8] = format!("riffdb.storage.v1.{}", INDEX_V2_RECORD.0);
    writable_names[9] = "riffdb.storage.v1.StoredIndexGenerationV2".to_owned();
    writable_names[10] = "riffdb.storage.v1.StoredPendingAdmissionV3".to_owned();
    writable_names[11] = "riffdb.storage.v1.StoredExecutionFailedV3".to_owned();
    writable_names[12] = "riffdb.storage.v1.StoredOutcomeV3".to_owned();
    writable_names[14] = "riffdb.storage.v1.StoredOutboxIntentV2".to_owned();
    writable_names[15] = "riffdb.storage.v1.StoredProvenanceRecordV2".to_owned();
    writable_names[16] = "riffdb.storage.v1.StoredCommitRecordV3".to_owned();
    writable_names[21] = "riffdb.storage.v1.ServiceAuditRecordV2".to_owned();
    writable_names.insert(14, "riffdb.storage.v1.StoredDurableEventV2".to_owned());
    writable_names.extend(
        QUERY_MODULE_RECORDS
            .iter()
            .map(|(name, _)| format!("riffdb.storage.v1.{name}")),
    );
    writable_names.push("riffdb.storage.v1.StoredHistoryIncarnationV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredServiceAuditRequestIndexV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredEventRouteV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredContractMigrationJournalV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredContractMigrationRecordV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredContractWriteRetirementV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredRetiredEntityRecordV1".to_owned());
    writable_names.push("riffdb.storage.v1.CapabilityRecordV2".to_owned());
    writable_names.push("riffdb.storage.v1.StoredRetentionWatermarkV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredRetentionHoldsV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredHistoryTombstoneV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredRetentionAdministrationV1".to_owned());
    writable_names.extend(
        REACTIVE_CONSUMER_RECORDS
            .iter()
            .map(|(name, _)| format!("riffdb.storage.v1.{name}")),
    );
    writable_names.push("riffdb.storage.v1.StoredCommandCapsuleV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandLocatorV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandAuditLocatorV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandDerivedIndexCheckpointV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredApplicationInstallationCampaignV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredEntityChainHeadV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredChangelogV2RotationReceiptV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandCapsuleV4".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandSegmentV3".to_owned());
    writable_names.push("riffdb.storage.v1.StoredValidatedPrefixCheckpointV2".to_owned());
    writable_names.push("riffdb.storage.v1.CapabilityRecordV3".to_owned());
    writable_names.push("riffdb.storage.v1.CapabilityRecordV4".to_owned());
    writable_names.push("riffdb.storage.v1.CapabilityRecordV5".to_owned());
    writable_names.push("riffdb.storage.v1.StoredApplicationExportOperationV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandCapsuleV5".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandSegmentV4".to_owned());
    writable_names.push("riffdb.storage.v1.CapabilityRecordV6".to_owned());
    writable_names.push("riffdb.storage.v1.CapabilityRecordV7".to_owned());
    writable_names.push("riffdb.storage.v1.CapabilityRecordV8".to_owned());
    writable_names.push("riffdb.storage.v1.StoredRecordRegistryV2".to_owned());
    writable_names.push("riffdb.storage.v1.StoredVectorEvidenceV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredVectorObservationV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredVectorEvidenceIndexV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredVectorHealthObservationV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredVectorProjectionControlV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandCapsuleV6".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandSegmentV5".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCleanCloseLifecycleV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredColumnarProjectionControlV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredChangelogTransactionAllocatorV3".to_owned());
    writable_names.push("riffdb.storage.v1.StoredAuthoritativeStateCatalogV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredLeadershipEpochV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredChangelogHistoryStateV3".to_owned());
    writable_names.push("riffdb.storage.v1.StoredReplicationFollowerStateV3".to_owned());
    writable_names.push("riffdb.storage.v1.StoredReplicationSourceHoldV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandCapsuleV7".to_owned());
    writable_names.push("riffdb.storage.v1.StoredCommandSegmentV6".to_owned());
    writable_names.push("riffdb.storage.v1.StoredReplicationSourceHoldV2".to_owned());
    writable_names.push("riffdb.storage.v1.StoredApplicationExportOperationV2".to_owned());
    writable_names.push("riffdb.storage.v1.StoredApplicationExportPageCommitmentV1".to_owned());
    writable_names.push("riffdb.storage.v1.ServiceAuditRecordV3".to_owned());
    writable_names.push("riffdb.storage.v1.StoredReplicationAdministrationV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredAuthoritativeStateCatalogV2".to_owned());
    writable_names.push("riffdb.storage.v1.ReplicationPrimaryAdmissionV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredPrimaryFenceAdministrationV1".to_owned());
    writable_names.push("riffdb.storage.v1.StoredPromotionAdministrationV1".to_owned());
    assert_eq!(
        READABLE_RECORD_SCHEMAS
            .iter()
            .map(|schema| schema.record_type())
            .collect::<Vec<_>>(),
        readable_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        WRITABLE_RECORD_SCHEMAS
            .iter()
            .map(|schema| schema.record_type())
            .collect::<Vec<_>>(),
        writable_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        READABLE_RECORD_SCHEMAS
            .iter()
            .map(|schema| schema.schema_hash())
            .collect::<BTreeSet<_>>()
            .len(),
        READABLE_RECORD_SCHEMA_COUNT
    );

    let readable_records = LEGACY_RECORDS
        .iter()
        .copied()
        .chain(QUERY_MODULE_RECORDS.iter().copied())
        .chain(std::iter::once(INDEX_V2_RECORD));
    for ((record_name, source), schema) in readable_records.zip(&READABLE_RECORD_SCHEMAS) {
        let root = format!("riffdb/storage/v1/{source}");
        let descriptor = descriptor_closure(&descriptors, &root).encode_to_vec();
        let record_type = format!("riffdb.storage.v1.{record_name}");
        let mut frame = Vec::new();
        frame.extend_from_slice(
            &u16::try_from(record_type.len())
                .expect("record type length fits")
                .to_be_bytes(),
        );
        frame.extend_from_slice(record_type.as_bytes());
        frame.extend_from_slice(
            &u64::try_from(descriptor.len())
                .expect("descriptor length fits")
                .to_be_bytes(),
        );
        frame.extend_from_slice(&descriptor);
        assert_eq!(schema.schema_hash(), hash_schema(&frame), "{record_type}");
        assert!(schema.max_payload_bytes() < MAX_STORED_ENVELOPE_BYTES);
        assert!(schema.max_envelope_bytes() <= MAX_STORED_ENVELOPE_BYTES);
        assert!(schema.max_envelope_bytes() > schema.max_payload_bytes());
        assert!(readable_record_schema(&record_type).is_some());
    }
    let legacy_capability = READABLE_RECORD_SCHEMAS
        .get(READABLE_RECORD_SCHEMA_COUNT - 5)
        .expect("pre-WP280 capability reader");
    assert_eq!(
        legacy_capability.record_type(),
        "riffdb.storage.v1.CapabilityRecordV1"
    );
    assert_eq!(
        schema_hash_hex(legacy_capability),
        "cb42c4ebbce8280123f8b34d4dcde74ca9483406847531f34d5fb3f18d40b342"
    );
    assert_eq!(
        schema_hash_hex(
            READABLE_RECORD_SCHEMAS
                .get(READABLE_RECORD_SCHEMA_COUNT - 4)
                .expect("pre-WP416 capability reader")
        ),
        "dee2398ebbc71824471fe5a5f96fcbebdde09e511fe6c11531c2b30210f9e6bf"
    );
    assert_eq!(
        READABLE_RECORD_SCHEMAS
            .iter()
            .map(|schema| schema.max_payload_bytes())
            .collect::<BTreeSet<_>>()
            .len(),
        23,
        "five semantic classes plus eighteen FQN-specific absolute maxima"
    );

    let v1 = "riffdb.storage.v1.StoredIndexEntryV1";
    let v2 = "riffdb.storage.v1.StoredIndexEntryV2";
    assert!(readable_record_schema(v1).is_some());
    assert!(readable_record_schema(v2).is_some());
    assert!(writable_record_schema(v1).is_none());
    assert!(current_record_schema(v1).is_none());
    assert!(writable_record_schema(v2).is_some());
    assert!(current_record_schema(v2).is_some());

    let commit_v2 = "riffdb.storage.v1.StoredCommitRecordV2";
    let commit_v3 = "riffdb.storage.v1.StoredCommitRecordV3";
    let v2_schema = readable_record_schema(commit_v2).expect("v2 readable");
    let v3_schema = readable_record_schema(commit_v3).expect("v3 readable");
    assert_eq!(v2_schema.compact_tag(), 17);
    assert_eq!(v2_schema.schema_revision(), 2);
    assert_eq!(v3_schema.compact_tag(), 17);
    assert_eq!(v3_schema.schema_revision(), 3);
    assert!(writable_record_schema(commit_v2).is_none());
    assert!(writable_record_schema(commit_v3).is_some());
    assert!(current_record_schema(commit_v3).is_some());

    let clean_close = writable_record_schema("riffdb.storage.v1.StoredCleanCloseLifecycleV1")
        .expect("ADR-0157 lifecycle record is current writable");
    assert_eq!(clean_close.compact_tag(), 66);
    assert_eq!(clean_close.schema_revision(), 1);
    assert_eq!(clean_close.max_payload_bytes(), 192);
    assert_eq!(clean_close.max_envelope_bytes(), 283);

    for unregistered in [
        "riffdb.storage.v1.StoredEnvelope",
        "riffdb.storage.v1.StoredReadDependenciesV1",
        "riffdb.storage.v1.CapabilityGrantV1",
        "riffdb.storage.v1.UnitV1",
        "riffdb.storage.v1.NodeIdV1",
        "riffdb.storage.v1.CleanShutdownV1",
        "riffdb.storage.v1.PendingTombstoneV1",
        "riffdb.storage.v1.OutcomePointerV1",
    ] {
        assert!(
            readable_record_schema(unregistered).is_none(),
            "{unregistered}"
        );
    }
}

// req: PRJ-005, PRJ-006, PRJ-007, PRJ-010, OQ-020, OQ-024, OQ-053
#[test]
fn columnar_control_v1_freezes_numeric_registry_and_bounds() {
    let descriptors = descriptors();
    let source = descriptors
        .file
        .iter()
        .find(|file| file.name() == "riffdb/storage/v1/columnar_projection_control_v1.proto")
        .expect("columnar control source descriptor");
    assert!(source.dependency.is_empty());
    assert_eq!(source.package(), "riffdb.storage.v1");
    assert_eq!(
        source
            .message_type
            .iter()
            .map(|message| message.name())
            .collect::<Vec<_>>(),
        vec![
            "StoredColumnarScalarSourceV1",
            "StoredColumnarVectorSourceV1",
            "StoredColumnarProjectionSourceV1",
            "StoredColumnarProjectionFrontierV1",
            "StoredColumnarProjectionFailureV1",
            "StoredColumnarProjectionGenerationV1",
            "StoredColumnarProjectionControlV1",
        ]
    );
    let messages = message_map(&descriptors);
    let fields = |name: &str| {
        messages[name]
            .field
            .iter()
            .map(|field| {
                (
                    field.name(),
                    field.number(),
                    field.r#type(),
                    field.proto3_optional(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        fields("riffdb.storage.v1.StoredColumnarScalarSourceV1"),
        vec![
            ("contract_lineage", 1, Type::String, false),
            ("definition_fingerprint", 2, Type::Bytes, false),
        ]
    );
    assert_eq!(
        fields("riffdb.storage.v1.StoredColumnarVectorSourceV1"),
        vec![
            ("contract_lineage", 1, Type::String, false),
            ("entity_type_id", 2, Type::Uint32, false),
            ("vector_field_id", 3, Type::Uint32, false),
        ]
    );
    assert_eq!(
        fields("riffdb.storage.v1.StoredColumnarProjectionFrontierV1"),
        vec![("applied_through", 1, Type::Uint64, true)]
    );
    assert_eq!(
        fields("riffdb.storage.v1.StoredColumnarProjectionFailureV1"),
        vec![
            ("target", 1, Type::Enum, false),
            ("reason", 2, Type::Enum, false),
            ("generation", 3, Type::Uint64, true),
        ]
    );
    assert_eq!(
        fields("riffdb.storage.v1.StoredColumnarProjectionGenerationV1"),
        vec![
            ("generation", 1, Type::Uint64, false),
            ("layout", 2, Type::Enum, false),
            ("frontier", 3, Type::Message, false),
            ("history_incarnation", 4, Type::Uint64, false),
            ("artifact_length", 5, Type::Uint64, true),
            ("checksum_bytes", 6, Type::Bytes, true),
            ("definition_fingerprint", 7, Type::Bytes, false),
            ("spec_hash", 8, Type::Bytes, false),
            ("physical_generation_fingerprint", 9, Type::Bytes, true),
            ("snapshot_frontier", 10, Type::Message, true),
            ("role", 11, Type::Enum, false),
        ]
    );
    assert_eq!(
        fields("riffdb.storage.v1.StoredColumnarProjectionControlV1"),
        vec![
            ("source", 1, Type::Message, false),
            ("target_definition_fingerprint", 2, Type::Bytes, false),
            ("target_spec_hash", 3, Type::Bytes, false),
            ("highest_generation", 4, Type::Uint64, false),
            ("published", 5, Type::Message, true),
            ("candidate", 6, Type::Message, true),
            ("predecessor", 7, Type::Message, true),
            ("lifecycle", 8, Type::Enum, false),
            ("failure", 9, Type::Message, true),
            ("replay_age_seconds", 10, Type::Uint64, false),
            ("replay_bytes", 11, Type::Uint64, false),
            ("replay_backlog", 12, Type::Uint64, false),
        ]
    );
    let reserved = |name: &str| {
        messages[name]
            .reserved_range
            .iter()
            .map(|range| (range.start(), range.end()))
            .collect::<Vec<_>>()
    };
    for (message, expected) in [
        ("StoredColumnarScalarSourceV1", (3, 16)),
        ("StoredColumnarVectorSourceV1", (4, 16)),
        ("StoredColumnarProjectionSourceV1", (3, 16)),
        ("StoredColumnarProjectionFrontierV1", (2, 16)),
        ("StoredColumnarProjectionFailureV1", (4, 16)),
        ("StoredColumnarProjectionGenerationV1", (12, 16)),
        ("StoredColumnarProjectionControlV1", (13, 32)),
    ] {
        assert_eq!(
            reserved(&format!("riffdb.storage.v1.{message}")),
            vec![expected],
            "{message} reserved field range"
        );
    }
    let source_union = &messages["riffdb.storage.v1.StoredColumnarProjectionSourceV1"];
    assert_eq!(source_union.oneof_decl.len(), 1);
    assert_eq!(source_union.oneof_decl[0].name(), "source");
    assert_eq!(
        source_union
            .field
            .iter()
            .map(|field| (field.name(), field.number(), field.oneof_index))
            .collect::<Vec<_>>(),
        vec![("scalar", 1, Some(0)), ("vector", 2, Some(0))]
    );
    let enum_values = |name: &str| {
        source
            .enum_type
            .iter()
            .find(|item| item.name() == name)
            .expect("named columnar enum")
            .value
            .iter()
            .map(|value| (value.name(), value.number()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        enum_values("ColumnarProjectionLayoutV1"),
        vec![
            ("COLUMNAR_PROJECTION_LAYOUT_UNSPECIFIED", 0),
            ("COLUMNAR_PROJECTION_LAYOUT_V1", 1),
            ("COLUMNAR_PROJECTION_LAYOUT_V2", 2),
        ]
    );
    assert_eq!(
        enum_values("ColumnarProjectionGenerationRoleV1"),
        vec![
            ("COLUMNAR_PROJECTION_GENERATION_ROLE_UNSPECIFIED", 0),
            ("COLUMNAR_PROJECTION_GENERATION_ROLE_PUBLISHED", 1),
            ("COLUMNAR_PROJECTION_GENERATION_ROLE_CANDIDATE", 2),
            ("COLUMNAR_PROJECTION_GENERATION_ROLE_PREDECESSOR", 3),
        ]
    );
    assert_eq!(
        enum_values("ColumnarProjectionLifecycleV1"),
        vec![
            ("COLUMNAR_PROJECTION_LIFECYCLE_UNSPECIFIED", 0),
            ("COLUMNAR_PROJECTION_LIFECYCLE_BUILDING", 1),
            ("COLUMNAR_PROJECTION_LIFECYCLE_CATCHING_UP", 2),
            ("COLUMNAR_PROJECTION_LIFECYCLE_READY", 3),
            ("COLUMNAR_PROJECTION_LIFECYCLE_REBUILDING", 4),
            ("COLUMNAR_PROJECTION_LIFECYCLE_DEGRADED", 5),
            ("COLUMNAR_PROJECTION_LIFECYCLE_INVALID", 6),
        ]
    );
    assert_eq!(
        enum_values("ColumnarProjectionFailureTargetV1"),
        vec![
            ("COLUMNAR_PROJECTION_FAILURE_TARGET_UNSPECIFIED", 0),
            ("COLUMNAR_PROJECTION_FAILURE_TARGET_CANDIDATE", 1),
            ("COLUMNAR_PROJECTION_FAILURE_TARGET_PUBLISHED", 2),
            ("COLUMNAR_PROJECTION_FAILURE_TARGET_PREDECESSOR", 3),
            ("COLUMNAR_PROJECTION_FAILURE_TARGET_CONTROL", 4),
        ]
    );
    assert_eq!(
        enum_values("ColumnarProjectionFailureReasonV1"),
        vec![
            ("COLUMNAR_PROJECTION_FAILURE_REASON_UNSPECIFIED", 0),
            ("COLUMNAR_PROJECTION_FAILURE_REASON_REPLAY_AGE", 1),
            ("COLUMNAR_PROJECTION_FAILURE_REASON_REPLAY_BYTES", 2),
            ("COLUMNAR_PROJECTION_FAILURE_REASON_REPLAY_BACKLOG", 3),
            ("COLUMNAR_PROJECTION_FAILURE_REASON_SPEC_CHANGED", 4),
            ("COLUMNAR_PROJECTION_FAILURE_REASON_ARTIFACT_INVALID", 5),
            ("COLUMNAR_PROJECTION_FAILURE_REASON_RESOURCE_LIMIT", 6),
            ("COLUMNAR_PROJECTION_FAILURE_REASON_STORAGE", 7),
            ("COLUMNAR_PROJECTION_FAILURE_REASON_CANCELLED", 8),
        ]
    );
    for (enumeration, expected) in [
        ("ColumnarProjectionLayoutV1", (3, 15)),
        ("ColumnarProjectionGenerationRoleV1", (4, 15)),
        ("ColumnarProjectionLifecycleV1", (7, 15)),
        ("ColumnarProjectionFailureTargetV1", (5, 15)),
        ("ColumnarProjectionFailureReasonV1", (9, 31)),
    ] {
        let ranges = source
            .enum_type
            .iter()
            .find(|item| item.name() == enumeration)
            .expect("named columnar enum")
            .reserved_range
            .iter()
            .map(|range| (range.start(), range.end()))
            .collect::<Vec<_>>();
        assert_eq!(ranges, vec![expected], "{enumeration} reserved range");
    }
    let schema = writable_record_schema("riffdb.storage.v1.StoredColumnarProjectionControlV1")
        .expect("tag-67 common columnar control is registered");
    assert_eq!(schema.compact_tag(), 67);
    assert_eq!(schema.schema_revision(), 1);
    assert_eq!(schema.max_payload_bytes(), 8_192);
    assert_eq!(schema.max_envelope_bytes(), 8_289);
    assert_eq!(
        schema_hash_hex(schema),
        "c1f21728900fac6f2c3b3b1d37d6a6cb1107d769e3ca2d83da25958c497baadd"
    );
}

#[test]
fn generated_registry_fixtures_freeze_exact_membership_and_hashes() {
    let legacy = registry_fixture_entries(LEGACY_REGISTRY_FIXTURE, 26);
    let readable = registry_fixture_entries(READABLE_REGISTRY_FIXTURE, 115);
    let writable = registry_fixture_entries(WRITABLE_REGISTRY_FIXTURE, 92);

    assert_eq!(legacy, readable[..legacy.len()]);
    assert_eq!(
        readable,
        READABLE_RECORD_SCHEMAS
            .iter()
            .map(|schema| (schema.record_type().to_owned(), schema_hash_hex(schema)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        writable,
        WRITABLE_RECORD_SCHEMAS
            .iter()
            .map(|schema| (schema.record_type().to_owned(), schema_hash_hex(schema)))
            .collect::<Vec<_>>()
    );
}

#[test]
fn canonical_command_capsule_records_are_closed_current_schemas() {
    for (record_type, compact_tag) in [
        ("riffdb.storage.v1.StoredCommandCapsuleV1", 51),
        ("riffdb.storage.v1.StoredCommandLocatorV1", 52),
        ("riffdb.storage.v1.StoredCommandAuditLocatorV1", 53),
        (
            "riffdb.storage.v1.StoredCommandDerivedIndexCheckpointV1",
            56,
        ),
    ] {
        let readable = readable_record_schema(record_type)
            .unwrap_or_else(|| panic!("{record_type} must be readable"));
        let writable = writable_record_schema(record_type)
            .unwrap_or_else(|| panic!("{record_type} must be current writable"));
        assert_eq!(
            readable.record_type(),
            writable.record_type(),
            "{record_type}"
        );
        assert_eq!(
            readable.schema_hash(),
            writable.schema_hash(),
            "{record_type}"
        );
        assert_eq!(readable.compact_tag(), compact_tag, "{record_type}");
        assert_eq!(readable.schema_revision(), 1, "{record_type}");
    }
    for (legacy, current, compact_tag) in [
        (
            "riffdb.storage.v1.StoredCommandCapsuleV2",
            "riffdb.storage.v1.StoredCommandCapsuleV6",
            54,
        ),
        (
            "riffdb.storage.v1.StoredCommandSegmentV1",
            "riffdb.storage.v1.StoredCommandSegmentV5",
            55,
        ),
    ] {
        assert!(readable_record_schema(legacy).is_some(), "{legacy}");
        assert!(writable_record_schema(legacy).is_none(), "{legacy}");
        let schema = writable_record_schema(current)
            .unwrap_or_else(|| panic!("{current} must be current writable"));
        assert_eq!(schema.compact_tag(), compact_tag, "{current}");
        assert_eq!(schema.schema_revision(), 5, "{current}");
    }
}

#[test]
fn vector_observation_is_a_closed_current_schema() {
    let message = riffdb_proto::storage::v1::StoredVectorObservationV1 {
        contract_lineage: "vectors".to_owned(),
        partition_key: vec![1, 2, 3],
        entity_type_id: 1,
        vector_field_id: 2,
        total_entities: 1,
        source_stale_entities: 0,
        model_counts: vec![riffdb_proto::storage::v1::StoredVectorModelCountV1 {
            model_identity: "model".to_owned(),
            model_version: "v1".to_owned(),
            entity_count: 1,
        }],
        revision_sequence: 1,
    };
    let encoded = riffdb_proto::durable::encode_current_message(&message);
    assert!(encoded.is_ok(), "vector observation encoding: {encoded:?}");

    let health = riffdb_proto::storage::v1::StoredVectorHealthObservationV1 {
        contract_lineage: "vectors".to_owned(),
        fields: vec![
            riffdb_proto::storage::v1::StoredVectorHealthFieldObservationV1 {
                entity_type_id: 1,
                vector_field_id: 2,
                stale_entity_count_threshold: 3,
                partition_count: 1,
                breached_partition_count: 0,
            },
        ],
        revision_sequence: 1,
    };
    let encoded = riffdb_proto::durable::encode_current_message(&health);
    assert!(encoded.is_ok(), "vector health encoding: {encoded:?}");
}

#[test]
fn correlated_index_transition_successor_preserves_the_frozen_predecessor_bound() {
    let transitions =
        vec![riffdb_proto::storage::v1::StoredIndexGenerationTransitionV1::default(); 4_097];
    let predecessor = riffdb_proto::storage::v1::StoredCommandCapsuleV5 {
        index_generation_transitions: transitions.clone(),
        ..Default::default()
    };
    assert!(
        riffdb_proto::durable::encode_current_message(&predecessor).is_err(),
        "the frozen V5 structural bound must not widen"
    );

    let successor = riffdb_proto::storage::v1::StoredCommandCapsuleV6 {
        index_generation_transitions: transitions,
        ..Default::default()
    };
    assert!(
        riffdb_proto::durable::encode_current_message(&successor).is_ok(),
        "V6 admits a collection above the historical bound"
    );
    let over_limit = riffdb_proto::storage::v1::StoredCommandCapsuleV6 {
        index_generation_transitions: vec![
            riffdb_proto::storage::v1::StoredIndexGenerationTransitionV1::default();
            65_536
        ],
        ..Default::default()
    };
    assert!(
        riffdb_proto::durable::encode_current_message(&over_limit).is_err(),
        "V6 remains structurally bounded"
    );
}

#[test]
fn service_value_successors_preserve_compact_record_roles() {
    for (legacy, current, compact_tag, current_revision) in [
        (
            "riffdb.storage.v1.StoredPendingAdmissionV2",
            "riffdb.storage.v1.StoredPendingAdmissionV3",
            11,
            3,
        ),
        (
            "riffdb.storage.v1.StoredExecutionFailedV2",
            "riffdb.storage.v1.StoredExecutionFailedV3",
            12,
            3,
        ),
        (
            "riffdb.storage.v1.StoredOutcomeV2",
            "riffdb.storage.v1.StoredOutcomeV3",
            13,
            3,
        ),
        (
            "riffdb.storage.v1.StoredCommandCapsuleV3",
            "riffdb.storage.v1.StoredCommandCapsuleV6",
            54,
            5,
        ),
        (
            "riffdb.storage.v1.StoredCommandSegmentV2",
            "riffdb.storage.v1.StoredCommandSegmentV5",
            55,
            5,
        ),
    ] {
        let legacy_schema = readable_record_schema(legacy)
            .unwrap_or_else(|| panic!("{legacy} must remain readable"));
        let current_schema = writable_record_schema(current)
            .unwrap_or_else(|| panic!("{current} must be current writable"));
        assert_eq!(legacy_schema.compact_tag(), compact_tag, "{legacy}");
        assert_eq!(current_schema.compact_tag(), compact_tag, "{current}");
        assert_eq!(
            current_schema.schema_revision(),
            current_revision,
            "{current}"
        );
        assert!(writable_record_schema(legacy).is_none(), "{legacy}");
    }
}

fn registry_fixture_entries(fixture: &str, expected_count: usize) -> Vec<(String, String)> {
    let mut lines = fixture.lines();
    let _header = lines.next().expect("registry fixture header");
    assert_eq!(
        lines.next(),
        Some(format!("records {expected_count}").as_str())
    );
    let entries = lines
        .map(|line| {
            let mut columns = line.split_ascii_whitespace();
            let record_type = columns.next().expect("registry record type").to_owned();
            let schema_hash = columns
                .find_map(|column| column.strip_prefix("schema-hash="))
                .expect("registry schema hash")
                .to_owned();
            (record_type, schema_hash)
        })
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), expected_count);
    entries
}

fn schema_hash_hex(schema: &riffdb_proto::envelope::RecordSchema<'_>) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(64);
    for byte in schema.schema_hash().as_bytes() {
        write!(output, "{byte:02x}").expect("String writes are infallible");
    }
    output
}

#[test]
// req: STO-012
fn tag_67_successor_registry_digest_is_frozen() {
    // The accepted predecessor remains frozen, even as WP-772 registers its
    // own additive identities. WP-749 successors at existing tags are likewise
    // excluded from this historical set, as is ADR-0232's export-head revision.
    // Never replace its frozen digest.
    let mut schemas = READABLE_RECORD_SCHEMAS
        .iter()
        .filter(|schema| {
            schema.compact_tag() <= 67
                && !(schema.compact_tag() == 22 && schema.schema_revision() >= 3)
                && !(matches!(schema.compact_tag(), 54 | 55) && schema.schema_revision() >= 6)
                && !(schema.compact_tag() == 60 && schema.schema_revision() >= 2)
        })
        .collect::<Vec<_>>();
    schemas.sort_by_key(|schema| (schema.compact_tag(), schema.schema_revision()));
    assert_eq!(schemas.len(), 98);
    let mut canonical = b"riffdb-compact-record-registry-v2\0".to_vec();
    canonical.extend_from_slice(&98_u16.to_be_bytes());
    for schema in schemas {
        canonical.push(schema.compact_tag());
        canonical.extend_from_slice(&schema.schema_revision().to_be_bytes());
        canonical.extend_from_slice(
            &u16::try_from(schema.record_type().len())
                .unwrap()
                .to_be_bytes(),
        );
        canonical.extend_from_slice(schema.record_type().as_bytes());
        canonical.extend_from_slice(schema.schema_hash().as_bytes());
    }
    let predecessor = hash_schema(&canonical);
    assert_ne!(predecessor, record_registry_digest());
    let actual = predecessor
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        actual,
        "9c44ee378921101a16324eb22326c26c048eddab0fcfe5cb9a1408890a5f54ed"
    );
}

#[test]
fn every_semantic_golden_payload_and_envelope_is_canonical() {
    let mut lines = DURABLE_WIRE_VECTORS.lines();
    assert_eq!(lines.next(), Some("riffdb-durable-wire-vectors-v1"));
    assert_eq!(lines.next(), Some("records\t27"));
    let vectors = lines.collect::<Vec<_>>();
    assert_eq!(vectors.len(), 27);

    let registry = readable_record_registry();
    for line in vectors {
        let columns = line.split('\t').collect::<Vec<_>>();
        assert_eq!(columns.len(), 3, "one FQN, payload, and envelope per line");
        let schema = readable_record_schema(columns[0])
            .unwrap_or_else(|| panic!("{} must remain readable", columns[0]));

        let payload = decode_lower_hex(columns[1]);
        let envelope = decode_lower_hex(columns[2]);
        assert_eq!(
            encode_v1(schema, &payload).expect("semantic golden payload is canonical"),
            envelope,
            "{}",
            schema.record_type()
        );

        let decoded = registry
            .decode(&envelope)
            .expect("semantic golden envelope passes the closed registry");
        assert_eq!(decoded.record_type(), schema.record_type());
        assert_eq!(decoded.schema_hash(), schema.schema_hash());
        assert_eq!(decoded.payload(), payload);

        let raw = StoredEnvelope::decode(envelope.as_slice()).expect("golden outer message");
        assert_eq!(raw.encode_to_vec(), envelope);
        assert_eq!(raw.payload, payload);
    }
}

#[test]
fn pre_wp280_capability_payload_remains_readable_under_its_original_hash() {
    let line = DURABLE_WIRE_VECTORS
        .lines()
        .find(|line| line.starts_with("riffdb.storage.v1.CapabilityRecordV1\t"))
        .expect("capability golden");
    let payload = decode_lower_hex(line.split('\t').nth(1).expect("capability payload column"));
    let legacy = READABLE_RECORD_SCHEMAS
        .get(READABLE_RECORD_SCHEMA_COUNT - 5)
        .expect("pre-WP280 capability reader");
    let envelope = encode(legacy, &payload).expect("old payload is canonical under old hash");
    let decoded = readable_record_registry()
        .decode(&envelope)
        .expect("old capability envelope remains readable");
    assert_eq!(decoded.record_type(), legacy.record_type());
    assert_eq!(decoded.schema_hash(), legacy.schema_hash());
    assert_eq!(decoded.payload(), payload);
}

#[test]
fn semantic_optional_wire_presence_is_exact() {
    let descriptors = descriptors();
    let messages = message_map(&descriptors);
    let actual = messages
        .iter()
        .flat_map(|(message_name, message)| {
            message
                .field
                .iter()
                .filter(|field| field.proto3_optional())
                .map(move |field| format!("{message_name}.{}", field.name()))
        })
        .collect::<BTreeSet<_>>();
    let expected = [
        "AdmittedActorContextV1.agent_session_id",
        "CapabilityAdministrationAuditV1.approval_id",
        "CapabilityAdministrationAuditV1.revocation_reason",
        "CapabilityPermissionV1.contract_lineage",
        "CapabilityPermissionV1.query_module_hash",
        "CapabilityPermissionV1.query_name",
        "CapabilityPermissionV1.application_role_hash",
        "CapabilityPermissionV1.reactive_module_hash",
        "CapabilityPermissionV1.reactive_operation_name",
        "CapabilityPermissionV1.stable_id",
        "OutboxDeadLetterV1.last_safe_error",
        "OutboxRetryMetadataV1.last_safe_error",
        "ProjectionFailureV1.at_sequence",
        "ServiceAuditRecordV1.approval_id",
        "ServiceAuditRecordV2.approval_id",
        "ServiceAuditRecordV3.approval_id",
        "StoredAdmittedProvenanceClaimsV1.approval_id",
        "StoredAdmittedProvenanceClaimsV1.reason",
        "StoredAdmittedProvenanceClaimsV1.source_commit",
        "StoredAdmittedProvenanceClaimsV1.source_repository",
        "StoredCatalogAdministrationV1.approval_id",
        "StoredChangelogV2RotationReceiptV1.predecessor_administration_frontier",
        "StoredChangelogV2RotationReceiptV1.predecessor_application_frontier",
        "StoredColumnarProjectionControlV1.candidate",
        "StoredColumnarProjectionControlV1.failure",
        "StoredColumnarProjectionControlV1.predecessor",
        "StoredColumnarProjectionControlV1.published",
        "StoredColumnarProjectionFailureV1.generation",
        "StoredColumnarProjectionFrontierV1.applied_through",
        "StoredColumnarProjectionGenerationV1.artifact_length",
        "StoredColumnarProjectionGenerationV1.checksum_bytes",
        "StoredColumnarProjectionGenerationV1.physical_generation_fingerprint",
        "StoredColumnarProjectionGenerationV1.snapshot_frontier",
        "StoredCommittedEntityTransitionV1.prior_transition_hash",
        "StoredCommandAuditInvocationV1.approval_id",
        "StoredContractMigrationJournalV1.exclusive_cursor",
        "StoredContractMigrationJournalV1.frozen_application_frontier",
        "StoredContractMigrationJournalV1.previous_journal_hash",
        "StoredContractMigrationRecordV1.approval_id",
        "StoredContractMigrationRecordV1.predecessor_application_frontier",
        "StoredContractMigrationRecordV1.successor_application_frontier",
        "StoredHistoryTombstoneV1.previous_tombstone_hash",
        "StoredIndexGenerationTransitionV1.prior_generation",
        "StoredProjectionControlV1.published_apply_mode",
        "StoredQueryModuleAdministrationV1.approval_id",
        "StoredPrimaryFenceAdministrationV1.approval_id",
        "StoredPromotionAdministrationV1.approval_id",
        "StoredEventConsumerV1.checkpoint",
        "StoredEntityChainStateV1.entity_version",
        "StoredReactiveModuleAdministrationV1.approval_id",
        "StoredRetentionWatermarkV1.chain_root_registry_digest",
        "StoredReplicationSourceHoldV2.expires_at_application_sequence",
        "StoredValidatedPrefixCheckpointV1.previous_checkpoint_hash",
        "StoredVectorEvidenceIndexV1.newest_source_write_sequence",
        "StoredVectorEvidenceV1.newest_source_write_sequence",
        "StoredVectorProjectionControlV1.rebuild_reason",
        "StoredVectorProjectionFrontierV1.applied_through",
    ]
    .into_iter()
    .map(|suffix| format!("riffdb.storage.v1.{suffix}"))
    .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);

    for (message, field) in [
        ("StoredCatalogAdministrationV1", "previous_active"),
        ("CapabilityAdministrationAuditV1", "initiator"),
        ("ServiceAuditRecordV1", "principal"),
        ("ServiceAuditRecordV2", "principal"),
        ("OutboxRetryMetadataV1", "next_attempt_at"),
        ("StoredProjectionControlV1", "published"),
        ("StoredProjectionControlV1", "candidate"),
        ("StoredProjectionControlV1", "failure"),
    ] {
        let descriptor = messages[&format!("riffdb.storage.v1.{message}")];
        let field = descriptor
            .field
            .iter()
            .find(|candidate| candidate.name() == field)
            .expect("accepted optional message field");
        assert!(!field.proto3_optional());
        assert!(field.oneof_index.is_none());
        assert_eq!(field.r#type(), Type::Message);
    }
}

#[test]
fn closed_oneof_and_enum_registries_are_exact() {
    let descriptors = descriptors();
    let messages = message_map(&descriptors);
    let mut oneofs = BTreeMap::new();
    for (message_name, message) in &messages {
        for (index, oneof) in message.oneof_decl.iter().enumerate() {
            let fields = message
                .field
                .iter()
                .filter(|field| {
                    field.oneof_index == i32::try_from(index).ok() && !field.proto3_optional()
                })
                .map(|field| (field.name(), field.number()))
                .collect::<Vec<_>>();
            if !fields.is_empty() {
                oneofs.insert(format!("{message_name}.{}", oneof.name()), fields);
            }
        }
    }
    assert_eq!(
        oneofs,
        BTreeMap::from([
            (
                "riffdb.storage.v1.StoredChangelogTransactionAllocatorV3.state".to_owned(),
                vec![("next_transaction_sequence", 1), ("exhausted", 2)],
            ),
            (
                "riffdb.storage.v1.StoredReplicationFollowerStateV3.state".to_owned(),
                vec![("detached", 1), ("attached", 2)],
            ),
            (
                "riffdb.storage.v1.ReplicationPrimaryAdmissionV1.state".to_owned(),
                vec![("active", 1), ("fenced", 2)],
            ),
            (
                "riffdb.storage.v1.CapabilityLifecycleV1.state".to_owned(),
                vec![("active", 1), ("revoked", 2)],
            ),
            (
                "riffdb.storage.v1.ExpectedEntityStateV1.state".to_owned(),
                vec![("absent", 1), ("present_entity_version", 2)],
            ),
            (
                "riffdb.storage.v1.FrontierPositionV1.position".to_owned(),
                vec![("before_first", 1), ("applied_through", 2)],
            ),
            (
                "riffdb.storage.v1.IndexEpochPositionV1.position".to_owned(),
                vec![("before_first", 1), ("epoch", 2)],
            ),
            (
                "riffdb.storage.v1.PartitionScopeV1.scope".to_owned(),
                vec![("all", 1), ("explicit", 2)],
            ),
            (
                "riffdb.storage.v1.ServiceAuditLinkV1.link".to_owned(),
                vec![("none", 1), ("command", 2), ("control_plane", 3)],
            ),
            (
                "riffdb.storage.v1.ServiceAuditTargetV1.target".to_owned(),
                vec![
                    ("contract_lineage", 1),
                    ("contract_version", 2),
                    ("entity_type", 3),
                    ("command", 4),
                    ("projection", 5),
                    ("index", 6),
                    ("commit_sequence", 7),
                    ("provenance_id", 8),
                    ("capability_id", 9),
                ],
            ),
            (
                "riffdb.storage.v1.ServiceAuditTargetV2.target".to_owned(),
                vec![
                    ("contract_lineage", 1),
                    ("contract_version", 2),
                    ("entity_type", 3),
                    ("command", 4),
                    ("projection", 5),
                    ("index", 6),
                    ("commit_sequence", 7),
                    ("provenance_id", 8),
                    ("capability_id", 9),
                    ("event_consumer", 10),
                ],
            ),
            (
                "riffdb.storage.v1.ServiceAuditTargetV3.target".to_owned(),
                vec![
                    ("contract_lineage", 1),
                    ("contract_version", 2),
                    ("entity_type", 3),
                    ("command", 4),
                    ("projection", 5),
                    ("index", 6),
                    ("commit_sequence", 7),
                    ("provenance_id", 8),
                    ("capability_id", 9),
                    ("event_consumer", 10),
                    ("replication_follower", 11),
                ],
            ),
            (
                "riffdb.storage.v1.StoredReplicationAdministrationV1.before".to_owned(),
                vec![("absent", 7), ("legacy", 8), ("registered", 9)],
            ),
            (
                "riffdb.storage.v1.StoredReplicationAdministrationV1.origin".to_owned(),
                vec![("explicit", 11), ("configured_expiry", 12)],
            ),
            (
                "riffdb.storage.v1.StoredAdministrationSequenceAllocatorV1.state".to_owned(),
                vec![("next_administration_sequence", 1), ("exhausted", 2)],
            ),
            (
                "riffdb.storage.v1.StoredApplicationSequenceAllocatorV1.state".to_owned(),
                vec![("next_commit_sequence", 1), ("exhausted", 2)],
            ),
            (
                "riffdb.storage.v1.StoredColumnarProjectionSourceV1.source".to_owned(),
                vec![("scalar", 1), ("vector", 2)],
            ),
            (
                "riffdb.storage.v1.StoredDurableEventVariantV1.value".to_owned(),
                vec![("unanchored", 1), ("anchored", 2)],
            ),
            (
                "riffdb.storage.v1.StoredOutboxStatusV1.state".to_owned(),
                vec![
                    ("pending", 2),
                    ("delivering", 3),
                    ("delivered", 4),
                    ("dead_letter", 5),
                ],
            ),
            (
                "riffdb.storage.v1.StoredEventConsumerDeliveryV1.state".to_owned(),
                vec![("leased", 4), ("retry", 5), ("dead_lettered", 6)],
            ),
            (
                "riffdb.storage.v1.StoredReadDependencyV1.dependency".to_owned(),
                vec![("entity_observation", 1), ("index_range_epoch", 2)],
            ),
            (
                "riffdb.storage.v1.TenantScopeV1.scope".to_owned(),
                vec![("global", 1), ("tenant_id", 2)],
            ),
        ])
    );

    let mut enums = descriptors
        .file
        .iter()
        .flat_map(|file| {
            file.enum_type.iter().map(|enumeration| {
                let values = enumeration
                    .value
                    .iter()
                    .map(|value| format!("{}={}", value.name(), value.number()))
                    .collect::<Vec<_>>()
                    .join(",");
                (enumeration.name().to_owned(), values)
            })
        })
        .collect::<Vec<_>>();
    enums.sort();
    assert_eq!(
        enums,
        [
            ("ActorKindV1", "ACTOR_KIND_UNSPECIFIED=0,ACTOR_KIND_HUMAN=1,ACTOR_KIND_AGENT=2,ACTOR_KIND_SERVICE=3"),
            ("CapabilityAdministrationOperationV1", "CAPABILITY_ADMINISTRATION_OPERATION_UNSPECIFIED=0,CAPABILITY_ADMINISTRATION_OPERATION_BOOTSTRAP=1,CAPABILITY_ADMINISTRATION_OPERATION_CREATE=2,CAPABILITY_ADMINISTRATION_OPERATION_REVOKE=3"),
            ("CapabilityApplicationExportScopeV1", "CAPABILITY_APPLICATION_EXPORT_SCOPE_UNSPECIFIED=0,CAPABILITY_APPLICATION_EXPORT_SCOPE_PRINCIPAL_FILTERED=1,CAPABILITY_APPLICATION_EXPORT_SCOPE_WHOLE_APPLICATION=2"),
            ("CapabilityApplicationReimportScopeV1", "CAPABILITY_APPLICATION_REIMPORT_SCOPE_UNSPECIFIED=0,CAPABILITY_APPLICATION_REIMPORT_SCOPE_PRINCIPAL_FILTERED=1,CAPABILITY_APPLICATION_REIMPORT_SCOPE_WHOLE_APPLICATION=2"),
            ("CapabilityPermissionKindV1", "CAPABILITY_PERMISSION_KIND_UNSPECIFIED=0,CAPABILITY_PERMISSION_KIND_VALIDATE_CONTRACT=1,CAPABILITY_PERMISSION_KIND_READ_CONTRACT=2,CAPABILITY_PERMISSION_KIND_EXPLAIN_COMMAND=3,CAPABILITY_PERMISSION_KIND_DEPLOY_CONTRACT=4,CAPABILITY_PERMISSION_KIND_INVOKE_COMMAND=5,CAPABILITY_PERMISSION_KIND_READ_ENTITY=6,CAPABILITY_PERMISSION_KIND_SCAN_INDEX=7,CAPABILITY_PERMISSION_KIND_QUERY_PROJECTION=8,CAPABILITY_PERMISSION_KIND_READ_PROJECTION_STATUS=9,CAPABILITY_PERMISSION_KIND_READ_COMMIT=10,CAPABILITY_PERMISSION_KIND_SCAN_COMMITS=11,CAPABILITY_PERMISSION_KIND_SUBSCRIBE_COMMITS=12,CAPABILITY_PERMISSION_KIND_READ_PROVENANCE=13,CAPABILITY_PERMISSION_KIND_INSPECT_OUTBOX=14,CAPABILITY_PERMISSION_KIND_READ_HEALTH=15,CAPABILITY_PERMISSION_KIND_READ_STATISTICS=16,CAPABILITY_PERMISSION_KIND_CREATE_CAPABILITY=17,CAPABILITY_PERMISSION_KIND_REVOKE_CAPABILITY=18,CAPABILITY_PERMISSION_KIND_ADMINISTER_CAPABILITIES=19,CAPABILITY_PERMISSION_KIND_CHECK_AD_HOC_QUERY=20,CAPABILITY_PERMISSION_KIND_EXPLAIN_AD_HOC_QUERY=21,CAPABILITY_PERMISSION_KIND_EXECUTE_AD_HOC_QUERY=22,CAPABILITY_PERMISSION_KIND_EXPLAIN_NAMED_QUERY=23,CAPABILITY_PERMISSION_KIND_EXECUTE_NAMED_QUERY=24,CAPABILITY_PERMISSION_KIND_APPLICATION_ROLE_IDENTITY=25,CAPABILITY_PERMISSION_KIND_CONSUME_EVENT_STREAM=27,CAPABILITY_PERMISSION_KIND_SEEK_EVENT_STREAM_CONSUMER=28,CAPABILITY_PERMISSION_KIND_WATCH_NAMED_QUERY=29,CAPABILITY_PERMISSION_KIND_CONSUME_CONTEXTUAL_SUBSCRIPTION=30"),
            ("CapabilityRowPolicyOperationV1", "CAPABILITY_ROW_POLICY_OPERATION_UNSPECIFIED=0,CAPABILITY_ROW_POLICY_OPERATION_READ=1,CAPABILITY_ROW_POLICY_OPERATION_CREATE=2,CAPABILITY_ROW_POLICY_OPERATION_UPDATE=3,CAPABILITY_ROW_POLICY_OPERATION_DELETE=4"),
            ("ColumnarProjectionFailureReasonV1", "COLUMNAR_PROJECTION_FAILURE_REASON_UNSPECIFIED=0,COLUMNAR_PROJECTION_FAILURE_REASON_REPLAY_AGE=1,COLUMNAR_PROJECTION_FAILURE_REASON_REPLAY_BYTES=2,COLUMNAR_PROJECTION_FAILURE_REASON_REPLAY_BACKLOG=3,COLUMNAR_PROJECTION_FAILURE_REASON_SPEC_CHANGED=4,COLUMNAR_PROJECTION_FAILURE_REASON_ARTIFACT_INVALID=5,COLUMNAR_PROJECTION_FAILURE_REASON_RESOURCE_LIMIT=6,COLUMNAR_PROJECTION_FAILURE_REASON_STORAGE=7,COLUMNAR_PROJECTION_FAILURE_REASON_CANCELLED=8"),
            ("ColumnarProjectionFailureTargetV1", "COLUMNAR_PROJECTION_FAILURE_TARGET_UNSPECIFIED=0,COLUMNAR_PROJECTION_FAILURE_TARGET_CANDIDATE=1,COLUMNAR_PROJECTION_FAILURE_TARGET_PUBLISHED=2,COLUMNAR_PROJECTION_FAILURE_TARGET_PREDECESSOR=3,COLUMNAR_PROJECTION_FAILURE_TARGET_CONTROL=4"),
            ("ColumnarProjectionGenerationRoleV1", "COLUMNAR_PROJECTION_GENERATION_ROLE_UNSPECIFIED=0,COLUMNAR_PROJECTION_GENERATION_ROLE_PUBLISHED=1,COLUMNAR_PROJECTION_GENERATION_ROLE_CANDIDATE=2,COLUMNAR_PROJECTION_GENERATION_ROLE_PREDECESSOR=3"),
            ("ColumnarProjectionLayoutV1", "COLUMNAR_PROJECTION_LAYOUT_UNSPECIFIED=0,COLUMNAR_PROJECTION_LAYOUT_V1=1,COLUMNAR_PROJECTION_LAYOUT_V2=2"),
            ("ColumnarProjectionLifecycleV1", "COLUMNAR_PROJECTION_LIFECYCLE_UNSPECIFIED=0,COLUMNAR_PROJECTION_LIFECYCLE_BUILDING=1,COLUMNAR_PROJECTION_LIFECYCLE_CATCHING_UP=2,COLUMNAR_PROJECTION_LIFECYCLE_READY=3,COLUMNAR_PROJECTION_LIFECYCLE_REBUILDING=4,COLUMNAR_PROJECTION_LIFECYCLE_DEGRADED=5,COLUMNAR_PROJECTION_LIFECYCLE_INVALID=6"),
            ("CommandDerivedIndexKindV1", "COMMAND_DERIVED_INDEX_KIND_UNSPECIFIED=0,COMMAND_DERIVED_INDEX_KIND_IDEMPOTENCY=1,COMMAND_DERIVED_INDEX_KIND_PROVENANCE=2,COMMAND_DERIVED_INDEX_KIND_AUDIT_SEQUENCE=3,COMMAND_DERIVED_INDEX_KIND_AUDIT_REQUEST=4,COMMAND_DERIVED_INDEX_KIND_EVENT_ROUTE=5,COMMAND_DERIVED_INDEX_KIND_PENDING_OUTBOX=6"),
            ("CommandDerivedMemberV1", "COMMAND_DERIVED_MEMBER_UNSPECIFIED=0,COMMAND_DERIVED_MEMBER_COMMAND=1,COMMAND_DERIVED_MEMBER_AUDIT_STARTED=2,COMMAND_DERIVED_MEMBER_AUDIT_TERMINAL=3,COMMAND_DERIVED_MEMBER_EVENT=4"),
            ("ContractMigrationJournalStepV1", "CONTRACT_MIGRATION_JOURNAL_STEP_UNSPECIFIED=0,CONTRACT_MIGRATION_JOURNAL_STEP_TRANSFORMING=1,CONTRACT_MIGRATION_JOURNAL_STEP_REBUILDING_PROJECTIONS=2,CONTRACT_MIGRATION_JOURNAL_STEP_VALIDATING=3,CONTRACT_MIGRATION_JOURNAL_STEP_READY_FOR_CUTOVER=4,CONTRACT_MIGRATION_JOURNAL_STEP_COMPLETE=5"),
            ("DurabilityModeV1", "DURABILITY_MODE_UNSPECIFIED=0,DURABILITY_MODE_SYNC=1,DURABILITY_MODE_GROUP=2,DURABILITY_MODE_MEMORY=3"),
            ("EntityChainStateKindV1", "ENTITY_CHAIN_STATE_KIND_UNSPECIFIED=0,ENTITY_CHAIN_STATE_KIND_NEVER_EXISTED=1,ENTITY_CHAIN_STATE_KIND_LIVE=2,ENTITY_CHAIN_STATE_KIND_DELETED=3"),
            ("ExecutionFailureCodeV1", "EXECUTION_FAILURE_CODE_UNSPECIFIED=0,EXECUTION_FAILURE_CODE_ARITHMETIC_FAULT=1,EXECUTION_FAILURE_CODE_RESOURCE_LIMIT=2,EXECUTION_FAILURE_CODE_UNIQUE_CONFLICT=3"),
            ("ProjectionFailureCodeV1", "PROJECTION_FAILURE_CODE_UNSPECIFIED=0,PROJECTION_FAILURE_CODE_ARITHMETIC_OVERFLOW=1,PROJECTION_FAILURE_CODE_MALFORMED_DURABLE_EVENT=2,PROJECTION_FAILURE_CODE_MISSING_COMMIT=3,PROJECTION_FAILURE_CODE_PLAN_OR_SCHEMA_UNAVAILABLE=4,PROJECTION_FAILURE_CODE_PROJECTION_STATE_INTEGRITY=5,PROJECTION_FAILURE_CODE_HARD_LIMIT_EXCEEDED=6"),
            ("ProjectionLifecycleV1", "PROJECTION_LIFECYCLE_UNSPECIFIED=0,PROJECTION_LIFECYCLE_BUILDING=1,PROJECTION_LIFECYCLE_CATCHING_UP=2,PROJECTION_LIFECYCLE_READY=3,PROJECTION_LIFECYCLE_DEGRADED=4,PROJECTION_LIFECYCLE_REBUILDING=5,PROJECTION_LIFECYCLE_INVALID=6"),
            ("PublishedApplyModeV1", "PUBLISHED_APPLY_MODE_UNSPECIFIED=0,PUBLISHED_APPLY_MODE_ENABLED=1,PUBLISHED_APPLY_MODE_SUSPENDED=2"),
            ("RetentionAdministrationActionV1", "RETENTION_ADMINISTRATION_ACTION_V1_UNSPECIFIED=0,RETENTION_ADMINISTRATION_ACTION_V1_PROJECTION_DETACH=1,RETENTION_ADMINISTRATION_ACTION_V1_PROJECTION_REATTACH=2"),
            ("RetentionHoldKindV1", "RETENTION_HOLD_KIND_V1_UNSPECIFIED=0,RETENTION_HOLD_KIND_V1_OPERATOR=1,RETENTION_HOLD_KIND_V1_PROJECTION_DETACH=2"),
            ("RevocationReasonCodeV1", "REVOCATION_REASON_CODE_UNSPECIFIED=0,REVOCATION_REASON_CODE_REQUESTED=1,REVOCATION_REASON_CODE_REPLACED=2,REVOCATION_REASON_CODE_SUSPECTED_COMPROMISE=3,REVOCATION_REASON_CODE_POLICY_CHANGE=4"),
            ("ServiceAuditPhaseV1", "SERVICE_AUDIT_PHASE_UNSPECIFIED=0,SERVICE_AUDIT_PHASE_STARTED=1,SERVICE_AUDIT_PHASE_SUCCEEDED=2,SERVICE_AUDIT_PHASE_DENIED=3,SERVICE_AUDIT_PHASE_CANCELLED=4,SERVICE_AUDIT_PHASE_FAILED=5,SERVICE_AUDIT_PHASE_OUTCOME_UNCERTAIN=6"),
            ("ServiceIngressKindV1", "SERVICE_INGRESS_KIND_UNSPECIFIED=0,SERVICE_INGRESS_KIND_GRPC=1,SERVICE_INGRESS_KIND_MCP_HTTP=2,SERVICE_INGRESS_KIND_IN_PROCESS_TEST_COMPARISON=3"),
            ("ServiceOperationV1", "SERVICE_OPERATION_UNSPECIFIED=0,SERVICE_OPERATION_VALIDATE_CONTRACT=1,SERVICE_OPERATION_EXPLAIN_COMMAND=2,SERVICE_OPERATION_DEPLOY_CONTRACT=3,SERVICE_OPERATION_GET_ACTIVE_CONTRACT=4,SERVICE_OPERATION_GET_CONTRACT_VERSION=5,SERVICE_OPERATION_EXECUTE_COMMAND=6,SERVICE_OPERATION_RESOLVE_COMMAND_OUTCOME=7,SERVICE_OPERATION_GET_ENTITY=8,SERVICE_OPERATION_SCAN_INDEX=9,SERVICE_OPERATION_QUERY_PROJECTION=10,SERVICE_OPERATION_GET_PROJECTION_STATUS=11,SERVICE_OPERATION_GET_COMMIT=12,SERVICE_OPERATION_SCAN_COMMITS=13,SERVICE_OPERATION_SUBSCRIBE_TO_COMMITS=14,SERVICE_OPERATION_TRACE_PROVENANCE=15,SERVICE_OPERATION_GET_HEALTH=16,SERVICE_OPERATION_GET_STATISTICS=17,SERVICE_OPERATION_CREATE_CAPABILITY=18,SERVICE_OPERATION_REVOKE_CAPABILITY=19,SERVICE_OPERATION_LIST_PENDING_OUTBOX_DELIVERIES=20,SERVICE_OPERATION_DISCOVER_COMMAND_TOOLS=21,SERVICE_OPERATION_DISCOVER_RESOURCES=22,SERVICE_OPERATION_DESCRIBE_CONTRACT=23,SERVICE_OPERATION_CHECK_QUERY=24,SERVICE_OPERATION_EXPLAIN_QUERY=25,SERVICE_OPERATION_EXECUTE_QUERY=26,SERVICE_OPERATION_DEPLOY_QUERY_MODULE=27"),
            ("StoredCleanCloseLifecycleStateV1", "STORED_CLEAN_CLOSE_LIFECYCLE_STATE_V1_UNSPECIFIED=0,STORED_CLEAN_CLOSE_LIFECYCLE_STATE_V1_DIRTY=1,STORED_CLEAN_CLOSE_LIFECYCLE_STATE_V1_CLEAN=2"),
            ("StoredCommandAuditMemberV1", "STORED_COMMAND_AUDIT_MEMBER_UNSPECIFIED=0,STORED_COMMAND_AUDIT_MEMBER_STARTED=1,STORED_COMMAND_AUDIT_MEMBER_TERMINAL=2"),
            ("StoredVectorProjectionLifecycleV1", "STORED_VECTOR_PROJECTION_LIFECYCLE_V1_UNSPECIFIED=0,STORED_VECTOR_PROJECTION_LIFECYCLE_V1_BUILDING=1,STORED_VECTOR_PROJECTION_LIFECYCLE_V1_READY=2,STORED_VECTOR_PROJECTION_LIFECYCLE_V1_REBUILD_REQUIRED=3,STORED_VECTOR_PROJECTION_LIFECYCLE_V1_REBUILDING=4,STORED_VECTOR_PROJECTION_LIFECYCLE_V1_INVALID=5"),
            ("StoredVectorProjectionRebuildReasonV1", "STORED_VECTOR_PROJECTION_REBUILD_REASON_V1_UNSPECIFIED=0,STORED_VECTOR_PROJECTION_REBUILD_REASON_V1_REPLAY_AGE=1,STORED_VECTOR_PROJECTION_REBUILD_REASON_V1_REPLAY_BYTES=2,STORED_VECTOR_PROJECTION_REBUILD_REASON_V1_REPLAY_BACKLOG=3,STORED_VECTOR_PROJECTION_REBUILD_REASON_V1_DEFINITION_CHANGED=4"),
        ]
        .into_iter()
        .map(|(name, values)| (name.to_owned(), values.to_owned()))
        .collect::<Vec<_>>()
    );
}

#[test]
fn critical_cross_package_field_numbers_are_frozen() {
    let descriptors = descriptors();
    let messages = message_map(&descriptors);
    let fields = |message: &str| {
        messages[&format!("riffdb.storage.v1.{message}")]
            .field
            .iter()
            .map(|field| (field.name(), field.number()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        fields("StoredDurableEventV1"),
        vec![
            ("event_id", 1),
            ("event_type_id", 2),
            ("canonical_payload", 3),
            ("event_hash", 4),
        ]
    );
    assert_eq!(
        fields("StoredReadDependenciesV1"),
        vec![("dependencies", 1)]
    );
    assert_eq!(
        fields("CapabilityGrantV1"),
        vec![
            ("tenant_scope", 1),
            ("partition_scope", 2),
            ("permissions", 3),
            ("field_visibility", 4),
            ("max_scan_rows", 5),
            ("approval_required", 6),
        ]
    );
    assert_eq!(fields("CapabilityPermissionsV1"), vec![("values", 1)]);
    assert_eq!(
        fields("CapabilityRecordV2"),
        vec![("base", 1), ("migration", 2)],
        "ADR-0089 froze CapabilityRecordV2 as migration-only; later authority must use a successor record"
    );
    assert_eq!(
        schema_hash_hex(
            readable_record_schema("riffdb.storage.v1.CapabilityRecordV2")
                .expect("frozen capability V2 schema"),
        ),
        "2c272df752f4b06b213c507cfbc3cf8dd8d2aea86243d5691a1d1c159c1cacc1",
        "the original ADR-0089 V2 schema hash is a source literal, not a regenerated expectation"
    );
    assert_eq!(
        fields("CapabilityRecordV3"),
        vec![("base", 1), ("migration", 2), ("installation", 3)]
    );
    assert_eq!(
        fields("CapabilityRowPolicyBindingV1"),
        vec![
            ("contract_lineage", 1),
            ("policy_name", 2),
            ("entity_type_id", 3),
            ("operations", 4),
        ]
    );
    assert_eq!(
        fields("CapabilityRowPolicyGrantExtensionV1"),
        vec![
            ("application_role_hash", 1),
            ("canonical_principal_facts", 2),
            ("policies", 3),
        ]
    );
    assert_eq!(
        fields("CapabilityRecordV4"),
        vec![
            ("base", 1),
            ("migration", 2),
            ("installation", 3),
            ("row_policy", 4),
        ]
    );
    assert_eq!(
        fields("CapabilityApplicationExportGrantV1"),
        vec![
            ("contract_lineage", 1),
            ("scope", 2),
            ("entities", 3),
            ("events", 4),
            ("provenance", 5),
            ("public_audit", 6),
        ]
    );
    assert_eq!(
        fields("CapabilityExportGrantExtensionV1"),
        vec![("applications", 1)]
    );
    assert_eq!(
        fields("CapabilityRecordV5"),
        vec![
            ("base", 1),
            ("migration", 2),
            ("installation", 3),
            ("row_policy", 4),
            ("export", 5),
        ]
    );
    assert_eq!(
        fields("StoredOutcomeV1"),
        vec![
            ("identity", 1),
            ("commit_sequence", 2),
            ("admission_request_id", 3),
            ("plan", 4),
            ("canonical_input_hash", 5),
            ("actor", 6),
            ("logical_time", 7),
            ("partition_hash", 8),
            ("conflict_hashes", 9),
            ("declared_outcome", 10),
            ("admitted_claims", 11),
            ("provenance_id", 12),
            ("durability_mode", 13),
            ("partition_key", 14),
        ]
    );
    assert_eq!(fields("StoredCommitRecordV1")[8], ("read_dependencies", 9));
}

#[test]
fn canonical_wire_validation_rejects_alternate_encodings() {
    let schema = current_record_schema("riffdb.storage.v1.StoredStorageFormatVersionV1")
        .expect("registered metadata record");
    let canonical = StoredStorageFormatVersionV1 {
        storage_format_version: 1,
    }
    .encode_to_vec();
    assert!(encode(schema, &canonical).is_ok());

    for (alternate, expected) in [
        (vec![0x08, 0x81, 0x00], PayloadValidationError::NonCanonical),
        (
            vec![0x08, 0x01, 0x08, 0x01],
            PayloadValidationError::NonCanonical,
        ),
        (vec![0x0a, 0x01, 0x01], PayloadValidationError::Malformed),
        (
            vec![0x08, 0x01, 0xf8, 0x07, 0x00],
            PayloadValidationError::NonCanonical,
        ),
    ] {
        assert_eq!(
            encode(schema, &alternate),
            Err(EnvelopeError::InvalidPayload(expected)),
            "{alternate:?}"
        );
    }
}

#[test]
fn durable_preflight_distinguishes_malformed_widths_from_resource_limits() {
    let capability_lookup = current_record_schema("riffdb.storage.v1.CapabilityTokenLookupV1")
        .expect("capability lookup schema");
    let short_capability_id = nested_field(1, &[0x11; 15]);
    assert_eq!(
        encode(capability_lookup, &short_capability_id),
        Err(EnvelopeError::InvalidPayload(
            PayloadValidationError::Malformed
        ))
    );

    let bundle = current_record_schema("riffdb.storage.v1.StoredContractBundleV1")
        .expect("contract bundle schema");
    assert_eq!(
        encode(bundle, &[0x22, 0x00]),
        Err(EnvelopeError::InvalidPayload(
            PayloadValidationError::Malformed
        ))
    );
}

fn append_varint(output: &mut Vec<u8>, mut value: usize) {
    loop {
        let mut byte = u8::try_from(value & 0x7f).expect("seven bits fit");
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn nested_field(number: u8, payload: &[u8]) -> Vec<u8> {
    let mut output = vec![(number << 3) | 2];
    append_varint(&mut output, payload.len());
    output.extend_from_slice(payload);
    output
}

#[test]
fn durable_preflight_bounds_nested_collections_before_prost_allocation() {
    let capability =
        current_record_schema("riffdb.storage.v1.CapabilityRecordV1").expect("capability schema");
    let mut grant = Vec::with_capacity((65_535 + 1) * 2);
    for _ in 0..=65_535 {
        grant.extend_from_slice(&[0x22, 0x00]);
    }
    assert_eq!(
        encode(capability, &nested_field(13, &grant)),
        Err(EnvelopeError::InvalidPayload(
            PayloadValidationError::LimitExceeded
        ))
    );

    let projection = current_record_schema("riffdb.storage.v1.StoredProjectionStateV1")
        .expect("projection state schema");
    let repeated_empty_groups = [0x1a, 0x00].repeat(1_025);
    assert_eq!(
        encode(projection, &repeated_empty_groups),
        Err(EnvelopeError::InvalidPayload(
            PayloadValidationError::LimitExceeded
        ))
    );

    let bundle =
        current_record_schema("riffdb.storage.v1.StoredContractBundleV1").expect("bundle schema");
    assert_eq!(
        encode(bundle, &nested_field(1, &[b'x'; 257])),
        Err(EnvelopeError::InvalidPayload(
            PayloadValidationError::LimitExceeded
        ))
    );
}

#[test]
fn durable_preflight_preserves_noncanonical_error_classification() {
    let bundle =
        current_record_schema("riffdb.storage.v1.StoredContractBundleV1").expect("bundle schema");
    let duplicate_lineage = [nested_field(1, b"a"), nested_field(1, b"b")].concat();
    assert_eq!(
        encode(bundle, &duplicate_lineage),
        Err(EnvelopeError::InvalidPayload(
            PayloadValidationError::NonCanonical
        ))
    );

    let capability =
        current_record_schema("riffdb.storage.v1.CapabilityRecordV1").expect("capability schema");
    let split_packed_approval_required = nested_field(13, &[0x32, 0x01, 0x01, 0x32, 0x01, 0x02]);
    assert_eq!(
        encode(capability, &split_packed_approval_required),
        Err(EnvelopeError::InvalidPayload(
            PayloadValidationError::NonCanonical
        ))
    );
}

#[test]
fn exact_fifteen_mibibyte_bundle_content_fits_the_structural_ceiling() {
    let schema = current_record_schema("riffdb.storage.v1.StoredContractBundleV1")
        .expect("registered bundle record");
    let payload = StoredContractBundleV1 {
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        contract_bundle_hash: vec![0x22; 32],
        canonical_bundle: vec![0x33; 15 * 1024 * 1024],
    }
    .encode_to_vec();
    assert!(payload.len() > 15 * 1024 * 1024);
    assert!(payload.len() < MAX_STORED_ENVELOPE_BYTES);

    let envelope = encode(schema, &payload).expect("15 MiB semantic content must fit");
    let decoded = current_record_registry()
        .decode(&envelope)
        .expect("canonical large bundle wire form must decode");
    assert_eq!(decoded.record_type(), schema.record_type());
    assert_eq!(decoded.payload(), payload);
}

/// Frozen schema hashes for every durable readable record that existed at
/// branch base `a23f42a` (pre-Package-F). Source literals — never read from
/// regenerable fixtures at test time — so an additive own-file record cannot
/// silently rotate any prior descriptor hash (C1 / R1).
#[test]
fn legacy_durable_schema_hashes_are_frozen_source_literals() {
    // Pasted once from `git show a23f42a:fixtures/proto/durable-readable-registry.txt`.
    // Includes both CapabilityRecordV1 dual-hash entries present at that revision.
    const FROZEN_AT_A23F42A: &[(&str, &str)] = &[
        (
            "riffdb.storage.v1.StoredStorageFormatVersionV1",
            "07371c0b9eba9bfcad3118b345064b3d33a20a13641c6bde017e5c99dc6020ab",
        ),
        (
            "riffdb.storage.v1.StoredDatabaseIdentityV1",
            "de6f4f35fa52ca7664a85d9ffaa0041c506f3c5eb22632ba0ecd757dc6df1dea",
        ),
        (
            "riffdb.storage.v1.StoredApplicationSequenceAllocatorV1",
            "ff01f9e77e1d0ae47de12badcd541131a58fc6926b27543c681f0151f49aa24a",
        ),
        (
            "riffdb.storage.v1.StoredAdministrationSequenceAllocatorV1",
            "63cf6435a08786b0fae6e5eef2fde7c1e13395309e1e746a66e2c8b61c49e078",
        ),
        (
            "riffdb.storage.v1.StoredContractBundleV1",
            "1a7b3fc9926f3142189cd79e87b952fccbaf66be89778954e5549b1c4f5dfcf3",
        ),
        (
            "riffdb.storage.v1.ActiveCatalogPointerV1",
            "811264c2f4ab3c208bd958eab9cd5369d699da3c189ad3c2fa558c298ba381a0",
        ),
        (
            "riffdb.storage.v1.StoredCatalogAdministrationV1",
            "c3e8dd62161f441e6318bcd141c3541b21a7b83c2c50f9d19b0b190893ca7b13",
        ),
        (
            "riffdb.storage.v1.StoredEntityRecordV1",
            "67eb5bbd2b789438f7d74861cb97a36700a69926a9e2d848508d019a18d4a213",
        ),
        (
            "riffdb.storage.v1.StoredIndexEntryV1",
            "756e10e48e1ba1d8c604d4937db0a9f2e1e8ba9ee2e4d3b0ac7d495e8e626fc8",
        ),
        (
            "riffdb.storage.v1.StoredIndexEpochV1",
            "48237ae4cd23465b4405eab943b7d61cb45651f68c315b18080871f17cab70ae",
        ),
        (
            "riffdb.storage.v1.StoredPendingAdmissionV1",
            "21b56b7c63eac315cee9ac38a3797a732c645f93e0067ee8e846ea112af029b1",
        ),
        (
            "riffdb.storage.v1.StoredExecutionFailedV1",
            "886673ce516e8b3c16d48b0b831ba59f6fca91e4601ba4f2b64eca3021fa5207",
        ),
        (
            "riffdb.storage.v1.StoredOutcomeV1",
            "08c0d3dbeb5194a5be3ea044cde426880c919ae4b1adc2b7a887b597a8933c61",
        ),
        (
            "riffdb.storage.v1.StoredDurableEventV1",
            "49fd5c15b3489ed21d312a2f16446f18039f0b4b643e817998cc32911c1745bf",
        ),
        (
            "riffdb.storage.v1.StoredOutboxIntentV1",
            "8416e1497f7a47184a80b43c3949f9d02ecc416c3e269425d1fe5c4323eb09c5",
        ),
        (
            "riffdb.storage.v1.StoredProvenanceRecordV1",
            "92e47c16db12bec0c1de5f081608f9a0d510ab32eb2cd098284d2e1b92bbbaef",
        ),
        (
            "riffdb.storage.v1.StoredCommitRecordV1",
            "7c99c855bf3f0e6c66d3e47d390a6e1a6aa61474a414a666df3ffa0339bb6302",
        ),
        (
            "riffdb.storage.v1.CapabilityRecordV1",
            "dee2398ebbc71824471fe5a5f96fcbebdde09e511fe6c11531c2b30210f9e6bf",
        ),
        (
            "riffdb.storage.v1.CapabilityTokenLookupV1",
            "e00696f3d2c110c5b2b99685e7987f7204d2f9576f8b41c5fcd5a894c746bc86",
        ),
        (
            "riffdb.storage.v1.CapabilityBootstrapMarkerV1",
            "1204e270169688244bc489071db70c7d2481111a970d5a9da420ad7b9334e4ea",
        ),
        (
            "riffdb.storage.v1.CapabilityAdministrationAuditV1",
            "28d4a9d5f63eb9f5bacb2042af50d47ea39532bc2ef60c3dc52a450cc7b49a46",
        ),
        (
            "riffdb.storage.v1.ServiceAuditRecordV1",
            "9dde7509a82b5a74dd99505a8499134dca982aecc0c4fc96d1c156c98109c6bf",
        ),
        (
            "riffdb.storage.v1.StoredOutboxStatusV1",
            "264d9f5ec757041a342ccd93e48927cd0fab8e8e886ad7d73b9e20e731ecc900",
        ),
        (
            "riffdb.storage.v1.StoredProjectionStateV1",
            "3897440250ec0b068d332c5f3f7604ec02c0ddc3060995a5b0ad6156cc9a46b5",
        ),
        (
            "riffdb.storage.v1.StoredProjectionApplyV1",
            "484a6446b348ccb25ccd14a59a722d7649f7570180cdde7aee41d40b1d6259f6",
        ),
        (
            "riffdb.storage.v1.StoredProjectionControlV1",
            "3f5eadce258bb61d1148434f61958fc0df6429a4456a44e9db8b64d9cbf8779d",
        ),
        (
            "riffdb.storage.v1.StoredQueryModuleV1",
            "f16ce44c3c7d1380992f89b7ac87fba29abd7164f177ffd304bbb57ecb7cabc8",
        ),
        (
            "riffdb.storage.v1.ActiveQueryModulePointerV1",
            "18391395db2fb6d83127c78129a4f152c8a94a7f84c0673926827db3b2e6a934",
        ),
        (
            "riffdb.storage.v1.StoredQueryModuleAdministrationV1",
            "29138b6c43f55b4904bca27c2abdfab1bedc6491eeaae63bcc7c451ee8c711e4",
        ),
        (
            "riffdb.storage.v1.StoredIndexEntryV2",
            "1be795682748d69f9da74f2d9697dd9a041b7c5e642f757fe4daf902e78368f8",
        ),
        (
            "riffdb.storage.v1.StoredRecordRegistryV2",
            "0e1a29c18da0131e8231eb5426a120345397de4a93afb4079601f51f82a70d7e",
        ),
        (
            "riffdb.storage.v1.StoredCommitRecordV2",
            "a518232c65098a9fcd0b2a6e62fb1dcfec5962d431e2ec07a8d2a680ebb924b3",
        ),
        (
            "riffdb.storage.v1.StoredOutboxIntentV2",
            "4c66e68d3c86f0cb6efdf88625852e86aeff9db6c864569669bc34ccbda6a80c",
        ),
        (
            "riffdb.storage.v1.StoredIndexGenerationV2",
            "b0bed2d75281f5f1c1882df7d503b6e1c045e1cc69615c00690ce01841037e14",
        ),
        (
            "riffdb.storage.v1.CapabilityRecordV1",
            "cb42c4ebbce8280123f8b34d4dcde74ca9483406847531f34d5fb3f18d40b342",
        ),
    ];

    let live: BTreeSet<(String, String)> = READABLE_RECORD_SCHEMAS
        .iter()
        .map(|schema| (schema.record_type().to_owned(), schema_hash_hex(schema)))
        .collect();

    for (record_type, expected_hex) in FROZEN_AT_A23F42A {
        assert!(
            live.contains(&((*record_type).to_owned(), (*expected_hex).to_owned())),
            "{record_type} schema-hash={expected_hex} rotated or dropped — additive durable records must use own-file protos"
        );
    }

    // New history incarnation must not live in metadata.proto.
    let fixture_line = READABLE_REGISTRY_FIXTURE
        .lines()
        .find(|line| line.contains("StoredHistoryIncarnationV1"))
        .expect("history incarnation fixture line");
    assert!(
        fixture_line.contains("source=history_incarnation_v1.proto"),
        "history incarnation must use own-file source, got: {fixture_line}"
    );
}

#[test]
// req: REP-006, STO-012
fn replication_administration_nested_origin_and_closed_actions_are_exact() {
    let descriptors = descriptors();
    let messages = message_map(&descriptors);
    let record = messages["riffdb.storage.v1.StoredReplicationAdministrationV1"];
    let actions = &record.enum_type[0];
    assert_eq!(actions.name(), "Action");
    assert_eq!(
        actions
            .value
            .iter()
            .map(|v| (v.name(), v.number()))
            .collect::<Vec<_>>(),
        [
            ("ACTION_UNSPECIFIED", 0),
            ("ACTION_REGISTER_FOLLOWER", 1),
            ("ACTION_RETIRE_FOLLOWER", 2),
            ("ACTION_EXPIRE_FOLLOWER", 3)
        ]
    );
    assert_eq!(
        record
            .nested_type
            .iter()
            .map(|v| v.name())
            .collect::<Vec<_>>(),
        ["Explicit", "ConfiguredExpiry"]
    );
    let explicit = &record.nested_type[0];
    assert_eq!(
        explicit
            .field
            .iter()
            .map(|f| (f.name(), f.number(), f.proto3_optional()))
            .collect::<Vec<_>>(),
        [
            ("request_id", 1, false),
            ("principal", 2, false),
            ("approval_id", 3, true)
        ]
    );
    let expiry = &record.nested_type[1];
    assert_eq!(
        expiry
            .field
            .iter()
            .map(|f| (f.name(), f.number()))
            .collect::<Vec<_>>(),
        [("registration_administration_sequence", 1)]
    );
}
