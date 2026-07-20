//! Descriptor and structural-wire compatibility tests for ADR-0022.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorSet, field_descriptor_proto::Type};
use riffdb_proto::STORAGE_FILE_DESCRIPTOR_SET;
use riffdb_proto::durable::{
    CURRENT_RECORD_SCHEMA_COUNT, CURRENT_RECORD_SCHEMAS, current_record_registry,
    current_record_schema,
};
use riffdb_proto::envelope::{
    EnvelopeError, MAX_STORED_ENVELOPE_BYTES, PayloadValidationError, encode,
};
use riffdb_proto::storage::v1::{
    StoredContractBundleV1, StoredEnvelope, StoredStorageFormatVersionV1,
};
use riffdb_types::hash_schema;

const RECORDS: &[(&str, &str)] = &[
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

const DURABLE_WIRE_VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-wire-vectors.txt"
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
                "riffdb/storage/v1/catalog.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
            ),
            ("riffdb/storage/v1/common.proto".to_owned(), vec![]),
            ("riffdb/storage/v1/envelope.proto".to_owned(), vec![]),
            (
                "riffdb/storage/v1/metadata.proto".to_owned(),
                vec!["riffdb/storage/v1/common.proto"],
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
        75,
        "74 ADR-0022 messages plus the unchanged StoredEnvelope"
    );
    assert_eq!(
        descriptors
            .file
            .iter()
            .map(|file| file.enum_type.len())
            .sum::<usize>(),
        12
    );
    assert!(
        descriptors
            .file
            .iter()
            .flat_map(|file| &file.message_type)
            .all(|message| message.reserved_name.is_empty() && message.reserved_range.is_empty())
    );
    assert!(
        descriptors
            .file
            .iter()
            .flat_map(|file| &file.enum_type)
            .all(|enumeration| enumeration.reserved_name.is_empty()
                && enumeration.reserved_range.is_empty())
    );
}

#[test]
fn closed_registry_order_and_schema_hashes_are_exactly_derived() {
    assert_eq!(CURRENT_RECORD_SCHEMA_COUNT, 26);
    assert_eq!(CURRENT_RECORD_SCHEMAS.len(), RECORDS.len());
    let descriptors = descriptors();
    let expected_names = RECORDS
        .iter()
        .map(|(name, _)| format!("riffdb.storage.v1.{name}"))
        .collect::<Vec<_>>();
    assert_eq!(
        CURRENT_RECORD_SCHEMAS
            .iter()
            .map(|schema| schema.record_type())
            .collect::<Vec<_>>(),
        expected_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        CURRENT_RECORD_SCHEMAS
            .iter()
            .map(|schema| schema.schema_hash())
            .collect::<BTreeSet<_>>()
            .len(),
        CURRENT_RECORD_SCHEMA_COUNT
    );

    for ((record_name, source), schema) in RECORDS.iter().zip(&CURRENT_RECORD_SCHEMAS) {
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
        assert!(current_record_schema(&record_type).is_some());
    }
    assert_eq!(
        CURRENT_RECORD_SCHEMAS
            .iter()
            .map(|schema| schema.max_payload_bytes())
            .collect::<BTreeSet<_>>()
            .len(),
        6,
        "four semantic classes plus three FQN-specific absolute maxima"
    );

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
            current_record_schema(unregistered).is_none(),
            "{unregistered}"
        );
    }
}

#[test]
fn every_semantic_golden_payload_and_envelope_is_canonical() {
    let mut lines = DURABLE_WIRE_VECTORS.lines();
    assert_eq!(lines.next(), Some("riffdb-durable-wire-vectors-v1"));
    assert_eq!(lines.next(), Some("records\t26"));
    let vectors = lines.collect::<Vec<_>>();
    assert_eq!(vectors.len(), CURRENT_RECORD_SCHEMA_COUNT);

    let registry = current_record_registry();
    for (line, schema) in vectors.iter().zip(&CURRENT_RECORD_SCHEMAS) {
        let columns = line.split('\t').collect::<Vec<_>>();
        assert_eq!(columns.len(), 3, "one FQN, payload, and envelope per line");
        assert_eq!(columns[0], schema.record_type());

        let payload = decode_lower_hex(columns[1]);
        let envelope = decode_lower_hex(columns[2]);
        assert_eq!(
            encode(schema, &payload).expect("semantic golden payload is canonical"),
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
        "CapabilityPermissionV1.stable_id",
        "OutboxDeadLetterV1.last_safe_error",
        "OutboxRetryMetadataV1.last_safe_error",
        "ProjectionFailureV1.at_sequence",
        "ServiceAuditRecordV1.approval_id",
        "StoredAdmittedProvenanceClaimsV1.approval_id",
        "StoredAdmittedProvenanceClaimsV1.reason",
        "StoredAdmittedProvenanceClaimsV1.source_commit",
        "StoredAdmittedProvenanceClaimsV1.source_repository",
        "StoredCatalogAdministrationV1.approval_id",
        "StoredProjectionControlV1.published_apply_mode",
    ]
    .into_iter()
    .map(|suffix| format!("riffdb.storage.v1.{suffix}"))
    .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);

    for (message, field) in [
        ("StoredCatalogAdministrationV1", "previous_active"),
        ("CapabilityAdministrationAuditV1", "initiator"),
        ("ServiceAuditRecordV1", "principal"),
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
                "riffdb.storage.v1.StoredAdministrationSequenceAllocatorV1.state".to_owned(),
                vec![("next_administration_sequence", 1), ("exhausted", 2)],
            ),
            (
                "riffdb.storage.v1.StoredApplicationSequenceAllocatorV1.state".to_owned(),
                vec![("next_commit_sequence", 1), ("exhausted", 2)],
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
            ("CapabilityPermissionKindV1", "CAPABILITY_PERMISSION_KIND_UNSPECIFIED=0,CAPABILITY_PERMISSION_KIND_VALIDATE_CONTRACT=1,CAPABILITY_PERMISSION_KIND_READ_CONTRACT=2,CAPABILITY_PERMISSION_KIND_EXPLAIN_COMMAND=3,CAPABILITY_PERMISSION_KIND_DEPLOY_CONTRACT=4,CAPABILITY_PERMISSION_KIND_INVOKE_COMMAND=5,CAPABILITY_PERMISSION_KIND_READ_ENTITY=6,CAPABILITY_PERMISSION_KIND_SCAN_INDEX=7,CAPABILITY_PERMISSION_KIND_QUERY_PROJECTION=8,CAPABILITY_PERMISSION_KIND_READ_PROJECTION_STATUS=9,CAPABILITY_PERMISSION_KIND_READ_COMMIT=10,CAPABILITY_PERMISSION_KIND_SCAN_COMMITS=11,CAPABILITY_PERMISSION_KIND_SUBSCRIBE_COMMITS=12,CAPABILITY_PERMISSION_KIND_READ_PROVENANCE=13,CAPABILITY_PERMISSION_KIND_INSPECT_OUTBOX=14,CAPABILITY_PERMISSION_KIND_READ_HEALTH=15,CAPABILITY_PERMISSION_KIND_READ_STATISTICS=16,CAPABILITY_PERMISSION_KIND_CREATE_CAPABILITY=17,CAPABILITY_PERMISSION_KIND_REVOKE_CAPABILITY=18,CAPABILITY_PERMISSION_KIND_ADMINISTER_CAPABILITIES=19"),
            ("DurabilityModeV1", "DURABILITY_MODE_UNSPECIFIED=0,DURABILITY_MODE_SYNC=1,DURABILITY_MODE_GROUP=2,DURABILITY_MODE_MEMORY=3"),
            ("ExecutionFailureCodeV1", "EXECUTION_FAILURE_CODE_UNSPECIFIED=0,EXECUTION_FAILURE_CODE_ARITHMETIC_FAULT=1,EXECUTION_FAILURE_CODE_RESOURCE_LIMIT=2"),
            ("ProjectionFailureCodeV1", "PROJECTION_FAILURE_CODE_UNSPECIFIED=0,PROJECTION_FAILURE_CODE_ARITHMETIC_OVERFLOW=1,PROJECTION_FAILURE_CODE_MALFORMED_DURABLE_EVENT=2,PROJECTION_FAILURE_CODE_MISSING_COMMIT=3,PROJECTION_FAILURE_CODE_PLAN_OR_SCHEMA_UNAVAILABLE=4,PROJECTION_FAILURE_CODE_PROJECTION_STATE_INTEGRITY=5,PROJECTION_FAILURE_CODE_HARD_LIMIT_EXCEEDED=6"),
            ("ProjectionLifecycleV1", "PROJECTION_LIFECYCLE_UNSPECIFIED=0,PROJECTION_LIFECYCLE_BUILDING=1,PROJECTION_LIFECYCLE_CATCHING_UP=2,PROJECTION_LIFECYCLE_READY=3,PROJECTION_LIFECYCLE_DEGRADED=4,PROJECTION_LIFECYCLE_REBUILDING=5,PROJECTION_LIFECYCLE_INVALID=6"),
            ("PublishedApplyModeV1", "PUBLISHED_APPLY_MODE_UNSPECIFIED=0,PUBLISHED_APPLY_MODE_ENABLED=1,PUBLISHED_APPLY_MODE_SUSPENDED=2"),
            ("RevocationReasonCodeV1", "REVOCATION_REASON_CODE_UNSPECIFIED=0,REVOCATION_REASON_CODE_REQUESTED=1,REVOCATION_REASON_CODE_REPLACED=2,REVOCATION_REASON_CODE_SUSPECTED_COMPROMISE=3,REVOCATION_REASON_CODE_POLICY_CHANGE=4"),
            ("ServiceAuditPhaseV1", "SERVICE_AUDIT_PHASE_UNSPECIFIED=0,SERVICE_AUDIT_PHASE_STARTED=1,SERVICE_AUDIT_PHASE_SUCCEEDED=2,SERVICE_AUDIT_PHASE_DENIED=3,SERVICE_AUDIT_PHASE_CANCELLED=4,SERVICE_AUDIT_PHASE_FAILED=5,SERVICE_AUDIT_PHASE_OUTCOME_UNCERTAIN=6"),
            ("ServiceIngressKindV1", "SERVICE_INGRESS_KIND_UNSPECIFIED=0,SERVICE_INGRESS_KIND_GRPC=1,SERVICE_INGRESS_KIND_MCP_HTTP=2,SERVICE_INGRESS_KIND_IN_PROCESS_TEST_COMPARISON=3"),
            ("ServiceOperationV1", "SERVICE_OPERATION_UNSPECIFIED=0,SERVICE_OPERATION_VALIDATE_CONTRACT=1,SERVICE_OPERATION_EXPLAIN_COMMAND=2,SERVICE_OPERATION_DEPLOY_CONTRACT=3,SERVICE_OPERATION_GET_ACTIVE_CONTRACT=4,SERVICE_OPERATION_GET_CONTRACT_VERSION=5,SERVICE_OPERATION_EXECUTE_COMMAND=6,SERVICE_OPERATION_RESOLVE_COMMAND_OUTCOME=7,SERVICE_OPERATION_GET_ENTITY=8,SERVICE_OPERATION_SCAN_INDEX=9,SERVICE_OPERATION_QUERY_PROJECTION=10,SERVICE_OPERATION_GET_PROJECTION_STATUS=11,SERVICE_OPERATION_GET_COMMIT=12,SERVICE_OPERATION_SCAN_COMMITS=13,SERVICE_OPERATION_SUBSCRIBE_TO_COMMITS=14,SERVICE_OPERATION_TRACE_PROVENANCE=15,SERVICE_OPERATION_GET_HEALTH=16,SERVICE_OPERATION_GET_STATISTICS=17,SERVICE_OPERATION_CREATE_CAPABILITY=18,SERVICE_OPERATION_REVOKE_CAPABILITY=19,SERVICE_OPERATION_LIST_PENDING_OUTBOX_DELIVERIES=20,SERVICE_OPERATION_DISCOVER_COMMAND_TOOLS=21,SERVICE_OPERATION_DISCOVER_RESOURCES=22"),
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
