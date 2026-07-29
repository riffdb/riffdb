//! Descriptor-level compatibility assertions for the WP-020 schema baseline.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use prost_types::{
    DescriptorProto, EnumDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::Type,
};
use riffdb_proto::PRODUCTION_FILE_DESCRIPTOR_SET;
use riffdb_types::hash_schema;

const PUBLIC_SCHEMA_HASHES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/public-schema-hashes.txt"
));
const PRE_WP137_PUBLIC_SCHEMA_HASHES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/pre-wp137-public-schema-hashes.txt"
));

fn descriptors() -> FileDescriptorSet {
    FileDescriptorSet::decode(PRODUCTION_FILE_DESCRIPTOR_SET).expect("checked descriptor fixture")
}

fn message_map(descriptors: &FileDescriptorSet) -> BTreeMap<String, &DescriptorProto> {
    let mut messages = BTreeMap::new();
    for file in &descriptors.file {
        collect_messages(file.package(), &file.message_type, &mut messages);
    }
    messages
}

fn collect_messages<'a>(
    prefix: &str,
    source: &'a [DescriptorProto],
    output: &mut BTreeMap<String, &'a DescriptorProto>,
) {
    for message in source {
        let name = format!("{prefix}.{}", message.name());
        assert!(output.insert(name.clone(), message).is_none());
        collect_messages(&name, &message.nested_type, output);
    }
}

fn field_numbers(message: &DescriptorProto) -> Vec<(&str, i32)> {
    message
        .field
        .iter()
        .map(|field| (field.name(), field.number()))
        .collect()
}

fn enum_values(enumeration: &EnumDescriptorProto) -> Vec<(&str, i32)> {
    enumeration
        .value
        .iter()
        .map(|value| (value.name(), value.number()))
        .collect()
}

fn top_level_enum<'a>(
    descriptors: &'a FileDescriptorSet,
    package: &str,
    name: &str,
) -> &'a EnumDescriptorProto {
    descriptors
        .file
        .iter()
        .filter(|file| file.package() == package)
        .flat_map(|file| &file.enum_type)
        .find(|enumeration| enumeration.name() == name)
        .expect("expected enum")
}

#[test]
fn descriptors_are_normalized_and_use_only_accepted_packages() {
    let descriptors = descriptors();
    let names = descriptors
        .file
        .iter()
        .map(FileDescriptorProto::name)
        .collect::<Vec<_>>();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted);
    assert!(names.iter().all(|name| !name.contains('\\')));
    assert!(
        descriptors
            .file
            .iter()
            .all(|file| file.source_code_info.is_none())
    );
    assert_eq!(
        descriptors
            .file
            .iter()
            .map(FileDescriptorProto::package)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["riffdb.app.v1", "riffdb.storage.v1", "riffdb.v1"])
    );
}

#[test]
fn exact_value_execute_error_and_envelope_fields_are_frozen() {
    let descriptors = descriptors();
    let messages = message_map(&descriptors);

    let expected = [
        (
            "riffdb.v1.Value",
            vec![
                ("null_value", 1),
                ("bool_value", 2),
                ("i64_value", 3),
                ("u64_value", 4),
                ("decimal_value", 5),
                ("money_value", 6),
                ("string_value", 7),
                ("bytes_value", 8),
                ("uuid_value", 9),
                ("date_value", 10),
                ("timestamp_value", 11),
                ("enum_value", 12),
                ("list_value", 13),
                ("record_value", 14),
            ],
        ),
        (
            "riffdb.v1.ExecuteCommandRequest",
            vec![
                ("request_id", 1),
                ("command_name", 2),
                ("expected_contract_version", 3),
                ("input", 4),
            ],
        ),
        (
            "riffdb.v1.ExecuteCommandResponse",
            vec![
                ("status", 1),
                ("commit_sequence", 2),
                ("contract_version", 3),
                ("plan_hash", 4),
                ("outcome_type", 5),
                ("outcome", 6),
                ("provenance_uri", 7),
                ("durability_mode", 8),
                ("outcome_uri", 9),
            ],
        ),
        (
            "riffdb.v1.GetOutcomeRequest",
            vec![
                ("request_id", 1),
                ("contract_lineage", 2),
                ("command_name", 3),
                ("idempotency_key", 4),
                ("outcome_uri", 5),
            ],
        ),
        (
            "riffdb.v1.PublicError",
            vec![
                ("kind", 1),
                ("code", 2),
                ("safe_message", 3),
                ("recovery_action", 4),
                ("validation", 5),
                ("contract_mismatch", 6),
                ("execution_failure", 8),
                ("incident_id", 7),
            ],
        ),
        (
            "riffdb.v1.CommandExecutionFailureDetails",
            vec![("code", 1)],
        ),
        (
            "riffdb.storage.v1.StoredEnvelope",
            vec![
                ("storage_format_version", 1),
                ("record_type", 2),
                ("payload", 3),
                ("payload_crc32c", 4),
                ("schema_hash", 5),
            ],
        ),
    ];
    for (name, fields) in expected {
        assert_eq!(field_numbers(messages[name]), fields, "{name}");
    }

    let request = messages["riffdb.v1.ExecuteCommandRequest"];
    assert!(request.field[2].proto3_optional());
    assert_eq!(request.field[2].r#type(), Type::Uint64);
    let public_error = messages["riffdb.v1.PublicError"];
    assert!(public_error.field[7].proto3_optional());
    let execution_failure = public_error
        .field
        .iter()
        .find(|field| field.name() == "execution_failure")
        .expect("execution failure detail field");
    assert_eq!(execution_failure.r#type(), Type::Message);
    assert_eq!(
        execution_failure.type_name(),
        ".riffdb.v1.CommandExecutionFailureDetails"
    );
    for detail_name in ["validation", "contract_mismatch"] {
        let detail = public_error
            .field
            .iter()
            .find(|field| field.name() == detail_name)
            .expect("existing public error detail field");
        assert_eq!(detail.oneof_index, execution_failure.oneof_index);
    }
    let value_field = messages["riffdb.v1.ValueField"];
    assert!(value_field.field[0].proto3_optional());
    let envelope = messages["riffdb.storage.v1.StoredEnvelope"];
    assert_eq!(envelope.field[3].r#type(), Type::Fixed32);
}

#[test]
fn exact_closed_enum_registries_are_frozen() {
    let descriptors = descriptors();
    assert_eq!(
        enum_values(top_level_enum(&descriptors, "riffdb.v1", "PublicErrorKind")),
        vec![
            ("PUBLIC_ERROR_KIND_UNSPECIFIED", 0),
            ("PUBLIC_ERROR_KIND_VALIDATION", 1),
            ("PUBLIC_ERROR_KIND_IDEMPOTENCY_KEY_REUSE", 2),
            ("PUBLIC_ERROR_KIND_AUTHORIZATION_DENIED", 3),
            ("PUBLIC_ERROR_KIND_CONCURRENCY_DEADLINE_EXCEEDED", 4),
            ("PUBLIC_ERROR_KIND_CONTRACT_MISMATCH", 5),
            ("PUBLIC_ERROR_KIND_STORAGE_UNAVAILABLE", 6),
            ("PUBLIC_ERROR_KIND_OUTCOME_UNKNOWN", 7),
            ("PUBLIC_ERROR_KIND_INTERNAL_DEFECT", 8),
            ("PUBLIC_ERROR_KIND_COMMAND_EXECUTION_FAILED", 9),
        ]
    );
    assert_eq!(
        enum_values(top_level_enum(&descriptors, "riffdb.v1", "RecoveryAction")),
        vec![
            ("RECOVERY_ACTION_UNSPECIFIED", 0),
            ("RECOVERY_ACTION_CORRECT_REQUEST", 1),
            ("RECOVERY_ACTION_RETRY", 2),
            ("RECOVERY_ACTION_RESOLVE_WITH_SAME_IDEMPOTENCY_KEY", 3),
            ("RECOVERY_ACTION_OBTAIN_PERMISSION", 4),
            ("RECOVERY_ACTION_REFRESH_CONTRACT", 5),
            ("RECOVERY_ACTION_CONTACT_OPERATOR", 6),
        ]
    );
    assert_eq!(
        enum_values(top_level_enum(&descriptors, "riffdb.v1", "ValidationCode")),
        vec![
            ("VALIDATION_CODE_UNSPECIFIED", 0),
            ("VALIDATION_CODE_MISSING_REQUIRED_VALUE", 1),
            ("VALIDATION_CODE_TYPE_MISMATCH", 2),
            ("VALIDATION_CODE_INVALID_VALUE", 3),
            ("VALIDATION_CODE_OUT_OF_RANGE", 4),
            ("VALIDATION_CODE_TOO_LONG", 5),
            ("VALIDATION_CODE_TOO_MANY_ITEMS", 6),
            ("VALIDATION_CODE_UNKNOWN_FIELD", 7),
            ("VALIDATION_CODE_DUPLICATE_FIELD", 8),
        ]
    );
    assert_eq!(
        enum_values(top_level_enum(
            &descriptors,
            "riffdb.v1",
            "ExecutionFailureCode"
        )),
        vec![
            ("EXECUTION_FAILURE_CODE_UNSPECIFIED", 0),
            ("EXECUTION_FAILURE_CODE_ARITHMETIC_FAULT", 1),
            ("EXECUTION_FAILURE_CODE_RESOURCE_LIMIT", 2),
        ]
    );

    let messages = message_map(&descriptors);
    let response = messages["riffdb.v1.ExecuteCommandResponse"];
    assert_eq!(
        enum_values(&response.enum_type[0]),
        vec![
            ("COMPLETION_STATUS_UNSPECIFIED", 0),
            ("COMMITTED", 1),
            ("REPLAYED", 2),
            ("EXECUTED_READ_ONLY", 3),
        ]
    );
}

#[test]
fn offline_maintenance_surface_is_exact_and_additive() {
    let descriptors = descriptors();
    let messages = message_map(&descriptors);
    for (name, expected) in [
        (
            "riffdb.v1.OfflineMaintenanceOperation",
            vec![
                ("operation_id", 1),
                ("kind", 2),
                ("backup_name", 3),
                ("input_hash", 4),
                ("phase", 5),
                ("failure", 6),
            ],
        ),
        (
            "riffdb.v1.CreateOfflineBackupRequest",
            vec![("request_id", 1), ("operation_id", 2), ("backup_name", 3)],
        ),
        (
            "riffdb.v1.CreateOfflineBackupResponse",
            vec![("disposition", 1), ("operation", 2)],
        ),
        (
            "riffdb.v1.RestoreOfflineBackupRequest",
            vec![
                ("request_id", 1),
                ("operation_id", 2),
                ("backup_name", 3),
                ("replacement_confirmation", 4),
            ],
        ),
        (
            "riffdb.v1.RestoreOfflineBackupResponse",
            vec![("disposition", 1), ("operation", 2)],
        ),
        (
            "riffdb.v1.GetOfflineMaintenanceOperationRequest",
            vec![("request_id", 1), ("operation_id", 2)],
        ),
        (
            "riffdb.v1.GetOfflineMaintenanceOperationResponse",
            vec![("not_found", 1), ("found", 2)],
        ),
    ] {
        assert_eq!(field_numbers(messages[name]), expected, "{name}");
    }
    for (name, expected) in [
        (
            "OfflineMaintenanceOperationKind",
            vec![
                ("OFFLINE_MAINTENANCE_OPERATION_KIND_UNSPECIFIED", 0),
                ("OFFLINE_MAINTENANCE_OPERATION_KIND_CREATE_BACKUP", 1),
                ("OFFLINE_MAINTENANCE_OPERATION_KIND_RESTORE_BACKUP", 2),
            ],
        ),
        (
            "OfflineMaintenanceReplacementConfirmation",
            vec![
                (
                    "OFFLINE_MAINTENANCE_REPLACEMENT_CONFIRMATION_UNSPECIFIED",
                    0,
                ),
                (
                    "OFFLINE_MAINTENANCE_REPLACEMENT_CONFIRMATION_ALLOW_REPLACE_NONEMPTY_TARGET",
                    1,
                ),
            ],
        ),
        (
            "OfflineMaintenanceStartDisposition",
            vec![
                ("OFFLINE_MAINTENANCE_START_DISPOSITION_UNSPECIFIED", 0),
                ("OFFLINE_MAINTENANCE_START_DISPOSITION_ACCEPTED", 1),
                ("OFFLINE_MAINTENANCE_START_DISPOSITION_ALREADY_ACCEPTED", 2),
                ("OFFLINE_MAINTENANCE_START_DISPOSITION_TERMINAL", 3),
            ],
        ),
        (
            "OfflineMaintenancePhase",
            vec![
                ("OFFLINE_MAINTENANCE_PHASE_UNSPECIFIED", 0),
                ("OFFLINE_MAINTENANCE_PHASE_ACCEPTED", 1),
                ("OFFLINE_MAINTENANCE_PHASE_DRAINING", 2),
                ("OFFLINE_MAINTENANCE_PHASE_OFFLINE", 3),
                ("OFFLINE_MAINTENANCE_PHASE_ARTIFACT_PUBLISHED", 4),
                ("OFFLINE_MAINTENANCE_PHASE_VALIDATING", 5),
                ("OFFLINE_MAINTENANCE_PHASE_SUCCEEDED", 6),
                ("OFFLINE_MAINTENANCE_PHASE_FAILED_CLOSED", 7),
            ],
        ),
        (
            "OfflineMaintenanceFailureClass",
            vec![
                ("OFFLINE_MAINTENANCE_FAILURE_CLASS_UNSPECIFIED", 0),
                ("OFFLINE_MAINTENANCE_FAILURE_CLASS_QUIESCENCE_FAILED", 1),
                ("OFFLINE_MAINTENANCE_FAILURE_CLASS_ARTIFACT_UNAVAILABLE", 2),
                ("OFFLINE_MAINTENANCE_FAILURE_CLASS_ARTIFACT_INVALID", 3),
                (
                    "OFFLINE_MAINTENANCE_FAILURE_CLASS_STAGED_AUTHORIZATION_FAILED",
                    4,
                ),
                ("OFFLINE_MAINTENANCE_FAILURE_CLASS_STORAGE_UNAVAILABLE", 5),
                ("OFFLINE_MAINTENANCE_FAILURE_CLASS_VALIDATION_FAILED", 6),
                ("OFFLINE_MAINTENANCE_FAILURE_CLASS_RECEIPT_UNAVAILABLE", 7),
                ("OFFLINE_MAINTENANCE_FAILURE_CLASS_INTERNAL_FAILURE", 8),
            ],
        ),
    ] {
        assert_eq!(
            enum_values(top_level_enum(&descriptors, "riffdb.v1", name)),
            expected,
            "{name}"
        );
    }
}

#[test]
fn service_inventory_and_completed_phase_zero_messages_are_exact() {
    let descriptors = descriptors();
    let services = descriptors
        .file
        .iter()
        .flat_map(|file| file.service.iter().map(|service| service.name()))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        services,
        BTreeSet::from([
            "AdminService",
            "ApplicationQueryService",
            "CommandService",
            "CommitService",
            "ContractService",
            "QueryService",
        ])
    );
    assert_eq!(
        descriptors
            .file
            .iter()
            .map(|file| file.service.len())
            .sum::<usize>(),
        6
    );
    let mut methods = Vec::new();
    for file in &descriptors.file {
        for service in &file.service {
            for method in &service.method {
                methods.push((
                    service.name().to_owned(),
                    method.name().to_owned(),
                    method.input_type().to_owned(),
                    method.output_type().to_owned(),
                    method.client_streaming(),
                    method.server_streaming(),
                ));
            }
        }
    }
    methods.sort();
    assert_eq!(methods.len(), 31);
    let descriptor_order = descriptors
        .file
        .iter()
        .flat_map(|file| &file.service)
        .map(|service| {
            (
                service.name(),
                service
                    .method
                    .iter()
                    .map(|method| method.name())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        descriptor_order["ApplicationQueryService"],
        vec![
            "DescribeContract",
            "CheckQuery",
            "ExplainQuery",
            "ExecuteQuery",
            "DeployQueryModule",
            "GetQueryModule",
        ]
    );
    assert_eq!(
        descriptor_order["ContractService"],
        vec![
            "ValidateContract",
            "ExplainCommand",
            "DeployContract",
            "GetActiveContract",
            "GetContractVersion",
            "DiscoverCommandTools",
            "DiscoverResources",
        ]
    );
    assert_eq!(
        descriptor_order["CommandService"],
        vec!["Execute", "GetOutcome"]
    );
    assert_eq!(
        descriptor_order["QueryService"],
        vec![
            "GetEntity",
            "ScanIndex",
            "QueryProjection",
            "GetProjectionStatus",
        ]
    );
    assert_eq!(
        descriptor_order["CommitService"],
        vec![
            "GetCommit",
            "ScanCommits",
            "SubscribeCommits",
            "TraceProvenance",
        ]
    );
    assert_eq!(
        descriptor_order["AdminService"],
        vec![
            "Health",
            "Stats",
            "CreateCapability",
            "RevokeCapability",
            "ListPendingOutboxDeliveries",
            "CreateOfflineBackup",
            "RestoreOfflineBackup",
            "GetOfflineMaintenanceOperation",
        ]
    );
    assert_eq!(
        methods
            .iter()
            .map(|method| method.0.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "AdminService",
            "ApplicationQueryService",
            "CommandService",
            "CommitService",
            "ContractService",
            "QueryService",
        ])
    );
    assert!(methods.iter().all(|method| !method.4));
    assert_eq!(
        methods
            .iter()
            .filter(|method| method.5)
            .map(|method| (method.0.as_str(), method.1.as_str()))
            .collect::<Vec<_>>(),
        vec![("CommitService", "SubscribeCommits")]
    );

    let messages = message_map(&descriptors);
    let completed = [
        "CommitNotification",
        "CreateCapabilityRequest",
        "CreateCapabilityResponse",
        "DeployContractRequest",
        "DeployContractResponse",
        "ExplainCommandRequest",
        "ExplainCommandResponse",
        "GetActiveContractRequest",
        "GetActiveContractResponse",
        "GetContractVersionRequest",
        "GetContractVersionResponse",
        "DiscoverCommandToolsRequest",
        "DiscoverCommandToolsResponse",
        "DiscoverResourcesRequest",
        "DiscoverResourcesResponse",
        "GetCommitRequest",
        "GetCommitResponse",
        "GetEntityRequest",
        "GetEntityResponse",
        "GetOutcomeRequest",
        "GetOutcomeResponse",
        "GetProjectionStatusRequest",
        "GetProjectionStatusResponse",
        "HealthRequest",
        "HealthResponse",
        "QueryProjectionRequest",
        "QueryProjectionResponse",
        "RevokeCapabilityRequest",
        "RevokeCapabilityResponse",
        "ScanCommitsRequest",
        "ScanCommitsResponse",
        "ScanIndexRequest",
        "ScanIndexResponse",
        "StatsRequest",
        "StatsResponse",
        "SubscribeCommitsRequest",
        "TraceProvenanceRequest",
        "TraceProvenanceResponse",
        "ListPendingOutboxDeliveriesRequest",
        "ListPendingOutboxDeliveriesResponse",
        "OfflineMaintenanceOperation",
        "CreateOfflineBackupRequest",
        "CreateOfflineBackupResponse",
        "RestoreOfflineBackupRequest",
        "RestoreOfflineBackupResponse",
        "GetOfflineMaintenanceOperationRequest",
        "GetOfflineMaintenanceOperationResponse",
        "ValidateContractRequest",
        "ValidateContractResponse",
    ];
    assert!(
        completed
            .iter()
            .all(|name| { !messages[&format!("riffdb.v1.{name}")].field.is_empty() })
    );
    assert_eq!(
        messages
            .keys()
            .filter(|name| name.starts_with("riffdb.v1."))
            .count(),
        158
    );
    assert_eq!(
        messages
            .iter()
            .filter(|(name, message)| name.starts_with("riffdb.v1.") && message.field.is_empty())
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec!["riffdb.v1.Unit"]
    );
}

#[test]
fn dynamic_values_never_use_floating_point_or_struct_fallbacks() {
    let descriptors = descriptors();
    let messages = message_map(&descriptors);
    for message in messages.values() {
        assert!(message.field.iter().all(|field| {
            !matches!(field.r#type(), Type::Double | Type::Float)
                && field.type_name() != ".google.protobuf.Struct"
        }));
    }
}

fn collect_hash_message_inputs(
    prefix: &str,
    messages: &[DescriptorProto],
    output: &mut BTreeMap<(String, String), Vec<u8>>,
) {
    for message in messages {
        let name = format!("{prefix}.{}", message.name());
        output.insert(
            ("message".to_owned(), name.clone()),
            message.encode_to_vec(),
        );
        for field in &message.field {
            output.insert(
                ("field".to_owned(), format!("{name}.{}", field.name())),
                field.encode_to_vec(),
            );
        }
        for oneof in &message.oneof_decl {
            output.insert(
                ("oneof".to_owned(), format!("{name}.{}", oneof.name())),
                oneof.encode_to_vec(),
            );
        }
        for enumeration in &message.enum_type {
            collect_hash_enum_inputs(
                &format!("{name}.{}", enumeration.name()),
                enumeration,
                output,
            );
        }
        collect_hash_message_inputs(&name, &message.nested_type, output);
    }
}

fn collect_hash_enum_inputs(
    name: &str,
    enumeration: &EnumDescriptorProto,
    output: &mut BTreeMap<(String, String), Vec<u8>>,
) {
    output.insert(
        ("enum".to_owned(), name.to_owned()),
        enumeration.encode_to_vec(),
    );
    for value in &enumeration.value {
        output.insert(
            ("enum-value".to_owned(), format!("{name}.{}", value.name())),
            value.encode_to_vec(),
        );
    }
}

fn parse_schema_hash_fixture(source: &str) -> BTreeMap<(String, String), String> {
    let mut lines = source.lines();
    assert_eq!(lines.next(), Some("riffdb-public-schema-hashes-v1"));
    lines
        .map(|line| {
            let mut fields = line.split_whitespace();
            let kind = fields.next().expect("descriptor kind").to_owned();
            let name = fields.next().expect("descriptor name").to_owned();
            let hash = fields.next().expect("descriptor hash").to_owned();
            assert!(fields.next().is_none());
            ((kind, name), hash)
        })
        .collect()
}

#[test]
fn pre_wp137_public_descriptor_surface_is_additively_compatible() {
    let baseline = parse_schema_hash_fixture(PRE_WP137_PUBLIC_SCHEMA_HASHES);
    let current = parse_schema_hash_fixture(PUBLIC_SCHEMA_HASHES);

    for ((kind, name), baseline_hash) in baseline {
        let current_hash = current
            .get(&(kind.clone(), name.clone()))
            .unwrap_or_else(|| panic!("pre-WP-137 {kind} symbol was removed: {name}"));
        match kind.as_str() {
            // These rows isolate every compatibility-sensitive leaf. Their
            // encoded descriptors include field presence/type/number, enum
            // number, and RPC input/output/streaming shape respectively.
            "field" | "enum-value" | "oneof" | "rpc" => {
                assert_eq!(
                    current_hash, &baseline_hash,
                    "pre-WP-137 {kind} changed: {name}"
                );
            }
            // Container hashes may change when an additive child is appended,
            // but every pre-existing container must remain addressable.
            "schema" | "file" | "message" | "enum" | "service" => {}
            _ => panic!("unknown descriptor fixture kind: {kind}"),
        }
    }
}

#[test]
fn every_public_descriptor_element_has_an_exact_schema_hash() {
    let descriptors = descriptors();
    let public = FileDescriptorSet {
        file: descriptors
            .file
            .into_iter()
            .filter(|file| file.package() == "riffdb.v1")
            .collect(),
    };
    let mut expected = BTreeMap::new();
    expected.insert(
        ("schema".to_owned(), "riffdb.v1".to_owned()),
        public.encode_to_vec(),
    );
    for file in &public.file {
        expected.insert(
            ("file".to_owned(), file.name().to_owned()),
            file.encode_to_vec(),
        );
        collect_hash_message_inputs(file.package(), &file.message_type, &mut expected);
        for enumeration in &file.enum_type {
            collect_hash_enum_inputs(
                &format!("{}.{}", file.package(), enumeration.name()),
                enumeration,
                &mut expected,
            );
        }
        for service in &file.service {
            let name = format!("{}.{}", file.package(), service.name());
            expected.insert(
                ("service".to_owned(), name.clone()),
                service.encode_to_vec(),
            );
            for method in &service.method {
                expected.insert(
                    ("rpc".to_owned(), format!("{name}.{}", method.name())),
                    method.encode_to_vec(),
                );
            }
        }
    }

    let mut lines = PUBLIC_SCHEMA_HASHES.lines();
    assert_eq!(lines.next(), Some("riffdb-public-schema-hashes-v1"));
    let mut actual_names = BTreeSet::new();
    for line in lines {
        let mut fields = line.split_whitespace();
        let kind = fields.next().expect("kind");
        let name = fields.next().expect("name");
        let actual_hash = fields.next().expect("hash");
        assert!(fields.next().is_none());
        let key = (kind.to_owned(), name.to_owned());
        let descriptor = expected.get(&key).expect("known descriptor element");
        let mut frame = Vec::new();
        frame.extend_from_slice(kind.as_bytes());
        frame.push(0);
        frame.extend_from_slice(name.as_bytes());
        frame.push(0);
        frame.extend_from_slice(descriptor);
        let expected_hash = hash_schema(&frame)
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(actual_hash, expected_hash);
        assert!(actual_names.insert(key));
    }
    assert_eq!(actual_names, expected.keys().cloned().collect());
}
