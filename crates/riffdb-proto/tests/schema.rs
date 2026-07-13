//! Descriptor-level compatibility assertions for the WP-020 schema baseline.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use prost_types::{
    DescriptorProto, EnumDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::Type,
};
use riffdb_proto::PRODUCTION_FILE_DESCRIPTOR_SET;

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
        BTreeSet::from(["riffdb.storage.v1", "riffdb.v1"])
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
                ("incident_id", 7),
            ],
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
    assert!(public_error.field[6].proto3_optional());
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

    let messages = message_map(&descriptors);
    let response = messages["riffdb.v1.ExecuteCommandResponse"];
    assert_eq!(
        enum_values(&response.enum_type[0]),
        vec![
            ("COMPLETION_STATUS_UNSPECIFIED", 0),
            ("COMMITTED", 1),
            ("REPLAYED", 2),
        ]
    );
}

#[test]
fn service_inventory_and_phase_zero_shells_are_exact() {
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
        5
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
    assert_eq!(methods.len(), 16);
    assert_eq!(
        methods
            .iter()
            .map(|method| method.0.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "AdminService",
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
    let shells = [
        "CommitNotification",
        "CreateCapabilityRequest",
        "CreateCapabilityResponse",
        "DeployContractRequest",
        "DeployContractResponse",
        "ExplainCommandRequest",
        "ExplainCommandResponse",
        "GetActiveContractRequest",
        "GetActiveContractResponse",
        "GetCommitRequest",
        "GetCommitResponse",
        "GetEntityRequest",
        "GetEntityResponse",
        "GetOutcomeRequest",
        "GetOutcomeResponse",
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
        "ValidateContractRequest",
        "ValidateContractResponse",
    ];
    assert!(
        shells
            .iter()
            .all(|name| messages[&format!("riffdb.v1.{name}")].field.is_empty())
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
