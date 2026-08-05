//! Language-neutral request/result fixtures for every accepted public RPC.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use prost_types::{DescriptorProto, EnumDescriptorProto, FileDescriptorSet};
use riffdb_proto::{
    PRODUCTION_FILE_DESCRIPTOR_SET, PublicMessage, PublicWireError, decode_public_message, v1,
};

const VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/public-client-vectors.txt"
));
const PRE_WP137_PUBLIC_SCHEMA_HASHES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/pre-wp137-public-schema-hashes.txt"
));
const REGISTRY_HEADER: &str = "riffdb-public-client-registry-v1";

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16)
                .expect("valid hex")
        })
        .collect()
}

fn decode<M: PublicMessage>(bytes: &[u8]) {
    decode_public_message::<M>(bytes).expect("strict public fixture");
}

#[test]
fn every_client_vector_passes_its_strict_public_boundary() {
    let mut lines = VECTORS.lines();
    assert_eq!(lines.next(), Some("riffdb-public-client-vectors-v1"));
    let mut rpcs = BTreeSet::new();
    let mut request_rpcs = BTreeSet::new();
    let mut visible_rpcs = BTreeSet::new();
    let mut count = 0usize;
    for line in lines {
        if line == REGISTRY_HEADER {
            break;
        }
        let mut fields = line.split_whitespace();
        let rpc = fields.next().expect("RPC");
        let direction = fields.next().expect("direction");
        let _branch = fields.next().expect("branch");
        let message_type = fields.next().expect("message type");
        let bytes = decode_hex(fields.next().expect("hex bytes"));
        assert!(fields.next().is_none());
        rpcs.insert(rpc);
        if direction == "request" {
            request_rpcs.insert(rpc);
        } else {
            visible_rpcs.insert(rpc);
        }
        match message_type {
            "riffdb.v1.ValidateContractRequest" => decode::<v1::ValidateContractRequest>(&bytes),
            "riffdb.v1.ValidateContractResponse" => decode::<v1::ValidateContractResponse>(&bytes),
            "riffdb.v1.ExplainCommandRequest" => decode::<v1::ExplainCommandRequest>(&bytes),
            "riffdb.v1.ExplainCommandResponse" => decode::<v1::ExplainCommandResponse>(&bytes),
            "riffdb.v1.DeployContractRequest" => decode::<v1::DeployContractRequest>(&bytes),
            "riffdb.v1.DeployContractResponse" => decode::<v1::DeployContractResponse>(&bytes),
            "riffdb.v1.GetActiveContractRequest" => decode::<v1::GetActiveContractRequest>(&bytes),
            "riffdb.v1.GetActiveContractResponse" => {
                decode::<v1::GetActiveContractResponse>(&bytes)
            }
            "riffdb.v1.GetContractVersionRequest" => {
                decode::<v1::GetContractVersionRequest>(&bytes)
            }
            "riffdb.v1.GetContractVersionResponse" => {
                decode::<v1::GetContractVersionResponse>(&bytes)
            }
            "riffdb.v1.DiscoverCommandToolsRequest" => {
                decode::<v1::DiscoverCommandToolsRequest>(&bytes)
            }
            "riffdb.v1.DiscoverCommandToolsResponse" => {
                decode::<v1::DiscoverCommandToolsResponse>(&bytes)
            }
            "riffdb.v1.DiscoverResourcesRequest" => decode::<v1::DiscoverResourcesRequest>(&bytes),
            "riffdb.v1.DiscoverResourcesResponse" => {
                decode::<v1::DiscoverResourcesResponse>(&bytes)
            }
            "riffdb.v1.ExecuteCommandRequest" => decode::<v1::ExecuteCommandRequest>(&bytes),
            "riffdb.v1.ExecuteCommandResponse" => decode::<v1::ExecuteCommandResponse>(&bytes),
            "riffdb.v1.GetOutcomeRequest" => decode::<v1::GetOutcomeRequest>(&bytes),
            "riffdb.v1.GetOutcomeResponse" => decode::<v1::GetOutcomeResponse>(&bytes),
            "riffdb.v1.GetEntityRequest" => decode::<v1::GetEntityRequest>(&bytes),
            "riffdb.v1.GetEntityResponse" => decode::<v1::GetEntityResponse>(&bytes),
            "riffdb.v1.ScanIndexRequest" => decode::<v1::ScanIndexRequest>(&bytes),
            "riffdb.v1.ScanIndexResponse" => decode::<v1::ScanIndexResponse>(&bytes),
            "riffdb.v1.QueryProjectionRequest" => decode::<v1::QueryProjectionRequest>(&bytes),
            "riffdb.v1.QueryProjectionResponse" => decode::<v1::QueryProjectionResponse>(&bytes),
            "riffdb.v1.WatchNamedQueryRequest" => decode::<v1::WatchNamedQueryRequest>(&bytes),
            "riffdb.v1.LiveQueryUpdate" => decode::<v1::LiveQueryUpdate>(&bytes),
            "riffdb.v1.GetProjectionStatusRequest" => {
                decode::<v1::GetProjectionStatusRequest>(&bytes)
            }
            "riffdb.v1.GetProjectionStatusResponse" => {
                decode::<v1::GetProjectionStatusResponse>(&bytes)
            }
            "riffdb.v1.GetCommitRequest" => decode::<v1::GetCommitRequest>(&bytes),
            "riffdb.v1.GetCommitResponse" => decode::<v1::GetCommitResponse>(&bytes),
            "riffdb.v1.ScanCommitsRequest" => decode::<v1::ScanCommitsRequest>(&bytes),
            "riffdb.v1.ScanCommitsResponse" => decode::<v1::ScanCommitsResponse>(&bytes),
            "riffdb.v1.SubscribeCommitsRequest" => decode::<v1::SubscribeCommitsRequest>(&bytes),
            "riffdb.v1.CommitNotification" => decode::<v1::CommitNotification>(&bytes),
            "riffdb.v1.TraceProvenanceRequest" => decode::<v1::TraceProvenanceRequest>(&bytes),
            "riffdb.v1.TraceProvenanceResponse" => decode::<v1::TraceProvenanceResponse>(&bytes),
            "riffdb.v1.HealthRequest" => decode::<v1::HealthRequest>(&bytes),
            "riffdb.v1.HealthResponse" => decode::<v1::HealthResponse>(&bytes),
            "riffdb.v1.StatsRequest" => decode::<v1::StatsRequest>(&bytes),
            "riffdb.v1.StatsResponse" => decode::<v1::StatsResponse>(&bytes),
            "riffdb.v1.CreateCapabilityRequest" => decode::<v1::CreateCapabilityRequest>(&bytes),
            "riffdb.v1.CreateCapabilityResponse" => decode::<v1::CreateCapabilityResponse>(&bytes),
            "riffdb.v1.RevokeCapabilityRequest" => decode::<v1::RevokeCapabilityRequest>(&bytes),
            "riffdb.v1.RevokeCapabilityResponse" => decode::<v1::RevokeCapabilityResponse>(&bytes),
            "riffdb.v1.ListPendingOutboxDeliveriesRequest" => {
                decode::<v1::ListPendingOutboxDeliveriesRequest>(&bytes)
            }
            "riffdb.v1.ListPendingOutboxDeliveriesResponse" => {
                decode::<v1::ListPendingOutboxDeliveriesResponse>(&bytes)
            }
            "riffdb.v1.CreateOfflineBackupRequest" => {
                decode::<v1::CreateOfflineBackupRequest>(&bytes)
            }
            "riffdb.v1.CreateOfflineBackupResponse" => {
                decode::<v1::CreateOfflineBackupResponse>(&bytes)
            }
            "riffdb.v1.RestoreOfflineBackupRequest" => {
                decode::<v1::RestoreOfflineBackupRequest>(&bytes)
            }
            "riffdb.v1.RestoreOfflineBackupResponse" => {
                decode::<v1::RestoreOfflineBackupResponse>(&bytes)
            }
            "riffdb.v1.GetOfflineMaintenanceOperationRequest" => {
                decode::<v1::GetOfflineMaintenanceOperationRequest>(&bytes)
            }
            "riffdb.v1.GetOfflineMaintenanceOperationResponse" => {
                decode::<v1::GetOfflineMaintenanceOperationResponse>(&bytes)
            }
            other => panic!("unexpected client fixture type: {other}"),
        }
        count += 1;
    }
    assert_eq!(count, 141);
    assert_eq!(rpcs.len(), 26);
    assert_eq!(request_rpcs, rpcs);
    assert_eq!(visible_rpcs, rpcs);
}

#[derive(Debug)]
struct FixtureVector<'a> {
    message_type: &'a str,
    bytes: Vec<u8>,
}

fn fixture_sections() -> (BTreeMap<String, FixtureVector<'static>>, BTreeSet<String>) {
    let mut lines = VECTORS.lines();
    assert_eq!(lines.next(), Some("riffdb-public-client-vectors-v1"));
    let mut vectors = BTreeMap::new();
    let mut registry = BTreeSet::new();
    let mut in_registry = false;
    for line in lines {
        if line == REGISTRY_HEADER {
            assert!(!in_registry, "duplicate registry header");
            in_registry = true;
            continue;
        }
        if in_registry {
            assert!(registry.insert(line.to_owned()), "duplicate registry row");
            continue;
        }
        let fields = line.split_whitespace().collect::<Vec<_>>();
        assert_eq!(fields.len(), 5);
        let key = format!("{}:{}:{}", fields[0], fields[1], fields[2]);
        assert!(
            vectors
                .insert(
                    key,
                    FixtureVector {
                        message_type: fields[3],
                        bytes: decode_hex(fields[4]),
                    },
                )
                .is_none(),
            "duplicate client-vector key"
        );
    }
    assert!(in_registry, "missing registry section");
    (vectors, registry)
}

fn baseline_symbols() -> BTreeSet<String> {
    let mut lines = PRE_WP137_PUBLIC_SCHEMA_HASHES.lines();
    assert_eq!(lines.next(), Some("riffdb-public-schema-hashes-v1"));
    lines
        .map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            assert_eq!(fields.len(), 3);
            format!("{} {}", fields[0], fields[1])
        })
        .collect()
}

fn collect_messages<'a>(
    prefix: &str,
    messages: &'a [DescriptorProto],
    output: &mut Vec<(String, &'a DescriptorProto)>,
) {
    for message in messages {
        let name = format!("{prefix}.{}", message.name());
        output.push((name.clone(), message));
        collect_messages(&name, &message.nested_type, output);
    }
}

fn collect_enums<'a>(
    prefix: &str,
    enums: &'a [EnumDescriptorProto],
    messages: &'a [DescriptorProto],
    output: &mut Vec<(String, &'a EnumDescriptorProto)>,
) {
    output.extend(
        enums
            .iter()
            .map(|enumeration| (format!("{prefix}.{}", enumeration.name()), enumeration)),
    );
    for message in messages {
        let name = format!("{prefix}.{}", message.name());
        collect_enums(&name, &message.enum_type, &message.nested_type, output);
    }
}

fn descriptor_delta() -> (BTreeSet<String>, BTreeSet<String>) {
    let baseline = baseline_symbols();
    let descriptors = FileDescriptorSet::decode(PRODUCTION_FILE_DESCRIPTOR_SET)
        .expect("checked production descriptor set");
    let public_files = descriptors
        .file
        .iter()
        .filter(|file| file.package() == "riffdb.v1")
        .collect::<Vec<_>>();
    let mut enums = Vec::new();
    for file in &public_files {
        collect_enums(
            file.package(),
            &file.enum_type,
            &file.message_type,
            &mut enums,
        );
    }
    let enum_values = enums
        .into_iter()
        .flat_map(|(name, enumeration)| {
            let baseline = &baseline;
            enumeration.value.iter().filter_map(move |value| {
                let symbol = format!("enum-value {name}.{}", value.name());
                (!baseline.contains(&symbol))
                    .then(|| format!("enum-value {name} {} {}", value.number(), value.name()))
            })
        })
        .collect();

    let mut messages = Vec::new();
    for file in public_files {
        collect_messages(file.package(), &file.message_type, &mut messages);
    }
    let optional_fields = messages
        .into_iter()
        .flat_map(|(message_name, message)| {
            let baseline = &baseline;
            message.field.iter().filter_map(move |field| {
                let field_name = format!("{message_name}.{}", field.name());
                (field.proto3_optional() && !baseline.contains(&format!("field {field_name}")))
                    .then_some(field_name)
            })
        })
        .collect();
    (enum_values, optional_fields)
}

fn expected_enum_values() -> BTreeSet<String> {
    let mut values = [
        (
            "riffdb.v1.CapabilityPermissionKind",
            20,
            "CAPABILITY_PERMISSION_KIND_CHECK_AD_HOC_QUERY",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            21,
            "CAPABILITY_PERMISSION_KIND_EXPLAIN_AD_HOC_QUERY",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            22,
            "CAPABILITY_PERMISSION_KIND_EXECUTE_AD_HOC_QUERY",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            23,
            "CAPABILITY_PERMISSION_KIND_EXPLAIN_NAMED_QUERY",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            24,
            "CAPABILITY_PERMISSION_KIND_EXECUTE_NAMED_QUERY",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            25,
            "CAPABILITY_PERMISSION_KIND_APPLICATION_ROLE_IDENTITY",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            26,
            "CAPABILITY_PERMISSION_KIND_MIGRATE_CONTRACT",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            27,
            "CAPABILITY_PERMISSION_KIND_CONSUME_EVENT_STREAM",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            28,
            "CAPABILITY_PERMISSION_KIND_SEEK_EVENT_STREAM_CONSUMER",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            29,
            "CAPABILITY_PERMISSION_KIND_WATCH_NAMED_QUERY",
        ),
        (
            "riffdb.v1.CapabilityPermissionKind",
            30,
            "CAPABILITY_PERMISSION_KIND_CONSUME_CONTEXTUAL_SUBSCRIPTION",
        ),
        (
            "riffdb.v1.ContextualQueryCardinality",
            0,
            "CONTEXTUAL_QUERY_CARDINALITY_UNSPECIFIED",
        ),
        (
            "riffdb.v1.ContextualQueryCardinality",
            1,
            "CONTEXTUAL_QUERY_CARDINALITY_ONE",
        ),
        (
            "riffdb.v1.ContextualQueryCardinality",
            2,
            "CONTEXTUAL_QUERY_CARDINALITY_MAYBE",
        ),
        (
            "riffdb.v1.ContextualQueryCardinality",
            3,
            "CONTEXTUAL_QUERY_CARDINALITY_MANY",
        ),
        (
            "riffdb.v1.ContractMigrationApplyConfirmation",
            0,
            "CONTRACT_MIGRATION_APPLY_CONFIRMATION_UNSPECIFIED",
        ),
        (
            "riffdb.v1.ContractMigrationApplyConfirmation",
            1,
            "CONTRACT_MIGRATION_APPLY_CONFIRMATION_ALLOW_APPLY_CONTRACT_MIGRATION",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            0,
            "CONTRACT_MIGRATION_FAILURE_CLASS_UNSPECIFIED",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            1,
            "CONTRACT_MIGRATION_FAILURE_CLASS_ARTIFACT_MISMATCH",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            2,
            "CONTRACT_MIGRATION_FAILURE_CLASS_INVALID_PREDECESSOR",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            3,
            "CONTRACT_MIGRATION_FAILURE_CLASS_PENDING_ADMISSION",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            4,
            "CONTRACT_MIGRATION_FAILURE_CLASS_CAPACITY_EXHAUSTED",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            5,
            "CONTRACT_MIGRATION_FAILURE_CLASS_DISK_UNAVAILABLE",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            6,
            "CONTRACT_MIGRATION_FAILURE_CLASS_STAGE_CORRUPT",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            7,
            "CONTRACT_MIGRATION_FAILURE_CLASS_PUBLICATION_UNCERTAIN",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            8,
            "CONTRACT_MIGRATION_FAILURE_CLASS_PUBLISHED_VALIDATION_FAILED",
        ),
        (
            "riffdb.v1.ContractMigrationFailureClass",
            9,
            "CONTRACT_MIGRATION_FAILURE_CLASS_ROLLBACK_FAILED",
        ),
        (
            "riffdb.v1.ContractMigrationOperationKind",
            0,
            "CONTRACT_MIGRATION_OPERATION_KIND_UNSPECIFIED",
        ),
        (
            "riffdb.v1.ContractMigrationOperationKind",
            1,
            "CONTRACT_MIGRATION_OPERATION_KIND_CHECK",
        ),
        (
            "riffdb.v1.ContractMigrationOperationKind",
            2,
            "CONTRACT_MIGRATION_OPERATION_KIND_APPLY",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            0,
            "CONTRACT_MIGRATION_PHASE_UNSPECIFIED",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            1,
            "CONTRACT_MIGRATION_PHASE_ACCEPTED",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            2,
            "CONTRACT_MIGRATION_PHASE_DRAINING",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            3,
            "CONTRACT_MIGRATION_PHASE_PREFLIGHT",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            4,
            "CONTRACT_MIGRATION_PHASE_BACKUP_PUBLISHED",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            5,
            "CONTRACT_MIGRATION_PHASE_STAGING",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            6,
            "CONTRACT_MIGRATION_PHASE_TRANSFORMING",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            7,
            "CONTRACT_MIGRATION_PHASE_REBUILDING_PROJECTIONS",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            8,
            "CONTRACT_MIGRATION_PHASE_VALIDATING_STAGE",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            9,
            "CONTRACT_MIGRATION_PHASE_PUBLISHING",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            10,
            "CONTRACT_MIGRATION_PHASE_VALIDATING_PUBLISHED",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            11,
            "CONTRACT_MIGRATION_PHASE_ROLLING_BACK",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            12,
            "CONTRACT_MIGRATION_PHASE_SUCCEEDED",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            13,
            "CONTRACT_MIGRATION_PHASE_FAILED_CLOSED",
        ),
        (
            "riffdb.v1.ContractMigrationPhase",
            14,
            "CONTRACT_MIGRATION_PHASE_FAILED_ROLLED_BACK",
        ),
        (
            "riffdb.v1.ContractMigrationStartDisposition",
            0,
            "CONTRACT_MIGRATION_START_DISPOSITION_UNSPECIFIED",
        ),
        (
            "riffdb.v1.ContractMigrationStartDisposition",
            1,
            "CONTRACT_MIGRATION_START_DISPOSITION_ACCEPTED",
        ),
        (
            "riffdb.v1.ContractMigrationStartDisposition",
            2,
            "CONTRACT_MIGRATION_START_DISPOSITION_ALREADY_ACCEPTED",
        ),
        (
            "riffdb.v1.ContractMigrationStartDisposition",
            3,
            "CONTRACT_MIGRATION_START_DISPOSITION_TERMINAL",
        ),
        (
            "riffdb.v1.ContractMigrationStartDisposition",
            4,
            "CONTRACT_MIGRATION_START_DISPOSITION_ALREADY_APPLIED",
        ),
        (
            "riffdb.v1.ContractCompatibilityClass",
            0,
            "CONTRACT_COMPATIBILITY_CLASS_UNSPECIFIED",
        ),
        (
            "riffdb.v1.ContractCompatibilityClass",
            1,
            "CONTRACT_COMPATIBILITY_CLASS_COMPATIBLE",
        ),
        (
            "riffdb.v1.ContractCompatibilityClass",
            2,
            "CONTRACT_COMPATIBILITY_CLASS_REQUIRES_EXPLICIT_VERSION",
        ),
        (
            "riffdb.v1.ContractCompatibilityClass",
            3,
            "CONTRACT_COMPATIBILITY_CLASS_INCOMPATIBLE",
        ),
        (
            "riffdb.v1.ContractCompatibilityClass",
            4,
            "CONTRACT_COMPATIBILITY_CLASS_REQUIRES_MIGRATION",
        ),
        (
            "riffdb.v1.DiscoveryRepresentation",
            0,
            "DISCOVERY_REPRESENTATION_UNSPECIFIED",
        ),
        (
            "riffdb.v1.DiscoveryRepresentation",
            1,
            "DISCOVERY_REPRESENTATION_FULL",
        ),
        (
            "riffdb.v1.DiscoveryRepresentation",
            2,
            "DISCOVERY_REPRESENTATION_COMPACT_OBSERVATION",
        ),
        (
            "riffdb.v1.EventConsumerMutationResult",
            0,
            "EVENT_CONSUMER_MUTATION_RESULT_UNSPECIFIED",
        ),
        (
            "riffdb.v1.EventConsumerMutationResult",
            1,
            "EVENT_CONSUMER_MUTATION_RESULT_APPLIED",
        ),
        (
            "riffdb.v1.EventConsumerMutationResult",
            2,
            "EVENT_CONSUMER_MUTATION_RESULT_STATE_CHANGED",
        ),
        (
            "riffdb.v1.EventConsumerMutationResult",
            3,
            "EVENT_CONSUMER_MUTATION_RESULT_NOT_FOUND",
        ),
        (
            "riffdb.v1.EventConsumerMutationResult",
            4,
            "EVENT_CONSUMER_MUTATION_RESULT_OUTSTANDING_LEASE",
        ),
        (
            "riffdb.v1.EventConsumerMutationResult",
            5,
            "EVENT_CONSUMER_MUTATION_RESULT_STALE_LEASE",
        ),
        (
            "riffdb.v1.EventConsumerMutationResult",
            6,
            "EVENT_CONSUMER_MUTATION_RESULT_LEASE_EXPIRED",
        ),
        (
            "riffdb.v1.ExecutionFailureCode",
            3,
            "EXECUTION_FAILURE_CODE_UNIQUE_CONFLICT",
        ),
        ("riffdb.v1.FixedToolKind", 0, "FIXED_TOOL_KIND_UNSPECIFIED"),
        (
            "riffdb.v1.FixedToolKind",
            1,
            "FIXED_TOOL_KIND_VALIDATE_CONTRACT",
        ),
        (
            "riffdb.v1.FixedToolKind",
            2,
            "FIXED_TOOL_KIND_GET_ACTIVE_CONTRACT",
        ),
        (
            "riffdb.v1.FixedToolKind",
            3,
            "FIXED_TOOL_KIND_EXPLAIN_COMMAND",
        ),
        (
            "riffdb.v1.FixedToolKind",
            4,
            "FIXED_TOOL_KIND_DEPLOY_CONTRACT",
        ),
        (
            "riffdb.v1.FixedToolKind",
            5,
            "FIXED_TOOL_KIND_RESOLVE_COMMAND_OUTCOME",
        ),
        ("riffdb.v1.FixedToolKind", 6, "FIXED_TOOL_KIND_GET_ENTITY"),
        ("riffdb.v1.FixedToolKind", 7, "FIXED_TOOL_KIND_SCAN_INDEX"),
        ("riffdb.v1.FixedToolKind", 8, "FIXED_TOOL_KIND_GET_COMMIT"),
        ("riffdb.v1.FixedToolKind", 9, "FIXED_TOOL_KIND_SCAN_COMMITS"),
        (
            "riffdb.v1.FixedToolKind",
            10,
            "FIXED_TOOL_KIND_TRACE_PROVENANCE",
        ),
        (
            "riffdb.v1.FixedToolKind",
            11,
            "FIXED_TOOL_KIND_QUERY_PROJECTION",
        ),
        (
            "riffdb.v1.FixedToolKind",
            12,
            "FIXED_TOOL_KIND_GET_PROJECTION_STATUS",
        ),
        (
            "riffdb.v1.FixedToolKind",
            13,
            "FIXED_TOOL_KIND_LIST_PENDING_OUTBOX_DELIVERIES",
        ),
        ("riffdb.v1.FixedToolKind", 14, "FIXED_TOOL_KIND_GET_HEALTH"),
        (
            "riffdb.v1.FixedToolKind",
            15,
            "FIXED_TOOL_KIND_DESCRIBE_CONTRACT",
        ),
        ("riffdb.v1.FixedToolKind", 16, "FIXED_TOOL_KIND_CHECK_QUERY"),
        (
            "riffdb.v1.FixedToolKind",
            17,
            "FIXED_TOOL_KIND_EXPLAIN_QUERY",
        ),
        (
            "riffdb.v1.FixedToolKind",
            18,
            "FIXED_TOOL_KIND_EXECUTE_QUERY",
        ),
        ("riffdb.v1.FixedToolKind", 19, "FIXED_TOOL_KIND_RUN_COMMAND"),
        ("riffdb.v1.FixedToolKind", 20, "FIXED_TOOL_KIND_EVENT_NEXT"),
        ("riffdb.v1.FixedToolKind", 21, "FIXED_TOOL_KIND_EVENT_ACK"),
        ("riffdb.v1.FixedToolKind", 22, "FIXED_TOOL_KIND_EVENT_NACK"),
        ("riffdb.v1.FixedToolKind", 23, "FIXED_TOOL_KIND_EVENT_SEEK"),
        (
            "riffdb.v1.FixedToolKind",
            24,
            "FIXED_TOOL_KIND_EVENT_STATUS",
        ),
        ("riffdb.v1.FixedToolKind", 25, "FIXED_TOOL_KIND_QUERY_WATCH"),
        (
            "riffdb.v1.FixedToolKind",
            26,
            "FIXED_TOOL_KIND_CONTEXTUAL_NEXT",
        ),
        (
            "riffdb.v1.FixedToolKind",
            27,
            "FIXED_TOOL_KIND_CONTEXTUAL_ACK",
        ),
        (
            "riffdb.v1.FixedToolKind",
            28,
            "FIXED_TOOL_KIND_CONTEXTUAL_NACK",
        ),
        (
            "riffdb.v1.FixedToolKind",
            29,
            "FIXED_TOOL_KIND_CONTEXTUAL_STATUS",
        ),
        (
            "riffdb.v1.FixedToolKind",
            30,
            "FIXED_TOOL_KIND_CONTEXTUAL_REACT",
        ),
        (
            "riffdb.v1.OutboxDeliveryState",
            0,
            "OUTBOX_DELIVERY_STATE_UNSPECIFIED",
        ),
        (
            "riffdb.v1.OutboxDeliveryState",
            1,
            "OUTBOX_DELIVERY_STATE_PENDING",
        ),
        (
            "riffdb.v1.OutboxDeliveryState",
            2,
            "OUTBOX_DELIVERY_STATE_RETRY_SCHEDULED",
        ),
        (
            "riffdb.v1.OutboxDeliveryState",
            3,
            "OUTBOX_DELIVERY_STATE_DELIVERING",
        ),
        (
            "riffdb.v1.OutboxDeliveryState",
            4,
            "OUTBOX_DELIVERY_STATE_DEAD_LETTER",
        ),
        (
            "riffdb.v1.PublicErrorKind",
            10,
            "PUBLIC_ERROR_KIND_HISTORY_INCARNATION_MISMATCH",
        ),
        (
            "riffdb.v1.PublicErrorKind",
            11,
            "PUBLIC_ERROR_KIND_OVERLOADED",
        ),
        (
            "riffdb.v1.PublicErrorKind",
            12,
            "PUBLIC_ERROR_KIND_HISTORY_PRUNED",
        ),
        (
            "riffdb.v1.ResourceDiscoveryKind",
            0,
            "RESOURCE_DISCOVERY_KIND_UNSPECIFIED",
        ),
        (
            "riffdb.v1.ResourceDiscoveryKind",
            1,
            "RESOURCE_DISCOVERY_KIND_ALL",
        ),
        (
            "riffdb.v1.ResourceDiscoveryKind",
            2,
            "RESOURCE_DISCOVERY_KIND_CONCRETE",
        ),
        (
            "riffdb.v1.ResourceDiscoveryKind",
            3,
            "RESOURCE_DISCOVERY_KIND_TEMPLATE",
        ),
    ]
    .into_iter()
    .map(|(enumeration, number, name)| format!("enum-value {enumeration} {number} {name}"))
    .collect::<BTreeSet<_>>();
    for (enumeration, names) in [
        (
            "riffdb.v1.OfflineMaintenanceOperationKind",
            &[
                "OFFLINE_MAINTENANCE_OPERATION_KIND_UNSPECIFIED",
                "OFFLINE_MAINTENANCE_OPERATION_KIND_CREATE_BACKUP",
                "OFFLINE_MAINTENANCE_OPERATION_KIND_RESTORE_BACKUP",
            ][..],
        ),
        (
            "riffdb.v1.OfflineMaintenanceReplacementConfirmation",
            &[
                "OFFLINE_MAINTENANCE_REPLACEMENT_CONFIRMATION_UNSPECIFIED",
                "OFFLINE_MAINTENANCE_REPLACEMENT_CONFIRMATION_ALLOW_REPLACE_NONEMPTY_TARGET",
            ][..],
        ),
        (
            "riffdb.v1.OfflineMaintenanceStartDisposition",
            &[
                "OFFLINE_MAINTENANCE_START_DISPOSITION_UNSPECIFIED",
                "OFFLINE_MAINTENANCE_START_DISPOSITION_ACCEPTED",
                "OFFLINE_MAINTENANCE_START_DISPOSITION_ALREADY_ACCEPTED",
                "OFFLINE_MAINTENANCE_START_DISPOSITION_TERMINAL",
            ][..],
        ),
        (
            "riffdb.v1.OfflineMaintenancePhase",
            &[
                "OFFLINE_MAINTENANCE_PHASE_UNSPECIFIED",
                "OFFLINE_MAINTENANCE_PHASE_ACCEPTED",
                "OFFLINE_MAINTENANCE_PHASE_DRAINING",
                "OFFLINE_MAINTENANCE_PHASE_OFFLINE",
                "OFFLINE_MAINTENANCE_PHASE_ARTIFACT_PUBLISHED",
                "OFFLINE_MAINTENANCE_PHASE_VALIDATING",
                "OFFLINE_MAINTENANCE_PHASE_SUCCEEDED",
                "OFFLINE_MAINTENANCE_PHASE_FAILED_CLOSED",
            ][..],
        ),
        (
            "riffdb.v1.OfflineMaintenanceFailureClass",
            &[
                "OFFLINE_MAINTENANCE_FAILURE_CLASS_UNSPECIFIED",
                "OFFLINE_MAINTENANCE_FAILURE_CLASS_QUIESCENCE_FAILED",
                "OFFLINE_MAINTENANCE_FAILURE_CLASS_ARTIFACT_UNAVAILABLE",
                "OFFLINE_MAINTENANCE_FAILURE_CLASS_ARTIFACT_INVALID",
                "OFFLINE_MAINTENANCE_FAILURE_CLASS_STAGED_AUTHORIZATION_FAILED",
                "OFFLINE_MAINTENANCE_FAILURE_CLASS_STORAGE_UNAVAILABLE",
                "OFFLINE_MAINTENANCE_FAILURE_CLASS_VALIDATION_FAILED",
                "OFFLINE_MAINTENANCE_FAILURE_CLASS_RECEIPT_UNAVAILABLE",
                "OFFLINE_MAINTENANCE_FAILURE_CLASS_INTERNAL_FAILURE",
            ][..],
        ),
        (
            "riffdb.v1.LiveQueryResultCardinality",
            &[
                "LIVE_QUERY_RESULT_CARDINALITY_UNSPECIFIED",
                "LIVE_QUERY_RESULT_CARDINALITY_ONE",
                "LIVE_QUERY_RESULT_CARDINALITY_MAYBE",
                "LIVE_QUERY_RESULT_CARDINALITY_MANY",
            ][..],
        ),
        (
            "riffdb.v1.LiveQueryResetReason",
            &[
                "LIVE_QUERY_RESET_REASON_UNSPECIFIED",
                "LIVE_QUERY_RESET_REASON_OUTCOME_CHANGED",
                "LIVE_QUERY_RESET_REASON_DIFF_LIMIT_EXCEEDED",
                "LIVE_QUERY_RESET_REASON_DEFINITION_CHANGED",
                "LIVE_QUERY_RESET_REASON_HISTORY_CHANGED",
                "LIVE_QUERY_RESET_REASON_CURSOR_EXPIRED",
            ][..],
        ),
        (
            "riffdb.v1.LiveQueryTerminalReason",
            &[
                "LIVE_QUERY_TERMINAL_REASON_UNSPECIFIED",
                "LIVE_QUERY_TERMINAL_REASON_AUTHORIZATION_CHANGED",
                "LIVE_QUERY_TERMINAL_REASON_BUFFER_PRESSURE",
                "LIVE_QUERY_TERMINAL_REASON_LIFETIME_EXPIRED",
                "LIVE_QUERY_TERMINAL_REASON_SERVICE_UNAVAILABLE",
                "LIVE_QUERY_TERMINAL_REASON_INTEGRITY_FAILURE",
                "LIVE_QUERY_TERMINAL_REASON_DEFINITION_CHANGED",
            ][..],
        ),
    ] {
        for (number, name) in names.iter().enumerate() {
            assert!(values.insert(format!("enum-value {enumeration} {number} {name}")));
        }
    }
    values
}

fn expected_optional_registry() -> BTreeSet<String> {
    [
        (
            "riffdb.v1.CommandToolDiscoveryPage.next_cursor",
            "ContractService.DiscoverCommandTools:response:full-boundary-empty-exact-end",
            "ContractService.DiscoverCommandTools:response:full-boundary-limit-500-continuation",
        ),
        (
            "riffdb.v1.CompactCommandToolDiscoveryPage.next_cursor",
            "ContractService.DiscoverCommandTools:response:compact-boundary-empty-exact-end",
            "ContractService.DiscoverCommandTools:response:compact-boundary-limit-500-continuation",
        ),
        (
            "riffdb.v1.CompactResourceDiscoveryPage.next_cursor",
            "ContractService.DiscoverResources:response:compact-boundary-empty-exact-end",
            "ContractService.DiscoverResources:response:compact-boundary-limit-500-continuation",
        ),
        (
            "riffdb.v1.CompiledContractCandidate.parent_version",
            "ContractService.ValidateContract:response:valid",
            "ContractService.ValidateContract:response:candidate-preview",
        ),
        (
            "riffdb.v1.ContractCompatibilitySummary.parent_bundle_hash",
            "ContractService.GetActiveContract:response:present",
            "ContractService.GetActiveContract:response:present-successor",
        ),
        (
            "riffdb.v1.ContractCompatibilitySummary.parent_contract_version",
            "ContractService.GetActiveContract:response:present",
            "ContractService.GetActiveContract:response:present-successor",
        ),
        (
            "riffdb.v1.Decimal.precision",
            "CommandService.Execute:request:decimal-legacy-no-precision",
            "CommandService.Execute:request:decimal-with-precision",
        ),
        (
            "riffdb.v1.ExecuteCommandResponse.outcome_uri",
            "CommandService.Execute:response:committed-legacy-no-locator",
            "CommandService.Execute:response:committed",
        ),
        (
            "riffdb.v1.GetOutcomeRequest.outcome_uri",
            "CommandService.GetOutcome:request:resolve",
            "CommandService.GetOutcome:request:resolve-locator",
        ),
        (
            "riffdb.v1.OutboxDeliveryPage.next_cursor",
            "AdminService.ListPendingOutboxDeliveries:response:page",
            "AdminService.ListPendingOutboxDeliveries:response:page-with-cursor",
        ),
        (
            "riffdb.v1.ProvenanceClaims.approval_id",
            "CommitService.TraceProvenance:response:found-no-optional-claims",
            "CommitService.TraceProvenance:response:found",
        ),
        (
            "riffdb.v1.ProvenanceClaims.reason",
            "CommitService.TraceProvenance:response:found-no-optional-claims",
            "CommitService.TraceProvenance:response:found",
        ),
        (
            "riffdb.v1.ProvenanceClaims.source_commit",
            "CommitService.TraceProvenance:response:found-no-optional-claims",
            "CommitService.TraceProvenance:response:found",
        ),
        (
            "riffdb.v1.ProvenanceClaims.source_repository",
            "CommitService.TraceProvenance:response:found-no-optional-claims",
            "CommitService.TraceProvenance:response:found",
        ),
        (
            "riffdb.v1.ResourceDiscoveryPage.next_cursor",
            "ContractService.DiscoverResources:response:full-boundary-empty-exact-end",
            "ContractService.DiscoverResources:response:full-boundary-limit-500-continuation",
        ),
        (
            "riffdb.v1.GetCommitRequest.observed_history_incarnation",
            "CommitService.GetCommit:request:sequence",
            "CommitService.GetCommit:request:sequence-with-observed-incarnation",
        ),
        (
            "riffdb.v1.ScanCommitsRequest.observed_history_incarnation",
            "CommitService.ScanCommits:request:first-page",
            "CommitService.ScanCommits:request:first-page-with-observed-incarnation",
        ),
        (
            "riffdb.v1.SubscribeCommitsRequest.observed_history_incarnation",
            "CommitService.SubscribeCommits:request:from-head",
            "CommitService.SubscribeCommits:request:from-head-with-observed-incarnation",
        ),
        (
            "riffdb.v1.WatchNamedQueryRequest.cursor",
            "QueryService.WatchNamedQuery:request:fresh",
            "QueryService.WatchNamedQuery:request:resume",
        ),
    ]
    .into_iter()
    .map(|(field, absent, present)| format!("optional {field} {absent} {present}"))
    .collect()
}

fn expected_page_registry() -> BTreeSet<String> {
    let endpoints = [
        (
            "riffdb.v1.CommandToolDiscoveryPage",
            "ContractService.DiscoverCommandTools",
            "full",
        ),
        (
            "riffdb.v1.CompactCommandToolDiscoveryPage",
            "ContractService.DiscoverCommandTools",
            "compact",
        ),
        (
            "riffdb.v1.ResourceDiscoveryPage",
            "ContractService.DiscoverResources",
            "full",
        ),
        (
            "riffdb.v1.CompactResourceDiscoveryPage",
            "ContractService.DiscoverResources",
            "compact",
        ),
    ];
    let boundaries = [
        ("empty-exact-end", 0, "absent", "exact_end"),
        ("one-item-exact-end", 1, "absent", "exact_end"),
        ("limit-500-continuation", 500, "present", "continuation"),
        ("limit-500-exact-end", 500, "absent", "exact_end"),
    ];
    endpoints
        .into_iter()
        .flat_map(|(message, rpc, representation)| {
            boundaries.into_iter().map(move |(suffix, items, cursor, end)| {
                format!(
                    "page {message} {rpc}:response:{representation}-boundary-{suffix} items={items} cursor={cursor} end={end}"
                )
            })
        })
        .collect()
}

fn strict_decode<M: PublicMessage>(vector: &FixtureVector<'_>, message_type: &str) -> M {
    assert_eq!(vector.message_type, message_type);
    decode_public_message::<M>(&vector.bytes).expect("strict registered public fixture")
}

fn optional_field_present(field: &str, vector: &FixtureVector<'_>) -> bool {
    match field {
        "riffdb.v1.CompiledContractCandidate.parent_version" => {
            let response = strict_decode::<v1::ValidateContractResponse>(
                vector,
                "riffdb.v1.ValidateContractResponse",
            );
            match response.result.expect("registered validation result") {
                v1::validate_contract_response::Result::Candidate(candidate) => {
                    candidate.parent_version.is_some()
                }
                v1::validate_contract_response::Result::Valid(_) => false,
                _ => panic!("optional candidate fixture uses the wrong validation result"),
            }
        }
        field if field.starts_with("riffdb.v1.ContractCompatibilitySummary.") => {
            let response = strict_decode::<v1::GetActiveContractResponse>(
                vector,
                "riffdb.v1.GetActiveContractResponse",
            );
            let v1::get_active_contract_response::Result::Present(descriptor) =
                response.result.expect("registered active-contract result")
            else {
                panic!("compatibility fixture must contain an active contract");
            };
            let compatibility = descriptor
                .compatibility
                .expect("new server fixture supplies compatibility");
            match field {
                "riffdb.v1.ContractCompatibilitySummary.parent_bundle_hash" => {
                    compatibility.parent_bundle_hash.is_some()
                }
                "riffdb.v1.ContractCompatibilitySummary.parent_contract_version" => {
                    compatibility.parent_contract_version.is_some()
                }
                _ => unreachable!("closed compatibility optional registry"),
            }
        }
        "riffdb.v1.Decimal.precision" => {
            let request = strict_decode::<v1::ExecuteCommandRequest>(
                vector,
                "riffdb.v1.ExecuteCommandRequest",
            );
            let value = request.input.expect("registered command input");
            let Some(v1::value::Kind::DecimalValue(decimal)) = value.kind else {
                panic!("registered decimal fixture must contain a decimal");
            };
            decimal.precision.is_some()
        }
        "riffdb.v1.ExecuteCommandResponse.outcome_uri" => {
            strict_decode::<v1::ExecuteCommandResponse>(vector, "riffdb.v1.ExecuteCommandResponse")
                .outcome_uri
                .is_some()
        }
        "riffdb.v1.GetOutcomeRequest.outcome_uri" => {
            strict_decode::<v1::GetOutcomeRequest>(vector, "riffdb.v1.GetOutcomeRequest")
                .outcome_uri
                .is_some()
        }
        field if field.starts_with("riffdb.v1.ProvenanceClaims.") => {
            let response = strict_decode::<v1::TraceProvenanceResponse>(
                vector,
                "riffdb.v1.TraceProvenanceResponse",
            );
            let v1::trace_provenance_response::Result::Found(provenance) =
                response.result.expect("registered provenance result")
            else {
                panic!("optional claims fixture must be found");
            };
            let claims = provenance.claims.expect("claims container is required");
            match field {
                "riffdb.v1.ProvenanceClaims.approval_id" => claims.approval_id.is_some(),
                "riffdb.v1.ProvenanceClaims.reason" => claims.reason.is_some(),
                "riffdb.v1.ProvenanceClaims.source_commit" => claims.source_commit.is_some(),
                "riffdb.v1.ProvenanceClaims.source_repository" => {
                    claims.source_repository.is_some()
                }
                _ => unreachable!("closed optional-claims registry"),
            }
        }
        "riffdb.v1.OutboxDeliveryPage.next_cursor" => {
            strict_decode::<v1::ListPendingOutboxDeliveriesResponse>(
                vector,
                "riffdb.v1.ListPendingOutboxDeliveriesResponse",
            )
            .page
            .expect("registered outbox page")
            .next_cursor
            .is_some()
        }
        "riffdb.v1.CommandToolDiscoveryPage.next_cursor" => {
            let response = strict_decode::<v1::DiscoverCommandToolsResponse>(
                vector,
                "riffdb.v1.DiscoverCommandToolsResponse",
            );
            let v1::discover_command_tools_response::Result::Page(page) =
                response.result.expect("registered discovery result")
            else {
                panic!("registered command page uses the wrong representation");
            };
            page.next_cursor.is_some()
        }
        "riffdb.v1.CompactCommandToolDiscoveryPage.next_cursor" => {
            let response = strict_decode::<v1::DiscoverCommandToolsResponse>(
                vector,
                "riffdb.v1.DiscoverCommandToolsResponse",
            );
            let v1::discover_command_tools_response::Result::CompactPage(page) =
                response.result.expect("registered discovery result")
            else {
                panic!("registered command page uses the wrong representation");
            };
            page.next_cursor.is_some()
        }
        "riffdb.v1.ResourceDiscoveryPage.next_cursor" => {
            let response = strict_decode::<v1::DiscoverResourcesResponse>(
                vector,
                "riffdb.v1.DiscoverResourcesResponse",
            );
            let v1::discover_resources_response::Result::Page(page) =
                response.result.expect("registered discovery result")
            else {
                panic!("registered resource page uses the wrong representation");
            };
            page.next_cursor.is_some()
        }
        "riffdb.v1.CompactResourceDiscoveryPage.next_cursor" => {
            let response = strict_decode::<v1::DiscoverResourcesResponse>(
                vector,
                "riffdb.v1.DiscoverResourcesResponse",
            );
            let v1::discover_resources_response::Result::CompactPage(page) =
                response.result.expect("registered discovery result")
            else {
                panic!("registered resource page uses the wrong representation");
            };
            page.next_cursor.is_some()
        }
        "riffdb.v1.GetCommitRequest.observed_history_incarnation" => {
            strict_decode::<v1::GetCommitRequest>(vector, "riffdb.v1.GetCommitRequest")
                .observed_history_incarnation
                .is_some()
        }
        "riffdb.v1.ScanCommitsRequest.observed_history_incarnation" => {
            strict_decode::<v1::ScanCommitsRequest>(vector, "riffdb.v1.ScanCommitsRequest")
                .observed_history_incarnation
                .is_some()
        }
        "riffdb.v1.SubscribeCommitsRequest.observed_history_incarnation" => {
            strict_decode::<v1::SubscribeCommitsRequest>(
                vector,
                "riffdb.v1.SubscribeCommitsRequest",
            )
            .observed_history_incarnation
            .is_some()
        }
        "riffdb.v1.WatchNamedQueryRequest.cursor" => {
            strict_decode::<v1::WatchNamedQueryRequest>(vector, "riffdb.v1.WatchNamedQueryRequest")
                .cursor
                .is_some()
        }
        _ => panic!("unknown optional-field registry entry: {field}"),
    }
}

fn assert_discovery_fence(fence: &v1::DiscoveryCatalogFence) {
    assert!(fence.state.is_some());
    assert_eq!(fence.server_generation.len(), 16);
    assert!(fence.operation_schemas.is_some());
}

fn discovery_page_shape(page_type: &str, vector: &FixtureVector<'_>) -> (usize, Option<usize>) {
    match page_type {
        "riffdb.v1.CommandToolDiscoveryPage" => {
            let response = strict_decode::<v1::DiscoverCommandToolsResponse>(
                vector,
                "riffdb.v1.DiscoverCommandToolsResponse",
            );
            let v1::discover_command_tools_response::Result::Page(page) =
                response.result.expect("registered discovery result")
            else {
                panic!("registered command page uses the wrong representation");
            };
            assert_discovery_fence(page.observed_fence.as_ref().expect("observed fence"));
            assert!(page.operation_schemas.is_some());
            (page.items.len(), page.next_cursor.as_ref().map(Vec::len))
        }
        "riffdb.v1.CompactCommandToolDiscoveryPage" => {
            let response = strict_decode::<v1::DiscoverCommandToolsResponse>(
                vector,
                "riffdb.v1.DiscoverCommandToolsResponse",
            );
            let v1::discover_command_tools_response::Result::CompactPage(page) =
                response.result.expect("registered discovery result")
            else {
                panic!("registered command page uses the wrong representation");
            };
            assert_discovery_fence(page.observed_fence.as_ref().expect("observed fence"));
            (page.items.len(), page.next_cursor.as_ref().map(Vec::len))
        }
        "riffdb.v1.ResourceDiscoveryPage" => {
            let response = strict_decode::<v1::DiscoverResourcesResponse>(
                vector,
                "riffdb.v1.DiscoverResourcesResponse",
            );
            let v1::discover_resources_response::Result::Page(page) =
                response.result.expect("registered discovery result")
            else {
                panic!("registered resource page uses the wrong representation");
            };
            assert_discovery_fence(page.observed_fence.as_ref().expect("observed fence"));
            (page.items.len(), page.next_cursor.as_ref().map(Vec::len))
        }
        "riffdb.v1.CompactResourceDiscoveryPage" => {
            let response = strict_decode::<v1::DiscoverResourcesResponse>(
                vector,
                "riffdb.v1.DiscoverResourcesResponse",
            );
            let v1::discover_resources_response::Result::CompactPage(page) =
                response.result.expect("registered discovery result")
            else {
                panic!("registered resource page uses the wrong representation");
            };
            assert_discovery_fence(page.observed_fence.as_ref().expect("observed fence"));
            (page.items.len(), page.next_cursor.as_ref().map(Vec::len))
        }
        _ => panic!("unknown page registry entry: {page_type}"),
    }
}

fn assert_unspecified_enum_rejected(enumeration: &str, message_type: &str, bytes: &[u8]) {
    match enumeration {
        "riffdb.v1.FixedToolKind" => {
            assert_eq!(message_type, "riffdb.v1.DiscoverCommandToolsResponse");
            let message = v1::DiscoverCommandToolsResponse::decode(bytes).expect("raw fixture");
            let Some(v1::discover_command_tools_response::Result::CompactPage(page)) =
                message.result
            else {
                panic!("fixed-tool rejection fixture must be a compact page");
            };
            let Some(v1::compact_command_tool_discovery_item::Item::FixedTool(value)) =
                page.items[0].item
            else {
                panic!("fixed-tool rejection fixture must carry the enum");
            };
            assert_eq!(value, 0);
            assert!(matches!(
                decode_public_message::<v1::DiscoverCommandToolsResponse>(bytes),
                Err(PublicWireError::InvalidEnum)
            ));
        }
        "riffdb.v1.DiscoveryRepresentation" => {
            assert_eq!(message_type, "riffdb.v1.DiscoverCommandToolsRequest");
            let message = v1::DiscoverCommandToolsRequest::decode(bytes).expect("raw fixture");
            assert_eq!(message.representation, 0);
            assert!(matches!(
                decode_public_message::<v1::DiscoverCommandToolsRequest>(bytes),
                Err(PublicWireError::InvalidEnum)
            ));
        }
        "riffdb.v1.ResourceDiscoveryKind" => {
            assert_eq!(message_type, "riffdb.v1.DiscoverResourcesRequest");
            let message = v1::DiscoverResourcesRequest::decode(bytes).expect("raw fixture");
            assert_eq!(message.kind, 0);
            assert!(matches!(
                decode_public_message::<v1::DiscoverResourcesRequest>(bytes),
                Err(PublicWireError::InvalidEnum)
            ));
        }
        "riffdb.v1.OutboxDeliveryState" => {
            assert_eq!(
                message_type,
                "riffdb.v1.ListPendingOutboxDeliveriesResponse"
            );
            let message =
                v1::ListPendingOutboxDeliveriesResponse::decode(bytes).expect("raw fixture");
            assert_eq!(message.page.expect("outbox page").items[0].state, 0);
            assert!(matches!(
                decode_public_message::<v1::ListPendingOutboxDeliveriesResponse>(bytes),
                Err(PublicWireError::InvalidEnum)
            ));
        }
        _ => panic!("unknown unspecified-enum registry entry: {enumeration}"),
    }
}

#[test]
fn wp137_enum_optional_and_page_registry_is_complete() {
    let (vectors, registry) = fixture_sections();
    assert_eq!(vectors.len(), 141);

    let expected_enums = expected_enum_values();
    let expected_optionals = expected_optional_registry();
    let expected_pages = expected_page_registry();
    let actual_enums = registry
        .iter()
        .filter(|row| row.starts_with("enum-value "))
        .cloned()
        .collect::<BTreeSet<_>>();
    let actual_optionals = registry
        .iter()
        .filter(|row| row.starts_with("optional "))
        .cloned()
        .collect::<BTreeSet<_>>();
    let actual_pages = registry
        .iter()
        .filter(|row| row.starts_with("page "))
        .cloned()
        .collect::<BTreeSet<_>>();
    let actual_rejections = registry
        .iter()
        .filter(|row| row.starts_with("enum-rejection "))
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_enums, expected_enums);
    assert_eq!(actual_optionals, expected_optionals);
    assert_eq!(actual_pages, expected_pages);
    assert_eq!(actual_rejections.len(), 4);
    assert_eq!(
        actual_enums.len() + actual_optionals.len() + actual_pages.len() + actual_rejections.len(),
        registry.len(),
        "every registry row must have one closed evidence kind"
    );

    let (descriptor_enums, descriptor_optionals) = descriptor_delta();
    assert_eq!(descriptor_enums, expected_enums);
    assert_eq!(
        descriptor_optionals,
        expected_optionals
            .iter()
            .map(|row| row.split_whitespace().nth(1).expect("optional field"))
            .map(str::to_owned)
            .collect(),
        "new optional fields require independent test-owned coverage"
    );

    for row in &expected_optionals {
        let fields = row.split_whitespace().collect::<Vec<_>>();
        assert_eq!(fields.len(), 4);
        let absent = vectors.get(fields[2]).expect("registered absent vector");
        let present = vectors.get(fields[3]).expect("registered present vector");
        assert!(!optional_field_present(fields[1], absent), "{row}");
        assert!(optional_field_present(fields[1], present), "{row}");
    }

    for row in &expected_pages {
        let fields = row.split_whitespace().collect::<Vec<_>>();
        assert_eq!(fields.len(), 6);
        let vector = vectors.get(fields[2]).expect("registered page vector");
        let expected_items = fields[3]
            .strip_prefix("items=")
            .expect("items registry value")
            .parse::<usize>()
            .expect("item count");
        let expected_cursor = match fields[4] {
            "cursor=absent" => None,
            "cursor=present" => Some(16),
            value => panic!("unknown cursor registry value: {value}"),
        };
        match fields[5] {
            "end=exact_end" => assert!(expected_cursor.is_none()),
            "end=continuation" => assert_eq!(expected_cursor, Some(16)),
            value => panic!("unknown page-end registry value: {value}"),
        }
        assert_eq!(
            discovery_page_shape(fields[1], vector),
            (expected_items, expected_cursor),
            "{row}"
        );
    }

    let expected_rejections = BTreeSet::from([
        "riffdb.v1.DiscoveryRepresentation",
        "riffdb.v1.FixedToolKind",
        "riffdb.v1.OutboxDeliveryState",
        "riffdb.v1.ResourceDiscoveryKind",
    ])
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    let mut rejection_enums = BTreeSet::new();
    for row in actual_rejections {
        let fields = row.split_whitespace().collect::<Vec<_>>();
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[4], "invalid_enum");
        let bytes = decode_hex(fields[3]);
        assert_unspecified_enum_rejected(fields[1], fields[2], &bytes);
        assert!(rejection_enums.insert(fields[1].to_owned()));
    }
    assert_eq!(rejection_enums, expected_rejections);

    let command_page = strict_decode::<v1::DiscoverCommandToolsResponse>(
        &vectors["ContractService.DiscoverCommandTools:response:full-page"],
        "riffdb.v1.DiscoverCommandToolsResponse",
    );
    let Some(v1::discover_command_tools_response::Result::Page(command_page)) = command_page.result
    else {
        panic!("full-page enum fixture uses the wrong representation");
    };
    assert_eq!(
        command_page
            .items
            .into_iter()
            .filter_map(|item| match item.item {
                Some(v1::command_tool_discovery_item::Item::FixedTool(value)) => Some(value),
                _ => None,
            })
            .collect::<BTreeSet<_>>(),
        (1..=14).collect()
    );

    let representations = [
        "ContractService.DiscoverCommandTools:request:full-first-page",
        "ContractService.DiscoverCommandTools:request:compact-conditional",
    ]
    .map(|key| {
        strict_decode::<v1::DiscoverCommandToolsRequest>(
            &vectors[key],
            "riffdb.v1.DiscoverCommandToolsRequest",
        )
        .representation
    })
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(representations, BTreeSet::from([1, 2]));

    let resource_kinds = [
        "ContractService.DiscoverResources:request:full-all",
        "ContractService.DiscoverResources:request:full-concrete",
        "ContractService.DiscoverResources:request:full-template",
    ]
    .map(|key| {
        strict_decode::<v1::DiscoverResourcesRequest>(
            &vectors[key],
            "riffdb.v1.DiscoverResourcesRequest",
        )
        .kind
    })
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(resource_kinds, BTreeSet::from([1, 2, 3]));

    let outbox_states = [
        "AdminService.ListPendingOutboxDeliveries:response:page-pending",
        "AdminService.ListPendingOutboxDeliveries:response:page",
        "AdminService.ListPendingOutboxDeliveries:response:page-delivering",
        "AdminService.ListPendingOutboxDeliveries:response:page-dead-letter",
    ]
    .map(|key| {
        strict_decode::<v1::ListPendingOutboxDeliveriesResponse>(
            &vectors[key],
            "riffdb.v1.ListPendingOutboxDeliveriesResponse",
        )
        .page
        .expect("outbox page")
        .items[0]
            .state
    })
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(outbox_states, BTreeSet::from([1, 2, 3, 4]));
}
