//! Structural conformance for the completed phase-zero public messages.

use prost::Message;
use riffdb_errors::{
    ApplicationError, ApplicationErrorCode, ApplicationErrorContext, ApplicationOperation,
};
use riffdb_proto::{
    PublicWireError, app::v1 as app_v1, application_error_to_proto, decode_public_message, v1,
    validate_contract_validation_exchange, validate_create_capability_exchange,
    validate_create_offline_backup_exchange, validate_execute_command_batch_exchange,
    validate_explain_command_exchange, validate_get_offline_maintenance_operation_exchange,
    validate_public_message, validate_query_projection_exchange,
    validate_restore_offline_backup_exchange, validate_scan_commits_exchange,
    validate_scan_index_exchange,
};
use riffdb_types::{
    BackupNameV1, OfflineMaintenanceOperationKind, OfflineMaintenanceReplacementConfirmation,
    hash_schema, offline_maintenance_input_hash,
};

fn uuid_v7() -> Vec<u8> {
    vec![
        0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb, 0xbd,
        0x23,
    ]
}

fn active_contract() -> v1::ContractSelection {
    v1::ContractSelection {
        selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
    }
}

fn null_value() -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::NullValue(v1::NullValue::NullValue as i32)),
    }
}

fn empty_record() -> v1::ValueRecord {
    v1::ValueRecord { fields: Vec::new() }
}

fn entity_key(owner: u32) -> Vec<u8> {
    let mut bytes = vec![0x45, 0x01];
    bytes.extend_from_slice(&owner.to_be_bytes());
    bytes
}

fn partition_key(owner: u32) -> Vec<u8> {
    let mut bytes = vec![0x50, 0x01];
    bytes.extend_from_slice(&owner.to_be_bytes());
    bytes
}

fn index_key(owner: u32, ordinal: u32) -> Vec<u8> {
    let mut bytes = vec![0x49, 0x01];
    bytes.extend_from_slice(&owner.to_be_bytes());
    bytes.extend_from_slice(&10_u32.to_be_bytes());
    bytes.extend_from_slice(&[0x45, 0x01, 0, 0, 0, 1]);
    bytes.extend_from_slice(&ordinal.to_be_bytes());
    bytes
}

fn before_first() -> v1::FrontierPosition {
    v1::FrontierPosition {
        position: Some(v1::frontier_position::Position::BeforeFirst(v1::Unit {})),
    }
}

fn applied(sequence: u64) -> v1::FrontierPosition {
    v1::FrontierPosition {
        position: Some(v1::frontier_position::Position::AppliedThrough(sequence)),
    }
}

fn grant() -> v1::CapabilityGrant {
    v1::CapabilityGrant {
        tenant_scope: Some(v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
        }),
        partition_scope: Some(v1::PartitionScope {
            scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
        }),
        permissions: Vec::new(),
        field_visibility: Vec::new(),
        max_scan_rows: 1,
        approval_required: Vec::new(),
    }
}

fn create_request(mode: v1::CapabilityCreateMode) -> v1::CreateCapabilityRequest {
    v1::CreateCapabilityRequest {
        request_id: uuid_v7(),
        mode: mode as i32,
        capability_id: uuid_v7(),
        principal_id: "operator".to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: 60,
        audiences: vec!["riffdb-cli".to_owned()],
        grant: Some(grant()),
    }
}

fn transition() -> v1::CapabilityTransition {
    v1::CapabilityTransition {
        identity: Some(v1::CapabilityIdentity {
            capability_id: uuid_v7(),
            revision: 1,
        }),
        administration_sequence: 1,
    }
}

fn commit() -> v1::Commit {
    v1::Commit {
        commit_sequence: 1,
        admission_request_id: uuid_v7(),
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        command_id: 1,
        plan_hash: vec![0x11; 32],
        canonical_input_hash: vec![0x22; 32],
        actor: Some(v1::AdmittedActor {
            principal_id: "operator".to_owned(),
            actor_kind: v1::ActorKind::Human as i32,
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            agent_session_id: Some(uuid_v7()),
        }),
        logical_time: Some(v1::Timestamp {
            seconds: 1,
            nanos: 2,
        }),
        partition_hash: vec![0x33; 32],
        conflict_hashes: Vec::new(),
        affected_entities: Vec::new(),
        events: Vec::new(),
        outcome: Some(v1::DeclaredOutcome {
            outcome_id: 1,
            outcome_name: "Reserved".to_owned(),
            value: Some(empty_record()),
        }),
        provenance_uri: "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23".to_owned(),
        durability: v1::CommandDurability::Synchronous as i32,
    }
}

fn contract_descriptor() -> v1::ContractDescriptor {
    v1::ContractDescriptor {
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        bundle_hash: vec![0x11; 32],
        source_hash: vec![0x22; 32],
        plan_root_hash: vec![0x33; 32],
        compatibility: Some(v1::ContractCompatibilitySummary {
            parent_contract_version: None,
            parent_bundle_hash: None,
            overall: v1::ContractCompatibilityClass::Compatible as i32,
            code_counts: Vec::new(),
        }),
    }
}

fn schema_artifact(artifact: v1::schema_artifact_key::Artifact) -> v1::GeneratedSchemaArtifact {
    let canonical_json = "{}".to_owned();
    v1::GeneratedSchemaArtifact {
        key: Some(v1::SchemaArtifactKey {
            artifact: Some(artifact),
        }),
        dialect: "https://json-schema.org/draft/2020-12/schema".to_owned(),
        schema_hash: hash_schema(canonical_json.as_bytes()).as_bytes().to_vec(),
        canonical_json,
    }
}

fn explained_command() -> v1::ExplainCommandResponse {
    v1::ExplainCommandResponse {
        result: Some(v1::explain_command_response::Result::Found(
            v1::ExplainedCommand {
                contract: Some(contract_descriptor()),
                command_id: 1,
                plan_hash: vec![0x55; 32],
                tool_name: "riffdb_cmd_legalspend_reserve".to_owned(),
                explanation: Some(v1::CommandExplain {
                    command_id: 1,
                    execution_class: v1::ExecutionClass::IdempotentMutation as i32,
                    partition_component_count: 1,
                    conflict_key_count: 256,
                    binding_ids: vec![0, 1],
                    read_fields: vec![v1::BindingFieldRef {
                        binding_id: 0,
                        field_id: 1,
                    }],
                    write_fields: vec![v1::BindingFieldRef {
                        binding_id: 1,
                        field_id: 2,
                    }],
                    invariant_ids: vec![1, 1, 2],
                    event_type_ids: vec![2, 1, 2],
                    outcome_ids: vec![1],
                    rendered_text: "command budget.reserve".to_owned(),
                }),
                input_schema: Some(schema_artifact(
                    v1::schema_artifact_key::Artifact::CommandInputId(1),
                )),
                outcome_schema: Some(schema_artifact(
                    v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(1),
                )),
            },
        )),
    }
}

#[test]
fn root_duplicate_fields_reject_before_prost_merge() {
    let request = v1::GetCommitRequest {
        request_id: uuid_v7(),
        commit_sequence: 1,

        observed_history_incarnation: None,
    };
    let mut encoded = request.encode_to_vec();
    encoded.extend_from_slice(&[0x10, 0x02]);
    assert_eq!(
        decode_public_message::<v1::GetCommitRequest>(&encoded),
        Err(PublicWireError::MalformedEncoding)
    );
}

#[test]
fn nested_duplicates_and_repeated_limits_reject_before_prost_merge() {
    let get = v1::GetCommitRequest {
        request_id: uuid_v7(),
        commit_sequence: 1,

        observed_history_incarnation: None,
    };
    decode_public_message::<v1::GetCommitRequest>(&get.encode_to_vec())
        .expect("valid GetCommit remains wired to root-only preflight");

    let mut duplicate_page = vec![0x0a, 0x10];
    duplicate_page.extend(uuid_v7());
    duplicate_page.extend_from_slice(&[0x12, 0x04, 0x08, 0x01, 0x08, 0x02]);
    assert_eq!(
        decode_public_message::<v1::ScanCommitsRequest>(&duplicate_page),
        Err(PublicWireError::MalformedEncoding)
    );

    let oversized_diagnostics = v1::ValidateContractResponse {
        result: Some(v1::validate_contract_response::Result::Invalid(
            v1::CompilationDiagnostics {
                diagnostics: Some(v1::compilation_diagnostics::Diagnostics::Syntax(
                    v1::SyntaxDiagnosticList {
                        diagnostics: vec![v1::SyntaxDiagnostic::default(); 33],
                    },
                )),
            },
        )),
    };
    assert_eq!(
        decode_public_message::<v1::ValidateContractResponse>(
            &oversized_diagnostics.encode_to_vec()
        ),
        Err(PublicWireError::PreflightLimitExceeded)
    );
}

#[test]
fn diagnostics_use_the_closed_registry_and_submitted_source() {
    let request = v1::ValidateContractRequest {
        request_id: uuid_v7(),
        source: "x".to_owned(),
        preview_active_successor: false,
    };
    let mut response = v1::ValidateContractResponse {
        result: Some(v1::validate_contract_response::Result::Invalid(
            v1::CompilationDiagnostics {
                diagnostics: Some(v1::compilation_diagnostics::Diagnostics::Syntax(
                    v1::SyntaxDiagnosticList {
                        diagnostics: vec![v1::SyntaxDiagnostic {
                            code: "RDB-S004".to_owned(),
                            summary: "contract source contains an unexpected token".to_owned(),
                            help: Some(
                                "use the grammar-version-1 spelling shown in the language reference"
                                    .to_owned(),
                            ),
                            span: Some(v1::SourceSpan { start: 0, end: 1 }),
                            expected: vec!["entity".to_owned()],
                        }],
                    },
                )),
            },
        )),
    };
    validate_contract_validation_exchange(&request, &response).expect("closed diagnostic");

    if let Some(v1::validate_contract_response::Result::Invalid(diagnostics)) =
        response.result.as_mut()
        && let Some(v1::compilation_diagnostics::Diagnostics::Syntax(list)) =
            diagnostics.diagnostics.as_mut()
    {
        list.diagnostics[0].summary = "caller-controlled".to_owned();
    }
    assert_eq!(
        validate_public_message(&response),
        Err(PublicWireError::InvalidBytes)
    );
    if let Some(v1::validate_contract_response::Result::Invalid(diagnostics)) =
        response.result.as_mut()
        && let Some(v1::compilation_diagnostics::Diagnostics::Syntax(list)) =
            diagnostics.diagnostics.as_mut()
    {
        list.diagnostics[0].summary = "contract source contains an unexpected token".to_owned();
        list.diagnostics[0].span.as_mut().expect("span").end = 2;
    }
    validate_public_message(&response).expect("response alone has no submitted source");
    assert_eq!(
        validate_contract_validation_exchange(&request, &response),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn explain_validation_matches_compiler_local_id_and_occurrence_semantics() {
    let mut response = explained_command();
    validate_public_message(&response).expect("checked compiler explain shape");
    match response.result.as_mut().expect("result") {
        v1::explain_command_response::Result::Found(found) => {
            found.explanation.as_mut().expect("explanation").binding_ids = vec![1, 2];
        }
        v1::explain_command_response::Result::NotFound(_) => unreachable!(),
    }
    assert_eq!(
        validate_public_message(&response),
        Err(PublicWireError::NonCanonical)
    );
    match response.result.as_mut().expect("result") {
        v1::explain_command_response::Result::Found(found) => {
            let explanation = found.explanation.as_mut().expect("explanation");
            explanation.binding_ids = vec![0, 1];
            explanation.conflict_key_count = 257;
        }
        v1::explain_command_response::Result::NotFound(_) => unreachable!(),
    }
    assert_eq!(
        validate_public_message(&response),
        Err(PublicWireError::TooManyItems)
    );
}

#[test]
fn every_shared_identifier_class_uses_exact_uuid_v7() {
    let mut get = v1::GetCommitRequest {
        request_id: uuid_v7(),
        commit_sequence: 1,

        observed_history_incarnation: None,
    };
    validate_public_message(&get).expect("request UUIDv7");
    get.request_id[6] = 0x40;
    assert_eq!(
        validate_public_message(&get),
        Err(PublicWireError::InvalidUuidV7)
    );
    get.request_id = uuid_v7();
    get.request_id[8] = 0x40;
    assert_eq!(
        validate_public_message(&get),
        Err(PublicWireError::InvalidUuidV7)
    );
    get.request_id.pop();
    assert_eq!(
        validate_public_message(&get),
        Err(PublicWireError::InvalidUuidV7)
    );

    let mut create = create_request(v1::CapabilityCreateMode::Normal);
    create.capability_id[6] = 0x40;
    assert_eq!(
        validate_public_message(&create),
        Err(PublicWireError::InvalidUuidV7)
    );

    let mut response = v1::GetCommitResponse {
        result: Some(v1::get_commit_response::Result::Found(commit())),

        history_incarnation: 1,
    };
    response
        .result
        .as_mut()
        .and_then(|result| match result {
            v1::get_commit_response::Result::Found(commit) => commit.actor.as_mut(),
            v1::get_commit_response::Result::NotFound(_) => None,
        })
        .expect("actor")
        .agent_session_id = Some(vec![0; 16]);
    assert_eq!(
        validate_public_message(&response),
        Err(PublicWireError::InvalidUuidV7)
    );
}

#[test]
fn cursors_remain_opaque_and_are_not_reclassified_as_uuids() {
    let request = v1::ScanCommitsRequest {
        request_id: uuid_v7(),
        page: Some(v1::PageRequest {
            limit: Some(1),
            cursor: Some(vec![0; 16]),
        }),

        observed_history_incarnation: None,
    };
    validate_public_message(&request).expect("opaque cursor");
}

#[test]
fn key_stage_one_distinguishes_envelope_from_schema_validation() {
    let mut request = v1::GetEntityRequest {
        request_id: uuid_v7(),
        contract: Some(active_contract()),
        entity_type_id: 7,
        entity_key: entity_key(7),
        fields: Some(v1::FieldSelection {
            field_ids: Vec::new(),
        }),
    };
    validate_public_message(&request).expect("minimum entity envelope");

    request.entity_key.extend_from_slice(&[0xff, 0xff, 0xff]);
    validate_public_message(&request).expect("component bytes require catalog schema");
    request.entity_key[0] = 0x50;
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::InvalidKeyEnvelope)
    );
    request.entity_key = entity_key(8);
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::KeyOwnerMismatch)
    );

    request.entity_key = entity_key(7);
    request.entity_key.resize(4_097, 0);
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::InvalidKeyEnvelope)
    );

    let mut explicit = grant();
    explicit.partition_scope = Some(v1::PartitionScope {
        scope: Some(v1::partition_scope::Scope::Explicit(
            v1::ExplicitPartitionScope {
                partitions: vec![v1::ScopedPartition {
                    contract_lineage: "budget".to_owned(),
                    partition_key: partition_key(3),
                }],
            },
        )),
    });
    let mut create = create_request(v1::CapabilityCreateMode::Normal);
    create.grant = Some(explicit);
    validate_public_message(&create).expect("partition envelope only");
}

#[test]
fn read_only_and_outcome_replay_shapes_are_closed() {
    let read_only = v1::ExecuteCommandResponse {
        status: v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32,
        commit_sequence: 0,
        contract_version: 1,
        plan_hash: vec![0x44; 32],
        outcome_type: "Balance".to_owned(),
        outcome: Some(null_value()),
        provenance_uri: String::new(),
        durability_mode: String::new(),
        outcome_uri: None,

        history_incarnation: 1,
    };
    validate_public_message(&read_only).expect("exact read-only sentinels");
    let mut invalid = read_only.clone();
    invalid.commit_sequence = 1;
    assert_eq!(
        validate_public_message(&invalid),
        Err(PublicWireError::InconsistentFields)
    );

    let mut journaled = read_only;
    journaled.status = v1::execute_command_response::CompletionStatus::Replayed as i32;
    journaled.commit_sequence = 1;
    journaled.provenance_uri =
        "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23".to_owned();
    journaled.durability_mode = "sync".to_owned();
    let response = v1::GetOutcomeResponse {
        result: Some(v1::get_outcome_response::Result::Found(journaled.clone())),
    };
    validate_public_message(&response).expect("found outcome is replayed");
    journaled.status = v1::execute_command_response::CompletionStatus::Committed as i32;
    assert_eq!(
        validate_public_message(&v1::GetOutcomeResponse {
            result: Some(v1::get_outcome_response::Result::Found(journaled)),
        }),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn projection_durations_pages_and_lifecycle_shapes_are_checked() {
    let mut request = v1::QueryProjectionRequest {
        request_id: uuid_v7(),
        contract: Some(active_contract()),
        projection_id: 1,
        leading_components: Vec::new(),
        required_sequence: None,
        wait_nanos: 0,
        page: Some(v1::PageRequest {
            limit: Some(500),
            cursor: None,
        }),
    };
    validate_public_message(&request).expect("non-waiting query");
    request.wait_nanos = 1;
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::InvalidIdentity)
    );
    request.required_sequence = Some(1);
    request.wait_nanos = 30_000_000_000;
    validate_public_message(&request).expect("maximum wait");
    request.wait_nanos += 1;
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::InvalidIdentity)
    );

    let identity = v1::ProjectionIdentity {
        contract_lineage: "budget".to_owned(),
        projection_id: 1,
        projection_plan_hash: vec![0x55; 32],
    };
    let uninitialized = v1::ProjectionStatus {
        identity: Some(identity.clone()),
        lifecycle: v1::ProjectionLifecycle::Building as i32,
        published: None,
        candidate: None,
        published_apply_mode: None,
        failure: None,
        authoritative_head: Some(before_first()),
    };
    validate_public_message(&uninitialized).expect("uninitialized shape");

    let ready = v1::ProjectionStatus {
        identity: Some(identity),
        lifecycle: v1::ProjectionLifecycle::Ready as i32,
        published: Some(v1::ProjectionGenerationFrontier {
            generation: 1,
            frontier: Some(applied(1)),
        }),
        candidate: None,
        published_apply_mode: Some(v1::PublishedApplyMode::Enabled as i32),
        failure: None,
        authoritative_head: Some(applied(1)),
    };
    validate_public_message(&ready).expect("ready shape");
    let mut invalid = ready;
    invalid.candidate = invalid.published;
    assert_eq!(
        validate_public_message(&invalid),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn collection_limits_are_checked_at_exact_boundaries() {
    let mut request = v1::ScanIndexRequest {
        request_id: uuid_v7(),
        contract: Some(active_contract()),
        index_id: 1,
        leading_components: vec![null_value(); 1_024],
        fields: Some(v1::FieldSelection {
            field_ids: Vec::new(),
        }),
        page: Some(v1::PageRequest {
            limit: Some(1),
            cursor: None,
        }),
    };
    decode_public_message::<v1::ScanIndexRequest>(&request.encode_to_vec())
        .expect("1,024 components");
    request.leading_components.push(null_value());
    assert_eq!(
        decode_public_message::<v1::ScanIndexRequest>(&request.encode_to_vec()),
        Err(PublicWireError::PreflightLimitExceeded)
    );

    let mut response = v1::GetCommitResponse {
        result: Some(v1::get_commit_response::Result::Found(commit())),

        history_incarnation: 1,
    };
    let found = match response.result.as_mut().expect("result") {
        v1::get_commit_response::Result::Found(commit) => commit,
        v1::get_commit_response::Result::NotFound(_) => unreachable!(),
    };
    found.conflict_hashes = (0_u32..4_096)
        .map(|value| {
            let mut hash = vec![0; 32];
            hash[..4].copy_from_slice(&value.to_be_bytes());
            hash
        })
        .collect();
    decode_public_message::<v1::GetCommitResponse>(&response.encode_to_vec())
        .expect("4,096 conflict hashes");
    match response.result.as_mut().expect("result") {
        v1::get_commit_response::Result::Found(commit) => {
            commit.conflict_hashes.push(vec![0xaa; 32])
        }
        v1::get_commit_response::Result::NotFound(_) => unreachable!(),
    }
    assert_eq!(
        decode_public_message::<v1::GetCommitResponse>(&response.encode_to_vec()),
        Err(PublicWireError::PreflightLimitExceeded)
    );
}

#[test]
fn capability_mode_result_token_and_canonical_grant_are_checked() {
    let normal_request = create_request(v1::CapabilityCreateMode::Normal);
    let normal_response = v1::CreateCapabilityResponse {
        result: Some(v1::create_capability_response::Result::Normal(
            v1::NormalCreateCapabilityResult {
                result: Some(v1::normal_create_capability_result::Result::Created(
                    v1::NormalCapabilityCreated {
                        transition: Some(transition()),
                        token: "A".repeat(43),
                    },
                )),
            },
        )),
    };
    validate_create_capability_exchange(&normal_request, &normal_response).expect("normal result");

    let mut noncanonical_token = normal_response.clone();
    if let Some(v1::create_capability_response::Result::Normal(normal)) =
        noncanonical_token.result.as_mut()
        && let Some(v1::normal_create_capability_result::Result::Created(created)) =
            normal.result.as_mut()
    {
        created.token = format!("{}B", "A".repeat(42));
    }
    assert_eq!(
        validate_public_message(&noncanonical_token),
        Err(PublicWireError::InvalidBytes)
    );

    let bootstrap_response = v1::CreateCapabilityResponse {
        result: Some(v1::create_capability_response::Result::Bootstrap(
            v1::BootstrapCreateCapabilityResult {
                result: Some(v1::bootstrap_create_capability_result::Result::Created(
                    transition(),
                )),
            },
        )),
    };
    assert_eq!(
        validate_create_capability_exchange(&normal_request, &bootstrap_response),
        Err(PublicWireError::InconsistentFields)
    );

    let mut bootstrap_request = create_request(v1::CapabilityCreateMode::Bootstrap);
    bootstrap_request.grant.as_mut().expect("grant").permissions = vec![v1::CapabilityPermission {
        permission: Some(
            v1::capability_permission::Permission::AdministerCapabilities(v1::Unit {}),
        ),
    }];
    validate_create_capability_exchange(&bootstrap_request, &bootstrap_response)
        .expect("bootstrap human administrator");
    bootstrap_request.actor_kind = v1::ActorKind::Service as i32;
    assert_eq!(
        validate_public_message(&bootstrap_request),
        Err(PublicWireError::InconsistentFields)
    );

    let mut noncanonical = normal_request;
    noncanonical.audiences = vec!["z".to_owned(), "a".to_owned()];
    assert_eq!(
        validate_public_message(&noncanonical),
        Err(PublicWireError::NonCanonical)
    );
}

#[test]
fn capability_audiences_are_visible_ascii() {
    for audience in [" ", "\0", "\u{7f}"] {
        let mut request = create_request(v1::CapabilityCreateMode::Normal);
        request.audiences = vec![audience.to_owned()];
        assert_eq!(
            validate_public_message(&request),
            Err(PublicWireError::NonCanonical),
            "audience {audience:?} must fail closed"
        );
    }
}

#[test]
fn capability_grant_uses_the_complete_semantic_byte_bound() {
    let visibility = |lineage_bytes: usize, entity_type_id: u32| v1::EntityFieldVisibility {
        contract_lineage: "a".repeat(lineage_bytes),
        entity_type_id,
        field_ids: vec![1],
    };
    let mut exact = create_request(v1::CapabilityCreateMode::Normal);
    let fields = &mut exact.grant.as_mut().expect("grant").field_visibility;
    fields.extend((1..=20).map(|id| visibility(107, id)));
    fields.extend((1..=8_172).map(|id| visibility(108, id)));
    assert!(exact.encoded_len() <= 1_048_576);
    validate_public_message(&exact).expect("exact 1 MiB semantic grant");

    exact
        .grant
        .as_mut()
        .expect("grant")
        .field_visibility
        .last_mut()
        .expect("last visibility")
        .contract_lineage
        .push('a');
    assert!(exact.encoded_len() <= 1_048_576);
    assert_eq!(
        validate_public_message(&exact),
        Err(PublicWireError::TooManyItems)
    );
}

#[test]
fn exchange_validation_enforces_selected_contract_and_effective_page_limits() {
    let exact_contract = v1::ContractSelection {
        selection: Some(v1::contract_selection::Selection::Exact(
            v1::ExactContractSelection {
                contract_lineage: "budget".to_owned(),
                contract_version: 1,
            },
        )),
    };
    let mut explain_request = v1::ExplainCommandRequest {
        request_id: uuid_v7(),
        contract: Some(exact_contract),
        command_name: "reserve".to_owned(),
    };
    let explain_response = explained_command();
    validate_explain_command_exchange(&explain_request, &explain_response)
        .expect("matching exact contract");
    if let Some(v1::contract_selection::Selection::Exact(exact)) = explain_request
        .contract
        .as_mut()
        .and_then(|contract| contract.selection.as_mut())
    {
        exact.contract_version = 2;
    }
    assert_eq!(
        validate_explain_command_exchange(&explain_request, &explain_response),
        Err(PublicWireError::InconsistentFields)
    );

    let page = || v1::PageRequest {
        limit: Some(1),
        cursor: None,
    };
    let index_request = v1::ScanIndexRequest {
        request_id: uuid_v7(),
        contract: Some(active_contract()),
        index_id: 1,
        leading_components: Vec::new(),
        fields: Some(v1::FieldSelection { field_ids: vec![] }),
        page: Some(page()),
    };
    let index_response = v1::ScanIndexResponse {
        page: Some(v1::IndexPage {
            items: (0..2)
                .map(|ordinal| v1::IndexRow {
                    index_entry_key: index_key(1, ordinal),
                    values: Some(empty_record()),
                })
                .collect(),
            next_cursor: None,
            observed_fence: Some(v1::IndexScanFence {
                position: Some(v1::index_scan_fence::Position::AppliedEpoch(1)),
            }),
        }),
    };
    assert_eq!(
        validate_scan_index_exchange(&index_request, &index_response),
        Err(PublicWireError::InconsistentFields)
    );

    let projection_request = v1::QueryProjectionRequest {
        request_id: uuid_v7(),
        contract: Some(active_contract()),
        projection_id: 1,
        leading_components: Vec::new(),
        required_sequence: None,
        wait_nanos: 0,
        page: Some(page()),
    };
    let projection_response = v1::QueryProjectionResponse {
        result: Some(v1::query_projection_response::Result::Ready(
            v1::QueryProjectionReady {
                data: Some(v1::ProjectionPage {
                    items: vec![
                        v1::ProjectionRow {
                            group: Vec::new(),
                            values: Some(empty_record()),
                        },
                        v1::ProjectionRow {
                            group: Vec::new(),
                            values: Some(empty_record()),
                        },
                    ],
                    next_cursor: None,
                    observed_fence: Some(v1::ProjectionPageFence {
                        identity: Some(v1::ProjectionIdentity {
                            contract_lineage: "budget".to_owned(),
                            projection_id: 1,
                            projection_plan_hash: vec![0x55; 32],
                        }),
                        generation: 1,
                        frontier: Some(before_first()),
                    }),
                }),
                frontier: Some(before_first()),
            },
        )),
    };
    assert_eq!(
        validate_query_projection_exchange(&projection_request, &projection_response),
        Err(PublicWireError::InconsistentFields)
    );

    let commit_request = v1::ScanCommitsRequest {
        request_id: uuid_v7(),
        page: Some(page()),

        observed_history_incarnation: None,
    };
    let first = commit();
    let mut second = first.clone();
    second.commit_sequence = 2;
    let commit_response = v1::ScanCommitsResponse {
        page: Some(v1::CommitPage {
            items: vec![first, second],
            next_cursor: None,
            observed_fence: Some(applied(2)),

            history_incarnation: 1,
        }),
    };
    assert_eq!(
        validate_scan_commits_exchange(&commit_request, &commit_response),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn public_pages_and_commit_collections_preserve_canonical_order() {
    let mut commit = commit();
    commit.conflict_hashes = vec![vec![0x22; 32], vec![0x11; 32]];
    let mut response = v1::GetCommitResponse {
        result: Some(v1::get_commit_response::Result::Found(commit)),

        history_incarnation: 1,
    };
    assert_eq!(
        validate_public_message(&response),
        Err(PublicWireError::NonCanonical)
    );

    let found = match response.result.as_mut().expect("result") {
        v1::get_commit_response::Result::Found(commit) => commit,
        v1::get_commit_response::Result::NotFound(_) => unreachable!(),
    };
    found.conflict_hashes.clear();
    let mut high_key = entity_key(1);
    high_key.push(2);
    let mut low_key = entity_key(1);
    low_key.push(1);
    found.affected_entities = vec![
        v1::AffectedEntity {
            entity_key: high_key,
            entity_version: 1,
        },
        v1::AffectedEntity {
            entity_key: low_key,
            entity_version: 1,
        },
    ];
    assert_eq!(
        validate_public_message(&response),
        Err(PublicWireError::NonCanonical)
    );

    let duplicate_index_key = index_key(1, 0);
    let index = v1::ScanIndexResponse {
        page: Some(v1::IndexPage {
            items: vec![
                v1::IndexRow {
                    index_entry_key: duplicate_index_key.clone(),
                    values: Some(empty_record()),
                },
                v1::IndexRow {
                    index_entry_key: duplicate_index_key,
                    values: Some(empty_record()),
                },
            ],
            next_cursor: None,
            observed_fence: Some(v1::IndexScanFence {
                position: Some(v1::index_scan_fence::Position::AppliedEpoch(1)),
            }),
        }),
    };
    assert_eq!(
        validate_public_message(&index),
        Err(PublicWireError::NonCanonical)
    );
}

#[test]
fn index_scan_fence_is_closed_and_preserves_before_first() {
    let response = |position| v1::ScanIndexResponse {
        page: Some(v1::IndexPage {
            items: Vec::new(),
            next_cursor: None,
            observed_fence: Some(v1::IndexScanFence { position }),
        }),
    };

    assert!(
        validate_public_message(&response(Some(
            v1::index_scan_fence::Position::BeforeFirst(v1::Unit {}),
        )))
        .is_ok()
    );
    assert!(
        validate_public_message(&response(Some(
            v1::index_scan_fence::Position::AppliedEpoch(1),
        )))
        .is_ok()
    );
    assert_eq!(
        validate_public_message(&response(None)),
        Err(PublicWireError::MissingRequiredField)
    );
    assert_eq!(
        validate_public_message(&response(Some(
            v1::index_scan_fence::Position::AppliedEpoch(0),
        ))),
        Err(PublicWireError::InvalidIdentity)
    );
}

#[test]
fn index_and_event_route_pages_allow_empty_bounded_progress() {
    let cursor = Some(vec![0x55; 16]);
    let index = v1::ScanIndexResponse {
        page: Some(v1::IndexPage {
            items: Vec::new(),
            next_cursor: cursor.clone(),
            observed_fence: Some(v1::IndexScanFence {
                position: Some(v1::index_scan_fence::Position::AppliedEpoch(1)),
            }),
        }),
    };
    assert!(validate_public_message(&index).is_ok());

    let events = v1::ReplayEventsResponse {
        page: Some(v1::EventPage {
            items: Vec::new(),
            next_cursor: vec![0x44; 16],
            observed_upper: Some(v1::EventId {
                commit_sequence: 7,
                event_ordinal: 0,
            }),
            history_incarnation: 1,
        }),
    };
    assert!(validate_public_message(&events).is_ok());

    let projection = v1::QueryProjectionResponse {
        result: Some(v1::query_projection_response::Result::Ready(
            v1::QueryProjectionReady {
                data: Some(v1::ProjectionPage {
                    items: Vec::new(),
                    next_cursor: cursor.clone(),
                    observed_fence: Some(v1::ProjectionPageFence {
                        identity: Some(v1::ProjectionIdentity {
                            contract_lineage: "budget".to_owned(),
                            projection_id: 1,
                            projection_plan_hash: vec![0x55; 32],
                        }),
                        generation: 1,
                        frontier: Some(before_first()),
                    }),
                }),
                frontier: Some(before_first()),
            },
        )),
    };
    assert_eq!(
        validate_public_message(&projection),
        Err(PublicWireError::InconsistentFields)
    );

    let commits = v1::ScanCommitsResponse {
        page: Some(v1::CommitPage {
            items: Vec::new(),
            next_cursor: cursor,
            observed_fence: Some(applied(1)),

            history_incarnation: 1,
        }),
    };
    assert_eq!(
        validate_public_message(&commits),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn event_descriptors_and_pages_reject_cross_field_and_fence_substitution() {
    let descriptor = v1::DescribeEventResponse {
        result: Some(v1::describe_event_response::Result::Found(
            v1::EventDescriptor {
                contract_lineage: "budget".to_owned(),
                contract_version: 1,
                contract_bundle_hash: vec![0x11; 32],
                event_name: "BudgetAllocated".to_owned(),
                application_streamable: true,
                partition_fields: vec![v1::EventFieldDescriptor {
                    name: "organization_id".to_owned(),
                    value_type: "uuid".to_owned(),
                }],
                payload_fields: vec![v1::EventFieldDescriptor {
                    name: "organization_id".to_owned(),
                    value_type: "i64".to_owned(),
                }],
            },
        )),
    };
    assert_eq!(
        validate_public_message(&descriptor),
        Err(PublicWireError::InconsistentFields)
    );

    let timed_out_with_progress = v1::TailEventsResponse {
        page: Some(v1::EventPage {
            items: Vec::new(),
            next_cursor: vec![0x44; 16],
            observed_upper: Some(v1::EventId {
                commit_sequence: 7,
                event_ordinal: 0,
            }),
            history_incarnation: 1,
        }),
        wait_timed_out: true,
    };
    assert_eq!(
        validate_public_message(&timed_out_with_progress),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn capability_grant_order_uses_the_authoritative_length_framed_keys() {
    let scoped_permission = |lineage: &str| v1::CapabilityPermission {
        permission: Some(v1::capability_permission::Permission::ExplainCommand(
            v1::LineageScopedStableId {
                contract_lineage: lineage.to_owned(),
                stable_id: 1,
            },
        )),
    };
    let visibility = |lineage: &str| v1::EntityFieldVisibility {
        contract_lineage: lineage.to_owned(),
        entity_type_id: 1,
        field_ids: vec![1],
    };
    let mut request = create_request(v1::CapabilityCreateMode::Normal);
    let grant = request.grant.as_mut().expect("grant");
    grant.partition_scope = Some(v1::PartitionScope {
        scope: Some(v1::partition_scope::Scope::Explicit(
            v1::ExplicitPartitionScope {
                partitions: vec![
                    v1::ScopedPartition {
                        contract_lineage: "b".to_owned(),
                        partition_key: partition_key(1),
                    },
                    v1::ScopedPartition {
                        contract_lineage: "aa".to_owned(),
                        partition_key: partition_key(1),
                    },
                ],
            },
        )),
    });
    grant.permissions = vec![scoped_permission("b"), scoped_permission("aa")];
    grant.field_visibility = vec![visibility("b"), visibility("aa")];
    validate_public_message(&request).expect("length-framed canonical order");

    request.grant.as_mut().expect("grant").permissions.reverse();
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::NonCanonical)
    );

    let grant = request.grant.as_mut().expect("grant");
    grant.permissions.reverse();
    let explicit = match grant
        .partition_scope
        .as_mut()
        .and_then(|scope| scope.scope.as_mut())
        .expect("scope")
    {
        v1::partition_scope::Scope::Explicit(explicit) => explicit,
        v1::partition_scope::Scope::All(_) => unreachable!(),
    };
    explicit.partitions = vec![
        v1::ScopedPartition {
            contract_lineage: "b".to_owned(),
            partition_key: {
                let mut key = partition_key(1);
                key[5] = 0xff;
                key
            },
        },
        v1::ScopedPartition {
            contract_lineage: "b".to_owned(),
            partition_key: {
                let mut key = partition_key(1);
                key.push(0);
                key
            },
        },
    ];
    validate_public_message(&request).expect("shorter framed key sorts first");
}

#[test]
fn named_query_permissions_require_exact_bounded_module_and_symbol_identity() {
    let mut request = create_request(v1::CapabilityCreateMode::Normal);
    request.grant.as_mut().expect("grant").permissions = vec![v1::CapabilityPermission {
        permission: Some(v1::capability_permission::Permission::ExecuteNamedQuery(
            v1::NamedQueryPermission {
                contract_lineage: "ticketdesk".to_owned(),
                query_module_hash: vec![0x5a; 32],
                query_name: "TicketPage".to_owned(),
            },
        )),
    }];
    validate_public_message(&request).expect("exact named query permission");

    let v1::capability_permission::Permission::ExecuteNamedQuery(permission) =
        request.grant.as_mut().expect("grant").permissions[0]
            .permission
            .as_mut()
            .expect("permission")
    else {
        panic!("named permission");
    };
    permission.query_module_hash.pop();
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::InvalidIdentity)
    );
}

#[test]
fn health_and_subscription_closed_bounds_are_checked() {
    let pre_bootstrap = v1::HealthResponse {
        result: Some(v1::health_response::Result::PreBootstrap(
            v1::PreBootstrapHealth {
                lifecycle: v1::PreBootstrapLifecycle::InitializingValidation as i32,
                liveness: true,
                readiness: false,
            },
        )),
        database_alias: "default".to_owned(),
        authentication_audience: "riffdb-grpc-loopback".to_owned(),
    };
    validate_public_message(&pre_bootstrap).expect("pre-bootstrap shape");
    let mut invalid = pre_bootstrap;
    match invalid.result.as_mut().expect("result") {
        v1::health_response::Result::PreBootstrap(health) => health.readiness = true,
        v1::health_response::Result::Authenticated(_) => unreachable!(),
    }
    assert_eq!(
        validate_public_message(&invalid),
        Err(PublicWireError::InconsistentFields)
    );

    let mut subscribe = v1::SubscribeCommitsRequest {
        request_id: uuid_v7(),
        after_sequence: None,
        maximum_lifetime_nanos: 900_000_000_000,

        observed_history_incarnation: None,
    };
    validate_public_message(&subscribe).expect("maximum subscription lifetime");
    subscribe.maximum_lifetime_nanos += 1;
    assert_eq!(
        validate_public_message(&subscribe),
        Err(PublicWireError::InvalidIdentity)
    );
}

#[test]
fn offline_maintenance_identity_phase_and_exchange_are_closed() {
    let name = BackupNameV1::new("before-upgrade").expect("name");
    let operation = v1::OfflineMaintenanceOperation {
        operation_id: uuid_v7(),
        kind: v1::OfflineMaintenanceOperationKind::CreateBackup as i32,
        backup_name: name.as_str().to_owned(),
        input_hash: offline_maintenance_input_hash(
            OfflineMaintenanceOperationKind::CreateBackup,
            &name,
            OfflineMaintenanceReplacementConfirmation::NotProvided,
        )
        .into_bytes()
        .to_vec(),
        phase: v1::OfflineMaintenancePhase::Accepted as i32,
        failure: v1::OfflineMaintenanceFailureClass::Unspecified as i32,
    };
    let request = v1::CreateOfflineBackupRequest {
        request_id: uuid_v7(),
        operation_id: operation.operation_id.clone(),
        backup_name: operation.backup_name.clone(),
    };
    let response = v1::CreateOfflineBackupResponse {
        disposition: v1::OfflineMaintenanceStartDisposition::Accepted as i32,
        operation: Some(operation.clone()),
    };
    validate_create_offline_backup_exchange(&request, &response).expect("exact create exchange");

    let mut terminal_nonterminal = response.clone();
    terminal_nonterminal.disposition = v1::OfflineMaintenanceStartDisposition::Terminal as i32;
    assert_eq!(
        validate_public_message(&terminal_nonterminal),
        Err(PublicWireError::InconsistentFields)
    );

    let mut failed_without_class = operation.clone();
    failed_without_class.phase = v1::OfflineMaintenancePhase::FailedClosed as i32;
    assert_eq!(
        validate_public_message(&v1::CreateOfflineBackupResponse {
            disposition: v1::OfflineMaintenanceStartDisposition::Terminal as i32,
            operation: Some(failed_without_class),
        }),
        Err(PublicWireError::InconsistentFields)
    );

    for invalid_name in [".maintenance", "../backup", "Upper", ""] {
        let mut invalid = request.clone();
        invalid.backup_name = invalid_name.to_owned();
        assert_eq!(
            validate_public_message(&invalid),
            Err(PublicWireError::InvalidIdentity)
        );
    }

    let restore_request = v1::RestoreOfflineBackupRequest {
        request_id: uuid_v7(),
        operation_id: uuid_v7(),
        backup_name: name.as_str().to_owned(),
        replacement_confirmation:
            v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
    };
    let restore_operation = v1::OfflineMaintenanceOperation {
        operation_id: restore_request.operation_id.clone(),
        kind: v1::OfflineMaintenanceOperationKind::RestoreBackup as i32,
        backup_name: name.as_str().to_owned(),
        input_hash: offline_maintenance_input_hash(
            OfflineMaintenanceOperationKind::RestoreBackup,
            &name,
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
        )
        .into_bytes()
        .to_vec(),
        phase: v1::OfflineMaintenancePhase::Succeeded as i32,
        failure: v1::OfflineMaintenanceFailureClass::Unspecified as i32,
    };
    validate_restore_offline_backup_exchange(
        &restore_request,
        &v1::RestoreOfflineBackupResponse {
            disposition: v1::OfflineMaintenanceStartDisposition::Terminal as i32,
            operation: Some(restore_operation),
        },
    )
    .expect("exact restore exchange");

    let poll_request = v1::GetOfflineMaintenanceOperationRequest {
        request_id: uuid_v7(),
        operation_id: uuid_v7(),
    };
    validate_get_offline_maintenance_operation_exchange(
        &poll_request,
        &v1::GetOfflineMaintenanceOperationResponse {
            result: Some(
                v1::get_offline_maintenance_operation_response::Result::NotFound(v1::Unit {}),
            ),
        },
    )
    .expect("not-found carries no operation data");
    let mut mismatched = operation;
    mismatched.operation_id[15] ^= 1;
    assert_eq!(
        validate_get_offline_maintenance_operation_exchange(
            &poll_request,
            &v1::GetOfflineMaintenanceOperationResponse {
                result: Some(
                    v1::get_offline_maintenance_operation_response::Result::Found(mismatched),
                ),
            },
        ),
        Err(PublicWireError::InconsistentFields)
    );
}

fn batch_read_only_response() -> v1::ExecuteCommandResponse {
    v1::ExecuteCommandResponse {
        status: v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32,
        commit_sequence: 0,
        contract_version: 1,
        plan_hash: vec![0x44; 32],
        outcome_type: "Balance".to_owned(),
        outcome: Some(null_value()),
        provenance_uri: String::new(),
        durability_mode: String::new(),
        outcome_uri: None,
        history_incarnation: 1,
    }
}

fn batch_application_error(code: ApplicationErrorCode) -> app_v1::ApplicationError {
    application_error_to_proto(&ApplicationError::new(
        code,
        ApplicationOperation::BatchCommand,
        ApplicationErrorContext::empty(),
        None,
    ))
}

fn batch_item_response(response: v1::ExecuteCommandResponse) -> v1::ExecuteCommandBatchItem {
    v1::ExecuteCommandBatchItem {
        result: Some(v1::execute_command_batch_item::Result::Response(response)),
    }
}

fn batch_item_error(code: ApplicationErrorCode) -> v1::ExecuteCommandBatchItem {
    v1::ExecuteCommandBatchItem {
        result: Some(v1::execute_command_batch_item::Result::Error(
            batch_application_error(code),
        )),
    }
}

fn batch_execute_request(count: usize) -> v1::ExecuteCommandBatchRequest {
    let mut commands = Vec::with_capacity(count);
    for index in 0..count {
        let mut request_id = uuid_v7();
        request_id[15] = u8::try_from(index).expect("batch fixture fits u8");
        commands.push(v1::ExecuteCommandRequest {
            request_id,
            command_name: "CreateTicket".to_owned(),
            expected_contract_version: None,
            input: Some(null_value()),
        });
    }
    v1::ExecuteCommandBatchRequest { commands }
}

#[test]
fn batch_all_success_response_has_consistent_legacy_and_item_fields() {
    let response_a = batch_read_only_response();
    let mut response_b = batch_read_only_response();
    response_b.plan_hash = vec![0x55; 32];
    let message = v1::ExecuteCommandBatchResponse {
        responses: vec![response_a.clone(), response_b.clone()],
        items: vec![
            batch_item_response(response_a.clone()),
            batch_item_response(response_b.clone()),
        ],
    };
    validate_public_message(&message).expect("all-success batch");
    assert_eq!(
        message.responses[0],
        match message.items[0].result.as_ref().expect("set") {
            v1::execute_command_batch_item::Result::Response(response) => response.clone(),
            _ => panic!("response arm"),
        }
    );
    assert_eq!(
        message.responses[1],
        match message.items[1].result.as_ref().expect("set") {
            v1::execute_command_batch_item::Result::Response(response) => response.clone(),
            _ => panic!("response arm"),
        }
    );
    validate_execute_command_batch_exchange(&batch_execute_request(2), &message)
        .expect("items primary exchange");
}

#[test]
fn batch_mixed_response_keeps_legacy_field_empty() {
    let success = batch_read_only_response();
    let message = v1::ExecuteCommandBatchResponse {
        responses: Vec::new(),
        items: vec![
            batch_item_response(success),
            batch_item_error(ApplicationErrorCode::Overloaded),
        ],
    };
    validate_public_message(&message).expect("mixed batch with empty field 1");
    validate_execute_command_batch_exchange(&batch_execute_request(2), &message)
        .expect("items-length exchange");
}

#[test]
fn batch_validate_structure_rejects_partial_legacy_field_and_length_mismatch() {
    let success = batch_read_only_response();
    let partial = v1::ExecuteCommandBatchResponse {
        responses: vec![success.clone()],
        items: vec![
            batch_item_response(success.clone()),
            batch_item_error(ApplicationErrorCode::InputInvalid),
        ],
    };
    assert_eq!(
        validate_public_message(&partial),
        Err(PublicWireError::InconsistentFields)
    );

    let length_mismatch = v1::ExecuteCommandBatchResponse {
        responses: vec![success.clone()],
        items: vec![
            batch_item_response(success.clone()),
            batch_item_response(success),
        ],
    };
    assert_eq!(
        validate_public_message(&length_mismatch),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn batch_preflight_rejects_unset_oneof_double_arm_and_overflow() {
    // Unset oneof via hand-built wire: empty nested item under field 2.
    let mut unset_bytes = Vec::new();
    // field 2 (items), empty length-delimited message
    unset_bytes.push(0x12);
    prost::encoding::encode_varint(0, &mut unset_bytes);
    assert_eq!(
        decode_public_message::<v1::ExecuteCommandBatchResponse>(&unset_bytes),
        Err(PublicWireError::MissingRequiredField)
    );

    // Double-set oneof on the wire (both field 1 and field 2 present).
    let response_bytes = batch_read_only_response().encode_to_vec();
    let error_bytes = batch_application_error(ApplicationErrorCode::Overloaded).encode_to_vec();
    let mut item_bytes = Vec::new();
    // field 1 (response), length-delimited
    item_bytes.push(0x0a);
    prost::encoding::encode_varint(response_bytes.len() as u64, &mut item_bytes);
    item_bytes.extend_from_slice(&response_bytes);
    // field 2 (error), length-delimited
    item_bytes.push(0x12);
    prost::encoding::encode_varint(error_bytes.len() as u64, &mut item_bytes);
    item_bytes.extend_from_slice(&error_bytes);
    let mut batch_bytes = Vec::new();
    // field 2 (items)
    batch_bytes.push(0x12);
    prost::encoding::encode_varint(item_bytes.len() as u64, &mut batch_bytes);
    batch_bytes.extend_from_slice(&item_bytes);
    assert_eq!(
        decode_public_message::<v1::ExecuteCommandBatchResponse>(&batch_bytes),
        Err(PublicWireError::MalformedEncoding)
    );

    // More than 16 items with both fields populated.
    let too_many = v1::ExecuteCommandBatchResponse {
        responses: (0..17).map(|_| batch_read_only_response()).collect(),
        items: (0..17)
            .map(|_| batch_item_response(batch_read_only_response()))
            .collect(),
    };
    assert_eq!(
        validate_public_message(&too_many),
        Err(PublicWireError::TooManyItems)
    );
    let encoded_overflow = too_many.encode_to_vec();
    assert_eq!(
        decode_public_message::<v1::ExecuteCommandBatchResponse>(&encoded_overflow),
        Err(PublicWireError::PreflightLimitExceeded)
    );

    // More than 16 items with only the items field populated (mixed-error shape).
    let items_only_overflow = v1::ExecuteCommandBatchResponse {
        responses: Vec::new(),
        items: (0..17)
            .map(|index| {
                if index == 0 {
                    batch_item_error(ApplicationErrorCode::Overloaded)
                } else {
                    batch_item_response(batch_read_only_response())
                }
            })
            .collect(),
    };
    assert_eq!(
        validate_public_message(&items_only_overflow),
        Err(PublicWireError::TooManyItems)
    );
    assert_eq!(
        decode_public_message::<v1::ExecuteCommandBatchResponse>(
            &items_only_overflow.encode_to_vec()
        ),
        Err(PublicWireError::PreflightLimitExceeded)
    );
}

#[test]
fn batch_exchange_validator_items_primary_and_legacy_fallback() {
    let success = batch_read_only_response();
    let with_items = v1::ExecuteCommandBatchResponse {
        responses: vec![success.clone(), success.clone()],
        items: vec![
            batch_item_response(success.clone()),
            batch_item_response(success.clone()),
        ],
    };
    validate_execute_command_batch_exchange(&batch_execute_request(2), &with_items)
        .expect("items primary");

    // When items is present, mismatched command count fails even if responses match.
    assert_eq!(
        validate_execute_command_batch_exchange(&batch_execute_request(1), &with_items),
        Err(PublicWireError::InconsistentFields)
    );

    // Legacy items-absent path uses responses length.
    let legacy = v1::ExecuteCommandBatchResponse {
        responses: vec![success.clone(), success],
        items: Vec::new(),
    };
    validate_public_message(&legacy).expect("legacy success rows");
    validate_execute_command_batch_exchange(&batch_execute_request(2), &legacy)
        .expect("legacy fallback");
    assert_eq!(
        validate_execute_command_batch_exchange(&batch_execute_request(1), &legacy),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn contextual_reaction_requires_one_exact_request_identity() {
    let mut request = v1::ExecuteContextualReactionRequest {
        request_id: uuid_v7(),
        selection: Some(v1::EventConsumerSelection {
            reactive_module_hash: vec![0x21; 32],
            operation_name: "TriageTicket".to_owned(),
            parameters: Vec::new(),
            consumer_name: "triage-worker".to_owned(),
        }),
        causation_token: vec![0x31; 33],
        reaction_name: "assign".to_owned(),
        command: Some(v1::ExecuteCommandRequest {
            request_id: uuid_v7(),
            command_name: "AssignTicket".to_owned(),
            expected_contract_version: Some(1),
            input: Some(v1::Value {
                kind: Some(v1::value::Kind::RecordValue(empty_record())),
            }),
        }),
    };
    validate_public_message(&request).expect("matching request identities");

    request.command.as_mut().expect("command").request_id[15] ^= 1;
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::InconsistentFields)
    );
}
