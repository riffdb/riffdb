//! Structural conformance for the completed phase-zero public messages.

use prost::Message;
use riffdb_proto::{
    PublicWireError, decode_public_message, v1, validate_contract_validation_exchange,
    validate_create_capability_exchange, validate_explain_command_exchange,
    validate_public_message, validate_query_projection_exchange, validate_scan_commits_exchange,
    validate_scan_index_exchange,
};
use riffdb_types::hash_schema;

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
    };
    let first = commit();
    let mut second = first.clone();
    second.commit_sequence = 2;
    let commit_response = v1::ScanCommitsResponse {
        page: Some(v1::CommitPage {
            items: vec![first, second],
            next_cursor: None,
            observed_fence: Some(applied(2)),
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
fn health_and_subscription_closed_bounds_are_checked() {
    let pre_bootstrap = v1::HealthResponse {
        result: Some(v1::health_response::Result::PreBootstrap(
            v1::PreBootstrapHealth {
                lifecycle: v1::PreBootstrapLifecycle::InitializingValidation as i32,
                liveness: true,
                readiness: false,
            },
        )),
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
    };
    validate_public_message(&subscribe).expect("maximum subscription lifetime");
    subscribe.maximum_lifetime_nanos += 1;
    assert_eq!(
        validate_public_message(&subscribe),
        Err(PublicWireError::InvalidIdentity)
    );
}
