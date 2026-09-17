//! Structural conformance for the completed phase-zero public messages.

use prost::Message;
use riffdb_errors::{
    ApplicationError, ApplicationErrorCode, ApplicationErrorContext, ApplicationOperation,
};
use riffdb_proto::{
    PublicWireError, app::v1 as app_v1, application_error_to_proto, decode_public_message, v1,
    validate_contract_validation_exchange, validate_create_capability_exchange,
    validate_create_offline_backup_exchange, validate_execute_command_batch_exchange,
    validate_explain_command_exchange, validate_get_application_installation_exchange,
    validate_get_offline_maintenance_operation_exchange, validate_public_message,
    validate_query_projection_exchange, validate_restore_offline_backup_exchange,
    validate_scan_commits_exchange, validate_scan_index_exchange,
    validate_start_application_installation_exchange,
};
use riffdb_types::{
    BackupNameV1, OfflineMaintenanceOperationKind, OfflineMaintenanceReplacementConfirmation,
    hash_application_installation_plan, hash_application_installation_receipt, hash_schema,
    offline_maintenance_input_hash,
};

fn uuid_v7() -> Vec<u8> {
    vec![
        0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb, 0xbd,
        0x23,
    ]
}

#[test]
// req: REP-004
fn replication_statistics_wire_preserves_unknown_progress_and_rejects_false_lag() {
    let position = |value| v1::FrontierPosition {
        position: Some(if value == 0 {
            v1::frontier_position::Position::BeforeFirst(v1::Unit {})
        } else {
            v1::frontier_position::Position::AppliedThrough(value)
        }),
    };
    let pair = |app, admin| v1::ReplicationFrontier {
        application: Some(position(app)),
        administration: Some(position(admin)),
    };
    let unknown = v1::ReplicationStatistics {
        role: v1::ReplicationRole::Follower as i32,
        applied_frontier: Some(pair(10, 3)),
        ..Default::default()
    };
    let lagged = v1::ReplicationStatistics {
        source_frontier: Some(pair(12, 4)),
        acknowledged_frontier: Some(pair(8, 2)),
        application_lag_sequences: Some(2),
        administration_lag_sequences: Some(1),
        ..unknown
    };
    let caught_up = v1::ReplicationStatistics {
        source_frontier: Some(pair(10, 3)),
        acknowledged_frontier: Some(pair(10, 3)),
        application_lag_sequences: Some(0),
        administration_lag_sequences: Some(0),
        ..unknown
    };
    let primary = v1::ReplicationStatistics {
        role: v1::ReplicationRole::Primary as i32,
        source_frontier: Some(pair(10, 3)),
        acknowledged_frontier: Some(pair(8, 2)),
        registered_followers: Some(1),
        application_lag_sequences: Some(2),
        administration_lag_sequences: Some(1),
        ..unknown
    };
    let no_followers = v1::ReplicationStatistics {
        acknowledged_frontier: None,
        registered_followers: Some(0),
        application_lag_sequences: None,
        administration_lag_sequences: None,
        ..primary
    };
    let before_first = v1::ReplicationStatistics {
        applied_frontier: Some(pair(0, 0)),
        source_frontier: Some(pair(u64::MAX, u64::MAX)),
        application_lag_sequences: Some(u64::MAX),
        administration_lag_sequences: Some(u64::MAX),
        ..unknown
    };
    let response = |replication| v1::StatsResponse {
        history_incarnation: 1,
        replication,
        ..Default::default()
    };
    for progress in [
        None,
        Some(unknown),
        Some(lagged),
        Some(caught_up),
        Some(primary),
        Some(no_followers),
        Some(before_first),
    ] {
        let value = response(progress);
        assert_eq!(
            decode_public_message::<v1::StatsResponse>(&value.encode_to_vec()),
            Ok(value)
        );
    }
    for invalid in [
        v1::ReplicationStatistics { role: 0, ..unknown },
        v1::ReplicationStatistics {
            registered_followers: Some(0),
            ..unknown
        },
        v1::ReplicationStatistics {
            application_lag_sequences: Some(0),
            ..unknown
        },
        v1::ReplicationStatistics {
            applied_frontier: None,
            ..unknown
        },
        v1::ReplicationStatistics {
            applied_frontier: Some(v1::ReplicationFrontier {
                administration: None,
                ..pair(10, 3)
            }),
            ..unknown
        },
        v1::ReplicationStatistics {
            acknowledged_frontier: Some(pair(11, 2)),
            ..unknown
        },
        v1::ReplicationStatistics {
            source_frontier: Some(pair(9, 4)),
            ..lagged
        },
        v1::ReplicationStatistics {
            application_lag_sequences: Some(1),
            ..lagged
        },
        v1::ReplicationStatistics {
            registered_followers: None,
            ..primary
        },
        v1::ReplicationStatistics {
            acknowledged_frontier: None,
            ..primary
        },
        v1::ReplicationStatistics {
            registered_followers: Some(0),
            ..primary
        },
    ] {
        assert!(
            decode_public_message::<v1::StatsResponse>(&response(Some(invalid)).encode_to_vec())
                .is_err()
        );
    }
    let wrap = |nested: &[u8]| {
        let mut encoded = response(None).encode_to_vec();
        encoded.push(0x3a); // stats field7
        prost::encoding::encode_varint(nested.len().try_into().unwrap(), &mut encoded);
        encoded.extend_from_slice(nested);
        encoded
    };
    let mut duplicate = lagged.encode_to_vec();
    duplicate.extend_from_slice(&[0x08, 0x02]); // repeated known role
    assert!(decode_public_message::<v1::StatsResponse>(&wrap(&duplicate)).is_err());
    let mut oversized = lagged.encode_to_vec();
    oversized.extend_from_slice(&[0x42, 0x80, 0x04]); // unknown field8,512 bytes
    oversized.extend_from_slice(&[0; 512]);
    assert_eq!(
        decode_public_message::<v1::StatsResponse>(&wrap(&oversized)),
        Err(PublicWireError::PreflightLimitExceeded)
    );
}

#[test]
// req: REP-003
fn replication_wire_binds_the_complete_position_and_refuses_malformed_or_oversized_items() {
    let before = || {
        Some(v1::FrontierPosition {
            position: Some(v1::frontier_position::Position::BeforeFirst(v1::Unit {})),
        })
    };
    let request = v1::StreamChangelogRequest {
        request_id: uuid_v7(),
        database_id: uuid_v7(),
        history_incarnation: 1,
        leadership_epoch: 1,
        readable_format: "riffdb.changelog-frame/v3".into(),
        catalog_digest: vec![0x76; 32],
        after: Some(v1::ReplicationPosition {
            transaction_sequence: 1,
            history_hash: vec![0x77; 32],
            application_frontier: before(),
            administration_frontier: before(),
        }),
        maximum_frame_bytes: 32 * 1024 * 1024,
        maximum_transitions: 256,
        bootstrap: None,
        attachment: None,
        follower_hold_id: vec![],
    };
    assert_eq!(
        decode_public_message::<v1::StreamChangelogRequest>(&request.encode_to_vec()).unwrap(),
        request
    );
    // The optional ID turns only an exact tail position into a durable follower
    // claim. Unknown additive fields remain ignored; duplicate known IDs fail.
    let mut follower = request.clone();
    follower.follower_hold_id = vec![0; 16];
    follower.follower_hold_id[15] = 1;
    assert_eq!(
        decode_public_message::<v1::StreamChangelogRequest>(&follower.encode_to_vec()).unwrap(),
        follower
    );
    for id in [vec![0; 16], vec![1; 15], vec![1; 17]] {
        let mut invalid = follower.clone();
        invalid.follower_hold_id = id;
        assert_eq!(
            validate_public_message(&invalid),
            Err(PublicWireError::InvalidIdentity)
        );
    }
    for attachment in [false, true] {
        let mut invalid = follower.clone();
        let acknowledged = invalid.after.take();
        if attachment {
            invalid.attachment = Some(v1::ReplicationBootstrapAttachment {
                manifest: vec![1; 512],
                acknowledged,
            });
        } else {
            invalid.bootstrap = Some(v1::ReplicationBootstrapRequest {
                hold_id: vec![1; 16],
                resume_manifest: vec![],
                after_page: 0,
            });
        }
        assert_eq!(
            validate_public_message(&invalid),
            Err(PublicWireError::InvalidIdentity)
        );
    }
    let mut duplicate = follower.encode_to_vec();
    duplicate.extend_from_slice(&[0x62, 16]); // field 12, bytes
    duplicate.extend_from_slice(&[1; 16]);
    assert_eq!(
        decode_public_message::<v1::StreamChangelogRequest>(&duplicate),
        Err(PublicWireError::MalformedEncoding)
    );
    let mut unknown = follower.encode_to_vec();
    unknown.extend_from_slice(&[0x6a, 1, 1]); // field 13, bytes
    assert_eq!(
        decode_public_message::<v1::StreamChangelogRequest>(&unknown).unwrap(),
        follower
    );
    for fault in 0..6 {
        let mut malformed = request.clone();
        match fault {
            0 => malformed.leadership_epoch = 0,
            1 => malformed.catalog_digest.pop().map(|_| ()).unwrap(),
            2 => malformed.after.as_mut().unwrap().transaction_sequence = 0,
            3 => malformed.after.as_mut().unwrap().history_hash.clear(),
            4 => malformed.after.as_mut().unwrap().administration_frontier = None,
            _ => malformed.readable_format = "x".repeat(129),
        }
        assert!(
            decode_public_message::<v1::StreamChangelogRequest>(&malformed.encode_to_vec())
                .is_err()
        );
    }
    for code in 1..=10 {
        let response = v1::StreamChangelogResponse {
            source_head: None,
            item: Some(v1::stream_changelog_response::Item::Refusal(code)),
        };
        assert_eq!(
            decode_public_message::<v1::StreamChangelogResponse>(&response.encode_to_vec())
                .unwrap(),
            response
        );
    }
    for code in [0, 11, -1] {
        assert!(
            validate_public_message(&v1::StreamChangelogResponse {
                source_head: None,
                item: Some(v1::stream_changelog_response::Item::Refusal(code)),
            })
            .is_err()
        );
    }
    let oversized = v1::StreamChangelogResponse {
        source_head: None,
        item: Some(v1::stream_changelog_response::Item::Frame(vec![
            0;
            32 * 1024
                * 1024
                + 1
        ])),
    };
    assert!(validate_public_message(&oversized).is_err());
}

#[test]
// req: REP-004
fn replication_source_head_is_optional_bounded_and_only_valid_with_a_frame() {
    let position = |value| {
        Some(v1::FrontierPosition {
            position: Some(v1::frontier_position::Position::AppliedThrough(value)),
        })
    };
    let head = v1::ReplicationSourceHead {
        transaction_sequence: 9,
        application_frontier: position(5),
        administration_frontier: position(2),
    };
    let original = v1::StreamChangelogResponse {
        item: Some(v1::stream_changelog_response::Item::Frame(vec![1, 2, 3])),
        source_head: Some(head),
    };
    assert_eq!(
        decode_public_message::<v1::StreamChangelogResponse>(&original.encode_to_vec()).unwrap(),
        original
    );
    let mut absent = original.clone();
    absent.source_head = None;
    assert_eq!(
        decode_public_message::<v1::StreamChangelogResponse>(&absent.encode_to_vec()).unwrap(),
        absent
    );
    for fault in 0..5 {
        let mut changed = original.clone();
        match fault {
            0 => changed.source_head.as_mut().unwrap().transaction_sequence = 0,
            1 => changed.source_head.as_mut().unwrap().application_frontier = None,
            2 => {
                changed
                    .source_head
                    .as_mut()
                    .unwrap()
                    .administration_frontier = position(0)
            }
            3 => {
                changed.item = Some(v1::stream_changelog_response::Item::Refusal(
                    v1::ReplicationRefusal::Unavailable as i32,
                ))
            }
            _ => changed.item = Some(v1::stream_changelog_response::Item::BootstrapPage(vec![1])),
        }
        assert!(
            decode_public_message::<v1::StreamChangelogResponse>(&changed.encode_to_vec()).is_err()
        );
    }
    let mut duplicate_head = head.encode_to_vec();
    duplicate_head.extend_from_slice(&[8, 9]); // repeat nested transaction_sequence
    let mut encoded = absent.encode_to_vec();
    encoded.extend_from_slice(&[42, u8::try_from(duplicate_head.len()).unwrap()]);
    encoded.extend_from_slice(&duplicate_head);
    assert_eq!(
        decode_public_message::<v1::StreamChangelogResponse>(&encoded),
        Err(PublicWireError::MalformedEncoding)
    );
    let mut unknown = head.encode_to_vec();
    unknown.extend_from_slice(&[32, 1]); // unknown nested field 4 is ignored
    let mut encoded = absent.encode_to_vec();
    encoded.extend_from_slice(&[42, u8::try_from(unknown.len()).unwrap()]);
    encoded.extend_from_slice(&unknown);
    assert_eq!(
        decode_public_message::<v1::StreamChangelogResponse>(&encoded).unwrap(),
        original
    );
    let mut bounded_head = head.encode_to_vec();
    let padding = 128 - bounded_head.len() - 2;
    bounded_head.extend_from_slice(&[34, u8::try_from(padding).unwrap()]);
    bounded_head.resize(128, 0);
    let mut encoded = absent.encode_to_vec();
    encoded.extend_from_slice(&[42, 0x80, 1]); // field 5, length 128
    encoded.extend_from_slice(&bounded_head);
    assert_eq!(
        decode_public_message::<v1::StreamChangelogResponse>(&encoded).unwrap(),
        original
    );
    bounded_head.extend_from_slice(&[40, 0]); // one extra unknown field exceeds the head bound
    let mut encoded = absent.encode_to_vec();
    encoded.extend_from_slice(&[42, 0x82, 1]); // length 130
    encoded.extend_from_slice(&bounded_head);
    assert!(decode_public_message::<v1::StreamChangelogResponse>(&encoded).is_err());

    let maximum_frame = v1::StreamChangelogResponse {
        item: Some(v1::stream_changelog_response::Item::Frame(vec![
            7;
            32 * 1024
                * 1024
        ])),
        source_head: Some(head),
    };
    assert_eq!(
        decode_public_message::<v1::StreamChangelogResponse>(&maximum_frame.encode_to_vec())
            .unwrap(),
        maximum_frame
    );
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
        row_policy: None,
        export: None,
        reimport: None,
        vector_inspection: None,
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

#[test]
// req: REP-003
fn replication_grant_refuses_bootstrap_roles_and_partial_database_scope() {
    use v1::capability_permission::Permission;
    let mut request = create_request(v1::CapabilityCreateMode::Normal);
    request.grant.as_mut().expect("grant").permissions = vec![
        v1::CapabilityPermission {
            permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
        },
        v1::CapabilityPermission {
            permission: Some(Permission::ReplicateChangelog(v1::Unit {})),
        },
    ];
    let encoded = request.encode_to_vec();
    assert_eq!(
        decode_public_message::<v1::CreateCapabilityRequest>(&encoded),
        Ok(request.clone())
    );
    let mut bootstrap = request.clone();
    bootstrap.mode = v1::CapabilityCreateMode::Bootstrap as i32;
    assert_eq!(
        validate_public_message(&bootstrap),
        Err(PublicWireError::InconsistentFields)
    );
    let mut role = request.clone();
    role.grant.as_mut().expect("grant").permissions.insert(
        1,
        v1::CapabilityPermission {
            permission: Some(Permission::ApplicationRoleIdentity(vec![0x42; 32])),
        },
    );
    assert_eq!(
        validate_public_message(&role),
        Err(PublicWireError::InconsistentFields)
    );
    request.grant.as_mut().expect("grant").tenant_scope = Some(v1::TenantScope {
        scope: Some(v1::tenant_scope::Scope::TenantId("one-org".to_owned())),
    });
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn export_capability_request_is_strict_and_unknown_scope_is_rejected() {
    let mut request = create_request(v1::CapabilityCreateMode::Normal);
    request.grant.as_mut().expect("grant").export = Some(v1::CapabilityExportGrant {
        applications: vec![v1::CapabilityApplicationExportGrant {
            contract_lineage: "TicketDesk".to_owned(),
            scope: v1::CapabilityApplicationExportScope::WholeApplication as i32,
            entities: true,
            events: true,
            provenance: false,
            public_audit: true,
        }],
    });
    validate_public_message(&request).expect("export capability request");
    decode_public_message::<v1::CreateCapabilityRequest>(&request.encode_to_vec())
        .expect("export capability wire request");

    request
        .grant
        .as_mut()
        .expect("grant")
        .export
        .as_mut()
        .expect("export")
        .applications[0]
        .scope = i32::MAX;
    assert_eq!(
        validate_public_message(&request),
        Err(riffdb_proto::PublicWireError::InvalidEnum)
    );

    let grant = request.grant.as_mut().expect("grant");
    grant.export.as_mut().expect("export").applications[0].scope =
        v1::CapabilityApplicationExportScope::WholeApplication as i32;
    grant.row_policy = Some(v1::CapabilityRowPolicyGrant {
        application_role_hash: vec![0x71; 32],
        principal_facts: Vec::new(),
        policies: vec![v1::CapabilityRowPolicyBinding {
            contract_lineage: "TicketDesk".to_owned(),
            policy_name: "VisibleTicket".to_owned(),
            entity_type_id: 1,
            operations: vec![v1::CapabilityRowPolicyOperation::Read as i32],
        }],
    });
    decode_public_message::<v1::CreateCapabilityRequest>(&request.encode_to_vec())
        .expect("row policy plus export wire request");
    request
        .grant
        .as_mut()
        .expect("grant")
        .row_policy
        .as_mut()
        .expect("row policy")
        .policies[0]
        .operations[0] = i32::MAX;
    assert_eq!(
        validate_public_message(&request),
        Err(riffdb_proto::PublicWireError::InvalidEnum)
    );
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
fn capability_scan_authority_uses_the_shared_application_query_ceiling() {
    for maximum in [500, 501, 50_000, 65_535] {
        let mut request = create_request(v1::CapabilityCreateMode::Normal);
        request.grant.as_mut().expect("grant").max_scan_rows = maximum;
        validate_public_message(&request)
            .unwrap_or_else(|error| panic!("scan authority {maximum} rejected: {error}"));
    }

    let mut excessive = create_request(v1::CapabilityCreateMode::Normal);
    excessive.grant.as_mut().expect("grant").max_scan_rows = 65_536;
    assert_eq!(
        validate_public_message(&excessive),
        Err(PublicWireError::TooManyItems)
    );
}

#[test]
fn capability_grant_uses_the_complete_semantic_byte_bound() {
    let visibility = |lineage_bytes: usize, entity_type_id: u32| v1::EntityFieldVisibility {
        contract_lineage: "a".repeat(lineage_bytes),
        entity_type_id,
        field_ids: vec![1],
        secret_field_ids: Vec::new(),
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
fn application_installation_wire_never_confuses_partial_with_installed() {
    let canonical_plan = br#"{"schema":"riffdb.application-installation-plan/v1"}"#.to_vec();
    let request = v1::StartApplicationInstallationRequest {
        request_id: uuid_v7(),
        campaign_id: uuid_v7(),
        canonical_plan: canonical_plan.clone(),
        external_completion: None,
    };
    let running = v1::ApplicationInstallationObservation {
        campaign_id: request.campaign_id.clone(),
        plan_hash: hash_application_installation_plan(&canonical_plan)
            .as_bytes()
            .to_vec(),
        contract_lineage: "Ea".to_owned(),
        phase: v1::ApplicationInstallationPhase::Running as i32,
        completed_stages: Vec::new(),
        next_stage: v1::ApplicationInstallationStage::Preflight as i32,
        next_action: v1::ApplicationInstallationNextAction::ValidateLocalArtifacts as i32,
        failure: None,
        receipt_hash: Vec::new(),
    };
    let running_response = v1::StartApplicationInstallationResponse {
        observation: Some(running.clone()),
        canonical_receipt: Vec::new(),
    };
    validate_start_application_installation_exchange(&request, &running_response)
        .expect("running campaign is structurally exact");

    let mut partial = running;
    partial.phase = v1::ApplicationInstallationPhase::Partial as i32;
    partial.failure = Some(v1::ApplicationInstallationFailure {
        stage: v1::ApplicationInstallationStage::Preflight as i32,
        code: v1::ApplicationInstallationFailureCode::LocalArtifactMismatch as i32,
        next_action: v1::ApplicationInstallationNextAction::ValidateLocalArtifacts as i32,
    });
    validate_public_message(&v1::StartApplicationInstallationResponse {
        observation: Some(partial.clone()),
        canonical_receipt: Vec::new(),
    })
    .expect("typed partial campaign remains observable");

    partial.phase = v1::ApplicationInstallationPhase::Installed as i32;
    partial.next_stage = v1::ApplicationInstallationStage::Unspecified as i32;
    partial.next_action = v1::ApplicationInstallationNextAction::None as i32;
    partial.failure = None;
    partial.receipt_hash = vec![0x11; 32];
    assert_eq!(
        validate_public_message(&v1::StartApplicationInstallationResponse {
            observation: Some(partial),
            canonical_receipt: b"receipt".to_vec(),
        }),
        Err(PublicWireError::InconsistentFields)
    );
}

#[test]
fn installation_external_completion_is_closed_bounded_and_canonical() {
    use v1::application_installation_external_completion::Completion;

    let mut request = v1::StartApplicationInstallationRequest {
        request_id: uuid_v7(),
        campaign_id: uuid_v7(),
        canonical_plan: b"bounded-plan".to_vec(),
        external_completion: Some(v1::ApplicationInstallationExternalCompletion {
            completion: Some(Completion::DriverProof(
                v1::ApplicationInstallationDriverProof {
                    drivers: vec![
                        v1::ApplicationInstallationDriver::Rust as i32,
                        v1::ApplicationInstallationDriver::Typescript as i32,
                    ],
                },
            )),
        }),
    };
    validate_public_message(&request).expect("canonical exact driver proof");
    decode_public_message::<v1::StartApplicationInstallationRequest>(&request.encode_to_vec())
        .expect("canonical proof survives structural preflight");

    request.external_completion = Some(v1::ApplicationInstallationExternalCompletion {
        completion: Some(Completion::DriverProof(
            v1::ApplicationInstallationDriverProof {
                drivers: vec![
                    v1::ApplicationInstallationDriver::Rust as i32,
                    v1::ApplicationInstallationDriver::Rust as i32,
                ],
            },
        )),
    });
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::NonCanonical)
    );

    request.external_completion =
        Some(v1::ApplicationInstallationExternalCompletion { completion: None });
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::MissingRequiredField)
    );

    let seed = v1::ApplicationInstallationSeedReceipt {
        name: "seed-001".to_owned(),
        content_hash: vec![0x51; 32],
        succeeded: 3,
        replayed: 2,
    };
    request.external_completion = Some(v1::ApplicationInstallationExternalCompletion {
        completion: Some(Completion::SeedReceipts(
            v1::ApplicationInstallationSeedReceipts {
                seeds: vec![seed.clone()],
            },
        )),
    });
    validate_public_message(&request).expect("bounded seed receipt");

    let mut overflowing = seed.clone();
    overflowing.succeeded = u64::MAX;
    overflowing.replayed = 1;
    request.external_completion = Some(v1::ApplicationInstallationExternalCompletion {
        completion: Some(Completion::SeedReceipts(
            v1::ApplicationInstallationSeedReceipts {
                seeds: vec![overflowing],
            },
        )),
    });
    assert_eq!(
        validate_public_message(&request),
        Err(PublicWireError::InvalidIdentity)
    );

    request.external_completion = Some(v1::ApplicationInstallationExternalCompletion {
        completion: Some(Completion::SeedReceipts(
            v1::ApplicationInstallationSeedReceipts {
                seeds: vec![seed; 257],
            },
        )),
    });
    assert_eq!(
        decode_public_message::<v1::StartApplicationInstallationRequest>(&request.encode_to_vec()),
        Err(PublicWireError::PreflightLimitExceeded)
    );
}

#[test]
fn terminal_installation_receipt_and_poll_identity_are_content_addressed() {
    let canonical_plan = b"bounded-plan".to_vec();
    let canonical_receipt = b"redacted-terminal-receipt".to_vec();
    let campaign_id = uuid_v7();
    let start = v1::StartApplicationInstallationRequest {
        request_id: uuid_v7(),
        campaign_id: campaign_id.clone(),
        canonical_plan: canonical_plan.clone(),
        external_completion: None,
    };
    let response = v1::StartApplicationInstallationResponse {
        observation: Some(v1::ApplicationInstallationObservation {
            campaign_id: campaign_id.clone(),
            plan_hash: hash_application_installation_plan(&canonical_plan)
                .as_bytes()
                .to_vec(),
            contract_lineage: "Ea".to_owned(),
            phase: v1::ApplicationInstallationPhase::Installed as i32,
            completed_stages: (1..=10).collect(),
            next_stage: v1::ApplicationInstallationStage::Unspecified as i32,
            next_action: v1::ApplicationInstallationNextAction::None as i32,
            failure: None,
            receipt_hash: hash_application_installation_receipt(&canonical_receipt)
                .as_bytes()
                .to_vec(),
        }),
        canonical_receipt: canonical_receipt.clone(),
    };
    validate_start_application_installation_exchange(&start, &response)
        .expect("terminal response binds plan and receipt");

    let poll = v1::GetApplicationInstallationRequest {
        request_id: uuid_v7(),
        campaign_id: campaign_id.clone(),
    };
    let found = v1::GetApplicationInstallationResponse {
        result: Some(v1::get_application_installation_response::Result::Found(
            response.clone(),
        )),
    };
    validate_get_application_installation_exchange(&poll, &found)
        .expect("poll echoes the exact campaign");

    let mut wrong_poll = poll;
    let mut other = uuid_v7();
    other[15] ^= 1;
    wrong_poll.campaign_id = other;
    assert_eq!(
        validate_get_application_installation_exchange(&wrong_poll, &found),
        Err(PublicWireError::InconsistentFields)
    );

    let mut tampered = response;
    tampered.canonical_receipt = b"another-receipt".to_vec();
    assert_eq!(
        validate_public_message(&tampered),
        Err(PublicWireError::InconsistentFields)
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
            disposition: v1::EventPageDisposition::BoundedProgress.into(),
        }),
    };
    assert!(validate_public_message(&events).is_ok());

    let mut missing_cursor = events.clone();
    missing_cursor
        .page
        .as_mut()
        .expect("page")
        .next_cursor
        .clear();
    assert_eq!(
        validate_public_message(&missing_cursor),
        Err(PublicWireError::InconsistentFields)
    );

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
            disposition: v1::EventPageDisposition::Page.into(),
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
        secret_field_ids: Vec::new(),
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
// req: REP-004
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

    let process_only = v1::HealthResponse {
        result: Some(v1::health_response::Result::PreBootstrap(
            v1::PreBootstrapHealth {
                lifecycle: v1::PreBootstrapLifecycle::Unspecified as i32,
                liveness: true,
                readiness: false,
            },
        )),
        database_alias: String::new(),
        authentication_audience: String::new(),
    };
    validate_public_message(&process_only).expect("payload-free process liveness");
    let mut disclosed = process_only;
    disclosed.database_alias = "default".to_owned();
    assert_eq!(
        validate_public_message(&disclosed),
        Err(PublicWireError::InconsistentFields)
    );

    let authenticated = v1::HealthResponse {
        result: Some(v1::health_response::Result::Authenticated(
            v1::AuthenticatedHealth {
                status: v1::HealthStatus::Degraded as i32,
                active_contract_version: Some(1),
                last_commit_sequence: Some(1),
                components: [
                    v1::HealthComponentKind::AuthoritativeStorage,
                    v1::HealthComponentKind::Catalog,
                    v1::HealthComponentKind::CommitCoordinator,
                    v1::HealthComponentKind::Projection,
                    v1::HealthComponentKind::Outbox,
                    v1::HealthComponentKind::VectorStaleness,
                    v1::HealthComponentKind::Replication,
                ]
                .into_iter()
                .map(|kind| v1::HealthComponent {
                    component: kind as i32,
                    replication: None,
                    status: if matches!(
                        kind,
                        v1::HealthComponentKind::VectorStaleness
                            | v1::HealthComponentKind::Replication
                    ) {
                        v1::HealthComponentStatus::Unavailable as i32
                    } else {
                        v1::HealthComponentStatus::Healthy as i32
                    },
                })
                .collect(),
                started_at: Some(v1::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                build: Some(v1::BuildInfo {
                    semantic_version: "0.1.0".to_owned(),
                    git_revision: "0123456".to_owned(),
                    rust_version: "1.97.0".to_owned(),
                    enabled_features: vec!["default".to_owned()],
                    storage_format_version: 1,
                    contract_ir_version: 1,
                    mcp_protocol_baseline: "2025-06-18".to_owned(),
                }),
                history_incarnation: 1,
            },
        )),
        database_alias: "default".to_owned(),
        authentication_audience: "riffdb-grpc-loopback".to_owned(),
    };
    validate_public_message(&authenticated).expect("seven canonical health components");
    let encoded = authenticated.encode_to_vec();
    assert_eq!(
        decode_public_message::<v1::HealthResponse>(&encoded),
        Ok(authenticated.clone())
    );

    let before = v1::FrontierPosition {
        position: Some(v1::frontier_position::Position::BeforeFirst(v1::Unit {})),
    };
    let unknown = v1::ReplicationStatistics {
        role: v1::ReplicationRole::Follower.into(),
        applied_frontier: Some(v1::ReplicationFrontier {
            application: Some(before),
            administration: Some(before),
        }),
        ..Default::default()
    };
    for (kind, status, progress, valid) in [
        // req: REP-006
        // Configured sequence expiry degrades a primary's retention policy even
        // when its follower has acknowledged the complete application head.
        (
            v1::HealthComponentKind::Replication,
            v1::HealthComponentStatus::Degraded,
            Some(v1::ReplicationStatistics {
                role: v1::ReplicationRole::Primary.into(),
                source_frontier: unknown.applied_frontier,
                applied_frontier: unknown.applied_frontier,
                acknowledged_frontier: unknown.applied_frontier,
                registered_followers: Some(1),
                application_lag_sequences: Some(0),
                administration_lag_sequences: Some(0),
            }),
            true,
        ),
        (
            v1::HealthComponentKind::Replication,
            v1::HealthComponentStatus::Degraded,
            Some(unknown),
            true,
        ),
        (
            v1::HealthComponentKind::Replication,
            v1::HealthComponentStatus::Healthy,
            Some(unknown),
            false,
        ),
        (
            v1::HealthComponentKind::Replication,
            v1::HealthComponentStatus::Unavailable,
            Some(unknown),
            false,
        ),
        (
            v1::HealthComponentKind::Replication,
            v1::HealthComponentStatus::Healthy,
            None,
            false,
        ),
        (
            v1::HealthComponentKind::Catalog,
            v1::HealthComponentStatus::Degraded,
            Some(unknown),
            false,
        ),
    ] {
        let mut value = authenticated.clone();
        let Some(v1::health_response::Result::Authenticated(report)) = value.result.as_mut() else {
            unreachable!()
        };
        report.components = vec![v1::HealthComponent {
            component: kind.into(),
            status: status.into(),
            replication: progress,
        }];
        assert_eq!(
            decode_public_message::<v1::HealthResponse>(&value.encode_to_vec()).is_ok(),
            valid
        );
    }

    let mut too_many = authenticated;
    let v1::health_response::Result::Authenticated(report) =
        too_many.result.as_mut().expect("authenticated result")
    else {
        unreachable!();
    };
    report.components.push(v1::HealthComponent {
        component: v1::HealthComponentKind::VectorStaleness as i32,
        status: v1::HealthComponentStatus::Unavailable as i32,
        replication: None,
    });
    assert_eq!(
        validate_public_message(&too_many),
        Err(PublicWireError::InvalidEnum)
    );
    assert_eq!(
        decode_public_message::<v1::HealthResponse>(&too_many.encode_to_vec()),
        Err(PublicWireError::PreflightLimitExceeded)
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
        archive_restore: None,
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
        archive_restore: None,
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

#[test]
// req: REP-003
fn bootstrap_replication_uses_the_same_rpc_with_bounded_manifest_page_and_attachment() {
    let request = v1::StreamChangelogRequest {
        request_id: uuid_v7(),
        database_id: uuid_v7(),
        history_incarnation: 1,
        leadership_epoch: 1,
        readable_format: "riffdb.changelog-frame/v3".into(),
        catalog_digest: vec![0x76; 32],
        maximum_frame_bytes: 32 * 1024 * 1024,
        maximum_transitions: 256,
        after: None,
        bootstrap: Some(v1::ReplicationBootstrapRequest {
            hold_id: vec![1; 16],
            resume_manifest: vec![],
            after_page: 0,
        }),
        attachment: None,
        follower_hold_id: vec![],
    };
    assert_eq!(
        decode_public_message::<v1::StreamChangelogRequest>(&request.encode_to_vec()).unwrap(),
        request
    );
    let mut malformed = request.clone();
    malformed.bootstrap.as_mut().unwrap().after_page = 1;
    assert!(validate_public_message(&malformed).is_err());
    for fault in 0..4 {
        let mut malformed = request.clone();
        let bootstrap = malformed.bootstrap.as_mut().unwrap();
        match fault {
            0 => bootstrap.hold_id = vec![0; 16],
            1 => bootstrap.hold_id.pop().map(|_| ()).unwrap(),
            2 => bootstrap.resume_manifest = vec![1; 513],
            _ => bootstrap.after_page = 1_048_577,
        }
        assert!(validate_public_message(&malformed).is_err());
    }
    let before = || {
        Some(v1::FrontierPosition {
            position: Some(v1::frontier_position::Position::BeforeFirst(v1::Unit {})),
        })
    };
    let acknowledged = v1::ReplicationPosition {
        transaction_sequence: 9,
        history_hash: vec![0x52; 32],
        application_frontier: before(),
        administration_frontier: before(),
    };
    let mut attachment = request.clone();
    attachment.bootstrap = None;
    attachment.attachment = Some(v1::ReplicationBootstrapAttachment {
        manifest: vec![0x71; 512],
        acknowledged: Some(acknowledged.clone()),
    });
    assert_eq!(
        decode_public_message::<v1::StreamChangelogRequest>(&attachment.encode_to_vec()).unwrap(),
        attachment
    );
    let mut conflict = attachment.clone();
    conflict.after = Some(acknowledged);
    assert!(
        decode_public_message::<v1::StreamChangelogRequest>(&conflict.encode_to_vec()).is_err()
    );
    for fault in 0..3 {
        let mut malformed = attachment.clone();
        let attachment = malformed.attachment.as_mut().unwrap();
        match fault {
            0 => attachment.acknowledged = None,
            1 => attachment.manifest.clear(),
            _ => {
                attachment
                    .acknowledged
                    .as_mut()
                    .unwrap()
                    .transaction_sequence = 0
            }
        }
        assert!(validate_public_message(&malformed).is_err());
    }
    // Prost would accept a repeated singular nested field by taking its last
    // value. Public preflight must refuse before that information is lost.
    for unknown in [false, true] {
        let mut nested = request.bootstrap.as_ref().unwrap().encode_to_vec();
        if unknown {
            nested.extend_from_slice(&[0x20, 1]);
        } else {
            nested.extend_from_slice(&[0x0a, 16]);
            nested.extend_from_slice(&[1; 16]);
        }
        let mut root = request.clone();
        root.bootstrap = None;
        let mut encoded = root.encode_to_vec();
        encoded.extend_from_slice(&[0x52, u8::try_from(nested.len()).unwrap()]);
        encoded.extend_from_slice(&nested);
        if unknown {
            // ADR-0006: public unknown fields are ignored and never relayed.
            let decoded = decode_public_message::<v1::StreamChangelogRequest>(&encoded).unwrap();
            assert_eq!(decoded, request);
            assert_eq!(decoded.encode_to_vec(), request.encode_to_vec());
        } else {
            assert!(decode_public_message::<v1::StreamChangelogRequest>(&encoded).is_err());
        }
    }
    let manifest = v1::StreamChangelogResponse {
        source_head: None,
        item: Some(v1::stream_changelog_response::Item::BootstrapManifest(
            vec![1; 512],
        )),
    };
    assert!(validate_public_message(&manifest).is_ok());
    let page = v1::StreamChangelogResponse {
        source_head: None,
        item: Some(v1::stream_changelog_response::Item::BootstrapPage(
            vec![1; 32 * 1024 * 1024 + 512],
        )),
    };
    assert!(validate_public_message(&page).is_ok());
    for item in [
        v1::stream_changelog_response::Item::BootstrapManifest(vec![1; 513]),
        v1::stream_changelog_response::Item::BootstrapPage(vec![]),
        v1::stream_changelog_response::Item::BootstrapPage(vec![1; 32 * 1024 * 1024 + 513]),
    ] {
        assert!(
            validate_public_message(&v1::StreamChangelogResponse {
                source_head: None,
                item: Some(item)
            })
            .is_err()
        );
    }
}
