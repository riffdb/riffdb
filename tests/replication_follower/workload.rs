//! The app-baseline smoke dataset and its transactional write probes, sent
//! through the checked-in generated TicketDesk client over real TLS.
use super::support::{Fixture, capability_id, request_id};
use riffdb_app_baseline_core::{Scale, SeedDataset, format_uuid as uuid};
use riffdb_client_rust::{AttemptBudget, BearerCredential, CallMetadata, RiffDbClient, v1};
use riffdb_ticketdesk::*;

const CONTRACT: &str = include_str!("../../examples/app-baseline/contracts/ticketdesk.riff");

pub(super) async fn deploy(client: &mut RiffDbClient, metadata: &CallMetadata) {
    let result = client
        .deploy_contract(
            v1::DeployContractRequest {
                request_id: request_id(20),
                source: CONTRACT.into(),
                expected_active_version: None,
                expected_active_bundle_hash: vec![],
                expected_candidate_bundle_hash: vec![],
            },
            metadata,
        )
        .await
        .unwrap();
    assert!(matches!(
        result.result,
        Some(v1::deploy_contract_response::Result::Activated(_))
    ));
}

async fn authority(client: &mut RiffDbClient, metadata: &CallMetadata) -> CallMetadata {
    authority_with_seed(client, metadata, 3).await
}

pub(super) async fn authority_with_seed(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    seed: u8,
) -> CallMetadata {
    let bundle = riffdb_contract_compiler::compile_contract_source(CONTRACT).unwrap();
    let grant = v1::CapabilityGrant {
        tenant_scope: Some(v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
        }),
        partition_scope: Some(v1::PartitionScope {
            scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
        }),
        permissions: bundle
            .commands()
            .iter()
            .map(|command| v1::CapabilityPermission {
                permission: Some(v1::capability_permission::Permission::InvokeCommand(
                    v1::LineageScopedStableId {
                        contract_lineage: "TicketDesk".into(),
                        stable_id: command.command_id().get(),
                    },
                )),
            })
            .collect(),
        field_visibility: bundle
            .schema()
            .entities()
            .iter()
            .map(|entity| v1::EntityFieldVisibility {
                contract_lineage: "TicketDesk".into(),
                entity_type_id: entity.id().get(),
                field_ids: entity
                    .record()
                    .fields()
                    .iter()
                    .map(|field| field.id().get())
                    .collect(),
                secret_field_ids: vec![],
            })
            .collect(),
        max_scan_rows: 500,
        ..Default::default()
    };
    let response = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: request_id(seed),
                mode: v1::CapabilityCreateMode::Normal as i32,
                capability_id: capability_id(seed).as_bytes().to_vec(),
                principal_id: "replication-ticketdesk".into(),
                actor_kind: v1::ActorKind::Service as i32,
                requested_lifetime_seconds: 3600,
                audiences: vec!["replication-process-test".into()],
                grant: Some(grant),
            },
            metadata,
        )
        .await
        .unwrap();
    let Some(v1::create_capability_response::Result::Normal(result)) = response.result else {
        panic!("normal capability expected")
    };
    let Some(v1::normal_create_capability_result::Result::Created(result)) = result.result else {
        panic!("fresh workload capability expected")
    };
    CallMetadata::authenticated(BearerCredential::new(&result.token).unwrap())
}

pub(super) async fn run(
    fixture: &Fixture,
    primary: &mut RiffDbClient,
    admin: &CallMetadata,
) -> u64 {
    run_observed(fixture, primary, admin, async |_, _| {}).await
}

pub(super) async fn run_observed(
    fixture: &Fixture,
    primary: &mut RiffDbClient,
    admin: &CallMetadata,
    mut observer: impl AsyncFnMut(u64, u64),
) -> u64 {
    let metadata = authority(primary, admin).await;
    let mut client = TicketDeskClient::new(
        fixture.application_client().await,
        metadata,
        AttemptBudget::new(2).unwrap(),
    );
    let dataset = SeedDataset::generate(Scale::smoke());
    let mut last = 0;
    let mut count = 0;
    macro_rules! execute {
        ($method:ident, $input:expr, $outcome:pat) => {{
            let input = $input;
            let result = client.$method(input.clone()).await.unwrap();
            assert!(
                matches!(result.outcome, $outcome),
                "unexpected workload business outcome"
            );
            assert!(!result.replayed);
            let sequence = result.commit_sequence.unwrap();
            assert!(sequence > last);
            last = sequence;
            count += 1;
            // Every operation replays its exact typed business value and durable
            // identity, including multi-row commands and their outbox events.
            let replay = client.$method(input).await.unwrap();
            assert!(replay.replayed);
            assert_eq!(replay.commit_sequence, Some(sequence));
            assert_eq!(replay.outcome, result.outcome);
            assert_eq!(replay.outcome_uri, result.outcome_uri);
            observer(count, sequence).await;
        }};
    }
    for row in &dataset.organizations {
        execute!(
            create_organization,
            CreateOrganizationInput {
                name: row.name.clone(),
                organization_id: uuid(row.organization_id),
                idempotency_key: format!("seed-org-{}", uuid(row.organization_id))
            },
            CreateOrganizationOutcome::Created { .. }
        );
    }
    for row in &dataset.users {
        execute!(
            create_user,
            CreateUserInput {
                organization_id: uuid(row.organization_id),
                user_id: uuid(row.user_id),
                email: row.email.clone(),
                display_name: row.display_name.clone(),
                idempotency_key: format!("seed-user-{}", uuid(row.user_id))
            },
            CreateUserOutcome::Created { .. }
        );
    }
    for row in &dataset.projects {
        execute!(
            create_project,
            CreateProjectInput {
                organization_id: uuid(row.organization_id),
                project_id: uuid(row.project_id),
                name: row.name.clone(),
                idempotency_key: format!("seed-project-{}", uuid(row.project_id))
            },
            CreateProjectOutcome::Created { .. }
        );
    }
    for (index, row) in dataset.members.iter().enumerate() {
        execute!(
            add_project_member,
            AddProjectMemberInput {
                organization_id: uuid(row.organization_id),
                project_id: uuid(row.project_id),
                user_id: uuid(row.user_id),
                role: row.role.clone(),
                idempotency_key: format!("seed-member-{index}")
            },
            AddProjectMemberOutcome::Created { .. }
        );
    }
    for row in &dataset.tickets {
        execute!(
            create_ticket,
            CreateTicketInput {
                organization_id: uuid(row.organization_id),
                ticket_id: uuid(row.ticket_id),
                project_id: uuid(row.project_id),
                reporter_id: uuid(row.reporter_id),
                assignee_id: uuid(row.assignee_id),
                title: row.title.clone(),
                status: match row.status {
                    riffdb_app_baseline_core::TicketStatus::Open => "Open",
                    riffdb_app_baseline_core::TicketStatus::Closed => "Closed",
                    riffdb_app_baseline_core::TicketStatus::InProgress => "InProgress",
                }
                .into(),
                idempotency_key: format!("seed-ticket-{}", uuid(row.ticket_id))
            },
            CreateTicketOutcome::Created { .. }
        );
    }
    for row in &dataset.comments {
        execute!(
            create_comment,
            CreateCommentInput {
                organization_id: uuid(row.organization_id),
                ticket_id: uuid(row.ticket_id),
                comment_id: uuid(row.comment_id),
                author_id: uuid(row.author_id),
                body: row.body.clone(),
                idempotency_key: format!("seed-comment-{}", uuid(row.comment_id))
            },
            CreateCommentOutcome::Created { .. }
        );
    }
    for row in &dataset.labels {
        execute!(
            create_label,
            CreateLabelInput {
                organization_id: uuid(row.organization_id),
                label_id: uuid(row.label_id),
                name: row.name.clone(),
                idempotency_key: format!("seed-label-{}", uuid(row.label_id))
            },
            CreateLabelOutcome::Created { .. }
        );
    }
    for (index, row) in dataset.ticket_labels.iter().enumerate() {
        execute!(
            attach_label,
            AttachLabelInput {
                organization_id: uuid(row.organization_id),
                ticket_id: uuid(row.ticket_id),
                label_id: uuid(row.label_id),
                idempotency_key: format!("seed-link-{index}")
            },
            AttachLabelOutcome::Created { .. }
        );
    }
    let row = dataset.probes().write_comment(0);
    execute!(
        create_comment,
        CreateCommentInput {
            organization_id: uuid(row.row.organization_id),
            ticket_id: uuid(row.row.ticket_id),
            comment_id: uuid(row.row.comment_id),
            author_id: uuid(row.row.author_id),
            body: row.row.body,
            idempotency_key: row.idempotency_key
        },
        CreateCommentOutcome::Created { .. }
    );
    let row = dataset.probes().close_ticket_with_comment(0);
    execute!(
        close_ticket_with_comment,
        CloseTicketWithCommentInput {
            organization_id: uuid(row.organization_id),
            ticket_id: uuid(row.ticket_id),
            comment_id: uuid(row.comment_id),
            author_id: uuid(row.author_id),
            body: row.body,
            idempotency_key: row.idempotency_key
        },
        CloseTicketWithCommentOutcome::Closed { .. }
    );
    let row = dataset.probes().swap_member_roles(0);
    execute!(
        swap_member_roles,
        SwapMemberRolesInput {
            organization_id: uuid(row.organization_id),
            project_id: uuid(row.project_id),
            user_a: uuid(row.user_a),
            user_b: uuid(row.user_b),
            role_a: row.role_a,
            role_b: row.role_b,
            idempotency_key: row.idempotency_key
        },
        SwapMemberRolesOutcome::Swapped { .. }
    );
    let row = dataset.probes().open_ticket_with_labels(0);
    execute!(
        open_ticket_with_labels,
        OpenTicketWithLabelsInput {
            organization_id: uuid(row.organization_id),
            ticket_id: uuid(row.ticket_id),
            project_id: uuid(row.project_id),
            reporter_id: uuid(row.reporter_id),
            assignee_id: uuid(row.assignee_id),
            title: row.title,
            label_a: uuid(row.label_a),
            label_b: uuid(row.label_b),
            idempotency_key: row.idempotency_key
        },
        OpenTicketWithLabelsOutcome::Created { .. }
    );
    assert_eq!(count, dataset.scale.approximate_row_count() + 4);
    last
}
