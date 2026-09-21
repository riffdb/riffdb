//! A real source-issued scoped token and post-cutover application reads.
use super::*;
use riffdb_client_rust::{
    ApplicationContract, ApplicationUuid, ApplicationValue, ProjectedQuery, ProjectedQueryOutcome,
    RiffDbClient,
};
use std::io::Write;

pub(super) fn configure(fixture: &Fixture, name: &str) {
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(fixture.config(name))
        .unwrap();
    file.write_all(b"\n[[projections]]\nname = \"organization_view\"\nentity = \"Organization\"\nprojected_fields = [\"name\"]\norg_scope_field = \"organization_id\"\n").unwrap();
}

pub(super) async fn authority(client: &mut RiffDbClient, admin: &CallMetadata) -> CallMetadata {
    let bundle = riffdb_contract_compiler::compile_contract_source(include_str!(
        "../../examples/ticketdesk/riffdb/contract.riff"
    ))
    .unwrap();
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Organization")
        .unwrap();
    let permission = |permission| v1::CapabilityPermission {
        permission: Some(permission),
    };
    let result = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: request_id(31),
                capability_id: capability_id(31).as_bytes().to_vec(),
                mode: v1::CapabilityCreateMode::Normal as i32,
                principal_id: "promotion-causal-reader".into(),
                actor_kind: v1::ActorKind::Service as i32,
                requested_lifetime_seconds: 3600,
                audiences: vec!["replication-process-test".into()],
                grant: Some(v1::CapabilityGrant {
                    tenant_scope: Some(v1::TenantScope {
                        scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                    }),
                    partition_scope: Some(v1::PartitionScope {
                        scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                    }),
                    permissions: vec![
                        permission(v1::capability_permission::Permission::ReadEntity(
                            v1::LineageScopedStableId {
                                contract_lineage: "TicketDesk".into(),
                                stable_id: entity.id().get(),
                            },
                        )),
                        permission(v1::capability_permission::Permission::ExecuteAdHocQuery(
                            v1::Unit {},
                        )),
                        permission(
                            v1::capability_permission::Permission::ApplicationRoleIdentity(
                                vec![0x5d; 32],
                            ),
                        ),
                    ],
                    field_visibility: vec![v1::EntityFieldVisibility {
                        contract_lineage: "TicketDesk".into(),
                        entity_type_id: entity.id().get(),
                        field_ids: entity
                            .record()
                            .fields()
                            .iter()
                            .map(|field| field.id().get())
                            .collect(),
                        secret_field_ids: vec![],
                    }],
                    max_scan_rows: 10,
                    ..Default::default()
                }),
            },
            admin,
        )
        .await
        .unwrap();
    let Some(v1::create_capability_response::Result::Normal(result)) = result.result else {
        panic!("normal read capability")
    };
    let Some(v1::normal_create_capability_result::Result::Created(result)) = result.result else {
        panic!("created read capability")
    };
    CallMetadata::authenticated(BearerCredential::new(&result.token).unwrap())
}

fn query(token: CommitToken) -> ProjectedQuery {
    ProjectedQuery::new(
        ApplicationContract::Active,
        "organization_view",
        ApplicationValue::Uuid(ApplicationUuid::from_bytes([
            0x01, 0x8f, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 0x80,
        ])),
    )
    .unwrap()
    .select(vec!["name".into()])
    .limit(Some(10))
    .freshness(FreshnessPolicy::Causal {
        token,
        max_wait: Duration::from_secs(5),
    })
}

pub(super) async fn token(
    fixture: &Fixture,
    reader: &CallMetadata,
    lineage: riffdb_storage_api::ChangelogLineageV3,
) -> CommitToken {
    let mut client = fixture.application_client().await;
    let query = query(CommitToken::new_scoped(
        lineage.database_id(),
        lineage.history_incarnation(),
        CommitSequence::new(1).unwrap(),
    ));
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match client
                .execute_projected_query(query.clone(), reader)
                .await
                .unwrap()
            {
                ProjectedQueryOutcome::Ready {
                    commit_token: Some(token),
                    ..
                } => break token,
                ProjectedQueryOutcome::Building { .. } | ProjectedQueryOutcome::Lagging { .. } => {
                    tokio::task::yield_now().await
                }
                result => panic!("source must return a checked committed token: {result:?}"),
            }
        }
    })
    .await
    .unwrap()
}

pub(super) async fn assert_old_token_refused(
    fixture: &Fixture,
    reader: &CallMetadata,
    token: CommitToken,
) {
    let mut client = fixture.application_client_for("follower").await;
    let error = client
        .execute_projected_query(query(token), reader)
        .await
        .unwrap_err();
    assert_eq!(
        error.semantic_error().map(|error| error.code()),
        Some(ApplicationErrorCode::HistoryIncarnationMismatch)
    );
}

pub(super) async fn assert_new_entity(client: &mut RiffDbClient, reader: &CallMetadata) {
    let bundle = riffdb_contract_compiler::compile_contract_source(include_str!(
        "../../examples/ticketdesk/riffdb/contract.riff"
    ))
    .unwrap();
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Organization")
        .unwrap();
    let name = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "name")
        .unwrap()
        .id()
        .get();
    let mut key = EntityKeyBuilder::new(entity.id());
    key.push_uuid(&[
        0x01, 0x8f, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 0x96,
    ])
    .unwrap();
    let result = client
        .get_entity(
            v1::GetEntityRequest {
                request_id: request_id(236),
                contract: Some(v1::ContractSelection {
                    selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
                }),
                entity_type_id: entity.id().get(),
                entity_key: key.finish().unwrap().into_bytes(),
                fields: Some(v1::FieldSelection {
                    field_ids: vec![name],
                }),
            },
            reader,
        )
        .await
        .unwrap();
    let Some(v1::get_entity_response::Result::Found(entity)) = result.result else {
        panic!("new source entity")
    };
    assert_eq!(entity.entity_version, 1);
    assert!(entity.fields.unwrap().fields.iter().any(|field| matches!(field.value.as_ref().and_then(|v| v.kind.as_ref()), Some(v1::value::Kind::StringValue(value)) if value == "post-promotion write")));
}
