//! Small generated workload; every suffix command overwrites the same entity.
use riffdb_client_rust::generated::legal_spend::{
    AllocateBudget, Amount, CONTRACT_LINEAGE, CreateBudget,
};
use riffdb_client_rust::{
    AttemptBudget, BearerCredential, CallMetadata, RiffDbClient, generate_request_id, v1,
};
const CONTRACT: &str = include_str!("../../contracts/examples/budget.riff");
const ORG: [u8; 16] = [0x31; 16];
fn request_id() -> Vec<u8> {
    generate_request_id().unwrap().into_bytes().to_vec()
}
pub(super) async fn prepare(client: &mut RiffDbClient, admin: &CallMetadata) -> CallMetadata {
    let deployed = client
        .deploy_contract(
            v1::DeployContractRequest {
                request_id: request_id(),
                source: CONTRACT.into(),
                ..Default::default()
            },
            admin,
        )
        .await
        .unwrap();
    assert!(matches!(
        deployed.result,
        Some(v1::deploy_contract_response::Result::Activated(_))
    ));
    use v1::capability_permission::Permission;
    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: CONTRACT_LINEAGE.into(),
        stable_id,
    };
    let response = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: request_id(),
                mode: v1::CapabilityCreateMode::Normal as i32,
                capability_id: super::support::capability_id(9).as_bytes().to_vec(),
                principal_id: "archive-writer".into(),
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
                    permissions: [
                        Permission::InvokeCommand(scoped(1)),
                        Permission::InvokeCommand(scoped(2)),
                        Permission::ReadEntity(scoped(1)),
                    ]
                    .into_iter()
                    .map(|permission| v1::CapabilityPermission {
                        permission: Some(permission),
                    })
                    .collect(),
                    field_visibility: vec![v1::EntityFieldVisibility {
                        contract_lineage: CONTRACT_LINEAGE.into(),
                        entity_type_id: 1,
                        field_ids: vec![1, 3, 5],
                        secret_field_ids: vec![],
                    }],
                    max_scan_rows: 100,
                    ..Default::default()
                }),
            },
            admin,
        )
        .await
        .unwrap();
    let Some(v1::create_capability_response::Result::Normal(normal)) = response.result else {
        panic!("normal capability required")
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = normal.result else {
        panic!("new capability required")
    };
    let writer = CallMetadata::authenticated(BearerCredential::new(&created.token).unwrap());
    let created = client
        .execute_generated(
            &CreateBudget {
                idempotency_key: "archive-base".into(),
                organization_id: ORG,
                fiscal_year: 2027,
                approved_amount: Amount::from_minor_units(10000).unwrap(),
            },
            AttemptBudget::new(1).unwrap(),
            &writer,
        )
        .await
        .unwrap();
    assert_eq!(created.response().commit_sequence, 1);
    writer
}
pub(super) async fn allocate(
    client: &mut RiffDbClient,
    writer: &CallMetadata,
    sequence: u64,
) -> v1::ExecuteCommandResponse {
    let result = client
        .execute_generated(
            &AllocateBudget {
                idempotency_key: format!("archive-suffix-{sequence}"),
                organization_id: ORG,
                fiscal_year: 2027,
                matter_id: [sequence as u8; 16],
                amount: Amount::from_minor_units(100).unwrap(),
            },
            AttemptBudget::new(1).unwrap(),
            writer,
        )
        .await
        .unwrap();
    assert_eq!(result.response().commit_sequence, sequence);
    result.response().clone()
}
pub(super) async fn entity(client: &mut RiffDbClient, writer: &CallMetadata) -> v1::Entity {
    let mut key = riffdb_types::EntityKeyBuilder::new(riffdb_types::EntityTypeId::new(1).unwrap());
    key.push_uuid(&ORG).unwrap();
    key.push_i64(2027).unwrap();
    let response = client
        .get_entity(
            v1::GetEntityRequest {
                request_id: request_id(),
                contract: Some(v1::ContractSelection {
                    selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
                }),
                entity_type_id: 1,
                entity_key: key.finish().unwrap().into_bytes(),
                fields: Some(v1::FieldSelection {
                    field_ids: vec![1, 3, 5],
                }),
            },
            writer,
        )
        .await
        .unwrap();
    let Some(v1::get_entity_response::Result::Found(entity)) = response.result else {
        panic!("budget missing")
    };
    entity
}
