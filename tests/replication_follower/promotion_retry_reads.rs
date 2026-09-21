//! Real checked read of the command result, including after a new source owner.
use super::*;
use riffdb_client_rust::RiffDbClient;

pub(super) async fn authority(
    client: &mut RiffDbClient,
    admin: &CallMetadata,
) -> (CallMetadata, v1::GetEntityRequest) {
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
    let fields: Vec<_> = entity
        .record()
        .fields()
        .iter()
        .filter(|field| !entity.primary_key_fields().contains(&field.id()))
        .map(|field| field.id().get())
        .collect();
    let created = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: request_id(225),
                mode: v1::CapabilityCreateMode::Normal as i32,
                capability_id: capability_id(225).as_bytes().to_vec(),
                principal_id: "promotion-result-reader".into(),
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
                    permissions: vec![v1::CapabilityPermission {
                        permission: Some(v1::capability_permission::Permission::ReadEntity(
                            v1::LineageScopedStableId {
                                contract_lineage: "TicketDesk".into(),
                                stable_id: entity.id().get(),
                            },
                        )),
                    }],
                    field_visibility: vec![v1::EntityFieldVisibility {
                        contract_lineage: "TicketDesk".into(),
                        entity_type_id: entity.id().get(),
                        field_ids: fields.clone(),
                        secret_field_ids: vec![],
                    }],
                    max_scan_rows: 1,
                    ..Default::default()
                }),
            },
            admin,
        )
        .await
        .unwrap();
    let Some(v1::create_capability_response::Result::Normal(created)) = created.result else {
        panic!("normal reader capability")
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = created.result else {
        panic!("created reader capability")
    };
    let reader = CallMetadata::authenticated(BearerCredential::new(&created.token).unwrap());
    let mut key = EntityKeyBuilder::new(entity.id());
    key.push_uuid(&[
        0x01, 0x8f, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 0x96,
    ])
    .unwrap();
    (
        reader,
        v1::GetEntityRequest {
            request_id: request_id(226),
            contract: Some(v1::ContractSelection {
                selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
            }),
            entity_type_id: entity.id().get(),
            entity_key: key.finish().unwrap().into_bytes(),
            fields: Some(v1::FieldSelection { field_ids: fields }),
        },
    )
}

pub(super) async fn read(
    client: &mut RiffDbClient,
    reader: &CallMetadata,
    mut request: v1::GetEntityRequest,
    seed: u8,
) -> v1::Entity {
    // Callers retain the checked selection, while each submission gets a fresh ID.
    request.request_id = request_id(seed);
    let result = client.get_entity(request, reader).await.unwrap();
    let Some(v1::get_entity_response::Result::Found(entity)) = result.result else {
        panic!("promoted command result must be readable")
    };
    entity
}
