//! Fencing grants retain their distinct authority and bootstrap exclusion.
// req: REP-005
use super::{super::*, sample};
use crate::{
    BootstrapDigestCandidatesV1, BootstrapServiceAuditStartV1, CapabilityBootstrapIntentV1,
    CapabilityGrantV1, CapabilityPermissionsV1, CapabilityRequestedRecordV1, PartitionScopeV1,
    StorageValueError, StoredCapabilityRecordV1,
};
use prost::Message;
use riffdb_proto::{
    durable::{readable_record_registry, readable_record_schema},
    envelope,
    storage::v1 as wire,
};
use riffdb_types::{
    CapabilityPermissionKindV1 as Kind, CapabilityPermissionV1 as Permission, ServiceAuditTargetV1,
    ServiceAuditTargetsV1, ServiceIngressKindV1, TenantScope,
};
use std::num::{NonZeroU16, NonZeroU32};

fn record() -> StoredCapabilityRecordV1 {
    let (base, _, _, _) = sample::capability_records();
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            Permission::unparameterized(Kind::AdministerCapabilities).unwrap(),
            Permission::unparameterized(Kind::FenceReplicationPrimary).unwrap(),
        ])
        .unwrap(),
        vec![],
        NonZeroU16::MIN,
        vec![Kind::FenceReplicationPrimary],
    )
    .unwrap();
    StoredCapabilityRecordV1::from_stored_parts(
        base.capability_id(),
        base.revision(),
        base.token_digest(),
        base.database_id(),
        base.environment().clone(),
        base.principal_id().clone(),
        base.actor_kind(),
        base.audiences().to_vec(),
        base.issued_at(),
        base.expires_at(),
        base.creation_sequence(),
        base.creation_request_id(),
        grant,
        base.lifecycle().clone(),
    )
    .unwrap()
}

#[test]
fn primary_fence_permission_durable_vector_preserves_distinct_authority() {
    let record = record();
    let encoded = encode_capability_record_v1(&record).unwrap();
    assert_eq!(
        decode_capability_record_v1(encoded.as_bytes())
            .unwrap()
            .value(),
        &record
    );
    let envelope = readable_record_registry()
        .decode(encoded.as_bytes())
        .unwrap();
    let wire = wire::CapabilityRecordV1::decode(envelope.payload()).unwrap();
    let grant = wire.grant.unwrap();
    assert_eq!(
        grant
            .permissions
            .unwrap()
            .values
            .iter()
            .map(|p| p.kind)
            .collect::<Vec<_>>(),
        vec![19, 34]
    );
    assert_eq!(grant.approval_required, vec![34]);
    assert!(
        !record
            .grant()
            .permissions()
            .contains_kind(Kind::ReplicateChangelog)
    );
    let hex = encoded
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let vector = format!("primary-fence-capability-v1 {hex}\n");
    if let Some(path) = std::env::var_os("RIFFDB_PRIMARY_FENCE_CAPABILITY_VECTOR_OUTPUT") {
        std::fs::write(path, vector).unwrap();
    } else {
        assert_eq!(
            std::fs::read_to_string(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../fixtures/replication/primary-fence-capability-v1.hex")
            )
            .unwrap(),
            vector
        );
    }
}

#[test]
fn primary_fence_durable_grant_refuses_parameter_role_scope_and_unknown_tags() {
    let encoded = encode_capability_record_v1(&record()).unwrap();
    let envelope = readable_record_registry()
        .decode(encoded.as_bytes())
        .unwrap();
    let valid = wire::CapabilityRecordV1::decode(envelope.payload()).unwrap();
    let (scoped, _, _, _) = sample::capability_records();
    let scoped = encode_capability_record_v1(&scoped).unwrap();
    let scoped = readable_record_registry()
        .decode(scoped.as_bytes())
        .unwrap();
    let scoped = wire::CapabilityRecordV1::decode(scoped.payload())
        .unwrap()
        .grant
        .unwrap();
    for case in 0..7 {
        let mut bad = valid.clone();
        let grant = bad.grant.as_mut().unwrap();
        let permissions = &mut grant.permissions.as_mut().unwrap().values;
        match case {
            0 => permissions[1].contract_lineage = Some("Foreign".into()),
            1 => permissions[1].stable_id = Some(1),
            2 => permissions[1].kind = 35,
            3 => grant.approval_required = vec![35],
            4 => grant.tenant_scope = scoped.tenant_scope.clone(),
            5 => grant.partition_scope = scoped.partition_scope.clone(),
            6 => permissions.insert(
                1,
                wire::CapabilityPermissionV1 {
                    kind: 25,
                    application_role_hash: Some(vec![0x71; 32]),
                    ..Default::default()
                },
            ),
            _ => unreachable!(),
        }
        let payload = bad.encode_to_vec();
        // Recompute the envelope checksum: semantic rejection must not depend on corruption.
        let bytes = envelope::encode(
            readable_record_schema("riffdb.storage.v1.CapabilityRecordV1").unwrap(),
            &payload,
        )
        .unwrap();
        assert!(decode_capability_record_v1(&bytes).is_err(), "case {case}");
    }
}

#[test]
fn primary_fence_permission_cannot_be_issued_by_principal_less_bootstrap() {
    let base = record();
    let requested = CapabilityRequestedRecordV1::new(
        base.database_id(),
        base.environment().clone(),
        base.principal_id().clone(),
        base.actor_kind(),
        NonZeroU32::new(60).unwrap(),
        base.audiences().to_vec(),
        base.grant().clone(),
    )
    .unwrap();
    let start = BootstrapServiceAuditStartV1::new(
        base.creation_request_id(),
        base.issued_at(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(base.capability_id())])
            .unwrap(),
        None,
    )
    .unwrap();
    let result = CapabilityBootstrapIntentV1::new(
        base.capability_id(),
        requested,
        BootstrapDigestCandidatesV1::new(vec![base.token_digest()], base.token_digest()).unwrap(),
        base.issued_at(),
        base.expires_at(),
        start,
    );
    assert_eq!(result, Err(StorageValueError::InvalidShape));
}
