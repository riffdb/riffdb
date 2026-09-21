#![forbid(unsafe_code)]
//! Fencing authority is a distinct, unscoped administrative permission.
// req: REP-005
use riffdb_types::{
    AggregateTypeId, ApplicationRoleHash, CapabilityGrantError, CapabilityGrantV1,
    CapabilityPermissionKindV1 as Kind, CapabilityPermissionV1 as Permission,
    CapabilityPermissionsV1 as Permissions, ContractLineage, PartitionKeyBuilder,
    PartitionScopeV1 as Partitions, ScopedPartitionV1, TenantId, TenantScope,
};
use std::num::NonZeroU16;

#[test]
fn primary_fence_permission_is_distinct_from_replication_and_closed_at_tag_34() {
    assert_eq!(Kind::FenceReplicationPrimary.tag(), 34);
    assert_eq!(Kind::from_tag(34), Some(Kind::FenceReplicationPrimary));
    assert_eq!(Kind::ReplicateChangelog.tag(), 33);
    assert_eq!(Kind::from_tag(35), None);
    let fence = Permission::unparameterized(Kind::FenceReplicationPrimary).unwrap();
    assert_eq!(fence.canonical_key(), [34]);
    let streaming = Permissions::new(vec![
        Permission::unparameterized(Kind::ReplicateChangelog).unwrap(),
    ])
    .unwrap();
    assert!(!streaming.contains_exact(&fence));
    assert!(!streaming.contains_kind(Kind::FenceReplicationPrimary));
}

#[test]
fn primary_fence_grants_require_global_all_partitions_and_no_application_role() {
    let fence = Permission::unparameterized(Kind::FenceReplicationPrimary).unwrap();
    let partition = ScopedPartitionV1::new(
        ContractLineage::new("ScopedApplication").unwrap(),
        PartitionKeyBuilder::new(AggregateTypeId::new(1).unwrap())
            .finish()
            .unwrap(),
    );
    for tenant in [
        TenantScope::Global,
        TenantScope::Tenant(TenantId::new("tenant-a").unwrap()),
    ] {
        for partitions in [
            Partitions::All,
            Partitions::explicit(vec![partition.clone()]).unwrap(),
        ] {
            for role in [false, true] {
                let mut permissions = vec![fence.clone()];
                if role {
                    permissions.push(Permission::ApplicationRoleIdentity(
                        ApplicationRoleHash::from_bytes([0x71; 32]),
                    ));
                }
                let expected =
                    tenant == TenantScope::Global && partitions == Partitions::All && !role;
                let result = CapabilityGrantV1::new(
                    tenant.clone(),
                    partitions.clone(),
                    Permissions::new(permissions).unwrap(),
                    vec![],
                    NonZeroU16::new(1).unwrap(),
                    vec![Kind::FenceReplicationPrimary],
                );
                if expected {
                    let grant = result.unwrap();
                    assert_eq!(grant.approval_required(), &[Kind::FenceReplicationPrimary]);
                } else {
                    assert_eq!(result, Err(CapabilityGrantError::InvalidShape));
                }
            }
        }
    }
}
