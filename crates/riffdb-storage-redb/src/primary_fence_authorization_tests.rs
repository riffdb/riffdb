//! Transaction-current fence authority before any physical mutation.
// req: REP-005, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    CapabilityGrantV1, CapabilityLifecycleV1, CapabilityPermissionKindV1, CapabilityPermissionV1,
    CapabilityPermissionsV1, CapabilityRequestedRecordV1, CapabilityTokenLookupV1,
    PartitionScopeV1, StoredCapabilityRecordV1,
};
use riffdb_types::{Audience, CapabilityTokenDigest, DigestKeyId, Environment, TenantScope};
use std::num::{NonZeroU16, NonZeroU32};

pub(super) fn capability(f: &Fixture) -> StoredCapabilityRecordV1 {
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::FenceReplicationPrimary,
            )
            .unwrap(),
        ])
        .unwrap(),
        vec![],
        NonZeroU16::MIN,
        vec![],
    )
    .unwrap();
    let requested = CapabilityRequestedRecordV1::new(
        f.history.lineage().database_id(),
        Environment::new("test").unwrap(),
        f.principal.principal_id().clone(),
        f.principal.actor_kind(),
        NonZeroU32::new(60).unwrap(),
        vec![Audience::new("riffdb-test").unwrap()],
        grant,
    )
    .unwrap();
    StoredCapabilityRecordV1::active(
        f.principal.capability_id(),
        CapabilityTokenDigest::from_hmac_bytes(DigestKeyId::new(1).unwrap(), [1; 32]),
        requested,
        Timestamp::new(1_699_999_990, 0).unwrap(),
        Timestamp::new(1_700_000_050, 0).unwrap(),
        Admin::first(),
        f.request.request_id(),
    )
    .unwrap()
}

pub(super) fn seed(write: &redb::WriteTransaction, capability: &StoredCapabilityRecordV1) {
    let encoded = encode_capability_record_v1(capability).unwrap();
    write
        .open_table(crate::layout::CAPABILITIES)
        .unwrap()
        .insert(
            crate::keys::encode_capability_key(capability.capability_id()).as_slice(),
            encoded.as_bytes(),
        )
        .unwrap();
    let lookup =
        encode_capability_token_lookup_v1(CapabilityTokenLookupV1::new(capability.capability_id()))
            .unwrap();
    write
        .open_table(crate::layout::CAPABILITY_TOKENS)
        .unwrap()
        .insert(
            crate::keys::encode_capability_token_key(capability.token_digest()).as_slice(),
            lookup.as_bytes(),
        )
        .unwrap();
}

#[test]
fn fence_transaction_refuses_missing_current_authority_before_mutation() {
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("primary-fence-missing-authority");
    let database = f.database(&scope.join("db.redb"));
    let write = database.begin_write().unwrap();
    write
        .open_table(crate::layout::CAPABILITIES)
        .unwrap()
        .remove(crate::keys::encode_capability_key(f.principal.capability_id()).as_slice())
        .unwrap();
    write.commit().unwrap();
    let before = snapshot(&database);
    assert!(
        f.plan().unwrap().stage(&database).is_err(),
        "an unbound prepared principal must not authorize a physical fence"
    );
    assert_eq!(snapshot(&database), before);
}

#[test]
fn fence_authority_refuses_substituted_request_principal_and_invalid_time() {
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("primary-fence-authority-binding");
    let database = f.database(&scope.join("db.redb"));
    let before = snapshot(&database);
    let other_id =
        RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [99; 10]).unwrap();
    let other_operation =
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1_700_000_000_000, [99; 10])
            .unwrap();
    for request in [
        Request::new(
            other_id,
            f.request.operation_id(),
            f.request.target(),
            f.request.generation(),
        ),
        Request::new(
            f.request.request_id(),
            other_operation,
            f.request.target(),
            f.request.generation(),
        ),
        Request::new(
            f.request.request_id(),
            f.request.operation_id(),
            f.request.target(),
            Sequence::new(7).unwrap(),
        ),
    ] {
        let (awaiting, _) = f
            .plan()
            .unwrap()
            .open(&database)
            .unwrap()
            .read_transaction_current()
            .unwrap();
        assert!(
            awaiting
                .stage(request, f.principal.clone(), f.timestamp)
                .is_err()
        );
        assert_eq!(snapshot(&database), before);
    }
    for (principal, time) in [
        (
            AuditPrincipalV1::new(
                ActorId::new("other").unwrap(),
                ActorKind::Human,
                f.principal.capability_id(),
                NonZeroU64::MIN,
            ),
            f.timestamp,
        ),
        (
            f.principal.clone(),
            Timestamp::new(1_699_999_989, 999_999_999).unwrap(),
        ),
        (f.principal.clone(), capability(&f).expires_at()),
    ] {
        let (awaiting, _) = f
            .plan()
            .unwrap()
            .open(&database)
            .unwrap()
            .read_transaction_current()
            .unwrap();
        assert!(awaiting.stage(f.request, principal, time).is_err());
        assert_eq!(snapshot(&database), before);
    }
}

#[test]
fn fence_authority_uses_final_sample_and_abandons_without_writing() {
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("primary-fence-final-sample");
    let database = f.database(&scope.join("db.redb"));
    let before = snapshot(&database);
    drop(f.plan().unwrap().open(&database).unwrap());
    assert_eq!(snapshot(&database), before);
    let (awaiting, current) = f
        .plan()
        .unwrap()
        .open(&database)
        .unwrap()
        .read_transaction_current()
        .unwrap();
    let current = current.unwrap();
    assert_eq!(current.capability_id(), f.principal.capability_id());
    assert_eq!(current.lifecycle(), &CapabilityLifecycleV1::Active);
    drop(awaiting);
    assert_eq!(snapshot(&database), before);
    let (awaiting, _) = f
        .plan()
        .unwrap()
        .open(&database)
        .unwrap()
        .read_transaction_current()
        .unwrap();
    let final_time = Timestamp::new(1_700_000_001, 123).unwrap();
    let record = awaiting
        .stage(f.request, f.principal.clone(), final_time)
        .unwrap()
        .commit_for_test()
        .unwrap();
    assert_eq!(record.timestamp(), final_time);
    assert_eq!(record.principal(), &f.principal);
    assert_eq!(record.observed(), f.history.tail());
    let mut final_fixture = f;
    final_fixture.timestamp = final_time;
    final_fixture.assert_state(&database, true);
}

#[test]
fn fence_authority_refuses_revoked_capability_even_with_matching_revision() {
    for match_revision in [false, true] {
        let mut f = Fixture::new(5);
        let scope = crate::test_path::ScopedDirectory::new("primary-fence-revoked");
        let database = f.database(&scope.join("db.redb"));
        let revoked = capability(&f)
            .revoked(
                NonZeroU64::MIN,
                f.timestamp,
                Admin::new(2).unwrap(),
                riffdb_storage_api::RevocationReasonCodeV1::Requested,
            )
            .unwrap();
        let write = database.begin_write().unwrap();
        seed(&write, &revoked);
        write.commit().unwrap();
        if match_revision {
            f.principal = AuditPrincipalV1::new(
                f.principal.principal_id().clone(),
                f.principal.actor_kind(),
                f.principal.capability_id(),
                revoked.revision(),
            );
        }
        let before = snapshot(&database);
        assert!(f.plan().unwrap().stage(&database).is_err());
        assert_eq!(snapshot(&database), before);
    }
}

#[test]
fn fence_authority_refuses_missing_tables_and_reciprocal_lookup_without_repair() {
    for case in 0..3 {
        let f = Fixture::new(5);
        let scope = crate::test_path::ScopedDirectory::new("primary-fence-missing-lookup");
        let database = f.database(&scope.join("db.redb"));
        let write = database.begin_write().unwrap();
        match case {
            0 => {
                write.delete_table(crate::layout::CAPABILITIES).unwrap();
            }
            1 => {
                write
                    .delete_table(crate::layout::CAPABILITY_TOKENS)
                    .unwrap();
            }
            _ => {
                write
                    .open_table(crate::layout::CAPABILITY_TOKENS)
                    .unwrap()
                    .remove(
                        crate::keys::encode_capability_token_key(capability(&f).token_digest())
                            .as_slice(),
                    )
                    .unwrap();
            }
        }
        write.commit().unwrap();
        let before = snapshot(&database);
        assert!(f.plan().unwrap().stage(&database).is_err());
        assert_eq!(snapshot(&database), before);
    }
}
