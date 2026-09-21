//! Real root readers must understand explicit V2 source evidence.
// req: REP-005, REC-001, STO-012
use super::*;

#[test]
fn primary_fence_audit_dispatch_preserves_canonical_bytes_and_sequence() {
    let record = Fixture::new(5).plan().unwrap().record;
    let canonical = encode_primary_fence_administration_v1(&record).unwrap();
    let decoded =
        crate::codec::decode_administration_audit_record_v1(canonical.as_bytes()).unwrap();
    assert_eq!(
        decoded.value().administration_sequence(),
        record.administration_sequence()
    );
    assert_eq!(
        decoded.value(),
        &riffdb_storage_api::StoredAdministrationAuditRecordV1::PrimaryFence(Box::new(record))
    );
    assert_eq!(
        crate::codec::encode_administration_audit_record_v1(decoded.value()).unwrap(),
        canonical
    );
}

#[test]
fn v2_partial_admission_routes_to_strict_recovery_without_repairing_roots() {
    let f = Fixture::new(5);
    for value in [
        encode_replication_primary_admission_v1(&f.admission)
            .unwrap()
            .into_bytes(),
        vec![0xff],
    ] {
        let scope = crate::test_path::ScopedDirectory::new("v2-partial-admission-recovery");
        let database = Database::create(scope.join("db.redb")).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert(
                crate::layout::META_RECORD_REGISTRY,
                encode_record_registry_v2(crate::changelog_v3_activation::PRE_V3_REGISTRY)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
            meta.insert(
                crate::primary_admission_roots::key().unwrap(),
                value.as_slice(),
            )
            .unwrap();
        }
        write.commit().unwrap();
        let before = snapshot(&database);
        let read = database.begin_read().unwrap();
        assert!(
            crate::changelog_v3_journal::has_recovery_roots(&read).unwrap(),
            "surviving admission must never select legacy recovery"
        );
        assert!(
            crate::changelog_v3_journal::plan_recovery(&read, &[], DualFrontier::INITIAL).is_err()
        );
        drop(read);
        let write = database.begin_write().unwrap();
        assert!(crate::changelog_v3_journal::has_write_recovery_roots(&write).unwrap());
        assert!(crate::changelog_v3_roots::read_checkpoint_roots_for_write(&write).is_err());
        write.abort().unwrap();
        assert_eq!(snapshot(&database), before);
    }
}

#[test]
fn v2_clean_root_admission_rollback_must_not_reuse_v1_binding() {
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("v2-clean-admission-rollback");
    let database = f.database(&scope.join("db.redb"));
    f.plan()
        .unwrap()
        .stage(&database)
        .unwrap()
        .commit_for_test()
        .unwrap();
    let read = database.begin_read().unwrap();
    crate::changelog_v3_roots::validate_retained_history(&read).unwrap();
    let binding = crate::clean_close::bounded_state_binding_hash(&read, [0x51; 32]).unwrap();
    drop(read);
    // A checksum-valid stale Active row must not erase the retained fence.
    let write = database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            crate::primary_admission_roots::key().unwrap(),
            encode_replication_primary_admission_v1(&f.admission)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    write.commit().unwrap();
    let read = database.begin_read().unwrap();
    assert!(crate::changelog_v3_roots::validate_retained_history(&read).is_err());
    let unchanged_binding =
        crate::clean_close::bounded_state_binding_hash(&read, [0x51; 32]).unwrap() == binding;
    assert!(
        unchanged_binding,
        "the accepted V1 binding must stay frozen"
    );
    assert!(crate::changelog_v3_roots::read_checkpoint_roots(&read).is_ok());
    assert!(
        !crate::store::changelog_lifecycle::clean_roots_available(&read).unwrap(),
        "V2 must decline the V1 certificate even when bounded roots accept stale Active"
    );
}

#[test]
fn v2_source_roots_validate_active_and_fenced_without_changing_bytes() {
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("v2-primary-root-readers");
    let database = f.database(&scope.join("db.redb"));
    for fenced in [false, true] {
        if fenced {
            f.plan()
                .unwrap()
                .stage(&database)
                .unwrap()
                .commit_for_test()
                .unwrap();
        }
        let before = snapshot(&database);
        let read = database.begin_read().unwrap();
        let roots = crate::changelog_v3_roots::read_checkpoint_roots(&read);
        let admission = crate::primary_admission_roots::read_source_admission(&read).unwrap();
        assert_eq!(admission.lineage(), f.history.lineage());
        assert_eq!(admission.fence().is_some(), fenced);
        assert!(
            roots.is_ok(),
            "explicit V2 source roots must be readable: {roots:?}"
        );
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(&read).unwrap(),
            roots.unwrap()
        );
        assert_eq!(
            crate::primary_admission_roots::require_unfenced(&read.open_table(META).unwrap())
                .is_err(),
            fenced,
        );
        drop(read);
        let write = database.begin_write().unwrap();
        assert!(
            crate::changelog_v3_roots::read_checkpoint_roots_for_write(&write)
                .unwrap()
                .is_some()
        );
        assert!(
            crate::changelog_v3_roots::validate_retained_history_for_write(&write)
                .unwrap()
                .is_some()
        );
        write.abort().unwrap();
        assert_eq!(snapshot(&database), before);
    }
}

pub(super) fn assert_source_validation_refuses(database: &Database) {
    let before = snapshot(database);
    let read = database.begin_read().unwrap();
    assert!(crate::changelog_v3_roots::validate_retained_history(&read).is_err());
    drop(read);
    let write = database.begin_write().unwrap();
    assert!(crate::changelog_v3_roots::validate_retained_history_for_write(&write).is_err());
    write.abort().unwrap();
    assert_eq!(snapshot(database), before);
}

#[test]
fn v2_attached_roots_require_absent_source_admission_and_empty_source_tables() {
    use riffdb_storage_api::ReplicationFollowerStateV3;
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("v2-follower-root-readers");
    let database = f.database(&scope.join("db.redb"));
    let write = database.begin_write().unwrap();
    for definition in [HISTORY, SOURCE_HOLDS] {
        write
            .open_table(definition)
            .unwrap()
            .retain(|_, _| false)
            .unwrap();
    }
    let follower =
        ReplicationFollowerStateV3::attached(f.history.lineage(), f.history.tail(), None).unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            N::ReplicationFollowerState.metadata_key().unwrap(),
            encode_replication_follower_state_v3(follower)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    write.commit().unwrap();
    assert_source_validation_refuses(&database);
    let write = database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .remove(crate::primary_admission_roots::key().unwrap())
        .unwrap();
    write.commit().unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&read).unwrap(),
        Some(f.history)
    );
    drop(read);
    // Source-only rows cannot hide behind an attached follower root.
    let write = database.begin_write().unwrap();
    write
        .open_table(HISTORY)
        .unwrap()
        .insert(
            f.registration_receipt
                .binding()
                .sequence
                .get()
                .to_be_bytes()
                .as_slice(),
            f.registration_receipt.encode().unwrap().as_slice(),
        )
        .unwrap();
    write.commit().unwrap();
    assert_source_validation_refuses(&database);
    assert!(
        crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap()).is_err()
    );
}

#[test]
fn fenced_source_refuses_control_mutation_at_the_physical_receipt_owner() {
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("v2-fenced-control-refusal");
    let database = f.database(&scope.join("db.redb"));
    f.plan()
        .unwrap()
        .stage(&database)
        .unwrap()
        .commit_for_test()
        .unwrap();
    let history = f.plan().unwrap().successor;
    let capability = authorization::capability(&f);
    let revoked = capability
        .revoked(
            NonZeroU64::MIN,
            f.timestamp,
            Admin::new(2).unwrap(),
            riffdb_storage_api::RevocationReasonCodeV1::Requested,
        )
        .unwrap();
    let receipt = Receipt::new_for_catalog(
        Binding {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence: history.tail().sequence().checked_next().unwrap(),
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: history.tail().frontier(),
            prior_history_hash: history.tail().history_hash(),
        },
        Source::CapabilityAdministration,
        vec![
            Mutation::replace(
                N::Capabilities,
                &crate::keys::encode_capability_key(capability.capability_id()),
                encode_capability_record_v1(&capability).unwrap().as_bytes(),
                encode_capability_record_v1(&revoked).unwrap().as_bytes(),
            )
            .unwrap(),
        ],
        history.lineage().catalog_digest(),
    )
    .unwrap();
    let before = snapshot(&database);
    let attempt = crate::changelog_v3_write::PreparedImmediateReceipt::apply(
        &database,
        crate::store::RedbCommitProfile::Hardened,
        &receipt,
    );
    assert!(
        attempt.is_err(),
        "a durable fence must refuse control authority writes"
    );
    drop(attempt);
    assert_eq!(snapshot(&database), before);
}
