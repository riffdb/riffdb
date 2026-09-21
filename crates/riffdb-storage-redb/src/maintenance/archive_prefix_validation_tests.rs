//! Isolated private-root validation; complete command reconstruction has its
//! own graph and production-storage tests.
// req: REP-005, REP-007, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, AuthoritativeStateCatalogV1,
    AuthoritativeStateCatalogV2, ChangelogLineageV3, DatabaseInitializationPort, LeadershipEpochV1,
    ReplicationPrimaryAdmissionV1,
};
use riffdb_types::{AdministrationSequence, DatabaseId};

#[test]
fn private_archive_roots_bind_exact_catalog_and_exclude_primary_admission() {
    for v2 in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("private-archive-catalog-binding");
        let mut store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
        let id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x79; 10]).unwrap();
        if v2 {
            store.initialize_database(id).unwrap();
        } else {
            store.initialize_legacy_fixture(id).unwrap();
            crate::changelog_v3_activation::activate_validated(
                store.shared.database.begin_write().unwrap(),
                ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
                DualFrontier::INITIAL,
            )
            .unwrap();
        }
        let read = store.shared.database.begin_read().unwrap();
        let history = crate::changelog_v3_roots::validate_retained_history(&read)
            .unwrap()
            .unwrap();
        drop(read);
        let binding = PrivateArchiveValidationBinding::new(
            history,
            DualFrontier::new(
                Some(CommitSequence::first()),
                Some(AdministrationSequence::first()),
            ),
        );
        let write = store.shared.database.begin_write().unwrap();
        write
            .open_table(crate::changelog_v3_activation::HISTORY)
            .unwrap()
            .retain(|_, _| false)
            .unwrap();
        {
            let mut meta = write.open_table(crate::layout::META).unwrap();
            meta.remove(N::ReplicationFollowerState.metadata_key().unwrap())
                .unwrap();
            meta.remove(crate::primary_admission_roots::key().unwrap())
                .unwrap();
            meta.insert(
                crate::layout::META_APPLICATION_SEQUENCE,
                encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::Next(
                    CommitSequence::new(2).unwrap(),
                ))
                .unwrap()
                .as_bytes(),
            )
            .unwrap();
            meta.insert(
                crate::layout::META_ADMINISTRATION_SEQUENCE,
                encode_administration_sequence_allocator_v1(AdministrationSequenceAllocator::Next(
                    AdministrationSequence::new(2).unwrap(),
                ))
                .unwrap()
                .as_bytes(),
            )
            .unwrap();
        }
        write.commit().unwrap();
        assert_validation(&store, binding, true);
        let write = store.shared.database.begin_write().unwrap();
        let wrong = if v2 {
            encode_authoritative_state_catalog_v1(AuthoritativeStateCatalogV1).unwrap()
        } else {
            encode_authoritative_state_catalog_v2(AuthoritativeStateCatalogV2).unwrap()
        };
        write
            .open_table(crate::layout::META)
            .unwrap()
            .insert(
                N::AuthoritativeStateCatalog.metadata_key().unwrap(),
                wrong.as_bytes(),
            )
            .unwrap();
        write.commit().unwrap();
        assert_validation(&store, binding, false);
        let write = store.shared.database.begin_write().unwrap();
        let exact = if v2 {
            encode_authoritative_state_catalog_v2(AuthoritativeStateCatalogV2).unwrap()
        } else {
            encode_authoritative_state_catalog_v1(AuthoritativeStateCatalogV1).unwrap()
        };
        {
            let mut meta = write.open_table(crate::layout::META).unwrap();
            meta.insert(
                N::AuthoritativeStateCatalog.metadata_key().unwrap(),
                exact.as_bytes(),
            )
            .unwrap();
            let admission = ReplicationPrimaryAdmissionV1::active(
                ChangelogLineageV3::new_with_catalog(
                    id,
                    1,
                    LeadershipEpochV1::initial(),
                    AuthoritativeStateCatalogV2.digest(),
                )
                .unwrap(),
            )
            .unwrap();
            meta.insert(
                crate::primary_admission_roots::key().unwrap(),
                encode_replication_primary_admission_v1(&admission)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
        }
        write.commit().unwrap();
        assert_validation(&store, binding, false);
    }
}

fn assert_validation(
    store: &crate::RedbStore,
    binding: PrivateArchiveValidationBinding,
    valid: bool,
) {
    let read = store.shared.database.begin_read().unwrap();
    assert_eq!(binding.validate(&read).is_ok(), valid, "read snapshot");
    drop(read);
    let write = store.shared.database.begin_write().unwrap();
    assert_eq!(
        binding.validate_for_write(&write).is_ok(),
        valid,
        "write snapshot"
    );
    write.abort().unwrap();
}
