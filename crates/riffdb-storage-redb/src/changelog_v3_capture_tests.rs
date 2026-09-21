//! Exact mutation capture at the private physical write boundary.
// req: REP-003, REC-001, STO-012

use super::*;

#[test]
fn direct_table_capture_retains_original_preconditions_and_only_net_changes() {
    let scope = crate::test_path::ScopedDirectory::new("v3-direct-capture");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let transaction = database.begin_write().unwrap();
    transaction
        .open_table(ENTITIES)
        .unwrap()
        .insert(b"original".as_slice(), b"first".as_slice())
        .unwrap();
    transaction.commit().unwrap();
    let transaction = database.begin_write().unwrap();
    let capture = crate::changelog_v3_capture::MutationCapture::default();
    {
        let mut table = capture.table(transaction.open_table(ENTITIES).unwrap());
        assert_eq!(
            table
                .insert(b"original".as_slice(), b"second".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"first"
        );
        assert_eq!(
            table
                .remove(b"original".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"second"
        );
        table
            .insert(b"cancelled".as_slice(), b"transient".as_slice())
            .unwrap();
        table.remove(b"cancelled".as_slice()).unwrap();
        table
            .insert(b"retained".as_slice(), b"complete".as_slice())
            .unwrap();
    }
    let mutations = capture.finish().unwrap();
    assert_eq!(mutations.len(), 2);
    assert_eq!(mutations[0].key(), b"original");
    assert_eq!(mutations[0].value(), None);
    assert!(mutations[0].matches_prior(Some(b"first")));
    assert!(!mutations[0].matches_prior(Some(b"second")));
    assert_eq!(mutations[1].key(), b"retained");
    assert_eq!(mutations[1].value(), Some(b"complete".as_slice()));
    assert!(mutations[1].matches_prior(None));
    // Capture itself owns no commit: abort retains every original row.
    transaction.abort().unwrap();
    let snapshot = database.begin_read().unwrap();
    let table = snapshot.open_table(ENTITIES).unwrap();
    assert_eq!(
        table.get(b"original".as_slice()).unwrap().unwrap().value(),
        b"first"
    );
    assert!(table.get(b"retained".as_slice()).unwrap().is_none());
}

#[test]
fn direct_table_capture_refuses_control_mutations_and_poisons_the_whole_plan() {
    let scope = crate::test_path::ScopedDirectory::new("v3-direct-capture-control");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let transaction = database.begin_write().unwrap();
    let capture = crate::changelog_v3_capture::MutationCapture::default();
    {
        let mut table = capture.table(transaction.open_table(ENTITIES).unwrap());
        table
            .insert(b"valid".as_slice(), b"before-refusal".as_slice())
            .unwrap();
    }
    {
        let mut meta = capture.table(transaction.open_table(META).unwrap());
        let key = key(N::NextChangelogTransaction).unwrap();
        assert!(meta.insert(key, b"forged-control".as_slice()).is_err());
        assert!(meta.get(key).unwrap().is_none());
    }
    assert!(capture.finish().is_err());
    transaction.abort().unwrap();
}

#[test]
fn direct_table_capture_refuses_whole_receipt_overflow_before_the_offending_write() {
    let scope = crate::test_path::ScopedDirectory::new("v3-direct-capture-bound");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let transaction = database.begin_write().unwrap();
    let capture = crate::changelog_v3_capture::MutationCapture::default();
    let value = vec![0x42; 64 * 1024];
    let mut refused = false;
    {
        let mut table = capture.table(transaction.open_table(ENTITIES).unwrap());
        for index in 0_u64..1024 {
            let key = index.to_be_bytes();
            if table.insert(key.as_slice(), value.as_slice()).is_err() {
                assert!(table.get(key.as_slice()).unwrap().is_none());
                refused = true;
                assert!(
                    table
                        .insert(b"after-error".as_slice(), b"small".as_slice())
                        .is_err()
                );
                assert!(table.get(b"after-error".as_slice()).unwrap().is_none());
                break;
            }
        }
    }
    assert!(
        refused,
        "a physical transaction cannot outgrow one complete V3 receipt"
    );
    assert_eq!(
        capture.finish().unwrap_err().kind(),
        StorageErrorKind::LimitExceeded
    );
    transaction.abort().unwrap();
    assert!(
        database
            .begin_read()
            .unwrap()
            .open_table(ENTITIES)
            .unwrap()
            .is_empty()
            .unwrap()
    );
}

#[test]
fn captured_immediate_transaction_commits_one_exact_receipt_without_reopening_the_writer() {
    use crate::changelog_v3_write::CapturedImmediateWrite;
    let scope = crate::test_path::ScopedDirectory::new("v3-captured-immediate");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let write = CapturedImmediateWrite::begin(
        &database,
        RedbCommitProfile::Hardened,
        ChangelogAttributionV3::CatalogAdministration,
    )
    .unwrap();
    {
        let mut table = write.open_table(ENTITIES).unwrap();
        table
            .insert(b"key".as_slice(), b"original".as_slice())
            .unwrap();
        table
            .insert(b"key".as_slice(), b"final".as_slice())
            .unwrap();
    }
    let pin = database.begin_read().unwrap();
    let prepared = write.finish().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&pin).unwrap(),
        Some(history)
    );
    prepared.commit_for_test().unwrap();
    let read = database.begin_read().unwrap();
    let after = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    assert_eq!(
        after.tail().sequence().get(),
        history.tail().sequence().get() + 1
    );
    let table = read.open_table(HISTORY).unwrap();
    let row = table
        .get(after.tail().sequence().get().to_be_bytes().as_slice())
        .unwrap()
        .unwrap();
    let receipt = AuthoritativeTransactionV3::decode(row.value()).unwrap();
    assert_eq!(receipt.mutations().len(), 1);
    assert!(receipt.mutations()[0].matches_prior(None));
    assert_eq!(receipt.mutations()[0].value(), Some(b"final".as_slice()));
    assert!(
        pin.open_table(ENTITIES)
            .unwrap()
            .get(b"key".as_slice())
            .unwrap()
            .is_none()
    );
}

#[test]
fn captured_immediate_binds_actual_allocator_post_images_and_refuses_lineage_changes() {
    use crate::changelog_v3_write::CapturedImmediateWrite;
    use riffdb_types::CommitSequence;
    let scope = crate::test_path::ScopedDirectory::new("v3-captured-allocators");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let write = CapturedImmediateWrite::begin(
        &database,
        RedbCommitProfile::Standard,
        ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup,
    )
    .unwrap();
    write
        .open_table(COMMITS)
        .unwrap()
        .insert(1_u64.to_be_bytes().as_slice(), b"opaque-commit".as_slice())
        .unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            META_APPLICATION_SEQUENCE,
            encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::Next(
                CommitSequence::new(2).unwrap(),
            ))
            .unwrap()
            .as_bytes(),
        )
        .unwrap();
    write.finish().unwrap().commit_for_test().unwrap();
    let read = database.begin_read().unwrap();
    let after = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    assert_eq!(
        after.tail().frontier(),
        DualFrontier::new(CommitSequence::new(1), None)
    );
    assert_eq!(
        after.tail().sequence().get(),
        history.tail().sequence().get() + 1
    );
    let original_metadata = metadata(&database);
    let write = CapturedImmediateWrite::begin(
        &database,
        RedbCommitProfile::Hardened,
        ChangelogAttributionV3::CatalogAdministration,
    )
    .unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            META_HISTORY_INCARNATION,
            encode_history_incarnation_v1(2).unwrap().as_bytes(),
        )
        .unwrap();
    assert!(write.finish().is_err());
    assert_eq!(metadata(&database), original_metadata);
}

#[test]
fn captured_immediate_noop_keeps_position_and_missing_table_poisons_finish() {
    use crate::changelog_v3_write::CapturedImmediateWrite;
    let scope = crate::test_path::ScopedDirectory::new("v3-captured-noop");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let write = CapturedImmediateWrite::begin(
        &database,
        RedbCommitProfile::Standard,
        ChangelogAttributionV3::CatalogAdministration,
    )
    .unwrap();
    {
        let mut table = write.open_table(ENTITIES).unwrap();
        table
            .insert(b"cancelled".as_slice(), b"temporary".as_slice())
            .unwrap();
        table.remove(b"cancelled".as_slice()).unwrap();
        fn empty(table: &impl redb::ReadableTable<&'static [u8], &'static [u8]>) -> bool {
            table.is_empty().unwrap()
        }
        assert!(empty(&table));
    }
    write.finish().unwrap().commit_for_test().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
            .unwrap(),
        Some(history)
    );
    let raw = database.begin_write().unwrap();
    raw.delete_table(ENTITIES).unwrap();
    raw.commit().unwrap();
    let before = metadata(&database);
    let write = CapturedImmediateWrite::begin(
        &database,
        RedbCommitProfile::Standard,
        ChangelogAttributionV3::CatalogAdministration,
    )
    .unwrap();
    assert!(write.open_table(ENTITIES).is_err());
    assert!(write.finish().is_err());
    assert_eq!(metadata(&database), before);
    assert!(database.begin_read().unwrap().open_table(ENTITIES).is_err());
}

#[test]
fn captured_immediate_process_child() {
    use crate::changelog_v3_write::CapturedImmediateWrite;
    let Some(path) = std::env::var_os("RIFFDB_V3_CAPTURE_PATH") else {
        return;
    };
    let database = fixture(Path::new(&path), PRE_V3_REGISTRY);
    activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let write = CapturedImmediateWrite::begin(
        &database,
        RedbCommitProfile::Hardened,
        ChangelogAttributionV3::CatalogAdministration,
    )
    .unwrap();
    write
        .open_table(ENTITIES)
        .unwrap()
        .insert(b"key".as_slice(), b"complete".as_slice())
        .unwrap();
    write.finish().unwrap().commit_for_test().unwrap();
    panic!("the requested deterministic crash edge was not reached");
}

#[test]
fn captured_immediate_process_crashes_leave_old_or_complete_receipted_state() {
    for edge in ["mutations", "receipt", "roots", "committed"] {
        let scope = crate::test_path::ScopedDirectory::new("v3-captured-crash");
        let path = scope.join("db.redb");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("changelog_v3_activation::tests::direct_capture::captured_immediate_process_child")
            .arg("--nocapture")
            .env("RIFFDB_V3_CAPTURE_PATH", &path)
            .env("RIFFDB_V3_DIRECT_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "crash edge {edge}");
        let database = Database::open(&path).unwrap();
        let read = database.begin_read().unwrap();
        let history = crate::changelog_v3_roots::validate_retained_history(&read)
            .unwrap()
            .unwrap();
        let entities = read.open_table(ENTITIES).unwrap();
        let row = entities.get(b"key".as_slice()).unwrap();
        if edge == "committed" {
            assert_eq!(history.tail().sequence().get(), 2);
            assert_eq!(row.unwrap().value(), b"complete");
            let table = read.open_table(HISTORY).unwrap();
            let encoded = table.get(2_u64.to_be_bytes().as_slice()).unwrap().unwrap();
            let receipt = AuthoritativeTransactionV3::decode(encoded.value()).unwrap();
            assert_eq!(receipt.mutations().len(), 1);
            assert_eq!(receipt.mutations()[0].value(), Some(b"complete".as_slice()));
            assert!(receipt.mutations()[0].matches_prior(None));
        } else {
            assert_eq!(history.tail().sequence().get(), 1);
            assert!(row.is_none());
        }
    }
}

#[test]
// req: REP-005
fn primary_fencing_cannot_enter_generic_direct_mutation_owners() {
    use crate::changelog_v3_write::{CapturedImmediateWrite, require_direct_attribution};
    use riffdb_storage_api::ChangelogAttributionV3;
    assert_eq!(
        require_direct_attribution(ChangelogAttributionV3::PrimaryFence)
            .unwrap_err()
            .kind(),
        StorageErrorKind::InvariantViolation,
    );
    let scope = crate::test_path::ScopedDirectory::new("primary-fence-direct-refusal");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let before = metadata(&database);
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        let result =
            CapturedImmediateWrite::begin(&database, profile, ChangelogAttributionV3::PrimaryFence);
        assert!(
            matches!(result, Err(error) if error.kind() == StorageErrorKind::InvariantViolation)
        );
        assert_eq!(metadata(&database), before);
    }
}
