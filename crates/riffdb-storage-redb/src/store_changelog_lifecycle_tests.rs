//! Actual lifecycle writers with isolated V3 activation, not startup activation.
// req: REP-003, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AuthoritativeTransactionV3, ChangelogAttributionV3, ChangelogLineageV3, LeadershipEpochV1,
};

#[test]
// req: REP-002, REP-003
fn follower_restart_lifecycle_must_preserve_the_exact_upstream_successor() {
    use riffdb_storage_api::{
        AuthoritativeNamespaceV1 as N, ReplicationFollowerStateV3,
        proto_codec::encode_replication_follower_state_v3,
    };
    let scope = crate::test_path::ScopedDirectory::new("v3-follower-lifecycle-conflict");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x74; 10]).unwrap();
    store.initialize_legacy_fixture(database_id).unwrap();
    let lineage = ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap();
    let original = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        lineage,
        DualFrontier::INITIAL,
    )
    .unwrap();
    // Isolated attached-root fixture: this is not a bootstrap implementation or
    // an alternative production attachment authority.
    let attached = ReplicationFollowerStateV3::attached(lineage, original.tail(), None).unwrap();
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            N::ReplicationFollowerState.metadata_key().unwrap(),
            encode_replication_follower_state_v3(attached)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    store.shared.commit_durable(write).unwrap();
    let epoch = store.shared.durable_commit_epoch();
    store
        .shared
        .advance_dirty_lifecycle_before_activation(database_id, 1, None)
        .unwrap();
    let observed = crate::changelog_v3_roots::validate_retained_history(
        &store.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        observed.tail().sequence(),
        original.tail().sequence(),
        "local follower activation must not allocate the upstream's next V3 sequence",
    );
    assert_eq!(
        observed, original,
        "restart must preserve the upstream position and hash"
    );
    assert_eq!(store.shared.durable_commit_epoch(), epoch);
    let retained =
        crate::startup::read_retained_metadata_pub(&store.shared.database.begin_read().unwrap())
            .unwrap();
    crate::validated_prefix::write_validated_prefix_checkpoint(
        &store.shared,
        &retained,
        crate::validated_prefix::CheckpointPurpose::StartupValidation,
    )
    .unwrap();
    assert_eq!(
        store.shared.durable_commit_epoch(),
        epoch,
        "follower checkpointing must not allocate or rewrite source evidence"
    );
    assert!(
        store
            .shared
            .database
            .begin_read()
            .unwrap()
            .open_table(META)
            .unwrap()
            .get(META_CLEAN_CLOSE_LIFECYCLE)
            .unwrap()
            .is_none()
    );
    let close = store.shared.complete_graceful_close();
    assert_eq!(
        close.disposition(),
        crate::GracefulCheckpointDispositionV1::ClassificationFailed,
        "attached state cannot enter the source-only graceful close owner"
    );
    assert_eq!(store.shared.durable_commit_epoch(), epoch);
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            META_CLEAN_CLOSE_LIFECYCLE,
            b"conflicting-source-certificate".as_slice(),
        )
        .unwrap();
    store.shared.commit_durable(write).unwrap();
    let epoch = store.shared.durable_commit_epoch();
    assert_eq!(
        store
            .shared
            .advance_dirty_lifecycle_before_activation(database_id, 1, None)
            .unwrap_err()
            .kind(),
        StorageErrorKind::CorruptData,
        "follower activation must not consume or repair a source certificate"
    );
    assert_eq!(store.shared.durable_commit_epoch(), epoch);
    drop(store);
    assert_eq!(
        RedbStore::open(scope.join("db.redb")).err().unwrap().kind(),
        StorageErrorKind::InvariantViolation,
        "source-mode reopen cannot implicitly promote the attached follower"
    );
}

#[test]
fn dirty_clean_and_consumption_each_commit_one_non_recursive_v3_receipt() {
    let scope = crate::test_path::ScopedDirectory::new("v3-real-lifecycle");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x72; 10]).unwrap();
    store.initialize_legacy_fixture(database_id).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let initial_epoch = store.shared.durable_commit_epoch();
    store
        .shared
        .advance_dirty_lifecycle_before_activation(database_id, 1, None)
        .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), initial_epoch + 1);
    assert_receipt(&store.shared, 2, ChangelogAttributionV3::DirtyActivation);

    let _close = store.shared.complete_graceful_close();
    assert_eq!(store.shared.durable_commit_epoch(), initial_epoch + 2);
    assert_receipt(&store.shared, 3, ChangelogAttributionV3::CleanClose);
    let clean = {
        let read = store.shared.database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        let row = meta.get(META_CLEAN_CLOSE_LIFECYCLE).unwrap().unwrap();
        let clean = crate::clean_close::CleanCloseLifecycle::decode(row.value()).unwrap();
        assert!(matches!(
            clean.state(),
            crate::clean_close::CleanCloseState::Clean(_)
        ));
        clean
    };
    // This explicit next activation is a new lifecycle, not a write performed
    // by the final close. Consume the exact verified predecessor once.
    store
        .shared
        .advance_dirty_lifecycle_before_activation(database_id, 1, Some(clean))
        .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), initial_epoch + 3);
    assert_receipt(&store.shared, 4, ChangelogAttributionV3::DirtyActivation);
    assert!(
        store
            .shared
            .advance_dirty_lifecycle_before_activation(database_id, 1, Some(clean))
            .is_err()
    );
    assert_eq!(store.shared.durable_commit_epoch(), initial_epoch + 3);
    assert_receipt(&store.shared, 4, ChangelogAttributionV3::DirtyActivation);
}

fn assert_receipt(shared: &SharedRedb, sequence: u64, attribution: ChangelogAttributionV3) {
    let read = shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    assert_eq!(
        history.tail().sequence().get(),
        sequence,
        "lifecycle transaction needs its own receipt"
    );
    let rows = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let row = rows
        .get(sequence.to_be_bytes().as_slice())
        .unwrap()
        .unwrap();
    let receipt = AuthoritativeTransactionV3::decode(row.value()).unwrap();
    assert_eq!(receipt.attribution(), attribution);
    assert!(receipt.mutations().is_empty());
    assert_eq!(
        receipt.binding().predecessor_frontier,
        DualFrontier::INITIAL
    );
    assert_eq!(receipt.binding().covered_frontier, DualFrontier::INITIAL);
}

#[test]
fn lifecycle_receipt_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_V3_LIFECYCLE_PATH") else {
        return;
    };
    let mut store = RedbStore::open(path).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x72; 10]).unwrap();
    store.initialize_legacy_fixture(database_id).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    store
        .shared
        .advance_dirty_lifecycle_before_activation(database_id, 1, None)
        .unwrap();
    let _close = store.shared.complete_graceful_close();
    panic!("the requested lifecycle crash edge was not reached");
}

#[test]
fn lifecycle_process_crashes_preserve_atomic_control_and_history_roots() {
    for (edge, sequence) in [
        ("dirty-staged", 1),
        ("dirty-committed", 2),
        ("clean-staged", 2),
        ("clean-committed", 3),
    ] {
        let scope = crate::test_path::ScopedDirectory::new("v3-lifecycle-crash");
        let path = scope.join("db.redb");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::changelog_lifecycle_tests::lifecycle_receipt_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_V3_LIFECYCLE_PATH", &path)
            .env("RIFFDB_V3_LIFECYCLE_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        let mut previous = None;
        for _ in 0..2 {
            let database = Database::open(&path).unwrap();
            let read = database.begin_read().unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap();
            assert_eq!(history.tail().sequence().get(), sequence);
            let meta = read.open_table(META).unwrap();
            let lifecycle = meta.get(META_CLEAN_CLOSE_LIFECYCLE).unwrap();
            assert_eq!(lifecycle.is_some(), sequence > 1);
            if let Some(lifecycle) = &lifecycle {
                let lifecycle =
                    crate::clean_close::CleanCloseLifecycle::decode(lifecycle.value()).unwrap();
                assert_eq!(
                    matches!(
                        lifecycle.state(),
                        crate::clean_close::CleanCloseState::Clean(_)
                    ),
                    sequence == 3
                );
            }
            let rows = read
                .open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap();
            let encoded = rows
                .get(sequence.to_be_bytes().as_slice())
                .unwrap()
                .unwrap()
                .value()
                .to_vec();
            let receipt = AuthoritativeTransactionV3::decode(&encoded).unwrap();
            if sequence == 1 {
                assert_eq!(receipt.mutations().len(), 1);
                assert_eq!(
                    receipt.mutations()[0].namespace(),
                    riffdb_storage_api::AuthoritativeNamespaceV1::RecordRegistry
                );
            } else {
                assert!(receipt.mutations().is_empty());
            }
            assert_eq!(
                receipt.attribution(),
                match sequence {
                    1 => ChangelogAttributionV3::V3Activation,
                    2 => ChangelogAttributionV3::DirtyActivation,
                    _ => ChangelogAttributionV3::CleanClose,
                }
            );
            if let Some(previous) = &previous {
                assert_eq!(previous, &encoded);
            }
            previous = Some(encoded);
        }
    }
}

#[test]
fn malformed_lifecycle_history_declines_clean_roots_and_refuses_dirty_before_mutation() {
    let scope = crate::test_path::ScopedDirectory::new("v3-lifecycle-invalid-root");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x72; 10]).unwrap();
    store.initialize_legacy_fixture(database_id).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    store
        .shared
        .advance_dirty_lifecycle_before_activation(database_id, 1, None)
        .unwrap();
    let _close = store.shared.complete_graceful_close();
    let pinned = store.shared.database.begin_read().unwrap();
    assert!(changelog_lifecycle::clean_roots_available(&pinned).unwrap());
    let transaction = store.shared.database.begin_write().unwrap();
    transaction
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .remove(3u64.to_be_bytes().as_slice())
        .unwrap();
    transaction.commit().unwrap();
    let current = store.shared.database.begin_read().unwrap();
    assert!(!changelog_lifecycle::clean_roots_available(&current).unwrap());
    assert!(changelog_lifecycle::clean_roots_available(&pinned).unwrap());
    let lifecycle = current
        .open_table(META)
        .unwrap()
        .get(META_CLEAN_CLOSE_LIFECYCLE)
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let epoch = store.shared.durable_commit_epoch();
    assert_eq!(
        store
            .shared
            .advance_dirty_lifecycle_before_activation(database_id, 1, None)
            .unwrap_err()
            .kind(),
        StorageErrorKind::CorruptData
    );
    assert_eq!(store.shared.durable_commit_epoch(), epoch);
    let after = store.shared.database.begin_read().unwrap();
    assert_eq!(
        after
            .open_table(META)
            .unwrap()
            .get(META_CLEAN_CLOSE_LIFECYCLE)
            .unwrap()
            .unwrap()
            .value(),
        lifecycle
    );
}
