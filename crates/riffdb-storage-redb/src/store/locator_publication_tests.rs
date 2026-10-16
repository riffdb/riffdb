//! Source and exact-directory publication share the engine identity fence.
use super::*;

fn direct_command_receipt_fixture() -> (
    crate::test_path::ScopedDirectory,
    RedbOperationalPorts,
    RedbReadAccess,
    RedbReadAccess,
) {
    let (segment, bytes) = crate::derived::follower_indexes::segment(
        vec![crate::derived::follower_indexes::capsule(
            CommitSequence::first(),
        )],
        None,
    );
    let scope = crate::test_path::ScopedDirectory::new("locator-direct-command-receipt");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    store.initialize_database(segment.database_id()).unwrap();
    store.shared.rebuild_test_source_indexes().unwrap();
    let ports = RedbOperationalPorts {
        shared: store.shared,
    };
    let before = ports.begin_read().unwrap();
    let lease = ports.shared.mutation_gate.acquire().unwrap();
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        RedbCommitProfile::Hardened,
        riffdb_storage_api::ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup,
    )
    .unwrap();
    write
        .open_table(COMMITS)
        .unwrap()
        .insert(
            crate::keys::encode_application_sequence_key(CommitSequence::first()).as_slice(),
            bytes.as_bytes(),
        )
        .unwrap();
    {
        let mut metadata = write.open_table(META).unwrap();
        let application = encode_application_sequence_allocator_v1(
            riffdb_storage_api::ApplicationSequenceAllocator::Next(CommitSequence::new(2).unwrap()),
        )
        .unwrap();
        metadata
            .insert(META_APPLICATION_SEQUENCE, application.as_bytes())
            .unwrap();
        let administration = encode_administration_sequence_allocator_v1(
            riffdb_storage_api::AdministrationSequenceAllocator::Next(
                AdministrationSequence::new(3).unwrap(),
            ),
        )
        .unwrap();
        metadata
            .insert(META_ADMINISTRATION_SEQUENCE, administration.as_bytes())
            .unwrap();
    }
    write.finish().unwrap().commit(&ports.shared).unwrap();
    drop(lease);
    let after = ports.begin_read().unwrap();
    (scope, ports, before, after)
}

// req: TXN-042, STO-032, REC-001
#[test]
fn direct_receipt_command_membership_change_keeps_successor_pending() {
    let (_scope, ports, before, after) = direct_command_receipt_fixture();
    assert!(
        !before
            .locator_capability()
            .require_complete()
            .unwrap()
            .has_command_at(CommitSequence::first())
            .unwrap()
    );
    assert!(
        after
            .read_value(
                crate::journal::JournalTable::Commits,
                &crate::keys::encode_application_sequence_key(CommitSequence::first())
            )
            .unwrap()
            .is_some()
    );
    assert!(after.locator_capability().is_pending());
    assert_eq!(
        after
            .locator_capability()
            .require_complete()
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::Unavailable
    );
    ports.shared.rebuild_test_source_indexes().unwrap();
    let complete = ports.begin_read().unwrap();
    assert!(
        complete
            .locator_capability()
            .require_complete()
            .unwrap()
            .has_command_at(CommitSequence::first())
            .unwrap()
    );
    assert!(
        after.locator_capability().is_pending(),
        "a later exact rebuild cannot replace an older captured pending owner"
    );
}

// req: TXN-042, STO-032, REC-001
#[test]
fn direct_receipt_status_only_successor_preserves_complete_command_source() {
    let (_scope, ports, _empty, _pending) = direct_command_receipt_fixture();
    ports.shared.rebuild_test_source_indexes().unwrap();
    let before = ports.begin_read().unwrap();
    let original = before.locator_capability().require_complete().unwrap();
    assert!(original.has_command_at(CommitSequence::first()).unwrap());
    let history = crate::changelog_v3_cursor::history(&before).unwrap();
    let lease = ports.shared.mutation_gate.acquire().unwrap();
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        RedbCommitProfile::Hardened,
        riffdb_storage_api::ChangelogAttributionV3::OutboxTransition,
    )
    .unwrap();
    // As in the isolated source-control receipt fixtures, this status payload
    // exercises receipt publication rather than outbox application validation.
    write
        .open_table(OUTBOX_STATUS)
        .unwrap()
        .insert(b"locator-status-only".as_slice(), [9].as_slice())
        .unwrap();
    write.finish().unwrap().commit(&ports.shared).unwrap();
    drop(lease);
    let after = ports.begin_read().unwrap();
    assert!(
        !before
            .locator_capability()
            .same_construction(after.locator_capability())
    );
    let successor = after.locator_capability().require_complete().unwrap();
    assert!(
        original
            .owner_handle()
            .same_owner(&successor.owner_handle())
    );
    assert!(successor.has_command_at(CommitSequence::first()).unwrap());
    assert_eq!(
        crate::changelog_v3_cursor::history(&before).unwrap(),
        history
    );
    assert_ne!(
        crate::changelog_v3_cursor::history(&after).unwrap(),
        history
    );
    assert_eq!(
        before
            .read_value(
                crate::journal::JournalTable::OutboxStatus,
                b"locator-status-only"
            )
            .unwrap(),
        None
    );
    assert_eq!(
        after
            .read_value(
                crate::journal::JournalTable::OutboxStatus,
                b"locator-status-only"
            )
            .unwrap(),
        Some(vec![9])
    );
}

// req: TXN-042, STO-032, REC-001
#[test]
fn locator_builder_current_ownership_requires_exact_capability_and_stable_epoch() {
    let scope = crate::test_path::ScopedDirectory::new("locator-current-construction-owner");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    let database = DatabaseId::from_unix_milliseconds_and_random(1, [0x59; 10]).unwrap();
    store.initialize_database(database).unwrap();
    let ports = RedbOperationalPorts {
        shared: store.shared,
    };
    let original = ports.begin_read().unwrap();
    assert!(original.locator_capability().is_pending());
    assert!(ports.shared.owns_current_locator_source(&original));

    // Retiring a physical cache does not withdraw the shared engine's exact
    // current capability. Its admitted construction can still make progress.
    ports.shared.current_read_root.write().unwrap().take();
    ports
        .shared
        .current_read_root_live
        .store(false, Ordering::Release);
    ports.shared.durable_read_frontier.write().unwrap().take();
    assert!(ports.shared.owns_current_locator_source(&original));

    let root = original.clone().into_shared().unwrap();
    let impostor = RedbReadAccess::Current(Arc::new(
        CheckpointRoot::new(ports.shared.database.begin_read().unwrap(), root.identity()).unwrap(),
    ));
    assert!(
        !ports.shared.owns_current_locator_source(&impostor),
        "equal physical identity without the exact capability is not ownership"
    );
    ports
        .shared
        .commit_durable(ports.shared.database.begin_write().unwrap())
        .unwrap();
    assert!(
        !ports.shared.owns_current_locator_source(&original),
        "a successor publication withdraws the old unprotected construction owner"
    );
    let successor = ports.begin_read().unwrap();
    assert!(ports.shared.owns_current_locator_source(&successor));
    assert!(original.locator_capability().is_pending());
}

// req: TXN-042, REC-004
#[test]
fn locator_generation_budget_refuses_before_durable_submission() {
    let (segment, _) = crate::derived::follower_indexes::segment(
        vec![crate::derived::follower_indexes::capsule(
            CommitSequence::first(),
        )],
        None,
    );
    let scope = crate::test_path::ScopedDirectory::new("locator-budget-before-fence");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    store.initialize_database(segment.database_id()).unwrap();
    let pool = crate::locator_memory::NativeLocatorPool::isolated_for_tests();
    let generation =
        LocatorGeneration::try_empty_in_pool(Arc::clone(&pool), LocatorAllocationClass::Ordinary)
            .unwrap();
    let mut indexes = TransientIndexes::try_empty(LocatorAllocationClass::Ordinary).unwrap();
    indexes.install_locator_generation(generation.clone());
    *store.shared.transient_indexes.write().unwrap() = TransientIndexState::Ready(indexes);
    store
        .shared
        .bind_test_locator_generation(generation)
        .unwrap();
    let publication = store
        .shared
        .durable_root_publication
        .load(Ordering::Acquire);
    let ports = RedbOperationalPorts {
        shared: store.shared,
    };
    let access = ports.begin_write().unwrap();
    let before = ports.shared.durable_commit_epoch.load(Ordering::Acquire);
    let _ordinary_capacity = pool.exhaust_ordinary_for_tests();
    let error = access
        .commit_for_with_delta(
            RedbTestOperation::CommandBatch,
            Some(TransientIndexDelta::CommandSegmentPublished(
                Arc::new(segment),
                None,
            )),
        )
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert_eq!(
        ports.shared.durable_commit_epoch.load(Ordering::Acquire),
        before
    );
    assert_eq!(
        ports
            .shared
            .durable_root_publication
            .load(Ordering::Acquire),
        publication
    );
    assert!(!ports.shared.write_fenced.load(Ordering::Acquire));
    assert!(
        ports
            .begin_read()
            .unwrap()
            .read_value(
                JournalTable::Commits,
                &crate::keys::encode_application_sequence_key(CommitSequence::first())
            )
            .unwrap()
            .is_none()
    );
}
