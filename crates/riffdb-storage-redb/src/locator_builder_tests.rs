// req: TXN-042, STO-032, REC-001
use super::*;
use crate::checkpoint_root::CheckpointRoot;
use crate::locator_capability::LocatorCapability;
use crate::test_path::ScopedDirectory;
use redb::ReadableDatabase;
use riffdb_storage_api::{
    ApplicationSequenceAllocator, DatabaseInitializationPort, StoredCommandSegmentV1,
};
use riffdb_types::{CommitSequence, DatabaseId, SchemaHash};
use std::sync::Barrier;

fn source() -> (ScopedDirectory, redb::Database, RedbReadAccess) {
    let scope = ScopedDirectory::new("locator-builder-queue");
    let database = redb::Database::create(scope.join("source.redb")).unwrap();
    let identity = DatabaseId::from_unix_milliseconds_and_random(1, [9; 10]).unwrap();
    let registry = SchemaHash::from_bytes([2; 32]);
    let write = database.begin_write().unwrap();
    {
        let mut metadata = write.open_table(crate::layout::META).unwrap();
        let encoded = crate::codec::encode_database_identity_v1(identity).unwrap();
        metadata
            .insert(crate::layout::META_DATABASE_ID, encoded.as_bytes())
            .unwrap();
        let encoded = crate::codec::encode_history_incarnation_v1(1).unwrap();
        metadata
            .insert(crate::layout::META_HISTORY_INCARNATION, encoded.as_bytes())
            .unwrap();
        let encoded = crate::codec::encode_record_registry_v2(registry).unwrap();
        metadata
            .insert(crate::layout::META_RECORD_REGISTRY, encoded.as_bytes())
            .unwrap();
    }
    write.commit().unwrap();
    let root = Arc::new(CheckpointRoot::new(database.begin_read().unwrap(), 1).unwrap());
    root.locator_capability()
        .bind_source(
            DatabaseId::from_unix_milliseconds_and_random(1, [9; 10]).unwrap(),
            1,
            SchemaHash::from_bytes([2; 32]),
        )
        .unwrap();
    (scope, database, RedbReadAccess::Current(root))
}
fn job(access: RedbReadAccess) -> BuilderJob {
    let charge = NativeLocatorPool::global()
        .admit(
            std::mem::size_of::<BuilderJob>() + 64,
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
    BuilderJob {
        access,
        shared: None,
        custody: None,
        _charge: charge,
    }
}

fn append_segment(
    store: &crate::RedbStore,
    sequence: CommitSequence,
    prior: Option<riffdb_storage_api::CommandSegmentDigestV1>,
) -> StoredCommandSegmentV1 {
    let (segment, encoded) = crate::derived::follower_indexes::segment(
        vec![crate::derived::follower_indexes::capsule(sequence)],
        prior,
    );
    let write = store.shared.database.begin_write().unwrap();
    {
        let mut commands = write.open_table(crate::layout::COMMITS).unwrap();
        commands
            .insert(
                crate::keys::encode_application_sequence_key(sequence).as_slice(),
                encoded.as_bytes(),
            )
            .unwrap();
        let mut metadata = write.open_table(crate::layout::META).unwrap();
        let allocator = crate::codec::encode_application_sequence_allocator_v1(
            ApplicationSequenceAllocator::Next(sequence.checked_next().unwrap()),
        )
        .unwrap();
        metadata
            .insert(
                crate::layout::META_APPLICATION_SEQUENCE,
                allocator.as_bytes(),
            )
            .unwrap();
    }
    write.commit().unwrap();
    segment
}

fn pending_source(store: &crate::RedbStore, identity: u64) -> RedbReadAccess {
    let root = CheckpointRoot::new(store.shared.database.begin_read().unwrap(), identity)
        .unwrap()
        .retain_engine_and_history(Arc::clone(&store.shared.database), None);
    RedbReadAccess::Current(Arc::new(root))
}

fn populated_source() -> (
    ScopedDirectory,
    crate::RedbStore,
    RedbReadAccess,
    StoredCommandSegmentV1,
) {
    let scope = ScopedDirectory::new("locator-builder-populated");
    let mut store = crate::RedbStore::open(scope.join("source.redb")).unwrap();
    let (first, _) = crate::derived::follower_indexes::segment(
        vec![crate::derived::follower_indexes::capsule(
            CommitSequence::first(),
        )],
        None,
    );
    store.initialize_database(first.database_id()).unwrap();
    let first = append_segment(&store, CommitSequence::first(), None);
    let access = pending_source(&store, 1);
    (scope, store, access, first)
}

#[test]
fn locator_builder_completes_only_its_original_populated_captured_source() {
    let (_scope, store, original, first) = populated_source();
    let one = CommitSequence::first();
    let two = one.checked_next().unwrap();
    let second = append_segment(&store, two, Some(first.segment_digest()));
    let successor = pending_source(&store, 2);
    assert!(original.locator_capability().is_pending());
    assert!(successor.locator_capability().is_pending());
    assert!(original.locator_capability().request_build());
    let original_work = job(original.clone());
    process_job(&original_work).unwrap();
    let generation = original.locator_capability().require_complete().unwrap();
    assert!(generation.has_command_at(one).unwrap());
    assert!(
        !generation.has_command_at(two).unwrap(),
        "a later engine commit cannot enter the original generation"
    );
    let second_identity_key = second.commands()[0]
        .base()
        .outcome()
        .identity()
        .storage_key()
        .unwrap();
    let second_identity = crate::keys::encode_idempotency_key(&second_identity_key);
    assert!(
        !generation
            .has_command_member(
                riffdb_storage_api::CommandDerivedIndexKindV1::Idempotency,
                second_identity,
            )
            .unwrap()
    );
    assert!(
        successor.locator_capability().is_pending(),
        "completion cannot attach to a different captured root"
    );
    assert_eq!(
        successor
            .locator_capability()
            .require_complete()
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::Unavailable
    );
    drop(original_work);

    assert!(successor.locator_capability().request_build());
    let successor_work = job(successor.clone());
    process_job(&successor_work).unwrap();
    let successor_generation = successor.locator_capability().require_complete().unwrap();
    assert!(successor_generation.has_command_at(one).unwrap());
    assert!(successor_generation.has_command_at(two).unwrap());
    assert!(
        successor_generation
            .has_command_member(
                riffdb_storage_api::CommandDerivedIndexKindV1::Idempotency,
                second_identity,
            )
            .unwrap()
    );
    assert!(
        !generation.has_command_at(two).unwrap(),
        "completing a successor preserves the retained original negative"
    );
}

#[test]
fn locator_builder_mid_construction_cancellation_never_publishes_partial_membership() {
    let (_scope, store, original, first) = populated_source();
    append_segment(
        &store,
        CommitSequence::first().checked_next().unwrap(),
        Some(first.segment_digest()),
    );
    let access = pending_source(&store, 2);
    let capability = access.locator_capability();
    assert!(capability.request_build());
    let reservation = capability.construction_owner().unwrap();
    let mut checks = 0;
    let result = LocatorGeneration::rebuild_from_access_with_owner(
        &access,
        reservation,
        LocatorAllocationClass::Ordinary,
        || {
            checks += 1;
            // The first two checks precede the first row's complete decode and
            // insertion. The third is the next row boundary, after admitted
            // private membership exists but before it could prove absence.
            checks == 3
        },
    );
    assert_eq!(checks, 3);
    assert_eq!(result.err().unwrap().kind(), StorageErrorKind::Unavailable);
    assert!(capability.is_pending());
    assert_eq!(
        capability.require_complete().err().unwrap().kind(),
        StorageErrorKind::Unavailable
    );
    capability.release_build_request();
    assert!(
        capability.request_build(),
        "cancellation permits a later admitted attempt"
    );
    let retry = job(access.clone());
    process_job(&retry).unwrap();
    let complete = capability.require_complete().unwrap();
    assert!(complete.has_command_at(CommitSequence::first()).unwrap());
    assert!(
        complete
            .has_command_at(CommitSequence::first().checked_next().unwrap())
            .unwrap()
    );
    assert!(
        original.locator_capability().is_pending(),
        "a retry does not complete another source owner"
    );
}

#[test]
fn locator_builder_fixed_queue_refuses_overflow_without_losing_captured_sources() {
    let (_scope, _database, access) = source();
    let charge = NativeLocatorPool::global()
        .admit(
            2 * std::mem::size_of::<[Option<BuilderJob>; LOCATOR_MAX_OWNERS]>()
                + std::mem::size_of::<BuilderQueue>(),
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
    let queue = BuilderQueue {
        state: Mutex::new(BuilderState {
            jobs: Box::new(std::array::from_fn(|_| None)),
            read: 0,
            write: 0,
            len: 0,
            enabled: true,
        }),
        ready: Condvar::new(),
        _charge: charge,
    };
    for _ in 0..LOCATOR_MAX_OWNERS {
        queue.enqueue(job(access.clone())).unwrap();
    }
    assert!(queue.enqueue(job(access.clone())).is_err());
    assert_eq!(lock(&queue.state).len, LOCATOR_MAX_OWNERS);
    for _ in 0..LOCATOR_MAX_OWNERS / 2 {
        let queued = lock(&queue.state).pop().unwrap();
        assert!(
            queued
                .access
                .locator_capability()
                .same_construction(access.locator_capability())
        );
        drop(queued);
    }
    for _ in 0..LOCATOR_MAX_OWNERS / 2 {
        queue.enqueue(job(access.clone())).unwrap();
    }
    assert_eq!(lock(&queue.state).len, LOCATOR_MAX_OWNERS);
    // Drain the test queue explicitly in bounded groups, never via population Drop.
    for _ in 0..LOCATOR_MAX_OWNERS / crate::locator_memory::LOCATOR_RETIRE_QUOTA {
        for _ in 0..crate::locator_memory::LOCATOR_RETIRE_QUOTA {
            let queued = lock(&queue.state).pop().unwrap();
            assert!(
                queued
                    .access
                    .locator_capability()
                    .same_construction(access.locator_capability())
            );
            drop(queued);
        }
        std::thread::yield_now();
    }
    assert!(lock(&queue.state).pop().is_none());
    assert!(access.locator_capability().is_pending());
    assert!(access.locator_capability().require_complete().is_err());
}

#[test]
fn locator_builder_cancelled_last_source_never_constructs_or_proves_absence() {
    let (_scope, _database, access) = source();
    let external = access.clone();
    let work = job(access);
    assert!(source_has_external_owner(&work.access));
    drop(external);
    assert!(!source_has_external_owner(&work.access));
    let capability = work.access.locator_capability().clone();
    assert!(capability.request_build());
    // The physical command table is intentionally not initialized: successful early
    // cancellation proves that no row read or generation construction occurs.
    process_job(&work).unwrap();
    assert!(capability.is_pending());
    assert!(capability.require_complete().is_err());
    capability.release_build_request();
    assert!(capability.request_build());
    drop(work);
}

#[test]
fn locator_builder_parallel_requests_share_one_admitted_pending_owner() {
    let pending = LocatorCapability::try_pending(LocatorAllocationClass::Ordinary).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let accepted = std::thread::scope(|scope| {
        let first = pending.clone();
        let first_barrier = Arc::clone(&barrier);
        let first = scope.spawn(move || {
            first_barrier.wait();
            first.request_build()
        });
        let second = pending.clone();
        let second_barrier = Arc::clone(&barrier);
        let second = scope.spawn(move || {
            second_barrier.wait();
            second.request_build()
        });
        barrier.wait();
        usize::from(first.join().unwrap()) + usize::from(second.join().unwrap())
    });
    assert_eq!(accepted, 1);
    assert!(!pending.request_build());
    pending.release_build_request();
    assert!(pending.request_build());
    assert!(pending.is_pending());
}

#[test]
fn locator_builder_enqueue_wakes_predicate_wait_without_lost_notification() {
    let (_scope, _database, access) = source();
    let charge = NativeLocatorPool::global()
        .admit(
            2 * std::mem::size_of::<[Option<BuilderJob>; LOCATOR_MAX_OWNERS]>()
                + std::mem::size_of::<BuilderQueue>(),
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
    let queue = Arc::new(BuilderQueue {
        state: Mutex::new(BuilderState {
            jobs: Box::new(std::array::from_fn(|_| None)),
            read: 0,
            write: 0,
            len: 0,
            enabled: true,
        }),
        ready: Condvar::new(),
        _charge: charge,
    });
    let entering_wait = Arc::new(Barrier::new(2));
    std::thread::scope(|scope| {
        let worker_queue = Arc::clone(&queue);
        let worker_barrier = Arc::clone(&entering_wait);
        let expected = access.locator_capability().clone();
        let worker = scope.spawn(move || {
            let mut state = lock(&worker_queue.state);
            assert_eq!(state.len, 0);
            // Holding the predicate mutex here forces enqueue to occur only
            // after Condvar::wait atomically releases it.
            worker_barrier.wait();
            while state.len == 0 {
                state = worker_queue.ready.wait(state).unwrap();
            }
            let captured = state.pop().unwrap();
            drop(state);
            assert!(
                captured
                    .access
                    .locator_capability()
                    .same_construction(&expected)
            );
            drop(captured);
        });
        entering_wait.wait();
        queue.enqueue(job(access.clone())).unwrap();
        worker.join().unwrap();
    });
    assert_eq!(lock(&queue.state).len, 0);
}
