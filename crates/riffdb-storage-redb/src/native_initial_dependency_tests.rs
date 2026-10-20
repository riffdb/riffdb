// req: SEC-005, TXN-042, STO-032, REC-001
use super::*;
use crate::checkpoint_root::CheckpointRoot;
use crate::test_path::ScopedDirectory;
use crate::transient::LocatorGeneration;
use redb::ReadableDatabase;
use riffdb_storage_api::{ApplicationSequenceAllocator, DatabaseInitializationPort};
use riffdb_types::CommitSequence;
use std::sync::Barrier;

fn source() -> (ScopedDirectory, crate::RedbStore, RedbReadAccess) {
    let scope = ScopedDirectory::new("initial-native-dependency");
    let mut store = crate::RedbStore::open(scope.join("source.redb")).unwrap();
    let (segment, _) = crate::derived::follower_indexes::segment(
        vec![crate::derived::follower_indexes::capsule(
            CommitSequence::first(),
        )],
        None,
    );
    store.initialize_database(segment.database_id()).unwrap();
    let access = source_at(&store, 1);
    (scope, store, access)
}

fn source_at(store: &crate::RedbStore, identity: u64) -> RedbReadAccess {
    RedbReadAccess::Current(crate::checkpoint_root::CheckpointRootOwner::new(
        CheckpointRoot::new(store.shared.database.begin_read().unwrap(), identity)
            .unwrap()
            .retain_engine_and_history(store.shared.database.clone(), None),
    ))
}

fn complete_empty(source: &RedbReadAccess) {
    let owner = source.locator_capability().construction_owner().unwrap();
    let generation =
        LocatorGeneration::try_empty_with_owner(owner, LocatorAllocationClass::Ordinary).unwrap();
    source
        .locator_capability()
        .install_generation(generation)
        .unwrap();
}

#[test]
fn completed_proof_waits_until_the_builder_finishes_its_handoff() {
    let (_scope, _store, source) = source();
    let capability = source.locator_capability().clone();
    assert!(capability.request_build());
    complete_empty(&source);
    let waiting = Arc::new(Barrier::new(2));
    let worker = {
        let capability = capability.clone();
        let waiting = Arc::clone(&waiting);
        std::thread::spawn(move || {
            let mut entering = Some(waiting);
            capability.wait_for_initial_dependency_with(&AtomicBool::new(false), || {
                if let Some(waiting) = entering.take() {
                    waiting.wait();
                }
            })
        })
    };
    waiting.wait();
    capability.release_build_request();
    worker.join().unwrap().unwrap();
}

#[test]
fn cancellation_before_wait_keeps_the_control_charged_through_its_final_alias() {
    let (_scope, store, source) = source();
    let pool = NativeLocatorPool::isolated_for_tests();
    let before = pool.charged_bytes();
    let waiter =
        OwnedNativeDependencyWait::try_new(Arc::clone(&store.shared), source, &pool).unwrap();
    let cancellation = waiter.cancellation();
    let last_alias = cancellation.clone();
    let charged = pool.charged_bytes();
    assert!(charged > before);
    cancellation.cancel();
    assert_eq!(
        waiter.wait().unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(pool.charged_bytes(), charged);
    drop(cancellation);
    assert_eq!(pool.charged_bytes(), charged);
    drop(last_alias);
    assert_eq!(pool.charged_bytes(), before);
}

#[test]
fn cancellation_wakes_an_executing_wait_without_losing_the_predicate_handoff() {
    let (_scope, store, source) = source();
    let capability = source.locator_capability().clone();
    assert!(capability.request_build());
    let waiter = OwnedNativeDependencyWait::try_new(
        Arc::clone(&store.shared),
        source,
        &NativeLocatorPool::global(),
    )
    .unwrap();
    let cancellation = waiter.cancellation();
    let waiting = Arc::new(Barrier::new(2));
    let worker = {
        let control = waiter.control.clone();
        let waiting = Arc::clone(&waiting);
        std::thread::spawn(move || {
            let mut entering = Some(waiting);
            capability.wait_for_initial_dependency_with(&control.cancelled, || {
                if let Some(waiting) = entering.take() {
                    waiting.wait();
                }
            })
        })
    };
    waiting.wait();
    cancellation.cancel();
    assert_eq!(
        worker.join().unwrap().unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert!(
        waiter
            .control
            .initial
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .locator_capability()
            .is_pending()
    );
}

#[test]
fn failed_unchanged_construction_is_refused_without_an_operation_or_build_retry() {
    let (_scope, store, source) = source();
    assert!(source.locator_capability().request_build());
    source.locator_capability().release_build_request();
    let waiter = OwnedNativeDependencyWait::try_new(
        Arc::clone(&store.shared),
        source.clone(),
        &NativeLocatorPool::global(),
    )
    .unwrap();
    let mut requests = 0;
    let result = waiter.wait_with_sources(
        || Ok(source.clone()),
        |_| {
            requests += 1;
            Ok(())
        },
    );
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
    assert_eq!(requests, 0);
    assert!(source.locator_capability().is_pending());
}

#[test]
fn invalid_proof_preserves_its_integrity_refusal_without_current_source_admission() {
    let (_scope, store, source) = source();
    source
        .locator_capability()
        .invalidate(StorageErrorKind::CorruptData);
    let waiter = OwnedNativeDependencyWait::try_new(
        Arc::clone(&store.shared),
        source,
        &NativeLocatorPool::global(),
    )
    .unwrap();
    let mut captures = 0;
    let result = waiter.wait_with_sources(
        || {
            captures += 1;
            Err(unavailable())
        },
        |_| Err(unavailable()),
    );
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::CorruptData);
    assert_eq!(captures, 0);
}

#[test]
fn a_completed_older_source_cannot_admit_a_pending_current_source() {
    let (_scope, store, original) = source();
    complete_empty(&original);
    let current = source_at(&store, 2);
    let waiter = OwnedNativeDependencyWait::try_new(
        Arc::clone(&store.shared),
        original,
        &NativeLocatorPool::global(),
    )
    .unwrap();
    let mut requests = 0;
    let mut captures = 0;
    waiter
        .wait_with_sources(
            || {
                captures += 1;
                Ok(current.clone())
            },
            |requested| {
                requests += 1;
                assert!(
                    requested
                        .locator_capability()
                        .same_construction(current.locator_capability())
                );
                assert!(current.locator_capability().is_pending());
                assert!(current.locator_capability().request_build());
                complete_empty(&current);
                current.locator_capability().release_build_request();
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(requests, 1);
    assert_eq!(captures, 2);
    current.locator_capability().require_complete().unwrap();
}

#[test]
fn cancellation_during_a_source_switch_cannot_leave_the_new_predicate_asleep() {
    let (_scope, store, original) = source();
    complete_empty(&original);
    let current = source_at(&store, 2);
    let waiter = OwnedNativeDependencyWait::try_new(
        Arc::clone(&store.shared),
        original,
        &NativeLocatorPool::global(),
    )
    .unwrap();
    let cancellation = waiter.cancellation();
    let mut requests = 0;
    let result = waiter.wait_with_sources(
        || Ok(current.clone()),
        |_| {
            requests += 1;
            assert!(current.locator_capability().request_build());
            // Cancellation still sees the old notification target. The next
            // target must observe the flag before attempting its condition wait.
            cancellation.cancel();
            Ok(())
        },
    );
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
    assert_eq!(requests, 1);
    assert!(current.locator_capability().is_pending());
    current.locator_capability().release_build_request();
}

#[test]
fn a_complete_current_source_uses_the_synchronous_ready_fast_path() {
    let (_scope, store, _original) = source();
    let ports = RedbOperationalPorts {
        shared: Arc::clone(&store.shared),
    };
    let current = ports.shared.begin_composite_operational_read().unwrap();
    complete_empty(&current);
    assert!(ports.initial_native_dependency().unwrap().is_none());
}

#[test]
fn genuine_populated_construction_completes_without_a_waiter_holding_the_writer_lease() {
    let (_scope, store, _original) = source();
    let (segment, encoded) = crate::derived::follower_indexes::segment(
        vec![crate::derived::follower_indexes::capsule(
            CommitSequence::first(),
        )],
        None,
    );
    let write = store.shared.database.begin_write().unwrap();
    {
        let mut commands = write.open_table(crate::layout::COMMITS).unwrap();
        commands
            .insert(
                crate::keys::encode_application_sequence_key(segment.first_commit_sequence())
                    .as_slice(),
                encoded.as_bytes(),
            )
            .unwrap();
        let mut metadata = write.open_table(crate::layout::META).unwrap();
        let next = crate::codec::encode_application_sequence_allocator_v1(
            ApplicationSequenceAllocator::Next(CommitSequence::first().checked_next().unwrap()),
        )
        .unwrap();
        metadata
            .insert(crate::layout::META_APPLICATION_SEQUENCE, next.as_bytes())
            .unwrap();
    }
    write.commit().unwrap();
    store.shared.retire_current_read_root();
    let ports = RedbOperationalPorts {
        shared: Arc::clone(&store.shared),
    };
    let writer = store.acquire_mutation_lease().unwrap();
    let waiter = ports.initial_native_dependency().unwrap().unwrap();
    let current = ports.shared.begin_composite_operational_read().unwrap();
    assert!(current.locator_capability().is_pending());
    let worker = std::thread::spawn(move || waiter.wait());
    drop(writer);
    worker.join().unwrap().unwrap();
    let current = ports.shared.begin_composite_operational_read().unwrap();
    assert!(
        current
            .locator_capability()
            .require_complete()
            .unwrap()
            .has_command_at(CommitSequence::first())
            .unwrap()
    );
    ports.shared.ensure_transient_indexes_ready().unwrap();
}

#[test]
fn invalidated_unavailable_proof_is_an_integrity_failure_in_initial_admission() {
    let (_scope, store, source) = source();
    source
        .locator_capability()
        .invalidate(StorageErrorKind::Unavailable);
    // The pre-existing generic storage refusal remains unchanged.
    assert_eq!(
        source
            .locator_capability()
            .require_complete()
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::Unavailable
    );
    let waiter = OwnedNativeDependencyWait::try_new(
        Arc::clone(&store.shared),
        source,
        &NativeLocatorPool::global(),
    )
    .unwrap();
    assert_eq!(
        waiter.wait().unwrap_err().kind(),
        StorageErrorKind::InvariantViolation
    );

    let ports = RedbOperationalPorts {
        shared: Arc::clone(&store.shared),
    };
    let current = ports.shared.begin_composite_operational_read().unwrap();
    current
        .locator_capability()
        .invalidate(StorageErrorKind::Unavailable);
    assert_eq!(
        ports.initial_native_dependency().err().unwrap().kind(),
        StorageErrorKind::InvariantViolation
    );
}

#[test]
fn unknown_or_unbound_source_cannot_be_healthy_pending_admission_backpressure() {
    let pending = LocatorCapability::try_pending(LocatorAllocationClass::Ordinary).unwrap();
    assert!(pending.is_pending());
    assert_eq!(
        pending.source_binding().unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(
        initial_source_is_pending(&pending).unwrap_err().kind(),
        StorageErrorKind::InvariantViolation
    );
    let absent = LocatorCapability::invalid();
    assert!(!absent.is_pending());
    assert_eq!(
        initial_source_is_pending(&absent).unwrap_err().kind(),
        StorageErrorKind::InvariantViolation
    );
    assert_eq!(
        unidentified_source_error(unavailable()).kind(),
        StorageErrorKind::InvariantViolation
    );
}

#[test]
fn poisoned_waiter_controls_refuse_as_integrity_and_release_the_final_physical_charge() {
    for poison_current in [false, true] {
        let (_scope, store, source) = source();
        let pool = NativeLocatorPool::isolated_for_tests();
        let before = pool.charged_bytes();
        let waiter =
            OwnedNativeDependencyWait::try_new(Arc::clone(&store.shared), source, &pool).unwrap();
        let cancellation = waiter.cancellation();
        let charged = pool.charged_bytes();
        assert!(charged > before);
        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if poison_current {
                let _current = waiter.control.current.lock().unwrap();
                panic!("deterministic initial waiter target poison");
            } else {
                let _initial = waiter.control.initial.lock().unwrap();
                panic!("deterministic initial waiter source poison");
            }
        }));
        assert!(poison.is_err());
        assert_eq!(
            waiter.wait().unwrap_err().kind(),
            StorageErrorKind::InvariantViolation
        );
        assert_eq!(pool.charged_bytes(), charged);
        cancellation.cancel();
        drop(cancellation);
        assert_eq!(pool.charged_bytes(), before);
    }
}
