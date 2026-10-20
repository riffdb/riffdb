//! Real configured storage/backup ownership; no complete Source execution.
// req: STO-020, STO-032, PRJ-004, REC-001
#![allow(
    clippy::assertions_on_constants,
    clippy::drop_non_drop,
    clippy::err_expect
)]
use super::*;
use crate::maintenance::SourceOfflineStorageSlotV1;
use crate::{RedbMaintenanceStorage, SourceExecutionBootstrapV1};
use riffdb_types::{
    BackupNameV1, exact_provider_budget_v1::PROJECTION_SOURCE_WORKING_MEMORY_BYTES,
};

fn configured_fixture() -> (Fixture, PathBuf, BackupNameV1) {
    let mut fixture = Fixture::copy();
    let root = fixture._scope.join("configured-backups");
    std::fs::create_dir(&root).unwrap();
    let name = BackupNameV1::new("source-base").unwrap();
    let old = fixture.backup.clone();
    let backup = root.join(name.as_str());
    std::fs::rename(&old, &backup).unwrap();
    fixture.original_hashes = std::mem::take(&mut fixture.original_hashes)
        .into_iter()
        .map(|(path, hash)| {
            let path = if path.starts_with(&old) {
                backup.join(path.strip_prefix(&old).unwrap())
            } else {
                path
            };
            (path, hash)
        })
        .collect();
    fixture.backup = backup;
    (fixture, root, name)
}
fn unavailable(error: StorageError) {
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
}

#[test]
fn configured_source_task_tail_retains_same_ledger_and_real_lock_after_storage_and_operation_drop()
{
    let (fixture, root, name) = configured_fixture();
    let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
    let budget = bootstrap.numerical_budget().clone();
    let storage = RedbMaintenanceStorage::open_for_promotion_recovery_with_source_bootstrap(
        &fixture.source,
        &root,
        bootstrap,
    )
    .unwrap();
    let mut slot = SourceOfflineStorageSlotV1::new(storage);
    let mut operation = slot
        .loan(&name, inputs(), Arc::new(AtomicBool::new(false)))
        .unwrap();
    assert!(operation.numerical_budget().shares_owner_with(&budget));
    let task_tail = operation.take_task_retirement().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (drop_operation_tx, drop_operation_rx) = std::sync::mpsc::sync_channel(1);
    let (dropped_tx, dropped_rx) = std::sync::mpsc::sync_channel(1);
    let (drop_tail_tx, drop_tail_rx) = std::sync::mpsc::sync_channel(1);
    let budget_for_worker = budget.clone();
    std::thread::scope(|scope| {
        let owner = scope.spawn(move || {
            // The root product had no reader/decoder/native engine. Create
            // and complete those owners inside the consuming worker phase.
            let operation = operation.finish_maintenance_preparation_for_test().unwrap();
            assert!(
                operation
                    .numerical_budget()
                    .shares_owner_with(&budget_for_worker)
            );
            entered_tx.send(()).unwrap();
            drop_operation_rx.recv().unwrap();
            drop(operation);
            dropped_tx.send(()).unwrap();
            drop_tail_rx.recv().unwrap();
            drop(task_tail);
        });
        entered_rx.recv().unwrap();
        let primary_excluded = matches!(
            redb::ReadOnlyDatabase::open(&fixture.source),
            Err(redb::DatabaseError::DatabaseAlreadyOpen)
        );
        drop(slot);
        let after_storage =
            RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root)
                .err()
                .map(|error| error.kind());
        drop_operation_tx.send(()).unwrap();
        dropped_rx.recv().unwrap();
        let after_operation =
            RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root)
                .err()
                .map(|error| error.kind());
        let retained = budget.observation().unwrap().reserved_bytes;
        drop_tail_tx.send(()).unwrap();
        owner.join().unwrap();
        assert!(primary_excluded);
        assert_eq!(after_storage, Some(StorageErrorKind::Unavailable));
        assert_eq!(after_operation, Some(StorageErrorKind::Unavailable));
        assert!(retained > 0);
    });
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    drop(RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root).unwrap());
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    fixture.assert_unchanged();
}

#[test]
fn configured_source_full_startup_ledger_refuses_before_native_predecessor_and_preserves_lock_owner()
 {
    let (fixture, root, name) = configured_fixture();
    let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
    let budget = bootstrap.numerical_budget().clone();
    let storage = RedbMaintenanceStorage::open_for_promotion_recovery_with_source_bootstrap(
        &fixture.source,
        &root,
        bootstrap,
    )
    .unwrap();
    let live = budget.observation().unwrap();
    let free = PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        .checked_sub(live.fixed_owner_bytes)
        .unwrap()
        .checked_sub(live.owned_bytes)
        .unwrap()
        .checked_sub(live.reserved_bytes)
        .unwrap();
    let pressure = budget.reserve(free).unwrap();
    let before = budget.observation().unwrap();
    let mut slot = SourceOfflineStorageSlotV1::new(storage);
    unavailable(
        slot.loan(&name, inputs(), Arc::new(AtomicBool::new(false)))
            .err()
            .expect("same full startup ledger refuses before new native ownership"),
    );
    let after = budget.observation().unwrap();
    assert_eq!(after.reserved_bytes, before.reserved_bytes);
    assert_eq!(after.owned_bytes, before.owned_bytes);
    assert_eq!(after.fixed_owner_bytes, before.fixed_owner_bytes);
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    unavailable(
        RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root)
            .err()
            .expect("failed Source admission cannot release original configured exclusion"),
    );
    drop(pressure);
    // SAME owner is restored after failed loan; it can open the same pool and
    // still owns the actual maintenance exclusion, never a new opened owner.
    assert!(slot.original_budget_for_test().shares_owner_with(&budget));
    drop(slot);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    fixture.assert_unchanged();
}

#[test]
fn configured_current_engine_reopen_keeps_the_original_startup_ledger() {
    let directory = crate::test_path::ScopedDirectory::new("configured-source-ledger-reopen");
    let source = directory.join("current.redb");
    let root = directory.join("backups");
    let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
    let budget = bootstrap.numerical_budget().clone();
    let mut storage = RedbMaintenanceStorage::open_for_promotion_recovery_with_source_bootstrap(
        &source, &root, bootstrap,
    )
    .unwrap();
    let first = storage
        .open_configured_current_store(crate::RedbCommitProfile::Standard)
        .unwrap();
    assert!(
        first
            .configured_source_budget_for_test()
            .shares_owner_with(&budget)
    );
    drop(first);
    let second = storage
        .open_configured_current_store(crate::RedbCommitProfile::Standard)
        .unwrap();
    assert!(
        second
            .configured_source_budget_for_test()
            .shares_owner_with(&budget)
    );
    drop(second);
    drop(storage);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn configured_offline_slot_requires_final_task_retirement_before_same_owner_return() {
    let (fixture, root, name) = configured_fixture();
    let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
    let budget = bootstrap.numerical_budget().clone();
    let storage = RedbMaintenanceStorage::open_for_promotion_recovery_with_source_bootstrap(
        &fixture.source,
        &root,
        bootstrap,
    )
    .unwrap();
    let mut slot = SourceOfflineStorageSlotV1::new(storage);
    let mut operation = slot
        .loan(&name, inputs(), Arc::new(AtomicBool::new(false)))
        .unwrap();
    let tail = operation.take_task_retirement().unwrap();
    let returned = std::thread::scope(|scope| {
        scope
            .spawn(move || {
                operation
                    .finish_maintenance_preparation_for_test()
                    .unwrap()
                    .return_storage_after_native_drop_for_test()
            })
            .join()
            .unwrap()
    });
    // Native owners are physically gone, but actual task tail still blocks the
    // original slot. Failure retains the actual returned owner for exact retry.
    let blocked = match slot.restore_for_test(returned) {
        Ok(()) => panic!("task/header retirement is still outstanding"),
        Err(blocked) => blocked,
    };
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    unavailable(
        RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root)
            .err()
            .expect("same physical maintenance lock remains held by returned owner"),
    );
    drop(tail);
    assert!(blocked.retry_for_test(&mut slot).is_ok());
    assert!(slot.original_budget_for_test().shares_owner_with(&budget));
    drop(slot);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    drop(RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root).unwrap());
    fixture.assert_unchanged();
}

#[test]
fn configured_worker_refusal_retains_actual_owner_until_tail_and_same_ledger_retry() {
    let (fixture, root, name) = configured_fixture();
    let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
    let budget = bootstrap.numerical_budget().clone();
    let storage = RedbMaintenanceStorage::open_for_promotion_recovery_with_source_bootstrap(
        &fixture.source,
        &root,
        bootstrap,
    )
    .unwrap();
    let mut slot = SourceOfflineStorageSlotV1::new(storage);
    let missing = BackupNameV1::new("missing-source-base").unwrap();
    assert!(!root.join(missing.as_str()).exists());
    let mut operation = slot
        .loan(&missing, inputs(), Arc::new(AtomicBool::new(false)))
        .unwrap();
    let tail = operation.take_task_retirement().unwrap();
    let (returned, error) = std::thread::scope(|scope| {
        scope
            .spawn(
                move || match operation.finish_maintenance_preparation_for_test() {
                    Ok(_) => panic!("missing named backup must refuse actual worker preparation"),
                    Err(failure) => failure.return_after_refusal_for_test(),
                },
            )
            .join()
            .unwrap()
    });
    // validate_backup_inventory's failed symlink_metadata maps to typed
    // Unavailable. Assert only the value-safe class, never a path/error source.
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    drop(error);
    // Partial actual Native owners were destroyed before returning the failure
    // owner, while the SAME maintenance lock/domain survive in its closed loan.
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    unavailable(
        RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root)
            .err()
            .expect("failed worker returns the actual still-locked owner"),
    );
    let blocked = match slot.restore_for_test(returned) {
        Ok(()) => panic!("worker refusal cannot refund outstanding task/header custody"),
        Err(blocked) => blocked,
    };
    drop(tail);
    assert!(blocked.retry_for_test(&mut slot).is_ok());
    assert!(slot.original_budget_for_test().shares_owner_with(&budget));

    // Retry on that SAME owner/ledger using the unchanged genuine backup, not
    // a warmup command, new budget or reacquired maintenance lock.
    let mut retry = slot
        .loan(&name, inputs(), Arc::new(AtomicBool::new(false)))
        .unwrap();
    let retry_tail = retry.take_task_retirement().unwrap();
    let returned = std::thread::scope(|scope| {
        scope
            .spawn(move || {
                retry
                    .finish_maintenance_preparation_for_test()
                    .unwrap()
                    .return_storage_after_native_drop_for_test()
            })
            .join()
            .unwrap()
    });
    drop(retry_tail);
    assert!(slot.restore_for_test(returned).is_ok());
    assert!(slot.original_budget_for_test().shares_owner_with(&budget));
    drop(slot);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    fixture.assert_unchanged();
}

// All parsing below is ordinary diagnostic maintenance parsing of private copies,
// never a Source-funded INITIAL cold construction or PublishedSource acquisition.
// Parent.verify retains its ordinary path/symlink/canonicalization behavior;
// its scratch admission remains unqualified, independently of retained captures.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn configured_pair_case(
    present: bool,
) -> (
    Fixture,
    PathBuf,
    crate::maintenance::SourceOfflineStorageSlotV1,
    crate::maintenance::SourceOwnedOperationV1,
    riffdb_types::ProjectionSourceWorkingBudgetV1,
) {
    use std::os::unix::fs::PermissionsExt;
    let (fixture, root, name) = configured_fixture();
    let derived = crate::store::derived_store_path(&fixture.source);
    if present {
        std::fs::copy(fixture.backup.join("database.redb"), &derived).unwrap();
    }
    for path in std::iter::once(&fixture.source).chain(present.then_some(&derived)) {
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(permissions.mode() | 0o200);
        std::fs::set_permissions(path, permissions).unwrap();
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o200,
            0o200
        );
    }
    let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
    let budget = bootstrap.numerical_budget().clone();
    let storage = RedbMaintenanceStorage::open_for_promotion_recovery_with_source_bootstrap(
        &fixture.source,
        &root,
        bootstrap,
    )
    .unwrap();
    let mut slot = SourceOfflineStorageSlotV1::new(storage);
    let operation = slot
        .loan(&name, inputs(), Arc::new(AtomicBool::new(false)))
        .unwrap();
    (fixture, root, slot, operation, budget)
}
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn pair_live_work(budget: &riffdb_types::ProjectionSourceWorkingBudgetV1) -> (u64, u64) {
    let observation = budget.observation().unwrap();
    (observation.reserved_bytes, observation.owned_bytes)
}
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn pair_assert_excluded(path: &Path) {
    assert!(matches!(
        redb::Database::open(path),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        std::fs::File::open(path).unwrap().try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn configured_pair_rejects_genuine_wrong_sibling_before_native_parse_and_preserves_operation() {
    let (fixture, root, slot, operation, budget) = configured_pair_case(true);
    let derived = crate::store::derived_store_path(&fixture.source);
    let wrong = fixture._scope.join("wrong-derived.redb");
    std::fs::copy(&derived, &wrong).unwrap();
    let wrong_file = std::fs::File::open(&wrong).unwrap();
    let before = pair_live_work(&budget);
    let error = operation
        .configured_derived_check_file_for_test(&wrong_file)
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    assert_eq!(pair_live_work(&budget), before);
    operation
        .configured_derived_check_file_for_test(&std::fs::File::open(&derived).unwrap())
        .unwrap();
    assert_eq!(pair_live_work(&budget), before);
    assert!(operation.idle_probe_for_test().require_idle().is_err());
    assert!(
        matches!(RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    drop(wrong_file);
    drop(operation);
    drop(slot);
    assert_eq!(pair_live_work(&budget), (0, 0));
    drop(RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root).unwrap());
    fixture.assert_unchanged();
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(all(
    target_os = "linux",
    target_arch = "x86_64",
    feature = "projection-source-driver"
))]
#[test]
fn configured_pair_last_primary_alias_retains_actual_operation_after_semantic_and_task_drop() {
    use sha2::Digest;
    let (fixture, root, slot, mut operation, budget) = configured_pair_case(true);
    let derived = crate::store::derived_store_path(&fixture.source);
    let original_derived_hash = file_hash(&derived);
    let idle = operation.idle_probe_for_test();
    let task = operation.take_task_retirement().unwrap();
    let continuation = operation.configured_present_derived_for_test().unwrap();
    let validated = operation.finish_maintenance_preparation_for_test().unwrap();
    let (facade, intent) = validated.split_facade_for_test();
    let mut facade = match facade.attach_configured_derived(continuation) {
        Ok(facade) => facade,
        Err(failure) => {
            let (_, error) = failure.into_test_parts();
            panic!("configured attachment: {:?}", error.kind());
        }
    };
    assert!(!facade.configured_derived_is_absent().unwrap());
    let auxiliary_identity = {
        let mut read = facade.begin_auxiliary_read().unwrap();
        let mut meta = read.open_str_table("meta").unwrap();
        let value = meta.get("database_id").unwrap().unwrap();
        <[u8; 32]>::from(Sha256::digest(value.value()))
    };
    let primary_identity = {
        let mut read = facade.begin_read().unwrap();
        let mut meta = read.open_str_table("meta").unwrap();
        let value = meta.get("database_id").unwrap().unwrap();
        <[u8; 32]>::from(Sha256::digest(value.value()))
    };
    assert_eq!(auxiliary_identity, primary_identity);
    let brand = facade.prepare_catalog_instruction_brand_for_test().unwrap();
    let input = facade
        .catalog_instruction_input_for_test(&brand, &budget)
        .unwrap();
    drop(intent); // actual semantic SourceOperationClaim wrapper and storage
    drop(task); // actual physical task/header wrapper
    drop(slot);
    drop(facade);
    assert!(idle.require_idle().is_err());
    assert!(budget.try_acquire_worker().is_err());
    pair_assert_excluded(&derived);
    pair_assert_excluded(&fixture.source);
    assert!(
        matches!(RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    drop(input);
    assert!(idle.require_idle().is_err()); // the actual brand is still a base alias
    drop(brand);
    idle.require_idle().unwrap();
    drop(idle);
    assert_eq!(pair_live_work(&budget), (0, 0));
    drop(budget.try_acquire_worker().unwrap());
    // Prove Source/alias retirement left BOTH files unchanged before any
    // diagnostic reentry can touch a native header.
    fixture.assert_unchanged();
    assert_eq!(file_hash(&derived), original_derived_hash);
    drop(RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root).unwrap());
    fixture.assert_unchanged();
    assert_eq!(file_hash(&derived), original_derived_hash);
    // Reacquire the genuine native/legacy exclusions through the existing
    // O_RDONLY maintenance parser; writable Database::open is not a hash probe.
    let mut builder = redb::Database::builder();
    builder.set_cache_size(0);
    drop(
        builder
            .open_read_only_maintenance_file(std::fs::File::open(&derived).unwrap())
            .unwrap(),
    );
    fixture.assert_unchanged();
    assert_eq!(file_hash(&derived), original_derived_hash);
    drop(
        builder
            .open_read_only_maintenance_file(std::fs::File::open(&fixture.source).unwrap())
            .unwrap(),
    );
    fixture.assert_unchanged();
    assert_eq!(file_hash(&derived), original_derived_hash);
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn configured_pair_absence_uses_real_retained_parent_and_refuses_later_child_appearance() {
    let (fixture, _root, slot, operation, budget) = configured_pair_case(false);
    let derived = crate::store::derived_store_path(&fixture.source);
    assert!(!derived.exists());
    let continuation = operation.configured_absent_derived_for_test().unwrap();
    assert!(!derived.exists()); // absence did not create a native empty database
    let validated = operation.finish_maintenance_preparation_for_test().unwrap();
    let (facade, intent) = validated.split_facade_for_test();
    let facade = match facade.attach_configured_derived(continuation) {
        Ok(facade) => facade,
        Err(failure) => {
            let (_, error) = failure.into_test_parts();
            panic!("configured absence: {:?}", error.kind());
        }
    };
    assert!(facade.configured_derived_is_absent().unwrap());
    assert!(!derived.exists());
    // Deterministic state change between two real absence observations.
    std::fs::copy(fixture.backup.join("database.redb"), &derived).unwrap();
    assert!(
        matches!(facade.configured_derived_is_absent(), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    drop(facade);
    drop(intent);
    drop(slot);
    assert_eq!(pair_live_work(&budget), (0, 0));
    fixture.assert_unchanged();
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn configured_pair_original_ledger_n_minus_one_refuses_before_retained_capture_or_verification() {
    use crate::maintenance::PinnedDirectory;
    let (fixture, _root, slot, operation, budget) = configured_pair_case(false);
    let bytes = operation.configured_derived_bytes_for_test().unwrap();
    let retained_bytes = operation
        .configured_derived_retained_capture_bytes_for_test()
        .unwrap();
    let verification_bytes = operation
        .configured_derived_verification_bytes_for_test()
        .unwrap();
    assert!(retained_bytes > 0 && verification_bytes > 0);
    assert_eq!(bytes, retained_bytes + verification_bytes);
    let observation = budget.observation().unwrap();
    let available = PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        .checked_sub(observation.fixed_owner_bytes)
        .unwrap()
        .checked_sub(observation.reserved_bytes)
        .unwrap()
        .checked_sub(observation.owned_bytes)
        .unwrap();
    assert!(bytes > 0 && bytes <= available);
    let _ = PinnedDirectory::take_last_verification_drop_record_for_test();
    let before = pair_live_work(&budget);
    let pressure = budget.reserve(available - (bytes - 1)).unwrap();
    let held = pair_live_work(&budget);
    assert!(
        matches!(operation.configured_absent_derived_for_test(), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(
        PinnedDirectory::take_last_verification_drop_record_for_test(),
        None
    );
    assert_eq!(pair_live_work(&budget), held);
    drop(pressure);
    assert_eq!(pair_live_work(&budget), before);
    let exact_pressure = budget.reserve(available - bytes).unwrap();
    let continuation = operation.configured_absent_derived_for_test().unwrap();
    assert_eq!(
        PinnedDirectory::take_last_verification_drop_record_for_test(),
        Some((0, 0, 0, verification_bytes))
    );
    assert_eq!(
        budget.peak_admitted_bytes().unwrap(),
        PROJECTION_SOURCE_WORKING_MEMORY_BYTES
    );
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        before.0 + (available - bytes) + retained_bytes
    );
    drop(exact_pressure);
    let validated = operation.finish_maintenance_preparation_for_test().unwrap();
    let (facade, intent) = validated.split_facade_for_test();
    let facade = match facade.attach_configured_derived(continuation) {
        Ok(facade) => facade,
        Err(failure) => {
            let (_, error) = failure.into_test_parts();
            panic!("configured absence attachment: {:?}", error.kind());
        }
    };
    let post_attach_obs = budget.observation().unwrap();
    let post_attach_avail = PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        .checked_sub(post_attach_obs.fixed_owner_bytes)
        .unwrap()
        .checked_sub(post_attach_obs.reserved_bytes)
        .unwrap()
        .checked_sub(post_attach_obs.owned_bytes)
        .unwrap();
    let _ = PinnedDirectory::take_last_verification_drop_record_for_test();
    let recheck_short = budget
        .reserve(post_attach_avail - (verification_bytes - 1))
        .unwrap();
    assert!(
        matches!(facade.configured_derived_is_absent(), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(
        PinnedDirectory::take_last_verification_drop_record_for_test(),
        None
    );
    drop(recheck_short);
    let recheck_exact = budget
        .reserve(post_attach_avail - verification_bytes)
        .unwrap();
    assert!(facade.configured_derived_is_absent().unwrap());
    assert_eq!(
        PinnedDirectory::take_last_verification_drop_record_for_test(),
        Some((0, 0, 0, verification_bytes))
    );
    drop(recheck_exact);
    drop(facade);
    drop(intent);
    assert!(!crate::store::derived_store_path(&fixture.source).exists());
    drop(slot);
    assert_eq!(pair_live_work(&budget), (0, 0));
    fixture.assert_unchanged();
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn rustix_getcwd_raw_subprocess_matrix_covers_exact_short_empty_canary_non_utf8_and_unlinked_cwd() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    const CHILD_ENV: &str = "RIFFDB_WP825_GETCWD_RAW_SUBPROCESS_CHILD";
    if std::env::var_os(CHILD_ENV).is_some() {
        assert!(rustix::process::GETCWD_RAW_SUPPORTED);
        let temp = tempfile::tempdir().unwrap();
        let canonical_root = std::fs::canonicalize(temp.path()).unwrap();
        let non_utf8_dir = canonical_root.join(OsStr::from_bytes(b"cwd-\xff-\xfe"));
        std::fs::create_dir(&non_utf8_dir).unwrap();
        rustix::process::chdir(&non_utf8_dir).unwrap();

        let expected = rustix::process::getcwd(Vec::new()).unwrap();
        let expected_bytes = expected.as_bytes_with_nul();
        let n = expected_bytes.len();
        assert!(n > 2);
        assert_eq!(expected_bytes[n - 1], 0);
        assert_eq!(
            &expected_bytes[..n - 1],
            non_utf8_dir.as_os_str().as_bytes()
        );

        let mut exact_buf = vec![0x5Au8; n];
        let init_exact_len = rustix::process::getcwd_raw(&mut exact_buf[..]).unwrap();
        assert_eq!(init_exact_len, n);
        assert_eq!(&exact_buf[..init_exact_len], expected_bytes);

        let mut canary_buf = vec![0xA5u8; n + 16];
        let init_len = rustix::process::getcwd_raw(&mut canary_buf[..]).unwrap();
        assert_eq!(init_len, n);
        assert_eq!(&canary_buf[..init_len], expected_bytes);
        assert!(canary_buf[n..].iter().all(|&byte| byte == 0xA5));

        let mut short_buf = vec![0xCCu8; n - 1];
        assert_eq!(
            rustix::process::getcwd_raw(&mut short_buf[..]).unwrap_err(),
            rustix::io::Errno::RANGE
        );

        let mut one_buf = [0xDDu8; 1];
        assert_eq!(
            rustix::process::getcwd_raw(&mut one_buf[..]).unwrap_err(),
            rustix::io::Errno::RANGE
        );

        let mut empty_buf: [u8; 0] = [];
        assert_eq!(
            rustix::process::getcwd_raw(&mut empty_buf[..]).unwrap_err(),
            rustix::io::Errno::RANGE
        );

        std::fs::remove_dir(&non_utf8_dir).unwrap();
        let mut unlinked_buf = vec![0u8; n + 32];
        assert_eq!(
            rustix::process::getcwd_raw(&mut unlinked_buf[..]).unwrap_err(),
            rustix::io::Errno::NOENT
        );
        rustix::process::chdir(&canonical_root).unwrap();
        println!("RIFFDB_WP825_GETCWD_RAW_CHILD_OK");
        return;
    }

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .env(CHILD_ENV, "1")
        .arg("--exact")
        .arg("startup::offline_predecessor::tests::source_execution_domain_tests::rustix_getcwd_raw_subprocess_matrix_covers_exact_short_empty_canary_non_utf8_and_unlinked_cwd")
        .arg("--nocapture")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains("RIFFDB_WP825_GETCWD_RAW_CHILD_OK"),
        "subprocess failed: stdout={}\nstderr={}",
        stdout,
        stderr
    );
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn source_parent_verification_parity_and_deterministic_races_in_subprocess() {
    use crate::maintenance::PinnedDirectory;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::symlink;

    const CHILD_ENV: &str = "RIFFDB_WP825_SOURCE_PARENT_VERIFY_SUBPROCESS_CHILD";
    if std::env::var_os(CHILD_ENV).is_some() {
        let budget = riffdb_types::ProjectionSourceWorkingBudgetV1::new();
        let temp = tempfile::tempdir().unwrap();
        let base = std::fs::canonicalize(temp.path()).unwrap();
        rustix::process::chdir(&base).unwrap();

        // 1. Absolute and relative parity, including non-UTF8 and redundant separators.
        let rel_non_utf8 =
            PathBuf::from(OsStr::from_bytes(b"rel-\xff")).join(OsStr::from_bytes(b"dir-\xfe"));
        std::fs::create_dir_all(&rel_non_utf8).unwrap();
        let pinned_abs = PinnedDirectory::open(&base.join(&rel_non_utf8)).unwrap();
        pinned_abs.verify().unwrap();
        let _ = PinnedDirectory::take_last_verification_drop_record_for_test();
        pinned_abs.verify_source_for_test(&budget).unwrap();
        assert_eq!(
            PinnedDirectory::take_last_verification_drop_record_for_test(),
            Some((
                0,
                0,
                0,
                pinned_abs.source_parent_verification_bytes().unwrap()
            ))
        );
        assert_eq!(pair_live_work(&budget), (0, 0));

        let pinned_rel = PinnedDirectory::open(&rel_non_utf8).unwrap();
        pinned_rel.verify().unwrap();
        pinned_rel.verify_source_for_test(&budget).unwrap();
        let cloned_rel = pinned_rel.source_retained_clone_for_test(&budget).unwrap();
        cloned_rel.verify().unwrap();
        cloned_rel.verify_source_for_test(&budget).unwrap();
        drop(cloned_rel);
        assert_eq!(pair_live_work(&budget), (0, 0));

        let redundant_bytes = b"rel-\xff//dir-\xfe/";
        let pinned_redundant =
            PinnedDirectory::open(Path::new(OsStr::from_bytes(redundant_bytes))).unwrap();
        pinned_redundant.verify().unwrap();
        pinned_redundant.verify_source_for_test(&budget).unwrap();
        assert_eq!(pair_live_work(&budget), (0, 0));

        // 2. CurDir and ParentDir rejection parity.
        let dot_path = Path::new("./rel-\u{fffd}");
        assert_eq!(
            PinnedDirectory::open(dot_path).err().unwrap().kind(),
            StorageErrorKind::CorruptData
        );
        let injected_curdir = pinned_rel.with_test_paths_from(
            PathBuf::from(OsStr::from_bytes(b"./rel-\xff/dir-\xfe")),
            base.join(&rel_non_utf8),
        );
        assert_eq!(
            injected_curdir.verify().unwrap_err().kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            injected_curdir
                .verify_source_for_test(&budget)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
        let injected_parentdir = pinned_rel.with_test_paths_from(
            PathBuf::from(OsStr::from_bytes(b"rel-\xff/../rel-\xff/dir-\xfe")),
            base.join(&rel_non_utf8),
        );
        assert_eq!(
            injected_parentdir.verify().unwrap_err().kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            injected_parentdir
                .verify_source_for_test(&budget)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );

        // 3. Profile bounds (> 4096 bytes) and unsupported backend return Unavailable, not CorruptData.
        let oversized_path = PathBuf::from(OsStr::from_bytes(&vec![b'a'; 4097]));
        let oversized_guard =
            pinned_abs.with_test_paths_from(oversized_path, base.join(&rel_non_utf8));
        assert_eq!(
            oversized_guard
                .source_parent_verification_bytes()
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            oversized_guard
                .source_retained_clone_bytes()
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );

        PinnedDirectory::set_force_unsupported_getcwd_raw_for_test(true);
        let unsupported_err = pinned_rel.verify_source_for_test(&budget).unwrap_err();
        PinnedDirectory::set_force_unsupported_getcwd_raw_for_test(false);
        assert_eq!(unsupported_err.kind(), StorageErrorKind::Unavailable);

        // 4. Changed cwd before verification (existing sibling vs missing vs oversized cwd > C).
        let alt_with_same_rel = base.join("alt_same");
        std::fs::create_dir_all(alt_with_same_rel.join(&rel_non_utf8)).unwrap();
        rustix::process::chdir(&alt_with_same_rel).unwrap();
        assert_eq!(
            pinned_rel.verify().unwrap_err().kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            pinned_rel
                .verify_source_for_test(&budget)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );

        let alt_empty = base.join("alt_empty");
        std::fs::create_dir_all(&alt_empty).unwrap();
        rustix::process::chdir(&alt_empty).unwrap();
        assert_eq!(
            pinned_rel.verify().unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            pinned_rel
                .verify_source_for_test(&budget)
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );

        let long_cwd = base.join("long_cwd_exceeding_stored_canonical_length_1234567890");
        std::fs::create_dir_all(long_cwd.join(&rel_non_utf8)).unwrap();
        rustix::process::chdir(&long_cwd).unwrap();
        assert_eq!(
            pinned_rel
                .verify_source_for_test(&budget)
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );
        rustix::process::chdir(&base).unwrap();
        pinned_rel.verify_source_for_test(&budget).unwrap();

        // 5. Deterministic race hooks:
        // Phase 1 (AfterInitialCwdRead): cwd changes mid-verification to alt_with_same_rel.
        let alt_clone = alt_with_same_rel.clone();
        let _ = PinnedDirectory::take_last_verification_drop_record_for_test();
        let race_cwd_err = pinned_rel
            .verify_source_with_race_hook_for_test(&budget, 1, move || {
                rustix::process::chdir(&alt_clone).unwrap();
            })
            .unwrap_err();
        assert_eq!(race_cwd_err.kind(), StorageErrorKind::CorruptData);
        assert_eq!(
            PinnedDirectory::take_last_verification_drop_record_for_test(),
            Some((
                0,
                0,
                0,
                pinned_rel.source_parent_verification_bytes().unwrap()
            ))
        );
        rustix::process::chdir(&base).unwrap();

        // Phase 2 (BeforeCanonicalWalk): target component replaced with a symlink.
        let race_sym_dir = base.join("race_sym_target");
        let race_sym_backup = base.join("race_sym_backup");
        let race_sym_elsewhere = base.join("race_sym_elsewhere");
        std::fs::create_dir_all(&race_sym_dir).unwrap();
        std::fs::create_dir_all(&race_sym_elsewhere).unwrap();
        let pinned_sym = PinnedDirectory::open(Path::new("race_sym_target")).unwrap();
        let sym_dir_clone = race_sym_dir.clone();
        let sym_bak_clone = race_sym_backup.clone();
        let sym_else_clone = race_sym_elsewhere.clone();
        let race_sym_err = pinned_sym
            .verify_source_with_race_hook_for_test(&budget, 2, move || {
                std::fs::rename(&sym_dir_clone, &sym_bak_clone).unwrap();
                symlink(&sym_else_clone, &sym_dir_clone).unwrap();
            })
            .unwrap_err();
        assert_eq!(race_sym_err.kind(), StorageErrorKind::CorruptData);
        assert_eq!(
            PinnedDirectory::take_last_verification_drop_record_for_test(),
            Some((
                0,
                0,
                0,
                pinned_sym.source_parent_verification_bytes().unwrap()
            ))
        );

        // Phase 3 (BeforeRetainedCheck): target directory replaced after canonical walk.
        let race_rep_dir = base.join("race_rep_target");
        let race_rep_backup = base.join("race_rep_backup");
        std::fs::create_dir_all(&race_rep_dir).unwrap();
        let pinned_rep = PinnedDirectory::open(Path::new("race_rep_target")).unwrap();
        let rep_dir_clone = race_rep_dir.clone();
        let rep_bak_clone = race_rep_backup.clone();
        let race_rep_err = pinned_rep
            .verify_source_with_race_hook_for_test(&budget, 3, move || {
                std::fs::rename(&rep_dir_clone, &rep_bak_clone).unwrap();
                std::fs::create_dir(&rep_dir_clone).unwrap();
            })
            .unwrap_err();
        assert_eq!(race_rep_err.kind(), StorageErrorKind::CorruptData);
        assert_eq!(
            PinnedDirectory::take_last_verification_drop_record_for_test(),
            Some((
                0,
                0,
                0,
                pinned_rep.source_parent_verification_bytes().unwrap()
            ))
        );
        assert_eq!(pair_live_work(&budget), (0, 0));
        println!("RIFFDB_WP825_SOURCE_PARENT_VERIFY_CHILD_OK");
        return;
    }

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .env(CHILD_ENV, "1")
        .arg("--exact")
        .arg("startup::offline_predecessor::tests::source_execution_domain_tests::source_parent_verification_parity_and_deterministic_races_in_subprocess")
        .arg("--nocapture")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains("RIFFDB_WP825_SOURCE_PARENT_VERIFY_CHILD_OK"),
        "subprocess failed: stdout={}\nstderr={}",
        stdout,
        stderr
    );
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn configured_pair_cancellation_failure_owner_keeps_operation_until_last_primary_drop() {
    let (fixture, root, slot, mut operation, budget) = configured_pair_case(false);
    let idle = operation.idle_probe_for_test();
    let task = operation.take_task_retirement().unwrap();
    let continuation = operation.configured_absent_derived_for_test().unwrap();
    let validated = operation.finish_maintenance_preparation_for_test().unwrap();
    let (facade, intent) = validated.split_facade_for_test();
    intent.cancel_configured_pair_for_test();
    let failure = match facade.attach_configured_derived(continuation) {
        Ok(_) => panic!("actual cancellation must refuse before configured attachment"),
        Err(failure) => failure,
    };
    drop(intent);
    drop(task);
    drop(slot);
    assert!(idle.require_idle().is_err());
    assert!(
        matches!(RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    pair_assert_excluded(&fixture.source);
    let (primary, error) = failure.into_test_parts();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    drop(error);
    assert!(idle.require_idle().is_err());
    drop(primary);
    idle.require_idle().unwrap();
    drop(idle);
    assert_eq!(pair_live_work(&budget), (0, 0));
    drop(RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root).unwrap());
    fixture.assert_unchanged();
}

#[cfg(all(
    target_os = "linux",
    target_arch = "x86_64",
    feature = "projection-source-driver"
))]
fn baseline_unpaired_operation_fixture() -> (
    Fixture,
    PathBuf,
    SourceOfflineStorageSlotV1,
    crate::maintenance::SourceOwnedOperationV1,
    riffdb_types::ProjectionSourceWorkingBudgetV1,
) {
    use std::os::unix::fs::PermissionsExt;
    let (fixture, root, name) = configured_fixture();
    assert!(!crate::store::derived_store_path(&fixture.source).exists());
    // Owned private copy only. Genuine1818 remains immutable. Permission is
    // checked before the actual maintenance/native artifact binding.
    let mut permissions = std::fs::metadata(&fixture.source).unwrap().permissions();
    permissions.set_mode(permissions.mode() | 0o200);
    std::fs::set_permissions(&fixture.source, permissions).unwrap();
    let bootstrap = SourceExecutionBootstrapV1::try_new().unwrap();
    let budget = bootstrap.numerical_budget().clone();
    let storage = RedbMaintenanceStorage::open_for_promotion_recovery_with_source_bootstrap(
        &fixture.source,
        &root,
        bootstrap,
    )
    .unwrap();
    let mut slot = SourceOfflineStorageSlotV1::new(storage);
    let operation = slot
        .loan(&name, inputs(), Arc::new(AtomicBool::new(false)))
        .unwrap();
    (fixture, root, slot, operation, budget)
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(all(
    target_os = "linux",
    target_arch = "x86_64",
    feature = "projection-source-driver"
))]
#[test]
fn configured_original_read_domain_retains_operation_after_semantic_and_task_wrappers_drop() {
    let (fixture, root, slot, mut operation, budget) = baseline_unpaired_operation_fixture();
    let idle = operation.idle_probe_for_test();
    let task = operation.take_task_retirement().unwrap();
    let validated = operation.finish_maintenance_preparation_for_test().unwrap();
    let (mut facade, intent) = validated.split_facade_for_test();
    // No configured binding or replacement SourceWorkingDomain is present. This
    // is the REAL original read/catalog response owner on actual2135 production.
    let brand = facade.prepare_catalog_instruction_brand_for_test().unwrap();
    let input = facade
        .catalog_instruction_input_for_test(&brand, &budget)
        .unwrap();
    drop(intent);
    drop(task);
    drop(slot);
    drop(facade);
    let operation_still_active = idle.require_idle().is_err();
    let maintenance_still_excluded =
        match RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root) {
            Err(error) => error.kind() == StorageErrorKind::Unavailable,
            Ok(unexpected_owner) => {
                drop(unexpected_owner);
                false
            }
        };
    let native_still_excluded = matches!(
        redb::Database::open(&fixture.source),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    );
    drop(input);
    let operation_active_through_last_brand = idle.require_idle().is_err();
    drop(brand);
    idle.require_idle().unwrap();
    drop(idle);
    let released = budget.observation().unwrap();
    assert_eq!((released.reserved_bytes, released.owned_bytes), (0, 0));
    drop(RedbMaintenanceStorage::open_for_promotion_recovery(&fixture.source, &root).unwrap());
    fixture.assert_unchanged();
    // Clean up all physical aliases before reporting the exact original failure.
    // Earlier IO/parser/admission failures cannot satisfy this required marker.
    assert!(
        operation_still_active,
        "BASELINE_LIVE_ORIGINAL_READ_LOST_OPERATION_CLAIM active={} maintenance={} native={} last_brand={}",
        operation_still_active,
        maintenance_still_excluded,
        native_still_excluded,
        operation_active_through_last_brand
    );
    assert!(
        maintenance_still_excluded,
        "live original read must retain maintenance exclusion"
    );
    assert!(
        native_still_excluded,
        "real native original read custody is independently still held"
    );
    assert!(
        operation_active_through_last_brand,
        "last actual brand must retain the operation"
    );
}
