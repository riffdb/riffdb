//! Resource/lifetime proofs on actual1818 unchanged runtime writer4 artifacts.
//! No complete Original1..F/D4/Source publication/cut authority is claimed.
// req: STO-032, PRJ-004, REC-001, REC-005
use super::*;
use crate::engine_owner::source_reopen::OpenedPredecessorSourceReadV1;

fn into_facade(proof: MaintenanceValidatedPredecessorV1) -> crate::store::SourceOwnedOfflineReadV1 {
    let opened = OpenedPredecessorSourceReadV1::from_validated_predecessor(proof).unwrap();
    crate::store::SourceOwnedOfflineReadV1::from_opened(opened)
}

#[test]
fn consuming_source_phase_keeps_same_ledger_and_reads_real_native_point_and_range() {
    let fixture = Fixture::copy();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    let budget = proof
        .artifacts
        .backup
        .engine()
        .source_working_budget()
        .clone();
    let frontier = proof
        .observation
        .history
        .tail()
        .frontier()
        .application()
        .unwrap()
        .get();
    let expected_identity = {
        let read = proof.artifacts.backup.begin_read().unwrap();
        let meta = read.open_table(crate::layout::META).unwrap();
        let value = meta.get("database_id").unwrap().unwrap();
        <[u8; 32]>::from(Sha256::digest(value.value()))
    };
    let mut facade = into_facade(proof);
    assert!(
        facade
            .originating_budget_for_test()
            .shares_owner_with(&budget)
    );
    assert!(budget.try_acquire_worker().is_err());
    assert!(matches!(
        redb::ReadOnlyDatabase::open(&fixture.source),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    {
        let mut read = facade.begin_read().unwrap();
        {
            let mut meta = read.open_str_table("meta").unwrap();
            let value = meta.get("database_id").unwrap().unwrap();
            assert_eq!(
                <[u8; 32]>::from(Sha256::digest(value.value())),
                expected_identity
            );
        }
        {
            let mut commands = read.open_bytes_table("commits").unwrap();
            let mut cursor = commands.range_from(None).unwrap();
            let row = cursor
                .next()
                .unwrap()
                .expect("real runtime command/segment row");
            // Native segment keys identify the FIRST command, never its terminal.
            let first_command = crate::keys::decode_application_sequence_key(row.key()).unwrap();
            assert!(first_command.get() <= frontier);
            assert!(!row.value().is_empty());
        }
    }
    fixture.assert_unchanged();
    drop(facade);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    drop(
        budget
            .try_acquire_worker()
            .expect("actual moved worker retired"),
    );
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    fixture.assert_unchanged();
}

#[test]
fn consuming_source_phase_refuses_surviving_real_maintenance_engine_alias() {
    let fixture = Fixture::copy();
    let owner = fixture.owner();
    let alias = owner.artifacts.backup.engine().clone();
    let proof = owner.validate_complete_maintenance().unwrap();
    let error = OpenedPredecessorSourceReadV1::from_validated_predecessor(proof)
        .err()
        .expect("actual old engine alias prevents cache teardown");
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    // No invented epoch/IDs join can turn this retained engine into Source.
    drop(alias);
    fixture.assert_unchanged();
}

#[test]
fn consuming_source_phase_full_ledger_refuses_before_native_reopen_and_releases_custody() {
    let fixture = Fixture::copy();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    let budget = proof
        .artifacts
        .backup
        .engine()
        .source_working_budget()
        .clone();
    let live = budget.observation().unwrap();
    let free = riffdb_types::exact_provider_budget_v1::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        .checked_sub(live.fixed_owner_bytes)
        .unwrap()
        .checked_sub(live.owned_bytes)
        .unwrap()
        .checked_sub(live.reserved_bytes)
        .unwrap();
    let pressure = budget.reserve(free).unwrap();
    let error = OpenedPredecessorSourceReadV1::from_validated_predecessor(proof)
        .err()
        .expect("actual original64MiB ledger is saturated");
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    drop(pressure);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    drop(budget.try_acquire_worker().unwrap());
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    fixture.assert_unchanged();
}

#[test]
fn forgotten_source_range_retains_descriptors_primary_worker_and_actual_charge() {
    let fixture = Fixture::copy();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    let mut facade = into_facade(proof);
    let budget = facade.originating_budget_for_test().clone();
    let charged = budget.observation().unwrap().reserved_bytes;
    {
        let mut read = facade.begin_read().unwrap();
        let mut commands = read.open_bytes_table("commits").unwrap();
        let range = commands.range_from(None).unwrap();
        std::mem::forget(range);
        assert!(matches!(commands.get(&1u64.to_be_bytes()),
            Err(error) if error.kind()==StorageErrorKind::Unavailable));
    }
    assert!(matches!(facade.begin_read(),
        Err(error) if error.kind()==StorageErrorKind::Unavailable));
    drop(facade);
    assert!(budget.observation().unwrap().reserved_bytes >= charged);
    assert!(budget.try_acquire_worker().is_err());
    assert!(matches!(
        redb::ReadOnlyDatabase::open(&fixture.source),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    fixture.assert_unchanged();
}

#[test]
fn consuming_source_phase_replays_exact_original_prefix_with_owned_resolver_twice() {
    // Real immutable writer4 artifacts, complete maintenance validation, actual
    // consumed native facade and the moved ledger. No Primary ports/repairing
    // reopen, caller command/EOF, synthetic backend or Source publication.
    let fixture = Fixture::copy();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    let history = proof.observation.history;
    let frontier = history.tail().frontier().application();
    let head = frontier.expect("genuine original fixture has acknowledged commands");
    assert!(
        (2..=65_536).contains(&head.get()),
        "at least two real commands must be traversed"
    );
    let budget = proof
        .artifacts
        .backup
        .engine()
        .source_working_budget()
        .clone();
    let mut facade = into_facade(proof);
    assert!(
        facade
            .originating_budget_for_test()
            .shares_owner_with(&budget)
    );
    let mut first_count = None;
    for _pass in 0..2 {
        let mut cursor = facade.original_cursor().unwrap();
        assert_eq!(cursor.captured_history(), history);
        let mut expected = Some(riffdb_types::CommitSequence::first());
        let mut commands = 0_u64;
        while let Some(command) = cursor.next_commit().unwrap() {
            assert_eq!(Some(command.commit_sequence()), expected);
            assert!(frontier.is_some_and(|head| command.commit_sequence() <= head));
            expected = command.commit_sequence().checked_next();
            commands = commands.checked_add(1).unwrap();
            assert!(commands <= 65_536);
        }
        assert_eq!(commands, frontier.map_or(0, |head| head.get()));
        assert!(cursor.next_commit().unwrap().is_none());
        if let Some(first) = first_count {
            assert_eq!(first, commands);
        }
        first_count = Some(commands);
        drop(cursor);
        assert!(matches!(
            redb::ReadOnlyDatabase::open(&fixture.source),
            Err(redb::DatabaseError::DatabaseAlreadyOpen)
        ));
        fixture.assert_unchanged();
    }
    drop(facade);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    drop(
        budget
            .try_acquire_worker()
            .expect("same moved worker retired after cursor/facade"),
    );
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    fixture.assert_unchanged();
}

// req: STO-032, PRJ-004, REC-005
#[cfg(feature = "projection-source-driver")]
#[test]
fn source_catalog_instruction_decodes_real_owned_envelope_and_releases_original_native_custody() {
    let fixture = Fixture::copy();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    assert!(
        proof.observation.active_pointer_digest.is_some(),
        "genuine writer catalog exists"
    );
    let budget = proof
        .artifacts
        .backup
        .engine()
        .source_working_budget()
        .clone();
    let mut facade = into_facade(proof);
    let brand = facade.prepare_catalog_instruction_brand_for_test().unwrap();
    let before = budget.observation().unwrap();
    let inputs = facade
        .catalog_instruction_input_for_test(&brand, &budget)
        .unwrap();
    // Constructor earned the actual pointer identity against its SAME-owner
    // native envelope and sole StorageAPI decoder; no caller metadata is used.
    assert!(budget.observation().unwrap().owned_bytes > before.owned_bytes);
    drop(inputs);
    let mut released = budget.observation().unwrap();
    assert!(released.released_allocations > before.released_allocations);
    // Historical completed releases advance; every live gauge and all other
    // actual accounting fields return to their original baseline.
    released.released_allocations = before.released_allocations;
    assert_eq!(released, before);
    drop(brand);
    drop(facade);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    drop(budget.try_acquire_worker().unwrap());
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    fixture.assert_unchanged();
}

// req: STO-032, PRJ-004, REC-005
#[cfg(feature = "projection-source-driver")]
#[test]
fn forgotten_source_catalog_input_retains_actual_native_descriptors_worker_and_charge() {
    let fixture = Fixture::copy();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    assert!(
        proof.observation.active_pointer_digest.is_some(),
        "genuine writer catalog exists"
    );
    let budget = proof
        .artifacts
        .backup
        .engine()
        .source_working_budget()
        .clone();
    let mut facade = into_facade(proof);
    let brand = facade.prepare_catalog_instruction_brand_for_test().unwrap();
    let inputs = facade
        .catalog_instruction_input_for_test(&brand, &budget)
        .unwrap();
    let admitted = budget.observation().unwrap();
    assert!(admitted.owned_bytes > 0);
    std::mem::forget(inputs);
    drop(brand);
    drop(facade);
    let retained = budget.observation().unwrap();
    assert!(retained.owned_bytes > 0);
    assert!(retained.reserved_bytes > 0);
    assert!(retained.owned_bytes <= admitted.owned_bytes);
    assert!(budget.try_acquire_worker().is_err());
    assert!(matches!(
        redb::ReadOnlyDatabase::open(&fixture.source),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    fixture.assert_unchanged();
    // No recoverable child/Weak/raw handle remains. The private test process
    // physically retires the intentional leak; runner joins it before cleanup.
}

// Genuine already-parsed fixture engine only: this is NOT a cold Source open.
// Its ordinary test-stack construction precedes Source admission and cannot be
// used to claim a production initial-maintenance bound or derived-base authority.
#[cfg(feature = "projection-source-driver")]
fn auxiliary_d4_fixture(fixture: &Fixture) -> (PathBuf, [u8; 32], redb::ReadOnlyReopenHandoff) {
    use redb::ReadableDatabase;
    let path = fixture._scope.join("auxiliary-native.redb");
    std::fs::copy(fixture.backup.join("database.redb"), &path).unwrap();
    // Both are owned private copies. Correct/check owner-write permission
    // BEFORE either native engine/proof binds metadata; genuine1818 is untouched.
    private_fixture_writer_mode(&fixture.source);
    private_fixture_writer_mode(&path);
    let mut builder = redb::Database::builder();
    builder.set_cache_size(0);
    let database = builder
        .open_read_only_maintenance_file(std::fs::File::open(&path).unwrap())
        .unwrap();
    // Actual fixture identity validation also supplies the bytes later checked
    // by the planned auxiliary read; this is not a speculative warming pass.
    let identity = {
        let read = database.begin_read().unwrap();
        let meta = read.open_table(crate::layout::META).unwrap();
        let value = meta.get("database_id").unwrap().unwrap();
        <[u8; 32]>::from(Sha256::digest(value.value()))
    };
    let plan = database.plan_read_only_reopen().unwrap();
    let handoff = database.into_read_only_reopen_handoff(plan).unwrap();
    (path, identity, handoff)
}
#[cfg(feature = "projection-source-driver")]
fn private_fixture_writer_mode(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    if permissions.mode() & 0o200 == 0 {
        permissions.set_mode(permissions.mode() | 0o200);
        std::fs::set_permissions(path, permissions).unwrap();
    }
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o200,
        0o200
    );
}
#[cfg(feature = "projection-source-driver")]
fn assert_native_and_legacy_excluded(path: &Path) {
    assert!(matches!(
        redb::Database::open(path),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    let file = std::fs::File::open(path).unwrap();
    assert!(matches!(
        file.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(feature = "projection-source-driver")]
#[test]
fn paired_read_primary_catalog_alias_retains_auxiliary_exclusions_until_final_drop() {
    let fixture = Fixture::copy();
    let (auxiliary_path, expected_identity, handoff) = auxiliary_d4_fixture(&fixture);
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    let budget = proof.numerical_source_budget().clone();
    let mut facade = into_facade(proof);
    facade.attach_prepared_auxiliary_read(handoff).unwrap();
    {
        let mut read = facade.begin_auxiliary_read().unwrap();
        let mut meta = read.open_str_table("meta").unwrap();
        let value = meta.get("database_id").unwrap().unwrap();
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(value.value())),
            expected_identity
        );
    }
    let brand = facade.prepare_catalog_instruction_brand_for_test().unwrap();
    let inputs = facade
        .catalog_instruction_input_for_test(&brand, &budget)
        .unwrap();
    drop(facade);
    assert_native_and_legacy_excluded(&auxiliary_path);
    assert_native_and_legacy_excluded(&fixture.source);
    assert!(budget.try_acquire_worker().is_err());
    drop(inputs);
    assert_native_and_legacy_excluded(&auxiliary_path);
    drop(brand);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    drop(budget.try_acquire_worker().unwrap());
    drop(redb::ReadOnlyDatabase::open(&auxiliary_path).unwrap());
    drop(redb::ReadOnlyDatabase::open(&fixture.source).unwrap());
    fixture.assert_unchanged();
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(feature = "projection-source-driver")]
#[test]
fn paired_read_n_minus_one_refuses_auxiliary_open_without_refunding_primary_owner() {
    let fixture = Fixture::copy();
    let (auxiliary_path, _, handoff) = auxiliary_d4_fixture(&fixture);
    let required =
        crate::engine_owner::source_reopen::auxiliary_source_read_bytes(&handoff).unwrap();
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    let budget = proof.numerical_source_budget().clone();
    let mut facade = into_facade(proof);
    let before = budget.observation().unwrap();
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        .checked_sub(before.fixed_owner_bytes)
        .unwrap()
        .checked_sub(before.owned_bytes)
        .unwrap()
        .checked_sub(before.reserved_bytes)
        .unwrap();
    assert!(required > 0 && required <= available);
    let pressure = budget.reserve(available - (required - 1)).unwrap();
    let held = budget.observation().unwrap();
    let error = facade.attach_prepared_auxiliary_read(handoff).unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert_eq!(budget.observation().unwrap(), held);
    assert!(budget.try_acquire_worker().is_err());
    assert_native_and_legacy_excluded(&fixture.source);
    drop(redb::ReadOnlyDatabase::open(&auxiliary_path).unwrap());
    {
        let mut read = facade.begin_read().unwrap();
        let mut meta = read.open_str_table("meta").unwrap();
        assert!(meta.get("database_id").unwrap().is_some());
    }
    drop(pressure);
    assert_eq!(budget.observation().unwrap(), before);
    drop(facade);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    drop(budget.try_acquire_worker().unwrap());
    fixture.assert_unchanged();
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(feature = "projection-source-driver")]
#[test]
fn paired_read_forgotten_primary_catalog_input_retains_both_files_and_original_ledger() {
    let fixture = Fixture::copy();
    let (auxiliary_path, _, handoff) = auxiliary_d4_fixture(&fixture);
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    let budget = proof.numerical_source_budget().clone();
    let mut facade = into_facade(proof);
    facade.attach_prepared_auxiliary_read(handoff).unwrap();
    let brand = facade.prepare_catalog_instruction_brand_for_test().unwrap();
    let inputs = facade
        .catalog_instruction_input_for_test(&brand, &budget)
        .unwrap();
    let admitted = budget.observation().unwrap();
    std::mem::forget(inputs);
    drop(brand);
    drop(facade);
    let retained = budget.observation().unwrap();
    assert!(retained.reserved_bytes > 0 && retained.reserved_bytes <= admitted.reserved_bytes);
    assert!(retained.owned_bytes > 0 && retained.owned_bytes <= admitted.owned_bytes);
    assert!(budget.try_acquire_worker().is_err());
    assert_native_and_legacy_excluded(&fixture.source);
    assert_native_and_legacy_excluded(&auxiliary_path);
    fixture.assert_unchanged();
    // One intentional bounded leak. Only the joined private test process retires
    // it; no public Source-memory, completion, cold-construction or seed claim.
}

// req: STO-020, STO-032, PRJ-004, REC-005
#[cfg(feature = "projection-source-driver")]
#[test]
fn paired_read_materialized_primary_catalog_alias_refuses_late_auxiliary_attach() {
    let fixture = Fixture::copy();
    let (auxiliary_path, _, handoff) = auxiliary_d4_fixture(&fixture);
    let proof = fixture.owner().validate_complete_maintenance().unwrap();
    let budget = proof.numerical_source_budget().clone();
    let mut facade = into_facade(proof);
    let brand = facade.prepare_catalog_instruction_brand_for_test().unwrap();
    let inputs = facade
        .catalog_instruction_input_for_test(&brand, &budget)
        .unwrap();
    let held = budget.observation().unwrap();
    let error = facade.attach_prepared_auxiliary_read(handoff).unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert_eq!(budget.observation().unwrap(), held);
    assert!(budget.try_acquire_worker().is_err());
    assert_native_and_legacy_excluded(&fixture.source);
    // Refusal destroyed ONLY the unadopted auxiliary handoff. The actual
    // primary response/brand, ledger and worker remain unchanged and usable.
    drop(redb::ReadOnlyDatabase::open(&auxiliary_path).unwrap());
    let auxiliary_observer = std::fs::File::open(&auxiliary_path).unwrap();
    auxiliary_observer.try_lock().unwrap();
    auxiliary_observer.unlock().unwrap();
    drop(auxiliary_observer);
    {
        let mut read = facade.begin_read().unwrap();
        let mut meta = read.open_str_table("meta").unwrap();
        assert!(meta.get("database_id").unwrap().is_some());
    }
    assert_eq!(budget.observation().unwrap(), held);
    drop(inputs);
    drop(brand);
    drop(facade);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    drop(budget.try_acquire_worker().unwrap());
    fixture.assert_unchanged();
}
