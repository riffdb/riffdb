//! Resource-only native artifacts plus a genuine runtime Original lease.
//! A resource artifact is never interpreted as the captured application's input.
use super::*;
use crate::store::SourceOwnedNativeRead;
use redb::ReadableDatabase;
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};
const RESOURCE_ROWS: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("source-resource-rows");
const RESOURCE_META: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("source-resource-meta");
fn artifact(root: &crate::test_path::ScopedDirectory, large: bool) -> PathBuf {
    let path = root.join("numerical-read-artifact.redb");
    let database = redb::Database::create(&path).unwrap();
    let write = database.begin_write().unwrap();
    {
        let mut rows = write.open_table(RESOURCE_ROWS).unwrap();
        for i in 0..300_u32 {
            rows.insert(i.to_be_bytes().as_slice(), [7_u8; 300].as_slice())
                .unwrap();
        }
        if large {
            rows.insert(b"large".as_slice(), vec![9_u8; 128 * 1024].as_slice())
                .unwrap();
        }
    }
    write
        .open_table(RESOURCE_META)
        .unwrap()
        .insert("identity", b"resource-only".as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(database);
    path
}
fn descriptor(path: &Path) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap()
}
fn handoff(path: &Path, visit_rows: bool) -> redb::ReadOnlyReopenHandoff {
    let database = redb::Database::builder()
        .open_read_only_maintenance_file(descriptor(path))
        .unwrap();
    if visit_rows {
        let read = database.begin_read().unwrap();
        let rows = read.open_table(RESOURCE_ROWS).unwrap();
        for i in 0..300_u32 {
            assert_eq!(
                rows.get(i.to_be_bytes().as_slice())
                    .unwrap()
                    .unwrap()
                    .value(),
                [7; 300]
            );
        }
        assert_eq!(
            read.open_table(RESOURCE_META)
                .unwrap()
                .get("identity")
                .unwrap()
                .unwrap()
                .value(),
            b"resource-only"
        );
    }
    let plan = database.plan_read_only_reopen().unwrap();
    database.into_read_only_reopen_handoff(plan).unwrap()
}
fn assert_file_released(path: &Path) {
    let file = descriptor(path);
    file.try_lock().unwrap();
    file.unlock().unwrap();
}

// req: PRJ-004, STO-020, REC-001, REC-005
#[cfg(unix)]
#[test]
fn source_native_read_preadmits_peak_on_actual_prefix_ledger_and_keeps_all_children_charged() {
    let (root, ports, _) = fixture();
    join_actual_cold_locator_prerequisite(&ports);
    let path = artifact(&root, false);
    let handoff = handoff(&path, true);
    let peak = handoff.peak_bytes();
    // Native maintenance engine/cache aliases are gone before the Source lease
    // and admitted planned owner begin their resource-only phase.
    let prefix = ports.capture_original_prefix_retention().unwrap();
    assert!(prefix.belongs_to_ports(&ports));
    let budget = prefix.originating_budget();
    assert!(std::ptr::eq(
        budget,
        ports.shared.database.source_working_budget()
    ));
    let before = budget.observation().unwrap();
    let mut owner = SourceOwnedNativeRead::open(&prefix, handoff).unwrap();
    let admitted = budget.observation().unwrap();
    assert!(admitted.reserved_bytes - before.reserved_bytes >= peak);
    assert!(
        admitted.fixed_owner_bytes + admitted.reserved_bytes + admitted.owned_bytes
            <= riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
    );
    assert!(budget.try_acquire_worker().is_err());
    {
        let mut read = owner.begin_read().unwrap();
        {
            let mut meta = read.open_str_table("source-resource-meta").unwrap();
            let value = meta.get("identity").unwrap().unwrap();
            assert_eq!(value.value(), b"resource-only");
            assert_eq!(budget.observation().unwrap(), admitted);
        }
        let mut rows = read.open_bytes_table("source-resource-rows").unwrap();
        {
            let value = rows.get(&99_u32.to_be_bytes()).unwrap().unwrap();
            assert_eq!(value.value(), [7; 300]);
        }
        let mut range = rows.range_from(Some(&100_u32.to_be_bytes())).unwrap();
        let mut count = 0_u32;
        while let Some(row) = range.next().unwrap() {
            assert_eq!(row.key(), (100 + count).to_be_bytes());
            assert_eq!(row.value(), [7; 300]);
            assert_eq!(budget.observation().unwrap(), admitted);
            count += 1;
        }
        assert_eq!(count, 200);
    }
    assert_eq!(
        budget.observation().unwrap(),
        admitted,
        "dropping children cannot refund the live native owner"
    );
    drop(owner);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    assert_file_released(&path);
    assert!(
        budget.try_acquire_worker().is_err(),
        "resource teardown does not release its caller's Original lease"
    );
    drop(prefix);
    assert!(
        ports
            .shared
            .database
            .source_working_budget()
            .try_acquire_worker()
            .is_ok()
    );
}

// req: PRJ-004, STO-020, REC-001, REC-005
#[cfg(unix)]
#[test]
fn source_native_read_refusal_releases_handoff_and_preserves_originating_ledger() {
    let (root, ports, _) = fixture();
    let path = artifact(&root, false);
    let handoff = handoff(&path, true);
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget();
    let before = budget.observation().unwrap();
    let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.owned_bytes
        - before.reserved_bytes;
    let pressure = budget.reserve(free).unwrap();
    let saturated = budget.observation().unwrap();
    assert!(
        matches!(SourceOwnedNativeRead::open(&prefix, handoff), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), saturated);
    assert_file_released(&path);
    assert!(budget.try_acquire_worker().is_err());
    drop(pressure);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
}

// req: PRJ-004, STO-020, REC-001
#[cfg(unix)]
#[test]
fn source_native_read_early_return_destroys_native_custody_before_refund() {
    fn cancelled(
        prefix: &crate::store::OriginalPrefixRetentionLease,
        handoff: redb::ReadOnlyReopenHandoff,
    ) -> Result<(), StorageError> {
        let mut owner = SourceOwnedNativeRead::open(prefix, handoff)?;
        let mut read = owner.begin_read()?;
        let mut rows = read.open_bytes_table("source-resource-rows")?;
        let _value = rows.get(&0_u32.to_be_bytes())?.unwrap();
        // Closed cancellation outcome; every resource owner is local and drops
        // on this return. No callback, native error graph or read handle escapes.
        Err(crate::error::storage_error(StorageErrorKind::Unavailable))
    }
    let (root, ports, _) = fixture();
    let path = artifact(&root, false);
    let handoff = handoff(&path, true);
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let budget = prefix.originating_budget();
    let before = budget.observation().unwrap();
    assert_eq!(
        cancelled(&prefix, handoff).unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    assert_file_released(&path);
    assert!(budget.try_acquire_worker().is_err());
}

// req: PRJ-004, STO-020, REC-001, LOG-001
#[cfg(unix)]
#[test]
fn source_native_read_keeps_ordinary_compiled_writes_independent_of_its_full_ledger() {
    let (root, ports, _) = fixture();
    join_actual_cold_locator_prerequisite(&ports);
    let path = artifact(&root, false);
    let handoff = handoff(&path, true);
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let captured = prefix.captured_history();
    let budget = prefix.originating_budget();
    let owner = SourceOwnedNativeRead::open(&prefix, handoff).unwrap();
    let before = budget.observation().unwrap();
    let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.owned_bytes
        - before.reserved_bytes;
    let pressure = budget.reserve(free).unwrap();
    let saturated = budget.observation().unwrap();
    super::super::native_source_command_fixture::append_journaled(&ports, 2).unwrap();
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(2).unwrap())
            .unwrap()
            .unwrap()
            .commit_sequence()
            .get(),
        2
    );
    assert_eq!(budget.observation().unwrap(), saturated);
    assert_eq!(prefix.captured_history(), captured);
    drop(pressure);
    drop(owner);
    assert_file_released(&path);
}

// req: PRJ-004, STO-020, REC-001
#[cfg(unix)]
#[test]
fn source_native_read_maps_native_capacity_without_absence_or_escaped_diagnostics() {
    let (root, ports, _) = fixture();
    let path = artifact(&root, true);
    // Do not observe the oversized application leaf during normal maintenance
    // planning. This fixture exercises numerical refusal, not full Source input.
    let handoff = handoff(&path, false);
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let mut owner = SourceOwnedNativeRead::open(&prefix, handoff).unwrap();
    let budget = prefix.originating_budget();
    let admitted = budget.observation().unwrap();
    {
        let mut read = owner.begin_read().unwrap();
        let mut rows = read.open_bytes_table("source-resource-rows").unwrap();
        assert_eq!(
            rows.get(&0_u32.to_be_bytes()).unwrap().unwrap().value(),
            [7; 300]
        );
        assert!(
            matches!(rows.get(b"large"), Err(error) if error.kind() == StorageErrorKind::Unavailable)
        );
        assert!(
            matches!(rows.range_from(Some(&[0; 4097])), Err(error) if error.kind() == StorageErrorKind::Unavailable)
        );
    }
    assert_eq!(budget.observation().unwrap(), admitted);
    drop(owner);
    assert_file_released(&path);
}

// req: PRJ-004, STO-020, REC-001
#[cfg(unix)]
#[test]
fn source_native_read_forgotten_value_retains_the_actual_numeric_charge_after_parent_drop() {
    let (root, ports, _) = fixture();
    let path = artifact(&root, false);
    let handoff = handoff(&path, true);
    let budget = ports.shared.database.source_working_budget();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let before = budget.observation().unwrap();
    let mut owner = SourceOwnedNativeRead::open(&prefix, handoff).unwrap();
    let admitted = budget.observation().unwrap();
    let refused;
    {
        let mut read = owner.begin_read().unwrap();
        let mut table = read.open_bytes_table("source-resource-rows").unwrap();
        let value = table.get(&0_u32.to_be_bytes()).unwrap().unwrap();
        assert_eq!(value.value(), [7; 300]);
        // Forget is safe Rust. It intentionally leaks the native page alias;
        // the exact originating numeric charge must leak alongside that alias.
        std::mem::forget(value);
        refused = matches!(table.get(&1_u32.to_be_bytes()), Err(error) if error.kind() == StorageErrorKind::Unavailable);
    }
    drop(owner);
    eprintln!(
        "forgotten_native_alias_reserved_before={} admitted={} after_parent={}",
        before.reserved_bytes,
        admitted.reserved_bytes,
        budget.observation().unwrap().reserved_bytes
    );
    assert_eq!(
        budget.observation().unwrap(),
        admitted,
        "forgotten native alias must retain actual numeric admission"
    );
    assert!(
        refused,
        "forgotten child cannot authorize another native construction"
    );
    let numeric_charge = admitted.reserved_bytes - before.reserved_bytes;
    assert!(numeric_charge > 0);
    drop(prefix);
    assert!(budget.observation().unwrap().reserved_bytes >= numeric_charge);
    assert!(
        budget.try_acquire_worker().is_ok(),
        "numeric owner has no worker/engine capability"
    );
}

// req: PRJ-004, STO-020, REC-001, REC-005
#[cfg(unix)]
#[test]
fn source_native_read_forgotten_read_retains_file_custody_and_numeric_charge_after_parent_drop() {
    let (root, ports, _) = fixture();
    let path = artifact(&root, false);
    let handoff = handoff(&path, true);
    let budget = ports.shared.database.source_working_budget();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let before = budget.observation().unwrap();
    let mut owner = SourceOwnedNativeRead::open(&prefix, handoff).unwrap();
    let admitted = budget.observation().unwrap();
    let read = owner.begin_read().unwrap();
    std::mem::forget(read);
    let refused =
        matches!(owner.begin_read(), Err(error) if error.kind() == StorageErrorKind::Unavailable);
    drop(owner);
    eprintln!(
        "forgotten_native_alias_reserved_before={} admitted={} after_parent={}",
        before.reserved_bytes,
        admitted.reserved_bytes,
        budget.observation().unwrap().reserved_bytes
    );
    assert_eq!(
        budget.observation().unwrap(),
        admitted,
        "forgotten native alias must retain actual numeric admission"
    );
    assert!(
        refused,
        "forgotten child cannot authorize another native construction"
    );
    assert!(
        descriptor(&path).try_lock().is_err(),
        "forgotten native read still owns the actual file"
    );
    let numeric_charge = admitted.reserved_bytes - before.reserved_bytes;
    assert!(numeric_charge > 0);
    drop(prefix);
    assert!(budget.observation().unwrap().reserved_bytes >= numeric_charge);
    assert!(budget.try_acquire_worker().is_ok());
}

// req: PRJ-004, STO-020, REC-001
#[cfg(unix)]
#[test]
fn source_native_read_forgotten_range_retains_actual_numeric_charge_after_parent_drop() {
    let (root, ports, _) = fixture();
    let path = artifact(&root, false);
    let handoff = handoff(&path, true);
    let budget = ports.shared.database.source_working_budget();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let before = budget.observation().unwrap();
    let mut owner = SourceOwnedNativeRead::open(&prefix, handoff).unwrap();
    let admitted = budget.observation().unwrap();
    let refused;
    {
        let mut read = owner.begin_read().unwrap();
        let mut table = read.open_bytes_table("source-resource-rows").unwrap();
        let mut range = table.range_from(None).unwrap();
        let row = range.next().unwrap().unwrap();
        assert_eq!(row.key(), 0_u32.to_be_bytes());
        assert_eq!(row.value(), [7; 300]);
        drop(row);
        std::mem::forget(range);
        refused = matches!(table.get(&1_u32.to_be_bytes()), Err(error) if error.kind() == StorageErrorKind::Unavailable);
    }
    drop(owner);
    eprintln!(
        "forgotten_native_alias_reserved_before={} admitted={} after_parent={}",
        before.reserved_bytes,
        admitted.reserved_bytes,
        budget.observation().unwrap().reserved_bytes
    );
    assert_eq!(
        budget.observation().unwrap(),
        admitted,
        "forgotten native alias must retain actual numeric admission"
    );
    assert!(
        refused,
        "forgotten child cannot authorize another native construction"
    );
    assert!(
        descriptor(&path).try_lock().is_err(),
        "forgotten native range still owns the actual file"
    );
    let numeric_charge = admitted.reserved_bytes - before.reserved_bytes;
    assert!(numeric_charge > 0);
    drop(prefix);
    assert!(budget.observation().unwrap().reserved_bytes >= numeric_charge);
    assert!(budget.try_acquire_worker().is_ok());
}

// req: PRJ-004, STO-020, REC-001
#[cfg(unix)]
#[test]
fn source_native_read_forgotten_row_blocks_next_and_retains_actual_numeric_charge() {
    let (root, ports, _) = fixture();
    let path = artifact(&root, false);
    let handoff = handoff(&path, true);
    let budget = ports.shared.database.source_working_budget();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let before = budget.observation().unwrap();
    let mut owner = SourceOwnedNativeRead::open(&prefix, handoff).unwrap();
    let admitted = budget.observation().unwrap();
    let refused;
    {
        let mut read = owner.begin_read().unwrap();
        let mut table = read.open_bytes_table("source-resource-rows").unwrap();
        let mut range = table.range_from(None).unwrap();
        let row = range.next().unwrap().unwrap();
        assert_eq!(row.key(), 0_u32.to_be_bytes());
        assert_eq!(row.value(), [7; 300]);
        std::mem::forget(row);
        refused =
            matches!(range.next(), Err(error) if error.kind() == StorageErrorKind::Unavailable);
    }
    drop(owner);
    eprintln!(
        "forgotten_native_alias_reserved_before={} admitted={} after_parent={}",
        before.reserved_bytes,
        admitted.reserved_bytes,
        budget.observation().unwrap().reserved_bytes
    );
    assert_eq!(
        budget.observation().unwrap(),
        admitted,
        "forgotten native alias must retain actual numeric admission"
    );
    assert!(
        refused,
        "forgotten child cannot authorize another native construction"
    );
    let numeric_charge = admitted.reserved_bytes - before.reserved_bytes;
    assert!(numeric_charge > 0);
    drop(prefix);
    assert!(budget.observation().unwrap().reserved_bytes >= numeric_charge);
    assert!(budget.try_acquire_worker().is_ok());
}
