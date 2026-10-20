//! Real published primary bases, not reconstructed identities or Source authority.
use super::*;
use crate::store::OriginalSourceReadContext;
use std::fs::OpenOptions;

fn unknown_file_fixture() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
) {
    let root = crate::test_path::ScopedDirectory::new("source-primary-unknown-backend");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(root.join("db.redb"))
        .unwrap();
    let backend = redb::backends::FileBackend::new(file).unwrap();
    // The dynamic seam deliberately erases concrete backend provenance. A
    // caller-supplied file backend must not receive the closed File-arm brand.
    let media = crate::RedbStorageMedia::new(backend, std::sync::Arc::new(crate::RealJournalMedia));
    let mut store = RedbStore::open_with_storage_media(root.join("db.redb"), media).unwrap();
    let id =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    store.initialize_projection_source_fixture(id).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("genuine unknown-backend fixture must complete existing startup");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    super::super::native_source_command_fixture::activate(&mut ports).unwrap();
    super::super::native_source_command_fixture::append_journaled(&ports, 1).unwrap();
    (root, ports)
}

// req: PRJ-004, STO-020, REC-001, LOG-001
#[test]
fn actual_primary_context_reads_same_published_checkpoint_and_keeps_compiled_writes_independent() {
    let (_root, ports, active) = fixture();
    join_actual_cold_locator_prerequisite(&ports);
    // The ordinary command published this view before any source worker exists.
    assert!(matches!(
        ports.begin_composite_read().unwrap(),
        crate::store::RedbReadAccess::Composite(_)
    ));
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let context = OriginalSourceReadContext::admit(&ports).unwrap();
    let captured = context.begin_existing_native_captured_read().unwrap();
    assert!(captured.native_existing_composite_for_test());
    let expected_id = riffdb_storage_api::proto_codec::encode_database_identity_v1(
        prefix.captured_history().lineage().database_id(),
    )
    .unwrap();
    let expected_active =
        riffdb_storage_api::proto_codec::encode_active_catalog_pointer_v1(&active).unwrap();
    let budget = ports.shared.database.source_working_budget();
    let before = budget.observation().unwrap();
    let mut read = captured.open_native_checkpoint_read(&prefix).unwrap();
    let admitted = budget.observation().unwrap();
    assert!(admitted.reserved_bytes > before.reserved_bytes);
    {
        let mut meta = read.open_str_table("meta").unwrap();
        let id = meta.get(crate::layout::META_DATABASE_ID).unwrap().unwrap();
        assert_eq!(id.value(), expected_id.as_bytes());
        assert_eq!(budget.observation().unwrap(), admitted);
    }
    {
        let mut table = read.open_bytes_table("catalog_active").unwrap();
        assert_eq!(
            table
                .get(crate::layout::CATALOG_ACTIVE_KEY.as_slice())
                .unwrap()
                .unwrap()
                .value(),
            expected_active.as_bytes()
        );
        let mut range = table.range_from(None).unwrap();
        let row = range.next().unwrap().unwrap();
        assert_eq!(row.key(), crate::layout::CATALOG_ACTIVE_KEY);
        assert_eq!(row.value(), expected_active.as_bytes());
        drop(row);
        assert!(range.next().unwrap().is_none());
    }
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - admitted.fixed_owner_bytes
        - admitted.reserved_bytes
        - admitted.owned_bytes;
    let pressure = budget.reserve(available).unwrap();
    let full = budget.observation().unwrap();
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
    assert_eq!(prefix.captured_through(), CommitSequence::first());
    assert_eq!(budget.observation().unwrap(), full);
    {
        let mut meta = read.open_str_table("meta").unwrap();
        assert_eq!(
            meta.get(crate::layout::META_DATABASE_ID)
                .unwrap()
                .unwrap()
                .value(),
            expected_id.as_bytes()
        );
    }
    drop(pressure);
    drop(read);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn actual_primary_context_refuses_saturated_admission_and_foreign_same_identity_lease() {
    let (_root, ports, _) = fixture();
    let (_foreign_root, foreign, _) = fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let foreign_prefix = foreign.capture_original_prefix_retention().unwrap();
    assert_eq!(
        prefix.captured_history().lineage().database_id(),
        foreign_prefix.captured_history().lineage().database_id()
    );
    assert!(!foreign_prefix.belongs_to_ports(&ports));
    let context = OriginalSourceReadContext::admit(&ports).unwrap();
    let captured = context.begin_existing_native_captured_read().unwrap();
    let budget = ports.shared.database.source_working_budget();
    let foreign_budget = foreign.shared.database.source_working_budget();
    let before = budget.observation().unwrap();
    let foreign_before = foreign_budget.observation().unwrap();
    assert!(
        matches!(captured.open_native_checkpoint_read(&foreign_prefix), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), before);
    assert_eq!(foreign_budget.observation().unwrap(), foreign_before);
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.reserved_bytes
        - before.owned_bytes;
    let pressure = budget.reserve(available).unwrap();
    let full = budget.observation().unwrap();
    assert!(
        matches!(captured.open_native_checkpoint_read(&prefix), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), full);
    drop(pressure);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
}

// req: PRJ-004, STO-020, REC-001, LOG-001
#[test]
fn actual_primary_context_requires_closed_backend_provenance_and_preserves_ordinary_commands() {
    let (_root, ports) = unknown_file_fixture();
    let (_known_root, known_ports, _) = fixture();
    let known_prefix = known_ports.capture_original_prefix_retention().unwrap();
    join_actual_cold_locator_prerequisite(&ports);
    let budget = ports.shared.database.source_working_budget();
    let before = budget.observation().unwrap();
    assert!(
        matches!(ports.capture_original_prefix_retention(), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), before);
    let context = OriginalSourceReadContext::admit(&ports).unwrap();
    let captured = context.begin_existing_native_captured_read().unwrap();
    assert!(captured.native_existing_composite_for_test());
    let admitted = budget.observation().unwrap();
    assert!(
        matches!(captured.open_native_checkpoint_read(&known_prefix), Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), admitted);
    drop(captured);
    drop(context);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.reserved_bytes
        - before.owned_bytes;
    let pressure = budget.reserve(available).unwrap();
    let full = budget.observation().unwrap();
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
    assert_eq!(budget.observation().unwrap(), full);
    drop(pressure);
}

// req: PRJ-004, STO-020, REC-001, REC-005
#[cfg(unix)]
#[test]
fn actual_primary_context_forgotten_children_retain_same_base_engine_after_outer_ports_close() {
    for kind in [0_u8, 1, 2, 3] {
        let (root, ports, _) = fixture();
        join_actual_cold_locator_prerequisite(&ports);
        let weak_engine = ports
            .shared
            .database
            .admit_maintenance_weak_backing(&ports.shared.database)
            .unwrap();
        let weak_shared = std::sync::Arc::downgrade(&ports.shared);
        let budget = ports.shared.database.source_working_budget().clone();
        let prefix = ports.capture_original_prefix_retention().unwrap();
        let context = OriginalSourceReadContext::admit(&ports).unwrap();
        let captured = context.begin_existing_native_captured_read().unwrap();
        assert!(captured.native_existing_composite_for_test());
        let before = budget.observation().unwrap();
        let mut read = captured.open_native_checkpoint_read(&prefix).unwrap();
        let admitted = budget.observation().unwrap();
        let numeric = admitted.reserved_bytes - before.reserved_bytes;
        assert!(numeric > 0);
        if kind == 0 {
            std::mem::forget(read);
        } else {
            {
                let mut table = read.open_bytes_table("catalog_active").unwrap();
                if kind == 1 {
                    let value = table
                        .get(crate::layout::CATALOG_ACTIVE_KEY.as_slice())
                        .unwrap()
                        .unwrap();
                    assert!(!value.value().is_empty());
                    std::mem::forget(value);
                    assert!(
                        matches!(table.range_from(None), Err(error) if error.kind()==StorageErrorKind::Unavailable)
                    );
                } else {
                    let mut range = table.range_from(None).unwrap();
                    let row = range.next().unwrap().unwrap();
                    assert_eq!(row.key(), crate::layout::CATALOG_ACTIVE_KEY);
                    if kind == 2 {
                        drop(row);
                        std::mem::forget(range);
                    } else {
                        std::mem::forget(row);
                        assert!(
                            matches!(range.next(), Err(error) if error.kind()==StorageErrorKind::Unavailable)
                        );
                    }
                }
            }
            drop(read);
        }
        assert_eq!(budget.observation().unwrap(), admitted);
        drop(captured);
        drop(context);
        drop(prefix);
        assert!(budget.observation().unwrap().reserved_bytes >= numeric);
        assert!(budget.try_acquire_worker().is_ok());
        assert!(
            ports
                .complete_graceful_close()
                .disposition()
                .is_successful()
        );
        drop(ports);
        assert!(
            weak_shared.upgrade().is_none(),
            "base custody retains data, not the outer ports owner"
        );
        assert!(
            weak_engine.upgrade().is_some(),
            "forgotten child retains this actual base engine"
        );
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join("db.redb"))
            .unwrap();
        assert!(
            file.try_lock().is_err(),
            "actual original base backend must remain open"
        );
        eprintln!(
            "actual_primary_forgotten_child_kind={kind} retains_base_engine_and_numeric_charge"
        );
    }
}

// req: PRJ-004, STO-020, REC-001, REC-005
#[cfg(unix)]
#[test]
fn actual_primary_context_normal_drop_releases_new_charge_and_final_base_engine() {
    let (root, ports, _) = fixture();
    join_actual_cold_locator_prerequisite(&ports);
    let weak_engine = ports
        .shared
        .database
        .admit_maintenance_weak_backing(&ports.shared.database)
        .unwrap();
    let budget = ports.shared.database.source_working_budget().clone();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let context = OriginalSourceReadContext::admit(&ports).unwrap();
    let captured = context.begin_existing_native_captured_read().unwrap();
    let before = budget.observation().unwrap();
    {
        let mut read = captured.open_native_checkpoint_read(&prefix).unwrap();
        let mut meta = read.open_str_table("meta").unwrap();
        let value = meta.get(crate::layout::META_DATABASE_ID).unwrap().unwrap();
        assert!(!value.value().is_empty());
    }
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    drop(captured);
    drop(context);
    drop(prefix);
    assert!(
        ports
            .complete_graceful_close()
            .disposition()
            .is_successful()
    );
    drop(ports);
    assert!(
        weak_engine.upgrade().is_none(),
        "normal source custody must not retain its actual base after close"
    );
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("db.redb"))
        .unwrap();
    file.try_lock().unwrap();
    file.unlock().unwrap();
}
