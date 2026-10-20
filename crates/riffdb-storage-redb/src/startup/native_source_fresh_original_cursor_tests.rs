//! Original cursor controls on actual Fresh current storage and compiled runtime commands.
use super::*;
use crate::store::OriginalSourceCursor;
use riffdb_storage_api::{
    AuthoritativeScanReader, CommitScanRequest, OwnedSnapshotReader, StorageScanLimit,
};

fn fresh_runtime_fixture() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
) {
    let root = crate::test_path::ScopedDirectory::new("fresh-runtime-source-cursor");
    let mut store = RedbStore::open(root.join("db.redb")).unwrap();
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
        panic!("actual same-owner Fresh must validate before readiness");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    super::native_source_command_fixture::activate_source_projection(&mut ports).unwrap();
    let first = super::native_source_command_fixture::append_runtime_immediate(&ports, 1).unwrap();
    let second = super::native_source_command_fixture::append_runtime_immediate(&ports, 2).unwrap();
    assert_eq!(first.commit_sequence().get(), 1);
    assert_eq!(second.commit_sequence().get(), 2);
    (root, ports)
}

fn ordinary_commits(ports: &crate::RedbOperationalPorts, count: u16) -> Vec<StoredCommitRecordV1> {
    let snapshot = ports.open_owned_snapshot().unwrap();
    let page = snapshot
        .scan_commits(CommitScanRequest::initial(
            StorageScanLimit::new(count).unwrap(),
        ))
        .unwrap();
    assert_eq!(page.records().len(), usize::from(count));
    page.records()
        .iter()
        .map(|row| row.value().clone())
        .collect()
}
fn assert_live_gauges_equal(
    a: riffdb_types::exact_provider_budget_v1::BudgetObservation,
    b: riffdb_types::exact_provider_budget_v1::BudgetObservation,
) {
    assert_eq!(
        (
            a.fixed_owner_bytes,
            a.reserved_bytes,
            a.owned_bytes,
            a.reservations,
            a.allocations,
            a.active_owners,
            a.retiring_owners,
            a.pinned_owners,
            a.pins,
        ),
        (
            b.fixed_owner_bytes,
            b.reserved_bytes,
            b.owned_bytes,
            b.reservations,
            b.allocations,
            b.active_owners,
            b.retiring_owners,
            b.pinned_owners,
            b.pins,
        ),
    );
    assert!(
        a.released_allocations > b.released_allocations && a.released_owners >= b.released_owners
    );
}
// req: PRJ-004, STO-012, STO-020, REC-001
#[test]
fn fresh_runtime_original_cursor_admits_before_decode_retains_exact_graph_and_releases_last_charge()
{
    let (_directory, ports) = fresh_runtime_fixture();
    let expected = ordinary_commits(&ports, 2);
    let budget = ports.shared.database.source_working_budget().clone();
    let baseline = budget.observation().unwrap();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let mut cursor = OriginalSourceCursor::open(&ports, &prefix).unwrap();
    let held = budget.observation().unwrap();
    let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - held.fixed_owner_bytes
        - held.owned_bytes
        - held.reserved_bytes;
    let pressure = budget.reserve(free - (512 * 1024 + 8 * 1024)).unwrap();
    let blocked = budget.observation().unwrap();
    assert!(
        matches!(cursor.next_commit(),Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), blocked);
    assert_eq!(
        ordinary_commits(&ports, 2),
        expected,
        "ordinary scans remain Source64 independent"
    );
    drop(pressure);
    for wanted in &expected {
        let commit = cursor.next_commit().unwrap().unwrap();
        assert_eq!(
            commit, wanted,
            "complete canonical command and event reconstruction"
        );
    }
    assert!(
        budget.observation().unwrap().reserved_bytes > held.reserved_bytes,
        "returned borrow retains complete admitted graph"
    );
    assert!(cursor.next_commit().unwrap().is_none());
    assert_eq!(
        budget.observation().unwrap(),
        held,
        "EOF releases complete graph before cursor drop"
    );
    drop(cursor);
    assert!(
        budget.try_acquire_worker().is_err(),
        "prefix retains pruning/worker custody after cursor drop"
    );
    drop(prefix);
    assert_live_gauges_equal(budget.observation().unwrap(), baseline);
    assert!(budget.try_acquire_worker().is_ok());
}
// req: PRJ-004, STO-012, REC-001
#[test]
fn fresh_runtime_original_cursor_refuses_equal_lineage_foreign_engine_before_admission() {
    let (_directory, ports) = fresh_runtime_fixture();
    let (_other_directory, other) = fresh_runtime_fixture();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let other_prefix = other.capture_original_prefix_retention().unwrap();
    assert_eq!(
        prefix.captured_history().lineage(),
        other_prefix.captured_history().lineage()
    );
    let original_before = prefix.originating_budget().observation().unwrap();
    let foreign_before = other_prefix.originating_budget().observation().unwrap();
    assert!(
        matches!(OriginalSourceCursor::open(&other,&prefix),Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(
        prefix.originating_budget().observation().unwrap(),
        original_before
    );
    assert_eq!(
        other_prefix.originating_budget().observation().unwrap(),
        foreign_before
    );
    let alias = crate::RedbOperationalPorts {
        shared: Arc::clone(&ports.shared),
    };
    let mut cursor = OriginalSourceCursor::open(&alias, &prefix).unwrap();
    assert_eq!(
        cursor
            .next_commit()
            .unwrap()
            .unwrap()
            .commit_sequence()
            .get(),
        1
    );
}
// req: PRJ-004, STO-012, REC-001, LOG-001
#[test]
fn fresh_runtime_original_cursor_keeps_captured_frontier_across_real_compiled_append() {
    let (_directory, ports) = fresh_runtime_fixture();
    let expected = ordinary_commits(&ports, 2);
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let captured = prefix.captured_history();
    super::native_source_command_fixture::append_runtime_immediate(&ports, 3).unwrap();
    let current =
        crate::changelog_v3_cursor::history(&ports.begin_composite_read().unwrap()).unwrap();
    assert_eq!(current.tail().frontier().application().unwrap().get(), 3);
    assert!(current.tail().sequence() > captured.tail().sequence());
    let mut cursor = OriginalSourceCursor::open(&ports, &prefix).unwrap();
    for wanted in &expected {
        assert_eq!(cursor.next_commit().unwrap().unwrap(), wanted);
    }
    assert!(cursor.next_commit().unwrap().is_none());
    assert_eq!(
        prefix.captured_history(),
        captured,
        "later head cannot replace captured authority"
    );
    assert_eq!(ordinary_commits(&ports, 3).len(), 3);
}
// req: PRJ-004, STO-012, REC-001, LOG-001
#[test]
fn fresh_runtime_original_cursor_survives_real_extent_rotation_and_fences_reclamation() {
    let (directory, mut ports) = fresh_runtime_fixture();
    super::native_source_admission::complete_first_event(&mut ports);
    super::native_source_command_fixture::append_source_projection_journaled(&ports, 3).unwrap();
    assert_eq!(
        ports.shared.journal_suffix_activity().unwrap().1,
        1,
        "real deferred epoch must publish one acknowledged journaled command"
    );
    let expected = ordinary_commits(&ports, 3);
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let captured = prefix.captured_history();
    assert_eq!(prefix.captured_through().get(), 3);
    let database = directory.join("db.redb");
    let active = crate::journal::journal_path(&database);
    let generation =
        crate::journal::extent_generation_with_media(&crate::media::RealJournalMedia, &active)
            .unwrap()
            .unwrap();
    let sealed = crate::journal::sealed_journal_path(&database, generation);
    // Precondition: an actual acknowledged command is a retained active-frame
    // stub in the same Composite view, not a fabricated placement or callback.
    let access = ports.begin_composite_read().unwrap();
    assert!(matches!(
        &access,
        crate::store::RedbReadAccess::Composite(_)
    ));
    drop(access);
    let alias = crate::RedbOperationalPorts {
        shared: Arc::clone(&ports.shared),
    };
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let reader = std::thread::spawn(move || {
        assert!(crate::journal::install_source_frame_pause_for_test(
            active, entered_tx, release_rx
        ));
        let mut cursor = OriginalSourceCursor::open(&alias, &prefix).unwrap();
        for wanted in &expected {
            assert_eq!(cursor.next_commit().unwrap().unwrap(), wanted);
        }
        assert!(cursor.next_commit().unwrap().is_none());
        drop(cursor);
        prefix
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    // The paused reader has not opened the active file yet. The real existing
    // checkpoint/sealer moves that generation past the first sealed lookup.
    ports.checkpoint_original_cursor_fixture_for_test().unwrap();
    assert!(
        sealed.exists(),
        "real off-gate sealer must publish the captured generation"
    );
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::MIN, NonZeroU64::MIN)
            .unwrap(),
        crate::OnlineRetentionStep::Held(crate::OnlineRetentionHold::Fenced(_))
    ));
    assert!(
        !ports
            .replication_source_control()
            .reclaim_history()
            .unwrap()
    );
    let reclaim = ports.shared.reclaim_one_extent().unwrap();
    assert_eq!((reclaim.deleted, reclaim.recycled), (0, 0));
    assert!(
        sealed.exists(),
        "all physical retained command sources remain protected"
    );
    release_tx.send(()).unwrap();
    let prefix = reader.join().unwrap();
    assert_eq!(prefix.captured_history(), captured);
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::MIN, NonZeroU64::MIN)
            .unwrap(),
        crate::OnlineRetentionStep::Held(crate::OnlineRetentionHold::Fenced(_))
    ));
    drop(prefix);
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::MIN, NonZeroU64::MIN)
            .unwrap(),
        crate::OnlineRetentionStep::Advanced {
            through: 1,
            sequences: 1
        }
    ));
}
