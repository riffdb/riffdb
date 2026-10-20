//! Actual cold Source entry: no ordinary root warmup or substituted input.
use super::*;

fn complete_reopen(root: &crate::test_path::ScopedDirectory) -> crate::RedbOperationalPorts {
    let store = RedbStore::open(root.join("db.redb")).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("actual cold Source fixture must complete normal startup");
    };
    let (_, _, _, dormant) = opened.into_parts();
    dormant.into_operational_after_catalog_validation().unwrap()
}

// req: PRJ-004, STO-020, REC-001, LOG-001
#[test]
fn original_first_entry_uses_native_admission_without_shared_table_cache_growth_and_preserves_exact_f1()
 {
    let (root, old, active) = fixture();
    let expected = old.read_commit(CommitSequence::first()).unwrap().unwrap();
    let old_shared = std::sync::Arc::downgrade(&old.shared);
    assert!(old.complete_graceful_close().disposition().is_successful());
    drop(old);
    assert!(old_shared.upgrade().is_none());
    let ports = complete_reopen(&root);
    // Normal startup may legitimately publish an ordinary cached Current.
    // Owning observations open no table and alter none of that actual baseline.
    let initial_publications = ports.shared.source_read_publications_for_test().unwrap();
    let initial_current = ports
        .shared
        .source_current_required_journal_cache_for_test()
        .unwrap()
        .expect("this actual normal-reopen fixture publishes an ordinary Current baseline");
    eprintln!(
        "actual_startup_required_journal_cache commits={} events={}",
        initial_current.2, initial_current.3
    );
    assert!(
        !initial_current.2 || !initial_current.3,
        "genuine fixture must leave at least one cursor-required table uncached before Source"
    );
    let budget = ports.shared.database.source_working_budget().clone();
    let before = budget.observation().unwrap();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    assert!(prefix.belongs_to_ports(&ports));
    assert_eq!(prefix.captured_through(), CommitSequence::first());
    assert_eq!(prefix.captured_active_catalog_pointer(), Some(&active));
    assert!(prefix.tentative_source_locator().is_some());
    let captured_history = prefix.captured_history();
    assert_eq!(
        ports.shared.source_read_publications_for_test().unwrap(),
        initial_publications
    );
    assert_eq!(
        ports
            .shared
            .source_current_required_journal_cache_for_test()
            .unwrap(),
        Some(initial_current)
    );
    eprintln!("actual_source_first_capture_preserved_real_startup_current");
    {
        let mut cursor = crate::store::OriginalSourceCursor::open(&ports, &prefix).unwrap();
        assert_eq!(cursor.next_commit().unwrap().unwrap(), &expected);
        assert!(cursor.next_commit().unwrap().is_none());
    }
    assert_eq!(
        ports.shared.source_read_publications_for_test().unwrap(),
        initial_publications
    );
    eprintln!("actual_real_original_cursor_f1_complete_before_cache_check");
    let after_cursor = ports
        .shared
        .source_current_required_journal_cache_for_test()
        .unwrap()
        .expect("Source must retain the genuine existing Current baseline");
    eprintln!(
        "actual_source_cursor_required_cache_observation before_commits={} after_commits={} before_events={} after_events={}",
        initial_current.2, after_cursor.2, initial_current.3, after_cursor.3
    );
    assert_eq!(
        after_cursor, initial_current,
        "Source cursor must not populate persistent checkpoint journal-table caches"
    );
    // Normal command readiness runs only AFTER the real first Source entry.
    join_actual_cold_locator_prerequisite(&ports);
    let admitted = budget.observation().unwrap();
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - admitted.fixed_owner_bytes
        - admitted.reserved_bytes
        - admitted.owned_bytes;
    let pressure = budget.reserve(available).unwrap();
    let full = budget.observation().unwrap();
    super::super::native_source_command_fixture::append_journaled(&ports, 2).unwrap();
    assert_eq!(budget.observation().unwrap(), full);
    drop(pressure);
    // Ordinary writes may change their own publication/cache; Source must not.
    let ordinary_cache = ports
        .shared
        .source_current_required_journal_cache_for_test()
        .unwrap();
    {
        let mut cursor = crate::store::OriginalSourceCursor::open(&ports, &prefix).unwrap();
        assert_eq!(cursor.next_commit().unwrap().unwrap(), &expected);
        assert!(cursor.next_commit().unwrap().is_none());
    }
    assert_eq!(
        ports
            .shared
            .source_current_required_journal_cache_for_test()
            .unwrap(),
        ordinary_cache
    );
    assert_eq!(prefix.captured_history(), captured_history);
    assert_eq!(prefix.captured_through(), CommitSequence::first());
    eprintln!("actual_source_original_f1_excludes_real_command2_on_same_engine_ledger");
    drop(prefix);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn original_first_entry_full_source_refusal_preserves_real_startup_cache_and_worker() {
    let (root, old, _) = fixture();
    assert!(old.complete_graceful_close().disposition().is_successful());
    drop(old);
    let ports = complete_reopen(&root);
    let initial = ports.shared.source_read_publications_for_test().unwrap();
    let initial_current = ports
        .shared
        .source_current_required_journal_cache_for_test()
        .unwrap();
    let budget = ports.shared.database.source_working_budget().clone();
    let before = budget.observation().unwrap();
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.reserved_bytes
        - before.owned_bytes;
    let pressure = budget.reserve(available).unwrap();
    let full = budget.observation().unwrap();
    assert!(
        matches!(ports.capture_original_prefix_retention(), Err(error)
        if error.kind() == StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), full);
    assert_eq!(
        ports.shared.source_read_publications_for_test().unwrap(),
        initial
    );
    assert_eq!(
        ports
            .shared
            .source_current_required_journal_cache_for_test()
            .unwrap(),
        initial_current
    );
    assert!(budget.try_acquire_worker().is_ok());
    drop(pressure);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    let prefix = ports.capture_original_prefix_retention().unwrap();
    assert!(prefix.belongs_to_ports(&ports));
    assert_eq!(prefix.captured_through(), CommitSequence::first());
    assert_eq!(
        ports.shared.source_read_publications_for_test().unwrap(),
        initial
    );
    assert_eq!(
        ports
            .shared
            .source_current_required_journal_cache_for_test()
            .unwrap(),
        initial_current
    );
    drop(prefix);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    eprintln!("actual_first_source_full_refusal_preserves_startup_cache_and_releases_worker_pin");
}

// Genuine runtime projection command setup, using existing owning helpers.
fn runtime_source_fixture_for_stale_current() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
    riffdb_storage_api::ActiveCatalogPointerV1,
) {
    let root = crate::test_path::ScopedDirectory::new("source-native-runtime-immediate");
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
        panic!("genuine runtime Source fixture must validate before readiness");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    let active =
        super::super::native_source_command_fixture::activate_source_projection(&mut ports)
            .unwrap();
    super::super::native_source_command_fixture::append_source_projection_journaled(&ports, 1)
        .unwrap();
    (root, ports, active)
}

// req: PRJ-004, STO-020, REC-001, LOG-001
#[test]
fn original_first_entry_preserves_genuine_stale_current_after_runtime_immediate_and_fixed_f2() {
    let (root, old, active) = runtime_source_fixture_for_stale_current();
    let first = old.read_commit(CommitSequence::first()).unwrap().unwrap();
    assert!(old.complete_graceful_close().disposition().is_successful());
    drop(old);
    let ports = complete_reopen(&root);
    // An actual normal command prerequisite, followed by real runtime execution
    // and Immediate publication; no cache manipulation or ordinary read afterward.
    join_actual_cold_locator_prerequisite(&ports);
    let second =
        super::super::native_source_command_fixture::append_runtime_immediate(&ports, 2).unwrap();
    let initial_publications = ports.shared.source_read_publications_for_test().unwrap();
    let initial_current = ports
        .shared
        .source_current_required_journal_cache_for_test()
        .unwrap()
        .expect("the actual ordinary startup Current remains cached");
    let published_epoch = ports.shared.source_publication_epoch_for_test();
    eprintln!(
        "actual_ordinary_immediate_current_baseline stale={}",
        initial_current.0 != published_epoch
    );
    assert_ne!(
        initial_current.0, published_epoch,
        "actual Immediate command must leave the older cached Current for this counterexample"
    );
    let live_roots = ports
        .shared
        .source_live_tracked_root_count_for_test()
        .unwrap();
    eprintln!("actual_pre_source_live_tracked_checkpoint_roots={live_roots}");
    let budget = ports.shared.database.source_working_budget().clone();
    let before = budget.observation().unwrap();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    assert!(prefix.belongs_to_ports(&ports));
    assert_eq!(prefix.captured_through(), CommitSequence::new(2).unwrap());
    assert_eq!(prefix.captured_active_catalog_pointer(), Some(&active));
    assert!(prefix.tentative_source_locator().is_some());
    let captured_history = prefix.captured_history();
    eprintln!("actual_real_original_f2_capture_complete_after_runtime_immediate");
    let after_capture = ports
        .shared
        .source_current_required_journal_cache_for_test()
        .unwrap()
        .expect("Source must retain the actual cached Current baseline");
    eprintln!(
        "actual_source_current_publication_observation changed={} now_matches_published={}",
        after_capture != initial_current,
        after_capture.0 == published_epoch
    );
    assert_eq!(
        after_capture, initial_current,
        "Source first capture must not publish a replacement shared Current"
    );
    assert_eq!(
        ports.shared.source_read_publications_for_test().unwrap(),
        initial_publications
    );
    {
        let mut cursor = crate::store::OriginalSourceCursor::open(&ports, &prefix).unwrap();
        assert_eq!(cursor.next_commit().unwrap().unwrap(), &first);
        assert_eq!(cursor.next_commit().unwrap().unwrap(), &second);
        assert!(cursor.next_commit().unwrap().is_none());
    }
    assert_eq!(
        ports
            .shared
            .source_current_required_journal_cache_for_test()
            .unwrap(),
        Some(initial_current)
    );
    let admitted = budget.observation().unwrap();
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - admitted.fixed_owner_bytes
        - admitted.reserved_bytes
        - admitted.owned_bytes;
    let pressure = budget.reserve(available).unwrap();
    let full = budget.observation().unwrap();
    super::super::native_source_command_fixture::append_runtime_immediate(&ports, 3).unwrap();
    assert_eq!(budget.observation().unwrap(), full);
    drop(pressure);
    let ordinary_current = ports
        .shared
        .source_current_required_journal_cache_for_test()
        .unwrap();
    {
        let mut cursor = crate::store::OriginalSourceCursor::open(&ports, &prefix).unwrap();
        assert_eq!(cursor.next_commit().unwrap().unwrap(), &first);
        assert_eq!(cursor.next_commit().unwrap().unwrap(), &second);
        assert!(cursor.next_commit().unwrap().is_none());
    }
    assert_eq!(
        ports
            .shared
            .source_current_required_journal_cache_for_test()
            .unwrap(),
        ordinary_current
    );
    assert_eq!(prefix.captured_history(), captured_history);
    assert_eq!(prefix.captured_through(), CommitSequence::new(2).unwrap());
    drop(prefix);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    eprintln!("actual_source_f2_excludes_real_runtime_command3_with_ordinary_quota_independence");
}
