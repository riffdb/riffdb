//! Actual compiler/runtime output committed through the native coordinator.
use super::*;
use riffdb_storage_api::AuthoritativePointReader;

// req: PRJ-004, STO-012, STO-020, REC-001, LOG-001
#[test]
fn source_projection_fixture_replays_actual_compiled_runtime_events_and_outcomes() {
    let root = crate::test_path::ScopedDirectory::new("source-runtime-projection");
    let mut store = RedbStore::open(root.join("db.redb")).unwrap();
    let database =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    store
        .initialize_projection_source_fixture(database)
        .unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("genuine Fresh Source fixture must pass normal startup");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    let active = native_source_command_fixture::activate_source_projection(&mut ports).unwrap();
    let bundle = native_source_command_fixture::source_projection_bundle();
    assert_eq!(bundle.bundle().projections().len(), 1);
    assert_eq!(active.bundle_hash(), bundle.bundle_hash());
    native_source_command_fixture::append_source_projection_journaled(&ports, 1).unwrap();
    native_source_command_fixture::append_source_projection_journaled(&ports, 2).unwrap();
    let budget = ports.shared.database.source_working_budget().clone();
    let before = budget.observation().unwrap();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    assert_eq!(prefix.captured_through().get(), 2);
    assert_eq!(prefix.captured_active_catalog_pointer(), Some(&active));
    let mut cursor = crate::store::OriginalSourceCursor::open(&ports, &prefix).unwrap();
    for ordinal in 1..=2 {
        let actual = ports
            .read_commit(CommitSequence::new(ordinal).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(actual.plan().contract_bundle_hash(), bundle.bundle_hash());
        let events = actual.events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].payload().fields(),
            &[
                (FieldId::new(1).unwrap(), CanonicalValue::U64(6 + ordinal)),
                (FieldId::new(2).unwrap(), CanonicalValue::U64(1)),
            ]
        );
        assert_eq!(cursor.next_commit().unwrap().unwrap(), &actual);
    }
    assert!(cursor.next_commit().unwrap().is_none());
    drop(cursor);
    drop(prefix);
    let after = budget.observation().unwrap();
    assert_eq!(after.reserved_bytes, before.reserved_bytes);
    assert_eq!(after.owned_bytes, before.owned_bytes);
    assert!(budget.try_acquire_worker().is_ok());
}
