//! Same actual Fresh owner; no caller-supplied Next, catalog or origin authority.
use super::*;
use riffdb_storage_api::{AuthoritativePointReader, DeferredCommandEpochPort};

fn fixture() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
    riffdb_storage_api::ActiveCatalogPointerV1,
) {
    let root = crate::test_path::ScopedDirectory::new("source-v3-capture");
    let mut store = RedbStore::open(root.join("db.redb")).unwrap();
    let id =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    store.initialize_projection_source_fixture(id).unwrap();
    // Mandatory existing full startup on this same owner, before any command.
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("genuine same-owner Fresh must validate before readiness");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    let active = super::native_source_command_fixture::activate(&mut ports).unwrap();
    super::native_source_command_fixture::append_journaled(&ports, 1).unwrap();
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 1);
    (root, ports, active)
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
            a.pins
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
            b.pins
        )
    );
    assert!(
        a.released_allocations >= b.released_allocations && a.released_owners >= b.released_owners
    );
}

// req: PRJ-004, STO-012, STO-020, REC-001, LOG-001
#[test]
fn v3_original_capture_retains_exact_origin_next_pointer_and_frontier_under_append() {
    let (_root, ports, active) = fixture();
    let expected = ports.read_commit(CommitSequence::first()).unwrap().unwrap();
    let budget = ports.shared.database.source_working_budget().clone();
    let before = budget.observation().unwrap();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let history = prefix.captured_history();
    assert_eq!(
        history.lineage().catalog_digest(),
        riffdb_storage_api::AuthoritativeStateCatalogV3.digest()
    );
    assert_eq!(prefix.captured_through(), CommitSequence::first());
    let locator = prefix.tentative_source_locator().unwrap();
    assert_eq!(locator.database_id(), history.lineage().database_id());
    assert_eq!(
        locator.allocation_history_incarnation(),
        history.lineage().history_incarnation()
    );
    assert_eq!(
        locator.ordinal(),
        riffdb_types::ProjectionCheckpointOrdinal::first()
    );
    assert_eq!(prefix.captured_active_catalog_pointer(), Some(&active));
    let origin = prefix.captured_native_origin().unwrap();
    assert_eq!(
        origin.administration_sequence(),
        riffdb_types::AdministrationSequence::first()
    );
    assert!(
        budget.observation().unwrap().owned_bytes > before.owned_bytes,
        "retained pointer is Source-owned until capture destruction"
    );
    super::native_source_command_fixture::append_journaled(&ports, 2).unwrap();
    let current =
        crate::changelog_v3_cursor::history(&ports.begin_composite_read().unwrap()).unwrap();
    assert_eq!(current.tail().frontier().application().unwrap().get(), 2);
    let mut cursor = crate::store::OriginalSourceCursor::open(&ports, &prefix).unwrap();
    assert_eq!(cursor.next_commit().unwrap().unwrap(), &expected);
    assert!(cursor.next_commit().unwrap().is_none());
    drop(cursor);
    assert_eq!(prefix.captured_history(), history);
    assert_eq!(prefix.tentative_source_locator(), Some(locator));
    assert_eq!(prefix.captured_native_origin(), Some(origin));
    assert_eq!(prefix.captured_active_catalog_pointer(), Some(&active));
    drop(prefix);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    assert!(budget.try_acquire_worker().is_ok());
}

// req: PRJ-004, STO-012, REC-001
#[test]
fn v3_capture_pressure_refuses_before_pointer_graph_and_releases_worker_without_blocking_commands()
{
    let (_root, ports, _active) = fixture();
    let budget = ports.shared.database.source_working_budget().clone();
    let before = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.reserved_bytes
        - before.owned_bytes;
    let pressure = budget.reserve(remaining).unwrap();
    let saturated = budget.observation().unwrap();
    assert!(
        matches!(ports.capture_original_prefix_retention(),Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), saturated);
    assert!(budget.try_acquire_worker().is_ok());
    // Real ordinary command path remains independent of Source64 pressure.
    super::native_source_command_fixture::append_journaled(&ports, 2).unwrap();
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
    drop(pressure);
    assert_eq!(budget.observation().unwrap(), before);
}

// req: PRJ-004, STO-012, STO-020, REC-001, LOG-001
#[test]
fn cold_new_engine_original_capture_reconstructs_exact_prefix_and_keeps_writes_quota_independent() {
    let (root, ports, active) = fixture();
    let expected_first = ports.read_commit(CommitSequence::first()).unwrap().unwrap();
    let old_owner = std::sync::Arc::downgrade(&ports.shared);
    let close = ports.complete_graceful_close();
    assert!(
        close.disposition().is_successful(),
        "actual graceful close must finish before dropping every native owner"
    );
    drop(ports);
    assert!(
        old_owner.upgrade().is_none(),
        "no original SharedRedb or hot origin context remains"
    );

    // The new actual engine must earn its origin through the complete ordinary
    // startup owner. No caller-made context, origin proof or Next is supplied.
    let store = RedbStore::open(root.join("db.redb")).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("genuine cold Fresh must pass complete normal startup");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant.into_operational_after_catalog_validation().unwrap();
    let budget = ports.shared.database.source_working_budget().clone();
    let before = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.reserved_bytes
        - before.owned_bytes;
    let pressure = budget.reserve(remaining).unwrap();
    let saturated = budget.observation().unwrap();
    assert!(
        matches!(ports.capture_original_prefix_retention(), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), saturated);
    assert!(budget.try_acquire_worker().is_ok());
    // ADR0319 permits a cold pending directory after bounded readiness. Join
    // its actual construction before testing the independent Source quota;
    // pressure remains saturated and no Source read warms the locator owner.
    join_actual_cold_locator_prerequisite(&ports);
    assert_eq!(budget.observation().unwrap(), saturated);
    super::native_source_command_fixture::append_journaled(&ports, 2).unwrap();
    let expected_second = ports
        .read_commit(CommitSequence::new(2).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        budget.observation().unwrap(),
        saturated,
        "ordinary compiled command must not borrow Source capacity"
    );
    drop(pressure);
    assert_live_gauges_equal(budget.observation().unwrap(), before);

    let prefix = ports.capture_original_prefix_retention().unwrap();
    let history = prefix.captured_history();
    assert_eq!(
        history.lineage().catalog_digest(),
        riffdb_storage_api::AuthoritativeStateCatalogV3.digest()
    );
    assert_eq!(prefix.captured_through().get(), 2);
    assert_eq!(prefix.captured_active_catalog_pointer(), Some(&active));
    let origin = prefix.captured_native_origin().unwrap();
    assert_eq!(
        origin.administration_sequence(),
        riffdb_types::AdministrationSequence::first()
    );
    let locator = prefix.tentative_source_locator().unwrap();
    assert_eq!(locator.database_id(), history.lineage().database_id());
    assert_eq!(
        locator.allocation_history_incarnation(),
        history.lineage().history_incarnation()
    );
    assert_eq!(
        locator.ordinal(),
        riffdb_types::ProjectionCheckpointOrdinal::first()
    );
    super::native_source_command_fixture::append_journaled(&ports, 3).unwrap();
    let mut cursor = crate::store::OriginalSourceCursor::open(&ports, &prefix).unwrap();
    assert_eq!(cursor.next_commit().unwrap().unwrap(), &expected_first);
    assert_eq!(cursor.next_commit().unwrap().unwrap(), &expected_second);
    assert!(
        cursor.next_commit().unwrap().is_none(),
        "captured F excludes the later acknowledged command"
    );
    drop(cursor);
    assert_eq!(prefix.captured_history(), history);
    assert_eq!(prefix.captured_native_origin(), Some(origin));
    assert_eq!(prefix.tentative_source_locator(), Some(locator));
    drop(prefix);
    assert_live_gauges_equal(budget.observation().unwrap(), before);
    assert!(budget.try_acquire_worker().is_ok());
}

fn join_actual_cold_locator_prerequisite(ports: &crate::RedbOperationalPorts) {
    let access = ports.begin_composite_read().unwrap();
    crate::locator_builder::start_locator_builder(
        access.clone(),
        Some(std::sync::Arc::clone(&ports.shared)),
    )
    .unwrap();
    access
        .locator_capability()
        .wait_for_builder_for_test()
        .unwrap();
    access.locator_capability().require_complete().unwrap();
}

// req: PRJ-004, STO-012, STO-020, REC-001, LOG-001
#[cfg(feature = "test-fixtures")]
#[test]
fn cold_locator_pending_refusal_and_completion_are_independent_of_source_quota() {
    use crate::hooks::{RedbCheckpointTestPoint, RedbCheckpointTestSchedule};
    struct Release(Option<RedbCheckpointTestSchedule>);
    impl Drop for Release {
        fn drop(&mut self) {
            drop(self.0.take());
        }
    }
    for saturate_source in [false, true] {
        let (root, old_ports, _) = fixture();
        let old_owner = std::sync::Arc::downgrade(&old_ports.shared);
        assert!(
            old_ports
                .complete_graceful_close()
                .disposition()
                .is_successful()
        );
        drop(old_ports);
        assert!(old_owner.upgrade().is_none());
        let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
        let release = Release(Some(schedule));
        release
            .0
            .as_ref()
            .unwrap()
            .arm(RedbCheckpointTestPoint::BeforeLocatorPublicationLease);
        let store = RedbStore::open_with_test_controller(root.join("db.redb"), controller).unwrap();
        let mut session = store.begin_structural_evidence(inputs()).unwrap();
        let structural = finish_structural(&mut session);
        let (_, historical) = session
            .validate_startup_catalog_history()
            .unwrap()
            .into_parts();
        let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
        else {
            panic!("actual cold startup must retain its bounded readiness rules");
        };
        let (_, _, _, dormant) = opened.into_parts();
        let ports = dormant.into_operational_after_catalog_validation().unwrap();
        let budget = ports.shared.database.source_working_budget().clone();
        let before = budget.observation().unwrap();
        let pressure = if saturate_source {
            Some(
                budget
                    .reserve(
                        riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                            - before.fixed_owner_bytes
                            - before.owned_bytes
                            - before.reserved_bytes,
                    )
                    .unwrap(),
            )
        } else {
            None
        };
        let held = budget.observation().unwrap();
        let access = ports.begin_composite_read().unwrap();
        let capability = access.locator_capability().clone();
        let history = crate::changelog_v3_cursor::capture_history_position_v1(&access).unwrap();
        crate::locator_builder::start_locator_builder(
            access.clone(),
            Some(std::sync::Arc::clone(&ports.shared)),
        )
        .unwrap();
        release
            .0
            .as_ref()
            .unwrap()
            .wait(RedbCheckpointTestPoint::BeforeLocatorPublicationLease);
        assert!(
            capability.is_pending(),
            "real locator publication remains paused"
        );
        assert_eq!(
            ports.begin_deferred_command_epoch().err().unwrap().kind(),
            StorageErrorKind::Unavailable,
            "ADR0319 pending mutation refusal must occur with and without Source pressure"
        );
        assert_eq!(
            crate::changelog_v3_cursor::capture_history_position_v1(&access).unwrap(),
            history
        );
        assert_live_gauges_equal(budget.observation().unwrap(), held);
        release
            .0
            .as_ref()
            .unwrap()
            .release(RedbCheckpointTestPoint::BeforeLocatorPublicationLease);
        capability.wait_for_builder_for_test().unwrap();
        capability.require_complete().unwrap();
        assert_live_gauges_equal(budget.observation().unwrap(), held);
        super::native_source_command_fixture::append_journaled(&ports, 2).unwrap();
        assert_eq!(
            ports
                .read_commit(CommitSequence::new(2).unwrap())
                .unwrap()
                .unwrap()
                .commit_sequence()
                .get(),
            2
        );
        assert_live_gauges_equal(budget.observation().unwrap(), held);
        if saturate_source {
            assert_eq!(
                held.fixed_owner_bytes + held.owned_bytes + held.reserved_bytes,
                riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            );
        }
        drop(pressure);
        assert_live_gauges_equal(budget.observation().unwrap(), before);
    }
}

#[path = "source_original_control_tests.rs"]
mod original_control;

#[path = "source_native_read_tests.rs"]
mod native_read_resources;

#[path = "source_primary_native_read_tests.rs"]
mod primary_read_resources;

#[path = "source_first_lease_native_tests.rs"]
mod first_lease_native;
