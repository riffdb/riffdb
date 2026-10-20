// req: PRJ-004, STO-012, REC-001
use super::*;
use riffdb_storage_api::LeadershipEpochV1;
use riffdb_types::DatabaseId;

fn lineage() -> ChangelogLineageV3 {
    ChangelogLineageV3::new(
        DatabaseId::from_unix_milliseconds_and_random(42, [7; 10]).unwrap(),
        1,
        LeadershipEpochV1::initial(),
    )
    .unwrap()
}
fn pins() -> SourcePrefixPins {
    SourcePrefixPins::new(&ProjectionSourceWorkingBudgetV1::new()).unwrap()
}
fn through(value: u64) -> CommitSequence {
    CommitSequence::new(value).unwrap()
}
fn remaining(budget: &ProjectionSourceWorkingBudgetV1) -> u64 {
    let usage = budget.observation().unwrap();
    riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - usage.fixed_owner_bytes
        - usage.reserved_bytes
        - usage.owned_bytes
}

#[test]
fn prefix_last_reference_protects_base_and_tail_until_release() {
    let pins = pins();
    let provider = ProviderBudget::new();
    let needed = pins.provider_ownership_needed(&provider).unwrap();
    let mut work = provider.reserve(needed + 1).unwrap();
    pins.prepare_provider_ownership_from(&mut work, &provider)
        .unwrap()
        .commit();
    drop(work);
    let charged = pins.provider_charge_bytes();
    assert!(charged > 0);
    let pin = pins.try_register(lineage(), 4, through(9)).unwrap();
    let alias = pin.clone();
    assert_eq!(pins.application_floor(lineage()).unwrap(), Some(4));
    assert!(pins.prepare_prune(lineage(), 5).is_err());
    drop(pin);
    assert_eq!(pins.application_floor(lineage()).unwrap(), Some(4));
    drop(alias);
    assert_eq!(pins.application_floor(lineage()).unwrap(), None);
    assert!(pins.prepare_prune(lineage(), 5).is_ok());
    let last_pin = pins.try_register(lineage(), 4, through(9)).unwrap();
    drop(pins);
    assert_eq!(provider.observation().unwrap().owned_bytes, charged);
    drop(last_pin);
    assert_eq!(provider.observation().unwrap().owned_bytes, 0);
}

#[test]
fn submitted_prune_reservation_prevents_a_late_capture_under_two_safe_orders() {
    let pins = pins();
    let pin = pins.try_register(lineage(), 0, through(9)).unwrap();
    assert!(pins.prepare_prune(lineage(), 1).is_err());
    drop(pin);
    let mut reservation = pins.prepare_prune(lineage(), 1).unwrap();
    let guard = pins.state.lock().unwrap();
    let (entered, observed) = std::sync::mpsc::sync_channel(0);
    let worker_pins = pins.clone();
    let worker = std::thread::spawn(move || {
        assert!(worker_pins.state.try_lock().is_err());
        entered.send(()).unwrap();
        worker_pins.try_register(lineage(), 0, through(9))
    });
    observed.recv().unwrap();
    reservation.submission_started();
    drop(guard);
    assert!(matches!(worker.join().unwrap(), Err(error)
        if error.kind() == StorageErrorKind::HistoryPruned));
    drop(reservation);
    assert!(
        matches!(pins.try_register(lineage(), 0, through(9)), Err(error)
        if error.kind() == StorageErrorKind::HistoryPruned)
    );
    assert!(pins.try_register(lineage(), 1, through(9)).is_ok());
}

#[test]
fn known_presubmit_abort_releases_reservation_but_unknown_submission_retains_it() {
    let pins = pins();
    drop(pins.prepare_prune(lineage(), 2).unwrap());
    assert!(pins.try_register(lineage(), 0, through(9)).is_ok());
    let mut pending = pins.prepare_prune(lineage(), 2).unwrap();
    pending.submission_started();
    drop(pending);
    assert!(pins.try_register(lineage(), 0, through(9)).is_err());
}

#[test]
fn source_work_ledger_refuses_before_domain_or_lease_allocation() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let allowance = budget.reserve(remaining(&budget)).unwrap();
    assert!(SourcePrefixPins::new(&budget).is_err());
    drop(allowance);
    let pins = SourcePrefixPins::new(&budget).unwrap();
    let allowance = budget.reserve(remaining(&budget)).unwrap();
    assert!(pins.try_register(lineage(), 0, through(9)).is_err());
    assert_eq!(pins.application_floor(lineage()).unwrap(), None);
    drop(allowance);
    let pin = pins.try_register(lineage(), 0, through(9)).unwrap();
    assert!(pins.try_register(lineage(), 0, through(10)).is_err());
    assert_eq!(pin.captured_through(), through(9));
}
