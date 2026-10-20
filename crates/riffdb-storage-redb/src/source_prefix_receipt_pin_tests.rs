//! Deterministic registration/submission schedules for the compact receipt pin.
use super::*;
use riffdb_storage_api::{ChangelogTransactionSequence, LeadershipEpochV1};
use riffdb_types::{DatabaseId, DualFrontier};
fn history(first: u64, tail: u64) -> ChangelogHistoryStateV3 {
    let point = |number| {
        ChangelogHistoryPointV3::new(
            ChangelogTransactionSequence::new(number).unwrap(),
            [number as u8; 32],
            DualFrontier::new(Some(CommitSequence::new(number).unwrap()), None),
        )
    };
    ChangelogHistoryStateV3::new(
        ChangelogLineageV3::new(
            DatabaseId::from_unix_milliseconds_and_random(42, [7; 10]).unwrap(),
            1,
            LeadershipEpochV1::initial(),
        )
        .unwrap(),
        point(1),
        point(tail),
        point(first),
    )
    .unwrap()
}
fn pins() -> SourcePrefixPins {
    SourcePrefixPins::new(&ProjectionSourceWorkingBudgetV1::new()).unwrap()
}
// req: PRJ-004, STO-012, REC-001
#[test]
fn original_last_pin_keeps_the_entire_captured_retained_receipt_chain() {
    let pins = pins();
    let history = history(2, 9);
    let pin = pins.try_register_captured(history, 0).unwrap();
    let alias = pin.clone();
    assert_eq!(pins.receipt_floor(history.lineage()).unwrap(), Some(2));
    assert!(
        pins.prepare_receipt_prune(history.lineage(), history.tail())
            .is_err()
    );
    assert!(
        pins.prepare_receipt_prune(history.lineage(), history.minimum_resume())
            .is_ok()
    );
    drop(pin);
    assert_eq!(pins.receipt_floor(history.lineage()).unwrap(), Some(2));
    drop(alias);
    assert_eq!(pins.receipt_floor(history.lineage()).unwrap(), None);
    assert!(
        pins.prepare_receipt_prune(history.lineage(), history.tail())
            .is_ok()
    );
}
// req: PRJ-004, STO-012, REC-001
#[test]
fn receipt_pending_or_unknown_cut_refuses_late_original_capture_before_registration() {
    let pins = pins();
    let old = history(2, 9);
    let newer = history(9, 10);
    let pin = pins.try_register_captured(old, 0).unwrap();
    assert!(
        pins.prepare_receipt_prune(old.lineage(), old.tail())
            .is_err()
    );
    drop(pin);
    let mut reservation = pins
        .prepare_receipt_prune(old.lineage(), old.tail())
        .unwrap();
    let guard = pins.state.lock().unwrap();
    let (sent, received) = std::sync::mpsc::sync_channel(0);
    let other = pins.clone();
    let thread = std::thread::spawn(move || {
        assert!(other.state.try_lock().is_err());
        sent.send(()).unwrap();
        other.try_register_captured(old, 0)
    });
    received.recv().unwrap();
    reservation.submission_started();
    drop(guard);
    assert!(
        matches!(thread.join().unwrap(),Err(error) if error.kind()==StorageErrorKind::HistoryPruned)
    );
    drop(reservation);
    assert!(pins.try_register_captured(old, 0).is_err());
    assert!(pins.try_register_captured(newer, 0).is_ok());
}
// req: PRJ-004, STO-012, REC-001
#[test]
fn receipt_known_presubmit_abort_restores_prior_floor_and_unrelated_tail_does_not_invalidate_pin() {
    let pins = pins();
    let original = history(2, 9);
    let appended = history(2, 10);
    drop(
        pins.prepare_receipt_prune(original.lineage(), original.tail())
            .unwrap(),
    );
    let pin = pins.try_register_captured(original, 0).unwrap();
    assert_eq!(pin.captured_through(), CommitSequence::new(9).unwrap());
    assert_eq!(pins.receipt_floor(appended.lineage()).unwrap(), Some(2));
    assert!(
        pins.prepare_receipt_prune(appended.lineage(), appended.tail())
            .is_err()
    );
    drop(pin);
    assert!(
        pins.prepare_receipt_prune(appended.lineage(), appended.tail())
            .is_ok()
    );
}
