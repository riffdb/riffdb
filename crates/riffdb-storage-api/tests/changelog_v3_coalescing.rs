#![forbid(unsafe_code)]
//! Net receipt transitions preserve sequential expected-state checks.
// req: REP-003, REC-001

use riffdb_storage_api::{
    AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3 as M,
    AuthoritativeNamespaceV1 as N, ChangelogV3Error,
};
use sha2::{Digest, Sha256};

fn hash(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

#[test]
fn journal_order_collapses_to_one_exact_net_transition_per_key() {
    let mut changes = AuthoritativeMutationAccumulatorV3::default();
    changes
        .record(M::put(N::Entities, b"z", None, b"untouched-by-later-write").unwrap())
        .unwrap();
    changes
        .record(M::put(N::Entities, b"a", Some(hash(b"original")), b"intermediate").unwrap())
        .unwrap();
    changes
        .record(M::put(N::Entities, b"a", Some(hash(b"intermediate")), b"final").unwrap())
        .unwrap();
    let result = changes.finish().unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].key(), b"a");
    assert_eq!(result[0].value(), Some(b"final".as_slice()));
    assert_eq!(result[0].expected_hash(), Some(hash(b"original")));
    assert_eq!(result[1].key(), b"z");
    assert_eq!(result[1].expected_hash(), None);
}

#[test]
fn insert_delete_and_exact_restoration_leave_no_net_transition() {
    let mut changes = AuthoritativeMutationAccumulatorV3::default();
    changes
        .record(M::put(N::Entities, b"created", None, b"temporary").unwrap())
        .unwrap();
    changes
        .record(M::delete(N::Entities, b"created", hash(b"temporary")).unwrap())
        .unwrap();
    changes
        .record(M::delete(N::Entities, b"restored", hash(b"original")).unwrap())
        .unwrap();
    changes
        .record(M::put(N::Entities, b"restored", None, b"original").unwrap())
        .unwrap();
    assert!(changes.finish().unwrap().is_empty());
}

#[test]
fn ignored_intermediate_precondition_failure_poisoned_the_whole_receipt() {
    let mut changes = AuthoritativeMutationAccumulatorV3::default();
    changes
        .record(M::put(N::Entities, b"key", None, b"current").unwrap())
        .unwrap();
    assert_eq!(
        changes.record(M::delete(N::Entities, b"key", hash(b"stale")).unwrap()),
        Err(ChangelogV3Error::PredecessorMismatch)
    );
    assert_eq!(
        changes.record(M::delete(N::Entities, b"key", hash(b"current")).unwrap()),
        Err(ChangelogV3Error::PredecessorMismatch)
    );
    assert_eq!(changes.finish(), Err(ChangelogV3Error::PredecessorMismatch));
}

#[test]
fn cancellation_does_not_erase_the_observed_predecessor_for_later_writes() {
    let mut changes = AuthoritativeMutationAccumulatorV3::default();
    changes
        .record(M::put(N::Entities, b"key", None, b"temporary").unwrap())
        .unwrap();
    changes
        .record(M::delete(N::Entities, b"key", hash(b"temporary")).unwrap())
        .unwrap();
    assert_eq!(
        changes.record(M::put(N::Entities, b"key", Some(hash(b"foreign")), b"next").unwrap()),
        Err(ChangelogV3Error::PredecessorMismatch)
    );
    assert_eq!(changes.finish(), Err(ChangelogV3Error::PredecessorMismatch));
}

#[test]
fn all_five_step_put_delete_histories_match_an_independent_net_state_oracle() {
    for initial in [None, Some(b"original".as_slice())] {
        for program in 0..3_usize.pow(5) {
            let mut remaining = program;
            let mut observed = initial;
            let mut changes = AuthoritativeMutationAccumulatorV3::default();
            for _ in 0..5 {
                let next = match remaining % 3 {
                    0 => None,
                    1 => Some(b"A".as_slice()),
                    _ => Some(b"B".as_slice()),
                };
                remaining /= 3;
                match (observed, next) {
                    (None, None) => (),
                    (Some(before), None) => changes
                        .record(M::delete(N::Entities, b"key", hash(before)).unwrap())
                        .unwrap(),
                    (before, Some(after)) => changes
                        .record(M::put(N::Entities, b"key", before.map(hash), after).unwrap())
                        .unwrap(),
                }
                observed = next;
            }
            let net = changes.finish().unwrap();
            if initial == observed {
                assert!(
                    net.is_empty(),
                    "nonempty net for restoring history {program}"
                );
            } else {
                assert_eq!(net.len(), 1);
                assert_eq!(net[0].expected_hash(), initial.map(hash));
                assert_eq!(net[0].value(), observed);
            }
        }
    }
}

#[test]
fn receipt_total_budget_is_checked_across_distinct_keys_and_poisoned_on_overflow() {
    let mut changes = AuthoritativeMutationAccumulatorV3::default();
    // Reserve the complete frame wrapper as well as the receipt itself.
    let bytes = vec![0; riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES - 342 - 256];
    changes
        .record(M::put(N::Entities, b"a", None, &bytes).unwrap())
        .unwrap();
    drop(bytes);
    assert_eq!(
        changes.record(M::put(N::Entities, b"b", None, &[1; 256]).unwrap()),
        Err(ChangelogV3Error::LimitExceeded)
    );
    assert_eq!(changes.finish(), Err(ChangelogV3Error::LimitExceeded));
}
