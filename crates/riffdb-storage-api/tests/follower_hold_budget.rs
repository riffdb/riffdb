#![forbid(unsafe_code)]
// req: REP-006
//! Budget observations are health inputs, never a retention-release permit.

use riffdb_storage_api::{
    ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
    ChangelogLineageV3 as Lineage, ChangelogTransactionSequence as Sequence, ChangelogV3Error,
    FollowerHoldBudget, LeadershipEpochV1, ReplicationSourceHoldIdV1 as Id,
    ReplicationSourceHoldKindV1 as Kind, ReplicationSourceHoldV1 as Hold,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

fn lineage(incarnation: u64, epoch: u64) -> Lineage {
    Lineage::new(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap(),
        incarnation,
        LeadershipEpochV1::new(epoch).unwrap(),
    )
    .unwrap()
}

fn point(physical: u64, application: u64, administration: u64) -> Point {
    Point::new(
        Sequence::new(physical).unwrap(),
        [0x77; 32],
        DualFrontier::new(
            CommitSequence::new(application),
            AdministrationSequence::new(administration),
        ),
    )
}

fn hold(fence: Point) -> Hold {
    Hold::new(
        Id::new([0x81; 16]).unwrap(),
        Kind::FollowerAcknowledgement,
        lineage(1, 1),
        fence,
    )
}

fn history(minimum: Point, tail: Point) -> History {
    History::new(lineage(1, 1), minimum, tail, minimum).unwrap()
}

#[test]
fn follower_budget_degrades_at_the_limit_and_recovers_only_with_observed_progress() {
    assert!(FollowerHoldBudget::new(0).is_none());
    let budget = FollowerHoldBudget::new(10).unwrap();
    let anchor = point(1, 0, 0);
    let original = hold(anchor);
    for (sequence, remaining, exhausted) in
        [(0, 10, false), (9, 1, false), (10, 0, true), (11, 0, true)]
    {
        let observed = budget
            .observe(original, history(anchor, point(20, sequence, 0)))
            .unwrap();
        assert_eq!(observed.application_lag_sequences(), sequence);
        assert_eq!(observed.remaining_sequences(), remaining);
        assert_eq!(observed.is_exhausted(), exhausted);
    }
    let head = point(20, 11, 0);
    let caught_up = budget.observe(hold(head), history(anchor, head)).unwrap();
    assert_eq!(caught_up.application_lag_sequences(), 0);
    assert_eq!(caught_up.remaining_sequences(), 10);
    assert!(!caught_up.is_exhausted());
}

#[test]
fn follower_budget_counts_application_lag_without_charging_control_or_audit_sequences() {
    let budget = FollowerHoldBudget::new(1).unwrap();
    let anchor = point(1, 42, 0);
    let source = history(anchor, point(u64::MAX, 42, u64::MAX));
    let observation = budget.observe(hold(anchor), source).unwrap();
    assert_eq!(observation.application_lag_sequences(), 0);
    assert_eq!(observation.remaining_sequences(), 1);
    assert!(!observation.is_exhausted());
    assert_eq!(budget.sequences(), 1);
}

#[test]
fn follower_budget_refuses_foreign_roles_lineages_and_unretained_or_substituted_fences() {
    let budget = FollowerHoldBudget::new(10).unwrap();
    let minimum = point(5, 5, 5);
    let tail = point(20, 20, 20);
    let source = history(minimum, tail);
    let original = hold(minimum);
    let mut wrong_hash = minimum.history_hash();
    wrong_hash[0] ^= 1;
    let rejected = [
        Hold::new(
            original.id(),
            Kind::FollowerAcknowledgement,
            Lineage::new(
                DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x72; 10])
                    .unwrap(),
                1,
                LeadershipEpochV1::initial(),
            )
            .unwrap(),
            minimum,
        ),
        Hold::new(
            original.id(),
            Kind::ArchiveAcknowledgement,
            lineage(1, 1),
            minimum,
        ),
        Hold::new(original.id(), Kind::Bootstrap, lineage(1, 1), minimum),
        Hold::new(
            original.id(),
            Kind::FollowerAcknowledgement,
            lineage(2, 1),
            minimum,
        ),
        Hold::new(
            original.id(),
            Kind::FollowerAcknowledgement,
            lineage(1, 2),
            minimum,
        ),
        hold(point(4, 4, 4)),
        hold(point(21, 21, 21)),
        hold(point(10, 21, 10)),
        hold(point(10, 10, 21)),
        hold(Point::new(
            minimum.sequence(),
            wrong_hash,
            minimum.frontier(),
        )),
        hold(Point::new(tail.sequence(), wrong_hash, tail.frontier())),
    ];
    for bad in rejected {
        assert!(matches!(
            budget.observe(bad, source),
            Err(ChangelogV3Error::PredecessorMismatch)
        ));
    }
    assert!(budget.observe(original, source).is_ok());
}

#[test]
fn follower_budget_handles_the_complete_application_counter_range_without_wrapping() {
    let budget = FollowerHoldBudget::new(u64::MAX).unwrap();
    let anchor = point(1, 0, 0);
    let source = history(anchor, point(u64::MAX, u64::MAX, u64::MAX));
    let observation = budget.observe(hold(anchor), source).unwrap();
    assert_eq!(observation.application_lag_sequences(), u64::MAX);
    assert_eq!(observation.remaining_sequences(), 0);
    assert!(observation.is_exhausted());
}
