//! Checked policy lowering after the source barrier proves receipt ancestry.
use riffdb_storage_api::{
    ChangelogHistoryStateV3 as History, ChangelogV3Error as Error,
    FollowerRegistrationPhaseV1 as Phase, ReplicationSourceHoldV1 as Hold,
    ReplicationSourceHoldV2 as Policy,
};

pub(super) fn advance(prior: Policy, hold: Hold, history: History) -> Result<Policy, Error> {
    if prior.phase() != Phase::Attached
        || prior.hold().id() != hold.id()
        || prior.hold().lineage() != hold.lineage()
        || !prior.hold().fence().precedes_or_equals(hold.fence())
        || prior.hold().fence().sequence() >= hold.fence().sequence()
    {
        return Err(Error::PredecessorMismatch);
    }
    let budget = prior.budget().observe(hold, history)?;
    let head = history
        .tail()
        .frontier()
        .application()
        .map_or(0, |s| s.get());
    let expired = prior
        .expires_at()
        .is_some_and(|expiry| head >= expiry.get());
    // An old degradation point may precede this acknowledgement or no longer
    // prove exhaustion. Recompute from the already validated same-pin head;
    // this records health only and cannot retire the registration or its fence.
    let degraded_at = (budget.is_exhausted() || expired).then_some(history.tail());
    Policy::new(
        hold,
        prior.registered_at(),
        prior.budget(),
        prior.expires_at(),
        Phase::Attached,
        degraded_at,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_storage_api::{
        ChangelogHistoryPointV3 as Point, ChangelogLineageV3 as Lineage,
        ChangelogTransactionSequence as Sequence, FollowerHoldBudget,
        ReplicationSourceHoldKindV1 as Kind,
    };
    use riffdb_types::{
        CommitSequence, DatabaseId, DualFrontier, LeadershipEpochV1, ReplicationSourceHoldIdV1,
    };

    fn point(sequence: u64, application: u64) -> Point {
        Point::new(
            Sequence::new(sequence).unwrap(),
            [sequence as u8; 32],
            DualFrontier::new(CommitSequence::new(application), None),
        )
    }

    // req: REP-006
    #[test]
    fn acknowledgement_recomputes_budget_and_expiry_without_changing_policy() {
        let lineage = Lineage::new(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [1; 10]).unwrap(),
            1,
            LeadershipEpochV1::initial(),
        )
        .unwrap();
        let hold = |fence| {
            Hold::new(
                ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
                Kind::FollowerAcknowledgement,
                lineage,
                fence,
            )
        };
        let old = point(2, 2);
        let head = point(8, 7);
        let history = History::new(lineage, point(1, 0), head, point(1, 0)).unwrap();
        for expiry in [None, CommitSequence::new(6)] {
            let original = Policy::new(
                hold(old),
                old,
                FollowerHoldBudget::new(3).unwrap(),
                expiry,
                Phase::Attached,
                Some(point(6, 5)),
            )
            .unwrap();
            for (ack, exhausted) in [(point(5, 4), true), (point(7, 6), false), (head, false)] {
                let advanced = advance(original, hold(ack), history).unwrap();
                assert_eq!(
                    advanced.degraded_at(),
                    (exhausted || expiry.is_some()).then_some(head)
                );
                assert_eq!(advanced.hold(), hold(ack));
                assert_eq!(advanced.phase(), Phase::Attached);
                assert_eq!(advanced.registered_at(), original.registered_at());
                assert_eq!(advanced.generation(), original.generation());
                assert_eq!(advanced.budget(), original.budget());
                assert_eq!(advanced.expires_at(), original.expires_at());
            }
            assert!(advance(original, original.hold(), history).is_err());
        }
    }
}
