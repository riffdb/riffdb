// req: EFF-002, EFF-003, REC-001, END-008
use super::*;
use redb::ReadableDatabase;
use riffdb_storage_api::{
    OutboxClaimGroupResultV1, OutboxClaimGroupV1, PendingOutboxScanV1, PublishedDurableSnapshot,
};

fn history(ports: &RedbOperationalPorts) -> riffdb_storage_api::ChangelogHistoryStateV3 {
    crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap()
}

fn claim(event_id: EventId, expected: OutboxStatusObservationV1) -> OutboxClaimV1 {
    OutboxClaimV1::new(
        event_id,
        expected,
        destination(),
        timestamp(30),
        timestamp(40),
    )
    .unwrap()
}

fn claim_group(
    ports: &mut RedbOperationalPorts,
    claims: Vec<OutboxClaimV1>,
) -> OutboxClaimGroupResultV1 {
    ports
        .claim_outbox_group(&OutboxClaimGroupV1::new(claims).unwrap())
        .unwrap()
}

#[test]
fn outbox_claim_group_preserves_exact_status_cas_and_atomic_receipt() {
    let (_path, mut ports) = operational("outbox-claim-group");
    let events = seed_command(&ports, CommitSequence::first(), 16);
    let ids = events
        .iter()
        .map(|event| event.event_id())
        .collect::<Vec<_>>();
    // Event 3 has an explicit pending status from an earlier retried attempt.
    let OutboxTransitionResultV1::Applied(delivering) = ports
        .claim_outbox(
            &OutboxClaimV1::new(
                ids[3],
                OutboxStatusObservationV1::AbsentInitialPending,
                destination(),
                timestamp(10),
                timestamp(20),
            )
            .unwrap(),
        )
        .unwrap()
    else {
        panic!("fresh event must be claimed");
    };
    let OutboxTransitionResultV1::Applied(pending) = ports
        .retry_outbox(&OutboxRetryV1::new(delivering, Some(timestamp(12)), None).unwrap())
        .unwrap()
    else {
        panic!("claimed event must retry");
    };
    // Event 7 is claimed elsewhere first, so the group's absent observation is stale.
    let OutboxTransitionResultV1::Applied(raced) = ports
        .claim_outbox(&claim(
            ids[7],
            OutboxStatusObservationV1::AbsentInitialPending,
        ))
        .unwrap()
    else {
        panic!("fresh event must be claimed");
    };
    let claims = ids
        .iter()
        .enumerate()
        .map(|(index, &event_id)| {
            claim(
                event_id,
                if index == 3 {
                    OutboxStatusObservationV1::Present(pending.clone())
                } else {
                    OutboxStatusObservationV1::AbsentInitialPending
                },
            )
        })
        .collect::<Vec<_>>();
    let commits_before = ports.shared.outbox_stage_census.snapshot()[7][0];
    let prior_history = history(&ports);
    let OutboxClaimGroupResultV1::Complete(completion) = claim_group(&mut ports, claims.clone())
    else {
        panic!("fixture authority must exist");
    };
    for (index, (claim, result)) in claims.iter().zip(completion.results()).enumerate() {
        if index == 7 {
            assert_eq!(
                result,
                &OutboxTransitionResultV1::StateChanged(OutboxStatusObservationV1::Present(
                    raced.clone()
                ))
            );
            continue;
        }
        assert_eq!(
            result,
            &OutboxTransitionResultV1::Applied(claim.updated().clone())
        );
        assert_eq!(
            ports.read_outbox_status(claim.event_id()).unwrap(),
            OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::Present(
                claim.updated().clone()
            ))
        );
    }
    // One transaction, one receipt, one primary commit for fifteen claims.
    assert_eq!(
        ports.shared.outbox_stage_census.snapshot()[7][0] - commits_before,
        1
    );
    let next_history = history(&ports);
    assert_eq!(
        next_history.tail().sequence().get(),
        prior_history.tail().sequence().get() + 1
    );
    let snapshot =
        crate::changelog::RedbPublishedSnapshot::new(ports.begin_composite_read().unwrap());
    let mut receipts = snapshot
        .changelog_receipts_v3(prior_history.lineage(), prior_history.tail())
        .unwrap();
    let receipt = receipts.next_receipt().unwrap().unwrap();
    assert!(receipts.next_receipt().unwrap().is_none());
    assert_eq!(
        receipt.attribution(),
        riffdb_storage_api::ChangelogAttributionV3::OutboxTransition
    );
    let applied = claims
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != 7)
        .map(|(_, claim)| claim)
        .collect::<Vec<_>>();
    assert_eq!(receipt.mutations().len(), applied.len());
    for (mutation, claim) in receipt.mutations().iter().zip(&applied) {
        assert_eq!(mutation.key(), claim.event_id().to_be_bytes());
        // An initial event inserts; the explicit pending one replaces its bytes.
        let prior = match claim.expected() {
            OutboxStatusObservationV1::AbsentInitialPending => None,
            OutboxStatusObservationV1::Present(status) => {
                Some(encode_outbox_status_v1(status).unwrap())
            }
        };
        assert!(mutation.matches_prior(prior.as_ref().map(|bytes| bytes.as_bytes())));
        assert_eq!(
            mutation.value(),
            Some(encode_outbox_status_v1(claim.updated()).unwrap().as_bytes())
        );
    }
    // Claimed events leave the pending scan; they stay undelivered.
    let page = ports
        .scan_pending_outbox(
            None,
            OutboxPageLimit::new(std::num::NonZeroU16::new(32).unwrap()).unwrap(),
        )
        .unwrap();
    let (PendingOutboxScanV1::Page { items, .. } | PendingOutboxScanV1::ExactEnd { items }) = page;
    assert!(items.is_empty());
    assert!(ports.has_undelivered_outbox().unwrap());
    // A repeat matches nothing and writes no receipt.
    let OutboxClaimGroupResultV1::Complete(repeated) = claim_group(&mut ports, claims) else {
        panic!("fixture authority must exist");
    };
    assert!(
        repeated
            .results()
            .iter()
            .all(|result| matches!(result, OutboxTransitionResultV1::StateChanged(_)))
    );
    assert_eq!(history(&ports), next_history);
}

#[test]
fn claim_group_missing_authority_aborts_every_match() {
    let (_path, mut ports) = operational("outbox-claim-group-refusal");
    let events = seed_command(&ports, CommitSequence::first(), 2);
    let missing = EventId::new(CommitSequence::new(2).unwrap(), 0);
    let before = history(&ports);
    assert_eq!(
        claim_group(
            &mut ports,
            vec![
                claim(
                    events[0].event_id(),
                    OutboxStatusObservationV1::AbsentInitialPending
                ),
                claim(missing, OutboxStatusObservationV1::AbsentInitialPending),
            ],
        ),
        OutboxClaimGroupResultV1::AuthoritativeIntentMissing
    );
    assert_eq!(history(&ports), before);
    assert_eq!(
        ports.read_outbox_status(events[0].event_id()).unwrap(),
        OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::AbsentInitialPending)
    );
}
