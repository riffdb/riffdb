// req: EFF-002, EFF-003, REC-001, END-008
use super::*;
use redb::ReadableDatabase;
use riffdb_storage_api::{
    OutboxSucceedGroupResultV1, OutboxSucceedGroupV1, PublishedDurableSnapshot,
};

fn history(ports: &RedbOperationalPorts) -> riffdb_storage_api::ChangelogHistoryStateV3 {
    crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap()
}

pub(super) fn claimed_successes(
    ports: &mut RedbOperationalPorts,
    count: u32,
) -> Vec<OutboxSucceedV1> {
    let events = seed_command(ports, CommitSequence::first(), count);
    events
        .iter()
        .map(|event| {
            let claim = OutboxClaimV1::new(
                event.event_id(),
                OutboxStatusObservationV1::AbsentInitialPending,
                destination(),
                timestamp(10),
                timestamp(20),
            )
            .unwrap();
            let OutboxTransitionResultV1::Applied(delivering) = ports.claim_outbox(&claim).unwrap()
            else {
                panic!("fresh event must be claimed");
            };
            OutboxSucceedV1::new(delivering, timestamp(11)).unwrap()
        })
        .collect()
}

fn complete_group(
    ports: &mut RedbOperationalPorts,
    transitions: Vec<OutboxSucceedV1>,
) -> Vec<OutboxTransitionResultV1> {
    let group = riffdb_storage_api::OutboxSucceedGroupV1::new(transitions).unwrap();
    let riffdb_storage_api::OutboxSucceedGroupResultV1::Complete(completion) =
        ports.succeed_outbox_group(&group).unwrap()
    else {
        panic!("fixture authority must exist");
    };
    completion.results().to_vec()
}

#[test]
fn outbox_completion_group_preserves_exact_status_cas_and_atomic_receipt() {
    let (_path, mut ports) = operational("outbox-completion-group");
    let transitions = claimed_successes(&mut ports, 16);
    let before = ports.shared.outbox_stage_census.snapshot()[7][0];
    let prior_history = history(&ports);
    let results = complete_group(&mut ports, transitions.clone());
    assert_eq!(results.len(), 16);
    for (transition, result) in transitions.iter().zip(&results) {
        assert_eq!(
            result,
            &OutboxTransitionResultV1::Applied(transition.updated().clone())
        );
        assert_eq!(
            ports.read_outbox_status(transition.event_id()).unwrap(),
            OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::Present(
                transition.updated().clone()
            ))
        );
    }
    assert!(!ports.has_undelivered_outbox().unwrap());
    let next_history = history(&ports);
    assert_eq!(
        next_history.tail().sequence().get(),
        prior_history.tail().sequence().get() + 1
    );
    assert_eq!(
        next_history.tail().frontier(),
        prior_history.tail().frontier()
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
    assert_eq!(receipt.mutations().len(), 16);
    for (mutation, transition) in receipt.mutations().iter().zip(&transitions) {
        assert_eq!(
            mutation.namespace(),
            riffdb_storage_api::AuthoritativeNamespaceV1::OutboxStatus
        );
        assert_eq!(mutation.key(), transition.event_id().to_be_bytes());
        assert!(
            mutation.matches_prior(Some(
                encode_outbox_status_v1(transition.expected())
                    .unwrap()
                    .as_bytes()
            ))
        );
        assert_eq!(
            mutation.value(),
            Some(
                encode_outbox_status_v1(transition.updated())
                    .unwrap()
                    .as_bytes()
            )
        );
    }
    assert_eq!(
        ports.shared.outbox_stage_census.snapshot()[7][0] - before,
        1,
        "sixteen accepted successes must require one primary completion commit"
    );
    let repeated = complete_group(&mut ports, transitions);
    assert!(
        repeated
            .iter()
            .all(|result| matches!(result, OutboxTransitionResultV1::StateChanged(_)))
    );
    assert_eq!(history(&ports), next_history);
    assert_eq!(
        ports.shared.outbox_stage_census.snapshot()[7][0] - before,
        1
    );
}

#[test]
fn success_group_commits_matched_rows_without_overwriting_a_raced_status() {
    let (_path, mut ports) = operational("outbox-group-cas");
    let transitions = claimed_successes(&mut ports, 3);
    ports.succeed_outbox(&transitions[1]).unwrap();
    let before = history(&ports);
    let results = complete_group(&mut ports, transitions.clone());
    assert_eq!(
        results[0],
        OutboxTransitionResultV1::Applied(transitions[0].updated().clone())
    );
    assert_eq!(
        results[1],
        OutboxTransitionResultV1::StateChanged(OutboxStatusObservationV1::Present(
            transitions[1].updated().clone()
        ))
    );
    assert_eq!(
        results[2],
        OutboxTransitionResultV1::Applied(transitions[2].updated().clone())
    );
    let snapshot =
        crate::changelog::RedbPublishedSnapshot::new(ports.begin_composite_read().unwrap());
    let mut receipts = snapshot
        .changelog_receipts_v3(before.lineage(), before.tail())
        .unwrap();
    assert_eq!(
        receipts.next_receipt().unwrap().unwrap().mutations().len(),
        2
    );
    assert!(receipts.next_receipt().unwrap().is_none());
    assert!(!ports.has_undelivered_outbox().unwrap());
}

#[test]
fn success_group_missing_authority_or_corruption_aborts_every_match() {
    let (_path, mut ports) = operational("outbox-group-refusal");
    let transitions = claimed_successes(&mut ports, 3);
    let missing_id = EventId::new(CommitSequence::new(2).unwrap(), 0);
    let claim = OutboxClaimV1::new(
        missing_id,
        OutboxStatusObservationV1::AbsentInitialPending,
        destination(),
        timestamp(10),
        timestamp(20),
    )
    .unwrap();
    let missing = OutboxSucceedV1::new(claim.updated().clone(), timestamp(11)).unwrap();
    let group = OutboxSucceedGroupV1::new(vec![transitions[0].clone(), missing]).unwrap();
    let before = history(&ports);
    assert_eq!(
        ports.succeed_outbox_group(&group).unwrap(),
        OutboxSucceedGroupResultV1::AuthoritativeIntentMissing
    );
    assert_eq!(history(&ports), before);
    assert_eq!(
        ports.read_outbox_status(transitions[0].event_id()).unwrap(),
        OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::Present(
            transitions[0].expected().clone()
        ))
    );
    let raw = ports.shared.database.begin_write().unwrap();
    raw.open_table(OUTBOX_STATUS)
        .unwrap()
        .insert(
            encode_event_key(transitions[2].event_id()).as_slice(),
            b"corrupt".as_slice(),
        )
        .unwrap();
    raw.commit().unwrap();
    let group = OutboxSucceedGroupV1::new(transitions.clone()).unwrap();
    assert_eq!(
        ports.succeed_outbox_group(&group).unwrap_err().kind(),
        StorageErrorKind::CorruptData
    );
    assert_eq!(history(&ports), before);
    assert_eq!(
        ports.read_outbox_status(transitions[0].event_id()).unwrap(),
        OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::Present(
            transitions[0].expected().clone()
        ))
    );
}

#[test]
fn success_group_rejects_empty_duplicate_unordered_and_oversized_inputs() {
    let (_path, mut ports) = operational("outbox-group-inputs");
    let transitions = claimed_successes(&mut ports, 17);
    let before = ports.shared.outbox_stage_census.snapshot();
    assert_eq!(
        OutboxSucceedGroupV1::new(vec![]),
        Err(StorageValueError::Empty)
    );
    assert_eq!(
        OutboxSucceedGroupV1::new(vec![transitions[0].clone(); 2]),
        Err(StorageValueError::Duplicate)
    );
    assert_eq!(
        OutboxSucceedGroupV1::new(vec![transitions[1].clone(), transitions[0].clone()]),
        Err(StorageValueError::NonCanonicalOrder)
    );
    assert_eq!(
        OutboxSucceedGroupV1::new(transitions),
        Err(StorageValueError::LimitExceeded)
    );
    assert_eq!(ports.shared.outbox_stage_census.snapshot(), before);
}
