// req: REP-005
use super::*;
use riffdb_storage_api::{
    AdministrationSequenceAllocator, ReplicationPromotionPhaseV1 as Phase,
    ReplicationPromotionReceiptV1 as Receipt, ReplicationPromotionStepV1 as Step,
    StoredPromotionAdministrationV1 as Administration,
};
use riffdb_types::ServiceIngressKindV1;

fn pending() -> Receipt {
    let (request, follower, evidence) = fixture(2, 7, 11, 3);
    let mut attempt = Receipt::attempted(
        request,
        RequestId::from_unix_milliseconds_and_random(1234, [7; 10]).unwrap(),
        evidence.fence().principal().clone(),
        None,
        Timestamp::new(1234, 5).unwrap(),
    );
    attempt.advance(Step::Phase(Phase::Draining)).unwrap();
    attempt.advance(Step::Phase(Phase::Offline)).unwrap();
    attempt
        .record_selection(Selection::new(request, follower, evidence).unwrap())
        .unwrap();
    attempt.advance(Step::Phase(Phase::CutoverPending)).unwrap();
    attempt
}

#[path = "cutover_codec.rs"]
mod codec;

#[test]
fn promotion_administration_refuses_partial_attempts_wrong_sequence_and_mcp() {
    let pending = pending();
    let timestamp = Timestamp::new(1235, 0).unwrap();
    for phase_len in 1..pending.steps().len() {
        let earlier = Receipt::from_canonical_parts(
            pending.request(),
            pending.request_id(),
            pending.principal().clone(),
            pending.approval_id().cloned(),
            pending.timestamp(),
            (phase_len >= 4).then(|| pending.selection().unwrap().clone()),
            pending.steps()[..phase_len].to_vec(),
        )
        .unwrap();
        assert!(Administration::new(earlier, timestamp, ServiceIngressKindV1::Grpc).is_err());
    }
    let mut committed = pending.clone();
    committed
        .advance(Step::Phase(Phase::CutoverCommitted))
        .unwrap();
    assert!(Administration::new(committed, timestamp, ServiceIngressKindV1::Grpc).is_err());
    assert!(
        Administration::new(pending.clone(), timestamp, ServiceIngressKindV1::McpHttp).is_err()
    );
    for wrong in [1, 4, 5, 7, u64::MAX] {
        assert!(
            Administration::from_canonical_parts(
                AdministrationSequence::new(wrong).unwrap(),
                pending.clone(),
                timestamp,
                ServiceIngressKindV1::Grpc,
            )
            .is_err()
        );
    }
    let expected =
        Administration::new(pending.clone(), timestamp, ServiceIngressKindV1::Grpc).unwrap();
    assert_eq!(
        Administration::from_canonical_parts(
            expected.administration_sequence(),
            pending,
            timestamp,
            ServiceIngressKindV1::Grpc,
        )
        .unwrap(),
        expected
    );
}

fn pending_at_administration(administration: u64) -> Receipt {
    let (request, _, original) = fixture(2, 7, 11, 11);
    let lineage = original.source_history().lineage();
    let applied = point(11, 11, administration);
    let evidence = Evidence::new(
        original.fence().clone(),
        applied,
        History::new(
            lineage,
            point(1, 0, 0),
            point(12, 11, administration),
            point(1, 0, 0),
        )
        .unwrap(),
    )
    .unwrap();
    let selection = Selection::new(
        request,
        Follower::attached(lineage, applied, None).unwrap(),
        evidence,
    )
    .unwrap();
    let attempt = pending();
    Receipt::from_canonical_parts(
        attempt.request(),
        attempt.request_id(),
        attempt.principal().clone(),
        attempt.approval_id().cloned(),
        attempt.timestamp(),
        Some(selection),
        attempt.steps().to_vec(),
    )
    .unwrap()
}

#[test]
fn promotion_administration_preflights_all_three_counters_at_exhaustion() {
    let timestamp = Timestamp::new(1235, 0).unwrap();
    for prior in [u64::MAX - 2, u64::MAX - 1, u64::MAX] {
        assert!(
            Administration::new(
                pending_at_administration(prior),
                timestamp,
                ServiceIngressKindV1::Grpc,
            )
            .is_err()
        );
    }
    let final_range = Administration::new(
        pending_at_administration(u64::MAX - 3),
        timestamp,
        ServiceIngressKindV1::Grpc,
    )
    .unwrap();
    assert_eq!(final_range.started_sequence().get(), u64::MAX - 2);
    assert_eq!(final_range.administration_sequence().get(), u64::MAX - 1);
    assert_eq!(final_range.succeeded_sequence().get(), u64::MAX);
    assert_eq!(
        final_range.next_administration(),
        AdministrationSequenceAllocator::Exhausted
    );
    assert_eq!(
        final_range.attempt().selection().unwrap().application_rpo(),
        0
    );
}

#[test]
fn promotion_administration_binds_pending_attempt_and_complete_audit_allocation() {
    let attempt = pending();
    let record = Administration::new(
        attempt.clone(),
        Timestamp::new(1235, 0).unwrap(),
        ServiceIngressKindV1::Grpc,
    )
    .unwrap();
    assert_eq!(record.attempt(), &attempt);
    // Applied administration is four: start, control, and success are one range.
    assert_eq!(record.started_sequence().get(), 5);
    assert_eq!(record.administration_sequence().get(), 6);
    assert_eq!(record.succeeded_sequence().get(), 7);
    assert_eq!(
        record.covered_frontier(),
        DualFrontier::new(CommitSequence::new(3), AdministrationSequence::new(7))
    );
    let selection = record.attempt().selection().unwrap();
    assert_eq!(selection.application_rpo(), 8);
    assert_eq!(selection.published_lineage().history_incarnation(), 3);
    assert_eq!(selection.published_lineage().leadership_epoch().get(), 8);
    assert_eq!(
        format!("{record:?}"),
        "StoredPromotionAdministrationV1([redacted])"
    );
    // Metadata construction never records completed runtime validation or readiness.
    assert_eq!(record.attempt().phase(), Phase::CutoverPending);
    assert!(!record.attempt().is_terminal());
}
