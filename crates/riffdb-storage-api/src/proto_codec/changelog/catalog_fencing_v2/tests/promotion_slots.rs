use super::*;
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1};

// req: REP-007, REC-005, STO-012
#[test]
fn catalog_bound_promotion_reserves_inheritance_before_service_start() {
    let value = promotion(false);
    let selection = value.attempt().selection().unwrap();
    assert_eq!(
        selection.published_lineage().catalog_digest(),
        AuthoritativeStateCatalogV3.digest()
    );
    assert_eq!(
        selection
            .applied()
            .frontier()
            .administration()
            .unwrap()
            .get(),
        4
    );
    // Administration5 belongs to the separately persisted inheritance fact.
    assert_eq!(
        value.started_sequence().get(),
        6,
        "explicit V3 inheritance administration must precede Started"
    );
    assert_eq!(value.administration_sequence().get(), 7);
    assert_eq!(value.succeeded_sequence().get(), 8);
    assert_eq!(
        value.covered_frontier().application(),
        selection.applied().frontier().application()
    );
    assert_eq!(value.covered_frontier().administration().unwrap().get(), 8);
    assert_eq!(
        value.next_administration(),
        crate::AdministrationSequenceAllocator::next(AdministrationSequence::new(9).unwrap())
    );
    let [started, succeeded] = value.service_audits().unwrap();
    assert_eq!(started.administration_sequence().get(), 6);
    assert_eq!(started.phase(), ServiceAuditPhaseV1::Started);
    assert_eq!(started.link(), ServiceAuditLinkV1::None);
    assert_eq!(succeeded.administration_sequence().get(), 8);
    assert_eq!(succeeded.phase(), ServiceAuditPhaseV1::Succeeded);
    assert_eq!(
        succeeded.link(),
        ServiceAuditLinkV1::ControlPlane {
            administration_sequence: AdministrationSequence::new(7).unwrap()
        }
    );
    let reconstructed = Promotion::from_canonical_parts(
        AdministrationSequence::new(7).unwrap(),
        value.attempt().clone(),
        value.timestamp(),
        value.ingress(),
    )
    .unwrap();
    assert_eq!(reconstructed, value);
}

// req: REP-007, REC-005, STO-012
#[test]
fn catalog_bound_promotion_preflights_four_counters_and_exact_exhaustion() {
    let value = promotion(true);
    assert_eq!(value.inheritance_sequence().unwrap().get(), u64::MAX - 3);
    assert_eq!(value.started_sequence().get(), u64::MAX - 2);
    assert_eq!(value.administration_sequence().get(), u64::MAX - 1);
    assert_eq!(value.succeeded_sequence().get(), u64::MAX);
    assert_eq!(
        value.next_administration(),
        crate::AdministrationSequenceAllocator::Exhausted
    );
    let (request, _, source) = evidence(u64::MAX - 1, u64::MAX - 1, true);
    let original_applied = source.applied();
    // This independently valid frozen selection has only three counters left.
    let applied = crate::ChangelogHistoryPointV3::new(
        original_applied.sequence(),
        original_applied.history_hash(),
        riffdb_types::DualFrontier::new(
            original_applied.frontier().application(),
            AdministrationSequence::new(u64::MAX - 3),
        ),
    );
    let source =
        PrimaryFenceSourceEvidenceV1::new(source.fence().clone(), applied, source.source_history())
            .unwrap();
    let follower = crate::ReplicationFollowerStateV3::attached(
        source.source_history().lineage(),
        applied,
        None,
    )
    .unwrap();
    let selection = Selection::new(request, follower, source).unwrap();
    let original = value.attempt();
    let mut attempt = Receipt::attempted(
        request,
        original.request_id(),
        original.principal().clone(),
        original.approval_id().cloned(),
        original.timestamp(),
    );
    attempt.advance(Step::Phase(Phase::Draining)).unwrap();
    attempt.advance(Step::Phase(Phase::Offline)).unwrap();
    attempt.record_selection(selection).unwrap();
    attempt.advance(Step::Phase(Phase::CutoverPending)).unwrap();
    assert_eq!(
        Promotion::new(attempt.clone(), value.timestamp(), value.ingress()).unwrap_err(),
        crate::StorageValueError::SizeOverflow,
    );
    assert_eq!(
        Promotion::new_catalog_bound(attempt, value.timestamp(), value.ingress()).unwrap_err(),
        crate::StorageValueError::SizeOverflow,
    );
}

// req: REP-007, REC-005, STO-012
#[test]
fn catalog_bound_promotion_reconstruction_refuses_old_three_slot_assignment() {
    let value = promotion(false);
    assert_eq!(value.inheritance_sequence().unwrap().get(), 5);
    let forged_control = AdministrationSequence::new(6).unwrap();
    for reconstructed in [
        Promotion::from_canonical_parts(
            forged_control,
            value.attempt().clone(),
            value.timestamp(),
            value.ingress(),
        ),
        Promotion::from_catalog_bound_canonical_parts(
            forged_control,
            value.attempt().clone(),
            value.timestamp(),
            value.ingress(),
        ),
    ] {
        assert_eq!(
            reconstructed.unwrap_err(),
            crate::StorageValueError::IdentityMismatch
        );
    }
    let mut wire = wire_promotion();
    wire.administration_sequence = forged_control.get();
    assert!(decode_promotion_administration_v2(&raw(PROMOTION_V2, wire.encode_to_vec())).is_err());
    assert!(decode_promotion_administration(&raw(PROMOTION_V2, wire.encode_to_vec())).is_err());
}
