// req: REP-005, STO-012, REC-001
use super::*;

#[test]
fn catalog_v3_admission_and_fence_roundtrip_without_v1_reinterpretation() {
    let (_, _, source) = evidence(2, 7, false);
    let fence = source.fence();
    let active = Admission::active_catalog_bound(source.source_history().lineage()).unwrap();
    for original in [active, Admission::fenced(fence.clone())] {
        assert!(super::super::super::encode_replication_primary_admission_v1(&original).is_err());
        let encoded = encode_replication_primary_admission(&original).unwrap();
        let direct = decode_replication_primary_admission_v2(encoded.as_bytes()).unwrap();
        assert_eq!(direct.value(), &original);
        assert_eq!(
            decode_replication_primary_admission(encoded.as_bytes())
                .unwrap()
                .value(),
            &original
        );
        assert_eq!(
            direct.encoded_content_charge(),
            encoded.encoded_content_charge()
        );
        assert!(
            super::super::super::decode_replication_primary_admission_v1(encoded.as_bytes())
                .is_err()
        );
    }
    assert!(super::super::super::encode_primary_fence_administration_v1(fence).is_err());
    let encoded = encode_primary_fence_administration(fence).unwrap();
    assert_eq!(
        decode_primary_fence_administration(encoded.as_bytes())
            .unwrap()
            .value(),
        fence
    );
    assert!(
        super::super::super::decode_primary_fence_administration_v1(encoded.as_bytes()).is_err()
    );
    let bytes = source.encode_fence_record().unwrap();
    let history = source.source_history();
    let reconstructed = PrimaryFenceSourceEvidenceV1::from_fence_record(
        &bytes,
        source.applied(),
        history.anchor(),
        history.tail(),
        history.minimum_resume(),
    )
    .unwrap();
    assert_eq!(reconstructed, source);
    assert_eq!(reconstructed.encode_fence_record().unwrap(), bytes);
    assert!(
        Admission::active(history.lineage()).is_err(),
        "historical constructor stays V2"
    );
}

#[test]
fn catalog_v3_promotion_roundtrip_preserves_rpo_and_exact_successors() {
    let original = promotion(false);
    assert!(super::super::super::encode_promotion_administration_v1(&original).is_err());
    let encoded = encode_promotion_administration(&original).unwrap();
    let decoded = decode_promotion_administration(encoded.as_bytes()).unwrap();
    assert_eq!(decoded.value(), &original);
    assert_eq!(
        encode_promotion_administration_v2(decoded.value())
            .unwrap()
            .as_bytes(),
        encoded.as_bytes()
    );
    assert_eq!(
        decoded.encoded_content_charge(),
        encoded.encoded_content_charge()
    );
    let selected = decoded.value().attempt().selection().unwrap();
    assert_eq!(selected.application_rpo(), 8);
    assert_eq!(selected.published_lineage().history_incarnation(), 3);
    assert_eq!(selected.published_lineage().leadership_epoch().get(), 8);
    assert_eq!(
        selected.published_lineage().catalog_digest(),
        AuthoritativeStateCatalogV3.digest()
    );
    assert!(super::super::super::decode_promotion_administration_v1(encoded.as_bytes()).is_err());
}

#[test]
fn current_dispatch_preserves_exact_version_one_bytes_and_meaning() {
    let (request, _, source) = evidence(2, 7, false);
    let current = source.fence();
    let legacy_fence = Fence::new(
        current.administration_sequence(),
        current.timestamp(),
        current.operation_id(),
        current.request_id(),
        current.principal().clone(),
        current.approval_id().cloned(),
        current.target(),
        current.generation(),
        current.observed(),
    )
    .unwrap();
    let legacy = legacy_fence.lineage();
    assert_eq!(
        legacy.catalog_digest(),
        AuthoritativeStateCatalogV2.digest()
    );
    let old_fence =
        super::super::super::encode_primary_fence_administration_v1(&legacy_fence).unwrap();
    assert_eq!(
        encode_primary_fence_administration(&legacy_fence)
            .unwrap()
            .as_bytes(),
        old_fence.as_bytes()
    );
    assert_eq!(
        decode_primary_fence_administration(old_fence.as_bytes())
            .unwrap()
            .value(),
        &legacy_fence
    );
    assert!(encode_primary_fence_administration_v2(&legacy_fence).is_err());
    for admission in [
        Admission::active(legacy).unwrap(),
        Admission::fenced(legacy_fence.clone()),
    ] {
        let old = super::super::super::encode_replication_primary_admission_v1(&admission).unwrap();
        assert_eq!(
            encode_replication_primary_admission(&admission)
                .unwrap()
                .as_bytes(),
            old.as_bytes()
        );
        assert_eq!(
            decode_replication_primary_admission(old.as_bytes())
                .unwrap()
                .value(),
            &admission
        );
        assert!(encode_replication_primary_admission_v2(&admission).is_err());
    }
    let history = source.source_history();
    let old_source = PrimaryFenceSourceEvidenceV1::new(
        legacy_fence,
        source.applied(),
        crate::ChangelogHistoryStateV3::new(
            legacy,
            history.anchor(),
            history.tail(),
            history.minimum_resume(),
        )
        .unwrap(),
    )
    .unwrap();
    let follower =
        crate::ReplicationFollowerStateV3::attached(legacy, old_source.applied(), None).unwrap();
    let selection = Selection::new(request, follower, old_source).unwrap();
    let mut attempt = Receipt::attempted(
        request,
        current.request_id(),
        current.principal().clone(),
        current.approval_id().cloned(),
        current.timestamp(),
    );
    attempt.advance(Step::Phase(Phase::Draining)).unwrap();
    attempt.advance(Step::Phase(Phase::Offline)).unwrap();
    attempt.record_selection(selection).unwrap();
    attempt.advance(Step::Phase(Phase::CutoverPending)).unwrap();
    let promotion =
        Promotion::new(attempt, current.timestamp(), ServiceIngressKindV1::Grpc).unwrap();
    let old = super::super::super::encode_promotion_administration_v1(&promotion).unwrap();
    assert_eq!(
        encode_promotion_administration(&promotion)
            .unwrap()
            .as_bytes(),
        old.as_bytes()
    );
    assert_eq!(
        decode_promotion_administration(old.as_bytes())
            .unwrap()
            .value(),
        &promotion
    );
    assert!(encode_promotion_administration_v2(&promotion).is_err());
}
