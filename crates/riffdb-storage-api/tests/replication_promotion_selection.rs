#![forbid(unsafe_code)]
// req: REP-005
//! Selection values bind evidence; they do not authenticate a peer or grant a writer.
use riffdb_storage_api::{
    AuditPrincipalV1, AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2,
    ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
    ChangelogLineageV3 as Lineage, ChangelogTransactionSequence as Sequence,
    PrimaryFenceSourceEvidenceV1 as Evidence, ReplicationFollowerStateV3 as Follower,
    ReplicationPromotionRequestV1 as Request, ReplicationPromotionSelectionV1 as Selection,
    StoredPrimaryFenceAdministrationV1 as Fence,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, CapabilityId, CommitSequence, DatabaseId,
    DualFrontier, LeadershipEpochV1, ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1,
    ReplicationPromotionOperationId, ReplicationSourceHoldIdV1, RequestId, Timestamp,
};
use std::num::NonZeroU64;

fn point(sequence: u64, application: u64, administration: u64) -> Point {
    Point::new(
        Sequence::new(sequence).unwrap(),
        [sequence as u8; 32],
        DualFrontier::new(
            CommitSequence::new(application),
            AdministrationSequence::new(administration),
        ),
    )
}
fn fixture(
    incarnation: u64,
    epoch: u64,
    source_head: u64,
    applied: u64,
) -> (Request, Follower, Evidence) {
    let lineage = Lineage::new_with_catalog(
        DatabaseId::from_unix_milliseconds_and_random(1234, [1; 10]).unwrap(),
        incarnation,
        LeadershipEpochV1::new(epoch).unwrap(),
        AuthoritativeStateCatalogV2.digest(),
    )
    .unwrap();
    let target = ReplicationFollowerAuditTargetV1::new(
        lineage.database_id(),
        incarnation,
        lineage.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([2; 16]).unwrap(),
    )
    .unwrap();
    let fence_id =
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1234, [3; 10]).unwrap();
    let generation = Sequence::new(4).unwrap();
    let fence = Fence::new(
        AdministrationSequence::new(6).unwrap(),
        Timestamp::new(1234, 0).unwrap(),
        fence_id,
        RequestId::from_unix_milliseconds_and_random(1234, [4; 10]).unwrap(),
        AuditPrincipalV1::new(
            ActorId::new("fence-operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1234, [5; 10]).unwrap(),
            NonZeroU64::new(1).unwrap(),
        ),
        None,
        target,
        generation,
        point(9, source_head, 5),
    )
    .unwrap();
    let anchor = point(1, 0, 0);
    let evidence = Evidence::new(
        fence,
        point(8, applied, 4),
        History::new(lineage, anchor, point(10, source_head, 6), anchor).unwrap(),
    )
    .unwrap();
    let request = Request::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(1234, [6; 10]).unwrap(),
        fence_id,
        target,
        generation,
    );
    let follower = Follower::attached(lineage, evidence.applied(), Some(anchor)).unwrap();
    (request, follower, evidence)
}

#[test]
fn promotion_selection_derives_successors_and_exact_application_rpo() {
    for (head, applied) in [(0, 0), (11, 0), (11, 3), (11, 11), (u64::MAX, 1)] {
        let (request, follower, evidence) = fixture(2, 7, head, applied);
        let selected = Selection::new(request, follower, evidence.clone()).unwrap();
        assert_eq!(selected.request(), request);
        assert_eq!(selected.evidence(), &evidence);
        assert_eq!(selected.applied(), point(8, applied, 4));
        assert_eq!(selected.application_rpo(), head - applied);
        let lineage = selected.published_lineage();
        assert_eq!(lineage.database_id(), request.target().database_id());
        assert_eq!(lineage.history_incarnation(), 3);
        assert_eq!(lineage.leadership_epoch().get(), 8);
        assert_eq!(
            lineage.catalog_digest(),
            AuthoritativeStateCatalogV2.digest()
        );
        assert_eq!(
            Selection::from_canonical_parts(request, evidence, lineage, head - applied).unwrap(),
            selected
        );
    }
}

#[test]
fn promotion_selection_refuses_detached_or_substituted_drained_position() {
    let (request, follower, evidence) = fixture(2, 7, 11, 3);
    assert!(Selection::new(request, Follower::detached(), evidence.clone()).is_err());
    let (lineage, applied, ack) = follower.attached_state().unwrap();
    for changed in [
        point(7, 3, 4),
        point(8, 2, 4),
        point(8, 3, 3),
        Point::new(applied.sequence(), [0xaa; 32], applied.frontier()),
    ] {
        let replaced = Follower::attached(lineage, changed, ack).unwrap();
        assert!(Selection::new(request, replaced, evidence.clone()).is_err());
    }
    for changed in [
        Lineage::new_with_catalog(
            lineage.database_id(),
            3,
            lineage.leadership_epoch(),
            lineage.catalog_digest(),
        )
        .unwrap(),
        Lineage::new_with_catalog(
            lineage.database_id(),
            2,
            LeadershipEpochV1::new(8).unwrap(),
            lineage.catalog_digest(),
        )
        .unwrap(),
        Lineage::new_with_catalog(
            lineage.database_id(),
            2,
            lineage.leadership_epoch(),
            AuthoritativeStateCatalogV1.digest(),
        )
        .unwrap(),
        Lineage::new_with_catalog(
            DatabaseId::from_unix_milliseconds_and_random(1234, [9; 10]).unwrap(),
            2,
            lineage.leadership_epoch(),
            lineage.catalog_digest(),
        )
        .unwrap(),
    ] {
        assert!(
            Selection::new(
                request,
                Follower::attached(changed, applied, ack).unwrap(),
                evidence.clone()
            )
            .is_err()
        );
    }
}

#[test]
fn promotion_selection_refuses_different_fence_registration_or_generation() {
    let (request, follower, evidence) = fixture(2, 7, 11, 3);
    let target = request.target();
    let other_target = ReplicationFollowerAuditTargetV1::new(
        target.database_id(),
        target.history_incarnation(),
        target.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([9; 16]).unwrap(),
    )
    .unwrap();
    for changed in [
        Request::new(
            request.operation_id(),
            ReplicationFenceOperationId::from_unix_milliseconds_and_random(1234, [9; 10]).unwrap(),
            target,
            request.generation(),
        ),
        Request::new(
            request.operation_id(),
            request.fence_operation_id(),
            other_target,
            request.generation(),
        ),
        Request::new(
            request.operation_id(),
            request.fence_operation_id(),
            target,
            Sequence::new(5).unwrap(),
        ),
    ] {
        assert!(Selection::new(changed, follower, evidence.clone()).is_err());
    }
}

#[test]
fn promotion_selection_refuses_counter_exhaustion_without_wrapping() {
    for (incarnation, epoch) in [(u64::MAX, 7), (2, u64::MAX), (u64::MAX, u64::MAX)] {
        let (request, follower, evidence) = fixture(incarnation, epoch, 11, 3);
        assert!(Selection::new(request, follower, evidence).is_err());
    }
    let (request, follower, evidence) = fixture(u64::MAX - 1, u64::MAX - 1, 11, 3);
    let selected = Selection::new(request, follower, evidence).unwrap();
    assert_eq!(selected.published_lineage().history_incarnation(), u64::MAX);
    assert_eq!(
        selected.published_lineage().leadership_epoch().get(),
        u64::MAX
    );
}

#[test]
fn promotion_stored_selection_rejects_chosen_counters_catalog_and_rpo() {
    let (request, follower, evidence) = fixture(2, 7, 11, 3);
    let selected = Selection::new(request, follower, evidence.clone()).unwrap();
    let correct = selected.published_lineage();
    for changed in [
        Lineage::new_with_catalog(
            correct.database_id(),
            2,
            correct.leadership_epoch(),
            correct.catalog_digest(),
        )
        .unwrap(),
        Lineage::new_with_catalog(
            correct.database_id(),
            4,
            correct.leadership_epoch(),
            correct.catalog_digest(),
        )
        .unwrap(),
        Lineage::new_with_catalog(
            correct.database_id(),
            3,
            LeadershipEpochV1::new(7).unwrap(),
            correct.catalog_digest(),
        )
        .unwrap(),
        Lineage::new_with_catalog(
            correct.database_id(),
            3,
            LeadershipEpochV1::new(9).unwrap(),
            correct.catalog_digest(),
        )
        .unwrap(),
        Lineage::new_with_catalog(
            correct.database_id(),
            3,
            correct.leadership_epoch(),
            AuthoritativeStateCatalogV1.digest(),
        )
        .unwrap(),
    ] {
        assert!(Selection::from_canonical_parts(request, evidence.clone(), changed, 8).is_err());
    }
    for rpo in [0, 1, 7, 9, u64::MAX] {
        assert!(Selection::from_canonical_parts(request, evidence.clone(), correct, rpo).is_err());
    }
    assert_eq!(
        format!("{request:?}"),
        "ReplicationPromotionRequestV1([redacted])"
    );
    assert_eq!(
        format!("{selected:?}"),
        "ReplicationPromotionSelectionV1([redacted])"
    );
}

#[path = "replication_promotion/receipt.rs"]
mod receipt;

#[path = "replication_promotion/cutover.rs"]
mod cutover;
