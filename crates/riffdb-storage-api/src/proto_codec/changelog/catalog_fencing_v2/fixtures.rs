use super::*;
use crate::{
    AuditPrincipalV1, ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
    ReplicationFollowerStateV3 as Follower,
};
use riffdb_types::{ActorId, ActorKind, CapabilityId, CommitSequence, DualFrontier, Timestamp};
use std::num::NonZeroU64;

pub(super) fn point(sequence: u64, app: u64, admin: u64) -> Point {
    Point::new(
        ChangelogTransactionSequence::new(sequence).unwrap(),
        [sequence.to_be_bytes()[7]; 32],
        DualFrontier::new(CommitSequence::new(app), AdministrationSequence::new(admin)),
    )
}
pub(super) fn lineage(incarnation: u64, epoch: u64) -> ChangelogLineageV3 {
    ChangelogLineageV3::new_with_catalog(
        DatabaseId::from_unix_milliseconds_and_random(1234, [1; 10]).unwrap(),
        incarnation,
        LeadershipEpochV1::new(epoch).unwrap(),
        AuthoritativeStateCatalogV3.digest(),
    )
    .unwrap()
}
pub(super) fn evidence(
    incarnation: u64,
    epoch: u64,
    wide: bool,
) -> (Request, Follower, PrimaryFenceSourceEvidenceV1) {
    let source = lineage(incarnation, epoch);
    let target = ReplicationFollowerAuditTargetV1::new(
        source.database_id(),
        incarnation,
        source.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([2; 16]).unwrap(),
    )
    .unwrap();
    let fence_id =
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1234, [3; 10]).unwrap();
    let principal = AuditPrincipalV1::new(
        ActorId::new(if wide {
            "a".repeat(256)
        } else {
            "catalog-operator".to_owned()
        })
        .unwrap(),
        ActorKind::Human,
        CapabilityId::from_unix_milliseconds_and_random(1234, [4; 10]).unwrap(),
        NonZeroU64::new(if wide { u64::MAX } else { 1 }).unwrap(),
    );
    let (observed, applied, tail, generation, administration) = if wide {
        (
            point(u64::MAX - 2, u64::MAX, u64::MAX - 1),
            point(u64::MAX - 3, u64::MAX - 1, u64::MAX - 4),
            point(u64::MAX - 1, u64::MAX, u64::MAX),
            u64::MAX - 4,
            u64::MAX,
        )
    } else {
        (point(9, 11, 5), point(8, 3, 4), point(10, 11, 6), 4, 6)
    };
    let fence = Fence::new_catalog_bound(
        source,
        AdministrationSequence::new(administration).unwrap(),
        Timestamp::new(if wide { i64::MAX } else { 1234 }, 999_999_999).unwrap(),
        fence_id,
        RequestId::from_unix_milliseconds_and_random(1234, [5; 10]).unwrap(),
        principal,
        wide.then(|| ApprovalId::new("p".repeat(256)).unwrap()),
        target,
        ChangelogTransactionSequence::new(generation).unwrap(),
        observed,
    )
    .unwrap();
    let anchor = point(1, 0, 0);
    let evidence = PrimaryFenceSourceEvidenceV1::new(
        fence,
        applied,
        History::new(source, anchor, tail, anchor).unwrap(),
    )
    .unwrap();
    let request = Request::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(1234, [6; 10]).unwrap(),
        fence_id,
        target,
        ChangelogTransactionSequence::new(generation).unwrap(),
    );
    (
        request,
        Follower::attached(source, applied, Some(anchor)).unwrap(),
        evidence,
    )
}
pub(super) fn promotion(wide: bool) -> Promotion {
    let (request, follower, evidence) = evidence(
        if wide { u64::MAX - 1 } else { 2 },
        if wide { u64::MAX - 1 } else { 7 },
        wide,
    );
    let mut receipt = Receipt::attempted(
        request,
        RequestId::from_unix_milliseconds_and_random(1234, [7; 10]).unwrap(),
        evidence.fence().principal().clone(),
        evidence.fence().approval_id().cloned(),
        Timestamp::new(if wide { i64::MAX } else { 1234 }, 999_999_999).unwrap(),
    );
    receipt.advance(Step::Phase(Phase::Draining)).unwrap();
    receipt.advance(Step::Phase(Phase::Offline)).unwrap();
    receipt
        .record_selection(Selection::new(request, follower, evidence).unwrap())
        .unwrap();
    receipt.advance(Step::Phase(Phase::CutoverPending)).unwrap();
    Promotion::new(
        receipt,
        Timestamp::new(1235, 0).unwrap(),
        ServiceIngressKindV1::Grpc,
    )
    .unwrap()
}
