#![forbid(unsafe_code)]
// req: REP-005, STO-012, REC-001
//! Admission metadata is evidence, never a writer or authenticated fence proof.
use riffdb_storage_api::{
    AuditPrincipalV1, ChangelogHistoryPointV3 as Point, ChangelogLineageV3 as Lineage,
    ChangelogTransactionSequence as Sequence, ReplicationPrimaryAdmissionV1 as Admission,
    StoredPrimaryFenceAdministrationV1 as Fence,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, CapabilityId, CommitSequence, DatabaseId,
    DualFrontier, LeadershipEpochV1, ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1,
    ReplicationSourceHoldIdV1, RequestId, Timestamp,
};
use std::num::NonZeroU64;

fn lineage(incarnation: u64, epoch: u64) -> Lineage {
    Lineage::new_with_catalog(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [1; 10]).unwrap(),
        incarnation,
        LeadershipEpochV1::new(epoch).unwrap(),
        riffdb_storage_api::AuthoritativeStateCatalogV2.digest(),
    )
    .unwrap()
}
fn point(sequence: u64, application: u64, administration: u64) -> Point {
    Point::new(
        Sequence::new(sequence).unwrap(),
        [9; 32],
        DualFrontier::new(
            CommitSequence::new(application),
            AdministrationSequence::new(administration),
        ),
    )
}
fn fence(
    observed: Point,
    generation: u64,
    administration: u64,
    hold: u8,
) -> Result<Fence, riffdb_storage_api::StorageValueError> {
    let source = lineage(2, 3);
    Fence::new(
        AdministrationSequence::new(administration).unwrap(),
        Timestamp::new(1_700_000_000, 123).unwrap(),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1_700_000_000_000, [2; 10])
            .unwrap(),
        RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [3; 10]).unwrap(),
        AuditPrincipalV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1_700_000_000_000, [4; 10]).unwrap(),
            NonZeroU64::new(1).unwrap(),
        ),
        None,
        ReplicationFollowerAuditTargetV1::new(
            source.database_id(),
            source.history_incarnation(),
            source.leadership_epoch(),
            ReplicationSourceHoldIdV1::new([hold; 16]).unwrap(),
        )
        .unwrap(),
        Sequence::new(generation).unwrap(),
        observed,
    )
}

#[test]
fn fenced_admission_requires_exact_receipt_lineage_and_frozen_application_head() {
    let receipt = fence(point(9, 5, 2), 4, 3, 7).unwrap();
    let admission = Admission::fenced(receipt.clone());
    assert_eq!(admission.lineage(), lineage(2, 3));
    assert_eq!(admission.fence(), Some(&receipt));
    assert!(
        admission
            .validate_source_evidence(lineage(2, 3), CommitSequence::new(5), Some(&receipt))
            .is_ok()
    );
    assert!(
        admission
            .validate_source_evidence(lineage(2, 3), CommitSequence::new(5), None)
            .is_err()
    );
    for source in [lineage(3, 3), lineage(2, 4)] {
        assert!(
            admission
                .validate_source_evidence(source, CommitSequence::new(5), Some(&receipt))
                .is_err()
        );
    }
    for app in [0, 4, 6] {
        assert!(
            admission
                .validate_source_evidence(lineage(2, 3), CommitSequence::new(app), Some(&receipt))
                .is_err()
        );
    }
    let substituted = fence(point(9, 5, 2), 4, 3, 8).unwrap();
    assert!(
        admission
            .validate_source_evidence(lineage(2, 3), CommitSequence::new(5), Some(&substituted))
            .is_err()
    );
    assert_eq!(
        format!("{admission:?}"),
        "ReplicationPrimaryAdmissionV1([redacted])"
    );
    assert_eq!(
        format!("{receipt:?}"),
        "StoredPrimaryFenceAdministrationV1([redacted])"
    );
}

#[test]
fn active_metadata_cannot_hide_an_existing_fence_or_foreign_lineage() {
    let admission = Admission::active(lineage(2, 3)).unwrap();
    let receipt = fence(point(9, 5, 2), 4, 3, 7).unwrap();
    assert_eq!(admission.fence(), None);
    assert!(
        admission
            .validate_source_evidence(lineage(2, 3), None, None)
            .is_ok()
    );
    assert!(
        admission
            .validate_source_evidence(lineage(2, 3), CommitSequence::new(5), Some(&receipt))
            .is_err()
    );
    assert!(
        admission
            .validate_source_evidence(lineage(3, 3), None, None)
            .is_err()
    );
}

#[test]
fn fence_receipt_requires_exact_next_administration_and_existing_generation() {
    assert!(fence(point(9, 5, 2), 10, 3, 7).is_err());
    for admin in [1, 2, 4, u64::MAX] {
        assert!(fence(point(9, 5, 2), 4, admin, 7).is_err());
    }
    assert!(fence(point(u64::MAX, 5, 2), 4, 3, 7).is_err());
    assert!(fence(point(9, 5, u64::MAX), 4, 1, 7).is_err());
    assert!(fence(point(9, 0, 0), 4, 1, 7).is_err());
    let empty_application = fence(point(9, 0, 2), 4, 3, 7).unwrap();
    assert_eq!(empty_application.final_application_head(), None);
    assert_eq!(empty_application.observed(), point(9, 0, 2));
    assert!(
        Admission::fenced(empty_application.clone())
            .validate_source_evidence(lineage(2, 3), None, Some(&empty_application))
            .is_ok()
    );
}

#[path = "primary_admission/codec.rs"]
mod codec;

#[path = "primary_admission/service_audit.rs"]
mod service_audit;

#[path = "primary_admission/source_evidence.rs"]
mod source_evidence;
