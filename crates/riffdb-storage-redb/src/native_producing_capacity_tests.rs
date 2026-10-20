//! Exact producing-control error classification; no native apply is fabricated.
use super::*;
use riffdb_storage_api::proto_codec::encode_projection_source_maintenance_v1;
use riffdb_storage_api::{
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ChangelogTransactionSequence, DurableCodecError, DurableCodecErrorKind,
    ProjectionSourceDescriptorPartsV1, ProjectionSourceDescriptorV1, ProjectionSourceMaintenanceV1,
};
use riffdb_types::{
    CommitSequence, ContractLineage, DatabaseId, DualFrontier, HashDomain, LeadershipEpochV1,
    ProjectionCheckpointOrdinal, ProjectionId, ProjectionIdentity, ProjectionPlanHash,
    ProjectionSourceLocatorV1, hash,
};

// req: STO-020, STO-012, PRJ-004
#[test]
fn producing_maintenance_capacity_is_typed_refusal_not_corrupt_durable_data() {
    let database = DatabaseId::from_unix_milliseconds_and_random(42, [7; 10]).unwrap();
    let locator =
        ProjectionSourceLocatorV1::new(database, 1, ProjectionCheckpointOrdinal::new(1).unwrap())
            .unwrap();
    let point = |value| {
        ChangelogHistoryPointV3::new(
            ChangelogTransactionSequence::new(value).unwrap(),
            [value as u8; 32],
            DualFrontier::new(Some(CommitSequence::new(value).unwrap()), None),
        )
    };
    let descriptor = ProjectionSourceDescriptorV1::new(ProjectionSourceDescriptorPartsV1 {
        identity: ProjectionIdentity::new(
            ContractLineage::new("K").unwrap(),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([9; 32]),
        ),
        locator,
        applied_through: CommitSequence::new(3).unwrap(),
        captured_source: ChangelogHistoryStateV3::new(
            ChangelogLineageV3::new(database, 1, LeadershipEpochV1::initial()).unwrap(),
            point(1),
            point(3),
            point(2),
        )
        .unwrap(),
        origin: hash(
            HashDomain::ProjectionSourceOrigin,
            b"structural producing-capacity proof",
        ),
        prior_source_digest: None,
        row_count: 1,
        canonical_row_bytes: 10_000,
        row_root: hash(HashDomain::ProjectionSourceRows, b"structural rows only"),
    })
    .unwrap();
    let initial = ProjectionSourceMaintenanceV1::initial(descriptor.clone(), None).unwrap();
    assert!(encode_projection_source_maintenance_v1(&initial).is_ok());
    let after = ProjectionSourceMaintenanceV1::from_stored_parts(
        descriptor,
        None,
        1,
        10_000,
        Some(vec![0x51; 4079]),
        hash(
            HashDomain::ProjectionSourceCopyProgress,
            b"structural progress only",
        ),
    )
    .unwrap();
    let actual_error = encode_projection_source_maintenance_v1(&after).unwrap_err();
    assert_eq!(actual_error.kind(), DurableCodecErrorKind::LimitExceeded);
    // Existing owner helper yields CorruptData: this is the genuine semantic RED.
    assert_eq!(
        codec(ProjectionSourceRecordDecodeErrorV1::Codec(actual_error)).kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(initial.copied_rows(), 0);
    assert!(initial.last_copied_key().is_none());
}

// req: STO-020, STO-012, PRJ-004
#[test]
fn producing_capacity_refusal_does_not_reclassify_other_durable_failures() {
    for kind in [
        DurableCodecErrorKind::CorruptData,
        DurableCodecErrorKind::IncompatibleFormat,
        DurableCodecErrorKind::UnexpectedRecordType,
        DurableCodecErrorKind::InvariantViolation,
        DurableCodecErrorKind::ReservationExceeded,
    ] {
        assert_eq!(
            codec(ProjectionSourceRecordDecodeErrorV1::Codec(
                DurableCodecError::new(kind)
            ))
            .kind(),
            StorageErrorKind::CorruptData
        );
    }
    assert_eq!(
        codec(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable).kind(),
        StorageErrorKind::Unavailable
    );
}
