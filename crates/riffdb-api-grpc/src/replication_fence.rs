//! Checked wire observations. No constructor here authenticates a remote peer.
use riffdb_auth::{ChangelogHistoryPointV3 as Point, ChangelogTransactionSequence as Sequence};
use riffdb_proto::v1;
use riffdb_service::{
    PrimaryFenceRequestV1, PrimaryFenceSourceEvidenceV1 as Evidence, ReplicationFailure as Failure,
    ReplicationStreamErrorV3 as Error,
};
use riffdb_types::{
    DatabaseId, LeadershipEpochV1, ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1,
    ReplicationSourceHoldIdV1, RequestId,
};

pub(crate) fn selection(
    id: &[u8],
    database: DatabaseId,
    incarnation: u64,
    epoch: u64,
    value: &v1::ReplicationFenceEvidenceRequest,
) -> Result<PrimaryFenceRequestV1, Failure> {
    let invalid = || Failure::Source(Error::InvalidPosition);
    Ok(PrimaryFenceRequestV1::new(
        RequestId::from_bytes(id.try_into().map_err(|_| invalid())?).map_err(|_| invalid())?,
        ReplicationFenceOperationId::from_bytes(
            value
                .operation_id
                .as_slice()
                .try_into()
                .map_err(|_| invalid())?,
        )
        .map_err(|_| invalid())?,
        ReplicationFollowerAuditTargetV1::new(
            database,
            incarnation,
            LeadershipEpochV1::new(epoch).ok_or_else(invalid)?,
            ReplicationSourceHoldIdV1::new(
                value.hold_id.as_slice().try_into().map_err(|_| invalid())?,
            )
            .ok_or_else(invalid)?,
        )
        .ok_or_else(invalid)?,
        Sequence::new(value.registration_generation).ok_or_else(invalid)?,
    ))
}
fn point(value: Point) -> v1::ReplicationPosition {
    let frontier = |value: Option<u64>| {
        Some(v1::FrontierPosition {
            position: Some(match value {
                Some(value) => v1::frontier_position::Position::AppliedThrough(value),
                None => v1::frontier_position::Position::BeforeFirst(v1::Unit {}),
            }),
        })
    };
    v1::ReplicationPosition {
        transaction_sequence: value.sequence().get(),
        history_hash: value.history_hash().to_vec(),
        application_frontier: frontier(value.frontier().application().map(|v| v.get())),
        administration_frontier: frontier(value.frontier().administration().map(|v| v.get())),
    }
}
pub(crate) fn encode(value: &Evidence) -> Result<v1::ReplicationFenceEvidence, Failure> {
    let history = value.source_history();
    let result = v1::ReplicationFenceEvidence {
        fence_record: value.encode_fence_record().map_err(|_| corrupt())?,
        applied: Some(point(value.applied())),
        anchor: Some(point(history.anchor())),
        tail: Some(point(history.tail())),
        minimum_resume: Some(point(history.minimum_resume())),
    };
    validate(&result)?;
    Ok(result)
}
fn validate(value: &v1::ReplicationFenceEvidence) -> Result<(), Failure> {
    riffdb_proto::validate_public_message(&v1::StreamChangelogResponse {
        item: Some(v1::stream_changelog_response::Item::FenceEvidence(
            value.clone(),
        )),
        source_head: None,
    })
    .map_err(|_| corrupt())
}
#[cfg(feature = "client")]
pub(crate) fn decode(value: v1::ReplicationFenceEvidence) -> Result<Evidence, Failure> {
    validate(&value)?;
    Evidence::from_fence_record(
        &value.fence_record,
        decode_point(value.applied)?,
        decode_point(value.anchor)?,
        decode_point(value.tail)?,
        decode_point(value.minimum_resume)?,
    )
    .map_err(|_| corrupt())
}
#[cfg(feature = "client")]
fn decode_point(value: Option<v1::ReplicationPosition>) -> Result<Point, Failure> {
    let value = value.ok_or_else(corrupt)?;
    let frontier = |value: Option<v1::FrontierPosition>| match value.and_then(|v| v.position) {
        Some(v1::frontier_position::Position::BeforeFirst(_)) => Ok(None),
        Some(v1::frontier_position::Position::AppliedThrough(v)) if v > 0 => Ok(Some(v)),
        _ => Err(corrupt()),
    };
    Ok(Point::new(
        Sequence::new(value.transaction_sequence).ok_or_else(corrupt)?,
        value
            .history_hash
            .as_slice()
            .try_into()
            .map_err(|_| corrupt())?,
        riffdb_types::DualFrontier::new(
            frontier(value.application_frontier)?
                .map(|v| riffdb_types::CommitSequence::new(v).ok_or_else(corrupt))
                .transpose()?,
            frontier(value.administration_frontier)?
                .map(|v| riffdb_types::AdministrationSequence::new(v).ok_or_else(corrupt))
                .transpose()?,
        ),
    ))
}
fn corrupt() -> Failure {
    Failure::Source(Error::CorruptHistory)
}

#[cfg(all(test, feature = "client"))]
mod tests {
    use super::*;
    use riffdb_types::{AdministrationSequence, CommitSequence, DualFrontier};
    #[test]
    // req: REP-003, REP-005
    fn fence_wire_round_trip_preserves_existing_receipt_and_refuses_damage_or_contradiction() {
        let hex = include_str!("../../../fixtures/replication/primary-fence-v1.hex")
            .lines()
            .find_map(|line| line.strip_prefix("receipt "))
            .unwrap();
        let record: Vec<_> = hex
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let point = |seq, app, admin| {
            Point::new(
                Sequence::new(seq).unwrap(),
                [9; 32],
                DualFrontier::new(CommitSequence::new(app), AdministrationSequence::new(admin)),
            )
        };
        let anchor = point(1, 0, 0);
        let evidence =
            Evidence::from_fence_record(&record, point(8, 3, 2), anchor, point(10, 5, 3), anchor)
                .unwrap();
        let encoded = encode(&evidence).unwrap();
        assert_eq!(encoded.fence_record, record);
        assert_eq!(decode(encoded.clone()).unwrap(), evidence);
        assert_eq!(evidence.application_rpo(), 2);
        for length in 0..record.len() {
            let mut damaged = encoded.clone();
            damaged.fence_record.truncate(length);
            assert!(decode(damaged).is_err());
        }
        for change in 0..5 {
            let mut damaged = encoded.clone();
            match change {
                0 => *damaged.fence_record.last_mut().unwrap() ^= 1,
                1 => damaged.fence_record.push(0),
                2 => damaged.applied = Some(super::point(point(8, 6, 2))),
                3 => damaged.tail = Some(super::point(point(10, 6, 3))),
                _ => damaged.minimum_resume = Some(super::point(point(9, 5, 2))),
            }
            assert!(decode(damaged).is_err());
        }
    }
}
