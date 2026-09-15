//! Wire-only conversion of non-authoritative source-head observations.
use riffdb_proto::v1;
use riffdb_service::ReplicationSourceHead;
#[cfg(feature = "client")]
use riffdb_service::{ReplicationFailure, ReplicationStreamErrorV3};
#[cfg(feature = "client")]
use riffdb_types::{AdministrationSequence, CommitSequence, DualFrontier};

pub(crate) fn encode(head: ReplicationSourceHead) -> v1::ReplicationSourceHead {
    v1::ReplicationSourceHead {
        transaction_sequence: head.transaction_sequence(),
        application_frontier: frontier(head.frontier().application().map(|v| v.get())),
        administration_frontier: frontier(head.frontier().administration().map(|v| v.get())),
    }
}

pub(crate) fn encode_statistics(
    value: riffdb_service::ReplicationStatistics,
) -> v1::ReplicationStatistics {
    fn pair(value: riffdb_types::DualFrontier) -> v1::ReplicationFrontier {
        v1::ReplicationFrontier {
            application: frontier(value.application().map(|v| v.get())),
            administration: frontier(value.administration().map(|v| v.get())),
        }
    }
    v1::ReplicationStatistics {
        role: match value.role() {
            riffdb_service::ReplicationRole::Primary => v1::ReplicationRole::Primary,
            riffdb_service::ReplicationRole::Follower => v1::ReplicationRole::Follower,
        } as i32,
        source_frontier: value.source_frontier().map(pair),
        applied_frontier: Some(pair(value.applied_frontier())),
        acknowledged_frontier: value.acknowledged_frontier().map(pair),
        registered_followers: value.registered_followers(),
        application_lag_sequences: value.application_lag_sequences(),
        administration_lag_sequences: value.administration_lag_sequences(),
    }
}

pub(crate) fn frontier(value: Option<u64>) -> Option<v1::FrontierPosition> {
    Some(v1::FrontierPosition {
        position: Some(match value {
            None => v1::frontier_position::Position::BeforeFirst(v1::Unit {}),
            Some(value) => v1::frontier_position::Position::AppliedThrough(value),
        }),
    })
}

#[cfg(feature = "client")]
pub(crate) fn decode(
    head: v1::ReplicationSourceHead,
) -> Result<ReplicationSourceHead, ReplicationFailure> {
    let invalid = || ReplicationFailure::Source(ReplicationStreamErrorV3::CorruptHistory);
    let read =
        |frontier: Option<v1::FrontierPosition>| match frontier.and_then(|value| value.position) {
            Some(v1::frontier_position::Position::BeforeFirst(_)) => Ok(None),
            Some(v1::frontier_position::Position::AppliedThrough(value)) if value > 0 => {
                Ok(Some(value))
            }
            _ => Err(invalid()),
        };
    let frontier = DualFrontier::new(
        read(head.application_frontier)?
            .map(|v| CommitSequence::new(v).ok_or_else(invalid))
            .transpose()?,
        read(head.administration_frontier)?
            .map(|v| AdministrationSequence::new(v).ok_or_else(invalid))
            .transpose()?,
    );
    ReplicationSourceHead::new(head.transaction_sequence, frontier).ok_or_else(invalid)
}
