//! Bounded lineage-shared roots for the closed V3 authority catalog.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

use crate::{
    AuthoritativeStateCatalogV1, ChangelogHistoryPointV3, ChangelogHistoryStateV3,
    ChangelogLineageV3, ChangelogTransactionSequence, EncodedPageItem, LeadershipEpochV1,
    ReplicationFollowerStateV3, ReplicationSourceHoldIdV1, ReplicationSourceHoldKindV1,
    ReplicationSourceHoldV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, DurableCodecErrorKind, decode_message,
    encode_message, fixed, require,
};

const CATALOG: &str = "riffdb.storage.v1.StoredAuthoritativeStateCatalogV1";
const LEADERSHIP: &str = "riffdb.storage.v1.StoredLeadershipEpochV1";
const HISTORY: &str = "riffdb.storage.v1.StoredChangelogHistoryStateV3";
const FOLLOWER: &str = "riffdb.storage.v1.StoredReplicationFollowerStateV3";
const SOURCE_HOLD: &str = "riffdb.storage.v1.StoredReplicationSourceHoldV1";

/// Encodes one source-only fence, not a reclamation or acknowledgement permit.
pub fn encode_replication_source_hold_v1(
    hold: ReplicationSourceHoldV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(SOURCE_HOLD, &source_hold_to_wire(hold))
}

fn source_hold_to_wire(hold: ReplicationSourceHoldV1) -> wire::StoredReplicationSourceHoldV1 {
    let lineage = hold.lineage();
    wire::StoredReplicationSourceHoldV1 {
        kind: hold.kind() as i32,
        hold_id: hold.id().as_bytes().to_vec(),
        database_id: lineage.database_id().as_bytes().to_vec(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        catalog_digest: lineage.catalog_digest().to_vec(),
        fence: Some(encode_position(hold.fence())),
    }
}

/// Refuses unknown owners, zero IDs, malformed positions and foreign catalogs.
/// The storage owner must additionally validate the key and exact retained fence.
pub fn decode_replication_source_hold_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<ReplicationSourceHoldV1>, DurableCodecError> {
    decode_message::<wire::StoredReplicationSourceHoldV1, _, _>(
        SOURCE_HOLD,
        encoded,
        source_hold_from_wire,
    )
}

fn source_hold_from_wire(
    v: wire::StoredReplicationSourceHoldV1,
) -> Result<ReplicationSourceHoldV1, DurableCodecError> {
    let kind = match v.kind {
        1 => ReplicationSourceHoldKindV1::FollowerAcknowledgement,
        2 => ReplicationSourceHoldKindV1::ArchiveAcknowledgement,
        3 => ReplicationSourceHoldKindV1::Bootstrap,
        _ => return Err(DurableCodecError::corrupt()),
    };
    Ok(ReplicationSourceHoldV1::new(
        ReplicationSourceHoldIdV1::new(fixed(v.hold_id)?).ok_or_else(DurableCodecError::corrupt)?,
        kind,
        decode_lineage(
            v.database_id,
            v.history_incarnation,
            v.leadership_epoch,
            v.catalog_digest,
        )?,
        decode_position(require(v.fence)?)?,
    ))
}

mod administration;
pub use administration::*;
mod source_hold_v2;
pub use source_hold_v2::*;

/// Encodes only the storage owner's closed catalog; there is no caller-selected inventory.
pub fn encode_authoritative_state_catalog_v1(
    catalog: AuthoritativeStateCatalogV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        CATALOG,
        &wire::StoredAuthoritativeStateCatalogV1 {
            catalog_digest: catalog.digest().to_vec(),
        },
    )
}

/// Refuses foreign catalogs without exposing their digest or accepting partial authority.
pub fn decode_authoritative_state_catalog_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<AuthoritativeStateCatalogV1>, DurableCodecError> {
    decode_message::<wire::StoredAuthoritativeStateCatalogV1, _, _>(CATALOG, encoded, |value| {
        let digest: [u8; 32] = fixed(value.catalog_digest)?;
        if digest != AuthoritativeStateCatalogV1.digest() {
            return Err(DurableCodecError::new(
                DurableCodecErrorKind::IncompatibleFormat,
            ));
        }
        Ok(AuthoritativeStateCatalogV1)
    })
}

/// Encodes a nonzero leadership fence. Only the coordinator may persist advancement.
pub fn encode_leadership_epoch_v1(
    epoch: LeadershipEpochV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        LEADERSHIP,
        &wire::StoredLeadershipEpochV1 { epoch: epoch.get() },
    )
}

/// Decodes the exact leadership role, never a transaction or history-incarnation counter.
pub fn decode_leadership_epoch_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<LeadershipEpochV1>, DurableCodecError> {
    decode_message::<wire::StoredLeadershipEpochV1, _, _>(LEADERSHIP, encoded, |value| {
        LeadershipEpochV1::new(value.epoch).ok_or_else(DurableCodecError::corrupt)
    })
}

fn encode_position(
    point: ChangelogHistoryPointV3,
) -> wire::stored_changelog_history_state_v3::Position {
    wire::stored_changelog_history_state_v3::Position {
        transaction_sequence: point.sequence().get(),
        history_hash: point.history_hash().to_vec(),
        application_sequence: point.frontier().application().map_or(0, |s| s.get()),
        administration_sequence: point.frontier().administration().map_or(0, |s| s.get()),
    }
}

fn decode_position(
    point: wire::stored_changelog_history_state_v3::Position,
) -> Result<ChangelogHistoryPointV3, DurableCodecError> {
    Ok(ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(point.transaction_sequence)
            .ok_or_else(DurableCodecError::corrupt)?,
        fixed(point.history_hash)?,
        DualFrontier::new(
            CommitSequence::new(point.application_sequence),
            AdministrationSequence::new(point.administration_sequence),
        ),
    ))
}

fn decode_lineage(
    database: Vec<u8>,
    incarnation: u64,
    epoch: u64,
    catalog: Vec<u8>,
) -> Result<ChangelogLineageV3, DurableCodecError> {
    let digest: [u8; 32] = fixed(catalog)?;
    if digest != AuthoritativeStateCatalogV1.digest() {
        return Err(DurableCodecError::new(
            DurableCodecErrorKind::IncompatibleFormat,
        ));
    }
    ChangelogLineageV3::new(
        DatabaseId::from_bytes(fixed(database)?).map_err(|_| DurableCodecError::corrupt())?,
        incarnation,
        LeadershipEpochV1::new(epoch).ok_or_else(DurableCodecError::corrupt)?,
    )
    .map_err(|_| DurableCodecError::corrupt())
}

/// Encodes bounded lineage-shared roots with their distinct current durable role.
pub fn encode_changelog_history_state_v3(
    history: ChangelogHistoryStateV3,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let lineage = history.lineage();
    encode_message(
        HISTORY,
        &wire::StoredChangelogHistoryStateV3 {
            database_id: lineage.database_id().as_bytes().to_vec(),
            history_incarnation: lineage.history_incarnation(),
            leadership_epoch: lineage.leadership_epoch().get(),
            catalog_digest: lineage.catalog_digest().to_vec(),
            anchor: Some(encode_position(history.anchor())),
            tail: Some(encode_position(history.tail())),
            minimum_resume: Some(encode_position(history.minimum_resume())),
        },
    )
}

/// Refuses missing roots, foreign catalogs, regressing or substituted positions.
/// Successful decode alone cannot certify receipt ancestry or permit reclamation.
pub fn decode_changelog_history_state_v3(
    encoded: &[u8],
) -> Result<EncodedPageItem<ChangelogHistoryStateV3>, DurableCodecError> {
    decode_message::<wire::StoredChangelogHistoryStateV3, _, _>(HISTORY, encoded, |v| {
        ChangelogHistoryStateV3::new(
            decode_lineage(
                v.database_id,
                v.history_incarnation,
                v.leadership_epoch,
                v.catalog_digest,
            )?,
            decode_position(require(v.anchor)?)?,
            decode_position(require(v.tail)?)?,
            decode_position(require(v.minimum_resume)?)?,
        )
        .map_err(|_| DurableCodecError::corrupt())
    })
}

/// Encodes explicit detached state or exact locally durable follower progress.
pub fn encode_replication_follower_state_v3(
    follower: ReplicationFollowerStateV3,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    use wire::stored_replication_follower_state_v3::{Attached, Detached, State};
    let state = match follower.attached_state() {
        None => State::Detached(Detached {}),
        Some((lineage, applied, acknowledged)) => State::Attached(Attached {
            database_id: lineage.database_id().as_bytes().to_vec(),
            history_incarnation: lineage.history_incarnation(),
            leadership_epoch: lineage.leadership_epoch().get(),
            catalog_digest: lineage.catalog_digest().to_vec(),
            applied: Some(encode_position(applied)),
            acknowledged: acknowledged.map(encode_position),
        }),
    };
    encode_message(
        FOLLOWER,
        &wire::StoredReplicationFollowerStateV3 { state: Some(state) },
    )
}

/// Never interprets missing/unknown state as detached, or acknowledgement as apply.
pub fn decode_replication_follower_state_v3(
    encoded: &[u8],
) -> Result<EncodedPageItem<ReplicationFollowerStateV3>, DurableCodecError> {
    use wire::stored_replication_follower_state_v3::State;
    decode_message::<wire::StoredReplicationFollowerStateV3, _, _>(FOLLOWER, encoded, |v| {
        match require(v.state)? {
            State::Detached(_) => Ok(ReplicationFollowerStateV3::detached()),
            State::Attached(v) => ReplicationFollowerStateV3::attached(
                decode_lineage(
                    v.database_id,
                    v.history_incarnation,
                    v.leadership_epoch,
                    v.catalog_digest,
                )?,
                decode_position(require(v.applied)?)?,
                v.acknowledged.map(decode_position).transpose()?,
            )
            .map_err(|_| DurableCodecError::corrupt()),
        }
    })
}
