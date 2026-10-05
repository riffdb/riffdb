//! Bounded deletion under the parent's drained lease and checkpoint evidence.
//! No surviving receipt or authoritative row is rewritten.

use super::*;
use riffdb_storage_api::{
    ChangelogHistoryPointV3 as Point,
    proto_codec::{decode_replication_source_hold, encode_changelog_history_state_v3},
};

const MAX_RECLAIMED_RECEIPTS: u64 = 256;

pub(super) fn floor(
    root: &crate::checkpoint_root::CheckpointRoot,
    history: History,
    prior_checkpoint: Point,
    derived_floor: Option<u64>,
) -> Result<(Option<Point>, [u64; 8]), StorageError> {
    // The barrier completely validated the bounded holds and retained chain.
    let mut maximum = prior_checkpoint.sequence().get();
    if let Some(derived_floor) = derived_floor {
        maximum = maximum.min(derived_floor);
    }
    let holds = root.open_table(SOURCE_HOLDS).map_err(table_error)?;
    let mut hold_floor: Option<u64> = None;
    let mut hold_count = 0_u64;
    for row in holds.iter().map_err(precommit_storage_error)? {
        let (_, value) = row.map_err(precommit_storage_error)?;
        let state = *decode_replication_source_hold(value.value())
            .map_err(crate::error::codec_error)?
            .value();
        let hold = match state {
            riffdb_storage_api::ReplicationSourceHoldStateV1::Legacy(hold) => hold,
            riffdb_storage_api::ReplicationSourceHoldStateV1::Registered(policy) => {
                if policy.phase() == riffdb_storage_api::FollowerRegistrationPhaseV1::Retired {
                    continue;
                }
                policy.hold()
            }
        };
        hold_count = hold_count.saturating_add(1);
        hold_floor = Some(hold_floor.map_or(hold.fence().sequence().get(), |prior| {
            prior.min(hold.fence().sequence().get())
        }));
        maximum = maximum.min(hold.fence().sequence().get());
    }
    let first = history.minimum_resume().sequence().get();
    // Planning-only numbers from this exact barrier. Optional fences have
    // explicit presence bits; zero never stands for a missing protected fence.
    let observation = [
        first,
        history.tail().sequence().get(),
        prior_checkpoint.sequence().get(),
        derived_floor.unwrap_or(0),
        hold_floor.unwrap_or(0),
        maximum,
        hold_count,
        u64::from(derived_floor.is_some()) | (u64::from(hold_floor.is_some()) << 1),
    ];
    maximum = maximum.min(first.saturating_add(MAX_RECLAIMED_RECEIPTS));
    if maximum <= first {
        return Ok((None, observation));
    }
    let table = root.open_table(HISTORY).map_err(table_error)?;
    let row = table
        .get(maximum.to_be_bytes().as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let receipt = AuthoritativeTransactionV3::decode(&crate::history_stub::resolve_history_value(
        row.value(),
    )?)
    .map_err(value_error)?;
    Point::from_receipt(&receipt)
        .map(|point| (Some(point), observation))
        .map_err(value_error)
}

pub(super) fn stage(
    transaction: &WriteTransaction,
    validated: &crate::checkpoint_root::CheckpointRoot,
    predecessor: History,
    successor: History,
    floor: Point,
    receipt: PreparedHistoryAdvance,
) -> Result<(), StorageError> {
    let first = predecessor.minimum_resume().sequence().get();
    let end = floor.sequence().get();
    if end <= first || end - first > MAX_RECLAIMED_RECEIPTS {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let retained = History::new(
        successor.lineage(),
        successor.anchor(),
        successor.tail(),
        floor,
    )
    .map_err(value_error)?;
    let encoded = encode_changelog_history_state_v3(retained).map_err(crate::error::codec_error)?;
    {
        let prior = validated.open_table(HISTORY).map_err(table_error)?;
        let mut table = transaction.open_table(HISTORY).map_err(table_error)?;
        for sequence in first..end {
            let key = sequence.to_be_bytes();
            let expected = prior
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let removed = table
                .remove(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            if removed.value() != expected.value() {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
    }
    crash_edge("reclamation-deleted");
    receipt.stage(transaction)?;
    transaction
        .open_table(META)
        .map_err(table_error)?
        .insert(
            N::ChangelogHistoryState
                .metadata_key()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
            encoded.as_bytes(),
        )
        .map_err(precommit_storage_error)?;
    Ok(())
}
