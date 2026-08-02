//! Checked durable retention watermark, holds, tombstone, and administration mappings.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{AdministrationSequence, ProjectionId, SchemaHash};

use crate::{
    EncodedPageItem, HistoryTombstoneContentDigest, HistoryTombstoneHash,
    RetentionAdministrationAction, RetentionHoldKind, RetentionHoldV1, RetentionWatermarkHash,
    StoredHistoryTombstoneV1, StoredRetentionAdministrationV1, StoredRetentionHoldsV1,
    StoredRetentionWatermarkV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, common::timestamp_from_proto,
    common::timestamp_to_proto, decode_message, encode_message, fixed,
};

const WATERMARK: &str = "riffdb.storage.v1.StoredRetentionWatermarkV1";
const HOLDS: &str = "riffdb.storage.v1.StoredRetentionHoldsV1";
const TOMBSTONE: &str = "riffdb.storage.v1.StoredHistoryTombstoneV1";
const RETENTION_ADMINISTRATION: &str = "riffdb.storage.v1.StoredRetentionAdministrationV1";

/// Encodes one canonical retention-watermark envelope.
pub fn encode_retention_watermark_v1(
    value: &StoredRetentionWatermarkV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if value
        .computed_hash()
        .map_err(DurableCodecError::from_storage_value)?
        != value.watermark_hash()
    {
        return Err(DurableCodecError::corrupt());
    }
    encode_message(
        WATERMARK,
        &wire::StoredRetentionWatermarkV1 {
            watermark_sequence: value.watermark_sequence(),
            history_incarnation: value.history_incarnation(),
            watermark_hash: value.watermark_hash().as_bytes().to_vec(),
            chain_root_registry_digest: value
                .chain_root_registry_digest()
                .map(|digest| digest.as_bytes().to_vec()),
        },
    )
}

/// Decodes one canonical retention-watermark envelope.
pub fn decode_retention_watermark_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredRetentionWatermarkV1>, DurableCodecError> {
    decode_message::<wire::StoredRetentionWatermarkV1, _, _>(WATERMARK, encoded, |value| {
        StoredRetentionWatermarkV1::from_stored_parts(
            value.watermark_sequence,
            value.history_incarnation,
            RetentionWatermarkHash::from_bytes(fixed(value.watermark_hash)?),
            value
                .chain_root_registry_digest
                .map(|digest| fixed(digest).map(SchemaHash::from_bytes))
                .transpose()?,
        )
        .map_err(DurableCodecError::from_storage_value)
    })
}

/// Encodes one canonical retention-holds envelope.
pub fn encode_retention_holds_v1(
    value: &StoredRetentionHoldsV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        HOLDS,
        &wire::StoredRetentionHoldsV1 {
            holds: value
                .holds()
                .iter()
                .map(|hold| wire::RetentionHoldV1 {
                    hold_id: hold.hold_id().to_owned(),
                    sequence: hold.sequence(),
                    reason: hold.reason().to_owned(),
                    kind: hold_kind_to_proto(hold.kind()),
                })
                .collect(),
        },
    )
}

const fn hold_kind_to_proto(value: RetentionHoldKind) -> i32 {
    match value {
        RetentionHoldKind::Operator => wire::RetentionHoldKindV1::Operator as i32,
        RetentionHoldKind::ProjectionDetach => wire::RetentionHoldKindV1::ProjectionDetach as i32,
    }
}

fn hold_kind_from_proto(value: i32) -> Result<RetentionHoldKind, DurableCodecError> {
    match wire::RetentionHoldKindV1::try_from(value).map_err(|_| DurableCodecError::corrupt())? {
        wire::RetentionHoldKindV1::Operator => Ok(RetentionHoldKind::Operator),
        wire::RetentionHoldKindV1::ProjectionDetach => Ok(RetentionHoldKind::ProjectionDetach),
        wire::RetentionHoldKindV1::Unspecified => Err(DurableCodecError::corrupt()),
    }
}

/// Decodes one canonical retention-holds envelope.
pub fn decode_retention_holds_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredRetentionHoldsV1>, DurableCodecError> {
    decode_message::<wire::StoredRetentionHoldsV1, _, _>(HOLDS, encoded, |value| {
        let holds = value
            .holds
            .into_iter()
            .map(|hold| {
                let kind = hold_kind_from_proto(hold.kind)?;
                RetentionHoldV1::from_stored_parts(hold.hold_id, hold.sequence, hold.reason, kind)
                    .map_err(DurableCodecError::from_storage_value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        StoredRetentionHoldsV1::new(holds).map_err(DurableCodecError::from_storage_value)
    })
}

/// Encodes one canonical history-tombstone envelope.
pub fn encode_history_tombstone_v1(
    value: &StoredHistoryTombstoneV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if value
        .computed_hash()
        .map_err(DurableCodecError::from_storage_value)?
        != value.tombstone_hash()
    {
        return Err(DurableCodecError::corrupt());
    }
    encode_message(
        TOMBSTONE,
        &wire::StoredHistoryTombstoneV1 {
            first_sequence: value.first_sequence(),
            last_sequence: value.last_sequence(),
            commits_count: value.commits_count(),
            events_count: value.events_count(),
            outbox_count: value.outbox_count(),
            outbox_status_count: value.outbox_status_count(),
            content_digest: value.content_digest().as_bytes().to_vec(),
            previous_tombstone_hash: value
                .previous_tombstone_hash()
                .map(|hash| hash.as_bytes().to_vec()),
            tombstone_hash: value.tombstone_hash().as_bytes().to_vec(),
            history_incarnation: value.history_incarnation(),
        },
    )
}

/// Encodes one canonical retention-administration envelope.
pub fn encode_retention_administration_v1(
    value: &StoredRetentionAdministrationV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let action = match value.action() {
        RetentionAdministrationAction::ProjectionDetach => {
            wire::RetentionAdministrationActionV1::ProjectionDetach
        }
        RetentionAdministrationAction::ProjectionReattach => {
            wire::RetentionAdministrationActionV1::ProjectionReattach
        }
    };
    encode_message(
        RETENTION_ADMINISTRATION,
        &wire::StoredRetentionAdministrationV1 {
            administration_sequence: value.administration_sequence().get(),
            action: action as i32,
            projection_id: u64::from(value.projection_id().get()),
            reason: value.reason().to_owned(),
            timestamp: Some(timestamp_to_proto(value.timestamp())),
        },
    )
}

/// Decodes one canonical retention-administration envelope.
pub fn decode_retention_administration_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredRetentionAdministrationV1>, DurableCodecError> {
    decode_message::<wire::StoredRetentionAdministrationV1, _, _>(
        RETENTION_ADMINISTRATION,
        encoded,
        |value| {
            let action = match wire::RetentionAdministrationActionV1::try_from(value.action)
                .map_err(|_| DurableCodecError::corrupt())?
            {
                wire::RetentionAdministrationActionV1::ProjectionDetach => {
                    RetentionAdministrationAction::ProjectionDetach
                }
                wire::RetentionAdministrationActionV1::ProjectionReattach => {
                    RetentionAdministrationAction::ProjectionReattach
                }
                wire::RetentionAdministrationActionV1::Unspecified => {
                    return Err(DurableCodecError::corrupt());
                }
            };
            let sequence = AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?;
            let projection_id = u32::try_from(value.projection_id)
                .ok()
                .and_then(ProjectionId::new)
                .ok_or_else(DurableCodecError::corrupt)?;
            let timestamp =
                timestamp_from_proto(value.timestamp.ok_or_else(DurableCodecError::corrupt)?)?;
            StoredRetentionAdministrationV1::new(
                sequence,
                action,
                projection_id,
                value.reason,
                timestamp,
            )
            .map_err(DurableCodecError::from_storage_value)
        },
    )
}

/// Decodes one canonical history-tombstone envelope.
pub fn decode_history_tombstone_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredHistoryTombstoneV1>, DurableCodecError> {
    decode_message::<wire::StoredHistoryTombstoneV1, _, _>(TOMBSTONE, encoded, |value| {
        StoredHistoryTombstoneV1::from_stored_parts(
            value.first_sequence,
            value.last_sequence,
            value.commits_count,
            value.events_count,
            value.outbox_count,
            value.outbox_status_count,
            HistoryTombstoneContentDigest::from_bytes(fixed(value.content_digest)?),
            value
                .previous_tombstone_hash
                .map(|hash| fixed(hash).map(HistoryTombstoneHash::from_bytes))
                .transpose()?,
            HistoryTombstoneHash::from_bytes(fixed(value.tombstone_hash)?),
            value.history_incarnation,
        )
        .map_err(DurableCodecError::from_storage_value)
    })
}
