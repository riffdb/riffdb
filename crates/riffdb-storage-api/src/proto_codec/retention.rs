//! Checked durable retention watermark, holds, and history tombstone mappings.

use riffdb_proto::storage::v1 as wire;

use crate::{
    EncodedPageItem, HistoryTombstoneContentDigest, HistoryTombstoneHash, RetentionHoldV1,
    RetentionWatermarkHash, StoredHistoryTombstoneV1, StoredRetentionHoldsV1,
    StoredRetentionWatermarkV1,
};

use super::{CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message, fixed};

const WATERMARK: &str = "riffdb.storage.v1.StoredRetentionWatermarkV1";
const HOLDS: &str = "riffdb.storage.v1.StoredRetentionHoldsV1";
const TOMBSTONE: &str = "riffdb.storage.v1.StoredHistoryTombstoneV1";

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
                })
                .collect(),
        },
    )
}

/// Decodes one canonical retention-holds envelope.
pub fn decode_retention_holds_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredRetentionHoldsV1>, DurableCodecError> {
    decode_message::<wire::StoredRetentionHoldsV1, _, _>(HOLDS, encoded, |value| {
        let holds = value
            .holds
            .into_iter()
            .map(|hold| RetentionHoldV1::new(hold.hold_id, hold.sequence, hold.reason))
            .collect::<Result<Vec<_>, _>>()
            .map_err(DurableCodecError::from_storage_value)?;
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
