//! Archive codec retains one event semantic owner and strict envelope validation.

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, decode_durable_event_v1, encode_durable_event_v1,
};
use crate::{EncodedContentCharge, EncodedPageItem, LocalOutboxArchiveRecordV1};
use prost::Message;
use riffdb_proto::storage::v1 as wire;
use riffdb_proto::{
    local_outbox_archive_record_registry_v1, local_outbox_archive_record_schema_v1,
};
use riffdb_types::DatabaseId;
use std::num::NonZeroU64;

/// Generated destination-format fingerprint, separate from primary storage.
#[must_use]
pub fn local_outbox_archive_schema_hash_v1() -> riffdb_types::SchemaHash {
    local_outbox_archive_record_schema_v1().schema_hash()
}

/// Canonically encodes the destination record without entering the primary registry.
pub fn encode_local_outbox_archive_record_v1(
    value: &LocalOutboxArchiveRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let wire = wire::LocalOutboxArchiveRecordV1 {
        database_id: value.database_id().as_bytes().to_vec(),
        history_incarnation: value.history_incarnation().get(),
        event_envelope: encode_durable_event_v1(value.event())?.into_bytes(),
    };
    let bytes = riffdb_proto::envelope::encode(
        local_outbox_archive_record_schema_v1(),
        &wire.encode_to_vec(),
    )
    .map_err(DurableCodecError::from_encode_envelope)?;
    let charge = EncodedContentCharge::new(bytes.len()).ok_or_else(DurableCodecError::invariant)?;
    Ok(CanonicalStoredEnvelopeV1 { bytes, charge })
}

/// Validates the archive envelope, identity, event hash, and canonical payload.
pub fn decode_local_outbox_archive_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<LocalOutboxArchiveRecordV1>, DurableCodecError> {
    let registry =
        local_outbox_archive_record_registry_v1().map_err(|_| DurableCodecError::invariant())?;
    let envelope = registry
        .decode(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let wire = wire::LocalOutboxArchiveRecordV1::decode(envelope.payload())
        .map_err(|_| DurableCodecError::corrupt())?;
    let database_bytes: [u8; 16] = wire
        .database_id
        .as_slice()
        .try_into()
        .map_err(|_| DurableCodecError::corrupt())?;
    let database =
        DatabaseId::from_bytes(database_bytes).map_err(|_| DurableCodecError::corrupt())?;
    let incarnation =
        NonZeroU64::new(wire.history_incarnation).ok_or_else(DurableCodecError::corrupt)?;
    let event = decode_durable_event_v1(&wire.event_envelope)?
        .into_parts()
        .0;
    let value = LocalOutboxArchiveRecordV1::new(database, incarnation, event);
    let charge = EncodedContentCharge::new(encoded.len()).ok_or_else(DurableCodecError::corrupt)?;
    Ok(EncodedPageItem::new(value, charge))
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{CanonicalRecord, CommitSequence, EventId, EventTypeId};

    fn record() -> LocalOutboxArchiveRecordV1 {
        let id = EventId::new(CommitSequence::first(), 0);
        let kind = EventTypeId::try_from(1).unwrap();
        let payload = CanonicalRecord::new(vec![]).unwrap();
        let hash = crate::derive_event_hash_v1(id, kind, &payload).unwrap();
        LocalOutboxArchiveRecordV1::new(
            DatabaseId::from_unix_milliseconds_and_random(42, [1; 10]).unwrap(),
            NonZeroU64::MIN,
            crate::StoredDurableEventV1::new(id, kind, payload, hash).unwrap(),
        )
    }

    // req: STO-020, REC-001
    #[test]
    fn archive_roundtrip_is_canonical_checked_and_separate_from_primary_records() {
        let value = record();
        let encoded = encode_local_outbox_archive_record_v1(&value).unwrap();
        let fixture = include_str!("../../fixtures/local-outbox-archive-v1.hex").trim();
        let bytes = fixture
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(encoded.as_bytes(), bytes);
        let (decoded, charge) = decode_local_outbox_archive_record_v1(encoded.as_bytes())
            .unwrap()
            .into_parts();
        assert_eq!(decoded, value);
        assert_eq!(
            encode_local_outbox_archive_record_v1(&decoded)
                .unwrap()
                .as_bytes(),
            encoded.as_bytes()
        );
        assert_eq!(charge, encoded.encoded_content_charge());
        let primary = riffdb_proto::durable::readable_record_registry();
        assert!(primary.decode(encoded.as_bytes()).is_err());
        let mut corrupt = encoded.as_bytes().to_vec();
        let end = corrupt.len() - 1;
        corrupt[end] ^= 1;
        assert!(decode_local_outbox_archive_record_v1(&corrupt).is_err());
    }

    // req: STO-020
    #[test]
    fn archive_rejects_zero_incarnation_invalid_nested_event_and_unknown_wire_fields() {
        let value = record();
        let mut wire = wire::LocalOutboxArchiveRecordV1 {
            database_id: value.database_id().as_bytes().to_vec(),
            history_incarnation: 0,
            event_envelope: encode_durable_event_v1(value.event()).unwrap().into_bytes(),
        };
        let encode = |payload: &[u8]| {
            riffdb_proto::envelope::encode(local_outbox_archive_record_schema_v1(), payload)
        };
        let zero = encode(&wire.encode_to_vec()).unwrap();
        assert!(decode_local_outbox_archive_record_v1(&zero).is_err());
        wire.history_incarnation = 1;
        wire.event_envelope = vec![1, 2, 3];
        let nested = encode(&wire.encode_to_vec()).unwrap();
        assert!(decode_local_outbox_archive_record_v1(&nested).is_err());
        let mut unknown = wire.encode_to_vec();
        unknown.extend_from_slice(&[32, 1]);
        assert!(encode(&unknown).is_err());
    }
}
