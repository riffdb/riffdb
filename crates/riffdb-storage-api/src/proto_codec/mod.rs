//! One-way checked mappings between semantic storage DTOs and durable Protobuf.

mod application;
mod application_installation;
mod audit;
mod bounds;
mod capability;
mod catalog;
mod command_capsule;
mod command_segment;
mod common;
mod consumer;
mod contract_migration;
mod error;
mod metadata;
mod outbox;
mod projection;
mod retention;
mod validated_prefix_checkpoint;

use std::fmt;

use prost::Message;
use riffdb_proto::durable::{current_record_schema, readable_record_registry};

use crate::{EncodedContentCharge, EncodedPageItem};

use common::*;

pub use application::*;
pub use application_installation::*;
pub use audit::*;
pub use bounds::*;
pub use capability::*;
pub use catalog::*;
pub use command_capsule::*;
pub use command_segment::*;
pub use consumer::*;
pub use contract_migration::*;
pub use error::*;
pub use metadata::*;
pub use outbox::*;
pub use projection::*;
pub use retention::*;
pub use validated_prefix_checkpoint::*;

/// One complete canonical v1 durable envelope and its exact byte charge.
#[derive(Clone, Eq, PartialEq)]
pub struct CanonicalStoredEnvelopeV1 {
    bytes: Vec<u8>,
    charge: EncodedContentCharge,
}

impl CanonicalStoredEnvelopeV1 {
    /// Borrows the complete canonical envelope bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consumes the wrapper and returns the complete canonical envelope bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Returns the exact complete-envelope byte charge.
    #[must_use]
    pub const fn encoded_content_charge(&self) -> EncodedContentCharge {
        self.charge
    }
}

impl fmt::Debug for CanonicalStoredEnvelopeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalStoredEnvelopeV1")
            .field("bytes", &"[REDACTED]")
            .field("charge", &self.charge)
            .finish()
    }
}

/// Validates a readable V1 or V2 durable record and returns its exact canonical
/// compact V2 representation. Equal input/output bytes mean no rewrite is
/// required.
pub fn transcode_durable_record_to_v2(
    encoded: &[u8],
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let bytes = readable_record_registry()
        .transcode_to_v2(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let charge = EncodedContentCharge::new(bytes.len()).ok_or_else(DurableCodecError::corrupt)?;
    Ok(CanonicalStoredEnvelopeV1 { bytes, charge })
}

/// Produces immutable legacy V1 compatibility framing after fully validating
/// one readable durable record.
pub fn transcode_durable_record_to_v1(
    encoded: &[u8],
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let bytes = readable_record_registry()
        .transcode_to_v1(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let charge = EncodedContentCharge::new(bytes.len()).ok_or_else(DurableCodecError::corrupt)?;
    Ok(CanonicalStoredEnvelopeV1 { bytes, charge })
}

pub(super) fn encode_message<M: Message + riffdb_proto::durable::WritableRecordMessage>(
    record_type: &'static str,
    message: &M,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let schema = current_record_schema(record_type).ok_or_else(DurableCodecError::invariant)?;
    if M::record_schema().record_type() != schema.record_type() {
        return Err(DurableCodecError::invariant());
    }
    let bytes = riffdb_proto::durable::encode_current_message(message)
        .map_err(DurableCodecError::from_encode_envelope)?;
    let charge = EncodedContentCharge::new(bytes.len()).ok_or_else(DurableCodecError::invariant)?;
    Ok(CanonicalStoredEnvelopeV1 { bytes, charge })
}

/// Frames bytes emitted by a checked first-party structural encoder.
///
/// Callers must already have proved the generated durable-wire shape and
/// bounds by construction. The compact registry identity, exact outer bound,
/// and CRC remain checked by `riffdb-proto`.
pub(super) fn encode_structurally_proven_message<
    M: riffdb_proto::durable::WritableRecordMessage,
>(
    record_type: &'static str,
    payload: &[u8],
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let schema = current_record_schema(record_type).ok_or_else(DurableCodecError::invariant)?;
    if M::record_schema().record_type() != schema.record_type() {
        return Err(DurableCodecError::invariant());
    }
    let bytes = riffdb_proto::durable::encode_current_payload_after_structural_proof::<M>(payload)
        .map_err(DurableCodecError::from_encode_envelope)?;
    let charge = EncodedContentCharge::new(bytes.len()).ok_or_else(DurableCodecError::invariant)?;
    Ok(CanonicalStoredEnvelopeV1 { bytes, charge })
}

#[cfg(test)]
pub(super) fn encode_legacy_message<M: Message>(
    record_type: &'static str,
    message: &M,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let schema = riffdb_proto::durable::readable_record_schema(record_type)
        .ok_or_else(DurableCodecError::invariant)?;
    let bytes = riffdb_proto::envelope::encode_v1(schema, &message.encode_to_vec())
        .map_err(DurableCodecError::from_encode_envelope)?;
    let charge = EncodedContentCharge::new(bytes.len()).ok_or_else(DurableCodecError::invariant)?;
    Ok(CanonicalStoredEnvelopeV1 { bytes, charge })
}

#[cfg(any(test, feature = "test-fixtures"))]
pub(super) fn encode_readable_compact_message<M: Message>(
    record_type: &'static str,
    message: &M,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let schema = riffdb_proto::durable::readable_record_schema(record_type)
        .ok_or_else(DurableCodecError::invariant)?;
    let bytes = riffdb_proto::envelope::encode(schema, &message.encode_to_vec())
        .map_err(DurableCodecError::from_encode_envelope)?;
    let charge = EncodedContentCharge::new(bytes.len()).ok_or_else(DurableCodecError::invariant)?;
    Ok(CanonicalStoredEnvelopeV1 { bytes, charge })
}

pub(super) fn decode_message<M, T, F>(
    record_type: &'static str,
    encoded: &[u8],
    reconstruct: F,
) -> Result<EncodedPageItem<T>, DurableCodecError>
where
    M: Message + Default + riffdb_proto::durable::ReadableRecordMessage,
    F: FnOnce(M) -> Result<T, DurableCodecError>,
{
    if M::record_schema().record_type() != record_type {
        return Err(DurableCodecError::new(
            DurableCodecErrorKind::UnexpectedRecordType,
        ));
    }
    let message = match riffdb_proto::durable::decode_readable_message::<M>(encoded) {
        Ok(message) => message,
        Err(_) => {
            // A readable record type can retain more than one historical
            // schema identity (notably capability records). The sealed typed
            // fast path is exact-current; compatibility identities retain the
            // complete registry validator and fail closed if either the
            // envelope or generated message is not canonical.
            let decoded = readable_record_registry()
                .decode(encoded)
                .map_err(DurableCodecError::from_decode_envelope)?;
            if decoded.record_type() != record_type {
                return Err(DurableCodecError::new(
                    DurableCodecErrorKind::UnexpectedRecordType,
                ));
            }
            M::decode(decoded.payload()).map_err(|_| DurableCodecError::corrupt())?
        }
    };
    let value = reconstruct(message)?;
    let charge = EncodedContentCharge::new(encoded.len()).ok_or_else(DurableCodecError::corrupt)?;
    Ok(EncodedPageItem::new(value, charge))
}

pub(super) fn decode_record_variant(
    encoded: &[u8],
    current_record_type: &'static str,
    legacy_record_type: &'static str,
) -> Result<bool, DurableCodecError> {
    let decoded = readable_record_registry()
        .decode(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    match decoded.record_type() {
        record_type if record_type == current_record_type => Ok(true),
        record_type if record_type == legacy_record_type => Ok(false),
        _ => Err(DurableCodecError::new(
            DurableCodecErrorKind::UnexpectedRecordType,
        )),
    }
}

pub(super) fn decode_record_variant_chain(
    encoded: &[u8],
    accepted_record_types: &[&'static str],
) -> Result<usize, DurableCodecError> {
    let decoded = readable_record_registry()
        .decode(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    accepted_record_types
        .iter()
        .position(|record_type| *record_type == decoded.record_type())
        .ok_or_else(|| DurableCodecError::new(DurableCodecErrorKind::UnexpectedRecordType))
}

pub(super) fn require<T>(value: Option<T>) -> Result<T, DurableCodecError> {
    value.ok_or_else(DurableCodecError::corrupt)
}

pub(super) fn fixed<const N: usize>(bytes: Vec<u8>) -> Result<[u8; N], DurableCodecError> {
    bytes.try_into().map_err(|_| DurableCodecError::corrupt())
}

#[cfg(test)]
mod tests;
