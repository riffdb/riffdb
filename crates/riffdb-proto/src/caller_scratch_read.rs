//! Complete existing durable-message decoding with caller-owned encoding scratch.
//! Allocation admission belongs to the closed source owner before this helper;
//! this module neither constructs a ledger nor confers database authority.
use super::{EnvelopeError, MAX_STORED_ENVELOPE_BYTES, PayloadValidationError};
use crate::durable::ReadableRecordMessage;
use crate::storage::v1::StoredEnvelope;
use prost::Message;

/// Safe refusal from a caller-owned canonical decoder.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallerScratchDecodeErrorV1 {
    /// Full existing envelope, schema, checksum or canonicality rejection.
    Envelope(EnvelopeError),
    /// Caller has not admitted a sufficiently large encoding buffer.
    CapacityUnavailable,
}
impl From<EnvelopeError> for CallerScratchDecodeErrorV1 {
    fn from(error: EnvelopeError) -> Self {
        Self::Envelope(error)
    }
}
impl std::fmt::Display for CallerScratchDecodeErrorV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Envelope(error) => std::fmt::Display::fmt(error, formatter),
            Self::CapacityUnavailable => {
                formatter.write_str("canonical decode capacity unavailable")
            }
        }
    }
}
impl std::error::Error for CallerScratchDecodeErrorV1 {}

/// Borrows only a registered role for closed variant dispatch. This proves the
/// ordinary framing/checksum/preflight bounds, never full canonicality or
/// semantic authority. The selected typed decoder must run immediately after.
#[doc(hidden)]
pub fn inspect_readable_record_role_v1(encoded: &[u8]) -> Result<&'static str, EnvelopeError> {
    if encoded.len() > MAX_STORED_ENVELOPE_BYTES {
        return Err(EnvelopeError::EnvelopeTooLarge);
    }
    let registry = crate::durable::readable_record_registry();
    if encoded.starts_with(&super::COMPACT_RECORD_MAGIC_V2) {
        let record_type = registry
            .peek_compact_record_type(encoded)
            .ok_or(EnvelopeError::UnknownCompactIdentity)?;
        let schema = crate::durable::readable_record_schema(record_type)
            .ok_or(EnvelopeError::UnknownRecordType)?;
        registry.borrowed_current_compact_payload(encoded, schema)?;
        return Ok(record_type);
    }
    let (schema, _) = registry.borrowed_v1_payload(encoded)?;
    Ok(schema.record_type)
}

/// Decodes an exact sealed readable schema with unchanged full envelope and
/// generated-message canonical comparisons. The supplied buffer must already
/// have capacity for the complete input; this helper never grows that buffer.
/// Generated DTO ownership and mapper scratch must be admitted by the caller.
#[doc(hidden)]
pub fn decode_readable_message_with_caller_scratch_v1<M>(
    encoded: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<M, CallerScratchDecodeErrorV1>
where
    M: ReadableRecordMessage + Default,
{
    if encoded.len() > MAX_STORED_ENVELOPE_BYTES {
        return Err(EnvelopeError::EnvelopeTooLarge.into());
    }
    if scratch.capacity() < encoded.len() {
        return Err(CallerScratchDecodeErrorV1::CapacityUnavailable);
    }
    let registry = crate::durable::readable_record_registry();
    let expected = M::record_schema();
    let payload = if encoded.starts_with(&super::COMPACT_RECORD_MAGIC_V2) {
        registry.borrowed_current_compact_payload(encoded, expected)?
    } else {
        let (schema, frame) = registry.borrowed_v1_payload(encoded)?;
        if schema.record_type != expected.record_type() {
            return Err(EnvelopeError::UnknownRecordType.into());
        }
        if schema.schema_hash != expected.schema_hash() {
            return Err(EnvelopeError::UnsupportedSchemaHash.into());
        }
        let envelope = StoredEnvelope::decode(encoded).map_err(|_| EnvelopeError::Malformed)?;
        if !canonical(&envelope, encoded, scratch)? {
            return Err(EnvelopeError::NonCanonicalEnvelope.into());
        }
        frame.payload
    };
    let message = M::decode(payload)
        .map_err(|_| EnvelopeError::InvalidPayload(PayloadValidationError::Malformed))?;
    if !canonical(&message, payload, scratch)? {
        return Err(EnvelopeError::InvalidPayload(PayloadValidationError::NonCanonical).into());
    }
    Ok(message)
}
fn canonical<M: Message>(
    message: &M,
    expected: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<bool, CallerScratchDecodeErrorV1> {
    if message.encoded_len() != expected.len() {
        return Ok(false);
    }
    if scratch.capacity() < expected.len() {
        return Err(CallerScratchDecodeErrorV1::CapacityUnavailable);
    }
    scratch.clear();
    message
        .encode(&mut *scratch)
        .map_err(|_| EnvelopeError::Malformed)?;
    Ok(scratch.as_slice() == expected)
}

#[cfg(test)]
#[path = "caller_scratch_read_tests.rs"]
mod tests;
