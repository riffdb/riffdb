//! One-way checked mappings between semantic storage DTOs and durable Protobuf.

mod application;
mod audit;
mod bounds;
mod capability;
mod catalog;
mod common;
mod error;
mod metadata;
mod outbox;
mod projection;

use std::fmt;

use prost::Message;
use riffdb_proto::{
    durable::{current_record_registry, current_record_schema},
    envelope,
};

use crate::{EncodedContentCharge, EncodedPageItem};

use common::*;

pub use application::*;
pub use audit::*;
pub use bounds::*;
pub use capability::*;
pub use catalog::*;
pub use error::*;
pub use metadata::*;
pub use outbox::*;
pub use projection::*;

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

pub(super) fn encode_message<M: Message>(
    record_type: &'static str,
    message: &M,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let schema = current_record_schema(record_type).ok_or_else(DurableCodecError::invariant)?;
    let bytes = envelope::encode(schema, &message.encode_to_vec())
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
    M: Message + Default,
    F: FnOnce(M) -> Result<T, DurableCodecError>,
{
    let decoded = current_record_registry()
        .decode(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    if decoded.record_type() != record_type {
        return Err(DurableCodecError::new(
            DurableCodecErrorKind::UnexpectedRecordType,
        ));
    }
    let message = M::decode(decoded.payload()).map_err(|_| DurableCodecError::corrupt())?;
    let value = reconstruct(message)?;
    let charge = EncodedContentCharge::new(encoded.len()).ok_or_else(DurableCodecError::corrupt)?;
    Ok(EncodedPageItem::new(value, charge))
}

pub(super) fn require<T>(value: Option<T>) -> Result<T, DurableCodecError> {
    value.ok_or_else(DurableCodecError::corrupt)
}

pub(super) fn fixed<const N: usize>(bytes: Vec<u8>) -> Result<[u8; N], DurableCodecError> {
    bytes.try_into().map_err(|_| DurableCodecError::corrupt())
}

#[cfg(test)]
mod tests;
