//! Strict validation for versioned durable Protobuf envelopes.

use std::{error::Error, fmt};

use crc::{CRC_32_ISCSI, Crc, Table};
use prost::Message;
use riffdb_types::SchemaHash;

use crate::storage::v1::StoredEnvelope;
use crate::wire::{self, EnvelopePreflightError};

/// The only storage-envelope format version supported by the POC baseline.
pub const STORAGE_FORMAT_VERSION_V1: u32 = 1;
/// Compact durable-record format written after the pre-alpha V2 migration.
pub const STORAGE_FORMAT_VERSION_V2: u32 = 2;

/// Exact fixed byte length of the compact V2 record header.
pub const COMPACT_RECORD_HEADER_V2_BYTES: usize = 16;

const COMPACT_RECORD_MAGIC_V2: [u8; 4] = *b"RDB2";

/// Absolute upper bound for an encoded envelope or its payload.
pub const MAX_STORED_ENVELOPE_BYTES: usize = 16 * 1024 * 1024;

/// Maximum byte length of a fully qualified durable Protobuf message name.
pub const MAX_RECORD_TYPE_BYTES: usize = 256;

/// Maximum number of entries accepted in one closed record registry.
pub const MAX_REGISTERED_RECORD_SCHEMAS: usize = 256;

const CRC_32C: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISCSI);

/// Safe semantic failures returned by a record-specific payload validator.
///
/// A validator must decode a supported payload and deterministically re-encode
/// it, returning [`Self::NonCanonical`] when the bytes differ. It must not put
/// payload data or internal error sources into this public error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadValidationError {
    /// The payload is not a valid instance of the registered message.
    Malformed,
    /// The payload has an alternate or unknown-field encoding.
    NonCanonical,
    /// A semantic record-specific bound was exceeded.
    LimitExceeded,
}

impl fmt::Display for PayloadValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Malformed => "durable payload is malformed",
            Self::NonCanonical => "durable payload encoding is not canonical",
            Self::LimitExceeded => "durable payload exceeds a semantic limit",
        })
    }
}

impl Error for PayloadValidationError {}

/// One supported `(version, record type, schema hash)` registry entry.
///
/// Construction is crate-sealed. Each accepted production durable record must
/// expose a record-specific factory backed by its generated descriptor and
/// schema-hash constant.
#[derive(Clone, Copy)]
pub struct RecordSchema<'a> {
    record_type: &'a str,
    schema_hash: SchemaHash,
    compact_tag: u8,
    schema_revision: u16,
    max_payload_bytes: usize,
    max_envelope_bytes: usize,
    preflight_payload: fn(&[u8]) -> Result<(), PayloadValidationError>,
    validate_payload: fn(&[u8]) -> Result<(), PayloadValidationError>,
}

impl RecordSchema<'_> {
    pub(crate) const fn new_current(
        record_type: &'static str,
        schema_hash: SchemaHash,
        max_payload_bytes: usize,
        max_envelope_bytes: usize,
        preflight_payload: fn(&[u8]) -> Result<(), PayloadValidationError>,
        validate_payload: fn(&[u8]) -> Result<(), PayloadValidationError>,
    ) -> RecordSchema<'static> {
        RecordSchema {
            record_type,
            schema_hash,
            compact_tag: 0,
            schema_revision: 0,
            max_payload_bytes,
            max_envelope_bytes,
            preflight_payload,
            validate_payload,
        }
    }

    /// Binds this descriptor to one closed compact-record role and revision.
    pub(crate) const fn with_compact_identity(
        mut self,
        compact_tag: u8,
        schema_revision: u16,
    ) -> Self {
        self.compact_tag = compact_tag;
        self.schema_revision = schema_revision;
        self
    }

    /// Returns the fully qualified Protobuf record name.
    #[must_use]
    pub const fn record_type(&self) -> &str {
        self.record_type
    }

    /// Returns the schema-domain digest for this exact record descriptor.
    #[must_use]
    pub const fn schema_hash(&self) -> SchemaHash {
        self.schema_hash
    }

    /// Returns the closed nonzero V2 record tag.
    #[must_use]
    pub const fn compact_tag(&self) -> u8 {
        self.compact_tag
    }

    /// Returns the nonzero schema revision stored in every V2 row.
    #[must_use]
    pub const fn schema_revision(&self) -> u16 {
        self.schema_revision
    }

    /// Returns the record-specific payload ceiling.
    #[must_use]
    pub const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }

    /// Returns the conservative maximum complete canonical envelope size.
    #[must_use]
    pub const fn max_envelope_bytes(&self) -> usize {
        self.max_envelope_bytes
    }
}

impl fmt::Debug for RecordSchema<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordSchema")
            .field("record_type", &self.record_type)
            .field("schema_hash", &self.schema_hash)
            .field("compact_tag", &self.compact_tag)
            .field("schema_revision", &self.schema_revision)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .field("max_envelope_bytes", &self.max_envelope_bytes)
            .finish_non_exhaustive()
    }
}

/// Errors in a caller-supplied closed durable-record registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordRegistryError {
    /// The registry exceeds its hard entry-count limit.
    TooManySchemas,
    /// The registry contains the same v1 record type and schema hash twice.
    DuplicateSchema,
    /// A schema omitted its compact tag or revision.
    InvalidCompactIdentity,
    /// Two schemas claim the same compact tag and revision.
    DuplicateCompactIdentity,
}

impl fmt::Display for RecordRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TooManySchemas => "durable record registry is too large",
            Self::DuplicateSchema => "durable record registry contains a duplicate schema",
            Self::InvalidCompactIdentity => {
                "durable record registry contains an invalid compact identity"
            }
            Self::DuplicateCompactIdentity => {
                "durable record registry contains a duplicate compact identity"
            }
        })
    }
}

impl Error for RecordRegistryError {}

/// A bounded, closed set of durable record schemas supported by one reader.
///
/// Multiple hashes for the same record type are permitted so an explicit
/// migration can register historical readers. Exact duplicate tuples are not.
#[derive(Clone, Copy, Debug)]
pub struct RecordRegistry<'a> {
    schemas: &'a [RecordSchema<'a>],
}

impl<'a> RecordRegistry<'a> {
    /// Validates and borrows a closed registry. An empty registry is valid and
    /// refuses every durable record, which is useful before records exist.
    pub fn new(schemas: &'a [RecordSchema<'a>]) -> Result<Self, RecordRegistryError> {
        if schemas.len() > MAX_REGISTERED_RECORD_SCHEMAS {
            return Err(RecordRegistryError::TooManySchemas);
        }

        for (index, schema) in schemas.iter().enumerate() {
            if schema.compact_tag == 0 || schema.schema_revision == 0 {
                return Err(RecordRegistryError::InvalidCompactIdentity);
            }
            if schemas[..index].iter().any(|existing| {
                existing.record_type == schema.record_type
                    && existing.schema_hash == schema.schema_hash
            }) {
                return Err(RecordRegistryError::DuplicateSchema);
            }
            if schemas[..index].iter().any(|existing| {
                existing.compact_tag == schema.compact_tag
                    && existing.schema_revision == schema.schema_revision
            }) {
                return Err(RecordRegistryError::DuplicateCompactIdentity);
            }
        }

        Ok(Self { schemas })
    }

    /// Strictly decodes an envelope through its registered semantic validator.
    pub fn decode(&self, encoded: &[u8]) -> Result<DecodedEnvelope, EnvelopeError> {
        if encoded.len() > MAX_STORED_ENVELOPE_BYTES {
            return Err(EnvelopeError::EnvelopeTooLarge);
        }
        if encoded.starts_with(&COMPACT_RECORD_MAGIC_V2) {
            return self.decode_compact_v2(encoded);
        }
        self.decode_v1(encoded)
    }

    /// Validates either readable framing and returns the canonical compact V2
    /// representation of the same exact semantic payload.
    pub fn transcode_to_v2(&self, encoded: &[u8]) -> Result<Vec<u8>, EnvelopeError> {
        let decoded = self.decode(encoded)?;
        let schema = self
            .schemas
            .iter()
            .find(|schema| {
                schema.compact_tag == decoded.compact_tag
                    && schema.schema_revision == decoded.schema_revision
                    && schema.schema_hash == decoded.schema_hash
            })
            .ok_or(EnvelopeError::UnknownCompactIdentity)?;
        encode_compact_checked_payload(schema, &decoded.payload)
    }

    /// Validates either readable framing and returns the immutable canonical V1
    /// compatibility representation of the same exact semantic payload.
    pub fn transcode_to_v1(&self, encoded: &[u8]) -> Result<Vec<u8>, EnvelopeError> {
        let decoded = self.decode(encoded)?;
        let schema = self
            .schemas
            .iter()
            .find(|schema| {
                schema.compact_tag == decoded.compact_tag
                    && schema.schema_revision == decoded.schema_revision
                    && schema.schema_hash == decoded.schema_hash
            })
            .ok_or(EnvelopeError::UnknownCompactIdentity)?;
        encode_v1_checked_payload(schema, &decoded.payload)
    }

    fn decode_v1(&self, encoded: &[u8]) -> Result<DecodedEnvelope, EnvelopeError> {
        let preflight = match wire::stored_envelope(encoded) {
            Ok(preflight) => preflight,
            Err(EnvelopePreflightError::Malformed) => return Err(EnvelopeError::Malformed),
            Err(EnvelopePreflightError::InvalidRecordType) => {
                return Err(EnvelopeError::InvalidRecordType);
            }
            Err(EnvelopePreflightError::InvalidSchemaHashLength) => {
                return Err(EnvelopeError::InvalidSchemaHashLength);
            }
            Err(EnvelopePreflightError::NonCanonical) => {
                return Err(EnvelopeError::NonCanonicalEnvelope);
            }
        };

        if preflight.storage_format_version != STORAGE_FORMAT_VERSION_V1 {
            return Err(EnvelopeError::UnsupportedStorageFormatVersion);
        }

        let record_type =
            std::str::from_utf8(preflight.record_type).map_err(|_| EnvelopeError::Malformed)?;
        if !is_valid_record_type(record_type) {
            return Err(EnvelopeError::InvalidRecordType);
        }

        if !self
            .schemas
            .iter()
            .any(|schema| schema.record_type == record_type)
        {
            return Err(EnvelopeError::UnknownRecordType);
        }

        let encoded_schema_hash: [u8; 32] = preflight
            .schema_hash
            .try_into()
            .map_err(|_| EnvelopeError::InvalidSchemaHashLength)?;
        let schema = self
            .schemas
            .iter()
            .filter(|schema| schema.record_type == record_type)
            .find(|schema| schema.schema_hash.as_bytes() == &encoded_schema_hash)
            .ok_or(EnvelopeError::UnsupportedSchemaHash)?;

        if preflight.payload.len() > MAX_STORED_ENVELOPE_BYTES
            || preflight.payload.len() > schema.max_payload_bytes
        {
            return Err(EnvelopeError::PayloadTooLarge);
        }
        if encoded.len() > schema.max_envelope_bytes {
            return Err(EnvelopeError::EnvelopeTooLarge);
        }
        if payload_crc32c(preflight.payload) != preflight.payload_crc32c {
            return Err(EnvelopeError::ChecksumMismatch);
        }
        (schema.preflight_payload)(preflight.payload).map_err(EnvelopeError::InvalidPayload)?;

        // All knowable record-specific sizes have been checked against borrowed
        // wire slices before Prost allocates the owned envelope fields.
        let envelope = StoredEnvelope::decode(encoded).map_err(|_| EnvelopeError::Malformed)?;

        if envelope.encode_to_vec() != encoded {
            return Err(EnvelopeError::NonCanonicalEnvelope);
        }

        (schema.validate_payload)(&envelope.payload).map_err(EnvelopeError::InvalidPayload)?;

        Ok(DecodedEnvelope {
            record_type: envelope.record_type,
            schema_hash: schema.schema_hash,
            compact_tag: schema.compact_tag,
            schema_revision: schema.schema_revision,
            format: DurableRecordFormat::V1,
            payload: envelope.payload,
        })
    }

    fn decode_compact_v2(&self, encoded: &[u8]) -> Result<DecodedEnvelope, EnvelopeError> {
        if encoded.len() < COMPACT_RECORD_HEADER_V2_BYTES {
            return Err(EnvelopeError::Malformed);
        }
        if encoded[4] != u8::try_from(STORAGE_FORMAT_VERSION_V2).expect("V2 fits u8") {
            return Err(EnvelopeError::UnsupportedStorageFormatVersion);
        }
        let compact_tag = encoded[5];
        let schema_revision = u16::from_be_bytes([encoded[6], encoded[7]]);
        if compact_tag == 0 || schema_revision == 0 {
            return Err(EnvelopeError::InvalidCompactIdentity);
        }
        let payload_length =
            u32::from_be_bytes([encoded[8], encoded[9], encoded[10], encoded[11]]) as usize;
        let expected_length = COMPACT_RECORD_HEADER_V2_BYTES
            .checked_add(payload_length)
            .ok_or(EnvelopeError::EnvelopeTooLarge)?;
        if expected_length != encoded.len() {
            return Err(EnvelopeError::Malformed);
        }
        let checksum = u32::from_be_bytes([encoded[12], encoded[13], encoded[14], encoded[15]]);
        let payload = &encoded[COMPACT_RECORD_HEADER_V2_BYTES..];
        let schema = self
            .schemas
            .iter()
            .find(|schema| {
                schema.compact_tag == compact_tag && schema.schema_revision == schema_revision
            })
            .ok_or(EnvelopeError::UnknownCompactIdentity)?;
        if payload.len() > schema.max_payload_bytes {
            return Err(EnvelopeError::PayloadTooLarge);
        }
        if payload_crc32c(payload) != checksum {
            return Err(EnvelopeError::ChecksumMismatch);
        }
        (schema.preflight_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
        (schema.validate_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
        let canonical = encode_compact_checked_payload(schema, payload)?;
        if canonical != encoded {
            return Err(EnvelopeError::NonCanonicalEnvelope);
        }
        Ok(DecodedEnvelope {
            record_type: schema.record_type.to_owned(),
            schema_hash: schema.schema_hash,
            compact_tag,
            schema_revision,
            format: DurableRecordFormat::V2,
            payload: payload.to_vec(),
        })
    }
}

/// Closed physical framing observed for one checked durable record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableRecordFormat {
    /// Legacy Protobuf `StoredEnvelope` framing.
    V1,
    /// Compact fixed-header framing.
    V2,
}

/// A supported, integrity-checked, wire-canonical durable payload.
///
/// Storage's semantic codec must still reconstruct and validate the typed DTO
/// before the payload is treated as authoritative state.
pub struct DecodedEnvelope {
    record_type: String,
    schema_hash: SchemaHash,
    compact_tag: u8,
    schema_revision: u16,
    format: DurableRecordFormat,
    payload: Vec<u8>,
}

impl DecodedEnvelope {
    /// Returns the registered fully qualified record type.
    #[must_use]
    pub fn record_type(&self) -> &str {
        &self.record_type
    }

    /// Returns the registered schema digest.
    #[must_use]
    pub const fn schema_hash(&self) -> SchemaHash {
        self.schema_hash
    }

    /// Returns the closed compact record tag assigned to this semantic role.
    #[must_use]
    pub const fn compact_tag(&self) -> u8 {
        self.compact_tag
    }

    /// Returns the exact registered schema revision.
    #[must_use]
    pub const fn schema_revision(&self) -> u16 {
        self.schema_revision
    }

    /// Returns the checked physical framing.
    #[must_use]
    pub const fn format(&self) -> DurableRecordFormat {
        self.format
    }

    /// Borrows the exact canonical payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consumes the wrapper and returns the exact canonical payload bytes.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

impl fmt::Debug for DecodedEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedEnvelope")
            .field("record_type", &self.record_type)
            .field("schema_hash", &self.schema_hash)
            .field("compact_tag", &self.compact_tag)
            .field("schema_revision", &self.schema_revision)
            .field("format", &self.format)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

/// Safe failures from strict durable-envelope encoding or decoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnvelopeError {
    /// The encoded envelope exceeds the absolute hard limit.
    EnvelopeTooLarge,
    /// The outer bytes are not a decodable `StoredEnvelope`.
    Malformed,
    /// The storage format version is not registered by this implementation.
    UnsupportedStorageFormatVersion,
    /// No supported registry entry has this record type.
    UnknownRecordType,
    /// The record type is not a bounded, fully qualified Protobuf message name.
    InvalidRecordType,
    /// The encoded schema hash is not exactly 32 bytes.
    InvalidSchemaHashLength,
    /// The record type is known but this schema hash is not registered.
    UnsupportedSchemaHash,
    /// A compact row used zero for its closed tag or schema revision.
    InvalidCompactIdentity,
    /// No registered schema has the encoded compact tag and revision.
    UnknownCompactIdentity,
    /// The payload exceeds its absolute or record-specific ceiling.
    PayloadTooLarge,
    /// The CRC-32C does not cover the exact payload bytes.
    ChecksumMismatch,
    /// The outer envelope has an alternate or unknown-field encoding.
    NonCanonicalEnvelope,
    /// The record-specific decoder rejected the payload.
    InvalidPayload(PayloadValidationError),
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EnvelopeTooLarge => "durable envelope is too large",
            Self::Malformed => "durable envelope is malformed",
            Self::UnsupportedStorageFormatVersion => {
                "durable envelope storage format is unsupported"
            }
            Self::UnknownRecordType => "durable record type is unsupported",
            Self::InvalidRecordType => "durable record type is invalid",
            Self::InvalidSchemaHashLength => "durable schema hash has an invalid length",
            Self::UnsupportedSchemaHash => "durable record schema is unsupported",
            Self::InvalidCompactIdentity => "durable compact record identity is invalid",
            Self::UnknownCompactIdentity => "durable compact record identity is unsupported",
            Self::PayloadTooLarge => "durable payload is too large",
            Self::ChecksumMismatch => "durable payload checksum does not match",
            Self::NonCanonicalEnvelope => "durable envelope encoding is not canonical",
            Self::InvalidPayload(error) => return error.fmt(formatter),
        })
    }
}

impl Error for EnvelopeError {}

/// Encodes one semantically canonical payload in compact V2 framing.
pub fn encode(schema: &RecordSchema<'_>, payload: &[u8]) -> Result<Vec<u8>, EnvelopeError> {
    maximum_encoded_compact_record_bytes(schema, payload.len())?;
    (schema.validate_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
    encode_compact_checked_payload(schema, payload)
}

pub(crate) fn encode_preflighted(
    schema: &RecordSchema<'_>,
    payload: &[u8],
) -> Result<Vec<u8>, EnvelopeError> {
    maximum_encoded_compact_record_bytes(schema, payload.len())?;
    (schema.preflight_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
    encode_compact_checked_payload(schema, payload)
}

/// Encodes immutable compatibility bytes in legacy V1 envelope framing.
pub fn encode_v1(schema: &RecordSchema<'_>, payload: &[u8]) -> Result<Vec<u8>, EnvelopeError> {
    maximum_encoded_envelope_bytes(schema, payload.len())?;
    (schema.validate_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
    encode_v1_checked_payload(schema, payload)
}

fn encode_v1_checked_payload(
    schema: &RecordSchema<'_>,
    payload: &[u8],
) -> Result<Vec<u8>, EnvelopeError> {
    let envelope = StoredEnvelope {
        storage_format_version: STORAGE_FORMAT_VERSION_V1,
        record_type: schema.record_type.to_owned(),
        payload: payload.to_vec(),
        payload_crc32c: payload_crc32c(payload),
        schema_hash: schema.schema_hash.as_bytes().to_vec(),
    };
    let encoded = envelope.encode_to_vec();
    if encoded.len() > MAX_STORED_ENVELOPE_BYTES || encoded.len() > schema.max_envelope_bytes {
        return Err(EnvelopeError::EnvelopeTooLarge);
    }
    Ok(encoded)
}

fn encode_compact_checked_payload(
    schema: &RecordSchema<'_>,
    payload: &[u8],
) -> Result<Vec<u8>, EnvelopeError> {
    let total = maximum_encoded_compact_record_bytes(schema, payload.len())?;
    let payload_length =
        u32::try_from(payload.len()).map_err(|_| EnvelopeError::PayloadTooLarge)?;
    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(&COMPACT_RECORD_MAGIC_V2);
    encoded.push(u8::try_from(STORAGE_FORMAT_VERSION_V2).expect("V2 fits u8"));
    encoded.push(schema.compact_tag);
    encoded.extend_from_slice(&schema.schema_revision.to_be_bytes());
    encoded.extend_from_slice(&payload_length.to_be_bytes());
    encoded.extend_from_slice(&payload_crc32c(payload).to_be_bytes());
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

/// Returns the exact compact V2 framing charge for a registered payload length.
pub fn maximum_encoded_compact_record_bytes(
    schema: &RecordSchema<'_>,
    payload_bytes: usize,
) -> Result<usize, EnvelopeError> {
    if schema.compact_tag == 0 || schema.schema_revision == 0 {
        return Err(EnvelopeError::InvalidCompactIdentity);
    }
    if payload_bytes > MAX_STORED_ENVELOPE_BYTES || payload_bytes > schema.max_payload_bytes {
        return Err(EnvelopeError::PayloadTooLarge);
    }
    let total = COMPACT_RECORD_HEADER_V2_BYTES
        .checked_add(payload_bytes)
        .ok_or(EnvelopeError::EnvelopeTooLarge)?;
    if total > MAX_STORED_ENVELOPE_BYTES {
        return Err(EnvelopeError::EnvelopeTooLarge);
    }
    Ok(total)
}

/// Returns a conservative complete-envelope size for a registered payload.
///
/// The checksum field is charged as present even when a particular CRC-32C is
/// zero and Proto3 would omit it. Storage reservation code must use this helper
/// instead of sizing a placeholder `StoredEnvelope`.
pub fn maximum_encoded_envelope_bytes(
    schema: &RecordSchema<'_>,
    payload_bytes: usize,
) -> Result<usize, EnvelopeError> {
    if payload_bytes > MAX_STORED_ENVELOPE_BYTES || payload_bytes > schema.max_payload_bytes {
        return Err(EnvelopeError::PayloadTooLarge);
    }
    let maximum = maximum_encoded_envelope_bytes_for(schema.record_type, payload_bytes)?;
    if maximum > schema.max_envelope_bytes {
        return Err(EnvelopeError::EnvelopeTooLarge);
    }
    Ok(maximum)
}

/// Returns the conservative v1 envelope size for generation and compatibility checks.
///
/// Prefer [`maximum_encoded_envelope_bytes`] when a registry schema is available.
pub fn maximum_encoded_envelope_bytes_for(
    record_type: &str,
    payload_bytes: usize,
) -> Result<usize, EnvelopeError> {
    if !is_valid_record_type(record_type) {
        return Err(EnvelopeError::InvalidRecordType);
    }
    // Version and worst-case fixed32 checksum are always charged as present.
    let maximum = 2usize
        .checked_add(1 + varint_bytes(record_type.len()) + record_type.len())
        .and_then(|value| value.checked_add(1 + varint_bytes(payload_bytes) + payload_bytes))
        .and_then(|value| value.checked_add(5))
        .and_then(|value| value.checked_add(1 + varint_bytes(32) + 32))
        .ok_or(EnvelopeError::EnvelopeTooLarge)?;
    if maximum > MAX_STORED_ENVELOPE_BYTES {
        return Err(EnvelopeError::EnvelopeTooLarge);
    }
    Ok(maximum)
}

const fn varint_bytes(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

/// Computes CRC-32C/Castagnoli over the exact payload bytes.
#[must_use]
pub fn payload_crc32c(payload: &[u8]) -> u32 {
    CRC_32C.checksum(payload)
}

fn is_valid_record_type(record_type: &str) -> bool {
    !(record_type.is_empty()
        || record_type.len() > MAX_RECORD_TYPE_BYTES
        || !record_type.is_ascii()
        || !record_type.contains('.')
        || record_type.split('.').any(|segment| {
            let mut bytes = segment.bytes();
            !matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
                || bytes.any(|byte| !matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'))
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::hash_schema;

    const RECORD_TYPE: &str = "riffdb.testing.v1.CompatibilityProbe";
    const OTHER_RECORD_TYPE: &str = "riffdb.testing.v1.OtherProbe";
    const DESCRIPTOR: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/proto/descriptors/compatibility-probe-descriptor-set.bin"
    ));
    const PROBE_PAYLOAD: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/proto/compatibility-probe-payload.bin"
    ));
    const PROBE_ENVELOPE: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/proto/compatibility-probe-envelope.bin"
    ));
    const EXPECTED_SCHEMA_HASH: [u8; 32] = [
        0xbd, 0x08, 0xb5, 0x3d, 0x75, 0xaa, 0xd9, 0xe6, 0x03, 0xc4, 0xa9, 0x38, 0x2d, 0x0f, 0x1a,
        0x93, 0xc5, 0x28, 0xc1, 0xd0, 0x5a, 0xde, 0xe2, 0x58, 0x80, 0x4f, 0x74, 0x32, 0xec, 0x24,
        0x11, 0xd7,
    ];

    #[derive(Clone, PartialEq, Message)]
    struct CompatibilityProbe {
        #[prost(uint64, tag = "1")]
        value: u64,
        #[prost(message, optional, tag = "2")]
        metadata: Option<ProbeMetadata>,
    }

    #[derive(Clone, PartialEq, Message)]
    struct ProbeMetadata {
        #[prost(message, optional, tag = "1")]
        source: Option<ProbeSource>,
    }

    #[derive(Clone, PartialEq, Message)]
    struct ProbeSource {
        #[prost(string, tag = "1")]
        name: String,
    }

    fn validate_probe(payload: &[u8]) -> Result<(), PayloadValidationError> {
        let probe =
            CompatibilityProbe::decode(payload).map_err(|_| PayloadValidationError::Malformed)?;
        if probe.encode_to_vec() != payload {
            return Err(PayloadValidationError::NonCanonical);
        }
        Ok(())
    }

    fn preflight_probe(_payload: &[u8]) -> Result<(), PayloadValidationError> {
        Ok(())
    }

    #[derive(Debug, Eq, PartialEq)]
    enum TestSchemaError {
        InvalidRecordType,
        DescriptorSetTooLarge,
        InvalidPayloadLimit,
    }

    fn test_schema(
        record_type: &'static str,
        descriptor_set: &[u8],
        max_payload_bytes: usize,
    ) -> Result<RecordSchema<'static>, TestSchemaError> {
        if !is_valid_record_type(record_type) {
            return Err(TestSchemaError::InvalidRecordType);
        }
        if descriptor_set.len() > MAX_STORED_ENVELOPE_BYTES {
            return Err(TestSchemaError::DescriptorSetTooLarge);
        }
        if max_payload_bytes > MAX_STORED_ENVELOPE_BYTES {
            return Err(TestSchemaError::InvalidPayloadLimit);
        }

        let record_type_length =
            u16::try_from(record_type.len()).map_err(|_| TestSchemaError::InvalidRecordType)?;
        let descriptor_set_length = u64::try_from(descriptor_set.len())
            .map_err(|_| TestSchemaError::DescriptorSetTooLarge)?;
        let mut frame = Vec::with_capacity(2 + record_type.len() + 8 + descriptor_set.len());
        frame.extend_from_slice(&record_type_length.to_be_bytes());
        frame.extend_from_slice(record_type.as_bytes());
        frame.extend_from_slice(&descriptor_set_length.to_be_bytes());
        frame.extend_from_slice(descriptor_set);

        Ok(RecordSchema {
            record_type,
            schema_hash: hash_schema(&frame),
            compact_tag: 1,
            schema_revision: 1,
            max_payload_bytes,
            max_envelope_bytes: MAX_STORED_ENVELOPE_BYTES,
            preflight_payload: preflight_probe,
            validate_payload: validate_probe,
        })
    }

    fn schema() -> RecordSchema<'static> {
        test_schema(RECORD_TYPE, DESCRIPTOR, 32).expect("generated test descriptor is valid")
    }

    fn raw_envelope(schema: &RecordSchema<'_>, payload: Vec<u8>) -> StoredEnvelope {
        StoredEnvelope {
            storage_format_version: STORAGE_FORMAT_VERSION_V1,
            record_type: schema.record_type().to_owned(),
            payload_crc32c: payload_crc32c(&payload),
            payload,
            schema_hash: schema.schema_hash().as_bytes().to_vec(),
        }
    }

    fn decode_error(encoded: &[u8], expected: EnvelopeError) {
        let schema = schema();
        let schemas = [schema];
        let registry = RecordRegistry::new(&schemas).expect("test registry is valid");
        assert_eq!(
            registry.decode(encoded).expect_err("decode must fail"),
            expected
        );
    }

    #[test]
    fn generated_schema_round_trips_the_golden_envelope() {
        let schema = schema();
        assert_eq!(schema.schema_hash().as_bytes(), &EXPECTED_SCHEMA_HASH);
        let encoded = encode_v1(&schema, PROBE_PAYLOAD).expect("canonical payload encodes");
        assert_eq!(encoded, PROBE_ENVELOPE);

        let schemas = [schema];
        let decoded = RecordRegistry::new(&schemas)
            .expect("test registry is valid")
            .decode(&encoded)
            .expect("canonical envelope decodes");
        assert_eq!(decoded.record_type(), RECORD_TYPE);
        assert_eq!(decoded.schema_hash(), schema.schema_hash());
        assert_eq!(decoded.format(), DurableRecordFormat::V1);
        assert_eq!(decoded.payload(), PROBE_PAYLOAD);
    }

    #[test]
    fn compact_v2_round_trips_and_rejects_header_substitution() {
        let schema = schema();
        let schemas = [schema];
        let registry = RecordRegistry::new(&schemas).expect("test registry is valid");
        let encoded = encode(&schema, PROBE_PAYLOAD).expect("compact payload encodes");
        assert_eq!(encoded.len(), COMPACT_RECORD_HEADER_V2_BYTES + 2);
        let decoded = registry.decode(&encoded).expect("compact record decodes");
        assert_eq!(decoded.format(), DurableRecordFormat::V2);
        assert_eq!(decoded.compact_tag(), 1);
        assert_eq!(decoded.schema_revision(), 1);
        assert_eq!(decoded.payload(), PROBE_PAYLOAD);

        let mut unknown_tag = encoded.clone();
        unknown_tag[5] = 2;
        assert_eq!(
            registry
                .decode(&unknown_tag)
                .expect_err("unknown tag fails"),
            EnvelopeError::UnknownCompactIdentity
        );
        let mut zero_revision = encoded;
        zero_revision[6..8].copy_from_slice(&0_u16.to_be_bytes());
        assert_eq!(
            registry
                .decode(&zero_revision)
                .expect_err("zero revision fails"),
            EnvelopeError::InvalidCompactIdentity
        );
    }

    #[test]
    fn version_type_hash_and_checksum_fail_closed() {
        let schema = schema();

        let mut envelope = raw_envelope(&schema, PROBE_PAYLOAD.to_vec());
        envelope.storage_format_version = 2;
        decode_error(
            &envelope.encode_to_vec(),
            EnvelopeError::UnsupportedStorageFormatVersion,
        );

        let mut envelope = raw_envelope(&schema, PROBE_PAYLOAD.to_vec());
        envelope.record_type = OTHER_RECORD_TYPE.to_owned();
        decode_error(&envelope.encode_to_vec(), EnvelopeError::UnknownRecordType);

        let mut envelope = raw_envelope(&schema, PROBE_PAYLOAD.to_vec());
        envelope.schema_hash = vec![0; 31];
        decode_error(
            &envelope.encode_to_vec(),
            EnvelopeError::InvalidSchemaHashLength,
        );

        let mut envelope = raw_envelope(&schema, PROBE_PAYLOAD.to_vec());
        envelope.schema_hash = vec![0; 32];
        decode_error(
            &envelope.encode_to_vec(),
            EnvelopeError::UnsupportedSchemaHash,
        );

        let mut envelope = raw_envelope(&schema, PROBE_PAYLOAD.to_vec());
        envelope.payload_crc32c ^= 1;
        decode_error(&envelope.encode_to_vec(), EnvelopeError::ChecksumMismatch);
    }

    #[test]
    fn record_specific_limits_and_registry_duplicates_are_rejected() {
        let schema = schema();
        let payload = vec![0; 33];
        decode_error(
            &raw_envelope(&schema, payload).encode_to_vec(),
            EnvelopeError::PayloadTooLarge,
        );
        assert_eq!(
            test_schema(RECORD_TYPE, DESCRIPTOR, MAX_STORED_ENVELOPE_BYTES + 1)
                .expect_err("oversized record limit must fail"),
            TestSchemaError::InvalidPayloadLimit
        );
        assert_eq!(
            RecordRegistry::new(&[schema, schema]).expect_err("duplicate schema must fail"),
            RecordRegistryError::DuplicateSchema
        );
    }

    #[test]
    fn reservation_size_charges_a_nonzero_checksum_field() {
        let schema = schema();
        let envelope_without_checksum = StoredEnvelope {
            storage_format_version: STORAGE_FORMAT_VERSION_V1,
            record_type: schema.record_type().to_owned(),
            payload: PROBE_PAYLOAD.to_vec(),
            payload_crc32c: 0,
            schema_hash: schema.schema_hash().as_bytes().to_vec(),
        }
        .encode_to_vec();
        assert_eq!(
            maximum_encoded_envelope_bytes(&schema, PROBE_PAYLOAD.len())
                .expect("fixture payload is within its schema limit"),
            envelope_without_checksum.len() + 5
        );
    }

    #[test]
    fn duplicate_and_truncating_outer_scalars_fail_in_preflight() {
        let schema = schema();
        let canonical = encode_v1(&schema, PROBE_PAYLOAD).expect("canonical payload encodes");

        let mut duplicate_version = canonical.clone();
        duplicate_version.extend_from_slice(&[0x08, 0x01]);
        decode_error(&duplicate_version, EnvelopeError::NonCanonicalEnvelope);

        let mut duplicate_checksum = canonical.clone();
        duplicate_checksum.extend_from_slice(&[0x25, 0, 0, 0, 0]);
        decode_error(&duplicate_checksum, EnvelopeError::NonCanonicalEnvelope);

        let mut truncating_version = canonical;
        truncating_version.splice(0..2, [0x08, 0x81, 0x80, 0x80, 0x80, 0x10]);
        decode_error(&truncating_version, EnvelopeError::NonCanonicalEnvelope);
    }

    #[test]
    fn alternate_outer_and_payload_encodings_are_rejected() {
        let schema = schema();
        let mut encoded = encode_v1(&schema, PROBE_PAYLOAD).expect("canonical payload encodes");
        encoded.extend_from_slice(&[0x98, 0x06, 0x00]);
        decode_error(&encoded, EnvelopeError::NonCanonicalEnvelope);

        let noncanonical_payload = vec![0x08, 0x81, 0x00];
        decode_error(
            &raw_envelope(&schema, noncanonical_payload).encode_to_vec(),
            EnvelopeError::InvalidPayload(PayloadValidationError::NonCanonical),
        );
    }

    #[test]
    fn generated_schema_input_validation_is_bounded() {
        for invalid in [
            "",
            ".riffdb.storage.Message",
            "riffdb..Message",
            "unqualified",
            "riffdb.storage.1Message",
            "riffdb.storage.Message-name",
            "riffdb.storage.Mes\u{e9}sage",
        ] {
            assert_eq!(
                test_schema(invalid, DESCRIPTOR, 32).expect_err("invalid record type must fail"),
                TestSchemaError::InvalidRecordType,
                "{invalid:?}"
            );
        }

        let schemas = vec![schema(); MAX_REGISTERED_RECORD_SCHEMAS + 1];
        assert_eq!(
            RecordRegistry::new(&schemas).expect_err("oversized registry must fail"),
            RecordRegistryError::TooManySchemas
        );
    }
}
