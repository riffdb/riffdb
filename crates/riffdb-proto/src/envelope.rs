//! Strict validation for versioned durable Protobuf envelopes.

use std::cell::RefCell;
use std::time::Instant;
use std::{error::Error, fmt};

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

    /// Resolves a compact V2 record type from its header, without decoding.
    ///
    /// Callers that only need to learn which variant a record is paid a full
    /// validating decode, then decoded again as the chosen type. The compact
    /// header already carries the identity, so this selects exactly the schema
    /// `decode_compact_v2` would select from the same bytes.
    ///
    /// This deliberately validates nothing beyond the identity it reads:
    /// `None` means "cannot resolve from the header alone", including legacy
    /// V1 framing, and the caller must fall back to the full decode. Every
    /// payload check still happens when the caller decodes as the chosen type,
    /// so a corrupt header yields a decode failure rather than an acceptance.
    #[must_use]
    pub fn peek_compact_record_type(&self, encoded: &[u8]) -> Option<&'a str> {
        if encoded.len() < COMPACT_RECORD_HEADER_V2_BYTES
            || !encoded.starts_with(&COMPACT_RECORD_MAGIC_V2)
            || encoded[4] != u8::try_from(STORAGE_FORMAT_VERSION_V2).ok()?
        {
            return None;
        }
        let compact_tag = encoded[5];
        let schema_revision = u16::from_be_bytes([encoded[6], encoded[7]]);
        if compact_tag == 0 || schema_revision == 0 {
            return None;
        }
        self.schemas
            .iter()
            .find(|schema| {
                schema.compact_tag == compact_tag && schema.schema_revision == schema_revision
            })
            .map(|schema| schema.record_type)
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

    /// Decodes one current generated message without reconstructing the same
    /// Protobuf payload in both the registry validator and its semantic caller.
    ///
    /// The caller must supply the schema bound to the generated message by the
    /// crate-sealed durable-message trait. Legacy V1 framing retains the full
    /// compatibility decoder; compact V2 performs the same identity, checksum,
    /// bound, wire-canonicality, and Prost-canonicality checks exactly once.
    pub(crate) fn decode_current_message<M>(
        &self,
        encoded: &[u8],
        expected_schema: &RecordSchema<'_>,
    ) -> Result<M, EnvelopeError>
    where
        M: Message + Default,
    {
        if encoded.len() > MAX_STORED_ENVELOPE_BYTES {
            return Err(EnvelopeError::EnvelopeTooLarge);
        }
        if !encoded.starts_with(&COMPACT_RECORD_MAGIC_V2) {
            let decoded = self.decode_v1(encoded)?;
            if decoded.record_type() != expected_schema.record_type {
                return Err(EnvelopeError::UnknownRecordType);
            }
            return M::decode(decoded.payload())
                .map_err(|_| EnvelopeError::InvalidPayload(PayloadValidationError::Malformed));
        }
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
        let schema = self
            .schemas
            .iter()
            .find(|schema| {
                schema.compact_tag == compact_tag && schema.schema_revision == schema_revision
            })
            .ok_or(EnvelopeError::UnknownCompactIdentity)?;
        if schema.record_type != expected_schema.record_type {
            return Err(EnvelopeError::UnknownRecordType);
        }
        let payload_length =
            u32::from_be_bytes([encoded[8], encoded[9], encoded[10], encoded[11]]) as usize;
        let expected_length = COMPACT_RECORD_HEADER_V2_BYTES
            .checked_add(payload_length)
            .ok_or(EnvelopeError::EnvelopeTooLarge)?;
        if expected_length != encoded.len() {
            return Err(EnvelopeError::Malformed);
        }
        let payload = &encoded[COMPACT_RECORD_HEADER_V2_BYTES..];
        if payload.len() > schema.max_payload_bytes {
            return Err(EnvelopeError::PayloadTooLarge);
        }
        let checksum = u32::from_be_bytes([encoded[12], encoded[13], encoded[14], encoded[15]]);
        if payload_crc32c(payload) != checksum {
            return Err(EnvelopeError::ChecksumMismatch);
        }
        (schema.preflight_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
        let message = M::decode(payload)
            .map_err(|_| EnvelopeError::InvalidPayload(PayloadValidationError::Malformed))?;
        if !is_canonical_encoding(&message, payload) {
            return Err(EnvelopeError::InvalidPayload(
                PayloadValidationError::NonCanonical,
            ));
        }
        Ok(message)
    }

    /// Identical current-message decode with fixed-cardinality phase timing.
    pub(crate) fn decode_current_message_profiled<M>(
        &self,
        encoded: &[u8],
        expected_schema: &RecordSchema<'_>,
    ) -> Result<(M, CurrentMessageDecodeProfileV1), EnvelopeError>
    where
        M: Message + Default,
    {
        let identity_started = Instant::now();
        if encoded.len() > MAX_STORED_ENVELOPE_BYTES {
            return Err(EnvelopeError::EnvelopeTooLarge);
        }
        if !encoded.starts_with(&COMPACT_RECORD_MAGIC_V2) {
            let decoded = self.decode_v1(encoded)?;
            if decoded.record_type() != expected_schema.record_type {
                return Err(EnvelopeError::UnknownRecordType);
            }
            let identity_bounds_ns = elapsed_nanos(identity_started);
            let prost_started = Instant::now();
            let message = M::decode(decoded.payload())
                .map_err(|_| EnvelopeError::InvalidPayload(PayloadValidationError::Malformed))?;
            return Ok((
                message,
                CurrentMessageDecodeProfileV1 {
                    identity_bounds_ns,
                    prost_decode_ns: elapsed_nanos(prost_started),
                    ..CurrentMessageDecodeProfileV1::default()
                },
            ));
        }
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
        let schema = self
            .schemas
            .iter()
            .find(|schema| {
                schema.compact_tag == compact_tag && schema.schema_revision == schema_revision
            })
            .ok_or(EnvelopeError::UnknownCompactIdentity)?;
        if schema.record_type != expected_schema.record_type {
            return Err(EnvelopeError::UnknownRecordType);
        }
        let payload_length =
            u32::from_be_bytes([encoded[8], encoded[9], encoded[10], encoded[11]]) as usize;
        let expected_length = COMPACT_RECORD_HEADER_V2_BYTES
            .checked_add(payload_length)
            .ok_or(EnvelopeError::EnvelopeTooLarge)?;
        if expected_length != encoded.len() {
            return Err(EnvelopeError::Malformed);
        }
        let payload = &encoded[COMPACT_RECORD_HEADER_V2_BYTES..];
        if payload.len() > schema.max_payload_bytes {
            return Err(EnvelopeError::PayloadTooLarge);
        }
        let identity_bounds_ns = elapsed_nanos(identity_started);
        let checksum_started = Instant::now();
        let checksum = u32::from_be_bytes([encoded[12], encoded[13], encoded[14], encoded[15]]);
        if payload_crc32c(payload) != checksum {
            return Err(EnvelopeError::ChecksumMismatch);
        }
        let checksum_ns = elapsed_nanos(checksum_started);
        let preflight_started = Instant::now();
        (schema.preflight_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
        let wire_preflight_ns = elapsed_nanos(preflight_started);
        let prost_started = Instant::now();
        let message = M::decode(payload)
            .map_err(|_| EnvelopeError::InvalidPayload(PayloadValidationError::Malformed))?;
        let prost_decode_ns = elapsed_nanos(prost_started);
        let canonical_started = Instant::now();
        if !is_canonical_encoding(&message, payload) {
            return Err(EnvelopeError::InvalidPayload(
                PayloadValidationError::NonCanonical,
            ));
        }
        Ok((
            message,
            CurrentMessageDecodeProfileV1 {
                identity_bounds_ns,
                checksum_ns,
                wire_preflight_ns,
                prost_decode_ns,
                canonical_reencode_ns: elapsed_nanos(canonical_started),
            },
        ))
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

        if !is_canonical_encoding(&envelope, encoded) {
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

/// Fixed-cardinality phase timing for one current generated-message decode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CurrentMessageDecodeProfileV1 {
    pub(crate) identity_bounds_ns: u64,
    pub(crate) checksum_ns: u64,
    pub(crate) wire_preflight_ns: u64,
    pub(crate) prost_decode_ns: u64,
    pub(crate) canonical_reencode_ns: u64,
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
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

/// Proves a payload is the canonical encoding of the message decoded from it.
///
/// The byte-for-byte compare *is* the canonicality proof and is unchanged. Only
/// the buffer is: `encode_to_vec` allocated a fresh payload-sized `Vec` on every
/// decode purely to throw it away after the compare. Re-encoding into a reused
/// per-thread scratch buffer gives the identical answer with no allocation, and
/// the length pre-check rejects a mismatch before re-encoding at all.
pub(crate) fn is_canonical_encoding<M: Message>(message: &M, payload: &[u8]) -> bool {
    if message.encoded_len() != payload.len() {
        return false;
    }
    CANONICAL_SCRATCH.with(|scratch| {
        let Ok(mut scratch) = scratch.try_borrow_mut() else {
            // Re-entrant compare: fall back to a private buffer rather than
            // sharing one, so the proof still runs.
            return message.encode_to_vec() == payload;
        };
        scratch.clear();
        scratch.reserve(payload.len());
        if message.encode(&mut *scratch).is_err() {
            return false;
        }
        let canonical = scratch.as_slice() == payload;
        if scratch.capacity() > CANONICAL_SCRATCH_RETAINED_BYTES {
            scratch.shrink_to(CANONICAL_SCRATCH_RETAINED_BYTES);
        }
        canonical
    })
}

/// Scratch capacity kept between canonicality proofs. Larger payloads still
/// compare exactly; their buffer is released instead of held per thread.
const CANONICAL_SCRATCH_RETAINED_BYTES: usize = 64 * 1024;

thread_local! {
    static CANONICAL_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Encodes one semantically canonical payload in compact V2 framing.
pub fn encode(schema: &RecordSchema<'_>, payload: &[u8]) -> Result<Vec<u8>, EnvelopeError> {
    maximum_encoded_compact_record_bytes(schema, payload.len())?;
    (schema.validate_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
    encode_compact_checked_payload(schema, payload)
}

/// Frames one generated message straight into its compact V2 envelope.
///
/// The compact V2 header is a fixed 16 bytes and `Message::encoded_len` gives
/// the exact payload length, so the payload can be written once at its final
/// offset instead of being materialised in an intermediate buffer and copied
/// in. The same framing bounds apply, the same preflight runs over the exact
/// payload slice, and the CRC-32C still covers exactly those bytes, so the
/// output is byte-identical to framing a separately encoded payload.
pub(crate) fn encode_message_preflighted<M: Message>(
    schema: &RecordSchema<'_>,
    message: &M,
) -> Result<Vec<u8>, EnvelopeError> {
    let payload_bytes = message.encoded_len();
    let total = maximum_encoded_compact_record_bytes(schema, payload_bytes)?;
    let payload_length =
        u32::try_from(payload_bytes).map_err(|_| EnvelopeError::PayloadTooLarge)?;
    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(&COMPACT_RECORD_MAGIC_V2);
    encoded.push(u8::try_from(STORAGE_FORMAT_VERSION_V2).expect("V2 fits u8"));
    encoded.push(schema.compact_tag);
    encoded.extend_from_slice(&schema.schema_revision.to_be_bytes());
    encoded.extend_from_slice(&payload_length.to_be_bytes());
    encoded.extend_from_slice(&0_u32.to_be_bytes());
    message
        .encode(&mut encoded)
        .map_err(|_| EnvelopeError::Malformed)?;
    if encoded.len() != total {
        return Err(EnvelopeError::Malformed);
    }
    let payload = &encoded[COMPACT_RECORD_HEADER_V2_BYTES..];
    (schema.preflight_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
    let checksum = payload_crc32c(payload).to_be_bytes();
    encoded[COMPACT_RECORD_HEADER_V2_BYTES - checksum.len()..COMPACT_RECORD_HEADER_V2_BYTES]
        .copy_from_slice(&checksum);
    Ok(encoded)
}

pub(crate) fn encode_preflighted(
    schema: &RecordSchema<'_>,
    payload: &[u8],
) -> Result<Vec<u8>, EnvelopeError> {
    maximum_encoded_compact_record_bytes(schema, payload.len())?;
    (schema.preflight_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;
    encode_compact_checked_payload(schema, payload)
}

/// Frames one payload after a first-party typed encoder has already proved
/// the schema's canonical structural shape.
///
/// This deliberately retains the registered compact identity, exact payload
/// bound, and CRC-32C framing checks. The durable registry keeps this entry
/// point crate-private so callers can reach it only through a sealed current
/// record type and the explicit proof-bearing API in `durable`.
pub(crate) fn encode_after_structural_proof(
    schema: &RecordSchema<'_>,
    payload: &[u8],
) -> Result<Vec<u8>, EnvelopeError> {
    maximum_encoded_compact_record_bytes(schema, payload.len())?;
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
    // CRC-32C (Castagnoli), the polynomial SSE4.2 implements directly. The
    // previous slice-by-16 software table produced the same values at 5.5
    // GB/s; this runs at 9.5 GB/s on the measurement hosts. `crc_matches_the_
    // software_table_across_sizes_and_alignments` holds the two against each
    // other, because a crate that computed a different polynomial would
    // rewrite every envelope checksum without failing a performance test.
    crc32c::crc32c(payload)
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

    /// The scratch buffer behind `is_canonical_encoding` is reused across calls,
    /// so a large payload must not leave a tail that a later shorter payload
    /// could compare against, and a shorter one must not leave the buffer short.
    #[test]
    fn reused_canonicality_scratch_does_not_leak_between_payloads() {
        let large = crate::storage::v1::StoredRecordRegistryV2 {
            registry_digest: vec![0xAB; 4096],
        };
        let small = crate::storage::v1::StoredRecordRegistryV2 {
            registry_digest: vec![0xCD; 8],
        };
        let large_payload = large.encode_to_vec();
        let small_payload = small.encode_to_vec();

        for _ in 0..3 {
            assert!(is_canonical_encoding(&large, &large_payload));
            assert!(is_canonical_encoding(&small, &small_payload));
            assert!(!is_canonical_encoding(&large, &small_payload));
            assert!(!is_canonical_encoding(&small, &large_payload));
        }
    }

    /// The compare is the canonicality proof: any payload that is not exactly
    /// what the decoded message re-encodes to must still be rejected.
    #[test]
    fn canonicality_compare_rejects_noncanonical_payload_bytes() {
        let message = crate::storage::v1::StoredRecordRegistryV2 {
            registry_digest: vec![0x11; 32],
        };
        let canonical = message.encode_to_vec();
        assert!(is_canonical_encoding(&message, &canonical));

        let mut trailing = canonical.clone();
        trailing.push(0);
        assert!(!is_canonical_encoding(&message, &trailing));

        let mut truncated = canonical.clone();
        truncated.pop();
        assert!(!is_canonical_encoding(&message, &truncated));

        let mut flipped = canonical.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0xFF;
        assert!(!is_canonical_encoding(&message, &flipped));
    }

    /// A payload sitting above the retained scratch capacity still compares
    /// exactly, and the shrink afterwards must not disturb the next call.
    #[test]
    fn canonicality_compare_handles_payloads_above_the_retained_scratch() {
        let message = crate::storage::v1::StoredRecordRegistryV2 {
            registry_digest: vec![0x5A; CANONICAL_SCRATCH_RETAINED_BYTES + 1024],
        };
        let payload = message.encode_to_vec();
        assert!(payload.len() > CANONICAL_SCRATCH_RETAINED_BYTES);
        assert!(is_canonical_encoding(&message, &payload));

        let small = crate::storage::v1::StoredRecordRegistryV2 {
            registry_digest: vec![0x5A; 16],
        };
        assert!(is_canonical_encoding(&small, &small.encode_to_vec()));
    }

    const RECORD_TYPE: &str = "riffdb.testing.v1.CompatibilityProbe";
    const OTHER_RECORD_TYPE: &str = "riffdb.testing.v1.OtherProbe";
    const DESCRIPTOR: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/descriptors/compatibility-probe-descriptor-set.bin"
    ));
    const PROBE_PAYLOAD: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/compatibility-probe-payload.bin"
    ));
    const PROBE_ENVELOPE: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/compatibility-probe-envelope.bin"
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
        if !is_canonical_encoding(&probe, payload) {
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
    fn profiled_current_decode_preserves_exact_semantics() {
        let schema = schema();
        let schemas = [schema];
        let registry = RecordRegistry::new(&schemas).expect("test registry is valid");
        let encoded = encode(&schema, PROBE_PAYLOAD).expect("compact payload encodes");
        let ordinary = registry
            .decode_current_message::<CompatibilityProbe>(&encoded, &schema)
            .expect("ordinary current decode succeeds");
        let (profiled, _profile) = registry
            .decode_current_message_profiled::<CompatibilityProbe>(&encoded, &schema)
            .expect("profiled current decode succeeds");
        assert_eq!(profiled, ordinary);

        let mut corrupt = encoded;
        corrupt[12] ^= 1;
        let ordinary = registry
            .decode_current_message::<CompatibilityProbe>(&corrupt, &schema)
            .expect_err("ordinary checksum validation fails");
        let profiled = registry
            .decode_current_message_profiled::<CompatibilityProbe>(&corrupt, &schema)
            .expect_err("profiled checksum validation fails");
        assert_eq!(ordinary, profiled);
        assert_eq!(ordinary, EnvelopeError::ChecksumMismatch);
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

#[cfg(test)]
mod peek_record_type_tests {
    use super::*;

    fn readable() -> RecordRegistry<'static> {
        crate::durable::readable_record_registry()
    }

    /// The peek must select the same schema `decode_compact_v2` selects from
    /// the same header, for every registered readable schema.
    #[test]
    fn peek_matches_the_schema_decode_would_select() {
        for schema in crate::durable::READABLE_RECORD_SCHEMAS.iter() {
            let mut header = Vec::with_capacity(COMPACT_RECORD_HEADER_V2_BYTES);
            header.extend_from_slice(&COMPACT_RECORD_MAGIC_V2);
            header.push(u8::try_from(STORAGE_FORMAT_VERSION_V2).expect("V2 fits u8"));
            header.push(schema.compact_tag);
            header.extend_from_slice(&schema.schema_revision.to_be_bytes());
            header.extend_from_slice(&0_u32.to_be_bytes());
            header.extend_from_slice(&payload_crc32c(&[]).to_be_bytes());

            let peeked = readable()
                .peek_compact_record_type(&header)
                .expect("a registered identity resolves from its header");
            // decode resolves by the same (tag, revision) pair; first match wins
            // in both, so the peek must agree with that resolution exactly.
            let resolved = crate::durable::READABLE_RECORD_SCHEMAS
                .iter()
                .find(|candidate| {
                    candidate.compact_tag == schema.compact_tag
                        && candidate.schema_revision == schema.schema_revision
                })
                .expect("present by construction");
            assert_eq!(peeked, resolved.record_type);
        }
    }

    /// Anything the header cannot answer must fall back, never guess.
    #[test]
    fn peek_declines_everything_it_cannot_resolve() {
        let registry = readable();
        let schema = &crate::durable::READABLE_RECORD_SCHEMAS[0];
        let good = |tag: u8, revision: u16, magic: [u8; 4], version: u8| {
            let mut header = Vec::new();
            header.extend_from_slice(&magic);
            header.push(version);
            header.push(tag);
            header.extend_from_slice(&revision.to_be_bytes());
            header.extend_from_slice(&0_u32.to_be_bytes());
            header.extend_from_slice(&payload_crc32c(&[]).to_be_bytes());
            header
        };
        let version = u8::try_from(STORAGE_FORMAT_VERSION_V2).expect("V2 fits u8");
        // Too short.
        assert!(registry.peek_compact_record_type(&[]).is_none());
        assert!(
            registry
                .peek_compact_record_type(
                    &good(
                        schema.compact_tag,
                        schema.schema_revision,
                        COMPACT_RECORD_MAGIC_V2,
                        version
                    )[..8]
                )
                .is_none()
        );
        // Legacy framing.
        assert!(
            registry
                .peek_compact_record_type(&good(
                    schema.compact_tag,
                    schema.schema_revision,
                    *b"RDB1",
                    version
                ))
                .is_none()
        );
        // Wrong storage format version.
        assert!(
            registry
                .peek_compact_record_type(&good(
                    schema.compact_tag,
                    schema.schema_revision,
                    COMPACT_RECORD_MAGIC_V2,
                    version.wrapping_add(1)
                ))
                .is_none()
        );
        // Zero identity, and an unregistered tag.
        assert!(
            registry
                .peek_compact_record_type(&good(
                    0,
                    schema.schema_revision,
                    COMPACT_RECORD_MAGIC_V2,
                    version
                ))
                .is_none()
        );
        assert!(
            registry
                .peek_compact_record_type(&good(
                    schema.compact_tag,
                    0,
                    COMPACT_RECORD_MAGIC_V2,
                    version
                ))
                .is_none()
        );
        assert!(
            registry
                .peek_compact_record_type(&good(
                    u8::MAX,
                    u16::MAX,
                    COMPACT_RECORD_MAGIC_V2,
                    version
                ))
                .is_none()
        );
    }
}

#[cfg(test)]
mod crc_equivalence {
    use super::payload_crc32c;
    use crc::{CRC_32_ISCSI, Crc, Table};

    /// The slice-by-16 software table this path used before the hardware
    /// implementation replaced it. It is the oracle, not a fallback.
    const SOFTWARE_TABLE: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISCSI);

    #[test]
    fn crc_matches_the_software_table_across_sizes_and_alignments() {
        // A CRC crate that implements a different polynomial produces a
        // plausible-looking checksum for every input and rewrites every
        // envelope in every database. Nothing about the envelope's own tests
        // would fail, because both sides of an encode/decode round trip would
        // agree with each other. The first candidate tried for this swap,
        // crc32fast, is exactly that: it computes CRC-32/IEEE, not CRC-32C.
        // This test is the reason that would be caught.
        for size in [0usize, 1, 2, 3, 7, 8, 15, 16, 31, 63, 64, 127, 255, 512, 1024, 4096, 6393] {
            let ascending: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
            let uniform = vec![0x5a_u8; size];
            let sparse: Vec<u8> = (0..size).map(|index| u8::from(index % 97 == 0)).collect();
            for payload in [&ascending, &uniform, &sparse] {
                assert_eq!(
                    payload_crc32c(payload),
                    SOFTWARE_TABLE.checksum(payload),
                    "polynomial drift at size {size}"
                );
                // Unaligned starts: the hardware path processes a head, a
                // body and a tail, and only the body is the fast case.
                for offset in 1..payload.len().min(9) {
                    let shifted = &payload[offset..];
                    assert_eq!(
                        payload_crc32c(shifted),
                        SOFTWARE_TABLE.checksum(shifted),
                        "polynomial drift at size {size} offset {offset}"
                    );
                }
            }
        }
    }

    #[test]
    fn crc_of_a_known_vector_is_the_castagnoli_value() {
        // Anchors the polynomial to a published constant rather than only to
        // another implementation in this repository.
        assert_eq!(payload_crc32c(b"123456789"), 0xE306_9283);
    }
}
