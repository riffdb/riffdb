//! Strict validation for versioned durable Protobuf envelopes.

use std::{error::Error, fmt};

use crc::{CRC_32_ISCSI, Crc};
use prost::Message;
use riffdb_types::{SchemaHash, hash_schema};

use crate::storage::v1::StoredEnvelope;

/// The only storage-envelope format version supported by the POC baseline.
pub const STORAGE_FORMAT_VERSION_V1: u32 = 1;

/// Absolute upper bound for an encoded envelope or its payload.
pub const MAX_STORED_ENVELOPE_BYTES: usize = 16 * 1024 * 1024;

/// Maximum byte length of a fully qualified durable Protobuf message name.
pub const MAX_RECORD_TYPE_BYTES: usize = 256;

/// Maximum number of entries accepted in one closed record registry.
pub const MAX_REGISTERED_RECORD_SCHEMAS: usize = 256;

const CRC_32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

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

/// Errors in a statically configured durable-record schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordSchemaError {
    /// The record name is not a fully qualified Protobuf message name.
    InvalidRecordType,
    /// The canonical descriptor set exceeds the implementation hard limit.
    DescriptorSetTooLarge,
    /// The record-specific payload limit exceeds the absolute envelope limit.
    InvalidPayloadLimit,
}

impl fmt::Display for RecordSchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRecordType => "durable record type is invalid",
            Self::DescriptorSetTooLarge => "durable descriptor set is too large",
            Self::InvalidPayloadLimit => "durable payload limit is invalid",
        })
    }
}

impl Error for RecordSchemaError {}

/// One supported `(version, record type, schema hash)` registry entry.
#[derive(Clone, Copy)]
pub struct RecordSchema<'a> {
    record_type: &'a str,
    schema_hash: SchemaHash,
    max_payload_bytes: usize,
    validate_payload: fn(&[u8]) -> Result<(), PayloadValidationError>,
}

impl<'a> RecordSchema<'a> {
    /// Creates a v1 registry entry from a canonical, source-info-stripped
    /// transitive descriptor set.
    pub fn new(
        record_type: &'a str,
        canonical_descriptor_set: &[u8],
        max_payload_bytes: usize,
        validate_payload: fn(&[u8]) -> Result<(), PayloadValidationError>,
    ) -> Result<Self, RecordSchemaError> {
        if max_payload_bytes > MAX_STORED_ENVELOPE_BYTES {
            return Err(RecordSchemaError::InvalidPayloadLimit);
        }

        let schema_hash = durable_schema_hash(record_type, canonical_descriptor_set)?;
        Ok(Self {
            record_type,
            schema_hash,
            max_payload_bytes,
            validate_payload,
        })
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

    /// Returns the record-specific payload ceiling.
    #[must_use]
    pub const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }
}

impl fmt::Debug for RecordSchema<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordSchema")
            .field("record_type", &self.record_type)
            .field("schema_hash", &self.schema_hash)
            .field("max_payload_bytes", &self.max_payload_bytes)
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
}

impl fmt::Display for RecordRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TooManySchemas => "durable record registry is too large",
            Self::DuplicateSchema => "durable record registry contains a duplicate schema",
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
            if schemas[..index].iter().any(|existing| {
                existing.record_type == schema.record_type
                    && existing.schema_hash == schema.schema_hash
            }) {
                return Err(RecordRegistryError::DuplicateSchema);
            }
        }

        Ok(Self { schemas })
    }

    /// Strictly decodes an envelope through its registered semantic validator.
    pub fn decode(&self, encoded: &[u8]) -> Result<DecodedEnvelope, EnvelopeError> {
        if encoded.len() > MAX_STORED_ENVELOPE_BYTES {
            return Err(EnvelopeError::EnvelopeTooLarge);
        }

        let envelope = StoredEnvelope::decode(encoded).map_err(|_| EnvelopeError::Malformed)?;

        if envelope.storage_format_version != STORAGE_FORMAT_VERSION_V1 {
            return Err(EnvelopeError::UnsupportedStorageFormatVersion);
        }

        validate_record_type(&envelope.record_type)
            .map_err(|_| EnvelopeError::InvalidRecordType)?;

        if !self
            .schemas
            .iter()
            .any(|schema| schema.record_type == envelope.record_type)
        {
            return Err(EnvelopeError::UnknownRecordType);
        }

        let encoded_schema_hash: [u8; 32] = envelope
            .schema_hash
            .as_slice()
            .try_into()
            .map_err(|_| EnvelopeError::InvalidSchemaHashLength)?;
        let schema = self
            .schemas
            .iter()
            .filter(|schema| schema.record_type == envelope.record_type)
            .find(|schema| schema.schema_hash.as_bytes() == &encoded_schema_hash)
            .ok_or(EnvelopeError::UnsupportedSchemaHash)?;

        if envelope.payload.len() > MAX_STORED_ENVELOPE_BYTES
            || envelope.payload.len() > schema.max_payload_bytes
        {
            return Err(EnvelopeError::PayloadTooLarge);
        }

        if payload_crc32c(&envelope.payload) != envelope.payload_crc32c {
            return Err(EnvelopeError::ChecksumMismatch);
        }

        if envelope.encode_to_vec() != encoded {
            return Err(EnvelopeError::NonCanonicalEnvelope);
        }

        (schema.validate_payload)(&envelope.payload).map_err(EnvelopeError::InvalidPayload)?;

        Ok(DecodedEnvelope {
            record_type: envelope.record_type,
            schema_hash: schema.schema_hash,
            payload: envelope.payload,
        })
    }
}

/// A supported, integrity-checked, semantically canonical durable payload.
pub struct DecodedEnvelope {
    record_type: String,
    schema_hash: SchemaHash,
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
            Self::PayloadTooLarge => "durable payload is too large",
            Self::ChecksumMismatch => "durable payload checksum does not match",
            Self::NonCanonicalEnvelope => "durable envelope encoding is not canonical",
            Self::InvalidPayload(error) => return error.fmt(formatter),
        })
    }
}

impl Error for EnvelopeError {}

/// Encodes one semantically canonical payload in its registered v1 envelope.
pub fn encode(schema: &RecordSchema<'_>, payload: &[u8]) -> Result<Vec<u8>, EnvelopeError> {
    if payload.len() > MAX_STORED_ENVELOPE_BYTES || payload.len() > schema.max_payload_bytes {
        return Err(EnvelopeError::PayloadTooLarge);
    }
    (schema.validate_payload)(payload).map_err(EnvelopeError::InvalidPayload)?;

    let envelope = StoredEnvelope {
        storage_format_version: STORAGE_FORMAT_VERSION_V1,
        record_type: schema.record_type.to_owned(),
        payload: payload.to_vec(),
        payload_crc32c: payload_crc32c(payload),
        schema_hash: schema.schema_hash.as_bytes().to_vec(),
    };
    let encoded = envelope.encode_to_vec();
    if encoded.len() > MAX_STORED_ENVELOPE_BYTES {
        return Err(EnvelopeError::EnvelopeTooLarge);
    }
    Ok(encoded)
}

/// Computes CRC-32C/Castagnoli over the exact payload bytes.
#[must_use]
pub fn payload_crc32c(payload: &[u8]) -> u32 {
    CRC_32C.checksum(payload)
}

/// Computes the accepted schema-domain hash for one durable record descriptor.
pub fn durable_schema_hash(
    record_type: &str,
    canonical_descriptor_set: &[u8],
) -> Result<SchemaHash, RecordSchemaError> {
    validate_record_type(record_type)?;
    if canonical_descriptor_set.len() > MAX_STORED_ENVELOPE_BYTES {
        return Err(RecordSchemaError::DescriptorSetTooLarge);
    }

    let record_type_length =
        u16::try_from(record_type.len()).map_err(|_| RecordSchemaError::InvalidRecordType)?;
    let descriptor_set_length = u64::try_from(canonical_descriptor_set.len())
        .map_err(|_| RecordSchemaError::DescriptorSetTooLarge)?;
    let frame_capacity = 2usize
        .checked_add(record_type.len())
        .and_then(|length| length.checked_add(8))
        .and_then(|length| length.checked_add(canonical_descriptor_set.len()))
        .ok_or(RecordSchemaError::DescriptorSetTooLarge)?;
    let mut frame = Vec::with_capacity(frame_capacity);
    frame.extend_from_slice(&record_type_length.to_be_bytes());
    frame.extend_from_slice(record_type.as_bytes());
    frame.extend_from_slice(&descriptor_set_length.to_be_bytes());
    frame.extend_from_slice(canonical_descriptor_set);
    Ok(hash_schema(&frame))
}

fn validate_record_type(record_type: &str) -> Result<(), RecordSchemaError> {
    if record_type.is_empty()
        || record_type.len() > MAX_RECORD_TYPE_BYTES
        || !record_type.is_ascii()
        || !record_type.contains('.')
        || record_type.split('.').any(|segment| {
            let mut bytes = segment.bytes();
            !matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
                || bytes.any(|byte| !matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'))
        })
    {
        return Err(RecordSchemaError::InvalidRecordType);
    }
    Ok(())
}
