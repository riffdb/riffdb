//! Safe failures at the durable semantic Protobuf boundary.

use std::{error::Error, fmt};

use riffdb_proto::envelope::{EnvelopeError, PayloadValidationError};

use crate::StorageValueError;

/// Closed, payload-safe durable codec failure classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableCodecErrorKind {
    /// The envelope version, record type, or schema hash is unsupported.
    IncompatibleFormat,
    /// Durable bytes do not reconstruct one canonical semantic value.
    CorruptData,
    /// A durable hard size or collection bound was exceeded.
    LimitExceeded,
    /// A checked in-memory value could not be encoded as its accepted schema.
    InvariantViolation,
    /// Actual canonical bytes exceeded a retained pre-sequence reservation.
    ReservationExceeded,
    /// A typed decoder was given a different registered record type.
    UnexpectedRecordType,
}

/// A redacted durable codec failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableCodecError {
    kind: DurableCodecErrorKind,
}

impl DurableCodecError {
    /// Constructs a safe closed failure.
    #[must_use]
    pub const fn new(kind: DurableCodecErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the closed failure class.
    #[must_use]
    pub const fn kind(self) -> DurableCodecErrorKind {
        self.kind
    }

    pub(super) const fn corrupt() -> Self {
        Self::new(DurableCodecErrorKind::CorruptData)
    }

    pub(super) const fn invariant() -> Self {
        Self::new(DurableCodecErrorKind::InvariantViolation)
    }

    pub(super) const fn from_storage_value(error: StorageValueError) -> Self {
        match error {
            StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
                Self::new(DurableCodecErrorKind::LimitExceeded)
            }
            StorageValueError::Empty
            | StorageValueError::NonCanonicalOrder
            | StorageValueError::Duplicate
            | StorageValueError::IdentityMismatch
            | StorageValueError::InvalidShape => Self::corrupt(),
        }
    }

    pub(super) const fn from_decode_envelope(error: EnvelopeError) -> Self {
        match error {
            EnvelopeError::UnsupportedStorageFormatVersion
            | EnvelopeError::UnknownRecordType
            | EnvelopeError::UnsupportedSchemaHash => {
                Self::new(DurableCodecErrorKind::IncompatibleFormat)
            }
            EnvelopeError::EnvelopeTooLarge
            | EnvelopeError::PayloadTooLarge
            | EnvelopeError::InvalidPayload(PayloadValidationError::LimitExceeded) => {
                Self::new(DurableCodecErrorKind::LimitExceeded)
            }
            EnvelopeError::Malformed
            | EnvelopeError::InvalidRecordType
            | EnvelopeError::InvalidSchemaHashLength
            | EnvelopeError::ChecksumMismatch
            | EnvelopeError::NonCanonicalEnvelope
            | EnvelopeError::InvalidPayload(PayloadValidationError::Malformed)
            | EnvelopeError::InvalidPayload(PayloadValidationError::NonCanonical) => {
                Self::corrupt()
            }
        }
    }

    pub(super) const fn from_encode_envelope(error: EnvelopeError) -> Self {
        match error {
            EnvelopeError::EnvelopeTooLarge
            | EnvelopeError::PayloadTooLarge
            | EnvelopeError::InvalidPayload(PayloadValidationError::LimitExceeded) => {
                Self::new(DurableCodecErrorKind::LimitExceeded)
            }
            EnvelopeError::Malformed
            | EnvelopeError::UnsupportedStorageFormatVersion
            | EnvelopeError::UnknownRecordType
            | EnvelopeError::InvalidRecordType
            | EnvelopeError::InvalidSchemaHashLength
            | EnvelopeError::UnsupportedSchemaHash
            | EnvelopeError::ChecksumMismatch
            | EnvelopeError::NonCanonicalEnvelope
            | EnvelopeError::InvalidPayload(PayloadValidationError::Malformed)
            | EnvelopeError::InvalidPayload(PayloadValidationError::NonCanonical) => {
                Self::invariant()
            }
        }
    }
}

impl fmt::Display for DurableCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            DurableCodecErrorKind::IncompatibleFormat => "durable record format is incompatible",
            DurableCodecErrorKind::CorruptData => "durable record is corrupt",
            DurableCodecErrorKind::LimitExceeded => "durable record exceeds a hard limit",
            DurableCodecErrorKind::InvariantViolation => {
                "checked durable value violates its codec invariant"
            }
            DurableCodecErrorKind::ReservationExceeded => {
                "durable record exceeds its retained reservation"
            }
            DurableCodecErrorKind::UnexpectedRecordType => {
                "durable record has an unexpected registered type"
            }
        })
    }
}

impl Error for DurableCodecError {}
