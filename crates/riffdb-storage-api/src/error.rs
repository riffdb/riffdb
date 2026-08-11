//! Closed storage failures and safe semantic-construction failures.

use std::error::Error;
use std::fmt;

use riffdb_types::{CanonicalCodecError, CapabilityGrantError, IncidentId};

/// The closed engine-neutral storage failure taxonomy.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StorageErrorKind {
    /// The backend proved that the requested operation did not commit.
    Unavailable,
    /// The backend cannot prove whether a commit became durable.
    CommitStatusUnknown,
    /// Durable bytes or cross-record structure are corrupt.
    CorruptData,
    /// A storage, key, record, or schema version is unsupported.
    IncompatibleFormat,
    /// A semantic storage hard bound was exceeded.
    LimitExceeded,
    /// An impossible typed transition or structural state was requested.
    InvariantViolation,
    /// A sequence or epoch cannot advance without wrapping.
    SequenceExhausted,
    /// Requested history was retired by retention and is no longer retained.
    HistoryPruned,
}

impl StorageErrorKind {
    /// Returns stable, public-safe diagnostic text.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::Unavailable => "storage operation was not committed",
            Self::CommitStatusUnknown => "storage commit status is unknown",
            Self::CorruptData => "stored data is corrupt",
            Self::IncompatibleFormat => "stored data uses an incompatible format",
            Self::LimitExceeded => "storage semantic limit exceeded",
            Self::InvariantViolation => "storage invariant violated",
            Self::SequenceExhausted => "storage sequence is exhausted",
            Self::HistoryPruned => "requested history has been pruned",
        }
    }
}

/// A safe failure crossing the semantic storage boundary.
#[derive(Clone, Eq, PartialEq)]
pub struct StorageError {
    kind: StorageErrorKind,
    incident_id: Option<IncidentId>,
}

impl StorageError {
    /// Constructs a closed storage failure with optional opaque correlation.
    #[must_use]
    pub const fn new(kind: StorageErrorKind, incident_id: Option<IncidentId>) -> Self {
        Self { kind, incident_id }
    }

    /// Returns the closed failure kind.
    #[must_use]
    pub const fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    /// Returns the optional opaque incident identity.
    #[must_use]
    pub const fn incident_id(&self) -> Option<IncidentId> {
        self.incident_id
    }
}

impl fmt::Debug for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageError")
            .field("kind", &self.kind)
            .field("incident_id", &self.incident_id)
            .finish()
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())?;
        if let Some(incident_id) = self.incident_id {
            write!(formatter, " (incident {incident_id})")?;
        }
        Ok(())
    }
}

impl Error for StorageError {}

/// A safe failure to construct a bounded semantic storage value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageValueError {
    /// A supplied collection or byte document is empty when one item is required.
    Empty,
    /// A supplied count or encoded size exceeds its accepted bound.
    LimitExceeded,
    /// Items are not in the required canonical order.
    NonCanonicalOrder,
    /// The same semantic identity occurs more than once.
    Duplicate,
    /// Related identities, keys, plans, or records do not agree.
    IdentityMismatch,
    /// A value has an invalid closed tag, shape, or combination.
    InvalidShape,
    /// Checked byte accounting overflowed.
    SizeOverflow,
}

impl fmt::Display for StorageValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "required storage value is empty",
            Self::LimitExceeded => "storage value exceeds a hard limit",
            Self::NonCanonicalOrder => "storage value is not canonically ordered",
            Self::Duplicate => "storage value contains a duplicate identity",
            Self::IdentityMismatch => "related storage identities do not match",
            Self::InvalidShape => "storage value has an invalid semantic shape",
            Self::SizeOverflow => "storage value size calculation overflowed",
        })
    }
}

impl Error for StorageValueError {}

impl From<CapabilityGrantError> for StorageValueError {
    fn from(error: CapabilityGrantError) -> Self {
        match error {
            CapabilityGrantError::Empty => Self::Empty,
            CapabilityGrantError::LimitExceeded => Self::LimitExceeded,
            CapabilityGrantError::Duplicate => Self::Duplicate,
            CapabilityGrantError::InvalidShape => Self::InvalidShape,
            CapabilityGrantError::SizeOverflow => Self::SizeOverflow,
        }
    }
}

pub(crate) const fn canonical_codec_storage_error(
    error: &CanonicalCodecError,
) -> StorageValueError {
    match error {
        CanonicalCodecError::DocumentTooLarge { .. } => StorageValueError::LimitExceeded,
        CanonicalCodecError::StringTooLarge { .. }
        | CanonicalCodecError::BytesTooLarge { .. }
        | CanonicalCodecError::TooManyEntries { .. }
        | CanonicalCodecError::NestingTooDeep { .. }
        | CanonicalCodecError::UnsupportedVersion { .. }
        | CanonicalCodecError::UnknownTag { .. }
        | CanonicalCodecError::InvalidBoolean { .. }
        | CanonicalCodecError::InvalidDecimal
        | CanonicalCodecError::InvalidCurrency
        | CanonicalCodecError::InvalidTimestamp
        | CanonicalCodecError::InvalidUtf8
        | CanonicalCodecError::NonCanonicalRecordOrder
        | CanonicalCodecError::ZeroEnumTypeId
        | CanonicalCodecError::ZeroEnumVariantId
        | CanonicalCodecError::ZeroFieldId
        | CanonicalCodecError::UnexpectedEnd
        | CanonicalCodecError::TrailingBytes { .. }
        | CanonicalCodecError::VectorDimensionOutOfRange { .. }
        | CanonicalCodecError::NonCanonicalVectorComponent { .. } => {
            StorageValueError::InvalidShape
        }
    }
}
