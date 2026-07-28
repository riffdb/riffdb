//! Closed, redaction-safe projection orchestration failures.

use std::error::Error;
use std::fmt;

use riffdb_storage_api::{StorageError, StorageErrorKind, StorageValueError};

/// Stable public-safe classification for projection-core failures.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProjectionCoreErrorKind {
    /// A bounded storage operation is temporarily unavailable.
    StorageUnavailable,
    /// Storage cannot yet prove whether a derived-state commit completed.
    CommitStatusUnknown,
    /// Durable or process-local projection evidence is inconsistent.
    Integrity,
    /// A fixed projection resource limit was exceeded.
    LimitExceeded,
    /// The nonzero generation space is exhausted.
    GenerationExhausted,
    /// The bounded waiter registry has no remaining capacity.
    WaiterCapacityExceeded,
}

impl ProjectionCoreErrorKind {
    /// Returns fixed safe text without identities, keys, values, or engine text.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::StorageUnavailable => "projection storage is unavailable",
            Self::CommitStatusUnknown => "projection commit status is unknown",
            Self::Integrity => "projection state integrity failure",
            Self::LimitExceeded => "projection resource limit exceeded",
            Self::GenerationExhausted => "projection generation space exhausted",
            Self::WaiterCapacityExceeded => "projection waiter capacity exceeded",
        }
    }
}

/// A typed projection failure that never retains an engine error string.
#[derive(Clone, Eq, PartialEq)]
pub struct ProjectionCoreError {
    kind: ProjectionCoreErrorKind,
}

impl ProjectionCoreError {
    /// Constructs an error from its closed safe classification.
    #[must_use]
    pub const fn new(kind: ProjectionCoreErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the closed safe classification.
    #[must_use]
    pub const fn kind(&self) -> ProjectionCoreErrorKind {
        self.kind
    }
}

impl fmt::Debug for ProjectionCoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionCoreError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for ProjectionCoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for ProjectionCoreError {}

impl From<StorageError> for ProjectionCoreError {
    fn from(error: StorageError) -> Self {
        let kind = match error.kind() {
            StorageErrorKind::Unavailable => ProjectionCoreErrorKind::StorageUnavailable,
            StorageErrorKind::CommitStatusUnknown => ProjectionCoreErrorKind::CommitStatusUnknown,
            StorageErrorKind::LimitExceeded => ProjectionCoreErrorKind::LimitExceeded,
            StorageErrorKind::CorruptData
            | StorageErrorKind::IncompatibleFormat
            | StorageErrorKind::InvariantViolation
            | StorageErrorKind::SequenceExhausted => ProjectionCoreErrorKind::Integrity,
        };
        Self::new(kind)
    }
}

impl From<StorageValueError> for ProjectionCoreError {
    fn from(error: StorageValueError) -> Self {
        let kind = match error {
            StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
                ProjectionCoreErrorKind::LimitExceeded
            }
            StorageValueError::Empty
            | StorageValueError::InvalidShape
            | StorageValueError::Duplicate
            | StorageValueError::NonCanonicalOrder
            | StorageValueError::IdentityMismatch => ProjectionCoreErrorKind::Integrity,
        };
        Self::new(kind)
    }
}
