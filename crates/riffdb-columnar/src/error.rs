//! Typed engine errors safe to expose at library boundaries.

use std::fmt;

use riffdb_storage_api::StorageErrorKind;
use riffdb_types::IncidentId;

use crate::checkpoint::CheckpointError;
use crate::definition::DefinitionError;
use crate::query::QueryError;

/// Structured storage failure: closed kind plus optional redacted correlation.
///
/// Never carries a stringified [`riffdb_storage_api::StorageError`]; public
/// text is always the kind's safe message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageFailure {
    kind: StorageErrorKind,
    incident_id: Option<IncidentId>,
}

impl StorageFailure {
    /// Builds a structured storage failure from a storage-api error.
    #[must_use]
    pub fn from_storage_error(error: &riffdb_storage_api::StorageError) -> Self {
        Self {
            kind: error.kind(),
            incident_id: error.incident_id(),
        }
    }

    /// Builds from an explicit kind (tests and synthetic paths).
    #[must_use]
    pub const fn from_kind(kind: StorageErrorKind, incident_id: Option<IncidentId>) -> Self {
        Self { kind, incident_id }
    }

    /// Closed storage failure kind.
    #[must_use]
    pub const fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    /// Optional opaque incident identity (never a storage payload dump).
    #[must_use]
    pub const fn incident_id(&self) -> Option<IncidentId> {
        self.incident_id
    }
}

impl fmt::Display for StorageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.kind.safe_message())?;
        if let Some(incident_id) = self.incident_id {
            write!(f, " (incident {incident_id})")?;
        }
        Ok(())
    }
}

/// Closed failure classes for the columnar engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnarError {
    /// Definition registration or fingerprint validation failed.
    Definition(DefinitionError),
    /// Query planning or execution failed.
    Query(QueryError),
    /// Checkpoint or segment durability failed.
    Checkpoint(CheckpointError),
    /// Authoritative storage read failed (structured; never a raw string dump).
    Storage(StorageFailure),
    /// Commit log integrity failed (gap, fence mismatch, shape error).
    Integrity(&'static str),
    /// Entity post-image could not be projected (missing field, encode failure).
    Projection(&'static str),
    /// I/O failed outside a typed checkpoint path.
    Io(String),
    /// Engine is in an invalid lifecycle state for the requested operation.
    InvalidState(&'static str),
}

impl fmt::Display for ColumnarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Definition(error) => write!(f, "columnar definition error: {error}"),
            Self::Query(error) => write!(f, "columnar query error: {error}"),
            Self::Checkpoint(error) => write!(f, "columnar checkpoint error: {error}"),
            Self::Storage(failure) => write!(f, "columnar storage error: {failure}"),
            Self::Integrity(message) => write!(f, "columnar integrity error: {message}"),
            Self::Projection(message) => write!(f, "columnar projection error: {message}"),
            Self::Io(message) => write!(f, "columnar I/O error: {message}"),
            Self::InvalidState(message) => write!(f, "columnar invalid state: {message}"),
        }
    }
}

impl std::error::Error for ColumnarError {}

impl From<DefinitionError> for ColumnarError {
    fn from(value: DefinitionError) -> Self {
        Self::Definition(value)
    }
}

impl From<QueryError> for ColumnarError {
    fn from(value: QueryError) -> Self {
        Self::Query(value)
    }
}

impl From<CheckpointError> for ColumnarError {
    fn from(value: CheckpointError) -> Self {
        Self::Checkpoint(value)
    }
}

impl From<std::io::Error> for ColumnarError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<riffdb_storage_api::StorageError> for ColumnarError {
    fn from(value: riffdb_storage_api::StorageError) -> Self {
        Self::Storage(StorageFailure::from_storage_error(&value))
    }
}
