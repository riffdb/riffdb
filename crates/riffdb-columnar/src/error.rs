//! Typed engine errors safe to expose at library boundaries.

use std::fmt;

use crate::checkpoint::CheckpointError;
use crate::definition::DefinitionError;
use crate::query::QueryError;

/// Closed failure classes for the columnar engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnarError {
    /// Definition registration or fingerprint validation failed.
    Definition(DefinitionError),
    /// Query planning or execution failed.
    Query(QueryError),
    /// Checkpoint or segment durability failed.
    Checkpoint(CheckpointError),
    /// Authoritative storage read failed.
    Storage(String),
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
            Self::Storage(message) => write!(f, "columnar storage error: {message}"),
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
