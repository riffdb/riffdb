//! Coordinator-owned source for service UUID observations.

use std::{error::Error, fmt};

/// Supplies UUIDv7 bytes before deterministic command evaluation.
///
/// Implementations belong to server composition. The runtime never receives
/// this port; it receives only values sealed into durable admission evidence.
pub trait ServiceUuidV7Source: Send + Sync {
    /// Returns one fresh, structurally valid UUIDv7 candidate.
    fn next_uuid_v7(&self) -> Result<[u8; 16], ServiceUuidV7SourceError>;
}

/// Redaction-safe failure to observe one service UUID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceUuidV7SourceError;

impl fmt::Display for ServiceUuidV7SourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("service UUID source is unavailable")
    }
}

impl Error for ServiceUuidV7SourceError {}
