//! Least-authority publication boundary for durable application commits.

use std::{error::Error, fmt};

use riffdb_types::CommitSequence;

/// Closed, redaction-safe failure to publish one process-local notification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationCommitNotificationError;

impl fmt::Display for ApplicationCommitNotificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application commit notification publication failed")
    }
}

impl Error for ApplicationCommitNotificationError {}

/// Process-local sink for the sequence of a first durable application commit.
///
/// The coordinator is the sole caller. Implementations receive no command,
/// principal, entity, outcome, or storage access and must return promptly.
pub trait ApplicationCommitNotificationSink: Send + Sync {
    /// Publishes one first-commit sequence after durable completion.
    fn publish_first_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<(), ApplicationCommitNotificationError>;
}
