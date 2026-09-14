//! Read-only, bounded receipt access from one published durable snapshot.

use super::{AuthoritativeTransactionV3, ChangelogHistoryStateV3};
use crate::StorageError;

/// A value-free refusal of a receipt cursor or its next complete row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChangelogCursorErrorV3 {
    /// The requested database or history incarnation differs from the pin.
    ForeignLineage,
    /// The requested leadership epoch differs from the pin.
    StaleEpoch,
    /// The resume position is beyond the pin or substitutes its hash/frontier.
    InvalidPosition,
    /// Closed backend refusal; pruned history uses `StorageErrorKind::HistoryPruned`.
    Storage(StorageError),
}

impl From<StorageError> for ChangelogCursorErrorV3 {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl std::fmt::Display for ChangelogCursorErrorV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ForeignLineage => "foreign changelog lineage",
            Self::StaleEpoch => "stale changelog epoch",
            Self::InvalidPosition => "invalid changelog resume position",
            Self::Storage(_) => "changelog storage refusal",
        })
    }
}

impl std::error::Error for ChangelogCursorErrorV3 {}

/// A cursor pinned to one published history, never a live or writer-private view.
/// Each call returns at most one complete frame-bounded receipt; no transaction
/// is split. End is exact at `history().tail()`, including allocator exhaustion.
/// Implementations are fused: after failure they keep refusing, never skip a row.
/// Opening and advancing must not acquire an authoritative mutation lease or
/// change durability. Dropping releases only the immutable read capabilities.
pub trait ChangelogReceiptCursorV3: Send {
    /// The immutable lineage, retained resume floor and covered tail of this pin.
    fn history(&self) -> ChangelogHistoryStateV3;

    /// Returns the exact successor receipt, or exact end. Bytes, keys and hashes
    /// must never appear in diagnostics. Backends validate continuity before return.
    fn next_receipt(
        &mut self,
    ) -> Result<Option<AuthoritativeTransactionV3>, ChangelogCursorErrorV3>;
}
