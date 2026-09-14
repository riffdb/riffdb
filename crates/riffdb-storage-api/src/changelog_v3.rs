//! Closed authoritative changelog V3 substrate values (ADR-0186).
//!
//! These are internal storage contracts, not application command inputs. The
//! coordinator remains the only owner of authoritative mutation and ordering.

mod cursor;
mod frame;
mod history;
mod leadership;
mod mutation;
mod receipt;
mod receipt_codec;
mod sequence;
mod source_hold;
mod state_cursor;

pub use cursor::{ChangelogCursorErrorV3, ChangelogReceiptCursorV3};
pub use frame::{ChangelogFrameBindingV3, ChangelogFrameV3};
pub use history::{
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ReplicationFollowerStateV3,
};
pub use leadership::LeadershipEpochV1;
pub use mutation::{AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3};
pub use receipt::{
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
};
pub use sequence::{ChangelogTransactionAllocator, ChangelogTransactionSequence};
pub use source_hold::{
    MAX_REPLICATION_SOURCE_HOLDS_V1, ReplicationSourceHoldIdV1, ReplicationSourceHoldKindV1,
    ReplicationSourceHoldV1,
};
pub use state_cursor::{
    AuthoritativeStateCursorV3, AuthoritativeStateRowV3, AuthoritativeStateStepV3,
};

/// A bounded, value-free V3 refusal. Never includes keys, values or hashes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangelogV3Error {
    /// Unknown or malformed encoding, including noncanonical absence.
    InvalidEncoding,
    /// A local, control, or unknown namespace cannot be an authoritative mutation.
    InvalidNamespace,
    /// An accepted frame, transaction, key, value or collection ceiling was exceeded.
    LimitExceeded,
    /// The physical transaction allocator has no representable successor.
    SequenceExhausted,
    /// The expected predecessor state does not match the observed state.
    PredecessorMismatch,
}

impl std::fmt::Display for ChangelogV3Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidEncoding => "invalid changelog V3 encoding",
            Self::InvalidNamespace => "invalid changelog V3 namespace",
            Self::LimitExceeded => "changelog V3 bound exceeded",
            Self::SequenceExhausted => "changelog transaction sequence exhausted",
            Self::PredecessorMismatch => "changelog V3 predecessor mismatch",
        })
    }
}

impl std::error::Error for ChangelogV3Error {}
