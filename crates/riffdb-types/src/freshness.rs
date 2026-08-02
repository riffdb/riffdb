//! Opaque commit tokens, projection frontiers, and freshness policies (ADR-0086 §3).
//!
//! Wire carriage of these types is a separate protocol concern. This module owns
//! only the semantic identities: each token/frontier binds a history incarnation
//! so a restore can never satisfy a stale pre-restore token.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use crate::{CommitSequence, FrontierPosition};

/// Opaque encoding version for [`CommitToken`] and [`ProjectionFrontier`] bytes.
const FRESHNESS_BYTES_V1: u8 = 0x01;

/// Position tag: no commit has been applied.
const POSITION_BEFORE_FIRST: u8 = 0x00;
/// Position tag: applied through an exact nonzero commit sequence.
const POSITION_APPLIED_THROUGH: u8 = 0x01;

/// An opaque, incarnation-bound commit identity used as a causal freshness fence.
///
/// Internally `(history_incarnation, commit_sequence)`. Public diagnostics redact
/// the payload; callers that need wire bytes use [`CommitToken::as_bytes`].
#[derive(Clone)]
pub struct CommitToken {
    bytes: Vec<u8>,
    history_incarnation: u64,
    commit_sequence: CommitSequence,
}

impl CommitToken {
    /// Builds a token for one exact commit under a history incarnation.
    #[must_use]
    pub fn new(history_incarnation: u64, commit_sequence: CommitSequence) -> Self {
        let mut bytes = Vec::with_capacity(1 + 8 + 1 + 8);
        bytes.push(FRESHNESS_BYTES_V1);
        bytes.extend_from_slice(&history_incarnation.to_be_bytes());
        bytes.push(POSITION_APPLIED_THROUGH);
        bytes.extend_from_slice(&commit_sequence.to_be_bytes());
        Self {
            bytes,
            history_incarnation,
            commit_sequence,
        }
    }

    /// Parses opaque token bytes produced by [`Self::as_bytes`].
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, FreshnessTokenError> {
        let (history_incarnation, position) = decode_frontier_payload(&bytes)?;
        let FrontierPosition::AppliedThrough(commit_sequence) = position else {
            return Err(FreshnessTokenError::InvalidShape);
        };
        Ok(Self {
            bytes,
            history_incarnation,
            commit_sequence,
        })
    }

    /// History incarnation bound into the token.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Commit sequence bound into the token.
    #[must_use]
    pub const fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }

    /// Opaque bytes for wire carriage (CP2b protocol) and storage.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consumes the token and returns its opaque bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// An opaque, incarnation-bound projection visibility frontier.
///
/// Internally `(history_incarnation, FrontierPosition)`. Comparison and
/// [`ProjectionFrontier::satisfies`] require equal incarnations; a restore that
/// bumps the incarnation can never satisfy a pre-restore causal token.
#[derive(Clone)]
pub struct ProjectionFrontier {
    bytes: Vec<u8>,
    history_incarnation: u64,
    position: FrontierPosition,
}

impl ProjectionFrontier {
    /// Builds a frontier at `position` under a history incarnation.
    #[must_use]
    pub fn new(history_incarnation: u64, position: FrontierPosition) -> Self {
        let mut bytes = Vec::with_capacity(1 + 8 + 1 + 8);
        bytes.push(FRESHNESS_BYTES_V1);
        bytes.extend_from_slice(&history_incarnation.to_be_bytes());
        match position {
            FrontierPosition::BeforeFirst => {
                bytes.push(POSITION_BEFORE_FIRST);
            }
            FrontierPosition::AppliedThrough(sequence) => {
                bytes.push(POSITION_APPLIED_THROUGH);
                bytes.extend_from_slice(&sequence.to_be_bytes());
            }
        }
        Self {
            bytes,
            history_incarnation,
            position,
        }
    }

    /// Parses opaque frontier bytes produced by [`Self::as_bytes`].
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, FreshnessTokenError> {
        let (history_incarnation, position) = decode_frontier_payload(&bytes)?;
        Ok(Self {
            bytes,
            history_incarnation,
            position,
        })
    }

    /// History incarnation bound into the frontier.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Sequence position of the frontier (may be [`FrontierPosition::BeforeFirst`]).
    #[must_use]
    pub const fn position(&self) -> FrontierPosition {
        self.position
    }

    /// Opaque bytes for wire carriage (CP2b protocol) and storage.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consumes the frontier and returns its opaque bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Whether this frontier covers `required` under the same history incarnation.
    ///
    /// Incarnation mismatch always fails closed (restore must not satisfy a
    /// pre-restore causal token).
    #[must_use]
    pub fn satisfies(&self, required: &CommitToken) -> bool {
        if self.history_incarnation != required.history_incarnation {
            return false;
        }
        match self.position {
            FrontierPosition::BeforeFirst => false,
            FrontierPosition::AppliedThrough(current) => {
                current.get() >= required.commit_sequence.get()
            }
        }
    }

    /// Sequence distance from this frontier to `head` when both share an
    /// incarnation and are sequenced.
    #[must_use]
    pub fn lag_sequences(&self, head: &Self) -> Option<u64> {
        if self.history_incarnation != head.history_incarnation {
            return None;
        }
        match (self.position, head.position) {
            (FrontierPosition::BeforeFirst, FrontierPosition::AppliedThrough(head_seq)) => {
                Some(head_seq.get())
            }
            (
                FrontierPosition::AppliedThrough(current_seq),
                FrontierPosition::AppliedThrough(head_seq),
            ) => head_seq.get().checked_sub(current_seq.get()),
            _ => None,
        }
    }
}

/// Projected-query freshness policy (ADR-0086 §3).
///
/// Wait/retry loops and wall clocks live outside the deterministic engine; the
/// policy is a pure data shape the service layer interprets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FreshnessPolicy {
    /// Serve only once the projection frontier satisfies `token`.
    Causal {
        /// Required causal fence.
        token: CommitToken,
        /// Maximum wait budget interpreted by the service wait loop.
        max_wait: Duration,
    },
    /// Serve only if head-to-frontier lag is within `max_lag_sequences`.
    Bounded {
        /// Maximum allowed sequence distance (head − frontier).
        max_lag_sequences: u64,
    },
    /// Serve the current published snapshot, reporting its frontier.
    Available,
}

/// Safe failure to construct or decode a freshness token or frontier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FreshnessTokenError {
    /// Bytes are truncated or carry a trailing suffix.
    TruncatedOrTrailing,
    /// Version, position tag, or sequence shape is invalid.
    InvalidShape,
    /// Commit sequence was the zero sentinel.
    ZeroCommitSequence,
}

impl fmt::Display for FreshnessTokenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TruncatedOrTrailing => "freshness token bytes are truncated or trailing",
            Self::InvalidShape => "freshness token bytes have an invalid shape",
            Self::ZeroCommitSequence => "commit sequence must be nonzero",
        })
    }
}

impl Error for FreshnessTokenError {}

fn decode_frontier_payload(bytes: &[u8]) -> Result<(u64, FrontierPosition), FreshnessTokenError> {
    if bytes.len() < 1 + 8 + 1 {
        return Err(FreshnessTokenError::TruncatedOrTrailing);
    }
    if bytes[0] != FRESHNESS_BYTES_V1 {
        return Err(FreshnessTokenError::InvalidShape);
    }
    let history_incarnation = u64::from_be_bytes(bytes[1..9].try_into().expect("length checked"));
    match bytes[9] {
        POSITION_BEFORE_FIRST => {
            if bytes.len() != 10 {
                return Err(FreshnessTokenError::TruncatedOrTrailing);
            }
            Ok((history_incarnation, FrontierPosition::BeforeFirst))
        }
        POSITION_APPLIED_THROUGH => {
            if bytes.len() != 18 {
                return Err(FreshnessTokenError::TruncatedOrTrailing);
            }
            let sequence = u64::from_be_bytes(bytes[10..18].try_into().expect("length checked"));
            let commit_sequence =
                CommitSequence::new(sequence).ok_or(FreshnessTokenError::ZeroCommitSequence)?;
            Ok((
                history_incarnation,
                FrontierPosition::AppliedThrough(commit_sequence),
            ))
        }
        _ => Err(FreshnessTokenError::InvalidShape),
    }
}

macro_rules! freshness_byte_traits {
    ($type:ty) => {
        impl PartialEq for $type {
            fn eq(&self, other: &Self) -> bool {
                self.bytes == other.bytes
            }
        }

        impl Eq for $type {}

        impl PartialOrd for $type {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $type {
            fn cmp(&self, other: &Self) -> Ordering {
                self.bytes.cmp(&other.bytes)
            }
        }

        impl Hash for $type {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.bytes.hash(state);
            }
        }
    };
}

freshness_byte_traits!(CommitToken);
freshness_byte_traits!(ProjectionFrontier);

impl fmt::Debug for CommitToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommitToken")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.bytes.len())
            .finish()
    }
}

impl fmt::Debug for ProjectionFrontier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionFrontier")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.bytes.len())
            .finish()
    }
}

impl TryFrom<Vec<u8>> for CommitToken {
    type Error = FreshnessTokenError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

impl TryFrom<Vec<u8>> for ProjectionFrontier {
    type Error = FreshnessTokenError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(value: u64) -> CommitSequence {
        CommitSequence::new(value).expect("nonzero")
    }

    #[test]
    fn commit_token_round_trips_opaque_bytes() {
        let token = CommitToken::new(3, seq(42));
        let decoded = CommitToken::from_bytes(token.as_bytes().to_vec()).expect("decode");
        assert_eq!(decoded.history_incarnation(), 3);
        assert_eq!(decoded.commit_sequence(), seq(42));
        assert_eq!(decoded, token);
        let debug = format!("{token:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("42"));
    }

    #[test]
    fn projection_frontier_round_trips_before_first_and_applied() {
        let before = ProjectionFrontier::new(1, FrontierPosition::BeforeFirst);
        let decoded = ProjectionFrontier::from_bytes(before.as_bytes().to_vec()).expect("decode");
        assert_eq!(decoded.position(), FrontierPosition::BeforeFirst);
        assert_eq!(decoded.history_incarnation(), 1);

        let applied = ProjectionFrontier::new(2, FrontierPosition::AppliedThrough(seq(9)));
        let decoded = ProjectionFrontier::from_bytes(applied.as_bytes().to_vec()).expect("decode");
        assert_eq!(decoded.position(), FrontierPosition::AppliedThrough(seq(9)));
        assert_eq!(decoded, applied);
    }

    #[test]
    fn frontier_satisfies_requires_matching_incarnation() {
        let frontier = ProjectionFrontier::new(1, FrontierPosition::AppliedThrough(seq(10)));
        let same = CommitToken::new(1, seq(10));
        let earlier = CommitToken::new(1, seq(7));
        let later = CommitToken::new(1, seq(11));
        let restored = CommitToken::new(2, seq(5));

        assert!(frontier.satisfies(&same));
        assert!(frontier.satisfies(&earlier));
        assert!(!frontier.satisfies(&later));
        // Falsifiable: dropping incarnation from the comparison would make this pass.
        assert!(
            !frontier.satisfies(&restored),
            "stale pre-restore token must not satisfy a new incarnation frontier"
        );
    }

    #[test]
    fn lag_sequences_is_none_across_incarnations() {
        let current = ProjectionFrontier::new(1, FrontierPosition::AppliedThrough(seq(3)));
        let head = ProjectionFrontier::new(1, FrontierPosition::AppliedThrough(seq(10)));
        assert_eq!(current.lag_sequences(&head), Some(7));
        let other = ProjectionFrontier::new(2, FrontierPosition::AppliedThrough(seq(10)));
        assert_eq!(current.lag_sequences(&other), None);
    }
}
