//! Typed caller-key digest-provider boundary.

use std::{error::Error, fmt};

use riffdb_storage_api::{IdempotencyKeyDigest, MAX_READABLE_DIGEST_KEYS};
use riffdb_types::IdempotencyKey;

/// A checked newest-first set of v1 caller-key digests.
///
/// The provider-defined first entry is the current write key. Remaining entries
/// are readable previous keys in newest-first configuration order. Numeric key
/// IDs do not define this order.
#[derive(Clone, Eq, PartialEq)]
pub struct IdempotencyDigestCandidatesV1(Vec<IdempotencyKeyDigest>);

impl IdempotencyDigestCandidatesV1 {
    /// Checks the one-through-eight bound and rejects duplicate key identities or digests.
    pub fn new(candidates: Vec<IdempotencyKeyDigest>) -> Result<Self, IdempotencyDigestError> {
        if candidates.is_empty() {
            return Err(IdempotencyDigestError::Empty);
        }
        if candidates.len() > MAX_READABLE_DIGEST_KEYS {
            return Err(IdempotencyDigestError::TooMany);
        }

        for (index, candidate) in candidates.iter().enumerate() {
            if candidates[..index]
                .iter()
                .any(|prior| prior.key_id() == candidate.key_id())
            {
                return Err(IdempotencyDigestError::DuplicateKeyId);
            }
            if candidates[..index]
                .iter()
                .any(|prior| prior.as_bytes() == candidate.as_bytes())
            {
                return Err(IdempotencyDigestError::DuplicateDigest);
            }
        }

        Ok(Self(candidates))
    }

    /// Borrows the current write-key digest.
    #[must_use]
    pub fn current(&self) -> &IdempotencyKeyDigest {
        &self.0[0]
    }

    /// Borrows every candidate in provider-defined newest-first order.
    #[must_use]
    pub fn as_slice(&self) -> &[IdempotencyKeyDigest] {
        &self.0
    }

    /// Returns the bounded number of readable digest candidates.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the candidate set is empty.
    ///
    /// A successfully constructed set is never empty; this method supports
    /// ordinary collection inspection without exposing its representation.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for IdempotencyDigestCandidatesV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdempotencyDigestCandidatesV1")
            .field("candidates", &"[REDACTED]")
            .field("length", &self.len())
            .finish()
    }
}

/// Safe failure at the caller-key digest-provider boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotencyDigestError {
    /// The provider could not calculate its configured candidates.
    Unavailable,
    /// No current write key was supplied.
    Empty,
    /// More than eight readable keys were supplied.
    TooMany,
    /// One key ID appeared more than once.
    DuplicateKeyId,
    /// Two distinct key IDs produced the same caller-key digest.
    DuplicateDigest,
}

impl fmt::Display for IdempotencyDigestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("idempotency digest provider is unavailable"),
            Self::Empty => formatter.write_str("idempotency digest candidates are empty"),
            Self::TooMany => formatter.write_str("too many idempotency digest candidates"),
            Self::DuplicateKeyId => {
                formatter.write_str("idempotency digest key IDs must be unique")
            }
            Self::DuplicateDigest => {
                formatter.write_str("idempotency digest candidates must be unique")
            }
        }
    }
}

impl Error for IdempotencyDigestError {}

/// Synchronous typed access to configured caller-key digest calculation.
///
/// Implementations own operational key material and must return the current
/// write-key digest first, followed by readable previous-key digests in
/// configured newest-first order. Operational key bytes never cross this port.
pub trait IdempotencyDigestProvider: Send + Sync {
    /// Calculates all configured v1 candidates for one checked caller key.
    fn digest_candidates(
        &self,
        caller_key: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError>;
}
