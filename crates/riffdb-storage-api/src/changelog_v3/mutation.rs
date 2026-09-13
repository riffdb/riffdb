use sha2::{Digest, Sha256};

use crate::{AuthoritativeNamespaceV1, MAX_CHANGELOG_FRAME_BYTES, ReplicationAuthorityClassV1};

use super::ChangelogV3Error;

/// One checked net key transition. Fields are private so a caller cannot
/// inject control state, an invalid metadata key, or an unbounded payload.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthoritativeMutationV3 {
    namespace: AuthoritativeNamespaceV1,
    key: Box<[u8]>,
    expected_hash: Option<[u8; 32]>,
    value: Option<Box<[u8]>>,
}

impl AuthoritativeMutationV3 {
    /// Builds an exact insert (absent prior) or replacement (SHA-256 prior).
    /// Input lengths are checked before any payload is copied.
    pub fn put(
        namespace: AuthoritativeNamespaceV1,
        key: &[u8],
        expected_hash: Option<[u8; 32]>,
        value: &[u8],
    ) -> Result<Self, ChangelogV3Error> {
        Self::validate(namespace, key, value.len())?;
        Ok(Self {
            namespace,
            key: key.into(),
            expected_hash,
            value: Some(value.into()),
        })
    }

    /// Builds a delete with a mandatory exact prior-value hash.
    pub fn delete(
        namespace: AuthoritativeNamespaceV1,
        key: &[u8],
        expected_hash: [u8; 32],
    ) -> Result<Self, ChangelogV3Error> {
        Self::validate(namespace, key, 0)?;
        Ok(Self {
            namespace,
            key: key.into(),
            expected_hash: Some(expected_hash),
            value: None,
        })
    }

    fn validate(
        namespace: AuthoritativeNamespaceV1,
        key: &[u8],
        value_len: usize,
    ) -> Result<(), ChangelogV3Error> {
        if namespace.class() != ReplicationAuthorityClassV1::ReplicatedAuthoritative
            || namespace
                .metadata_key()
                .is_some_and(|expected| expected.as_bytes() != key)
        {
            return Err(ChangelogV3Error::InvalidNamespace);
        }
        if key.is_empty() {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let bytes = 44_usize
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value_len))
            .ok_or(ChangelogV3Error::LimitExceeded)?;
        if bytes > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        Ok(())
    }

    /// Closed authoritative namespace.
    #[must_use]
    pub const fn namespace(&self) -> AuthoritativeNamespaceV1 {
        self.namespace
    }

    /// Exact physical key; only internal storage/replication owners consume it.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Complete post-image for puts; deletes have none.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Expected absent or exact SHA-256 prior state; never public diagnostic data.
    #[must_use]
    pub const fn expected_hash(&self) -> Option<[u8; 32]> {
        self.expected_hash
    }

    /// Validates a predecessor without interpreting absence as a wildcard.
    #[must_use]
    pub fn matches_prior(&self, prior: Option<&[u8]>) -> bool {
        self.expected_hash == prior.map(|value| <[u8; 32]>::from(Sha256::digest(value)))
    }

    /// Encoded mutation bytes, including the fixed namespace/operation/hash header.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        44 + self.key.len() + self.value.as_ref().map_or(0, |value| value.len())
    }
}

impl std::fmt::Debug for AuthoritativeMutationV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthoritativeMutationV3([redacted])")
    }
}

/// Bounded sequential-to-net conversion for one authoritative transaction.
/// This does not validate the first observation against storage: the writer or
/// validated journal owner must do so. Every subsequent observation of a key
/// must match the preceding post-image. Ignoring an error cannot yield a receipt.
pub struct AuthoritativeMutationAccumulatorV3 {
    changes: std::collections::BTreeMap<(AuthoritativeNamespaceV1, Box<[u8]>), NetTransition>,
    encoded_bytes: usize,
    failure: Option<ChangelogV3Error>,
}

struct NetTransition {
    expected_hash: Option<[u8; 32]>,
    current_hash: Option<[u8; 32]>,
    restored: bool,
    value: Option<Box<[u8]>>,
}

impl Default for AuthoritativeMutationAccumulatorV3 {
    fn default() -> Self {
        Self {
            changes: std::collections::BTreeMap::new(),
            encoded_bytes: 160,
            failure: None,
        }
    }
}

impl AuthoritativeMutationAccumulatorV3 {
    /// Records an already checked sequential mutation. Repeated-key writes
    /// preserve the original precondition, validate each intervening edge, and
    /// retain only the final value. Exact restorations have no net transition.
    pub fn record(&mut self, mutation: AuthoritativeMutationV3) -> Result<(), ChangelogV3Error> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let result = self.record_inner(mutation);
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }

    fn record_inner(&mut self, mutation: AuthoritativeMutationV3) -> Result<(), ChangelogV3Error> {
        let AuthoritativeMutationV3 {
            namespace,
            key,
            expected_hash,
            value,
        } = mutation;
        let target = (namespace, key);
        let previous = self.changes.get(&target);
        let original = if let Some(previous) = previous {
            if expected_hash != previous.current_hash {
                return Err(ChangelogV3Error::PredecessorMismatch);
            }
            previous.expected_hash
        } else {
            expected_hash
        };
        let final_hash = value
            .as_deref()
            .map(|bytes| <[u8; 32]>::from(Sha256::digest(bytes)));
        let is_restoration = original == final_hash;
        let old_bytes = previous.map_or(0, |previous| {
            44 + target.1.len() + previous.value.as_ref().map_or(0, |v| v.len())
        });
        // Keep a bounded observed-state marker even after exact cancellation:
        // forgetting it would let a later write invent a different predecessor.
        // The restored post-image bytes themselves are unnecessary; its digest
        // is enough to validate the next observation without retaining a copy.
        let value = if is_restoration { None } else { value };
        let next_bytes = 44 + target.1.len() + value.as_ref().map_or(0, |v| v.len());
        let encoded_bytes = self
            .encoded_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(next_bytes))
            .ok_or(ChangelogV3Error::LimitExceeded)?;
        if encoded_bytes > MAX_CHANGELOG_FRAME_BYTES
            || (previous.is_none() && self.changes.len() >= crate::MAX_CHANGELOG_FRAME_ENTRIES)
        {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        self.changes.insert(
            target,
            NetTransition {
                expected_hash: original,
                current_hash: final_hash,
                restored: is_restoration,
                value,
            },
        );
        self.encoded_bytes = encoded_bytes;
        Ok(())
    }

    /// Consumes the complete successful sequence as canonical namespace/key order.
    /// A previous bound or precondition failure refuses the entire result.
    pub fn finish(self) -> Result<Vec<AuthoritativeMutationV3>, ChangelogV3Error> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        Ok(self
            .changes
            .into_iter()
            .filter(|(_, transition)| !transition.restored)
            .map(|((namespace, key), transition)| AuthoritativeMutationV3 {
                namespace,
                key,
                expected_hash: transition.expected_hash,
                value: transition.value,
            })
            .collect())
    }
}

impl std::fmt::Debug for AuthoritativeMutationAccumulatorV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthoritativeMutationAccumulatorV3([redacted])")
    }
}
