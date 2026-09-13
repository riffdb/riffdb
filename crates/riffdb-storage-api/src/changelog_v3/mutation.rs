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
