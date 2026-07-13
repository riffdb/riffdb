//! Foundational capability-token digest values.

use std::fmt;

use crate::{DIGEST_SCHEME_V1, DigestKeyId};

/// A versioned capability-token lookup digest.
///
/// The digest bytes are not bearer credentials, but their formatting is still
/// redacted. Construction records only already-computed v1 HMAC bytes; the
/// domain-separated calculation is provided by
/// [`hash_capability_token`](crate::hash_capability_token).
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CapabilityTokenDigest {
    key_id: DigestKeyId,
    bytes: [u8; 32],
}

impl CapabilityTokenDigest {
    /// Creates a v1 capability-token digest from its key ID and HMAC bytes.
    #[must_use]
    pub const fn from_hmac_bytes(key_id: DigestKeyId, bytes: [u8; 32]) -> Self {
        Self { key_id, bytes }
    }

    /// Returns the immutable digest-scheme version.
    #[must_use]
    pub const fn scheme(self) -> u8 {
        DIGEST_SCHEME_V1
    }

    /// Returns the non-secret immutable digest-key identifier.
    #[must_use]
    pub const fn key_id(self) -> DigestKeyId {
        self.key_id
    }

    /// Explicitly borrows the 32 HMAC bytes for a reviewed storage lookup.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl fmt::Debug for CapabilityTokenDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityTokenDigest")
            .field("scheme", &DIGEST_SCHEME_V1)
            .field("key_id", &self.key_id)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Display for CapabilityTokenDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("capability token digest [REDACTED]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest_key_id(value: u32) -> DigestKeyId {
        DigestKeyId::try_from(value).expect("nonzero digest key ID")
    }

    #[test]
    fn formatting_redacts_digest_bytes() {
        let digest = CapabilityTokenDigest::from_hmac_bytes(digest_key_id(7), [0xab; 32]);
        let debug = format!("{digest:?}");
        let display = digest.to_string();

        assert!(debug.contains("[REDACTED]"));
        assert!(display.contains("[REDACTED]"));
        assert!(!debug.contains("abababab"));
        assert!(!display.contains("abababab"));
        assert_eq!(digest.scheme(), DIGEST_SCHEME_V1);
        assert_eq!(digest.key_id(), digest_key_id(7));
        assert_eq!(digest.as_bytes(), &[0xab; 32]);
    }
}
