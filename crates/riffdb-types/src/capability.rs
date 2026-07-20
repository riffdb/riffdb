//! Foundational capability-token digest values.

use std::fmt;

use crate::{DIGEST_SCHEME_V1, DigestKeyId};

/// Maximum audiences retained by one capability.
pub const MAX_CAPABILITY_AUDIENCES: usize = 8;
/// Maximum requested capability lifetime in seconds.
pub const MAX_CAPABILITY_LIFETIME_SECONDS: u32 = 2_592_000;

/// Closed revocation reason registry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RevocationReasonCodeV1 {
    /// Explicit operator request.
    Requested,
    /// Capability was replaced.
    Replaced,
    /// Suspected credential compromise.
    SuspectedCompromise,
    /// Current policy changed.
    PolicyChange,
}

impl RevocationReasonCodeV1 {
    /// Returns the immutable v1 tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Requested => 0x01,
            Self::Replaced => 0x02,
            Self::SuspectedCompromise => 0x03,
            Self::PolicyChange => 0x04,
        }
    }

    /// Decodes a stable v1 revocation-reason tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Requested),
            0x02 => Some(Self::Replaced),
            0x03 => Some(Self::SuspectedCompromise),
            0x04 => Some(Self::PolicyChange),
            _ => None,
        }
    }
}

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

    #[test]
    fn capability_hard_limits_have_exact_v1_values() {
        assert_eq!(MAX_CAPABILITY_AUDIENCES, 8);
        assert_eq!(MAX_CAPABILITY_LIFETIME_SECONDS, 2_592_000);
    }

    #[test]
    fn revocation_reason_registry_has_exact_v1_tags() {
        let registry = [
            (RevocationReasonCodeV1::Requested, 0x01),
            (RevocationReasonCodeV1::Replaced, 0x02),
            (RevocationReasonCodeV1::SuspectedCompromise, 0x03),
            (RevocationReasonCodeV1::PolicyChange, 0x04),
        ];

        for (reason, tag) in registry {
            assert_eq!(reason.tag(), tag);
            assert_eq!(RevocationReasonCodeV1::from_tag(tag), Some(reason));
        }
        assert_eq!(RevocationReasonCodeV1::from_tag(0), None);
        assert_eq!(RevocationReasonCodeV1::from_tag(5), None);
        assert_eq!(RevocationReasonCodeV1::from_tag(u8::MAX), None);
    }
}
