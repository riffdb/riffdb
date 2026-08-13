//! Exact identities shared by the operator-only application-reimport boundary.

use std::num::NonZeroU64;

use crate::{ApplicationPortabilityManifestHash, CapabilityId};

/// Exact current capability revision bound into one reimport safe point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationReimportAuthorityV1 {
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
}

impl ApplicationReimportAuthorityV1 {
    /// Binds one current durable Capability V7 revision.
    #[must_use]
    pub const fn new(capability_id: CapabilityId, capability_revision: NonZeroU64) -> Self {
        Self {
            capability_id,
            capability_revision,
        }
    }

    /// Durable capability identity.
    #[must_use]
    pub const fn capability_id(self) -> CapabilityId {
        self.capability_id
    }

    /// Exact revision revalidated at every operation safe point.
    #[must_use]
    pub const fn capability_revision(self) -> NonZeroU64 {
        self.capability_revision
    }
}

/// Exact immutable manifest identity retained by reimport requests and proofs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationReimportManifestBindingV1 {
    portability_manifest_hash: ApplicationPortabilityManifestHash,
}

impl ApplicationReimportManifestBindingV1 {
    /// Constructs the closed manifest binding.
    #[must_use]
    pub const fn new(portability_manifest_hash: ApplicationPortabilityManifestHash) -> Self {
        Self {
            portability_manifest_hash,
        }
    }

    /// Exact adapter-owned portability manifest.
    #[must_use]
    pub const fn portability_manifest_hash(self) -> ApplicationPortabilityManifestHash {
        self.portability_manifest_hash
    }
}
