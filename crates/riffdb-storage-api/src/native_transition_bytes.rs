//! Bounded structural transition bytes. Canonical decoding and native custody
//! are separate proofs; constructing one of these values grants no authority.

use crate::StorageValueError;

/// Exact retained predecessor bytes for a closed record-field ceiling.
/// The codec must validate the required original role and canonical envelope.
/// Ownership is moved here only after the actual source ledger admits it.
#[derive(Eq, PartialEq)]
pub struct NativeTransitionEnvelopeBytesV1<const MAX: usize> {
    bytes: Box<[u8]>,
}

impl<const MAX: usize> NativeTransitionEnvelopeBytesV1<MAX> {
    /// Keeps original bytes, without normalization or interpretation changes.
    pub fn new(bytes: Box<[u8]>) -> Result<Self, StorageValueError> {
        if bytes.is_empty() {
            return Err(StorageValueError::InvalidShape);
        }
        if bytes.len() > MAX {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self { bytes })
    }

    /// Original complete envelope, including its original canonical framing.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl<const MAX: usize> std::fmt::Debug for NativeTransitionEnvelopeBytesV1<MAX> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NativeTransitionEnvelopeBytesV1([redacted])")
    }
}

impl<const MAX: usize> riffdb_types::InspectOwnedCapacityV1
    for NativeTransitionEnvelopeBytesV1<MAX>
{
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        let Self { bytes } = self;
        inspection.visit(bytes)
    }
}

/// SHA-256 of a complete, canonically validated catalog-origin envelope.
/// This digest is a lookup binding, never a native authority capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeCatalogOriginEnvelopeDigestV1([u8; 32]);

impl NativeCatalogOriginEnvelopeDigestV1 {
    /// Reconstructs stored digest bytes; canonical origin validation is separate.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    /// Exact artifact digest bytes; semantic reserialization is forbidden.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Domain-separated streaming commitment to original retained V2 controls.
/// Complete original validation must precede its native attestation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeLegacyControlsCommitmentV1([u8; 32]);

impl NativeLegacyControlsCommitmentV1 {
    /// Reconstructs the stored commitment without attesting original controls.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    /// Exact ADR-0318 legacy-controls commitment.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Binding to one authorized immutable backup or promotion attempt selection.
/// A stored digest alone does not recreate that authorization or custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceInheritanceSelectionCommitmentV1([u8; 32]);

impl SourceInheritanceSelectionCommitmentV1 {
    /// Reconstructs a persisted selection commitment, without selection authority.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    /// Exact immutable selection commitment bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

riffdb_types::inspect_owned_capacity_copy_v1!(NativeCatalogOriginEnvelopeDigestV1);
riffdb_types::inspect_owned_capacity_copy_v1!(NativeLegacyControlsCommitmentV1);
riffdb_types::inspect_owned_capacity_copy_v1!(SourceInheritanceSelectionCommitmentV1);
