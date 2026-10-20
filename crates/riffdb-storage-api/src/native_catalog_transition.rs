//! Closed ADR-0318 origin/cut facts. These structural values neither acquire
//! offline custody nor prove an Empty/Active predecessor or exact mutation set.

use crate::{
    AuthoritativeStateCatalogV2, AuthoritativeStateCatalogV3, AuthoritativeTransactionBindingV3,
    ChangelogHistoryStateV3, NativeLegacyControlsCommitmentV1, NativeTransitionEnvelopeBytesV1,
    ProjectionCheckpointOrdinalAllocatorV1, ReplicationPrimaryAdmissionV1, StorageValueError,
};
use riffdb_types::{AdministrationSequence, DatabaseId, SchemaHash};

/// Exact retained origin; neither branch grants native transition authority.
#[derive(Debug, Eq, PartialEq)]
pub enum NativeCatalogTransitionOriginV1 {
    /// Native initialization must separately prove exclusive Empty custody.
    FreshV3,
    /// One ordinary same-incarnation V2 cut, preserving predecessor bytes.
    V2ToV3 {
        /// Complete fixed-V2 history immediately before the cut.
        predecessor_history: Box<ChangelogHistoryStateV3>,
        /// Original registry envelope, not a regenerated digest equivalent.
        original_registry: NativeTransitionEnvelopeBytesV1<256>,
        /// Exact original explicit ActiveAdmissionV1 envelope.
        original_active_admission: NativeTransitionEnvelopeBytesV1<1024>,
        /// Attestation to separately validated retained original controls.
        legacy_controls: NativeLegacyControlsCommitmentV1,
    },
}

/// Structural fields moved only under caller-admitted source ownership.
#[derive(Debug, Eq, PartialEq)]
pub struct NativeCatalogTransitionPartsV1 {
    /// Permanent native database identity.
    pub database_id: DatabaseId,
    /// Exactly one administration item assigned to this fact.
    pub administration_sequence: AdministrationSequence,
    /// Nonrecursive physical transaction inputs; no successor receipt hash.
    pub publication_binding: AuthoritativeTransactionBindingV3,
    /// Exact fixed V3 catalog, never inferred from the current process.
    pub target_catalog_digest: [u8; 32],
    /// Exact registry installed by this transition, preserved historically.
    pub target_registry_digest: SchemaHash,
    /// First installation at current-incarnation Next(1), never ordinal usage.
    pub initial_source_allocator: ProjectionCheckpointOrdinalAllocatorV1,
    /// Closed proven-Empty initialization or explicit ActiveV2 predecessor.
    pub origin: NativeCatalogTransitionOriginV1,
}

/// Canonical representation checks do not certify native publication or custody.
#[derive(Eq, PartialEq)]
pub struct NativeCatalogTransitionV1 {
    parts: NativeCatalogTransitionPartsV1,
}

impl NativeCatalogTransitionV1 {
    /// Checks fixed catalog, exact chain/frontier links and allocator arithmetic.
    /// Native custody and complete predecessor checks remain independently required.
    pub fn new(parts: NativeCatalogTransitionPartsV1) -> Result<Self, StorageValueError> {
        let binding = parts.publication_binding;
        let initial = ProjectionCheckpointOrdinalAllocatorV1::initial(binding.history_incarnation)?;
        if parts.target_catalog_digest != AuthoritativeStateCatalogV3.digest()
            || parts.database_id != binding.database_id
            || parts.initial_source_allocator != initial
            || binding.covered_frontier.application() != binding.predecessor_frontier.application()
            || binding.covered_frontier.administration() != Some(parts.administration_sequence)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let first = match binding.predecessor_frontier.administration() {
            Some(previous) => previous
                .checked_next()
                .ok_or(StorageValueError::SizeOverflow)?,
            None => AdministrationSequence::first(),
        };
        if first != parts.administration_sequence {
            return Err(StorageValueError::IdentityMismatch);
        }
        match &parts.origin {
            NativeCatalogTransitionOriginV1::FreshV3 => {
                if binding.predecessor.is_some()
                    || binding.sequence.get() != 1
                    || binding.prior_history_hash != [0; 32]
                    || binding.predecessor_frontier.application().is_some()
                    || binding.predecessor_frontier.administration().is_some()
                {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
            NativeCatalogTransitionOriginV1::V2ToV3 {
                predecessor_history,
                ..
            } => {
                let history = **predecessor_history;
                let prior = history.lineage();
                let tail = history.tail();
                if prior.catalog_digest() != AuthoritativeStateCatalogV2.digest()
                    || prior.database_id() != parts.database_id
                    || prior.history_incarnation() != binding.history_incarnation
                    || binding.predecessor != Some(tail.sequence())
                    || tail.sequence().checked_next() != Some(binding.sequence)
                    || binding.prior_history_hash != tail.history_hash()
                    || binding.predecessor_frontier != tail.frontier()
                {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        Ok(Self { parts })
    }

    /// Checks a separately canonical-decoded original ActiveV1 against the stored
    /// predecessor. The codec additionally binds these exact envelope bytes.
    /// This consistency check does not supply an offline owner or write permit.
    pub fn validate_original_active(
        &self,
        original: &ReplicationPrimaryAdmissionV1,
    ) -> Result<(), StorageValueError> {
        let NativeCatalogTransitionOriginV1::V2ToV3 {
            predecessor_history,
            ..
        } = &self.parts.origin
        else {
            return Err(StorageValueError::InvalidShape);
        };
        if original.fence().is_some()
            || original.lineage() != predecessor_history.lineage()
            || original.lineage().catalog_digest() != AuthoritativeStateCatalogV2.digest()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(())
    }

    /// Immutable persisted evidence; it cannot select a runtime catalog context.
    #[must_use]
    pub const fn parts(&self) -> &NativeCatalogTransitionPartsV1 {
        &self.parts
    }
}

impl std::fmt::Debug for NativeCatalogTransitionV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NativeCatalogTransitionV1([redacted])")
    }
}

impl riffdb_types::InspectOwnedCapacityV1 for NativeCatalogTransitionOriginV1 {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::FreshV3 => Ok(()),
            Self::V2ToV3 {
                predecessor_history,
                original_registry,
                original_active_admission,
                legacy_controls: _,
            } => {
                inspection.visit(predecessor_history)?;
                inspection.visit(original_registry)?;
                inspection.visit(original_active_admission)
            }
        }
    }
}

riffdb_types::inspect_owned_capacity_struct_v1!(NativeCatalogTransitionPartsV1 {
    owned: [origin],
    inline: [
        database_id,
        administration_sequence,
        publication_binding,
        target_catalog_digest,
        target_registry_digest,
        initial_source_allocator
    ]
});
riffdb_types::inspect_owned_capacity_struct_v1!(NativeCatalogTransitionV1 {
    owned: [parts],
    inline: []
});
