//! Structural ADR-0318 inheritance facts. Native selection, exact preimages,
//! descriptor closure and present root/base/pin authority are separate proofs.

use crate::{
    AuthoritativeStateCatalogV3, AuthoritativeTransactionBindingV3, ChangelogHistoryStateV3,
    ChangelogLineageV3, NativeCatalogOriginEnvelopeDigestV1, NativeTransitionEnvelopeBytesV1,
    SourceInheritanceSelectionCommitmentV1, StorageValueError,
};
use riffdb_types::{
    AdministrationSequence, ContentDigest, DIGEST_SCHEME_V1, HashDomain, ProjectionSourceLocatorV1,
};

/// Maximum number of distinct required source identities in one transition.
pub const MAX_SOURCE_INHERITANCE_WITNESSES_V1: usize = 128;

/// Closed new-incarnation recipe; it does not select an operator operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceInheritanceCauseV1 {
    /// Existing authorized backup selection and local rebuild rules.
    Restore,
    /// Existing authenticated fenced promotion and generation preservation.
    Promotion,
}

/// Direct immutable reference to the original catalog activation/cut fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeCatalogOriginReferenceV1 {
    administration_sequence: AdministrationSequence,
    digest: NativeCatalogOriginEnvelopeDigestV1,
}

impl NativeCatalogOriginReferenceV1 {
    /// Reconstructs stored lookup fields, without verifying the original fact.
    #[must_use]
    pub const fn new(
        administration_sequence: AdministrationSequence,
        digest: NativeCatalogOriginEnvelopeDigestV1,
    ) -> Self {
        Self {
            administration_sequence,
            digest,
        }
    }
    /// Canonical administration key of the original fact.
    #[must_use]
    pub const fn administration_sequence(self) -> AdministrationSequence {
        self.administration_sequence
    }
    /// SHA-256 of the complete canonical original envelope.
    #[must_use]
    pub const fn digest(self) -> NativeCatalogOriginEnvelopeDigestV1 {
        self.digest
    }
}

/// Fixed-width identity witness, not permission to acquire or resurrect rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceInheritanceWitnessV1 {
    locator: ProjectionSourceLocatorV1,
    descriptor_digest: ContentDigest,
    origin_commitment: ContentDigest,
}

impl SourceInheritanceWitnessV1 {
    /// Checks the two fixed digest roles while preserving the original locator.
    pub fn new(
        locator: ProjectionSourceLocatorV1,
        descriptor_digest: ContentDigest,
        origin_commitment: ContentDigest,
    ) -> Result<Self, StorageValueError> {
        if descriptor_digest.domain() != HashDomain::ProjectionSourceDescriptor
            || origin_commitment.domain() != HashDomain::ProjectionSourceOrigin
            || descriptor_digest.scheme() != DIGEST_SCHEME_V1
            || origin_commitment.scheme() != DIGEST_SCHEME_V1
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            locator,
            descriptor_digest,
            origin_commitment,
        })
    }
    /// Full original allocation identity; inheritance never rewrites it.
    #[must_use]
    pub const fn locator(self) -> ProjectionSourceLocatorV1 {
        self.locator
    }
    /// Exact immutable descriptor digest.
    #[must_use]
    pub const fn descriptor_digest(self) -> ContentDigest {
        self.descriptor_digest
    }
    /// Exact original source-origin commitment.
    #[must_use]
    pub const fn origin_commitment(self) -> ContentDigest {
        self.origin_commitment
    }
    /// The sole witness order key: database16, allocation-incarnation BE8,
    /// checkpoint-ordinal BE8. Digest differences never excuse duplicate keys.
    #[must_use]
    pub fn canonical_locator_key(self) -> [u8; 32] {
        let mut key = [0; 32];
        key[..16].copy_from_slice(self.locator.database_id().as_bytes());
        key[16..24].copy_from_slice(&self.locator.allocation_history_incarnation().to_be_bytes());
        key[24..].copy_from_slice(&self.locator.ordinal().to_be_bytes());
        key
    }
    /// Exact96B stored witness, independent of Rust padding or enum layout.
    #[must_use]
    pub fn canonical_witness_bytes(self) -> [u8; 96] {
        let mut bytes = [0; 96];
        bytes[..32].copy_from_slice(&self.canonical_locator_key());
        bytes[32..64].copy_from_slice(self.descriptor_digest.as_bytes());
        bytes[64..].copy_from_slice(self.origin_commitment.as_bytes());
        bytes
    }
}

/// Exact original scalar states. None is explicit absence, whose admissibility
/// must be checked by the selected native recipe against the real transaction.
#[derive(Debug, Eq, PartialEq)]
pub struct SourceInheritancePredecessorScalarsV1 {
    /// Original current-incarnation allocator envelope, if actually present.
    pub source_allocator: Option<NativeTransitionEnvelopeBytesV1<160>>,
    /// Original incarnation envelope, if the selected recipe permits absence.
    pub incarnation: Option<NativeTransitionEnvelopeBytesV1<128>>,
    /// Original registry envelope, including its exact canonical framing.
    pub registry: Option<NativeTransitionEnvelopeBytesV1<256>>,
    /// Original watermark envelope or checked exact absence.
    pub watermark: Option<NativeTransitionEnvelopeBytesV1<256>>,
}

/// Bounded structural parts, moved only under caller-admitted source ownership.
#[derive(Debug, Eq, PartialEq)]
pub struct SourceInheritanceAdministrationPartsV1 {
    /// Assigned first new administration item; never a caller-selected recipe.
    pub administration_sequence: AdministrationSequence,
    /// Closed restore/promotion cause.
    pub cause: SourceInheritanceCauseV1,
    /// Exact history of the frozen predecessor database being replaced.
    pub predecessor_history: ChangelogHistoryStateV3,
    /// Checked new-incarnation lineage selected by the existing native recipe.
    pub successor_lineage: ChangelogLineageV3,
    /// Exact nonrecursive first receipt binding in the successor incarnation.
    pub anchor_binding: AuthoritativeTransactionBindingV3,
    /// Original catalog fact, including after multiple checked inheritances.
    pub original_origin: NativeCatalogOriginReferenceV1,
    /// Commitment to the independently authorized immutable backup/attempt.
    pub authorized_selection: SourceInheritanceSelectionCommitmentV1,
    /// Every selected root and required valid generation base, once per locator.
    pub witnesses: Box<[SourceInheritanceWitnessV1]>,
    /// Exact scalar preimages; no normalized or synthesized substitute is valid.
    pub predecessor_scalars: SourceInheritancePredecessorScalarsV1,
}

/// A decoded fact never supplies the fields-private native inheritance owner.
#[derive(Eq, PartialEq)]
pub struct SourceInheritanceAdministrationV1 {
    parts: SourceInheritanceAdministrationPartsV1,
}

impl SourceInheritanceAdministrationV1 {
    /// Checks fixed structural links and all bounded administration arithmetic.
    /// Actual closure, origin, selection and scalar recipes remain native checks.
    pub fn new(parts: SourceInheritanceAdministrationPartsV1) -> Result<Self, StorageValueError> {
        let before = parts.predecessor_history;
        let prior = before.lineage();
        let after = parts.successor_lineage;
        let binding = parts.anchor_binding;
        let catalog = AuthoritativeStateCatalogV3.digest();
        if prior.catalog_digest() != catalog
            || after.catalog_digest() != catalog
            || prior.database_id() != after.database_id()
            || after.history_incarnation() <= prior.history_incarnation()
            || binding.database_id != after.database_id()
            || binding.history_incarnation != after.history_incarnation()
            || binding.predecessor.is_some()
            || binding.sequence.get() != 1
            || binding.prior_history_hash != [0; 32]
            || binding.predecessor_frontier != before.tail().frontier()
            || binding.covered_frontier.application() != before.tail().frontier().application()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let first = match before.tail().frontier().administration() {
            Some(previous) => previous
                .checked_next()
                .ok_or(StorageValueError::SizeOverflow)?,
            None => AdministrationSequence::first(),
        };
        if parts.administration_sequence != first {
            return Err(StorageValueError::IdentityMismatch);
        }
        let covered = match parts.cause {
            SourceInheritanceCauseV1::Restore => {
                if after.leadership_epoch() != prior.leadership_epoch() {
                    return Err(StorageValueError::IdentityMismatch);
                }
                first
            }
            SourceInheritanceCauseV1::Promotion => {
                if prior.leadership_epoch().get().checked_add(1)
                    != Some(after.leadership_epoch().get())
                {
                    return Err(StorageValueError::IdentityMismatch);
                }
                first
                    .checked_next()
                    .and_then(AdministrationSequence::checked_next)
                    .and_then(AdministrationSequence::checked_next)
                    .ok_or(StorageValueError::SizeOverflow)?
            }
        };
        if binding.covered_frontier.administration() != Some(covered)
            || parts.original_origin.administration_sequence().get()
                > before
                    .tail()
                    .frontier()
                    .administration()
                    .map_or(0, AdministrationSequence::get)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        if parts.witnesses.len() > MAX_SOURCE_INHERITANCE_WITNESSES_V1 {
            return Err(StorageValueError::LimitExceeded);
        }
        for witness in &parts.witnesses {
            if witness.locator().database_id() != prior.database_id()
                || witness.locator().allocation_history_incarnation() > prior.history_incarnation()
            {
                return Err(StorageValueError::IdentityMismatch);
            }
        }
        if parts
            .witnesses
            .windows(2)
            .any(|pair| pair[0].canonical_locator_key() >= pair[1].canonical_locator_key())
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self { parts })
    }

    /// Immutable structural evidence, not a validated native source capability.
    #[must_use]
    pub const fn parts(&self) -> &SourceInheritanceAdministrationPartsV1 {
        &self.parts
    }
}

impl std::fmt::Debug for SourceInheritanceAdministrationV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SourceInheritanceAdministrationV1([redacted])")
    }
}

riffdb_types::inspect_owned_capacity_copy_v1!(SourceInheritanceCauseV1);
riffdb_types::inspect_owned_capacity_copy_v1!(NativeCatalogOriginReferenceV1);
riffdb_types::inspect_owned_capacity_copy_v1!(SourceInheritanceWitnessV1);
riffdb_types::inspect_owned_capacity_struct_v1!(SourceInheritancePredecessorScalarsV1 {
    owned: [source_allocator, incarnation, registry, watermark],
    inline: []
});
riffdb_types::inspect_owned_capacity_struct_v1!(SourceInheritanceAdministrationPartsV1 {
    owned: [witnesses, predecessor_scalars],
    inline: [
        administration_sequence,
        cause,
        predecessor_history,
        successor_lineage,
        anchor_binding,
        original_origin,
        authorized_selection
    ]
});
riffdb_types::inspect_owned_capacity_struct_v1!(SourceInheritanceAdministrationV1 {
    owned: [parts],
    inline: []
});
