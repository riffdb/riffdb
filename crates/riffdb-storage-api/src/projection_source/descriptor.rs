//! Immutable structural descriptors and references; none grant source authority.

use riffdb_types::{
    AdministrationSequence, CommitSequence, ContentDigest, ContentHasher, DUAL_FRONTIER_BYTES,
    HashDomain, ProjectionGeneration, ProjectionIdentity, ProjectionSourceLocatorV1,
};

use super::{
    MAX_PROJECTION_SOURCE_CONTROL_BYTES, MAX_PROJECTION_SOURCE_OBJECT_BYTES,
    MAX_PROJECTION_SOURCE_ROWS, ProjectionSourceLifecycleV1,
};
use crate::{
    AuthoritativeTransactionBindingV3, ChangelogHistoryPointV3, ChangelogHistoryStateV3,
    StorageValueError,
};

/// Exact structural source reference, including immutable digest and origin.
/// A native reader must additionally verify publication/transition and closure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionSourceReferenceV1 {
    identity: ProjectionIdentity,
    locator: ProjectionSourceLocatorV1,
    digest: ContentDigest,
    origin: ContentDigest,
}

impl ProjectionSourceReferenceV1 {
    /// Reconstructs structural persisted references without granting selection.
    pub fn new(
        identity: ProjectionIdentity,
        locator: ProjectionSourceLocatorV1,
        digest: ContentDigest,
        origin: ContentDigest,
    ) -> Result<Self, StorageValueError> {
        check_domain(digest, HashDomain::ProjectionSourceDescriptor)?;
        check_domain(origin, HashDomain::ProjectionSourceOrigin)?;
        Ok(Self {
            identity,
            locator,
            digest,
            origin,
        })
    }
    /// Exact lineage/projection/plan identity.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }
    /// Original full allocation identity; never rewritten on inheritance.
    #[must_use]
    pub const fn locator(&self) -> ProjectionSourceLocatorV1 {
        self.locator
    }
    /// Immutable descriptor digest, never a lifecycle/progress hash.
    #[must_use]
    pub const fn digest(&self) -> ContentDigest {
        self.digest
    }
    /// Immutable origin commitment; a matching digest alone is not ancestry proof.
    #[must_use]
    pub const fn origin(&self) -> ContentDigest {
        self.origin
    }
}

/// Unverified structural descriptor components for a separately checked codec.
/// These fields cannot construct the projection-owned verified candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionSourceDescriptorPartsV1 {
    /// Exact projection identity, including the immutable semantic plan hash.
    pub identity: ProjectionIdentity,
    /// Full original allocation locator.
    pub locator: ProjectionSourceLocatorV1,
    /// Nonzero independently evaluated prefix frontier.
    pub applied_through: CommitSequence,
    /// Immutable captured authoritative source fence, not a recaptured live head.
    pub captured_source: ChangelogHistoryStateV3,
    /// Origin commitment whose transition validity is verified by native custody.
    pub origin: ContentDigest,
    /// Optional immutable exact prior descriptor digest.
    pub prior_source_digest: Option<ContentDigest>,
    /// Final canonical independent row count.
    pub row_count: u32,
    /// Final complete canonical row key/value charge. Whole-object admission
    /// additionally counts this object's complete descriptor/control record.
    pub canonical_row_bytes: u64,
    /// Canonical independent row root; hashing alone does not prove evaluation.
    pub row_root: ContentDigest,
}

/// Bounded immutable source descriptor; lifecycle/progress are deliberately absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionSourceDescriptorV1 {
    parts: ProjectionSourceDescriptorPartsV1,
    reference: ProjectionSourceReferenceV1,
}

impl ProjectionSourceDescriptorV1 {
    /// Checks structural links and computes the immutable descriptor digest.
    /// This never proves that its rows were independently evaluated or published.
    pub fn new(parts: ProjectionSourceDescriptorPartsV1) -> Result<Self, StorageValueError> {
        check_domain(parts.origin, HashDomain::ProjectionSourceOrigin)?;
        check_domain(parts.row_root, HashDomain::ProjectionSourceRows)?;
        if let Some(parent) = parts.prior_source_digest {
            check_domain(parent, HashDomain::ProjectionSourceDescriptor)?;
        }
        let lineage = parts.captured_source.lineage();
        if parts.locator.database_id() != lineage.database_id()
            || parts.locator.allocation_history_incarnation() != lineage.history_incarnation()
            || parts
                .captured_source
                .tail()
                .frontier()
                .application()
                .is_none_or(|head| head < parts.applied_through)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        if parts.row_count > MAX_PROJECTION_SOURCE_ROWS
            || parts.canonical_row_bytes > MAX_PROJECTION_SOURCE_OBJECT_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if (parts.row_count == 0) != (parts.canonical_row_bytes == 0)
            || parts.canonical_row_bytes < u64::from(parts.row_count)
        {
            return Err(StorageValueError::InvalidShape);
        }
        let identity = parts.identity.to_canonical_bytes();
        let preimage_bytes =
            descriptor_preimage_bytes(identity.len(), parts.prior_source_digest.is_some())?;
        if preimage_bytes
            .checked_add(32)
            .ok_or(StorageValueError::SizeOverflow)?
            > MAX_PROJECTION_SOURCE_CONTROL_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        let mut hash = ContentHasher::new(
            HashDomain::ProjectionSourceDescriptor,
            preimage_bytes as u64,
        );
        update(&mut hash, &1_u32.to_be_bytes())?;
        update(
            &mut hash,
            &u32::try_from(identity.len())
                .map_err(|_| StorageValueError::SizeOverflow)?
                .to_be_bytes(),
        )?;
        update(&mut hash, &identity)?;
        update(&mut hash, parts.locator.database_id().as_bytes())?;
        update(
            &mut hash,
            &parts.locator.allocation_history_incarnation().to_be_bytes(),
        )?;
        update(&mut hash, &parts.locator.ordinal().to_be_bytes())?;
        update(&mut hash, &parts.applied_through.to_be_bytes())?;
        update(&mut hash, lineage.database_id().as_bytes())?;
        update(&mut hash, &lineage.history_incarnation().to_be_bytes())?;
        update(&mut hash, &lineage.leadership_epoch().get().to_be_bytes())?;
        update(&mut hash, &lineage.catalog_digest())?;
        for point in [
            parts.captured_source.anchor(),
            parts.captured_source.tail(),
            parts.captured_source.minimum_resume(),
        ] {
            update_point(&mut hash, point)?;
        }
        update(&mut hash, parts.origin.as_bytes())?;
        match parts.prior_source_digest {
            None => update(&mut hash, &[0])?,
            Some(prior) => {
                update(&mut hash, &[1])?;
                update(&mut hash, prior.as_bytes())?;
            }
        }
        update(&mut hash, &parts.row_count.to_be_bytes())?;
        update(&mut hash, &parts.canonical_row_bytes.to_be_bytes())?;
        update(&mut hash, parts.row_root.as_bytes())?;
        let digest = hash.finish().map_err(|_| StorageValueError::InvalidShape)?;
        let reference = ProjectionSourceReferenceV1::new(
            parts.identity.clone(),
            parts.locator,
            digest,
            parts.origin,
        )?;
        Ok(Self { parts, reference })
    }
    /// Complete exact immutable source reference.
    #[must_use]
    pub const fn reference(&self) -> &ProjectionSourceReferenceV1 {
        &self.reference
    }
    /// Nonzero prefix frontier represented by the source rows.
    #[must_use]
    pub const fn applied_through(&self) -> CommitSequence {
        self.parts.applied_through
    }
    /// Original captured authoritative fence; latest-head appends do not replace it.
    #[must_use]
    pub const fn captured_source(&self) -> ChangelogHistoryStateV3 {
        self.parts.captured_source
    }
    /// Optional immutable preceding descriptor digest.
    #[must_use]
    pub const fn prior_source_digest(&self) -> Option<ContentDigest> {
        self.parts.prior_source_digest
    }
    /// Final complete independently evaluated row count.
    #[must_use]
    pub const fn row_count(&self) -> u32 {
        self.parts.row_count
    }
    /// Final canonical complete row key/value bytes.
    #[must_use]
    pub const fn canonical_row_bytes(&self) -> u64 {
        self.parts.canonical_row_bytes
    }
    /// Independent canonical ordered row root.
    #[must_use]
    pub const fn row_root(&self) -> ContentDigest {
        self.parts.row_root
    }
}

/// Structural object record with mutable closed lifecycle outside its digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionSourceObjectV1 {
    descriptor: ProjectionSourceDescriptorV1,
    lifecycle: ProjectionSourceLifecycleV1,
}

impl ProjectionSourceObjectV1 {
    /// Reconstructs structural state; a Published tag alone grants no authority.
    #[must_use]
    pub const fn new(
        descriptor: ProjectionSourceDescriptorV1,
        lifecycle: ProjectionSourceLifecycleV1,
    ) -> Self {
        Self {
            descriptor,
            lifecycle,
        }
    }
    /// Immutable descriptor retained across lifecycle updates.
    #[must_use]
    pub const fn descriptor(&self) -> &ProjectionSourceDescriptorV1 {
        &self.descriptor
    }
    /// Closed current lifecycle, separate from source identity.
    #[must_use]
    pub const fn lifecycle(&self) -> ProjectionSourceLifecycleV1 {
        self.lifecycle
    }
    /// Pure closed transition; native publication/reference checks remain required.
    pub fn transition(&self, next: ProjectionSourceLifecycleV1) -> Result<Self, StorageValueError> {
        Ok(Self {
            descriptor: self.descriptor.clone(),
            lifecycle: self.lifecycle.transition(next)?,
        })
    }
}

/// Structural selected-root record with a nonrecursive publication binding.
///
/// The assigned transaction and its predecessor are stored, never the hash of
/// the frame containing this root. The actual sealing transaction must verify
/// this binding/reference, then its resulting frame hash covers these root bytes.
/// Native source/origin/transition and closure validation remain mandatory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionSourceRootV1 {
    reference: ProjectionSourceReferenceV1,
    publication_binding: AuthoritativeTransactionBindingV3,
    administration_sequence: Option<AdministrationSequence>,
}

impl ProjectionSourceRootV1 {
    /// Reconstructs a root bound to its actual assigned physical transaction.
    /// An optional audit sequence must be the exact one-step administration
    /// advance. No raw binding grants publication or inherited-source authority.
    pub fn new(
        reference: ProjectionSourceReferenceV1,
        publication_binding: AuthoritativeTransactionBindingV3,
        administration_sequence: Option<AdministrationSequence>,
    ) -> Result<Self, StorageValueError> {
        let binding = publication_binding;
        if reference.locator().database_id() != binding.database_id
            || reference.locator().allocation_history_incarnation() != binding.history_incarnation
            || binding.predecessor.and_then(|prior| prior.checked_next()) != Some(binding.sequence)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let expected_administration = administration_sequence.map_or(
            Some(binding.predecessor_frontier.administration()),
            |assigned| {
                let next = match binding.predecessor_frontier.administration() {
                    None => Some(AdministrationSequence::first()),
                    Some(prior) => prior.checked_next(),
                };
                (next == Some(assigned)).then_some(Some(assigned))
            },
        );
        if binding.covered_frontier.application().is_none()
            || binding.covered_frontier.application() != binding.predecessor_frontier.application()
            || expected_administration != Some(binding.covered_frontier.administration())
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            reference,
            publication_binding,
            administration_sequence,
        })
    }
    /// Exact selected source locator/digest/origin and projection identity.
    #[must_use]
    pub const fn reference(&self) -> &ProjectionSourceReferenceV1 {
        &self.reference
    }
    /// Nonrecursive exact assigned publication transaction binding.
    #[must_use]
    pub const fn publication_binding(&self) -> AuthoritativeTransactionBindingV3 {
        self.publication_binding
    }
    /// Actual assigned administration audit sequence, only if one was allocated.
    #[must_use]
    pub const fn administration_sequence(&self) -> Option<AdministrationSequence> {
        self.administration_sequence
    }
    /// Structural cross-record check only; selected root and receipt still need
    /// authoritative transaction-current validation before granting acquisition.
    pub fn matches_published_object(&self, object: &ProjectionSourceObjectV1) -> bool {
        object.lifecycle == ProjectionSourceLifecycleV1::Published
            && &self.reference == object.descriptor.reference()
            && self
                .publication_binding
                .covered_frontier
                .application()
                .is_some_and(|head| head >= object.descriptor.applied_through())
    }
}

/// Immutable structural seed base for exactly one local derived generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationSourceBaseV1 {
    generation: ProjectionGeneration,
    reference: ProjectionSourceReferenceV1,
    applied_through: CommitSequence,
}

impl GenerationSourceBaseV1 {
    /// Reconstructs a persisted base, never permission to seed an arbitrary cache.
    #[must_use]
    pub const fn new(
        generation: ProjectionGeneration,
        reference: ProjectionSourceReferenceV1,
        applied_through: CommitSequence,
    ) -> Self {
        Self {
            generation,
            reference,
            applied_through,
        }
    }
    /// Exact local generation permanently bound to this source base.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }
    /// Full source identity/digest/origin; never silently replaced by the latest root.
    #[must_use]
    pub const fn reference(&self) -> &ProjectionSourceReferenceV1 {
        &self.reference
    }
    /// Seed frontier; commits at/below this position have no individual markers.
    #[must_use]
    pub const fn applied_through(&self) -> CommitSequence {
        self.applied_through
    }
    /// Structural retained-base check. Native lifetime custody must additionally
    /// prove this persisted base, transition validity AND the contiguous tail.
    pub fn matches_retained_object(&self, object: &ProjectionSourceObjectV1) -> bool {
        matches!(
            object.lifecycle,
            ProjectionSourceLifecycleV1::Published | ProjectionSourceLifecycleV1::Superseded
        ) && &self.reference == object.descriptor.reference()
            && self.applied_through == object.descriptor.applied_through()
    }
}

fn check_domain(digest: ContentDigest, expected: HashDomain) -> Result<(), StorageValueError> {
    if digest.domain() != expected {
        return Err(StorageValueError::InvalidShape);
    }
    Ok(())
}

fn descriptor_preimage_bytes(
    identity_bytes: usize,
    has_prior: bool,
) -> Result<usize, StorageValueError> {
    // Version, identity framing, full locator, frontier, lineage, three complete
    // receipt points, origin, optional prior, row count/charge and canonical root.
    let receipt_point_bytes =
        std::mem::size_of::<u64>() + std::mem::size_of::<[u8; 32]>() + DUAL_FRONTIER_BYTES;
    identity_bytes
        .checked_add(
            4 + 4
                + 32
                + 8
                + 64
                + 3 * receipt_point_bytes
                + 32
                + 1
                + if has_prior { 32 } else { 0 }
                + 4
                + 8
                + 32,
        )
        .ok_or(StorageValueError::SizeOverflow)
}

fn update(hash: &mut ContentHasher, bytes: &[u8]) -> Result<(), StorageValueError> {
    hash.update(bytes)
        .map_err(|_| StorageValueError::InvalidShape)
}

fn update_point(
    hash: &mut ContentHasher,
    point: ChangelogHistoryPointV3,
) -> Result<(), StorageValueError> {
    update(hash, &point.sequence().get().to_be_bytes())?;
    update(hash, &point.history_hash())?;
    update(hash, &point.frontier().to_canonical_bytes())
}

riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionSourceReferenceV1 {
    owned: [identity],
    inline: [locator, digest, origin]
});
riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionSourceDescriptorPartsV1 {
    owned: [identity],
    inline: [
        locator,
        applied_through,
        captured_source,
        origin,
        prior_source_digest,
        row_count,
        canonical_row_bytes,
        row_root
    ]
});
riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionSourceDescriptorV1 {
    owned: [parts, reference],
    inline: []
});
riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionSourceObjectV1 {
    owned: [descriptor],
    inline: [lifecycle]
});
riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionSourceRootV1 {
    owned: [reference],
    inline: [publication_binding, administration_sequence]
});
riffdb_types::inspect_owned_capacity_struct_v1!(GenerationSourceBaseV1 {
    owned: [reference],
    inline: [generation, applied_through]
});

impl ProjectionSourceReferenceV1 {
    /// Cheap conservative copy bound. Reserve before cloning; original/shared
    /// ownership remains charged until its own final release.
    pub fn clone_work_bound_v1(&self) -> Result<u64, StorageValueError> {
        metadata_clone_bound(1, std::mem::size_of::<Self>())
    }
}
impl ProjectionSourceDescriptorV1 {
    /// Includes both immutable owned identity representations and copy scratch.
    pub fn clone_work_bound_v1(&self) -> Result<u64, StorageValueError> {
        metadata_clone_bound(2, std::mem::size_of::<Self>())
    }
}
impl ProjectionSourceRootV1 {
    /// Includes the exact reference and bounded publication binding copy.
    pub fn clone_work_bound_v1(&self) -> Result<u64, StorageValueError> {
        metadata_clone_bound(1, std::mem::size_of::<Self>())
    }
}
fn metadata_clone_bound(controls: usize, inline: usize) -> Result<u64, StorageValueError> {
    controls
        .checked_mul(MAX_PROJECTION_SOURCE_CONTROL_BYTES)
        .and_then(|bytes| bytes.checked_add(inline))
        .and_then(|bytes| bytes.checked_add(512))
        .and_then(|bytes| bytes.checked_mul(8))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(StorageValueError::SizeOverflow)
}
