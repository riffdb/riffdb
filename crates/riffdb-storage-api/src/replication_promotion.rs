//! Bounded promotion evidence, never peer authentication or a cutover capability.
//!
//! The exclusive owner must obtain current authority and authenticated source
//! proof, drain the sole follower applier and close readers before cutover.
//! Constructing or decoding these values establishes none of those conditions.

#[path = "replication_promotion_receipt.rs"]
mod receipt;
pub use receipt::*;
#[path = "replication_promotion_inventory.rs"]
mod inventory;
pub use inventory::*;
#[path = "replication_promotion_administration.rs"]
mod administration;
pub use administration::*;

use crate::{
    ChangelogHistoryPointV3, ChangelogLineageV3, ChangelogTransactionSequence,
    PrimaryFenceSourceEvidenceV1, ReplicationFollowerStateV3, StorageValueError,
};
use riffdb_types::{
    ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1, ReplicationPromotionOperationId,
};

/// Exact stable request. Applications cannot select a result frontier, RPO,
/// incarnation or epoch. Invocation request IDs belong to individual audit attempts.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationPromotionRequestV1 {
    operation_id: ReplicationPromotionOperationId,
    fence_operation_id: ReplicationFenceOperationId,
    target: ReplicationFollowerAuditTargetV1,
    generation: ChangelogTransactionSequence,
}

impl ReplicationPromotionRequestV1 {
    /// Binds the existing source fence and registered follower generation.
    /// This does not prove that either exists or authorize promotion.
    #[must_use]
    pub const fn new(
        operation_id: ReplicationPromotionOperationId,
        fence_operation_id: ReplicationFenceOperationId,
        target: ReplicationFollowerAuditTargetV1,
        generation: ChangelogTransactionSequence,
    ) -> Self {
        Self {
            operation_id,
            fence_operation_id,
            target,
            generation,
        }
    }
    /// Stable promotion identity, separate from source fencing and transport.
    #[must_use]
    pub const fn operation_id(self) -> ReplicationPromotionOperationId {
        self.operation_id
    }
    /// The source fence that must be proved through the configured TLS peer.
    #[must_use]
    pub const fn fence_operation_id(self) -> ReplicationFenceOperationId {
        self.fence_operation_id
    }
    /// Exact selected follower in its original source lineage.
    #[must_use]
    pub const fn target(self) -> ReplicationFollowerAuditTargetV1 {
        self.target
    }
    /// Original physical registration generation, never an application position.
    #[must_use]
    pub const fn generation(self) -> ChangelogTransactionSequence {
        self.generation
    }
}

/// Immutable choice for the external promotion receipt. The evidence retains
/// the original source fence and exact applied point; derived values cannot be
/// substituted by a caller or changed when reconstructing persisted metadata.
#[derive(Clone, Eq, PartialEq)]
pub struct ReplicationPromotionSelectionV1 {
    request: ReplicationPromotionRequestV1,
    evidence: PrimaryFenceSourceEvidenceV1,
    published_lineage: ChangelogLineageV3,
}

impl ReplicationPromotionSelectionV1 {
    /// Checks an attached follower's exact drained position against source
    /// evidence and derives both checked successors before any mutation.
    /// The caller still owns proof of drain, durability, ancestry and peer origin.
    pub fn new(
        request: ReplicationPromotionRequestV1,
        follower: ReplicationFollowerStateV3,
        evidence: PrimaryFenceSourceEvidenceV1,
    ) -> Result<Self, StorageValueError> {
        let (lineage, applied, _) = follower
            .attached_state()
            .ok_or(StorageValueError::InvalidShape)?;
        if lineage != evidence.fence().lineage() || applied != evidence.applied() {
            return Err(StorageValueError::IdentityMismatch);
        }
        let published = successor_lineage(lineage)?;
        let rpo = evidence.application_rpo();
        Self::from_canonical_parts(request, evidence, published, rpo)
    }

    /// Checks every redundant persisted choice through the same validator.
    /// This is evidence reconstruction, not fresh authorization or peer proof.
    pub fn from_canonical_parts(
        request: ReplicationPromotionRequestV1,
        evidence: PrimaryFenceSourceEvidenceV1,
        published_lineage: ChangelogLineageV3,
        application_rpo: u64,
    ) -> Result<Self, StorageValueError> {
        let fence = evidence.fence();
        if request.fence_operation_id != fence.operation_id()
            || request.target != fence.target()
            || request.generation != fence.generation()
            || published_lineage != successor_lineage(fence.lineage())?
            || application_rpo != evidence.application_rpo()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            request,
            evidence,
            published_lineage,
        })
    }

    /// Frozen request; retries cannot select another follower or source fence.
    #[must_use]
    pub const fn request(&self) -> ReplicationPromotionRequestV1 {
        self.request
    }
    /// Exact source evidence, still requiring independent authenticated origin.
    #[must_use]
    pub const fn evidence(&self) -> &PrimaryFenceSourceEvidenceV1 {
        &self.evidence
    }
    /// Complete drained position, including history hash and both frontiers.
    #[must_use]
    pub const fn applied(&self) -> ChangelogHistoryPointV3 {
        self.evidence.applied()
    }
    /// Exact application-sequence loss at fencing; BeforeFirst contributes zero.
    #[must_use]
    pub const fn application_rpo(&self) -> u64 {
        self.evidence.application_rpo()
    }
    /// Same database and catalog with exact incremented incarnation and epoch.
    #[must_use]
    pub const fn published_lineage(&self) -> ChangelogLineageV3 {
        self.published_lineage
    }
}

fn successor_lineage(source: ChangelogLineageV3) -> Result<ChangelogLineageV3, StorageValueError> {
    let incarnation = source
        .history_incarnation()
        .checked_add(1)
        .ok_or(StorageValueError::SizeOverflow)?;
    let epoch = source
        .leadership_epoch()
        .checked_next()
        .ok_or(StorageValueError::SizeOverflow)?;
    ChangelogLineageV3::new_with_catalog(
        source.database_id(),
        incarnation,
        epoch,
        source.catalog_digest(),
    )
    .map_err(|_| StorageValueError::InvalidShape)
}

impl std::fmt::Debug for ReplicationPromotionRequestV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationPromotionRequestV1([redacted])")
    }
}
impl std::fmt::Debug for ReplicationPromotionSelectionV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationPromotionSelectionV1([redacted])")
    }
}
