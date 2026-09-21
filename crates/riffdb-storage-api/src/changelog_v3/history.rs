//! Bounded V3 retained-root values. Structural consistency does not certify
//! startup validation, ancestry, durable publication, or permission to prune.

use riffdb_types::{DatabaseId, DualFrontier};

use super::{
    AuthoritativeTransactionV3, ChangelogTransactionAllocator, ChangelogTransactionSequence,
    ChangelogV3Error, LeadershipEpochV1,
};
use crate::{AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2};

/// Exact lineage fence and checked catalog binding; neither grants source authority.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ChangelogLineageV3 {
    database_id: DatabaseId,
    history_incarnation: u64,
    leadership_epoch: LeadershipEpochV1,
    catalog_digest: [u8; 32],
}

impl ChangelogLineageV3 {
    /// Reconstructs the nonzero fences, without granting activation or leadership.
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        leadership_epoch: LeadershipEpochV1,
    ) -> Result<Self, ChangelogV3Error> {
        Self::new_with_catalog(
            database_id,
            history_incarnation,
            leadership_epoch,
            AuthoritativeStateCatalogV1.digest(),
        )
    }
    /// Reconstructs one explicitly selected, supported catalog. Callers must
    /// obtain this identity from validated durable or authenticated peer evidence;
    /// selecting V2 never upgrades V1 authority or implies Active admission.
    pub fn new_with_catalog(
        database_id: DatabaseId,
        history_incarnation: u64,
        leadership_epoch: LeadershipEpochV1,
        catalog_digest: [u8; 32],
    ) -> Result<Self, ChangelogV3Error> {
        if history_incarnation == 0
            || (catalog_digest != AuthoritativeStateCatalogV1.digest()
                && catalog_digest != AuthoritativeStateCatalogV2.digest())
        {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        Ok(Self {
            database_id,
            history_incarnation,
            leadership_epoch,
            catalog_digest,
        })
    }
    /// Permanent database identity.
    #[must_use]
    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }
    /// Destructive-restore and promotion fence.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation
    }
    /// Nonzero leadership fence.
    #[must_use]
    pub const fn leadership_epoch(self) -> LeadershipEpochV1 {
        self.leadership_epoch
    }
    /// Exact validated catalog identity; never silently replaced by the current one.
    #[must_use]
    pub const fn catalog_digest(self) -> [u8; 32] {
        self.catalog_digest
    }
}

/// A receipt position with its exact checksum and dual frontier, not a bare counter.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ChangelogHistoryPointV3 {
    sequence: ChangelogTransactionSequence,
    history_hash: [u8; 32],
    frontier: DualFrontier,
}

impl ChangelogHistoryPointV3 {
    /// Reconstructs a point; its storage owner must validate the retained receipt.
    #[must_use]
    pub const fn new(
        sequence: ChangelogTransactionSequence,
        history_hash: [u8; 32],
        frontier: DualFrontier,
    ) -> Self {
        Self {
            sequence,
            history_hash,
            frontier,
        }
    }
    /// Derives the exact point from complete canonical receipt bytes.
    pub fn from_receipt(receipt: &AuthoritativeTransactionV3) -> Result<Self, ChangelogV3Error> {
        Ok(Self::new(
            receipt.binding().sequence,
            receipt.history_hash()?,
            receipt.binding().covered_frontier,
        ))
    }
    /// Physical receipt position.
    #[must_use]
    pub const fn sequence(self) -> ChangelogTransactionSequence {
        self.sequence
    }
    /// Exact history checksum; never diagnostic data.
    #[must_use]
    pub const fn history_hash(self) -> [u8; 32] {
        self.history_hash
    }
    /// Application and administration frontiers covered at this position.
    #[must_use]
    pub const fn frontier(self) -> DualFrontier {
        self.frontier
    }

    /// Checks monotone sequence/frontier shape and exact identity at equality.
    /// This value comparison does not prove receipt ancestry or durability.
    #[must_use]
    pub fn precedes_or_equals(self, other: Self) -> bool {
        if self.sequence == other.sequence {
            return self == other;
        }
        self.sequence < other.sequence
            && (self.frontier == other.frontier || other.frontier.advances_from(self.frontier))
    }
}

/// Lineage-shared materialized history roots at one engine checkpoint.
/// `minimum_resume` is the earliest resumable fence, not proof that its receipt
/// or any successor can be deleted. Storage additionally verifies ancestry and
/// a later known-durable checkpoint plus all registered retention fences.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ChangelogHistoryStateV3 {
    lineage: ChangelogLineageV3,
    anchor: ChangelogHistoryPointV3,
    tail: ChangelogHistoryPointV3,
    minimum_resume: ChangelogHistoryPointV3,
}

impl ChangelogHistoryStateV3 {
    /// Checks bounded root consistency. No read, write or reclamation permit is minted.
    pub fn new(
        lineage: ChangelogLineageV3,
        anchor: ChangelogHistoryPointV3,
        tail: ChangelogHistoryPointV3,
        minimum_resume: ChangelogHistoryPointV3,
    ) -> Result<Self, ChangelogV3Error> {
        if !anchor.precedes_or_equals(minimum_resume) || !minimum_resume.precedes_or_equals(tail) {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        Ok(Self {
            lineage,
            anchor,
            tail,
            minimum_resume,
        })
    }
    /// Exact shared lineage.
    #[must_use]
    pub const fn lineage(self) -> ChangelogLineageV3 {
        self.lineage
    }
    /// Bootstrap-or-later anchor; pre-activation writes have no synthesized tail.
    #[must_use]
    pub const fn anchor(self) -> ChangelogHistoryPointV3 {
        self.anchor
    }
    /// Exact materialized terminal position.
    #[must_use]
    pub const fn tail(self) -> ChangelogHistoryPointV3 {
        self.tail
    }
    /// Earliest resumable position in this retained history.
    #[must_use]
    pub const fn minimum_resume(self) -> ChangelogHistoryPointV3 {
        self.minimum_resume
    }
    /// Computes the counter consistent with this tail, including explicit exhaustion.
    #[must_use]
    pub const fn expected_allocator(self) -> ChangelogTransactionAllocator {
        match self.tail.sequence.checked_next() {
            Some(next) => ChangelogTransactionAllocator::Next(next),
            None => ChangelogTransactionAllocator::Exhausted,
        }
    }
    /// Checks the separately retained counter against the materialized tail.
    /// A journal suffix must be accounted for separately at its published head.
    pub fn validate_allocator(
        self,
        allocator: ChangelogTransactionAllocator,
    ) -> Result<(), ChangelogV3Error> {
        if allocator != self.expected_allocator() {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        Ok(())
    }
    /// Verifies one exact tail receipt without trusting a numeric position alone.
    pub fn validate_terminal_receipt(
        self,
        receipt: &AuthoritativeTransactionV3,
    ) -> Result<(), ChangelogV3Error> {
        super::frame::validate_receipt_catalog(receipt, self.lineage.catalog_digest())?;
        if receipt.binding().database_id != self.lineage.database_id
            || receipt.binding().history_incarnation != self.lineage.history_incarnation
            || ChangelogHistoryPointV3::from_receipt(receipt)? != self.tail
        {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        Ok(())
    }
    /// Derives an unchanged-or-successful successor value from the original receipt.
    /// The caller persists it only with that receipt in the same engine transaction.
    pub fn advance(self, receipt: &AuthoritativeTransactionV3) -> Result<Self, ChangelogV3Error> {
        super::frame::validate_receipt_catalog(receipt, self.lineage.catalog_digest())?;
        let row = receipt.binding();
        if row.database_id != self.lineage.database_id
            || row.history_incarnation != self.lineage.history_incarnation
            || row.predecessor != Some(self.tail.sequence)
            || row.predecessor_frontier != self.tail.frontier
            || row.prior_history_hash != self.tail.history_hash
        {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        Self::new(
            self.lineage,
            self.anchor,
            ChangelogHistoryPointV3::from_receipt(receipt)?,
            self.minimum_resume,
        )
    }
}

/// Follower-local attachment and durable progress. Missing metadata is never
/// interpreted as Detached after V3 activation. No network success is inferred.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationFollowerStateV3 {
    attached: Option<(
        ChangelogLineageV3,
        ChangelogHistoryPointV3,
        Option<ChangelogHistoryPointV3>,
    )>,
}

impl ReplicationFollowerStateV3 {
    /// Explicit source/local state with no upstream attachment.
    #[must_use]
    pub const fn detached() -> Self {
        Self { attached: None }
    }
    /// Checks that acknowledgement cannot outrun or substitute the applied prefix.
    pub fn attached(
        lineage: ChangelogLineageV3,
        applied: ChangelogHistoryPointV3,
        acknowledged: Option<ChangelogHistoryPointV3>,
    ) -> Result<Self, ChangelogV3Error> {
        if acknowledged.is_some_and(|ack| !ack.precedes_or_equals(applied)) {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        Ok(Self {
            attached: Some((lineage, applied, acknowledged)),
        })
    }
    /// Returns checked attachment progress, never a serving-readiness permit.
    #[must_use]
    pub const fn attached_state(
        self,
    ) -> Option<(
        ChangelogLineageV3,
        ChangelogHistoryPointV3,
        Option<ChangelogHistoryPointV3>,
    )> {
        self.attached
    }
}

macro_rules! redacted_debug {
    ($($ty:ident),+) => { $(
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(concat!(stringify!($ty), "([redacted])"))
            }
        }
    )+ };
}
redacted_debug!(
    ChangelogLineageV3,
    ChangelogHistoryPointV3,
    ChangelogHistoryStateV3,
    ReplicationFollowerStateV3
);
