//! Bounded operator-selected follower lifecycle values; none grant authority.

use riffdb_auth::{ChangelogTransactionSequence, FollowerHoldBudget};
use riffdb_types::{CommitSequence, ReplicationFollowerAuditTargetV1};

pub use riffdb_commit::{
    ReplicationAdministrationOutcome as FollowerAdministrationResult,
    ReplicationAdministrationRefusal as FollowerAdministrationRefusal,
    ReplicationAdministrationResultReceipt as FollowerAdministrationReceipt,
};

/// Immutable policy for one lineage-scoped follower registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegisterFollowerRequest {
    target: ReplicationFollowerAuditTargetV1,
    budget: FollowerHoldBudget,
    expires_at: Option<CommitSequence>,
}
impl RegisterFollowerRequest {
    /// Selects an exact target and checked policy; current source state is checked later.
    #[must_use]
    pub const fn new(
        target: ReplicationFollowerAuditTargetV1,
        budget: FollowerHoldBudget,
        expires_at: Option<CommitSequence>,
    ) -> Self {
        Self {
            target,
            budget,
            expires_at,
        }
    }
    /// Exact request-selected target, never a capability.
    #[must_use]
    pub const fn target(self) -> ReplicationFollowerAuditTargetV1 {
        self.target
    }
    /// Nonzero sequence budget; exhaustion degrades health without releasing custody.
    #[must_use]
    pub const fn budget(self) -> FollowerHoldBudget {
        self.budget
    }
    /// Optional application-sequence expiry.
    #[must_use]
    pub const fn expires_at(self) -> Option<CommitSequence> {
        self.expires_at
    }
}

/// Exact generation to retire; it cannot release a different registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetireFollowerRequest {
    target: ReplicationFollowerAuditTargetV1,
    generation: ChangelogTransactionSequence,
}
impl RetireFollowerRequest {
    /// Selects the target and original registration generation.
    #[must_use]
    pub const fn new(
        target: ReplicationFollowerAuditTargetV1,
        generation: ChangelogTransactionSequence,
    ) -> Self {
        Self { target, generation }
    }
    /// Exact lineage-scoped follower selection.
    #[must_use]
    pub const fn target(self) -> ReplicationFollowerAuditTargetV1 {
        self.target
    }
    /// Original registration generation, checked again by the coordinator.
    #[must_use]
    pub const fn generation(self) -> ChangelogTransactionSequence {
        self.generation
    }
}

/// Registration uses the common checked lifecycle result.
pub type RegisterFollowerResult = FollowerAdministrationResult;
/// Retirement uses the common checked lifecycle result.
pub type RetireFollowerResult = FollowerAdministrationResult;
