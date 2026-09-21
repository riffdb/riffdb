//! Operator-selected primary fencing, never a promotion proof or an unfence.
use riffdb_auth::ChangelogTransactionSequence;
pub use riffdb_commit::{
    PrimaryFenceOutcome as FenceReplicationPrimaryResult, PrimaryFenceRefusal,
    PrimaryFenceResultReceipt,
};
use riffdb_types::{ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1};
/// Exact bounded selection; final sequence and administration receipt are coordinator-owned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FenceReplicationPrimaryRequest {
    operation_id: ReplicationFenceOperationId,
    target: ReplicationFollowerAuditTargetV1,
    generation: ChangelogTransactionSequence,
}
impl FenceReplicationPrimaryRequest {
    /// Selects one operation and exact follower generation under current authority.
    #[must_use]
    pub const fn new(
        operation_id: ReplicationFenceOperationId,
        target: ReplicationFollowerAuditTargetV1,
        generation: ChangelogTransactionSequence,
    ) -> Self {
        Self {
            operation_id,
            target,
            generation,
        }
    }
    /// Caller-stable identity retained across retries.
    #[must_use]
    pub const fn operation_id(self) -> ReplicationFenceOperationId {
        self.operation_id
    }
    /// Selected source lineage and follower.
    #[must_use]
    pub const fn target(self) -> ReplicationFollowerAuditTargetV1 {
        self.target
    }
    /// Selected original registration transaction.
    #[must_use]
    pub const fn generation(self) -> ChangelogTransactionSequence {
        self.generation
    }
}
