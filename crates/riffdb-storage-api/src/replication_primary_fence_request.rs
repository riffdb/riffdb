//! Exact caller selection; this value proves neither authority nor source history.
use crate::ChangelogTransactionSequence;
use riffdb_types::{ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1, RequestId};

/// Immutable fence request with distinct operation and transport identities.
/// The caller selects no final frontier, administration sequence or source proof.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct PrimaryFenceRequestV1 {
    request_id: RequestId,
    operation_id: ReplicationFenceOperationId,
    target: ReplicationFollowerAuditTargetV1,
    generation: ChangelogTransactionSequence,
}

impl PrimaryFenceRequestV1 {
    /// Selects one registered follower generation. The drained source transaction
    /// must validate current lineage, registration, authority and exact replay.
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        operation_id: ReplicationFenceOperationId,
        target: ReplicationFollowerAuditTargetV1,
        generation: ChangelogTransactionSequence,
    ) -> Self {
        Self {
            request_id,
            operation_id,
            target,
            generation,
        }
    }
    /// Authenticated invocation identity; a retry may use another invocation.
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }
    /// Stable operation identity retained across retries.
    #[must_use]
    pub const fn operation_id(self) -> ReplicationFenceOperationId {
        self.operation_id
    }
    /// Selected follower and exact source lineage.
    #[must_use]
    pub const fn target(self) -> ReplicationFollowerAuditTargetV1 {
        self.target
    }
    /// Exact original registration transaction, never an application sequence.
    #[must_use]
    pub const fn generation(self) -> ChangelogTransactionSequence {
        self.generation
    }
}

impl std::fmt::Debug for PrimaryFenceRequestV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrimaryFenceRequestV1([REDACTED])")
    }
}
