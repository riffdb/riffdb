//! Bounded operator receipt summary; it cannot certify a remote source.
pub use riffdb_storage_api::PrimaryFenceRefusalV1 as PrimaryFenceRefusal;
use riffdb_storage_api::{ChangelogTransactionSequence, StoredPrimaryFenceAdministrationV1};
use riffdb_types::{
    AdministrationSequence, CommitSequence, ReplicationFenceOperationId,
    ReplicationFollowerAuditTargetV1,
};

/// Immutable fence result without principal, approval or raw durable bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrimaryFenceResultReceipt {
    operation_id: ReplicationFenceOperationId,
    target: ReplicationFollowerAuditTargetV1,
    generation: ChangelogTransactionSequence,
    administration_sequence: AdministrationSequence,
    final_application_sequence: Option<CommitSequence>,
}
impl PrimaryFenceResultReceipt {
    pub(super) fn from_record(record: &StoredPrimaryFenceAdministrationV1) -> Self {
        Self {
            operation_id: record.operation_id(),
            target: record.target(),
            generation: record.generation(),
            administration_sequence: record.administration_sequence(),
            final_application_sequence: record.observed().frontier().application(),
        }
    }
    /// Original caller-stable operation ID.
    #[must_use]
    pub const fn operation_id(self) -> ReplicationFenceOperationId {
        self.operation_id
    }
    /// Exact registered follower and source lineage.
    #[must_use]
    pub const fn target(self) -> ReplicationFollowerAuditTargetV1 {
        self.target
    }
    /// Original registration transaction.
    #[must_use]
    pub const fn generation(self) -> ChangelogTransactionSequence {
        self.generation
    }
    /// Exact authoritative fence receipt sequence.
    #[must_use]
    pub const fn administration_sequence(self) -> AdministrationSequence {
        self.administration_sequence
    }
    /// Immutable application head at fencing; None means before the first command.
    #[must_use]
    pub const fn final_application_sequence(self) -> Option<CommitSequence> {
        self.final_application_sequence
    }
}
/// Closed fence result; a summary never authorizes promotion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrimaryFenceOutcome {
    /// This invocation durably fenced the source.
    Applied(PrimaryFenceResultReceipt),
    /// Exact authorized retry retained its original fence.
    Replayed(PrimaryFenceResultReceipt),
    /// Selection conflicts with checked source authority.
    Refused(PrimaryFenceRefusal),
}
