//! Bounded operator values; constructing one does not grant cutover or readiness.
pub use riffdb_auth::ReplicationPromotionRequestV1 as PromoteFollowerRequest;
use riffdb_auth::StoredPromotionAdministrationV1;
use riffdb_types::{
    AdministrationSequence, CommitSequence, LeadershipEpochV1, ReplicationFollowerAuditTargetV1,
    ReplicationPromotionOperationId,
};

/// Summary released by the eventual promotion owner after complete reconciliation.
/// The complete principal, approval and source evidence remain in durable audit.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct PromoteFollowerResult {
    request: PromoteFollowerRequest,
    administration_sequence: AdministrationSequence,
    applied_application: Option<CommitSequence>,
    history_incarnation: u64,
    leadership_epoch: LeadershipEpochV1,
    application_rpo: u64,
    replayed: bool,
}
impl PromoteFollowerResult {
    pub(crate) fn matches_request(self, request: PromoteFollowerRequest) -> bool {
        self.request == request
    }

    /// Projects checked committed evidence without conferring runtime readiness.
    pub fn from_committed_record(
        record: &StoredPromotionAdministrationV1,
        replayed: bool,
    ) -> Option<Self> {
        let selection = record.attempt().selection()?;
        Some(Self {
            request: record.attempt().request(),
            administration_sequence: record.administration_sequence(),
            applied_application: selection.applied().frontier().application(),
            history_incarnation: selection.published_lineage().history_incarnation(),
            leadership_epoch: selection.published_lineage().leadership_epoch(),
            application_rpo: selection.application_rpo(),
            replayed,
        })
    }
    /// Stable promotion operation identity.
    #[must_use]
    pub const fn operation_id(self) -> ReplicationPromotionOperationId {
        self.request.operation_id()
    }
    /// Exact old-lineage follower registration target.
    #[must_use]
    pub const fn target(self) -> ReplicationFollowerAuditTargetV1 {
        self.request.target()
    }
    /// Original registration generation.
    #[must_use]
    pub const fn generation(self) -> riffdb_auth::ChangelogTransactionSequence {
        self.request.generation()
    }
    /// New-lineage committed control sequence.
    #[must_use]
    pub const fn administration_sequence(self) -> AdministrationSequence {
        self.administration_sequence
    }
    /// Applied application prefix; None represents BeforeFirst.
    #[must_use]
    pub const fn applied_application(self) -> Option<CommitSequence> {
        self.applied_application
    }
    /// Checked successor incarnation.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation
    }
    /// Checked successor leadership epoch.
    #[must_use]
    pub const fn leadership_epoch(self) -> LeadershipEpochV1 {
        self.leadership_epoch
    }
    /// Exact lost application-sequence count at fencing.
    #[must_use]
    pub const fn application_rpo(self) -> u64 {
        self.application_rpo
    }
    /// Whether this observation reuses an already committed operation.
    #[must_use]
    pub const fn replayed(self) -> bool {
        self.replayed
    }
}
impl std::fmt::Debug for PromoteFollowerResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PromoteFollowerResult([redacted])")
    }
}
