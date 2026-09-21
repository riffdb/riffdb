//! Bounded primary-admission evidence. Values do not authorize a write, certify
//! retained ancestry, authenticate a source, or permit promotion or an upgrade.
#[path = "replication_primary_fence_request.rs"]
mod request;
pub use request::PrimaryFenceRequestV1;
#[path = "replication_primary_fence_transaction.rs"]
mod transaction;
pub use transaction::*;

/// Internal coordinator startup input from an already validated source store.
/// The implementation reads one immutable root, checks its source role and exact
/// lineage, and returns explicit admission. Missing, malformed, foreign or
/// follower state is an error, never an implicit Active default. This read does
/// not replace complete startup validation or authorize any mutation.
pub trait ReplicationPrimaryAdmissionReadPort {
    /// Reads the checked durable admission before any executor is exposed.
    fn read_replication_primary_admission(
        &self,
    ) -> Result<ReplicationPrimaryAdmissionV1, crate::StorageError>;
}

use crate::{
    AuditPrincipalV1, AuthoritativeStateCatalogV2, ChangelogHistoryPointV3, ChangelogLineageV3,
    ChangelogTransactionSequence, StorageValueError,
};
use riffdb_types::{
    AdministrationSequence, ApprovalId, CommitSequence, ReplicationFenceOperationId,
    ReplicationFollowerAuditTargetV1, RequestId, Timestamp,
};

/// Immutable fence administration result. Storage must atomically persist this
/// record, the matching Fenced metadata and the complete V3 transaction, after
/// current authorization and exact registration/history validation under the
/// coordinator barrier. Construction alone proves none of those preconditions.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredPrimaryFenceAdministrationV1 {
    administration_sequence: AdministrationSequence,
    timestamp: Timestamp,
    operation_id: ReplicationFenceOperationId,
    request_id: RequestId,
    principal: AuditPrincipalV1,
    approval_id: Option<ApprovalId>,
    target: ReplicationFollowerAuditTargetV1,
    generation: ChangelogTransactionSequence,
    observed: ChangelogHistoryPointV3,
    lineage: ChangelogLineageV3,
}

impl StoredPrimaryFenceAdministrationV1 {
    /// Binds a service-success link to this exact original fence authority.
    /// Retries may use another request ID, but cannot substitute target,
    /// principal, capability revision or approval. This grants no authority.
    #[must_use]
    pub fn matches_service_result(
        &self,
        operation: riffdb_types::ServiceOperationV1,
        targets: &riffdb_types::ServiceAuditTargetsV1,
        principal: Option<&AuditPrincipalV1>,
        approval_id: Option<&ApprovalId>,
    ) -> bool {
        operation == riffdb_types::ServiceOperationV1::FenceReplicationPrimary
            && principal == Some(&self.principal)
            && approval_id == self.approval_id.as_ref()
            && matches!(targets.as_slice(),
                [riffdb_types::ServiceAuditTargetV1::ReplicationFollower(target)] if *target == self.target)
    }

    /// Checks sequence shape, preserving the exact drained application head.
    /// Retries return the original value, including its original invocation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        administration_sequence: AdministrationSequence,
        timestamp: Timestamp,
        operation_id: ReplicationFenceOperationId,
        request_id: RequestId,
        principal: AuditPrincipalV1,
        approval_id: Option<ApprovalId>,
        target: ReplicationFollowerAuditTargetV1,
        generation: ChangelogTransactionSequence,
        observed: ChangelogHistoryPointV3,
    ) -> Result<Self, StorageValueError> {
        let next_administration = observed
            .frontier()
            .administration()
            .ok_or(StorageValueError::InvalidShape)?
            .get()
            .checked_add(1)
            .and_then(AdministrationSequence::new)
            .ok_or(StorageValueError::InvalidShape)?;
        if next_administration != administration_sequence
            || generation > observed.sequence()
            || observed.sequence().checked_next().is_none()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let lineage = ChangelogLineageV3::new_with_catalog(
            target.database_id(),
            target.history_incarnation(),
            target.leadership_epoch(),
            AuthoritativeStateCatalogV2.digest(),
        )
        .map_err(|_| StorageValueError::IdentityMismatch)?;
        Ok(Self {
            administration_sequence,
            timestamp,
            operation_id,
            request_id,
            principal,
            approval_id,
            target,
            generation,
            observed,
            lineage,
        })
    }
    /// Exact coordinator-assigned control sequence.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }
    /// Final authorization/audit sample supplied by the coordinator.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    /// Caller-stable fence identity, distinct from a transport submission.
    #[must_use]
    pub const fn operation_id(&self) -> ReplicationFenceOperationId {
        self.operation_id
    }
    /// Original authenticated transport submission, preserved on retries.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }
    /// Original authenticated principal and capability revision; no bearer.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }
    /// Original checked approval, when required by current policy.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
    /// Exact selected follower, including database incarnation and epoch.
    #[must_use]
    pub const fn target(&self) -> ReplicationFollowerAuditTargetV1 {
        self.target
    }
    /// Original physical registration generation; never an application sequence.
    #[must_use]
    pub const fn generation(&self) -> ChangelogTransactionSequence {
        self.generation
    }
    /// Drained predecessor, whose ancestry storage must prove independently.
    #[must_use]
    pub const fn observed(&self) -> ChangelogHistoryPointV3 {
        self.observed
    }
    /// Immutable application head; BeforeFirst is represented by absence.
    #[must_use]
    pub const fn final_application_head(&self) -> Option<CommitSequence> {
        self.observed.frontier().application()
    }
    /// Exact source identity and nonzero leadership fences.
    #[must_use]
    pub const fn lineage(&self) -> ChangelogLineageV3 {
        self.lineage
    }
}
impl std::fmt::Debug for StoredPrimaryFenceAdministrationV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoredPrimaryFenceAdministrationV1([redacted])")
    }
}

#[derive(Clone, Eq, PartialEq)]
enum State {
    Active(ChangelogLineageV3),
    Fenced(Box<StoredPrimaryFenceAdministrationV1>),
}

/// Required source-only admission metadata in AuthoritativeStateCatalogV2.
/// Missing metadata is not Active. Attached followers must not retain this row.
/// Constructing Active does not grant a writer or provide an unfence operation.
#[derive(Clone, Eq, PartialEq)]
pub struct ReplicationPrimaryAdmissionV1(State);

impl ReplicationPrimaryAdmissionV1 {
    /// Reconstructs explicit Active evidence. Only a validated atomic initial
    /// activation or promotion may install it; source opens may never infer it.
    pub fn active(lineage: ChangelogLineageV3) -> Result<Self, StorageValueError> {
        if lineage.catalog_digest() != AuthoritativeStateCatalogV2.digest() {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self(State::Active(lineage)))
    }
    /// Retains the complete exact control receipt, with no independently mutable
    /// operation, target, generation, lineage or application-head copy.
    #[must_use]
    pub fn fenced(receipt: StoredPrimaryFenceAdministrationV1) -> Self {
        Self(State::Fenced(Box::new(receipt)))
    }
    /// Identity bound by either closed state.
    #[must_use]
    pub fn lineage(&self) -> ChangelogLineageV3 {
        match &self.0 {
            State::Active(lineage) => *lineage,
            State::Fenced(receipt) => receipt.lineage(),
        }
    }
    /// Exact immutable control evidence, absent only for explicit Active.
    #[must_use]
    pub fn fence(&self) -> Option<&StoredPrimaryFenceAdministrationV1> {
        match &self.0 {
            State::Active(_) => None,
            State::Fenced(receipt) => Some(receipt),
        }
    }
    /// Rejects contradictory materialized source evidence. The storage owner must
    /// independently establish the current lineage/head, locate the original
    /// administration record, validate its complete V3 link, and retain the gate.
    /// Success here is consistency only, never write or promotion authority.
    pub fn validate_source_evidence(
        &self,
        lineage: ChangelogLineageV3,
        current_application_head: Option<CommitSequence>,
        retained_fence: Option<&StoredPrimaryFenceAdministrationV1>,
    ) -> Result<(), StorageValueError> {
        if self.lineage() != lineage {
            return Err(StorageValueError::IdentityMismatch);
        }
        match (&self.0, retained_fence) {
            (State::Active(_), None) => Ok(()),
            (State::Fenced(expected), Some(retained))
                if expected.as_ref() == retained
                    && expected.final_application_head() == current_application_head =>
            {
                Ok(())
            }
            _ => Err(StorageValueError::IdentityMismatch),
        }
    }
}
impl std::fmt::Debug for ReplicationPrimaryAdmissionV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationPrimaryAdmissionV1([redacted])")
    }
}
