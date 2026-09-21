//! Complete cutover evidence, not an offline owner, authorization or readiness permit.
use super::{ReplicationPromotionPhaseV1, ReplicationPromotionReceiptV1};
use crate::{AdministrationSequenceAllocator, StorageValueError};
use riffdb_types::{AdministrationSequence, DualFrontier, ServiceIngressKindV1, Timestamp};

/// New-lineage administration evidence for one exact cutover. The storage owner
/// must atomically persist this value, its start/success service-audit records,
/// the checked lineage stamp and Promotion anchor, after independently proving
/// current authority, authenticated fencing and exclusive drained ownership.
///
/// The embedded external attempt stops at CutoverPending. A database commit is
/// not evidence that subsequent ordinary source validation or external terminal
/// publication completed. Those steps must reconcile with this exact value.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredPromotionAdministrationV1 {
    attempt: ReplicationPromotionReceiptV1,
    timestamp: Timestamp,
    ingress: ServiceIngressKindV1,
    started_sequence: AdministrationSequence,
    administration_sequence: AdministrationSequence,
    succeeded_sequence: AdministrationSequence,
    covered_frontier: DualFrontier,
    next_administration: AdministrationSequenceAllocator,
}

impl StoredPromotionAdministrationV1 {
    /// Derives the entire consecutive start/control/success allocation from the
    /// frozen applied frontier. The caller cannot select any assigned sequence,
    /// RPO, incarnation or epoch. Storage must also verify the actual allocator.
    pub fn new(
        attempt: ReplicationPromotionReceiptV1,
        timestamp: Timestamp,
        ingress: ServiceIngressKindV1,
    ) -> Result<Self, StorageValueError> {
        if attempt.phase() != ReplicationPromotionPhaseV1::CutoverPending
            || attempt.is_terminal()
            || !matches!(
                ingress,
                ServiceIngressKindV1::Grpc | ServiceIngressKindV1::InProcessTestComparison
            )
        {
            return Err(StorageValueError::InvalidShape);
        }
        let applied = attempt
            .selection()
            .ok_or(StorageValueError::InvalidShape)?
            .applied()
            .frontier();
        let first = match applied.administration() {
            Some(previous) => previous
                .checked_next()
                .ok_or(StorageValueError::SizeOverflow)?,
            None => AdministrationSequence::first(),
        };
        let allocation = AdministrationSequenceAllocator::next(first)
            .allocate_consecutive(3)
            .map_err(|_| StorageValueError::SizeOverflow)?;
        let [
            started_sequence,
            administration_sequence,
            succeeded_sequence,
        ] = allocation.assigned()
        else {
            return Err(StorageValueError::InvalidShape);
        };
        Ok(Self {
            attempt,
            timestamp,
            ingress,
            started_sequence: *started_sequence,
            administration_sequence: *administration_sequence,
            succeeded_sequence: *succeeded_sequence,
            covered_frontier: DualFrontier::new(applied.application(), Some(*succeeded_sequence)),
            next_administration: allocation.next(),
        })
    }

    /// Rechecks the redundant stored control sequence through the same complete
    /// allocation. No decoder may reinterpret an existing result on overflow.
    pub fn from_canonical_parts(
        administration_sequence: AdministrationSequence,
        attempt: ReplicationPromotionReceiptV1,
        timestamp: Timestamp,
        ingress: ServiceIngressKindV1,
    ) -> Result<Self, StorageValueError> {
        let value = Self::new(attempt, timestamp, ingress)?;
        if value.administration_sequence != administration_sequence {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(value)
    }

    /// Exact authenticated attempt and immutable selection before possible cutover.
    #[must_use]
    pub const fn attempt(&self) -> &ReplicationPromotionReceiptV1 {
        &self.attempt
    }
    /// Final authorization/audit sample, independent of the initial attempt time.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    /// Operator transport identity; MCP is never a promotion ingress.
    #[must_use]
    pub const fn ingress(&self) -> ServiceIngressKindV1 {
        self.ingress
    }
    /// New-lineage normal service-audit start, immediately before this control row.
    #[must_use]
    pub const fn started_sequence(&self) -> AdministrationSequence {
        self.started_sequence
    }
    /// Exact cutover control sequence, never the source fence's sequence.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }
    /// Normal service-audit cutover success, immediately after this control row.
    #[must_use]
    pub const fn succeeded_sequence(&self) -> AdministrationSequence {
        self.succeeded_sequence
    }
    /// Unchanged applied application head and all three new administration rows.
    #[must_use]
    pub const fn covered_frontier(&self) -> DualFrontier {
        self.covered_frontier
    }
    /// Exact post-cutover allocator, including explicit exhaustion at the boundary.
    #[must_use]
    pub const fn next_administration(&self) -> AdministrationSequenceAllocator {
        self.next_administration
    }

    /// Reconstructs the exact ordinary service-audit pair committed with cutover.
    /// It records physical cutover success; readiness still requires validation
    /// and external receipt reconciliation by the runtime owner.
    pub fn service_audits(
        &self,
    ) -> Result<[crate::StoredServiceAuditRecordV1; 2], StorageValueError> {
        use riffdb_types::{
            ServiceAuditLinkV1 as Link, ServiceAuditPhaseV1 as Phase,
            ServiceAuditTargetV1 as Target, ServiceAuditTargetsV1, ServiceOperationV1,
        };
        let make = |sequence, phase, link| {
            crate::StoredServiceAuditRecordV1::from_stored_parts(
                sequence,
                self.attempt.request_id(),
                self.timestamp,
                ServiceOperationV1::PromoteFollower,
                phase,
                Some(self.attempt.principal().clone()),
                self.ingress,
                ServiceAuditTargetsV1::new([Target::ReplicationFollower(
                    self.attempt.request().target(),
                )])
                .map_err(|_| StorageValueError::InvalidShape)?,
                self.attempt.approval_id().cloned(),
                link,
            )
        };
        Ok([
            make(self.started_sequence, Phase::Started, Link::None)?,
            make(
                self.succeeded_sequence,
                Phase::Succeeded,
                Link::ControlPlane {
                    administration_sequence: self.administration_sequence,
                },
            )?,
        ])
    }

    /// Binds a normal source-mode retry result to this original control row.
    /// Each new invocation retains its own freshly authenticated principal and
    /// approval; this target/link check never authorizes that invocation.
    #[must_use]
    pub fn matches_service_result(
        &self,
        operation: riffdb_types::ServiceOperationV1,
        targets: &riffdb_types::ServiceAuditTargetsV1,
    ) -> bool {
        operation == riffdb_types::ServiceOperationV1::PromoteFollower
            && matches!(targets.as_slice(), [riffdb_types::ServiceAuditTargetV1::ReplicationFollower(target)]
                if *target == self.attempt.request().target())
    }
}

impl std::fmt::Debug for StoredPromotionAdministrationV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoredPromotionAdministrationV1([redacted])")
    }
}
