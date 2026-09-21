//! Closed source fence entry: the drained lease travels with the physical owner.
use super::*;
use crate::primary_fence_write::{
    PrimaryFenceAwaiting, PrimaryFenceCandidate, PrimaryFenceCompletion,
};
use riffdb_storage_api::{
    AuditPrincipalV1, PrimaryFenceAwaitingDecision, PrimaryFenceCandidateTransaction,
    PrimaryFenceCandidateV1, PrimaryFenceIntentV1, PrimaryFenceRequestV1, PrimaryFenceResultV1,
    PrimaryFenceTransactionPort, TransactionCurrentCapabilityObservationV1,
};

/// Drained fence candidate. Its physical writer and lease never escape this owner.
pub struct RedbPrimaryFenceCandidate(PrimaryFenceCandidate);
/// Current authority observation retained across the coordinator's pure policy decision.
pub struct RedbPrimaryFenceAwaiting(PrimaryFenceAwaiting);

impl PrimaryFenceTransactionPort for RedbOperationalPorts {
    type Candidate = RedbPrimaryFenceCandidate;
    fn begin_primary_fence_transaction(
        &self,
        candidate: PrimaryFenceCandidateV1,
    ) -> Result<Self::Candidate, StorageError> {
        self.begin_primary_fence(candidate.request(), candidate.principal().clone())
            .map(RedbPrimaryFenceCandidate)
    }
}

impl PrimaryFenceCandidateTransaction for RedbPrimaryFenceCandidate {
    type Awaiting = RedbPrimaryFenceAwaiting;
    fn read_transaction_current(
        self,
    ) -> Result<
        (
            Self::Awaiting,
            Option<TransactionCurrentCapabilityObservationV1>,
        ),
        StorageError,
    > {
        let (awaiting, current) = self.0.read_transaction_current()?;
        Ok((RedbPrimaryFenceAwaiting(awaiting), current))
    }
    fn abandon(self) {}
}

impl PrimaryFenceAwaitingDecision for RedbPrimaryFenceAwaiting {
    fn commit(self, intent: PrimaryFenceIntentV1) -> Result<PrimaryFenceResultV1, StorageError> {
        let candidate = intent.candidate();
        match self.0.stage(
            candidate.request(),
            candidate.principal().clone(),
            intent.timestamp(),
        )? {
            PrimaryFenceCompletion::Write(write) => write
                .commit()
                .map(|record| PrimaryFenceResultV1::Applied(Box::new(record))),
            PrimaryFenceCompletion::Replay(record) => Ok(PrimaryFenceResultV1::Replayed(record)),
            PrimaryFenceCompletion::Refused(refusal) => Ok(PrimaryFenceResultV1::Refused(refusal)),
        }
    }
    fn abandon(self) {}
}

impl RedbOperationalPorts {
    /// The coordinator must pause application admissions before entry and keep
    /// that pause through fresh policy authorization and the resulting commit.
    /// This internal storage entry grants no policy or promotion authority.
    pub(crate) fn begin_primary_fence(
        &self,
        request: PrimaryFenceRequestV1,
        principal: AuditPrincipalV1,
    ) -> Result<PrimaryFenceCandidate, StorageError> {
        if self.shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let write = Barrier::acquire(Arc::clone(&self.shared))?.begin()?;
        PrimaryFenceCandidate::from_drained(write, request, principal)
    }

    #[cfg(test)]
    pub(crate) fn primary_fence_lease_is_held(&self) -> bool {
        self.shared.mutation_gate.is_held()
    }
}
