//! Final primary-fence authorization under the storage owner's drained barrier.
use super::*;
use riffdb_policy::AuthorizedPrimaryFencePreparation;
use riffdb_storage_api::{
    PrimaryFenceAwaitingDecision, PrimaryFenceCandidateTransaction, PrimaryFenceCandidateV1,
    PrimaryFenceIntentV1, PrimaryFenceResultV1, PrimaryFenceTransactionPort,
};

/// Internal checked result. A receipt is not authenticated remote fence proof.
#[derive(Debug)]
pub struct PrimaryFenceExecutionResult {
    outcome: PrimaryFenceResultV1,
}
#[path = "primary_fence_public.rs"]
mod public;
pub use public::*;
impl PrimaryFenceExecutionResult {
    /// Returns a bounded operator summary, never authenticated source proof.
    #[must_use]
    pub fn public_outcome(&self) -> PrimaryFenceOutcome {
        match &self.outcome {
            PrimaryFenceResultV1::Applied(record) => {
                PrimaryFenceOutcome::Applied(PrimaryFenceResultReceipt::from_record(record))
            }
            PrimaryFenceResultV1::Replayed(record) => {
                PrimaryFenceOutcome::Replayed(PrimaryFenceResultReceipt::from_record(record))
            }
            PrimaryFenceResultV1::Refused(refusal) => PrimaryFenceOutcome::Refused(*refusal),
        }
    }

    /// Returns the checked durable outcome or typed request refusal.
    #[must_use]
    pub fn outcome(&self) -> &PrimaryFenceResultV1 {
        &self.outcome
    }
    /// Links successful completion to the original administration receipt.
    #[must_use]
    pub fn terminal_audit(&self) -> ControlPlaneTerminalAudit {
        terminal_audit(match &self.outcome {
            PrimaryFenceResultV1::Applied(record) | PrimaryFenceResultV1::Replayed(record) => {
                Some(record.administration_sequence())
            }
            PrimaryFenceResultV1::Refused(_) => None,
        })
    }
}

pub(crate) fn drive_primary_fence<R: PrimaryFenceTransactionPort + ?Sized>(
    repository: &R,
    clock: &dyn AuthorizationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: AuthorizedPrimaryFencePreparation,
) -> Result<PrimaryFenceExecutionResult, ControlPlaneExecutionError> {
    let principal = preparation.principal();
    let candidate = PrimaryFenceCandidateV1::new(
        preparation.request(),
        AuditPrincipalV1::new(
            principal.principal_id().clone(),
            principal.actor_kind(),
            principal.capability_id(),
            principal.capability_revision(),
        ),
    );
    // Opening drains prior journal publication and can itself encounter an
    // uncertain durable write. Preserve the writer's uncertainty classification.
    let transaction = repository
        .begin_primary_fence_transaction(candidate.clone())
        .map_err(|error| classify_write_error(error, lifecycle))?;
    let (awaiting, current) = transaction
        .read_transaction_current()
        .map_err(|error| classify_read_error(error, lifecycle))?;
    let Some(current) = current else {
        awaiting.abandon();
        return Err(ControlPlaneExecutionError::authorization(
            PolicyCode::InactiveOrStaleCapability,
        ));
    };
    let current = match lower_current_capability(&current) {
        Ok(current) => current,
        Err(error) => {
            awaiting.abandon();
            lifecycle.stop();
            return Err(error);
        }
    };
    let now = match clock.now() {
        Ok(now) => now,
        Err(error) => {
            awaiting.abandon();
            return Err(ControlPlaneExecutionError {
                kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
                detail: ControlPlaneExecutionErrorDetail::AuthorizationClock(error),
            });
        }
    };
    let authorized = match preparation.reauthorize(&current, now) {
        Ok(authorized) => authorized,
        Err(code) => {
            awaiting.abandon();
            return Err(ControlPlaneExecutionError::authorization(code));
        }
    };
    let (preparation, timestamp) = authorized.into_parts();
    if preparation.request() != candidate.request() {
        awaiting.abandon();
        lifecycle.stop();
        return Err(internal_defect(
            ControlPlaneExecutionErrorDetail::CapabilityFacts,
        ));
    }
    let outcome = awaiting
        .commit(PrimaryFenceIntentV1::new(candidate.clone(), timestamp))
        .map_err(|error| classify_write_error(error, lifecycle))?;
    if !matches_result(&outcome, &candidate, timestamp) {
        lifecycle.stop();
        return Err(internal_defect(
            ControlPlaneExecutionErrorDetail::CapabilityFacts,
        ));
    }
    Ok(PrimaryFenceExecutionResult { outcome })
}

fn matches_result(
    outcome: &PrimaryFenceResultV1,
    candidate: &PrimaryFenceCandidateV1,
    timestamp: Timestamp,
) -> bool {
    let (record, applied) = match outcome {
        PrimaryFenceResultV1::Applied(record) => (record, true),
        PrimaryFenceResultV1::Replayed(record) => (record, false),
        PrimaryFenceResultV1::Refused(_) => return true,
    };
    let request = candidate.request();
    record.operation_id() == request.operation_id()
        && record.target() == request.target()
        && record.generation() == request.generation()
        && record.principal() == candidate.principal()
        && (!applied
            || (record.request_id() == request.request_id() && record.timestamp() == timestamp))
}

#[cfg(test)]
#[path = "primary_fence_driver_tests.rs"]
pub(crate) mod tests;
