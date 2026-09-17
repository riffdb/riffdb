//! Sole-writer lifecycle orchestration using transaction-current policy facts.
use super::*;
use riffdb_policy::AuthorizedReplicationAdministrationPreparation;
use riffdb_storage_api::{
    ReplicationAdministrationAwaitingDecision, ReplicationAdministrationCandidateTransaction,
    ReplicationAdministrationCandidateV1, ReplicationAdministrationIntentV1,
    ReplicationAdministrationResultV1, ReplicationAdministrationTransactionPort,
};

/// Checked storage outcome and exact service-audit result linkage.
#[derive(Debug)]
pub struct ReplicationAdministrationExecutionResult {
    outcome: ReplicationAdministrationResultV1,
}
impl ReplicationAdministrationExecutionResult {
    /// Original receipt on both a new operation and an authorized exact retry.
    #[must_use]
    pub const fn outcome(&self) -> &ReplicationAdministrationResultV1 {
        &self.outcome
    }
    /// No caller-selected sequence can enter the success audit.
    #[must_use]
    pub fn terminal_audit(&self) -> ControlPlaneTerminalAudit {
        terminal_audit(match &self.outcome {
            ReplicationAdministrationResultV1::Applied(record)
            | ReplicationAdministrationResultV1::Replayed(record) => {
                Some(record.administration_sequence())
            }
            ReplicationAdministrationResultV1::Refused(_) => None,
        })
    }
}

pub(crate) fn drive_replication_administration<
    R: ReplicationAdministrationTransactionPort + ?Sized,
>(
    repository: &R,
    clock: &dyn AuthorizationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: AuthorizedReplicationAdministrationPreparation,
) -> Result<ReplicationAdministrationExecutionResult, ControlPlaneExecutionError> {
    let principal = preparation.principal();
    let candidate = ReplicationAdministrationCandidateV1::new(
        preparation.request(),
        AuditPrincipalV1::new(
            principal.principal_id().clone(),
            principal.actor_kind(),
            principal.capability_id(),
            principal.capability_revision(),
        ),
    );
    let transaction = repository
        .begin_replication_administration(candidate.clone())
        .map_err(|error| classify_read_error(error, lifecycle))?;
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
        .commit(ReplicationAdministrationIntentV1::new(candidate, timestamp))
        .map_err(|error| classify_write_error(error, lifecycle))?;
    Ok(ReplicationAdministrationExecutionResult { outcome })
}

pub(crate) fn drive_replication_maintenance<
    R: riffdb_storage_api::ReplicationRegistrationMaintenancePort + ?Sized,
>(
    repository: &R,
    clock: &dyn AdministrationClock,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> Result<
    riffdb_storage_api::ReplicationRegistrationMaintenanceResultV1,
    ControlPlaneExecutionError,
> {
    let timestamp = clock.now().map_err(|error| ControlPlaneExecutionError {
        kind: ControlPlaneExecutionErrorKind::StorageUnavailable,
        detail: ControlPlaneExecutionErrorDetail::AdministrationClock(error),
    })?;
    repository
        .maintain_replication_registration(timestamp)
        .map_err(|error| classify_write_error(error, lifecycle))
}
