//! Consuming read-only confirmation of one retained idempotency observation.

use std::{error::Error, fmt};

use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, ExecutablePlanRef, IdempotencyLookupCandidatesV1,
    StorageError, StoredAdmissionStateV1, StoredExecutionFailedV1, StoredOutcomeV1,
    StoredPendingAdmissionV1,
};
use riffdb_types::{
    ActorId, CanonicalInputHash, CanonicalRecord, CommandId, ContractLineage, DatabaseId,
    Environment, TenantScope,
};

use crate::PreparedCommandIdempotencyV1;

/// A move-only, plan-bound preparation for one transaction-adjacent recheck.
///
/// Construction retains the original observation, exact normalized input, and
/// exact selected plan. The value has no durable representation and can be used
/// for only one repository lookup.
///
/// ```compile_fail
/// use riffdb_idempotency::PreparedIdempotencyRecheckV1;
///
/// fn cannot_duplicate(value: &PreparedIdempotencyRecheckV1) {
///     let _: PreparedIdempotencyRecheckV1 =
///         <PreparedIdempotencyRecheckV1 as Clone>::clone(value);
/// }
/// ```
pub struct PreparedIdempotencyRecheckV1 {
    prepared_command: PreparedCommandIdempotencyV1,
    normalized_input: CanonicalRecord,
    selected_plan: ExecutablePlanRef,
    original_observation: AdmissionLookupResultV1,
}

impl PreparedIdempotencyRecheckV1 {
    pub(crate) const fn new(
        prepared_command: PreparedCommandIdempotencyV1,
        normalized_input: CanonicalRecord,
        selected_plan: ExecutablePlanRef,
        original_observation: AdmissionLookupResultV1,
    ) -> Self {
        Self {
            prepared_command,
            normalized_input,
            selected_plan,
            original_observation,
        }
    }

    /// Returns whether this preparation is bound to both supplied values.
    ///
    /// This comparison is non-consuming and intentionally reveals neither the
    /// retained values nor which value differed.
    #[must_use]
    pub fn matches_preparation(
        &self,
        selected_plan: &ExecutablePlanRef,
        normalized_input: &CanonicalRecord,
    ) -> bool {
        &self.selected_plan == selected_plan && &self.normalized_input == normalized_input
    }

    /// Returns whether every retained digest candidate has the expected scope.
    ///
    /// Expected values are supplied separately so callers must derive database
    /// and environment from trusted configuration and authorization-owned facts
    /// from the exact allow proof. Digest material and retained identities are
    /// not exposed, and digest-key rotation does not change scope equality.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn matches_scope(
        &self,
        database_id: DatabaseId,
        environment: &Environment,
        tenant_scope: &TenantScope,
        principal_id: &ActorId,
        contract_lineage: &ContractLineage,
        command_id: CommandId,
    ) -> bool {
        self.prepared_command
            .lookup_candidates()
            .as_slice()
            .iter()
            .all(|identity| {
                identity.database_id() == database_id
                    && identity.environment() == environment
                    && identity.tenant_scope() == tenant_scope
                    && identity.principal_id() == principal_id
                    && identity.contract_lineage() == contract_lineage
                    && identity.command_id() == command_id
            })
    }
}

impl fmt::Debug for PreparedIdempotencyRecheckV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedIdempotencyRecheckV1([REDACTED])")
    }
}

/// Move-only authority to propose a new pending admission after stable absence.
///
/// This token is privately constructed only for `Absent -> Absent`. Consuming
/// it returns the exact plan, normalized input, and idempotency preparation that
/// were inspected and rechecked together.
///
/// ```compile_fail
/// use riffdb_idempotency::VacantIdempotencyAdmissionV1;
///
/// fn cannot_duplicate(value: &VacantIdempotencyAdmissionV1) {
///     let _: VacantIdempotencyAdmissionV1 =
///         <VacantIdempotencyAdmissionV1 as Clone>::clone(value);
/// }
/// ```
pub struct VacantIdempotencyAdmissionV1 {
    selected_plan: ExecutablePlanRef,
    normalized_input: CanonicalRecord,
    prepared_command: PreparedCommandIdempotencyV1,
}

impl VacantIdempotencyAdmissionV1 {
    /// Borrows the exact selected executable-plan identity.
    #[must_use]
    pub const fn selected_plan(&self) -> &ExecutablePlanRef {
        &self.selected_plan
    }

    /// Borrows the exact schema-normalized command input.
    #[must_use]
    pub const fn normalized_input(&self) -> &CanonicalRecord {
        &self.normalized_input
    }

    /// Returns the hash bound to the retained normalized input.
    #[must_use]
    pub const fn canonical_input_hash(&self) -> CanonicalInputHash {
        self.prepared_command.canonical_input_hash()
    }

    /// Borrows the exact rotation-aware identities used by both observations.
    #[must_use]
    pub const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        self.prepared_command.lookup_candidates()
    }

    /// Consumes the one-use authority into its exact non-durable components.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ExecutablePlanRef,
        CanonicalRecord,
        PreparedCommandIdempotencyV1,
    ) {
        (
            self.selected_plan,
            self.normalized_input,
            self.prepared_command,
        )
    }
}

impl fmt::Debug for VacantIdempotencyAdmissionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VacantIdempotencyAdmissionV1([REDACTED])")
    }
}

/// Exact pending state paired with the normalized input checked against it.
///
/// This value is move-only so later coordinator code cannot duplicate resume
/// authority while retaining a detached copy of the checked input.
///
/// ```compile_fail
/// use riffdb_idempotency::RecheckedPendingAdmissionV1;
///
/// fn cannot_duplicate(value: &RecheckedPendingAdmissionV1) {
///     let _: RecheckedPendingAdmissionV1 =
///         <RecheckedPendingAdmissionV1 as Clone>::clone(value);
/// }
/// ```
pub struct RecheckedPendingAdmissionV1 {
    pending: StoredPendingAdmissionV1,
    normalized_input: CanonicalRecord,
}

impl RecheckedPendingAdmissionV1 {
    /// Borrows the exact durable pending admission returned by the recheck.
    #[must_use]
    pub const fn pending(&self) -> &StoredPendingAdmissionV1 {
        &self.pending
    }

    /// Borrows the exact schema-normalized input checked against the pending row.
    #[must_use]
    pub const fn normalized_input(&self) -> &CanonicalRecord {
        &self.normalized_input
    }

    /// Consumes the one-use resume value into its exact components.
    #[must_use]
    pub fn into_parts(self) -> (StoredPendingAdmissionV1, CanonicalRecord) {
        (self.pending, self.normalized_input)
    }
}

impl fmt::Debug for RecheckedPendingAdmissionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RecheckedPendingAdmissionV1([REDACTED])")
    }
}

/// Closed result of one consumed transaction-adjacent idempotency recheck.
pub enum IdempotencyRecheckResultV1 {
    /// Both observations were absent; a new pending row may be proposed once.
    Vacant(VacantIdempotencyAdmissionV1),
    /// The exact equal-input pending admission may be resumed.
    Pending(RecheckedPendingAdmissionV1),
    /// The exact equal-input committed outcome must be replayed.
    Outcome(StoredOutcomeV1),
    /// The exact equal-input deterministic failure must be replayed.
    ExecutionFailed(StoredExecutionFailedV1),
    /// A concurrent admission selected another exact historical plan.
    PreparationChanged,
    /// The same exact plan was admitted with different canonical input.
    InputMismatch,
}

impl fmt::Debug for IdempotencyRecheckResultV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Vacant(_) => "Vacant([REDACTED])",
            Self::Pending(_) => "Pending([REDACTED])",
            Self::Outcome(_) => "Outcome([REDACTED])",
            Self::ExecutionFailed(_) => "ExecutionFailed([REDACTED])",
            Self::PreparationChanged => "PreparationChanged",
            Self::InputMismatch => "InputMismatch",
        })
    }
}

/// Safe closed reasons that an observation or transition failed integrity checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotencyRecheckIntegrityV1 {
    /// A historical inspection was bound to a different plan.
    SelectedPlanMismatch,
    /// State observed as present disappeared before recheck.
    PreviouslyPresentMissing,
    /// More than one rotation candidate matched durable state.
    MultipleMatches,
    /// Storage returned an identity outside the exact bounded candidates.
    IdentityOutsideCandidates,
    /// Durable state changed through a transition forbidden by the v1 model.
    ImpossibleTransition,
}

impl fmt::Display for IdempotencyRecheckIntegrityV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("idempotency recheck integrity validation failed")
    }
}

impl Error for IdempotencyRecheckIntegrityV1 {}

/// Failure of the one-read idempotency recheck.
#[derive(Clone, Eq, PartialEq)]
pub enum IdempotencyRecheckError {
    /// The read-only storage lookup failed.
    Storage(StorageError),
    /// The current state violated the closed v1 transition model.
    Integrity(IdempotencyRecheckIntegrityV1),
}

impl fmt::Debug for IdempotencyRecheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => formatter.debug_tuple("Storage").field(error).finish(),
            Self::Integrity(reason) => formatter.debug_tuple("Integrity").field(reason).finish(),
        }
    }
}

impl fmt::Display for IdempotencyRecheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(formatter),
            Self::Integrity(reason) => reason.fmt(formatter),
        }
    }
}

impl Error for IdempotencyRecheckError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Integrity(reason) => Some(reason),
        }
    }
}

impl From<StorageError> for IdempotencyRecheckError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

/// Synchronous executor for one consumed, read-only idempotency recheck.
pub struct IdempotencyRecheckExecutor<'repository> {
    repository: &'repository dyn AdmissionRepository,
}

impl<'repository> IdempotencyRecheckExecutor<'repository> {
    /// Binds one executor to the semantic admission repository.
    #[must_use]
    pub const fn new(repository: &'repository dyn AdmissionRepository) -> Self {
        Self { repository }
    }

    /// Performs exactly one lookup of the originally prepared candidate set.
    ///
    /// The current plan is compared before the canonical input hash. This method
    /// never mutates admission state and samples neither a clock nor entropy.
    pub fn recheck(
        &self,
        prepared: PreparedIdempotencyRecheckV1,
    ) -> Result<IdempotencyRecheckResultV1, IdempotencyRecheckError> {
        let PreparedIdempotencyRecheckV1 {
            prepared_command,
            normalized_input,
            selected_plan,
            original_observation,
        } = prepared;
        let lookup_candidates = prepared_command.lookup_candidates().clone();
        let current_observation = self.repository.lookup_admission(lookup_candidates)?;

        let current = match current_observation {
            AdmissionLookupResultV1::NotFound => {
                return match original_observation {
                    AdmissionLookupResultV1::NotFound => Ok(IdempotencyRecheckResultV1::Vacant(
                        VacantIdempotencyAdmissionV1 {
                            selected_plan,
                            normalized_input,
                            prepared_command,
                        },
                    )),
                    AdmissionLookupResultV1::Found(_) => Err(integrity(
                        IdempotencyRecheckIntegrityV1::PreviouslyPresentMissing,
                    )),
                    AdmissionLookupResultV1::MultipleMatches => Err(integrity(
                        IdempotencyRecheckIntegrityV1::ImpossibleTransition,
                    )),
                };
            }
            AdmissionLookupResultV1::MultipleMatches => {
                return Err(integrity(IdempotencyRecheckIntegrityV1::MultipleMatches));
            }
            AdmissionLookupResultV1::Found(current) => current,
        };

        if !prepared_command
            .lookup_candidates()
            .contains(current.identity())
        {
            return Err(integrity(
                IdempotencyRecheckIntegrityV1::IdentityOutsideCandidates,
            ));
        }

        let original_state = match &original_observation {
            AdmissionLookupResultV1::NotFound => None,
            AdmissionLookupResultV1::Found(state) => Some(state.as_ref()),
            AdmissionLookupResultV1::MultipleMatches => {
                return Err(integrity(
                    IdempotencyRecheckIntegrityV1::ImpossibleTransition,
                ));
            }
        };

        if let Some(original) = original_state
            && original.identity() != current.identity()
        {
            return Err(integrity(
                IdempotencyRecheckIntegrityV1::ImpossibleTransition,
            ));
        }

        if state_plan(&current) != &selected_plan {
            return if original_state.is_none() {
                Ok(IdempotencyRecheckResultV1::PreparationChanged)
            } else {
                Err(integrity(
                    IdempotencyRecheckIntegrityV1::ImpossibleTransition,
                ))
            };
        }

        if let Some(original) = original_state
            && !is_valid_transition(original, &current)
        {
            return Err(integrity(
                IdempotencyRecheckIntegrityV1::ImpossibleTransition,
            ));
        }

        if state_input_hash(&current) != prepared_command.canonical_input_hash() {
            return Ok(IdempotencyRecheckResultV1::InputMismatch);
        }

        Ok(match *current {
            StoredAdmissionStateV1::Pending(pending) => {
                IdempotencyRecheckResultV1::Pending(RecheckedPendingAdmissionV1 {
                    pending,
                    normalized_input,
                })
            }
            StoredAdmissionStateV1::StoredOutcome(outcome) => {
                IdempotencyRecheckResultV1::Outcome(outcome)
            }
            StoredAdmissionStateV1::ExecutionFailed(failure) => {
                IdempotencyRecheckResultV1::ExecutionFailed(failure)
            }
        })
    }
}

impl fmt::Debug for IdempotencyRecheckExecutor<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IdempotencyRecheckExecutor([REDACTED])")
    }
}

fn integrity(reason: IdempotencyRecheckIntegrityV1) -> IdempotencyRecheckError {
    IdempotencyRecheckError::Integrity(reason)
}

fn state_plan(state: &StoredAdmissionStateV1) -> &ExecutablePlanRef {
    match state {
        StoredAdmissionStateV1::Pending(pending) => pending.plan(),
        StoredAdmissionStateV1::StoredOutcome(outcome) => outcome.plan(),
        StoredAdmissionStateV1::ExecutionFailed(failure) => failure.pending().plan(),
    }
}

fn state_input_hash(state: &StoredAdmissionStateV1) -> CanonicalInputHash {
    match state {
        StoredAdmissionStateV1::Pending(pending) => pending.canonical_input_hash(),
        StoredAdmissionStateV1::StoredOutcome(outcome) => outcome.canonical_input_hash(),
        StoredAdmissionStateV1::ExecutionFailed(failure) => {
            failure.pending().canonical_input_hash()
        }
    }
}

fn is_valid_transition(
    original: &StoredAdmissionStateV1,
    current: &StoredAdmissionStateV1,
) -> bool {
    match (original, current) {
        (StoredAdmissionStateV1::Pending(original), StoredAdmissionStateV1::Pending(current)) => {
            original == current
        }
        (
            StoredAdmissionStateV1::Pending(original),
            StoredAdmissionStateV1::StoredOutcome(current),
        ) => outcome_preserves_pending(current, original),
        (
            StoredAdmissionStateV1::Pending(original),
            StoredAdmissionStateV1::ExecutionFailed(current),
        ) => current.pending() == original,
        (
            StoredAdmissionStateV1::StoredOutcome(original),
            StoredAdmissionStateV1::StoredOutcome(current),
        ) => original == current,
        (
            StoredAdmissionStateV1::ExecutionFailed(original),
            StoredAdmissionStateV1::ExecutionFailed(current),
        ) => original == current,
        _ => false,
    }
}

fn outcome_preserves_pending(
    outcome: &StoredOutcomeV1,
    pending: &StoredPendingAdmissionV1,
) -> bool {
    outcome.identity() == pending.identity()
        && outcome.canonical_input_hash() == pending.canonical_input_hash()
        && outcome.admission_request_id() == pending.admission_request_id()
        && outcome.plan() == pending.plan()
        && outcome.logical_time() == pending.logical_time()
        && outcome.actor() == pending.actor()
        && outcome.partition_key() == pending.partition_key()
        && outcome.admitted_claims() == pending.provenance_claims()
}
