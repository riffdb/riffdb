//! Bounded read-only inspection before command-plan selection.

use std::{error::Error, fmt};

use riffdb_storage_api::{
    AdmissionLookupRepository, AdmissionLookupResultV1, ExecutablePlanRef, StorageError,
    StoredAdmissionStateV1,
};
use riffdb_types::{CanonicalRecord, FieldId, IdempotencyKey};

use crate::{
    IdempotencyPreparationError, IdempotencyRecheckIntegrityV1, PreparedCommandIdempotencyV1,
    PreparedIdempotencyLookupV1, PreparedIdempotencyRecheckV1, confirm_command_idempotency,
    confirm_server_derived_command_idempotency,
};

/// The only durable-state information exposed for command-plan selection.
#[derive(Clone, Eq, PartialEq)]
pub enum IdempotencyPlanSelectionV1 {
    /// No readable digest identity currently has durable state.
    Absent,
    /// One identity selected this exact immutable historical command plan.
    Historical(ExecutablePlanRef),
}

impl fmt::Debug for IdempotencyPlanSelectionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => formatter.write_str("Absent"),
            Self::Historical(_) => formatter.write_str("Historical([REDACTED])"),
        }
    }
}

/// A move-only inspection observation retained for coordinator confirmation.
///
/// Only [`Self::plan_selection`] is visible to service plan selection. The
/// complete storage observation and rotation-aware identities remain private and
/// are consumed when input is confirmed. They cannot be cloned or inspected by
/// service or transport code.
///
/// ```compile_fail
/// use riffdb_idempotency::InspectedIdempotencyV1;
///
/// fn cannot_read_storage_observation(value: InspectedIdempotencyV1) {
///     let _ = value.observation;
/// }
/// ```
///
/// ```compile_fail
/// use riffdb_idempotency::InspectedIdempotencyV1;
///
/// fn cannot_duplicate_confirmation(value: &InspectedIdempotencyV1) {
///     let _: InspectedIdempotencyV1 = Clone::clone(value);
/// }
/// ```
pub struct InspectedIdempotencyV1 {
    plan_selection: IdempotencyPlanSelectionV1,
    prepared_lookup: PreparedIdempotencyLookupV1,
    observation: AdmissionLookupResultV1,
}

impl InspectedIdempotencyV1 {
    /// Borrows the absent-or-exact-plan selection and no stored command data.
    #[must_use]
    pub const fn plan_selection(&self) -> &IdempotencyPlanSelectionV1 {
        &self.plan_selection
    }

    /// Confirms normalized input while preserving the original storage observation.
    ///
    /// This consumes the inspection capability so digest candidates cannot be
    /// recomputed or paired with another observation. It performs no storage read
    /// or mutation. The returned value remains opaque for a later coordinator
    /// recheck against the same bounded candidate identities.
    pub fn confirm_input(
        self,
        normalized_input: &CanonicalRecord,
        idempotency_field: FieldId,
        caller_key: &IdempotencyKey,
    ) -> Result<ConfirmedIdempotencyInspectionV1, IdempotencyPreparationError> {
        let prepared_command = confirm_command_idempotency(
            self.prepared_lookup,
            normalized_input,
            idempotency_field,
            caller_key,
        )?;
        Ok(ConfirmedIdempotencyInspectionV1 {
            prepared_command,
            normalized_input: normalized_input.clone(),
            observation: self.observation,
        })
    }

    /// Confirms the complete normalized input for a server-derived identity.
    ///
    /// No input field is treated as caller idempotency material. This path is
    /// reserved for higher-level operator campaigns that already derived the
    /// inspected identity from trusted artifacts.
    pub fn confirm_server_derived_input(
        self,
        normalized_input: &CanonicalRecord,
    ) -> Result<ConfirmedIdempotencyInspectionV1, IdempotencyPreparationError> {
        let prepared_command =
            confirm_server_derived_command_idempotency(self.prepared_lookup, normalized_input)?;
        Ok(ConfirmedIdempotencyInspectionV1 {
            prepared_command,
            normalized_input: normalized_input.clone(),
            observation: self.observation,
        })
    }
}

impl fmt::Debug for InspectedIdempotencyV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InspectedIdempotencyV1([REDACTED])")
    }
}

/// Input-confirmed, move-only evidence reserved for the commit coordinator.
///
/// Neither the newly prepared input hash nor the complete prior storage state is
/// exposed through this interface. A later coordinator slice consumes this value
/// to recheck the original observation before admission, resume, or replay.
///
/// ```compile_fail
/// use riffdb_idempotency::ConfirmedIdempotencyInspectionV1;
///
/// fn cannot_read_prepared_input(value: ConfirmedIdempotencyInspectionV1) {
///     let _ = value.prepared_command;
/// }
/// ```
///
/// ```compile_fail
/// use riffdb_idempotency::ConfirmedIdempotencyInspectionV1;
///
/// fn cannot_duplicate(value: &ConfirmedIdempotencyInspectionV1) {
///     let _: ConfirmedIdempotencyInspectionV1 =
///         <ConfirmedIdempotencyInspectionV1 as Clone>::clone(value);
/// }
/// ```
pub struct ConfirmedIdempotencyInspectionV1 {
    prepared_command: PreparedCommandIdempotencyV1,
    normalized_input: CanonicalRecord,
    observation: AdmissionLookupResultV1,
}

impl ConfirmedIdempotencyInspectionV1 {
    /// Binds the exact plan selected from this retained observation.
    ///
    /// A historical observation may only be bound to its exact immutable plan.
    /// Absence may be bound to the requested active or explicit plan. The result
    /// is consumed by the coordinator's one-read recheck immediately before
    /// admission, resume, or replay.
    pub fn bind_selected_plan(
        self,
        selected_plan: ExecutablePlanRef,
    ) -> Result<PreparedIdempotencyRecheckV1, IdempotencyRecheckIntegrityV1> {
        match &self.observation {
            AdmissionLookupResultV1::NotFound => {}
            AdmissionLookupResultV1::Found(state) if plan_for_state(state) == &selected_plan => {}
            AdmissionLookupResultV1::Found(_) => {
                return Err(IdempotencyRecheckIntegrityV1::SelectedPlanMismatch);
            }
            AdmissionLookupResultV1::MultipleMatches => {
                return Err(IdempotencyRecheckIntegrityV1::ImpossibleTransition);
            }
        }

        Ok(PreparedIdempotencyRecheckV1::new(
            self.prepared_command,
            self.normalized_input,
            selected_plan,
            self.observation,
        ))
    }
}

impl fmt::Debug for ConfirmedIdempotencyInspectionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _retained_observation = &self.observation;
        formatter
            .debug_struct("ConfirmedIdempotencyInspectionV1")
            .field("prepared_command", &self.prepared_command)
            .field("normalized_input", &"[REDACTED]")
            .field("observation", &"[REDACTED]")
            .finish()
    }
}

/// Safe failure of one bounded, read-only durable-state inspection.
#[derive(Clone, Eq, PartialEq)]
pub enum IdempotencyInspectionError {
    /// The storage observation failed before producing a selection.
    Storage(StorageError),
    /// More than one digest-key candidate resolved to durable state.
    MultipleMatches,
    /// Storage returned state outside the exact bounded candidate set.
    InvalidObservation,
}

impl fmt::Debug for IdempotencyInspectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => formatter.debug_tuple("Storage").field(error).finish(),
            Self::MultipleMatches => formatter.write_str("MultipleMatches"),
            Self::InvalidObservation => formatter.write_str("InvalidObservation"),
        }
    }
}

impl fmt::Display for IdempotencyInspectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(formatter),
            Self::MultipleMatches => {
                formatter.write_str("multiple idempotency identities matched durable state")
            }
            Self::InvalidObservation => {
                formatter.write_str("idempotency storage observation is invalid")
            }
        }
    }
}

impl Error for IdempotencyInspectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::MultipleMatches | Self::InvalidObservation => None,
        }
    }
}

impl From<StorageError> for IdempotencyInspectionError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

/// Synchronous read-only executor for the first inspect/select observation.
pub struct IdempotencyInspectionExecutor<'repository> {
    repository: &'repository dyn AdmissionLookupRepository,
}

impl<'repository> IdempotencyInspectionExecutor<'repository> {
    /// Binds one executor to the semantic admission repository.
    #[must_use]
    pub const fn new(repository: &'repository dyn AdmissionLookupRepository) -> Self {
        Self { repository }
    }

    /// Performs exactly one bounded lookup and retains the full result opaquely.
    ///
    /// This operation never calls the repository's mutating admission method. A
    /// multiple match or a state outside the supplied candidate set fails closed
    /// and produces no plan selection.
    pub fn inspect(
        &self,
        prepared_lookup: PreparedIdempotencyLookupV1,
    ) -> Result<InspectedIdempotencyV1, IdempotencyInspectionError> {
        let candidates = prepared_lookup.lookup_candidates().clone();
        let observation = self.repository.lookup_admission(candidates)?;
        inspect_observation(prepared_lookup, observation)
    }

    /// Performs a bounded FIFO group in one repository observation.
    ///
    /// A repository failure is copied to every item because no member obtained
    /// an observation. Semantic validation remains item-local, so one corrupt
    /// result cannot be mistaken for another member's selection.
    pub fn inspect_group(
        &self,
        prepared_lookups: Vec<PreparedIdempotencyLookupV1>,
    ) -> Vec<Result<InspectedIdempotencyV1, IdempotencyInspectionError>> {
        let count = prepared_lookups.len();
        let candidates = prepared_lookups
            .iter()
            .map(|prepared| prepared.lookup_candidates().clone())
            .collect();
        match self.repository.lookup_admission_group(candidates) {
            Ok(observations) if observations.len() == count => prepared_lookups
                .into_iter()
                .zip(observations)
                .map(|(prepared, observation)| inspect_observation(prepared, observation))
                .collect(),
            Ok(_) => {
                let error = IdempotencyInspectionError::InvalidObservation;
                (0..count).map(|_| Err(error.clone())).collect()
            }
            Err(error) => {
                let error = IdempotencyInspectionError::Storage(error);
                (0..count).map(|_| Err(error.clone())).collect()
            }
        }
    }
}

fn inspect_observation(
    prepared_lookup: PreparedIdempotencyLookupV1,
    observation: AdmissionLookupResultV1,
) -> Result<InspectedIdempotencyV1, IdempotencyInspectionError> {
    let plan_selection = match &observation {
        AdmissionLookupResultV1::NotFound => IdempotencyPlanSelectionV1::Absent,
        AdmissionLookupResultV1::MultipleMatches => {
            return Err(IdempotencyInspectionError::MultipleMatches);
        }
        AdmissionLookupResultV1::Found(state) => {
            if !prepared_lookup
                .lookup_candidates()
                .contains(state.identity())
            {
                return Err(IdempotencyInspectionError::InvalidObservation);
            }
            IdempotencyPlanSelectionV1::Historical(plan_for_state(state).clone())
        }
    };

    Ok(InspectedIdempotencyV1 {
        plan_selection,
        prepared_lookup,
        observation,
    })
}

impl fmt::Debug for IdempotencyInspectionExecutor<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IdempotencyInspectionExecutor([REDACTED])")
    }
}

fn plan_for_state(state: &StoredAdmissionStateV1) -> &ExecutablePlanRef {
    match state {
        StoredAdmissionStateV1::Pending(pending) => pending.plan(),
        StoredAdmissionStateV1::StoredOutcome(outcome) => outcome.plan(),
        StoredAdmissionStateV1::ExecutionFailed(failure) => failure.pending().plan(),
    }
}
