//! Coordinator-owned bounded inspection of command idempotency state.

use std::{error::Error, fmt};

use riffdb_idempotency::{
    CommandIdempotencyScopeV1, IdempotencyDigestProvider, IdempotencyInspectionError,
    IdempotencyInspectionExecutor, IdempotencyPlanSelectionV1, IdempotencyPreparationError,
    IdempotencyRecheckIntegrityV1, InspectedIdempotencyV1, PreparedIdempotencyRecheckV1,
    prepare_idempotency_lookup,
};
#[cfg(test)]
use riffdb_storage_api::AdmissionRepository;
use riffdb_storage_api::{
    AdmissionLookupRepository, ExecutablePlanRef, StorageError, StorageErrorKind,
};
use riffdb_types::{
    ActorId, CanonicalRecord, CommandId, ContractLineage, DatabaseId, Environment, FieldId,
    IdempotencyKey, TenantScope,
};

use crate::command_execution::CommandExecutionLifecycle;

/// Move-only foundational inputs for one plan-independent idempotency lookup.
///
/// The raw caller key remains private and is retained through the matching
/// inspection so confirmation cannot silently substitute another key. This
/// value contains no plan version, normalized command input, request ID,
/// credential, storage handle, or admission mutation authority.
///
/// ```compile_fail
/// use riffdb_commit::CommandIdempotencyInspectionRequest;
///
/// fn cannot_duplicate(value: &CommandIdempotencyInspectionRequest) {
///     let _: CommandIdempotencyInspectionRequest =
///         <CommandIdempotencyInspectionRequest as Clone>::clone(value);
/// }
/// ```
#[must_use = "an idempotency inspection request must be submitted or explicitly discarded"]
pub struct CommandIdempotencyInspectionRequest {
    database_id: DatabaseId,
    environment: Environment,
    tenant_scope: TenantScope,
    principal_id: ActorId,
    contract_lineage: ContractLineage,
    command_id: CommandId,
    caller_key: IdempotencyKey,
}

impl CommandIdempotencyInspectionRequest {
    /// Constructs one exact version-independent command identity request.
    pub const fn new(
        database_id: DatabaseId,
        environment: Environment,
        tenant_scope: TenantScope,
        principal_id: ActorId,
        contract_lineage: ContractLineage,
        command_id: CommandId,
        caller_key: IdempotencyKey,
    ) -> Self {
        Self {
            database_id,
            environment,
            tenant_scope,
            principal_id,
            contract_lineage,
            command_id,
            caller_key,
        }
    }

    fn into_parts(self) -> (CommandIdempotencyScopeV1, IdempotencyKey) {
        (
            CommandIdempotencyScopeV1::new(
                self.database_id,
                self.environment,
                self.tenant_scope,
                self.principal_id,
                self.contract_lineage,
                self.command_id,
            ),
            self.caller_key,
        )
    }
}

impl fmt::Debug for CommandIdempotencyInspectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandIdempotencyInspectionRequest([REDACTED])")
    }
}

/// Move-only commit-internal preparation for the actor's sole storage lookup.
///
/// Digest computation occurs before bounded queue reservation. The actor still
/// owns the repository observation, while the original caller key remains
/// coupled to that exact observation for later canonical-input confirmation.
#[must_use = "a prepared idempotency inspection must be submitted or explicitly discarded"]
pub(super) struct PreparedCommandIdempotencyInspection {
    prepared_lookup: riffdb_idempotency::PreparedIdempotencyLookupV1,
    caller_key: IdempotencyKey,
}

impl fmt::Debug for PreparedCommandIdempotencyInspection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedCommandIdempotencyInspection([REDACTED])")
    }
}

/// Commit-owned plan selection exposed by one retained inspection.
#[derive(Clone, Eq, PartialEq)]
pub enum CommandIdempotencyPlanSelection {
    /// No readable digest identity currently has durable command state.
    Absent,
    /// One readable identity selected this exact immutable historical plan.
    Historical(ExecutablePlanRef),
}

impl fmt::Debug for CommandIdempotencyPlanSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Absent => "CommandIdempotencyPlanSelection::Absent",
            Self::Historical(_) => "CommandIdempotencyPlanSelection::Historical([REDACTED])",
        })
    }
}

/// Move-only retained observation used for plan selection and input confirmation.
///
/// Only the absent-or-historical selection is inspectable. The original caller
/// key, digest candidates, and complete durable observation remain private and
/// are consumed together by [`Self::confirm_selected_plan`].
///
/// ```compile_fail
/// use riffdb_commit::InspectedCommandIdempotency;
///
/// fn cannot_duplicate(value: &InspectedCommandIdempotency) {
///     let _: InspectedCommandIdempotency =
///         <InspectedCommandIdempotency as Clone>::clone(value);
/// }
/// ```
#[must_use = "an inspected idempotency observation must be confirmed or explicitly discarded"]
pub struct InspectedCommandIdempotency {
    plan_selection: CommandIdempotencyPlanSelection,
    inspected: InspectedIdempotencyV1,
    caller_key: IdempotencyKey,
}

impl InspectedCommandIdempotency {
    /// Borrows the only durable-state information available for plan selection.
    #[must_use]
    pub const fn plan_selection(&self) -> &CommandIdempotencyPlanSelection {
        &self.plan_selection
    }

    /// Consumes this observation and binds canonical input to the selected plan.
    ///
    /// Confirmation performs no digest call, storage access, or mutation. The
    /// returned lower proof is intended for direct, inference-only handoff to
    /// [`crate::CommandExecutionPreparation::new`]; callers do not need to name
    /// its lower-crate type.
    pub fn confirm_selected_plan(
        self,
        normalized_input: &CanonicalRecord,
        idempotency_field: FieldId,
        selected_plan: ExecutablePlanRef,
    ) -> Result<PreparedIdempotencyRecheckV1, CommandIdempotencyConfirmationError> {
        let confirmed = self
            .inspected
            .confirm_input(normalized_input, idempotency_field, &self.caller_key)
            .map_err(CommandIdempotencyConfirmationError::from_preparation)?;
        confirmed
            .bind_selected_plan(selected_plan)
            .map_err(CommandIdempotencyConfirmationError::from_plan_binding)
    }
}

impl fmt::Debug for InspectedCommandIdempotency {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InspectedCommandIdempotency([REDACTED])")
    }
}

/// Closed safe failure while confirming input and one selected plan.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CommandIdempotencyConfirmationError {
    /// The selected normalized record omitted the declared caller-key field.
    MissingIdempotencyField,
    /// The declared caller-key field was not a canonical string.
    IdempotencyFieldNotString,
    /// The normalized field did not equal the originally inspected caller key.
    IdempotencyKeyMismatch,
    /// The normalized record could not be canonically hashed within v1 bounds.
    InvalidCanonicalInput,
    /// The selected plan did not equal the retained historical plan selection.
    SelectedPlanMismatch,
    /// A lower transition impossible after successful inspection was observed.
    InternalDefect,
}

impl CommandIdempotencyConfirmationError {
    const fn from_preparation(error: IdempotencyPreparationError) -> Self {
        match error {
            IdempotencyPreparationError::MissingIdempotencyField => Self::MissingIdempotencyField,
            IdempotencyPreparationError::IdempotencyFieldNotString => {
                Self::IdempotencyFieldNotString
            }
            IdempotencyPreparationError::IdempotencyKeyMismatch => Self::IdempotencyKeyMismatch,
            IdempotencyPreparationError::InvalidCanonicalInput => Self::InvalidCanonicalInput,
            IdempotencyPreparationError::DigestProvider(_)
            | IdempotencyPreparationError::InvalidLookupCandidates => Self::InternalDefect,
        }
    }

    const fn from_plan_binding(error: IdempotencyRecheckIntegrityV1) -> Self {
        match error {
            IdempotencyRecheckIntegrityV1::SelectedPlanMismatch => Self::SelectedPlanMismatch,
            IdempotencyRecheckIntegrityV1::PreviouslyPresentMissing
            | IdempotencyRecheckIntegrityV1::MultipleMatches
            | IdempotencyRecheckIntegrityV1::IdentityOutsideCandidates
            | IdempotencyRecheckIntegrityV1::ImpossibleTransition => Self::InternalDefect,
        }
    }
}

impl fmt::Display for CommandIdempotencyConfirmationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingIdempotencyField => "declared idempotency field is missing",
            Self::IdempotencyFieldNotString => "declared idempotency field must be a string",
            Self::IdempotencyKeyMismatch => {
                "declared idempotency key does not match the inspected key"
            }
            Self::InvalidCanonicalInput => "canonical command input is invalid",
            Self::SelectedPlanMismatch => {
                "selected plan does not match inspected idempotency state"
            }
            Self::InternalDefect => "idempotency confirmation encountered an internal defect",
        })
    }
}

impl Error for CommandIdempotencyConfirmationError {}

/// Closed safe failure kind for one pre-admission idempotency inspection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CommandIdempotencyInspectionErrorKind {
    /// A retryable digest-provider or storage dependency was unavailable.
    StorageUnavailable,
    /// Provider candidates or durable state were structurally contradictory.
    InternalDefect,
    /// The sole coordinator actor stopped before completing the inspection.
    CoordinatorStopped,
    /// An uncertain authoritative write fenced every later observation.
    CoordinatorFenced,
}

impl CommandIdempotencyInspectionErrorKind {
    const fn safe_message(self) -> &'static str {
        match self {
            Self::StorageUnavailable => "idempotency storage is unavailable",
            Self::InternalDefect => "idempotency inspection encountered an internal defect",
            Self::CoordinatorStopped => "command coordinator stopped",
            Self::CoordinatorFenced => "command coordinator fenced authoritative work",
        }
    }
}

/// Public-safe inspection failure retaining its trusted internal cause.
pub struct CommandIdempotencyInspectionError {
    kind: CommandIdempotencyInspectionErrorKind,
    #[allow(dead_code)] // Retained for trusted telemetry; never exposed as an error source.
    detail: CommandIdempotencyInspectionErrorDetail,
}

#[allow(dead_code)] // Variant payloads are retained for the trusted telemetry boundary.
enum CommandIdempotencyInspectionErrorDetail {
    None,
    Digest(IdempotencyPreparationError),
    Storage(StorageError),
    InvalidObservation,
}

impl CommandIdempotencyInspectionError {
    /// Returns the complete safe classification.
    #[must_use]
    pub const fn kind(&self) -> CommandIdempotencyInspectionErrorKind {
        self.kind
    }

    pub(super) const fn coordinator_stopped() -> Self {
        Self::without_detail(CommandIdempotencyInspectionErrorKind::CoordinatorStopped)
    }

    pub(super) const fn coordinator_fenced() -> Self {
        Self::without_detail(CommandIdempotencyInspectionErrorKind::CoordinatorFenced)
    }

    const fn without_detail(kind: CommandIdempotencyInspectionErrorKind) -> Self {
        Self {
            kind,
            detail: CommandIdempotencyInspectionErrorDetail::None,
        }
    }

    const fn digest(error: IdempotencyPreparationError) -> Self {
        let kind = match error {
            IdempotencyPreparationError::DigestProvider(
                riffdb_idempotency::IdempotencyDigestError::Unavailable,
            ) => CommandIdempotencyInspectionErrorKind::StorageUnavailable,
            IdempotencyPreparationError::DigestProvider(_)
            | IdempotencyPreparationError::InvalidLookupCandidates
            | IdempotencyPreparationError::MissingIdempotencyField
            | IdempotencyPreparationError::IdempotencyFieldNotString
            | IdempotencyPreparationError::IdempotencyKeyMismatch
            | IdempotencyPreparationError::InvalidCanonicalInput => {
                CommandIdempotencyInspectionErrorKind::InternalDefect
            }
        };
        Self {
            kind,
            detail: CommandIdempotencyInspectionErrorDetail::Digest(error),
        }
    }

    fn storage(error: StorageError) -> Self {
        let kind = if error.kind() == StorageErrorKind::Unavailable {
            CommandIdempotencyInspectionErrorKind::StorageUnavailable
        } else {
            CommandIdempotencyInspectionErrorKind::InternalDefect
        };
        Self {
            kind,
            detail: CommandIdempotencyInspectionErrorDetail::Storage(error),
        }
    }

    const fn invalid_observation() -> Self {
        Self {
            kind: CommandIdempotencyInspectionErrorKind::InternalDefect,
            detail: CommandIdempotencyInspectionErrorDetail::InvalidObservation,
        }
    }
}

impl fmt::Debug for CommandIdempotencyInspectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandIdempotencyInspectionError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for CommandIdempotencyInspectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for CommandIdempotencyInspectionError {}

/// Computes bounded digest candidates before reserving actor queue capacity.
pub(super) fn prepare_command_idempotency_inspection(
    digest_provider: &dyn IdempotencyDigestProvider,
    lifecycle: &dyn CommandExecutionLifecycle,
    request: CommandIdempotencyInspectionRequest,
) -> Result<PreparedCommandIdempotencyInspection, CommandIdempotencyInspectionError> {
    let (scope, caller_key) = request.into_parts();
    let prepared_lookup = match prepare_idempotency_lookup(&scope, &caller_key, digest_provider) {
        Ok(prepared) => prepared,
        Err(error) => {
            let failure = CommandIdempotencyInspectionError::digest(error);
            if failure.kind() == CommandIdempotencyInspectionErrorKind::InternalDefect {
                lifecycle.stop();
            }
            return Err(failure);
        }
    };

    Ok(PreparedCommandIdempotencyInspection {
        prepared_lookup,
        caller_key,
    })
}

/// Performs the actor-owned sole repository observation without admission.
pub(super) fn inspect_command_idempotency(
    repository: &dyn AdmissionLookupRepository,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparation: PreparedCommandIdempotencyInspection,
) -> Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError> {
    let inspected =
        IdempotencyInspectionExecutor::new(repository).inspect(preparation.prepared_lookup);
    finish_command_idempotency_inspection(lifecycle, preparation.caller_key, inspected)
}

/// Performs one bounded FIFO lookup group and preserves item-local results.
pub(super) fn inspect_command_idempotency_group(
    repository: &dyn AdmissionLookupRepository,
    lifecycle: &dyn CommandExecutionLifecycle,
    preparations: Vec<PreparedCommandIdempotencyInspection>,
) -> Vec<Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError>> {
    let mut caller_keys = Vec::with_capacity(preparations.len());
    let prepared = preparations
        .into_iter()
        .map(|preparation| {
            caller_keys.push(preparation.caller_key);
            preparation.prepared_lookup
        })
        .collect();
    IdempotencyInspectionExecutor::new(repository)
        .inspect_group(prepared)
        .into_iter()
        .zip(caller_keys)
        .map(|(inspected, caller_key)| {
            finish_command_idempotency_inspection(lifecycle, caller_key, inspected)
        })
        .collect()
}

fn finish_command_idempotency_inspection(
    lifecycle: &dyn CommandExecutionLifecycle,
    caller_key: IdempotencyKey,
    inspected: Result<InspectedIdempotencyV1, IdempotencyInspectionError>,
) -> Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError> {
    match inspected {
        Ok(inspected) => {
            let plan_selection = match inspected.plan_selection() {
                IdempotencyPlanSelectionV1::Absent => CommandIdempotencyPlanSelection::Absent,
                IdempotencyPlanSelectionV1::Historical(plan) => {
                    CommandIdempotencyPlanSelection::Historical(plan.clone())
                }
            };
            Ok(InspectedCommandIdempotency {
                plan_selection,
                inspected,
                caller_key,
            })
        }
        Err(IdempotencyInspectionError::Storage(error)) => {
            let failure = CommandIdempotencyInspectionError::storage(error);
            if failure.kind() == CommandIdempotencyInspectionErrorKind::InternalDefect {
                lifecycle.stop();
            }
            Err(failure)
        }
        Err(
            IdempotencyInspectionError::MultipleMatches
            | IdempotencyInspectionError::InvalidObservation,
        ) => {
            lifecycle.stop();
            Err(CommandIdempotencyInspectionError::invalid_observation())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use riffdb_idempotency::{IdempotencyDigestCandidatesV1, IdempotencyDigestError};
    use riffdb_storage_api::{
        AdmissionLookupResultV1, AdmissionRequestV1, AdmissionResultV1, IdempotencyKeyDigest,
        StoredAdmissionStateV1, StoredAdmittedProvenanceClaimsV1, StoredPendingAdmissionV1,
    };
    use riffdb_types::{
        ActorKind, AdmittedActorContext, CanonicalValue, ContractBundleHash, ContractVersion,
        DigestKeyId, LogicalTime, PartitionKeyBuilder, PlanHash, RequestId, Timestamp,
    };

    use super::*;

    const CALLER_KEY: &str = "inspection-caller-secret-canary";
    const PRINCIPAL: &str = "inspection-principal-secret-canary";

    struct ScriptedDigestProvider {
        result: Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError>,
        calls: AtomicUsize,
    }

    impl ScriptedDigestProvider {
        fn fixed() -> Self {
            Self {
                result: IdempotencyDigestCandidatesV1::new(vec![
                    IdempotencyKeyDigest::from_hmac_bytes(
                        DigestKeyId::new(1).expect("digest key ID"),
                        [0x31; 32],
                    ),
                ]),
                calls: AtomicUsize::new(0),
            }
        }

        fn failing(error: IdempotencyDigestError) -> Self {
            Self {
                result: Err(error),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl IdempotencyDigestProvider for ScriptedDigestProvider {
        fn digest_candidates(
            &self,
            caller_key: &IdempotencyKey,
        ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert_eq!(caller_key.expose_secret(), CALLER_KEY);
            self.result.clone()
        }
    }

    enum Observation {
        Absent,
        Historical(ExecutablePlanRef),
        Multiple,
        Error(StorageErrorKind),
    }

    struct ObservationRepository {
        observation: Observation,
        reads: Cell<usize>,
    }

    impl AdmissionRepository for ObservationRepository {
        fn admit_or_resolve(
            &self,
            _: AdmissionRequestV1,
        ) -> Result<AdmissionResultV1, StorageError> {
            panic!("inspection must not mutate admission state")
        }

        fn lookup_admission(
            &self,
            candidates: riffdb_storage_api::IdempotencyLookupCandidatesV1,
        ) -> Result<AdmissionLookupResultV1, StorageError> {
            self.reads.set(self.reads.get() + 1);
            match &self.observation {
                Observation::Absent => Ok(AdmissionLookupResultV1::NotFound),
                Observation::Historical(plan) => Ok(AdmissionLookupResultV1::Found(Box::new(
                    StoredAdmissionStateV1::Pending(pending(
                        candidates.as_slice()[0].clone(),
                        plan.clone(),
                    )),
                ))),
                Observation::Multiple => Ok(AdmissionLookupResultV1::MultipleMatches),
                Observation::Error(kind) => Err(StorageError::new(*kind, None)),
            }
        }
    }

    #[derive(Default)]
    struct Lifecycle {
        stopped: Cell<bool>,
    }

    impl CommandExecutionLifecycle for Lifecycle {
        fn fence(&self) {
            panic!("a read-only observation cannot require an uncertainty fence")
        }

        fn stop(&self) {
            self.stopped.set(true);
        }
    }

    fn request() -> CommandIdempotencyInspectionRequest {
        CommandIdempotencyInspectionRequest::new(
            database(),
            environment(),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("principal"),
            lineage(),
            CommandId::new(1).expect("command ID"),
            IdempotencyKey::new(CALLER_KEY).expect("caller key"),
        )
    }

    fn database() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("database UUIDv7")
    }

    fn environment() -> Environment {
        Environment::new("development").expect("environment")
    }

    fn lineage() -> ContractLineage {
        ContractLineage::new("budget").expect("lineage")
    }

    fn plan(seed: u8) -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            lineage(),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([seed; 32]),
            CommandId::new(1).expect("command ID"),
            PlanHash::from_bytes([seed.wrapping_add(1); 32]),
        )
    }

    fn pending(
        identity: riffdb_storage_api::IdempotencyIdentity,
        selected_plan: ExecutablePlanRef,
    ) -> StoredPendingAdmissionV1 {
        let mut partition =
            PartitionKeyBuilder::new(riffdb_types::AggregateTypeId::new(1).expect("aggregate ID"));
        partition.push_u64(7).expect("partition component");
        StoredPendingAdmissionV1::new(
            identity,
            riffdb_types::CanonicalInputHash::from_bytes([0x41; 32]),
            RequestId::from_unix_milliseconds_and_random(2, [2; 10]).expect("request ID"),
            selected_plan,
            LogicalTime::new(Timestamp::new(10, 3).expect("logical time")),
            AdmittedActorContext::new(
                ActorId::new(PRINCIPAL).expect("principal"),
                ActorKind::Agent,
                TenantScope::Global,
                None,
            ),
            partition.finish().expect("partition"),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending admission")
    }

    fn normalized_input(field: FieldId, value: CanonicalValue) -> CanonicalRecord {
        CanonicalRecord::new(vec![(field, value)]).expect("canonical input")
    }

    fn prepare_and_inspect(
        repository: &dyn AdmissionLookupRepository,
        provider: &dyn IdempotencyDigestProvider,
        lifecycle: &dyn CommandExecutionLifecycle,
        request: CommandIdempotencyInspectionRequest,
    ) -> Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError> {
        let preparation = prepare_command_idempotency_inspection(provider, lifecycle, request)?;
        inspect_command_idempotency(repository, lifecycle, preparation)
    }

    #[test]
    fn absence_uses_one_digest_call_and_one_nonmutating_lookup() {
        let provider = ScriptedDigestProvider::fixed();
        let repository = ObservationRepository {
            observation: Observation::Absent,
            reads: Cell::new(0),
        };
        let lifecycle = Lifecycle::default();

        let inspected = prepare_and_inspect(&repository, &provider, &lifecycle, request())
            .expect("inspection succeeds");

        assert_eq!(
            inspected.plan_selection(),
            &CommandIdempotencyPlanSelection::Absent
        );
        assert_eq!(provider.calls(), 1);
        assert_eq!(repository.reads.get(), 1);
        assert!(!lifecycle.stopped.get());
        assert_eq!(
            format!("{inspected:?}"),
            "InspectedCommandIdempotency([REDACTED])"
        );
        assert!(!format!("{inspected:?}").contains(CALLER_KEY));
    }

    #[test]
    fn historical_selection_confirms_with_the_original_caller_key() {
        let selected_plan = plan(0x51);
        let provider = ScriptedDigestProvider::fixed();
        let repository = ObservationRepository {
            observation: Observation::Historical(selected_plan.clone()),
            reads: Cell::new(0),
        };
        let lifecycle = Lifecycle::default();
        let inspected = prepare_and_inspect(&repository, &provider, &lifecycle, request())
            .expect("inspection succeeds");
        assert_eq!(
            inspected.plan_selection(),
            &CommandIdempotencyPlanSelection::Historical(selected_plan.clone())
        );

        let idempotency_field = FieldId::new(1).expect("field ID");
        let normalized = normalized_input(
            idempotency_field,
            CanonicalValue::string(CALLER_KEY).expect("caller key value"),
        );
        let confirmed = inspected
            .confirm_selected_plan(&normalized, idempotency_field, selected_plan.clone())
            .expect("exact input and historical plan confirm");

        assert!(confirmed.matches_preparation(&selected_plan, &normalized, idempotency_field));
        assert!(confirmed.matches_scope(
            database(),
            &environment(),
            &TenantScope::Global,
            &ActorId::new(PRINCIPAL).expect("principal"),
            &lineage(),
            CommandId::new(1).expect("command ID"),
        ));
        assert_eq!(provider.calls(), 1);
        assert_eq!(repository.reads.get(), 1);
        assert!(!format!("{confirmed:?}").contains(CALLER_KEY));
    }

    #[test]
    fn confirmation_reports_key_and_selected_plan_mismatches_without_reinspection() {
        let idempotency_field = FieldId::new(1).expect("field ID");
        let selected_plan = plan(0x61);
        for (normalized, bound_plan, expected) in [
            (
                normalized_input(
                    idempotency_field,
                    CanonicalValue::string("different-key").expect("different key"),
                ),
                selected_plan.clone(),
                CommandIdempotencyConfirmationError::IdempotencyKeyMismatch,
            ),
            (
                normalized_input(idempotency_field, CanonicalValue::I64(7)),
                selected_plan.clone(),
                CommandIdempotencyConfirmationError::IdempotencyFieldNotString,
            ),
            (
                normalized_input(
                    idempotency_field,
                    CanonicalValue::string(CALLER_KEY).expect("caller key"),
                ),
                plan(0x62),
                CommandIdempotencyConfirmationError::SelectedPlanMismatch,
            ),
        ] {
            let provider = ScriptedDigestProvider::fixed();
            let repository = ObservationRepository {
                observation: Observation::Historical(selected_plan.clone()),
                reads: Cell::new(0),
            };
            let lifecycle = Lifecycle::default();
            let inspected = prepare_and_inspect(&repository, &provider, &lifecycle, request())
                .expect("inspection succeeds");

            assert_eq!(
                inspected
                    .confirm_selected_plan(&normalized, idempotency_field, bound_plan)
                    .expect_err("confirmation mismatch must reject"),
                expected
            );
            assert_eq!(provider.calls(), 1);
            assert_eq!(repository.reads.get(), 1);
        }
    }

    #[test]
    fn unavailable_digest_or_storage_is_retryable_without_stopping_readiness() {
        let cases = [
            (
                ScriptedDigestProvider::failing(IdempotencyDigestError::Unavailable),
                Observation::Absent,
                0,
            ),
            (
                ScriptedDigestProvider::fixed(),
                Observation::Error(StorageErrorKind::Unavailable),
                1,
            ),
        ];
        for (provider, observation, expected_reads) in cases {
            let repository = ObservationRepository {
                observation,
                reads: Cell::new(0),
            };
            let lifecycle = Lifecycle::default();

            let error = prepare_and_inspect(&repository, &provider, &lifecycle, request())
                .expect_err("unavailable dependency must fail");
            assert_eq!(
                error.kind(),
                CommandIdempotencyInspectionErrorKind::StorageUnavailable
            );
            assert_eq!(provider.calls(), 1);
            assert_eq!(repository.reads.get(), expected_reads);
            assert!(!lifecycle.stopped.get());
            assert!(error.source().is_none());
            assert!(!format!("{error:?}").contains(CALLER_KEY));
        }
    }

    #[test]
    fn invalid_digest_candidate_failures_stop_before_storage_lookup() {
        for provider_error in [
            IdempotencyDigestError::Empty,
            IdempotencyDigestError::TooMany,
            IdempotencyDigestError::DuplicateKeyId,
            IdempotencyDigestError::DuplicateDigest,
        ] {
            let provider = ScriptedDigestProvider::failing(provider_error);
            let repository = ObservationRepository {
                observation: Observation::Absent,
                reads: Cell::new(0),
            };
            let lifecycle = Lifecycle::default();

            let error = prepare_and_inspect(&repository, &provider, &lifecycle, request())
                .expect_err("invalid provider state must fail closed");
            assert_eq!(
                error.kind(),
                CommandIdempotencyInspectionErrorKind::InternalDefect
            );
            assert_eq!(provider.calls(), 1);
            assert_eq!(repository.reads.get(), 0);
            assert!(lifecycle.stopped.get());
        }
    }

    #[test]
    fn contradictory_or_integrity_storage_results_stop_readiness() {
        for observation in [
            Observation::Multiple,
            Observation::Error(StorageErrorKind::CorruptData),
            Observation::Error(StorageErrorKind::CommitStatusUnknown),
        ] {
            let provider = ScriptedDigestProvider::fixed();
            let repository = ObservationRepository {
                observation,
                reads: Cell::new(0),
            };
            let lifecycle = Lifecycle::default();

            let failure = prepare_and_inspect(&repository, &provider, &lifecycle, request())
                .expect_err("invalid observation fails closed");

            assert_eq!(
                failure.kind(),
                CommandIdempotencyInspectionErrorKind::InternalDefect
            );
            assert_eq!(provider.calls(), 1);
            assert_eq!(repository.reads.get(), 1);
            assert!(lifecycle.stopped.get());
        }
    }

    #[test]
    fn public_debug_surfaces_do_not_expose_scope_or_caller_material() {
        let request = request();
        assert_eq!(
            format!("{request:?}"),
            "CommandIdempotencyInspectionRequest([REDACTED])"
        );
        assert!(!format!("{request:?}").contains(PRINCIPAL));
        assert!(!format!("{request:?}").contains(CALLER_KEY));
        assert_eq!(
            format!(
                "{:?}",
                CommandIdempotencyPlanSelection::Historical(plan(0x71))
            ),
            "CommandIdempotencyPlanSelection::Historical([REDACTED])"
        );
    }
}
