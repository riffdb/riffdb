//! Sole-writer coordinator actor and synchronous service-audit lowering.

use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use std::time::Instant;
use std::{error::Error, fmt, panic, thread};

use riffdb_storage_api::{
    AdmissionLookupRepository, AdmissionRepository, ApplicationCommandTransactionPort,
    AuditPrincipalV1, AuditedAdmissionRepository, CapabilityAdministrationTransactionPort,
    CapabilityBootstrapAdministrationRepository, CatalogAdministrationRepository,
    ExecutionFailureTransitionPort, QueryModuleAdministrationRepository,
    ReactiveModuleAdministrationRepository, ServiceAuditAppendIntentV1,
    ServiceAuditAppendRepository, ServiceAuditAppendResult, SnapshotReader, StorageError,
    StorageValueError,
};
use tokio::runtime;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use riffdb_conflict::ConflictManager;
use riffdb_idempotency::IdempotencyDigestProvider;
use riffdb_policy::AuthorizationClock;

use crate::{
    AdministrationAuditInputView, AdministrationClock, AdministrationClockError, AdmissionClock,
    ApplicationCommitNotificationSink, CommandExecutionPreparation, CommandPipelineStage,
    CommitCommandTerminal, CommitGroupDispatchReason, CommitTelemetry, CommitTelemetryEvent,
    CommittedOutcomeDisposition, CompletionLanePhase, NoopCommitTelemetry, ProvenanceIdSource,
    command_execution::{
        CommandEvaluationPool, CommandExecutionError, CommandExecutionLifecycle,
        CommandExecutionResult, CommandGroupDriveResult, CoordinatorDurability,
        RepeatableCommandBatchPort, drive_command_execution,
    },
    control_plane::{
        CapabilityBootstrapExecutionResult, CapabilityBootstrapPreparation,
        CapabilityBootstrapTerminalPreparation, CapabilityCreateExecutionResult,
        CapabilityCreatePreparation, CapabilityRevokeExecutionResult, CapabilityRevokePreparation,
        CatalogDeploymentPreparation, CatalogDeploymentResult, ControlPlaneExecutionError,
        QueryModuleDeploymentPreparation, QueryModuleDeploymentResult,
        ReactiveModulePublicationExecutionResult, ReactiveModulePublicationPreparation,
        drive_capability_bootstrap, drive_capability_bootstrap_terminal, drive_capability_create,
        drive_capability_revoke, drive_catalog_deployment, drive_query_module_deployment,
        drive_reactive_module_publication,
    },
    idempotency_inspection::{
        CommandIdempotencyInspectionError, CommandIdempotencyInspectionRequest,
        InspectedCommandIdempotency, PreparedCommandIdempotencyInspection,
        inspect_command_idempotency, inspect_command_idempotency_group,
        prepare_command_idempotency_inspection,
    },
    read_only_execution::{ReadOnlyExecutionResult, drive_read_only_execution},
    read_only_preparation::ReadOnlyExecutionPreparation,
};

/// Closed safe failure from one service-audit append attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrationAuditExecutionError {
    /// The coordinator could not obtain the one required timestamp.
    Clock(AdministrationClockError),
    /// The supposedly checked input could not form a storage-owned intent.
    InvalidInput(StorageValueError),
    /// Durable lifecycle state rejected this phase without allocating a sequence.
    PhaseConflict,
    /// The specialized atomic storage transition failed.
    Storage(StorageError),
    /// The coordinator stopped before it could report an accepted attempt.
    CoordinatorStopped,
    /// An earlier unknown authoritative write fenced this queued attempt.
    CoordinatorFenced,
}

impl fmt::Display for AdministrationAuditExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clock(error) => error.fmt(formatter),
            Self::InvalidInput(error) => error.fmt(formatter),
            Self::PhaseConflict => {
                formatter.write_str("service audit phase conflicts with durable state")
            }
            Self::Storage(error) => error.fmt(formatter),
            Self::CoordinatorStopped => formatter.write_str("command coordinator stopped"),
            Self::CoordinatorFenced => {
                formatter.write_str("command coordinator fenced authoritative writes")
            }
        }
    }
}

impl Error for AdministrationAuditExecutionError {}

/// Samples time once, copies the checked view, and attempts one atomic append.
///
/// This function runs only on the coordinator's sole-writer path. It never
/// returns the newly assigned audit-record sequence and never retries an
/// uncertain or failed append.
pub(crate) fn append_administration_audit(
    repository: &mut dyn ServiceAuditAppendRepository,
    clock: &dyn AdministrationClock,
    input: &dyn AdministrationAuditInputView,
) -> Result<(), AdministrationAuditExecutionError> {
    let intent = prepare_administration_audit(clock, input)?;
    append_prepared_administration_audit(repository, &intent)
}

pub(crate) fn prepare_administration_audit(
    clock: &dyn AdministrationClock,
    input: &dyn AdministrationAuditInputView,
) -> Result<ServiceAuditAppendIntentV1, AdministrationAuditExecutionError> {
    let timestamp = clock
        .now()
        .map_err(AdministrationAuditExecutionError::Clock)?;
    let principal = AuditPrincipalV1::new(
        input.principal_id().clone(),
        *input.actor_kind(),
        *input.capability_id(),
        *input.capability_revision(),
    );
    ServiceAuditAppendIntentV1::new(
        *input.request_id(),
        timestamp,
        *input.operation(),
        *input.phase(),
        principal,
        *input.ingress(),
        input.targets().clone(),
        input.approval_id().cloned(),
        *input.link(),
    )
    .map_err(AdministrationAuditExecutionError::InvalidInput)
}

pub(crate) fn prepare_command_terminal_audit(
    clock: &dyn AdministrationClock,
    started: &dyn AdministrationAuditInputView,
    link: riffdb_types::ServiceAuditLinkV1,
) -> Result<ServiceAuditAppendIntentV1, AdministrationAuditExecutionError> {
    if started.operation() != &riffdb_types::ServiceOperationV1::ExecuteCommand
        || started.phase() != &riffdb_types::ServiceAuditPhaseV1::Started
        || started.link() != &riffdb_types::ServiceAuditLinkV1::None
        || !matches!(link, riffdb_types::ServiceAuditLinkV1::Command { .. })
    {
        return Err(AdministrationAuditExecutionError::InvalidInput(
            riffdb_storage_api::StorageValueError::IdentityMismatch,
        ));
    }
    let timestamp = clock
        .now()
        .map_err(AdministrationAuditExecutionError::Clock)?;
    let principal = AuditPrincipalV1::new(
        started.principal_id().clone(),
        *started.actor_kind(),
        *started.capability_id(),
        *started.capability_revision(),
    );
    ServiceAuditAppendIntentV1::new(
        *started.request_id(),
        timestamp,
        *started.operation(),
        riffdb_types::ServiceAuditPhaseV1::Succeeded,
        principal,
        *started.ingress(),
        started.targets().clone(),
        started.approval_id().cloned(),
        link,
    )
    .map_err(AdministrationAuditExecutionError::InvalidInput)
}

pub(crate) fn prepare_command_failure_terminal_audit(
    clock: &dyn AdministrationClock,
    started: &dyn AdministrationAuditInputView,
) -> Result<ServiceAuditAppendIntentV1, AdministrationAuditExecutionError> {
    if started.operation() != &riffdb_types::ServiceOperationV1::ExecuteCommand
        || started.phase() != &riffdb_types::ServiceAuditPhaseV1::Started
        || started.link() != &riffdb_types::ServiceAuditLinkV1::None
    {
        return Err(AdministrationAuditExecutionError::InvalidInput(
            riffdb_storage_api::StorageValueError::IdentityMismatch,
        ));
    }
    let timestamp = clock
        .now()
        .map_err(AdministrationAuditExecutionError::Clock)?;
    let principal = AuditPrincipalV1::new(
        started.principal_id().clone(),
        *started.actor_kind(),
        *started.capability_id(),
        *started.capability_revision(),
    );
    ServiceAuditAppendIntentV1::new(
        *started.request_id(),
        timestamp,
        *started.operation(),
        riffdb_types::ServiceAuditPhaseV1::Failed,
        principal,
        *started.ingress(),
        started.targets().clone(),
        started.approval_id().cloned(),
        riffdb_types::ServiceAuditLinkV1::None,
    )
    .map_err(AdministrationAuditExecutionError::InvalidInput)
}

fn append_prepared_administration_audit(
    repository: &mut dyn ServiceAuditAppendRepository,
    intent: &ServiceAuditAppendIntentV1,
) -> Result<(), AdministrationAuditExecutionError> {
    match repository
        .append_service_audit(intent)
        .map_err(AdministrationAuditExecutionError::Storage)?
    {
        ServiceAuditAppendResult::Appended(_) => Ok(()),
        ServiceAuditAppendResult::PhaseConflict => {
            Err(AdministrationAuditExecutionError::PhaseConflict)
        }
    }
}

fn append_administration_audit_fused_pair(
    repository: &mut dyn ServiceAuditAppendRepository,
    clock: &dyn AdministrationClock,
    started: &dyn AdministrationAuditInputView,
    terminal: &dyn AdministrationAuditInputView,
) -> Result<(), AdministrationAuditExecutionError> {
    let started_intent = prepare_administration_audit(clock, started)?;
    let terminal_intent = prepare_administration_audit(clock, terminal)?;
    repository
        .append_service_audit_fused_pair(&started_intent, &terminal_intent)
        .map_err(AdministrationAuditExecutionError::Storage)
}

fn append_administration_audit_group(
    repository: &mut dyn ServiceAuditAppendRepository,
    clock: &dyn AdministrationClock,
    inputs: &[Box<dyn AdministrationAuditInputView>],
) -> Vec<Result<(), AdministrationAuditExecutionError>> {
    if inputs.len() <= 1 {
        return inputs
            .iter()
            .map(|input| append_administration_audit(repository, clock, input.as_ref()))
            .collect();
    }
    let mut outputs = (0..inputs.len()).map(|_| None).collect::<Vec<_>>();
    let mut prepared = Vec::with_capacity(inputs.len());
    for (index, input) in inputs.iter().enumerate() {
        match prepare_administration_audit(clock, input.as_ref()) {
            Ok(intent) => prepared.push((index, intent)),
            Err(error) => outputs[index] = Some(Err(error)),
        }
    }
    if prepared.is_empty() {
        return outputs
            .into_iter()
            .map(|output| {
                output.unwrap_or(Err(AdministrationAuditExecutionError::CoordinatorStopped))
            })
            .collect();
    }
    let intents = prepared
        .iter()
        .map(|(_, intent)| intent.clone())
        .collect::<Vec<_>>();
    match repository.append_service_audit_group(&intents) {
        Ok(results) if results.len() == prepared.len() => {
            for ((index, _), result) in prepared.into_iter().zip(results) {
                outputs[index] = Some(match result {
                    ServiceAuditAppendResult::Appended(_) => Ok(()),
                    ServiceAuditAppendResult::PhaseConflict => {
                        Err(AdministrationAuditExecutionError::PhaseConflict)
                    }
                });
            }
        }
        Ok(_) => {
            let error = StorageError::new(
                riffdb_storage_api::StorageErrorKind::InvariantViolation,
                None,
            );
            for (index, _) in prepared {
                outputs[index] = Some(Err(AdministrationAuditExecutionError::Storage(
                    error.clone(),
                )));
            }
        }
        Err(error) => {
            for (index, _) in prepared {
                outputs[index] = Some(Err(AdministrationAuditExecutionError::Storage(
                    error.clone(),
                )));
            }
        }
    }
    outputs
        .into_iter()
        .map(|output| output.unwrap_or(Err(AdministrationAuditExecutionError::CoordinatorStopped)))
        .collect()
}

enum AuditGroupDriveResult {
    Complete(Vec<Result<(), AdministrationAuditExecutionError>>),
    Submitted(SubmittedAuditGroup),
}

struct SubmittedAuditGroup {
    outputs: Vec<Option<Result<(), AdministrationAuditExecutionError>>>,
    prepared_indices: Vec<usize>,
    fence: Option<Box<dyn riffdb_storage_api::DeferredServiceAuditFence>>,
}

impl SubmittedAuditGroup {
    fn wait(self) -> Vec<Result<(), AdministrationAuditExecutionError>> {
        let mut group = self;
        let Some(fence) = group.fence.take() else {
            return group.fail(StorageError::new(
                riffdb_storage_api::StorageErrorKind::InvariantViolation,
                None,
            ));
        };
        match fence.wait() {
            Ok(results) => group.install(results),
            Err(error) => group.fail(error),
        }
    }

    fn install(
        &mut self,
        results: Vec<ServiceAuditAppendResult>,
    ) -> Vec<Result<(), AdministrationAuditExecutionError>> {
        if results.len() != self.prepared_indices.len() {
            return self.fail(StorageError::new(
                riffdb_storage_api::StorageErrorKind::InvariantViolation,
                None,
            ));
        }
        for (index, result) in self.prepared_indices.drain(..).zip(results) {
            self.outputs[index] = Some(match result {
                ServiceAuditAppendResult::Appended(_) => Ok(()),
                ServiceAuditAppendResult::PhaseConflict => {
                    Err(AdministrationAuditExecutionError::PhaseConflict)
                }
            });
        }
        self.take_outputs()
    }

    fn fail(&mut self, error: StorageError) -> Vec<Result<(), AdministrationAuditExecutionError>> {
        for index in self.prepared_indices.drain(..) {
            self.outputs[index] = Some(Err(AdministrationAuditExecutionError::Storage(
                error.clone(),
            )));
        }
        self.take_outputs()
    }

    fn take_outputs(&mut self) -> Vec<Result<(), AdministrationAuditExecutionError>> {
        self.outputs
            .drain(..)
            .map(|output| {
                output.unwrap_or(Err(AdministrationAuditExecutionError::CoordinatorStopped))
            })
            .collect()
    }
}

fn submit_administration_audit_group(
    repository: &mut dyn ServiceAuditAppendRepository,
    clock: &dyn AdministrationClock,
    inputs: &[Box<dyn AdministrationAuditInputView>],
) -> AuditGroupDriveResult {
    let mut outputs = (0..inputs.len()).map(|_| None).collect::<Vec<_>>();
    let mut prepared = Vec::with_capacity(inputs.len());
    for (index, input) in inputs.iter().enumerate() {
        match prepare_administration_audit(clock, input.as_ref()) {
            Ok(intent) => prepared.push((index, intent)),
            Err(error) => outputs[index] = Some(Err(error)),
        }
    }
    if prepared.is_empty() {
        return AuditGroupDriveResult::Complete(
            outputs
                .into_iter()
                .map(|output| {
                    output.unwrap_or(Err(AdministrationAuditExecutionError::CoordinatorStopped))
                })
                .collect(),
        );
    }
    let prepared_indices = prepared.iter().map(|(index, _)| *index).collect::<Vec<_>>();
    let intents = prepared
        .into_iter()
        .map(|(_, intent)| intent)
        .collect::<Vec<_>>();
    match repository.submit_service_audit_group(&intents) {
        Ok(riffdb_storage_api::ServiceAuditGroupAppend::Complete(results)) => {
            let mut submitted = SubmittedAuditGroup {
                outputs,
                prepared_indices,
                fence: None,
            };
            AuditGroupDriveResult::Complete(submitted.install(results))
        }
        Ok(riffdb_storage_api::ServiceAuditGroupAppend::Submitted(fence)) => {
            AuditGroupDriveResult::Submitted(SubmittedAuditGroup {
                outputs,
                prepared_indices,
                fence: Some(fence),
            })
        }
        Err(error) => {
            let mut submitted = SubmittedAuditGroup {
                outputs,
                prepared_indices,
                fence: None,
            };
            AuditGroupDriveResult::Complete(submitted.fail(error))
        }
    }
}

const LIFECYCLE_ACCEPTING: u8 = 0;
const LIFECYCLE_DRAINING: u8 = 1;
const LIFECYCLE_FENCED: u8 = 2;
const LIFECYCLE_STOPPED: u8 = 3;

/// Test-only: non-null points at the lifecycle Arc of the coordinator whose
/// intake actor should panic after the next dispatch (drop-order test).
#[cfg(test)]
static TEST_PANIC_LIFECYCLE: std::sync::atomic::AtomicPtr<AtomicU8> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());

/// Monotonic drop-order counters (atomics only; no blocking locks here).
#[cfg(test)]
static TEST_DROP_SEQ: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_WRITER_HANDLE_DROP_END_SEQ: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_STOPPED_LIFECYCLE_DROP_SEQ: AtomicU64 = AtomicU64::new(0);
const SUBMISSION_GATE_CLOSED: usize = 1 << (usize::BITS - 1);
const SUBMISSION_COUNT_MASK: usize = !SUBMISSION_GATE_CLOSED;
const MAX_QUEUED_COMMAND_BYTES: usize = 32 * 1_024 * 1_024;
const QUEUED_COMMAND_BYTE_UNIT: usize = 1_024;
const OLDEST_GROUPABLE_TRANSITION_MAX_AGE: Duration = Duration::from_micros(200);
/// Contention-only completion-edge window. Unlike the fresh unary window, this
/// opens only when a prior writer unit left at least two commands already
/// queued and no barrier is present. It lets clients released by the prior
/// commit contribute to the same next atomic redb group without penalizing an
/// idle singleton.
const POST_COMMIT_COALESCE_BUDGET: Duration = Duration::from_millis(2);
/// Tokio rounds timer deadlines to its millisecond wheel. Arm one tick early so
/// that rounding does not intentionally extend the accepted logical budget.
const POST_COMMIT_COALESCE_TIMER_GUARD: Duration = Duration::from_millis(1);

const fn completion_edge_coalescing_enabled(durability: CoordinatorDurability) -> bool {
    matches!(durability, CoordinatorDurability::Sync)
}

/// Exact number of coordinator workload messages admitted independently of shutdown.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoordinatorWorkloadCapacity(NonZeroU16);

impl CoordinatorWorkloadCapacity {
    /// Checks and constructs a nonzero workload capacity.
    #[must_use]
    pub const fn new(value: u16) -> Option<Self> {
        match NonZeroU16::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the exact number of workload slots.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// Safe failure to start the dedicated coordinator actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorStartError {
    /// Tokio could not construct the current-thread scheduler.
    RuntimeUnavailable,
    /// The dedicated operating-system thread could not be created.
    ThreadUnavailable,
    /// A bounded command-preparation worker could not be created.
    PreparationWorkerUnavailable,
    /// The permanently reserved shutdown slot could not be established.
    ShutdownCapacityUnavailable,
}

impl fmt::Display for CoordinatorStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RuntimeUnavailable => "coordinator runtime is unavailable",
            Self::ThreadUnavailable => "coordinator thread is unavailable",
            Self::PreparationWorkerUnavailable => "command preparation worker is unavailable",
            Self::ShutdownCapacityUnavailable => "coordinator shutdown capacity is unavailable",
        })
    }
}

impl Error for CoordinatorStartError {}

/// Process-local lifecycle of the authoritative coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorLifecycleState {
    /// New bounded work may be reserved and submitted.
    Accepting,
    /// Shutdown is draining work accepted before the close boundary.
    Draining,
    /// An unknown authoritative write forbids further work until recovery.
    Fenced,
    /// The actor exited without an unresolved authoritative-write fence.
    Stopped,
}

/// Safe rejection before an audit item is accepted by the coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdministrationAuditAdmissionError {
    /// Shutdown has begun and new work is no longer accepted.
    Draining,
    /// An unknown authoritative write fenced all later work.
    Fenced,
    /// The coordinator actor has stopped.
    Stopped,
}

impl fmt::Display for AdministrationAuditAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Draining => "command coordinator is draining",
            Self::Fenced => "command coordinator fenced authoritative writes",
            Self::Stopped => "command coordinator has stopped",
        })
    }
}

impl Error for AdministrationAuditAdmissionError {}

/// Safe failure while explicitly draining and joining the coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorShutdownError {
    /// Joining from the actor thread would deadlock and is rejected.
    SelfJoin,
    /// The actor terminated through a panic.
    ActorPanicked,
}

impl fmt::Display for CoordinatorShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SelfJoin => "coordinator cannot join its own actor thread",
            Self::ActorPanicked => "command coordinator actor terminated unexpectedly",
        })
    }
}

impl Error for CoordinatorShutdownError {}

/// Cloneable, non-generic application-service handle for audit work.
#[derive(Clone)]
pub struct AdministrationAuditExecutor {
    sender: mpsc::Sender<CoordinatorMessage>,
    lifecycle: Arc<AtomicU8>,
    submission_gate: Arc<SubmissionGate>,
    /// Counts accepted Single and FusedPair submissions (each counts as one message).
    accepted_submissions: Arc<AtomicU64>,
}

impl AdministrationAuditExecutor {
    /// Asynchronously reserves exactly one workload slot.
    ///
    /// Cancelling this future before it resolves leaves no retained capacity.
    /// The returned permit is move-only and must be consumed synchronously by
    /// [`AdministrationAuditCapacityPermit::submit`] after the caller's final
    /// authorization safe point.
    pub async fn reserve_capacity(
        &self,
    ) -> Result<AdministrationAuditCapacityPermit, AdministrationAuditAdmissionError> {
        self.reserve_capacity_after_reservation(|| {}).await
    }

    async fn reserve_capacity_after_reservation(
        &self,
        after_reservation: impl FnOnce(),
    ) -> Result<AdministrationAuditCapacityPermit, AdministrationAuditAdmissionError> {
        ensure_accepting(&self.lifecycle)?;
        let permit = self
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| lifecycle_error(&self.lifecycle))?;
        after_reservation();
        if self.submission_gate.is_closed() {
            drop(permit);
            return Err(lifecycle_error(&self.lifecycle));
        }
        ensure_accepting(&self.lifecycle)?;
        Ok(AdministrationAuditCapacityPermit {
            permit: Some(permit),
            lifecycle: Arc::clone(&self.lifecycle),
            submission_gate: Arc::clone(&self.submission_gate),
            accepted_submissions: Arc::clone(&self.accepted_submissions),
        })
    }

    #[cfg(test)]
    async fn reserve_capacity_with_hook(
        &self,
        after_reservation: impl FnOnce(),
    ) -> Result<AdministrationAuditCapacityPermit, AdministrationAuditAdmissionError> {
        self.reserve_capacity_after_reservation(after_reservation)
            .await
    }

    /// Returns the current process-local coordinator lifecycle.
    #[must_use]
    pub fn lifecycle_state(&self) -> CoordinatorLifecycleState {
        lifecycle_state(&self.lifecycle)
    }

    /// Number of accepted administration-audit submissions (Single or FusedPair).
    ///
    /// Each `submit` / `submit_fused_pair` success increments by one. Used by
    /// service tests that prove fused replay is one coordinator message.
    #[doc(hidden)]
    #[must_use]
    pub fn accepted_submission_count(&self) -> u64 {
        self.accepted_submissions.load(Ordering::Relaxed)
    }
}

impl fmt::Debug for AdministrationAuditExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AdministrationAuditExecutor([REDACTED])")
    }
}

/// Move-only authority to synchronously submit one checked audit input.
///
/// ```compile_fail
/// use riffdb_commit::AdministrationAuditCapacityPermit;
///
/// fn cannot_duplicate(value: &AdministrationAuditCapacityPermit) {
///     let _: AdministrationAuditCapacityPermit =
///         <AdministrationAuditCapacityPermit as Clone>::clone(value);
/// }
/// ```
#[must_use = "dropping the permit releases its reserved workload capacity"]
pub struct AdministrationAuditCapacityPermit {
    permit: Option<mpsc::OwnedPermit<CoordinatorMessage>>,
    lifecycle: Arc<AtomicU8>,
    submission_gate: Arc<SubmissionGate>,
    accepted_submissions: Arc<AtomicU64>,
}

impl AdministrationAuditCapacityPermit {
    /// Synchronously accepts one independently owned audit attempt.
    ///
    /// Success is the non-retroactive admission boundary. Dropping the returned
    /// receipt never cancels the accepted append attempt.
    pub fn submit(
        self,
        input: Box<dyn AdministrationAuditInputView>,
    ) -> Result<AdministrationAuditReceipt, AdministrationAuditAdmissionError> {
        self.submit_after_admission(AdministrationAuditSubmission::Single(input), || {})
    }

    /// Submits one Started+terminal pair that shares a single durable transition.
    pub fn submit_fused_pair(
        self,
        started: Box<dyn AdministrationAuditInputView>,
        terminal: Box<dyn AdministrationAuditInputView>,
    ) -> Result<AdministrationAuditReceipt, AdministrationAuditAdmissionError> {
        self.submit_after_admission(
            AdministrationAuditSubmission::FusedPair { started, terminal },
            || {},
        )
    }

    fn submit_after_admission(
        mut self,
        submission_payload: AdministrationAuditSubmission,
        after_admission: impl FnOnce(),
    ) -> Result<AdministrationAuditReceipt, AdministrationAuditAdmissionError> {
        let submission = self
            .submission_gate
            .begin()
            .ok_or_else(|| lifecycle_error(&self.lifecycle))?;
        ensure_accepting(&self.lifecycle)?;
        after_admission();
        let (completion, receiver) = oneshot::channel();
        let permit = self
            .permit
            .take()
            .expect("move-only audit capacity permit is consumed once");
        let _sender = permit.send(CoordinatorMessage::AdministrationAudit {
            submission: submission_payload,
            completion,
        });
        self.accepted_submissions.fetch_add(1, Ordering::Relaxed);
        drop(submission);
        Ok(AdministrationAuditReceipt { receiver })
    }

    #[cfg(test)]
    fn submit_with_hook(
        self,
        input: Box<dyn AdministrationAuditInputView>,
        after_admission: impl FnOnce(),
    ) -> Result<AdministrationAuditReceipt, AdministrationAuditAdmissionError> {
        self.submit_after_admission(
            AdministrationAuditSubmission::Single(input),
            after_admission,
        )
    }
}

impl fmt::Debug for AdministrationAuditCapacityPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AdministrationAuditCapacityPermit([REDACTED])")
    }
}

/// Move-only completion handle for one accepted audit attempt.
#[must_use = "await or deliberately drop the receipt; dropping does not cancel accepted work"]
pub struct AdministrationAuditReceipt {
    receiver: oneshot::Receiver<Result<(), AdministrationAuditExecutionError>>,
}

impl AdministrationAuditReceipt {
    /// Waits for the accepted append attempt to finish.
    pub async fn completion(self) -> Result<(), AdministrationAuditExecutionError> {
        self.receiver
            .await
            .unwrap_or(Err(AdministrationAuditExecutionError::CoordinatorStopped))
    }
}

impl fmt::Debug for AdministrationAuditReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AdministrationAuditReceipt([REDACTED])")
    }
}

/// Safe rejection before a control-plane preparation enters the sole-writer queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlPlaneExecutionAdmissionError {
    /// Shutdown has begun and new work is no longer accepted.
    Draining,
    /// An unknown authoritative write fenced all later work.
    Fenced,
    /// The coordinator actor has stopped.
    Stopped,
}

impl fmt::Display for ControlPlaneExecutionAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Draining => "command coordinator is draining",
            Self::Fenced => "command coordinator fenced authoritative writes",
            Self::Stopped => "command coordinator has stopped",
        })
    }
}

impl Error for ControlPlaneExecutionAdmissionError {}

/// Cloneable least-authority handle for typed control-plane work.
#[derive(Clone)]
pub struct ControlPlaneExecutor {
    sender: mpsc::Sender<CoordinatorMessage>,
    lifecycle: Arc<AtomicU8>,
    submission_gate: Arc<SubmissionGate>,
}

impl ControlPlaneExecutor {
    /// Reserves one slot in the same bounded queue used by commands and audit work.
    pub async fn reserve_capacity(
        &self,
    ) -> Result<ControlPlaneExecutionCapacityPermit, ControlPlaneExecutionAdmissionError> {
        ensure_control_plane_accepting(&self.lifecycle)?;
        let permit = self
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| control_plane_lifecycle_error(&self.lifecycle))?;
        if self.submission_gate.is_closed() {
            drop(permit);
            return Err(control_plane_lifecycle_error(&self.lifecycle));
        }
        ensure_control_plane_accepting(&self.lifecycle)?;
        Ok(ControlPlaneExecutionCapacityPermit {
            permit: Some(permit),
            lifecycle: Arc::clone(&self.lifecycle),
            submission_gate: Arc::clone(&self.submission_gate),
        })
    }

    /// Returns the current process-local coordinator lifecycle.
    #[must_use]
    pub fn lifecycle_state(&self) -> CoordinatorLifecycleState {
        lifecycle_state(&self.lifecycle)
    }
}

impl fmt::Debug for ControlPlaneExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControlPlaneExecutor([REDACTED])")
    }
}

/// Move-only authority to synchronously submit one typed control-plane operation.
#[must_use = "dropping the permit releases its reserved workload capacity"]
pub struct ControlPlaneExecutionCapacityPermit {
    permit: Option<mpsc::OwnedPermit<CoordinatorMessage>>,
    lifecycle: Arc<AtomicU8>,
    submission_gate: Arc<SubmissionGate>,
}

impl ControlPlaneExecutionCapacityPermit {
    /// Submits one authorized catalog deployment without another await point.
    pub fn submit_catalog_deployment(
        self,
        preparation: CatalogDeploymentPreparation,
    ) -> Result<CatalogDeploymentReceipt, ControlPlaneExecutionAdmissionError> {
        let (permit, submission) = self.into_submission()?;
        let (completion, receiver) = oneshot::channel();
        let _sender = permit.send(CoordinatorMessage::CatalogDeployment {
            preparation: Box::new(preparation),
            completion,
        });
        drop(submission);
        Ok(CatalogDeploymentReceipt { receiver })
    }

    /// Submits one exact-contract query-module activation.
    pub fn submit_query_module_deployment(
        self,
        preparation: QueryModuleDeploymentPreparation,
    ) -> Result<QueryModuleDeploymentReceipt, ControlPlaneExecutionAdmissionError> {
        let (permit, submission) = self.into_submission()?;
        let (completion, receiver) = oneshot::channel();
        let _sender = permit.send(CoordinatorMessage::QueryModuleDeployment {
            preparation: Box::new(preparation),
            completion,
        });
        drop(submission);
        Ok(QueryModuleDeploymentReceipt { receiver })
    }

    /// Submits one exact-contract immutable reactive-module publication.
    pub fn submit_reactive_module_publication(
        self,
        preparation: ReactiveModulePublicationPreparation,
    ) -> Result<ReactiveModulePublicationReceipt, ControlPlaneExecutionAdmissionError> {
        let (permit, submission) = self.into_submission()?;
        let (completion, receiver) = oneshot::channel();
        let _sender = permit.send(CoordinatorMessage::ReactiveModulePublication {
            preparation: Box::new(preparation),
            completion,
        });
        drop(submission);
        Ok(ReactiveModulePublicationReceipt { receiver })
    }

    /// Submits one freshly authorized normal capability creation.
    pub fn submit_capability_create(
        self,
        preparation: CapabilityCreatePreparation,
    ) -> Result<CapabilityCreateReceipt, ControlPlaneExecutionAdmissionError> {
        let (permit, submission) = self.into_submission()?;
        let (completion, receiver) = oneshot::channel();
        let _sender = permit.send(CoordinatorMessage::CapabilityCreate {
            preparation: Box::new(preparation),
            completion,
        });
        drop(submission);
        Ok(CapabilityCreateReceipt { receiver })
    }

    /// Submits one freshly authorized normal capability revocation.
    pub fn submit_capability_revoke(
        self,
        preparation: CapabilityRevokePreparation,
    ) -> Result<CapabilityRevokeReceipt, ControlPlaneExecutionAdmissionError> {
        let (permit, submission) = self.into_submission()?;
        let (completion, receiver) = oneshot::channel();
        let _sender = permit.send(CoordinatorMessage::CapabilityRevoke {
            preparation: Box::new(preparation),
            completion,
        });
        drop(submission);
        Ok(CapabilityRevokeReceipt { receiver })
    }

    /// Submits the closed principal-less compound bootstrap transition.
    pub fn submit_capability_bootstrap(
        self,
        preparation: CapabilityBootstrapPreparation,
    ) -> Result<CapabilityBootstrapReceipt, ControlPlaneExecutionAdmissionError> {
        let (permit, submission) = self.into_submission()?;
        let (completion, receiver) = oneshot::channel();
        let _sender = permit.send(CoordinatorMessage::CapabilityBootstrap {
            preparation: Box::new(preparation),
            completion,
        });
        drop(submission);
        Ok(CapabilityBootstrapReceipt { receiver })
    }

    /// Submits exactly one terminal append derived from a successful bootstrap.
    pub fn submit_capability_bootstrap_terminal(
        self,
        preparation: CapabilityBootstrapTerminalPreparation,
    ) -> Result<CapabilityBootstrapTerminalReceipt, ControlPlaneExecutionAdmissionError> {
        let (permit, submission) = self.into_submission()?;
        let (completion, receiver) = oneshot::channel();
        let _sender = permit.send(CoordinatorMessage::CapabilityBootstrapTerminal {
            preparation: Box::new(preparation),
            completion,
        });
        drop(submission);
        Ok(CapabilityBootstrapTerminalReceipt { receiver })
    }

    fn into_submission(
        mut self,
    ) -> Result<
        (mpsc::OwnedPermit<CoordinatorMessage>, ActiveSubmission),
        ControlPlaneExecutionAdmissionError,
    > {
        let submission = self
            .submission_gate
            .begin()
            .ok_or_else(|| control_plane_lifecycle_error(&self.lifecycle))?;
        ensure_control_plane_accepting(&self.lifecycle)?;
        let permit = self
            .permit
            .take()
            .expect("move-only control-plane capacity permit is consumed once");
        Ok((permit, submission))
    }
}

impl fmt::Debug for ControlPlaneExecutionCapacityPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControlPlaneExecutionCapacityPermit([REDACTED])")
    }
}

macro_rules! control_plane_receipt {
    ($name:ident, $result:ty) => {
        #[doc = "Move-only completion handle for one accepted control-plane operation."]
        #[must_use = "await or deliberately drop the receipt; dropping does not cancel accepted work"]
        pub struct $name {
            receiver: oneshot::Receiver<Result<$result, ControlPlaneExecutionError>>,
        }

        impl $name {
            /// Waits for the actor-owned operation to finish.
            pub async fn completion(self) -> Result<$result, ControlPlaneExecutionError> {
                self.receiver
                    .await
                    .unwrap_or_else(|_| Err(ControlPlaneExecutionError::coordinator_stopped()))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "([REDACTED])"))
            }
        }
    };
}

control_plane_receipt!(CatalogDeploymentReceipt, CatalogDeploymentResult);
control_plane_receipt!(QueryModuleDeploymentReceipt, QueryModuleDeploymentResult);
control_plane_receipt!(
    ReactiveModulePublicationReceipt,
    ReactiveModulePublicationExecutionResult
);
control_plane_receipt!(CapabilityCreateReceipt, CapabilityCreateExecutionResult);
control_plane_receipt!(CapabilityRevokeReceipt, CapabilityRevokeExecutionResult);
control_plane_receipt!(
    CapabilityBootstrapReceipt,
    CapabilityBootstrapExecutionResult
);
control_plane_receipt!(CapabilityBootstrapTerminalReceipt, ());

/// Safe rejection before a command preparation is accepted by the coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandExecutionAdmissionError {
    /// Coordinator queue depth or retained-byte budget is full.
    ///
    /// This is a certain-not-executed rejection: the coordinator did not accept
    /// the preparation. Callers map it to the typed overload public error.
    Overloaded,
    /// The independent retained-byte queue bound is full.
    ///
    /// Prefer [`Self::Overloaded`] at new call sites. Retained for legacy
    /// submit-time acquisition paths that have not yet pre-admitted bytes.
    RetainedByteCapacityExceeded,
    /// Pre-admitted retained-byte units were smaller than the preparation needs.
    ///
    /// This is an internal defect (units derivation mismatch), never a silent
    /// accept and never a capacity rejection after accept.
    PermitUnitMismatch,
    /// Shutdown has begun and new work is no longer accepted.
    Draining,
    /// An unknown authoritative write fenced all later work.
    Fenced,
    /// The coordinator actor has stopped.
    Stopped,
}

impl fmt::Display for CommandExecutionAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Overloaded => "command coordinator is over capacity",
            Self::RetainedByteCapacityExceeded => "command retained-byte capacity is full",
            Self::PermitUnitMismatch => {
                "command capacity permit retained-byte units undershoot preparation"
            }
            Self::Draining => "command coordinator is draining",
            Self::Fenced => "command coordinator fenced authoritative writes",
            Self::Stopped => "command coordinator has stopped",
        })
    }
}

impl Error for CommandExecutionAdmissionError {}

/// Cloneable, non-generic application-service handle for command work.
#[derive(Clone)]
pub struct CommandExecutor {
    sender: mpsc::Sender<CoordinatorMessage>,
    lifecycle: Arc<AtomicU8>,
    submission_gate: Arc<SubmissionGate>,
    retained_byte_capacity: Arc<Semaphore>,
    /// Writer-published EWMA of enqueue→start + service time, in microseconds.
    queue_delay_estimate_micros: Arc<AtomicU64>,
}

impl CommandExecutor {
    /// Asynchronously reserves exactly one shared coordinator workload slot.
    ///
    /// Cancelling this future before it resolves retains no capacity and
    /// submits no work. The returned permit is move-only and must be consumed
    /// synchronously after the caller's final authorization safe point.
    pub async fn reserve_capacity(
        &self,
    ) -> Result<CommandExecutionCapacityPermit, CommandExecutionAdmissionError> {
        ensure_command_accepting(&self.lifecycle)?;
        let permit = self
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| command_lifecycle_error(&self.lifecycle))?;
        Self::finish_queue_reservation(
            permit,
            &self.lifecycle,
            &self.submission_gate,
            &self.retained_byte_capacity,
        )
    }

    /// Non-blocking reservation of one shared coordinator workload slot.
    ///
    /// Returns [`CommandExecutionAdmissionError::Overloaded`] when the
    /// coordinator queue is full without waiting. No clock enters this path.
    /// Cancelling is unnecessary: the call never parks.
    pub fn try_reserve_capacity(
        &self,
    ) -> Result<CommandExecutionCapacityPermit, CommandExecutionAdmissionError> {
        ensure_command_accepting(&self.lifecycle)?;
        let permit = match self.sender.clone().try_reserve_owned() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(CommandExecutionAdmissionError::Overloaded);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(command_lifecycle_error(&self.lifecycle));
            }
        };
        Self::finish_queue_reservation(
            permit,
            &self.lifecycle,
            &self.submission_gate,
            &self.retained_byte_capacity,
        )
    }

    /// Non-blocking acquisition of retained-byte budget for one preparation.
    ///
    /// `units` must equal [`crate::command_preparation::queued_preparation_units`] for the normalized
    /// input that will be submitted. Exhaustion returns
    /// [`CommandExecutionAdmissionError::Overloaded`].
    pub fn try_acquire_retained_bytes(
        &self,
        units: u32,
    ) -> Result<OwnedSemaphorePermit, CommandExecutionAdmissionError> {
        ensure_command_accepting(&self.lifecycle)?;
        self.retained_byte_capacity
            .clone()
            .try_acquire_many_owned(units.max(1))
            .map_err(|_| CommandExecutionAdmissionError::Overloaded)
    }

    /// Bounded wait for retained-byte budget (same closed window as queue depth).
    ///
    /// Cancelling before resolution retains no bytes. Used by service admission
    /// after a non-blocking miss.
    pub async fn acquire_retained_bytes(
        &self,
        units: u32,
    ) -> Result<OwnedSemaphorePermit, CommandExecutionAdmissionError> {
        ensure_command_accepting(&self.lifecycle)?;
        self.retained_byte_capacity
            .clone()
            .acquire_many_owned(units.max(1))
            .await
            .map_err(|_| command_lifecycle_error(&self.lifecycle))
    }

    fn finish_queue_reservation(
        permit: mpsc::OwnedPermit<CoordinatorMessage>,
        lifecycle: &Arc<AtomicU8>,
        submission_gate: &Arc<SubmissionGate>,
        retained_byte_capacity: &Arc<Semaphore>,
    ) -> Result<CommandExecutionCapacityPermit, CommandExecutionAdmissionError> {
        if submission_gate.is_closed() {
            drop(permit);
            return Err(command_lifecycle_error(lifecycle));
        }
        ensure_command_accepting(lifecycle)?;
        Ok(CommandExecutionCapacityPermit {
            permit: Some(permit),
            lifecycle: Arc::clone(lifecycle),
            submission_gate: Arc::clone(submission_gate),
            retained_byte_capacity: Arc::clone(retained_byte_capacity),
            retained_byte_permit: None,
            retained_byte_units: 0,
        })
    }

    /// Returns the current process-local coordinator lifecycle.
    #[must_use]
    pub fn lifecycle_state(&self) -> CoordinatorLifecycleState {
        lifecycle_state(&self.lifecycle)
    }

    /// Latest writer EWMA of queue delay (enqueue→start + service), in microseconds.
    ///
    /// Zero until the writer has completed at least one unit. Callers use this as a
    /// pre-admission shed signal; a stale zero never rejects (estimate must be > 0).
    #[must_use]
    pub fn estimated_queue_delay_micros(&self) -> u64 {
        self.queue_delay_estimate_micros.load(Ordering::Relaxed)
    }

    /// Test-only: force the EWMA queue-delay estimate used by pre-admission shed.
    #[doc(hidden)]
    pub fn force_queue_delay_estimate_micros_for_tests(&self, micros: u64) {
        self.queue_delay_estimate_micros
            .store(micros, Ordering::Relaxed);
    }
}

impl fmt::Debug for CommandExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandExecutor([REDACTED])")
    }
}

/// Move-only authority to synchronously submit one checked command preparation.
///
/// ```compile_fail
/// use riffdb_commit::CommandExecutionCapacityPermit;
///
/// fn cannot_duplicate(value: &CommandExecutionCapacityPermit) {
///     let _: CommandExecutionCapacityPermit =
///         <CommandExecutionCapacityPermit as Clone>::clone(value);
/// }
/// ```
#[must_use = "dropping the permit releases its reserved workload capacity"]
pub struct CommandExecutionCapacityPermit {
    permit: Option<mpsc::OwnedPermit<CoordinatorMessage>>,
    lifecycle: Arc<AtomicU8>,
    submission_gate: Arc<SubmissionGate>,
    retained_byte_capacity: Arc<Semaphore>,
    retained_byte_permit: Option<OwnedSemaphorePermit>,
    retained_byte_units: u32,
}

impl CommandExecutionCapacityPermit {
    /// Attaches a pre-admitted retained-byte permit acquired for `units`.
    ///
    /// Service admission acquires retained bytes before authorization; submit
    /// then asserts the preparation does not need more units than attached.
    pub fn with_retained_bytes(mut self, permit: OwnedSemaphorePermit, units: u32) -> Self {
        self.retained_byte_permit = Some(permit);
        self.retained_byte_units = units;
        self
    }

    /// Synchronously transfers one checked command preparation to the actor.
    ///
    /// Success is the non-retroactive admission boundary. Dropping the returned
    /// receipt never cancels the accepted command.
    pub fn submit(
        mut self,
        preparation: CommandExecutionPreparation,
    ) -> Result<CommandExecutionReceipt, CommandExecutionAdmissionError> {
        let submission = self
            .submission_gate
            .begin()
            .ok_or_else(|| command_lifecycle_error(&self.lifecycle))?;
        ensure_command_accepting(&self.lifecycle)?;
        let retained_byte_permit =
            self.take_retained_byte_permit(preparation.queued_byte_units())?;
        let (completion, receiver) = oneshot::channel();
        let permit = self
            .permit
            .take()
            .expect("move-only command capacity permit is consumed once");
        let (command_id, ingress) = preparation.telemetry_identity();
        let _sender = permit.send(CoordinatorMessage::Command {
            preparation: Box::new(preparation),
            command_id,
            ingress,
            enqueued_at: Instant::now(),
            completion,
            _retained_byte_permit: retained_byte_permit,
        });
        drop(submission);
        Ok(CommandExecutionReceipt { receiver })
    }

    /// Synchronously transfers one checked read-only preparation to the actor.
    ///
    /// This consumes the same capacity authority and final-authorization
    /// boundary as a mutating command. The accepted operation creates no
    /// command admission, provenance, application sequence, or durable outcome.
    pub fn submit_read_only(
        mut self,
        preparation: ReadOnlyExecutionPreparation,
    ) -> Result<ReadOnlyExecutionReceipt, CommandExecutionAdmissionError> {
        let submission = self
            .submission_gate
            .begin()
            .ok_or_else(|| command_lifecycle_error(&self.lifecycle))?;
        ensure_command_accepting(&self.lifecycle)?;
        let retained_byte_permit =
            self.take_retained_byte_permit(preparation.queued_byte_units())?;
        let (completion, receiver) = oneshot::channel();
        let permit = self
            .permit
            .take()
            .expect("move-only command capacity permit is consumed once");
        let (command_id, ingress) = preparation.telemetry_identity();
        let _sender = permit.send(CoordinatorMessage::ReadOnlyCommand {
            preparation: Box::new(preparation),
            command_id,
            ingress,
            enqueued_at: Instant::now(),
            completion,
            _retained_byte_permit: retained_byte_permit,
        });
        drop(submission);
        Ok(ReadOnlyExecutionReceipt { receiver })
    }

    fn take_retained_byte_permit(
        &mut self,
        required_units: u32,
    ) -> Result<OwnedSemaphorePermit, CommandExecutionAdmissionError> {
        if let Some(held) = self.retained_byte_permit.take() {
            if required_units > self.retained_byte_units {
                // Undersized pre-admission is an internal defect, never silent accept.
                return Err(CommandExecutionAdmissionError::PermitUnitMismatch);
            }
            return Ok(held);
        }
        // Fallback: acquire at submit when the caller did not pre-admit bytes.
        // Maps to Overloaded (typed capacity) so residual paths stay fail-closed.
        self.retained_byte_capacity
            .clone()
            .try_acquire_many_owned(required_units.max(1))
            .map_err(|_| CommandExecutionAdmissionError::Overloaded)
    }
}

impl fmt::Debug for CommandExecutionCapacityPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandExecutionCapacityPermit([REDACTED])")
    }
}

/// Move-only completion handle for one actor-owned command execution.
#[must_use = "await or deliberately drop the receipt; dropping does not cancel accepted work"]
pub struct CommandExecutionReceipt {
    receiver: oneshot::Receiver<Result<CommandExecutionResult, CommandExecutionError>>,
}

impl CommandExecutionReceipt {
    /// Waits for the accepted command execution to finish.
    pub async fn completion(self) -> Result<CommandExecutionResult, CommandExecutionError> {
        self.receiver
            .await
            .unwrap_or_else(|_| Err(CommandExecutionError::coordinator_stopped()))
    }
}

impl fmt::Debug for CommandExecutionReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandExecutionReceipt([REDACTED])")
    }
}

/// Move-only completion handle for one accepted unjournaled command read.
#[must_use = "await or deliberately drop the receipt; dropping does not cancel accepted work"]
pub struct ReadOnlyExecutionReceipt {
    receiver: oneshot::Receiver<Result<ReadOnlyExecutionResult, CommandExecutionError>>,
}

impl ReadOnlyExecutionReceipt {
    /// Waits for the accepted read-only execution to finish.
    pub async fn completion(self) -> Result<ReadOnlyExecutionResult, CommandExecutionError> {
        self.receiver
            .await
            .unwrap_or_else(|_| Err(CommandExecutionError::coordinator_stopped()))
    }
}

impl fmt::Debug for ReadOnlyExecutionReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReadOnlyExecutionReceipt([REDACTED])")
    }
}

struct DirectIdempotencyInspectionMessage {
    preparation: PreparedCommandIdempotencyInspection,
    completion:
        oneshot::Sender<Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError>>,
}

#[derive(Clone)]
struct DirectIdempotencyInspectionBatcher {
    sender: mpsc::Sender<DirectIdempotencyInspectionMessage>,
}

impl DirectIdempotencyInspectionBatcher {
    fn start(
        repository: Arc<dyn AdmissionLookupRepository + Send + Sync>,
        lifecycle: ActorLifecyclePublisher,
    ) -> Option<Self> {
        let (sender, mut receiver) = mpsc::channel::<DirectIdempotencyInspectionMessage>(
            riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS * 2,
        );
        thread::Builder::new()
            .name("riffdb-idempotency-reader".to_owned())
            .spawn(move || {
                while let Some(first) = receiver.blocking_recv() {
                    let mut group = vec![first];
                    while group.len() < riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
                        match receiver.try_recv() {
                            Ok(message) => group.push(message),
                            Err(_) => break,
                        }
                    }

                    let (preparations, completions): (Vec<_>, Vec<_>) = group
                        .into_iter()
                        .map(|message| (message.preparation, message.completion))
                        .unzip();
                    let results = inspect_command_idempotency_group(
                        repository.as_ref(),
                        &lifecycle,
                        preparations,
                    );
                    for (completion, result) in completions.into_iter().zip(results) {
                        let _receiver_may_be_dropped = completion.send(result);
                    }
                }
            })
            .ok()?;
        Some(Self { sender })
    }
}

/// Cloneable least-authority handle for bounded pre-admission inspection.
///
/// Production uses a bounded read-only microbatch lane. The actor-backed path
/// remains for conformance repositories and never grants mutation authority.
#[derive(Clone)]
pub struct CommandIdempotencyInspector {
    sender: mpsc::Sender<CoordinatorMessage>,
    lifecycle: Arc<AtomicU8>,
    submission_gate: Arc<SubmissionGate>,
    digest_provider: Arc<dyn IdempotencyDigestProvider>,
    direct_batcher: Option<DirectIdempotencyInspectionBatcher>,
}

impl CommandIdempotencyInspector {
    /// Routes inspection through one activated least-authority MVCC reader
    /// instead of the authoritative writer queue.
    #[must_use]
    pub fn with_direct_repository(
        mut self,
        repository: Arc<dyn AdmissionLookupRepository + Send + Sync>,
    ) -> Self {
        self.direct_batcher = DirectIdempotencyInspectionBatcher::start(
            repository,
            ActorLifecyclePublisher {
                lifecycle: Arc::clone(&self.lifecycle),
                submission_gate: Arc::clone(&self.submission_gate),
            },
        );
        self
    }

    /// Performs one cancellation-safe bounded observation through the actor.
    ///
    /// Cancelling before the queue reservation resolves submits no work.
    /// Cancelling after synchronous submission may discard the response, but
    /// the actor still completes the read-only observation.
    pub async fn inspect(
        &self,
        request: CommandIdempotencyInspectionRequest,
    ) -> Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError> {
        let lifecycle = ActorLifecyclePublisher {
            lifecycle: Arc::clone(&self.lifecycle),
            submission_gate: Arc::clone(&self.submission_gate),
        };
        let preparation = prepare_command_idempotency_inspection(
            self.digest_provider.as_ref(),
            &lifecycle,
            request,
        )?;
        ensure_idempotency_inspection_accepting(&self.lifecycle)?;
        if let Some(batcher) = &self.direct_batcher {
            let permit = batcher
                .sender
                .clone()
                .reserve_owned()
                .await
                .map_err(|_| idempotency_inspection_lifecycle_error(&self.lifecycle))?;
            ensure_idempotency_inspection_accepting(&self.lifecycle)?;
            let (completion, receiver) = oneshot::channel();
            let _sender = permit.send(DirectIdempotencyInspectionMessage {
                preparation,
                completion,
            });
            return receiver
                .await
                .unwrap_or_else(|_| Err(CommandIdempotencyInspectionError::coordinator_stopped()));
        }
        let permit = self
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| idempotency_inspection_lifecycle_error(&self.lifecycle))?;
        if self.submission_gate.is_closed() {
            drop(permit);
            return Err(idempotency_inspection_lifecycle_error(&self.lifecycle));
        }
        let submission = self
            .submission_gate
            .begin()
            .ok_or_else(|| idempotency_inspection_lifecycle_error(&self.lifecycle))?;
        ensure_idempotency_inspection_accepting(&self.lifecycle)?;
        let (completion, receiver) = oneshot::channel();
        let _sender = permit.send(CoordinatorMessage::IdempotencyInspection {
            preparation: Box::new(preparation),
            completion,
        });
        drop(submission);
        receiver
            .await
            .unwrap_or_else(|_| Err(CommandIdempotencyInspectionError::coordinator_stopped()))
    }

    /// Returns the current process-local coordinator lifecycle.
    #[must_use]
    pub fn lifecycle_state(&self) -> CoordinatorLifecycleState {
        lifecycle_state(&self.lifecycle)
    }
}

impl fmt::Debug for CommandIdempotencyInspector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandIdempotencyInspector([REDACTED])")
    }
}

/// Owning lifecycle guard for the one extensible sole-writer coordinator actor.
///
/// Call [`Self::shutdown`] to close admission, drain already accepted work, and
/// join the actor. Dropping this guard initiates the same drain and joins the
/// intake actor (so `WriterHandle` joins the writer before drop returns).
pub struct RunningCommandCoordinator {
    executor: AdministrationAuditExecutor,
    command_executor: CommandExecutor,
    control_plane_executor: ControlPlaneExecutor,
    shutdown_permit: Option<mpsc::OwnedPermit<CoordinatorMessage>>,
    actor_thread: Option<thread::JoinHandle<()>>,
    actor_thread_id: thread::ThreadId,
    writer_panicked: Arc<AtomicU8>,
    /// One-shot hooks run on the intake actor after a unit is dispatched.
    /// Used only by SelfJoin coverage tests (safe test path onto the actor thread).
    #[cfg(test)]
    post_dispatch_hook_tx: Option<std::sync::mpsc::SyncSender<Box<dyn FnOnce() + Send>>>,
}

impl RunningCommandCoordinator {
    /// Starts one dedicated current-thread actor owning the authoritative repository.
    #[allow(clippy::too_many_arguments)]
    #[allow(private_bounds)] // Sealed blanket proof; callers supply ordinary storage ports.
    pub fn start<Repository>(
        workload_capacity: CoordinatorWorkloadCapacity,
        durability: CoordinatorDurability,
        repository: Repository,
        conflicts: Arc<dyn ConflictManager>,
        admission_clock: Arc<dyn AdmissionClock>,
        service_uuids: Arc<dyn crate::ServiceUuidV7Source>,
        administration_clock: Arc<dyn AdministrationClock>,
        authorization_clock: Arc<dyn AuthorizationClock>,
        provenance_source: Arc<dyn ProvenanceIdSource>,
        notifications: Arc<dyn ApplicationCommitNotificationSink>,
    ) -> Result<Self, CoordinatorStartError>
    where
        Repository: AdmissionRepository
            + AuditedAdmissionRepository
            + SnapshotReader
            + ApplicationCommandTransactionPort
            + RepeatableCommandBatchPort
            + ExecutionFailureTransitionPort
            + ServiceAuditAppendRepository
            + CatalogAdministrationRepository
            + QueryModuleAdministrationRepository
            + ReactiveModuleAdministrationRepository
            + CapabilityAdministrationTransactionPort
            + CapabilityBootstrapAdministrationRepository
            + Send
            + 'static,
    {
        Self::start_with_evaluation_pool(
            workload_capacity,
            durability,
            repository,
            conflicts,
            admission_clock,
            service_uuids,
            administration_clock,
            authorization_clock,
            provenance_source,
            notifications,
            Arc::new(NoopCommitTelemetry),
            None,
        )
    }

    /// Starts the coordinator with one least-authority semantic telemetry sink.
    ///
    /// Requires [`Clone`] on the repository so an evaluation worker pool can be
    /// installed. Prefer [`Self::start_with_commit_telemetry`] when the
    /// repository is move-only (e.g. redb operational ports).
    #[allow(clippy::too_many_arguments)]
    #[allow(private_bounds)] // Sealed blanket proof; callers supply ordinary storage ports.
    pub fn start_with_telemetry<Repository>(
        workload_capacity: CoordinatorWorkloadCapacity,
        durability: CoordinatorDurability,
        repository: Repository,
        conflicts: Arc<dyn ConflictManager>,
        admission_clock: Arc<dyn AdmissionClock>,
        service_uuids: Arc<dyn crate::ServiceUuidV7Source>,
        administration_clock: Arc<dyn AdministrationClock>,
        authorization_clock: Arc<dyn AuthorizationClock>,
        provenance_source: Arc<dyn ProvenanceIdSource>,
        notifications: Arc<dyn ApplicationCommitNotificationSink>,
        telemetry: Arc<dyn CommitTelemetry>,
    ) -> Result<Self, CoordinatorStartError>
    where
        Repository: AdmissionRepository
            + AuditedAdmissionRepository
            + SnapshotReader
            + ApplicationCommandTransactionPort
            + RepeatableCommandBatchPort
            + ExecutionFailureTransitionPort
            + ServiceAuditAppendRepository
            + CatalogAdministrationRepository
            + QueryModuleAdministrationRepository
            + ReactiveModuleAdministrationRepository
            + CapabilityAdministrationTransactionPort
            + CapabilityBootstrapAdministrationRepository
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let evaluation_pool = CommandEvaluationPool::new(
            repository.clone(),
            CommandEvaluationPool::production_worker_count(),
        )
        .map_err(|()| CoordinatorStartError::PreparationWorkerUnavailable)?;
        Self::start_with_evaluation_pool(
            workload_capacity,
            durability,
            repository,
            conflicts,
            admission_clock,
            service_uuids,
            administration_clock,
            authorization_clock,
            provenance_source,
            notifications,
            telemetry,
            Some(evaluation_pool),
        )
    }

    /// Supported pool-less entry point: starts the coordinator with telemetry
    /// and no evaluation worker pool (same bound set as [`Self::start`], no
    /// [`Clone`] on the repository). Use when storage ports are move-only
    /// (e.g. redb operational ports) but a real commit telemetry sink is needed.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[allow(private_bounds)]
    pub fn start_with_commit_telemetry<Repository>(
        workload_capacity: CoordinatorWorkloadCapacity,
        durability: CoordinatorDurability,
        repository: Repository,
        conflicts: Arc<dyn ConflictManager>,
        admission_clock: Arc<dyn AdmissionClock>,
        service_uuids: Arc<dyn crate::ServiceUuidV7Source>,
        administration_clock: Arc<dyn AdministrationClock>,
        authorization_clock: Arc<dyn AuthorizationClock>,
        provenance_source: Arc<dyn ProvenanceIdSource>,
        notifications: Arc<dyn ApplicationCommitNotificationSink>,
        telemetry: Arc<dyn CommitTelemetry>,
    ) -> Result<Self, CoordinatorStartError>
    where
        Repository: AdmissionRepository
            + AuditedAdmissionRepository
            + SnapshotReader
            + ApplicationCommandTransactionPort
            + RepeatableCommandBatchPort
            + ExecutionFailureTransitionPort
            + ServiceAuditAppendRepository
            + CatalogAdministrationRepository
            + QueryModuleAdministrationRepository
            + ReactiveModuleAdministrationRepository
            + CapabilityAdministrationTransactionPort
            + CapabilityBootstrapAdministrationRepository
            + Send
            + 'static,
    {
        Self::start_with_evaluation_pool(
            workload_capacity,
            durability,
            repository,
            conflicts,
            admission_clock,
            service_uuids,
            administration_clock,
            authorization_clock,
            provenance_source,
            notifications,
            telemetry,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_with_evaluation_pool<Repository>(
        workload_capacity: CoordinatorWorkloadCapacity,
        durability: CoordinatorDurability,
        repository: Repository,
        conflicts: Arc<dyn ConflictManager>,
        admission_clock: Arc<dyn AdmissionClock>,
        service_uuids: Arc<dyn crate::ServiceUuidV7Source>,
        administration_clock: Arc<dyn AdministrationClock>,
        authorization_clock: Arc<dyn AuthorizationClock>,
        provenance_source: Arc<dyn ProvenanceIdSource>,
        notifications: Arc<dyn ApplicationCommitNotificationSink>,
        telemetry: Arc<dyn CommitTelemetry>,
        evaluation_pool: Option<CommandEvaluationPool>,
    ) -> Result<Self, CoordinatorStartError>
    where
        Repository: AdmissionRepository
            + AuditedAdmissionRepository
            + SnapshotReader
            + ApplicationCommandTransactionPort
            + RepeatableCommandBatchPort
            + ExecutionFailureTransitionPort
            + ServiceAuditAppendRepository
            + CatalogAdministrationRepository
            + QueryModuleAdministrationRepository
            + ReactiveModuleAdministrationRepository
            + CapabilityAdministrationTransactionPort
            + CapabilityBootstrapAdministrationRepository
            + Send
            + 'static,
    {
        let operations_telemetry = Arc::clone(&telemetry);
        Self::spawn_with_operations(
            workload_capacity,
            completion_edge_coalescing_enabled(durability),
            notifications,
            telemetry,
            move |lifecycle| {
                Box::new(ProductionCoordinatorOperations {
                    repository,
                    conflicts,
                    admission_clock,
                    service_uuids,
                    administration_clock,
                    authorization_clock,
                    provenance_source,
                    durability,
                    lifecycle,
                    telemetry: operations_telemetry,
                    evaluation_pool,
                })
            },
        )
    }

    fn spawn_with_operations(
        workload_capacity: CoordinatorWorkloadCapacity,
        completion_edge_coalescing_enabled: bool,
        notifications: Arc<dyn ApplicationCommitNotificationSink>,
        telemetry: Arc<dyn CommitTelemetry>,
        operations: impl FnOnce(ActorLifecyclePublisher) -> Box<dyn CoordinatorActorOperations>,
    ) -> Result<Self, CoordinatorStartError> {
        let channel_capacity = usize::from(workload_capacity.get()) + 1;
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let shutdown_permit = sender
            .clone()
            .try_reserve_owned()
            .map_err(|_| CoordinatorStartError::ShutdownCapacityUnavailable)?;
        let runtime = runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|_| CoordinatorStartError::RuntimeUnavailable)?;
        let lifecycle = Arc::new(AtomicU8::new(LIFECYCLE_ACCEPTING));
        let submission_gate = Arc::new(SubmissionGate::new());
        let retained_byte_capacity = Arc::new(Semaphore::new(
            MAX_QUEUED_COMMAND_BYTES / QUEUED_COMMAND_BYTE_UNIT,
        ));
        let queue_delay_estimate_micros = Arc::new(AtomicU64::new(0));
        let actor_lifecycle = Arc::clone(&lifecycle);
        let actor_submission_gate = Arc::clone(&submission_gate);
        let lifecycle_publisher = ActorLifecyclePublisher {
            lifecycle: Arc::clone(&lifecycle),
            submission_gate: Arc::clone(&submission_gate),
        };
        let writer_lifecycle = lifecycle_publisher.clone();
        let writer_operations = operations(lifecycle_publisher.clone());
        let writer_notifications = Arc::clone(&notifications);
        let writer_telemetry = Arc::clone(&telemetry);
        let writer_queue_delay = Arc::clone(&queue_delay_estimate_micros);
        // Work channel is a std mpsc so the writer can block_recv without a
        // Tokio runtime context; capacity is enforced by the intake actor only
        // sending when the writer is idle (at most one in-flight unit).
        let (work_tx, work_rx) = std::sync::mpsc::sync_channel::<WorkUnit>(1);
        let (feedback_tx, feedback_rx) = mpsc::channel::<UnitCompleted>(2);
        let writer_panicked = Arc::new(AtomicU8::new(0));
        let writer_panic_flag = Arc::clone(&writer_panicked);
        #[cfg(test)]
        let (post_dispatch_hook_tx, post_dispatch_hook_rx) =
            std::sync::mpsc::sync_channel::<Box<dyn FnOnce() + Send>>(4);
        let writer_thread = thread::Builder::new()
            .name("riffdb-command-writer".to_owned())
            .spawn(move || {
                let writer_runtime = runtime::Builder::new_current_thread()
                    .build()
                    .expect("writer runtime");
                let writer = CommandWriter {
                    operations: writer_operations,
                    notifications: writer_notifications,
                    telemetry: writer_telemetry,
                    lifecycle: writer_lifecycle,
                    queue_delay_estimate_micros: writer_queue_delay,
                    service_ewma_micros: 0,
                    enqueue_ewma_micros: 0,
                    ewma_initialized: false,
                };
                writer.run(work_rx, feedback_tx, &writer_runtime);
            })
            .map_err(|_| CoordinatorStartError::ThreadUnavailable)?;
        let actor = CommandCoordinatorActor {
            receiver,
            telemetry,
            lifecycle: lifecycle_publisher,
            feedback: feedback_rx,
            // ADR-0104 makes the Standard journal lane self-coalescing: work
            // accumulated while the prior fence is in flight should dispatch
            // immediately. The accepted two-millisecond completion-edge
            // window remains useful only for the direct-redb Sync oracle.
            completion_edge_coalescing_enabled,
            #[cfg(test)]
            post_dispatch_hooks: Some(post_dispatch_hook_rx),
        };
        let actor_thread = thread::Builder::new()
            .name("riffdb-command-coordinator".to_owned())
            .spawn(move || {
                // Drop order is reverse of declaration: WriterHandle joins the
                // writer first; only then does StoppedLifecycle publish stop so
                // a panicking actor never advertises Stopped while a write txn
                // may still be open on the writer thread.
                let _stopped = StoppedLifecycle {
                    lifecycle: actor_lifecycle,
                    submission_gate: actor_submission_gate,
                };
                let _writer = WriterHandle {
                    work_tx: Some(work_tx.clone()),
                    join: Some(writer_thread),
                    panicked: writer_panic_flag,
                };
                runtime.block_on(actor.run(work_tx, usize::from(workload_capacity.get())));
            })
            .map_err(|_| CoordinatorStartError::ThreadUnavailable)?;
        let actor_thread_id = actor_thread.thread().id();
        let accepted_submissions = Arc::new(AtomicU64::new(0));
        Ok(Self {
            executor: AdministrationAuditExecutor {
                sender: sender.clone(),
                lifecycle: Arc::clone(&lifecycle),
                submission_gate: Arc::clone(&submission_gate),
                accepted_submissions,
            },
            command_executor: CommandExecutor {
                sender: sender.clone(),
                lifecycle: Arc::clone(&lifecycle),
                submission_gate: Arc::clone(&submission_gate),
                retained_byte_capacity,
                queue_delay_estimate_micros,
            },
            control_plane_executor: ControlPlaneExecutor {
                sender: sender.clone(),
                lifecycle: Arc::clone(&lifecycle),
                submission_gate: Arc::clone(&submission_gate),
            },
            shutdown_permit: Some(shutdown_permit),
            actor_thread: Some(actor_thread),
            actor_thread_id,
            writer_panicked,
            #[cfg(test)]
            post_dispatch_hook_tx: Some(post_dispatch_hook_tx),
        })
    }

    /// Queues a closure to run on the intake actor after the next unit dispatch.
    ///
    /// Used to drive SelfJoin/Drop-on-actor-thread coverage without deadlocking
    /// the test harness.
    #[cfg(test)]
    fn queue_post_dispatch_hook(&self, hook: Box<dyn FnOnce() + Send>) {
        self.post_dispatch_hook_tx
            .as_ref()
            .expect("test hook channel")
            .send(hook)
            .expect("actor accepts post-dispatch hook");
    }

    #[cfg(test)]
    fn start_audit_only<Repository, Clock>(
        workload_capacity: CoordinatorWorkloadCapacity,
        repository: Repository,
        clock: Clock,
    ) -> Result<Self, CoordinatorStartError>
    where
        Repository: ServiceAuditAppendRepository + Send + 'static,
        Clock: AdministrationClock + 'static,
    {
        Self::spawn_with_operations(
            workload_capacity,
            false,
            Arc::new(DiscardApplicationCommitNotifications),
            Arc::new(NoopCommitTelemetry),
            move |_| Box::new(AuditOnlyCoordinatorOperations { repository, clock }),
        )
    }

    #[cfg(test)]
    fn start_audit_with_telemetry<Repository, Clock>(
        workload_capacity: CoordinatorWorkloadCapacity,
        repository: Repository,
        clock: Clock,
        telemetry: Arc<dyn CommitTelemetry>,
    ) -> Result<Self, CoordinatorStartError>
    where
        Repository: ServiceAuditAppendRepository + Send + 'static,
        Clock: AdministrationClock + 'static,
    {
        Self::spawn_with_operations(
            workload_capacity,
            false,
            Arc::new(DiscardApplicationCommitNotifications),
            telemetry,
            move |_| Box::new(AuditOnlyCoordinatorOperations { repository, clock }),
        )
    }

    /// Returns a cloneable least-authority handle to this actor's audit path.
    #[must_use]
    pub fn administration_audit_executor(&self) -> AdministrationAuditExecutor {
        self.executor.clone()
    }

    /// Returns a cloneable least-authority handle to this actor's command path.
    #[must_use]
    pub fn command_executor(&self) -> CommandExecutor {
        self.command_executor.clone()
    }

    /// Returns a cloneable least-authority handle to typed control-plane work.
    #[must_use]
    pub fn control_plane_executor(&self) -> ControlPlaneExecutor {
        self.control_plane_executor.clone()
    }

    /// Returns a cloneable handle for bounded idempotency plan selection.
    #[must_use]
    pub fn command_idempotency_inspector(
        &self,
        digest_provider: Arc<dyn IdempotencyDigestProvider>,
    ) -> CommandIdempotencyInspector {
        CommandIdempotencyInspector {
            sender: self.command_executor.sender.clone(),
            lifecycle: Arc::clone(&self.command_executor.lifecycle),
            submission_gate: Arc::clone(&self.command_executor.submission_gate),
            digest_provider,
            direct_batcher: None,
        }
    }

    /// Stops admission, drains accepted work, and joins the dedicated actor thread.
    ///
    /// A caller holding an unsubmitted workload permit must submit or drop it
    /// before this call can finish draining the closed Tokio channel.
    pub fn shutdown(mut self) -> Result<(), CoordinatorShutdownError> {
        self.initiate_shutdown();
        if thread::current().id() == self.actor_thread_id {
            self.actor_thread.take();
            return Err(CoordinatorShutdownError::SelfJoin);
        }
        let actor_thread = self
            .actor_thread
            .take()
            .expect("running coordinator owns one actor thread");
        let actor_join = actor_thread.join();
        let writer_panicked = self.writer_panicked.load(Ordering::Acquire) != 0;
        match (actor_join, writer_panicked) {
            (Ok(()), false) => Ok(()),
            _ => Err(CoordinatorShutdownError::ActorPanicked),
        }
    }

    fn initiate_shutdown(&mut self) {
        self.initiate_shutdown_after_publication(|| {});
    }

    fn initiate_shutdown_after_publication(&mut self, after_publication: impl FnOnce()) {
        let _accepting_or_already_closed = self.executor.lifecycle.compare_exchange(
            LIFECYCLE_ACCEPTING,
            LIFECYCLE_DRAINING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        after_publication();
        self.executor.submission_gate.close();
        if let Some(permit) = self.shutdown_permit.take() {
            let _sender = permit.send(CoordinatorMessage::Shutdown);
        }
    }
}

impl fmt::Debug for RunningCommandCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunningCommandCoordinator([REDACTED])")
    }
}

impl Drop for RunningCommandCoordinator {
    fn drop(&mut self) {
        self.initiate_shutdown();
        // Off-thread: join the intake actor so WriterHandle Drop joins the
        // writer before this Drop returns (blocking is acceptable — Drop is
        // rare outside tests/shutdown and must not leave a write txn open).
        // On the actor thread, joining would deadlock (same SelfJoin case
        // shutdown() already refuses); detach instead.
        if let Some(actor_thread) = self.actor_thread.take() {
            if thread::current().id() == self.actor_thread_id {
                // Detach: drop JoinHandle without join (SelfJoin path).
                // No unconditional library eprintln — detach is silent by design;
                // tests assert via post-dispatch hooks that this branch runs.
                let _ = actor_thread;
                return;
            }
            let _ = actor_thread.join();
        }
    }
}

/// One accepted administration-audit submission payload.
enum AdministrationAuditSubmission {
    Single(Box<dyn AdministrationAuditInputView>),
    FusedPair {
        started: Box<dyn AdministrationAuditInputView>,
        terminal: Box<dyn AdministrationAuditInputView>,
    },
}

enum CoordinatorMessage {
    AdministrationAudit {
        submission: AdministrationAuditSubmission,
        completion: oneshot::Sender<Result<(), AdministrationAuditExecutionError>>,
    },
    Command {
        preparation: Box<CommandExecutionPreparation>,
        command_id: riffdb_types::CommandId,
        ingress: riffdb_types::ServiceIngressKindV1,
        enqueued_at: Instant,
        completion: oneshot::Sender<Result<CommandExecutionResult, CommandExecutionError>>,
        _retained_byte_permit: OwnedSemaphorePermit,
    },
    ReadOnlyCommand {
        preparation: Box<ReadOnlyExecutionPreparation>,
        command_id: riffdb_types::CommandId,
        ingress: riffdb_types::ServiceIngressKindV1,
        enqueued_at: Instant,
        completion: oneshot::Sender<Result<ReadOnlyExecutionResult, CommandExecutionError>>,
        _retained_byte_permit: OwnedSemaphorePermit,
    },
    IdempotencyInspection {
        preparation: Box<PreparedCommandIdempotencyInspection>,
        completion:
            oneshot::Sender<Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError>>,
    },
    CatalogDeployment {
        preparation: Box<CatalogDeploymentPreparation>,
        completion: oneshot::Sender<Result<CatalogDeploymentResult, ControlPlaneExecutionError>>,
    },
    QueryModuleDeployment {
        preparation: Box<QueryModuleDeploymentPreparation>,
        completion:
            oneshot::Sender<Result<QueryModuleDeploymentResult, ControlPlaneExecutionError>>,
    },
    ReactiveModulePublication {
        preparation: Box<ReactiveModulePublicationPreparation>,
        completion: oneshot::Sender<
            Result<ReactiveModulePublicationExecutionResult, ControlPlaneExecutionError>,
        >,
    },
    CapabilityCreate {
        preparation: Box<CapabilityCreatePreparation>,
        completion:
            oneshot::Sender<Result<CapabilityCreateExecutionResult, ControlPlaneExecutionError>>,
    },
    CapabilityRevoke {
        preparation: Box<CapabilityRevokePreparation>,
        completion:
            oneshot::Sender<Result<CapabilityRevokeExecutionResult, ControlPlaneExecutionError>>,
    },
    CapabilityBootstrap {
        preparation: Box<CapabilityBootstrapPreparation>,
        completion:
            oneshot::Sender<Result<CapabilityBootstrapExecutionResult, ControlPlaneExecutionError>>,
    },
    CapabilityBootstrapTerminal {
        preparation: Box<CapabilityBootstrapTerminalPreparation>,
        completion: oneshot::Sender<Result<(), ControlPlaneExecutionError>>,
    },
    Shutdown,
}

type LocalCommandFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CommandExecutionResult, CommandExecutionError>> + 'a>>;
type LocalCommandGroupFuture<'a> = Pin<Box<dyn Future<Output = CommandGroupDriveResult> + 'a>>;
type AuditGroupItem = (
    AdministrationAuditSubmission,
    oneshot::Sender<Result<(), AdministrationAuditExecutionError>>,
);
type CommandGroupItem = (
    CommandExecutionPreparation,
    riffdb_types::CommandId,
    riffdb_types::ServiceIngressKindV1,
    Instant,
    oneshot::Sender<Result<CommandExecutionResult, CommandExecutionError>>,
);
type CommandGroupMetadata = (
    riffdb_types::CommandId,
    riffdb_types::ServiceIngressKindV1,
    Instant,
    oneshot::Sender<Result<CommandExecutionResult, CommandExecutionError>>,
);

enum SubmittedWriterUnit {
    Command {
        pending: Option<PendingCommandPublication>,
        metadata: Vec<CommandGroupMetadata>,
        footprint: Option<crate::command_execution::DeferredPipelineFootprint>,
    },
    Audit {
        submitted: Option<SubmittedAuditGroup>,
        completions: Vec<oneshot::Sender<Result<(), AdministrationAuditExecutionError>>>,
    },
}

enum PendingCommandPublication {
    Fence(crate::command_execution::SubmittedCommandGroup),
    /// A private-frontier replay or no-write result. It owns no new durability
    /// work, but may be released only after every earlier FIFO fence publishes.
    AfterPredecessor(Vec<Result<CommandExecutionResult, CommandExecutionError>>),
}

trait CoordinatorActorOperations: Send {
    fn append_audit(
        &mut self,
        input: &dyn AdministrationAuditInputView,
    ) -> Result<(), AdministrationAuditExecutionError>;

    fn append_audit_group(
        &mut self,
        inputs: &[Box<dyn AdministrationAuditInputView>],
    ) -> Vec<Result<(), AdministrationAuditExecutionError>> {
        inputs
            .iter()
            .map(|input| self.append_audit(input.as_ref()))
            .collect()
    }

    fn drive_audit_group(
        &mut self,
        inputs: &[Box<dyn AdministrationAuditInputView>],
    ) -> AuditGroupDriveResult {
        AuditGroupDriveResult::Complete(self.append_audit_group(inputs))
    }

    fn append_audit_fused_pair(
        &mut self,
        started: &dyn AdministrationAuditInputView,
        terminal: &dyn AdministrationAuditInputView,
    ) -> Result<(), AdministrationAuditExecutionError> {
        // Default: sequential prepare+append is not atomic; production overrides.
        self.append_audit(started)?;
        self.append_audit(terminal)
    }

    fn drive_command(&mut self, preparation: CommandExecutionPreparation)
    -> LocalCommandFuture<'_>;

    fn drive_command_group(
        &mut self,
        preparations: Vec<CommandExecutionPreparation>,
        evaluation_frontier: crate::command_execution::CommandEvaluationFrontier,
    ) -> LocalCommandGroupFuture<'_> {
        let _ = evaluation_frontier;
        Box::pin(async move {
            let mut results = Vec::with_capacity(preparations.len());
            for preparation in preparations {
                results.push(self.drive_command(preparation).await);
            }
            CommandGroupDriveResult::Complete(results)
        })
    }

    fn inspect_idempotency(
        &mut self,
        preparation: PreparedCommandIdempotencyInspection,
    ) -> Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError>;

    fn drive_read_only(
        &mut self,
        preparation: ReadOnlyExecutionPreparation,
    ) -> Result<ReadOnlyExecutionResult, CommandExecutionError>;

    fn deploy_catalog(
        &mut self,
        preparation: CatalogDeploymentPreparation,
    ) -> Result<CatalogDeploymentResult, ControlPlaneExecutionError>;

    fn deploy_query_module(
        &mut self,
        preparation: QueryModuleDeploymentPreparation,
    ) -> Result<QueryModuleDeploymentResult, ControlPlaneExecutionError>;

    fn publish_reactive_module(
        &mut self,
        preparation: ReactiveModulePublicationPreparation,
    ) -> Result<ReactiveModulePublicationExecutionResult, ControlPlaneExecutionError>;

    fn create_capability(
        &mut self,
        preparation: CapabilityCreatePreparation,
    ) -> Result<CapabilityCreateExecutionResult, ControlPlaneExecutionError>;

    fn revoke_capability(
        &mut self,
        preparation: CapabilityRevokePreparation,
    ) -> Result<CapabilityRevokeExecutionResult, ControlPlaneExecutionError>;

    fn bootstrap_capability(
        &mut self,
        preparation: CapabilityBootstrapPreparation,
    ) -> Result<CapabilityBootstrapExecutionResult, ControlPlaneExecutionError>;

    fn append_bootstrap_terminal(
        &mut self,
        preparation: CapabilityBootstrapTerminalPreparation,
    ) -> Result<(), ControlPlaneExecutionError>;
}

struct ProductionCoordinatorOperations<Repository> {
    repository: Repository,
    conflicts: Arc<dyn ConflictManager>,
    admission_clock: Arc<dyn AdmissionClock>,
    service_uuids: Arc<dyn crate::ServiceUuidV7Source>,
    administration_clock: Arc<dyn AdministrationClock>,
    authorization_clock: Arc<dyn AuthorizationClock>,
    provenance_source: Arc<dyn ProvenanceIdSource>,
    durability: CoordinatorDurability,
    lifecycle: ActorLifecyclePublisher,
    telemetry: Arc<dyn CommitTelemetry>,
    evaluation_pool: Option<CommandEvaluationPool>,
}

impl<Repository> CoordinatorActorOperations for ProductionCoordinatorOperations<Repository>
where
    Repository: AdmissionRepository
        + AuditedAdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + RepeatableCommandBatchPort
        + ExecutionFailureTransitionPort
        + ServiceAuditAppendRepository
        + CatalogAdministrationRepository
        + QueryModuleAdministrationRepository
        + ReactiveModuleAdministrationRepository
        + CapabilityAdministrationTransactionPort
        + CapabilityBootstrapAdministrationRepository
        + Send,
{
    fn append_audit(
        &mut self,
        input: &dyn AdministrationAuditInputView,
    ) -> Result<(), AdministrationAuditExecutionError> {
        append_administration_audit(
            &mut self.repository,
            self.administration_clock.as_ref(),
            input,
        )
    }

    fn append_audit_group(
        &mut self,
        inputs: &[Box<dyn AdministrationAuditInputView>],
    ) -> Vec<Result<(), AdministrationAuditExecutionError>> {
        append_administration_audit_group(
            &mut self.repository,
            self.administration_clock.as_ref(),
            inputs,
        )
    }

    fn drive_audit_group(
        &mut self,
        inputs: &[Box<dyn AdministrationAuditInputView>],
    ) -> AuditGroupDriveResult {
        submit_administration_audit_group(
            &mut self.repository,
            self.administration_clock.as_ref(),
            inputs,
        )
    }

    fn append_audit_fused_pair(
        &mut self,
        started: &dyn AdministrationAuditInputView,
        terminal: &dyn AdministrationAuditInputView,
    ) -> Result<(), AdministrationAuditExecutionError> {
        append_administration_audit_fused_pair(
            &mut self.repository,
            self.administration_clock.as_ref(),
            started,
            terminal,
        )
    }

    fn deploy_query_module(
        &mut self,
        preparation: QueryModuleDeploymentPreparation,
    ) -> Result<QueryModuleDeploymentResult, ControlPlaneExecutionError> {
        drive_query_module_deployment(
            &mut self.repository,
            self.administration_clock.as_ref(),
            &self.lifecycle,
            preparation,
        )
    }

    fn publish_reactive_module(
        &mut self,
        preparation: ReactiveModulePublicationPreparation,
    ) -> Result<ReactiveModulePublicationExecutionResult, ControlPlaneExecutionError> {
        drive_reactive_module_publication(
            &mut self.repository,
            self.administration_clock.as_ref(),
            &self.lifecycle,
            preparation,
        )
    }

    fn drive_command(
        &mut self,
        preparation: CommandExecutionPreparation,
    ) -> LocalCommandFuture<'_> {
        Box::pin(drive_command_execution(
            &self.repository,
            self.conflicts.as_ref(),
            self.admission_clock.as_ref(),
            self.service_uuids.as_ref(),
            self.provenance_source.as_ref(),
            self.durability,
            &self.lifecycle,
            self.telemetry.as_ref(),
            preparation,
        ))
    }

    fn drive_command_group(
        &mut self,
        preparations: Vec<CommandExecutionPreparation>,
        evaluation_frontier: crate::command_execution::CommandEvaluationFrontier,
    ) -> LocalCommandGroupFuture<'_> {
        self.repository.drive_repeatable_group(
            self.conflicts.as_ref(),
            self.admission_clock.as_ref(),
            self.service_uuids.as_ref(),
            self.administration_clock.as_ref(),
            self.provenance_source.as_ref(),
            self.durability,
            &self.lifecycle,
            self.telemetry.as_ref(),
            self.evaluation_pool.as_ref(),
            evaluation_frontier,
            preparations,
        )
    }

    fn inspect_idempotency(
        &mut self,
        preparation: PreparedCommandIdempotencyInspection,
    ) -> Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError> {
        inspect_command_idempotency(&self.repository, &self.lifecycle, preparation)
    }

    fn drive_read_only(
        &mut self,
        preparation: ReadOnlyExecutionPreparation,
    ) -> Result<ReadOnlyExecutionResult, CommandExecutionError> {
        match drive_read_only_execution(
            &self.repository,
            self.admission_clock.as_ref(),
            preparation,
        ) {
            Ok(result) => Ok(result),
            Err(error) => {
                if error.requires_readiness_stop() {
                    self.lifecycle.stop();
                }
                Err(CommandExecutionError::from_read_only(error))
            }
        }
    }

    fn deploy_catalog(
        &mut self,
        preparation: CatalogDeploymentPreparation,
    ) -> Result<CatalogDeploymentResult, ControlPlaneExecutionError> {
        drive_catalog_deployment(
            &mut self.repository,
            self.administration_clock.as_ref(),
            &self.lifecycle,
            preparation,
        )
    }

    fn create_capability(
        &mut self,
        preparation: CapabilityCreatePreparation,
    ) -> Result<CapabilityCreateExecutionResult, ControlPlaneExecutionError> {
        drive_capability_create(
            &self.repository,
            self.authorization_clock.as_ref(),
            &self.lifecycle,
            preparation,
        )
    }

    fn revoke_capability(
        &mut self,
        preparation: CapabilityRevokePreparation,
    ) -> Result<CapabilityRevokeExecutionResult, ControlPlaneExecutionError> {
        drive_capability_revoke(
            &self.repository,
            self.authorization_clock.as_ref(),
            &self.lifecycle,
            preparation,
        )
    }

    fn bootstrap_capability(
        &mut self,
        preparation: CapabilityBootstrapPreparation,
    ) -> Result<CapabilityBootstrapExecutionResult, ControlPlaneExecutionError> {
        drive_capability_bootstrap(
            &mut self.repository,
            self.administration_clock.as_ref(),
            &self.lifecycle,
            preparation,
        )
    }

    fn append_bootstrap_terminal(
        &mut self,
        preparation: CapabilityBootstrapTerminalPreparation,
    ) -> Result<(), ControlPlaneExecutionError> {
        drive_capability_bootstrap_terminal(
            &mut self.repository,
            self.administration_clock.as_ref(),
            &self.lifecycle,
            preparation,
        )
    }
}

#[cfg(test)]
struct AuditOnlyCoordinatorOperations<Repository, Clock> {
    repository: Repository,
    clock: Clock,
}

#[cfg(test)]
impl<Repository, Clock> CoordinatorActorOperations
    for AuditOnlyCoordinatorOperations<Repository, Clock>
where
    Repository: ServiceAuditAppendRepository + Send,
    Clock: AdministrationClock,
{
    fn append_audit(
        &mut self,
        input: &dyn AdministrationAuditInputView,
    ) -> Result<(), AdministrationAuditExecutionError> {
        append_administration_audit(&mut self.repository, &self.clock, input)
    }

    fn append_audit_group(
        &mut self,
        inputs: &[Box<dyn AdministrationAuditInputView>],
    ) -> Vec<Result<(), AdministrationAuditExecutionError>> {
        append_administration_audit_group(&mut self.repository, &self.clock, inputs)
    }

    fn drive_command(&mut self, _: CommandExecutionPreparation) -> LocalCommandFuture<'_> {
        Box::pin(async { Err(CommandExecutionError::coordinator_stopped()) })
    }

    fn inspect_idempotency(
        &mut self,
        _: PreparedCommandIdempotencyInspection,
    ) -> Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError> {
        Err(CommandIdempotencyInspectionError::coordinator_stopped())
    }

    fn drive_read_only(
        &mut self,
        _: ReadOnlyExecutionPreparation,
    ) -> Result<ReadOnlyExecutionResult, CommandExecutionError> {
        Err(CommandExecutionError::coordinator_stopped())
    }

    fn deploy_catalog(
        &mut self,
        _: CatalogDeploymentPreparation,
    ) -> Result<CatalogDeploymentResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn deploy_query_module(
        &mut self,
        _: QueryModuleDeploymentPreparation,
    ) -> Result<QueryModuleDeploymentResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn publish_reactive_module(
        &mut self,
        _: ReactiveModulePublicationPreparation,
    ) -> Result<ReactiveModulePublicationExecutionResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn create_capability(
        &mut self,
        _: CapabilityCreatePreparation,
    ) -> Result<CapabilityCreateExecutionResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn revoke_capability(
        &mut self,
        _: CapabilityRevokePreparation,
    ) -> Result<CapabilityRevokeExecutionResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn bootstrap_capability(
        &mut self,
        _: CapabilityBootstrapPreparation,
    ) -> Result<CapabilityBootstrapExecutionResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn append_bootstrap_terminal(
        &mut self,
        _: CapabilityBootstrapTerminalPreparation,
    ) -> Result<(), ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }
}

#[derive(Clone)]
struct ActorLifecyclePublisher {
    lifecycle: Arc<AtomicU8>,
    submission_gate: Arc<SubmissionGate>,
}

impl CommandExecutionLifecycle for ActorLifecyclePublisher {
    fn fence(&self) {
        self.lifecycle.store(LIFECYCLE_FENCED, Ordering::Release);
        self.submission_gate.close();
    }

    fn stop(&self) {
        let mut observed = self.lifecycle.load(Ordering::Acquire);
        while observed != LIFECYCLE_FENCED && observed != LIFECYCLE_STOPPED {
            match self.lifecycle.compare_exchange_weak(
                observed,
                LIFECYCLE_STOPPED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => observed = current,
            }
        }
        self.submission_gate.close();
    }
}

#[cfg(test)]
struct DiscardApplicationCommitNotifications;

#[cfg(test)]
impl ApplicationCommitNotificationSink for DiscardApplicationCommitNotifications {
    fn publish_first_commit(
        &self,
        _: riffdb_types::CommitSequence,
    ) -> Result<(), crate::ApplicationCommitNotificationError> {
        Ok(())
    }
}

/// ADR-0060's sub-millisecond MAY-window is subsumed by event-driven
/// in-flight-commit formation. ADR-0098 additionally permits one real
/// two-millisecond deadline at a contended completion edge.
enum WorkUnit {
    CommandGroup(Vec<CommandGroupItem>),
    AuditGroup(Vec<AuditGroupItem>),
    Single(CoordinatorMessage),
    Shutdown,
}

struct UnitCompleted;

struct WriterHandle {
    work_tx: Option<std::sync::mpsc::SyncSender<WorkUnit>>,
    join: Option<thread::JoinHandle<()>>,
    panicked: Arc<AtomicU8>,
}

impl Drop for WriterHandle {
    fn drop(&mut self) {
        drop(self.work_tx.take());
        if let Some(join) = self.join.take()
            && join.join().is_err()
        {
            self.panicked.store(1, Ordering::Release);
        }
        #[cfg(test)]
        {
            let seq = TEST_DROP_SEQ.fetch_add(1, Ordering::AcqRel) + 1;
            TEST_WRITER_HANDLE_DROP_END_SEQ.store(seq, Ordering::Release);
        }
    }
}

struct CommandCoordinatorActor {
    receiver: mpsc::Receiver<CoordinatorMessage>,
    telemetry: Arc<dyn CommitTelemetry>,
    lifecycle: ActorLifecyclePublisher,
    feedback: mpsc::Receiver<UnitCompleted>,
    completion_edge_coalescing_enabled: bool,
    #[cfg(test)]
    post_dispatch_hooks: Option<std::sync::mpsc::Receiver<Box<dyn FnOnce() + Send>>>,
}

fn command_prefix_can_grow(pending: &VecDeque<CoordinatorMessage>) -> bool {
    if !matches!(
        pending.front().map(command_grouping_class),
        Some(CommandGroupingClass::Command)
    ) {
        return false;
    }
    let mut selected = 0_usize;
    for message in pending {
        match command_grouping_class(message) {
            CommandGroupingClass::Command => {
                selected += 1;
                if selected >= riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
                    return false;
                }
            }
            CommandGroupingClass::DeferrableObservation => {}
            CommandGroupingClass::Barrier => return false,
        }
    }
    true
}

fn post_commit_command_window_eligible(pending: &VecDeque<CoordinatorMessage>) -> bool {
    if !command_prefix_can_grow(pending) {
        return false;
    }
    let commands = pending
        .iter()
        .filter(|message| {
            matches!(
                command_grouping_class(message),
                CommandGroupingClass::Command
            )
        })
        .take(riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
        .count();
    (2..riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS).contains(&commands)
}

fn collect_until_group_deadline(
    receiver: &mut mpsc::Receiver<CoordinatorMessage>,
    pending: &mut VecDeque<CoordinatorMessage>,
    workload_capacity: usize,
    deadline: Instant,
    shutting_down: &mut bool,
) {
    while pending.len() < workload_capacity
        && command_prefix_can_grow(pending)
        && Instant::now() < deadline
    {
        match receiver.try_recv() {
            Ok(message) => pending.push_back(message),
            Err(mpsc::error::TryRecvError::Empty) => std::hint::spin_loop(),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                *shutting_down = true;
                return;
            }
        }
    }
}

async fn collect_until_coalesce_deadline(
    receiver: &mut mpsc::Receiver<CoordinatorMessage>,
    pending: &mut VecDeque<CoordinatorMessage>,
    workload_capacity: usize,
    deadline: Instant,
    shutting_down: &mut bool,
) {
    let park_deadline = deadline
        .checked_sub(POST_COMMIT_COALESCE_TIMER_GUARD)
        .unwrap_or(deadline);
    while pending.len() < workload_capacity
        && command_prefix_can_grow(pending)
        && Instant::now() < deadline
    {
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(park_deadline)) => return,
            message = receiver.recv() => {
                match message {
                    Some(message) => pending.push_back(message),
                    None => {
                        *shutting_down = true;
                        return;
                    }
                }
            }
        }
    }
}

impl CommandCoordinatorActor {
    async fn run(
        mut self,
        work_tx: std::sync::mpsc::SyncSender<WorkUnit>,
        workload_capacity: usize,
    ) {
        let mut pending = VecDeque::new();
        let mut writer_busy = false;
        let mut shutting_down = false;
        let mut formation_anchor = Instant::now();
        let mut formation_deadline = None;
        let mut completion_edge_coalescing = false;
        loop {
            if !writer_busy {
                // Formation edge: drain ready channel messages into pending while
                // under admission capacity, then honor the oldest command's real
                // 200-microsecond deadline with a timer-free bounded poll. Intake
                // while the writer is busy normally consumes that deadline before
                // the prior commit completes; only a genuinely fresh formation
                // waits here.
                //
                // Accepted backlog under a *blocked* writer is 2C+1: pending ≤ C
                // is enforced by the busy-path select arm (`pending.len() <
                // workload_capacity`), plus channel capacity C+1. This idle
                // try_recv bound is defense-in-depth for an idle burst larger
                // than C already sitting in the channel (also keeps pending ≤ C
                // after feedback when the channel was full).
                //
                // Pipelining-removal neuter (pre-split): disable the busy-path
                // select arm *and* this multi-message drain — arm-only is not
                // enough because this drain would re-batch at the completion edge.
                let pending_was_empty = pending.is_empty();
                let receiver_closed = loop {
                    // Defense-in-depth: cap pending on idle drain sweeps too.
                    if pending.len() >= workload_capacity {
                        break false;
                    }
                    match self.receiver.try_recv() {
                        Ok(message) => pending.push_back(message),
                        Err(mpsc::error::TryRecvError::Empty) => break false,
                        Err(mpsc::error::TryRecvError::Disconnected) => break true,
                    }
                };
                if receiver_closed {
                    shutting_down = true;
                }
                // Collection-time semantics: reset formation anchor on the first
                // arrival after an idle park (not only on writer feedback).
                if pending_was_empty && !pending.is_empty() {
                    formation_anchor = Instant::now();
                    formation_deadline = Some(
                        formation_anchor
                            .checked_add(OLDEST_GROUPABLE_TRANSITION_MAX_AGE)
                            .unwrap_or(formation_anchor),
                    );
                }
                if !shutting_down && let Some(deadline) = formation_deadline {
                    if completion_edge_coalescing {
                        collect_until_coalesce_deadline(
                            &mut self.receiver,
                            &mut pending,
                            workload_capacity,
                            deadline,
                            &mut shutting_down,
                        )
                        .await;
                    } else {
                        collect_until_group_deadline(
                            &mut self.receiver,
                            &mut pending,
                            workload_capacity,
                            deadline,
                            &mut shutting_down,
                        );
                    }
                }

                if let Some((unit, reason)) = form_next_unit(&mut pending) {
                    let reason = if matches!(reason, CommitGroupDispatchReason::QueueDrained)
                        && shutting_down
                    {
                        CommitGroupDispatchReason::ReceiverClosed
                    } else {
                        reason
                    };
                    let elapsed = formation_anchor.elapsed();
                    match &unit {
                        WorkUnit::Shutdown => {
                            self.receiver.close();
                            while let Some(message) = self.receiver.recv().await {
                                pending.push_back(message);
                            }
                            shutting_down = true;
                            continue;
                        }
                        WorkUnit::CommandGroup(group) => {
                            self.telemetry
                                .record(CommitTelemetryEvent::CommandGroupDispatched {
                                    reason,
                                    selected: u16::try_from(group.len()).unwrap_or(u16::MAX),
                                    deferred: u16::try_from(pending.len()).unwrap_or(u16::MAX),
                                    elapsed,
                                });
                        }
                        WorkUnit::AuditGroup(group) => {
                            self.telemetry
                                .record(CommitTelemetryEvent::CommandGroupDispatched {
                                    reason,
                                    selected: u16::try_from(group.len()).unwrap_or(u16::MAX),
                                    deferred: u16::try_from(pending.len()).unwrap_or(u16::MAX),
                                    elapsed,
                                });
                        }
                        WorkUnit::Single(_) => {}
                    }
                    work_tx
                        .try_send(unit)
                        .expect("writer idle: work channel must accept the formed unit");
                    writer_busy = true;
                    // Anything left was already available behind a count bound,
                    // duplicate boundary, or hard barrier. It is not granted a
                    // fresh delay when it reaches the front.
                    formation_deadline = None;
                    completion_edge_coalescing = false;
                    #[cfg(test)]
                    {
                        let target = TEST_PANIC_LIFECYCLE.load(Ordering::Acquire);
                        if !target.is_null()
                            && std::ptr::eq(Arc::as_ptr(&self.lifecycle.lifecycle), target)
                        {
                            TEST_PANIC_LIFECYCLE.store(std::ptr::null_mut(), Ordering::Release);
                            panic!("test: intentional actor panic after dispatch");
                        }
                        if let Some(hooks) = self.post_dispatch_hooks.as_ref() {
                            while let Ok(hook) = hooks.try_recv() {
                                hook();
                            }
                        }
                    }
                    continue;
                }
                if shutting_down && pending.is_empty() {
                    // Return so `work_tx` drops; WriterHandle then drops its
                    // clone and joins the writer.
                    return;
                }
            }

            if writer_busy {
                // While the writer holds unit N, accept at most one message when
                // pending is under capacity — further arrivals remain in the
                // bounded channel so reserve_capacity parks. Formation of the
                // next group uses the pending collected during this window at
                // the completion edge (see `if !writer_busy` above).
                tokio::select! {
                    biased;
                    completed = self.feedback.recv() => {
                        match completed {
                            Some(UnitCompleted) => {
                                writer_busy = false;
                                if matches!(
                                    lifecycle_state(&self.lifecycle.lifecycle),
                                    CoordinatorLifecycleState::Fenced
                                        | CoordinatorLifecycleState::Stopped
                                ) {
                                    // Older deferred work first, then channel.
                                    self.reject_pending(&mut pending);
                                    if lifecycle_state(&self.lifecycle.lifecycle)
                                        == CoordinatorLifecycleState::Fenced
                                    {
                                        self.reject_remaining_after_fence().await;
                                    } else {
                                        self.reject_remaining_after_stop().await;
                                    }
                                    return;
                                }
                                if self.completion_edge_coalescing_enabled
                                    && post_commit_command_window_eligible(&pending)
                                {
                                    formation_anchor = Instant::now();
                                    completion_edge_coalescing = true;
                                    formation_deadline = Some(
                                        formation_anchor
                                            .checked_add(POST_COMMIT_COALESCE_BUDGET)
                                            .unwrap_or(formation_anchor),
                                    );
                                }
                            }
                            None => {
                                self.lifecycle.stop();
                                self.reject_pending(&mut pending);
                                self.reject_remaining_after_stop().await;
                                return;
                            }
                        }
                    }
                    message = self.receiver.recv(), if !shutting_down
                        && pending.len() < workload_capacity =>
                    {
                        match message {
                            Some(message) => {
                                // First message of a new collection after idle.
                                if pending.is_empty() {
                                    formation_anchor = Instant::now();
                                    completion_edge_coalescing = false;
                                    formation_deadline = Some(
                                        formation_anchor
                                            .checked_add(OLDEST_GROUPABLE_TRANSITION_MAX_AGE)
                                            .unwrap_or(formation_anchor),
                                    );
                                }
                                pending.push_back(message);
                            }
                            None => shutting_down = true,
                        }
                    }
                }
            } else if !shutting_down {
                match self.receiver.recv().await {
                    Some(message) => {
                        formation_anchor = Instant::now();
                        completion_edge_coalescing = false;
                        formation_deadline = Some(
                            formation_anchor
                                .checked_add(OLDEST_GROUPABLE_TRANSITION_MAX_AGE)
                                .unwrap_or(formation_anchor),
                        );
                        pending.push_back(message);
                    }
                    None => shutting_down = true,
                }
            } else {
                return;
            }
        }
    }

    fn reject_pending(&self, pending: &mut VecDeque<CoordinatorMessage>) {
        while let Some(message) = pending.pop_front() {
            reject_message_stopped_or_fenced(message, &self.lifecycle);
        }
    }

    async fn reject_remaining_after_fence(&mut self) {
        self.drain_reject(reject_message_fenced).await;
    }

    async fn reject_remaining_after_stop(&mut self) {
        self.drain_reject(reject_message_stopped).await;
    }

    async fn drain_reject(&mut self, reject: fn(CoordinatorMessage)) {
        // Close first so recv terminates after draining residual accepted work.
        self.receiver.close();
        loop {
            match self.receiver.try_recv() {
                Ok(message) => reject(message),
                Err(mpsc::error::TryRecvError::Empty) => {
                    // May still race with a concurrent send; park once more.
                    match self.receiver.recv().await {
                        Some(message) => reject(message),
                        None => return,
                    }
                }
                Err(mpsc::error::TryRecvError::Disconnected) => return,
            }
        }
    }
}

struct CommandWriter {
    operations: Box<dyn CoordinatorActorOperations>,
    notifications: Arc<dyn ApplicationCommitNotificationSink>,
    telemetry: Arc<dyn CommitTelemetry>,
    lifecycle: ActorLifecyclePublisher,
    queue_delay_estimate_micros: Arc<AtomicU64>,
    service_ewma_micros: u64,
    enqueue_ewma_micros: u64,
    ewma_initialized: bool,
}

#[derive(Clone, Copy)]
struct InFlightWriterUnit {
    transition_count: usize,
    has_command_pipeline_proof: bool,
}

struct CompletionOwner {
    notifications: Arc<dyn ApplicationCommitNotificationSink>,
    telemetry: Arc<dyn CommitTelemetry>,
    lifecycle: ActorLifecyclePublisher,
}

struct SubmittedCompletionUnit {
    unit: SubmittedWriterUnit,
    submitted_at: Instant,
    submitted_depth: u16,
}

struct CompletionPublished;

impl SubmittedWriterUnit {
    fn transition_count(&self) -> usize {
        match self {
            Self::Command {
                pending: Some(PendingCommandPublication::Fence(_)),
                metadata,
                ..
            } => metadata.len(),
            Self::Command {
                pending: Some(PendingCommandPublication::AfterPredecessor(_)) | None,
                ..
            } => 0,
            Self::Audit { completions, .. } => completions.len(),
        }
    }

    fn requires_pipeline_drain(&self) -> bool {
        match self {
            Self::Command {
                pending: Some(PendingCommandPublication::Fence(submitted)),
                ..
            } => submitted.requires_pipeline_drain(),
            Self::Command {
                pending: Some(PendingCommandPublication::AfterPredecessor(_)) | None,
                ..
            } => false,
            Self::Audit { .. } => false,
        }
    }

    fn has_command_pipeline_proof(&self) -> bool {
        matches!(
            self,
            Self::Command {
                footprint: Some(_),
                ..
            }
        )
    }

    fn finish(self, owner: &CompletionOwner) {
        match self {
            Self::Command {
                pending, metadata, ..
            } => {
                let Some(publication) = pending else {
                    owner.lifecycle.stop();
                    return;
                };
                let results = match publication {
                    PendingCommandPublication::Fence(submitted) => {
                        submitted.wait(&owner.lifecycle, owner.telemetry.as_ref())
                    }
                    PendingCommandPublication::AfterPredecessor(results) => results,
                };
                CommandWriter::finish_command_group_with(
                    owner.notifications.as_ref(),
                    owner.telemetry.as_ref(),
                    &owner.lifecycle,
                    metadata,
                    results,
                );
            }
            Self::Audit {
                submitted,
                completions,
            } => {
                let Some(submitted) = submitted else {
                    owner.lifecycle.stop();
                    return;
                };
                let results = submitted.wait();
                CommandWriter::finish_audit_group_with(&owner.lifecycle, completions, results);
            }
        }
    }
}

impl CompletionOwner {
    fn run(
        self,
        receiver: std::sync::mpsc::Receiver<SubmittedCompletionUnit>,
        published: std::sync::mpsc::SyncSender<CompletionPublished>,
    ) {
        while let Ok(submitted) = receiver.recv() {
            submitted.unit.finish(&self);
            self.telemetry
                .record(CommitTelemetryEvent::CompletionLaneObserved {
                    phase: CompletionLanePhase::Published,
                    depth: submitted.submitted_depth,
                    reorder_occupancy: 0,
                    elapsed: submitted.submitted_at.elapsed(),
                });
            if published.send(CompletionPublished).is_err() {
                self.lifecycle.stop();
                return;
            }
        }
    }
}

fn accept_published_completion(
    in_flight: &mut VecDeque<InFlightWriterUnit>,
    journal_suffix_transitions: &mut usize,
    lifecycle: &ActorLifecyclePublisher,
) {
    let Some(completed) = in_flight.pop_front() else {
        lifecycle.stop();
        return;
    };
    if completed.transition_count > *journal_suffix_transitions {
        lifecycle.stop();
    }
    if in_flight.is_empty() {
        // The next writer unit begins after every prior fence is published;
        // storage can checkpoint the complete suffix before opening its
        // transaction.
        *journal_suffix_transitions = 0;
    }
}

fn drain_ready_completions(
    published: &std::sync::mpsc::Receiver<CompletionPublished>,
    in_flight: &mut VecDeque<InFlightWriterUnit>,
    journal_suffix_transitions: &mut usize,
    lifecycle: &ActorLifecyclePublisher,
) {
    loop {
        match published.try_recv() {
            Ok(CompletionPublished) => {
                accept_published_completion(in_flight, journal_suffix_transitions, lifecycle)
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => return,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                if !in_flight.is_empty() {
                    lifecycle.stop();
                    in_flight.clear();
                    *journal_suffix_transitions = 0;
                }
                return;
            }
        }
    }
}

fn drain_one_completion(
    published: &std::sync::mpsc::Receiver<CompletionPublished>,
    in_flight: &mut VecDeque<InFlightWriterUnit>,
    journal_suffix_transitions: &mut usize,
    lifecycle: &ActorLifecyclePublisher,
) {
    if in_flight.is_empty() {
        return;
    }
    match published.recv() {
        Ok(CompletionPublished) => {
            accept_published_completion(in_flight, journal_suffix_transitions, lifecycle);
        }
        Err(_) => {
            lifecycle.stop();
            in_flight.clear();
            *journal_suffix_transitions = 0;
        }
    }
}

fn drain_all_completions(
    published: &std::sync::mpsc::Receiver<CompletionPublished>,
    in_flight: &mut VecDeque<InFlightWriterUnit>,
    journal_suffix_transitions: &mut usize,
    lifecycle: &ActorLifecyclePublisher,
) -> Duration {
    let started = Instant::now();
    while !in_flight.is_empty() {
        drain_one_completion(published, in_flight, journal_suffix_transitions, lifecycle);
    }
    started.elapsed()
}

impl CommandWriter {
    fn run(
        mut self,
        work_rx: std::sync::mpsc::Receiver<WorkUnit>,
        feedback_tx: mpsc::Sender<UnitCompleted>,
        runtime: &runtime::Runtime,
    ) {
        const COMPLETION_LANE_CAPACITY: usize = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
        let (completion_tx, completion_rx) =
            std::sync::mpsc::sync_channel::<SubmittedCompletionUnit>(COMPLETION_LANE_CAPACITY);
        let (published_tx, published_rx) =
            std::sync::mpsc::sync_channel::<CompletionPublished>(COMPLETION_LANE_CAPACITY);
        let completion_owner = CompletionOwner {
            notifications: Arc::clone(&self.notifications),
            telemetry: Arc::clone(&self.telemetry),
            lifecycle: self.lifecycle.clone(),
        };
        let completion_lifecycle = self.lifecycle.clone();
        let completion_thread = match thread::Builder::new()
            .name("riffdb-command-completion".to_owned())
            .spawn(move || completion_owner.run(completion_rx, published_tx))
        {
            Ok(join) => join,
            Err(_) => {
                completion_lifecycle.stop();
                panic!("command completion thread unavailable");
            }
        };
        let mut last_edge = Instant::now();
        let mut in_flight = VecDeque::<InFlightWriterUnit>::new();
        // Complete journal suffix since the last point at which the writer
        // drained every fence and the next storage begin can checkpoint it.
        // Published frames remain in that suffix until checkpoint, so counting
        // only `submitted` would permit a continuously fed lane to exceed the
        // bounded recovery suffix even as old receipts are published.
        let mut journal_suffix_transitions = 0_usize;
        loop {
            // One census iteration spans the complete loop body, so the two
            // stretches the `busy`/`idle` counters miss -- the pre-execution
            // blocking drain and every post-submission step -- are named.
            crate::writer_census::begin_iteration();
            let iteration_started = crate::writer_census::stage_start();
            let drain_ready_started = crate::writer_census::stage_start();
            drain_ready_completions(
                &published_rx,
                &mut in_flight,
                &mut journal_suffix_transitions,
                &self.lifecycle,
            );
            crate::writer_census::charge(
                crate::writer_census::LOOP_DRAIN_READY,
                drain_ready_started,
            );
            let recv_started = crate::writer_census::stage_start();
            let unit = match work_rx.recv() {
                Ok(unit) => unit,
                Err(_) => break,
            };
            crate::writer_census::charge(crate::writer_census::LOOP_WORK_RECV, recv_started);
            let admit_gate_started = crate::writer_census::stage_start();
            let pipeline_transitions = match &unit {
                WorkUnit::CommandGroup(group) => group.len(),
                WorkUnit::AuditGroup(group) => group.len(),
                WorkUnit::Single(_) | WorkUnit::Shutdown => 0,
            };
            let command_pipeline_footprint = match &unit {
                WorkUnit::CommandGroup(group) => Some(
                    crate::command_execution::command_group_deferred_pipeline_footprint(
                        group.iter().map(|(preparation, ..)| preparation),
                    ),
                ),
                WorkUnit::AuditGroup(_) | WorkUnit::Single(_) | WorkUnit::Shutdown => None,
            };
            let (command_deferred_eligible, mut command_evaluation_frontier) =
                if in_flight.is_empty() {
                    (
                        true,
                        crate::command_execution::CommandEvaluationFrontier::Published,
                    )
                } else {
                    match &unit {
                        WorkUnit::CommandGroup(_) => (
                            command_pipeline_footprint
                                .as_ref()
                                .and_then(Option::as_ref)
                                .is_some()
                                && in_flight.iter().all(|unit| unit.has_command_pipeline_proof),
                            crate::command_execution::CommandEvaluationFrontier::WriterPrivate,
                        ),
                        WorkUnit::AuditGroup(_) => (
                            true,
                            crate::command_execution::CommandEvaluationFrontier::Published,
                        ),
                        WorkUnit::Single(_) | WorkUnit::Shutdown => (
                            false,
                            crate::command_execution::CommandEvaluationFrontier::Published,
                        ),
                    }
                };
            // Each submitted frame retains one exact redb read root until its
            // fence publishes. Stay below redb's finite live-read slot pool so
            // a saturated writer never blocks trying to capture the next
            // private successor before it can drain the oldest fence.
            const WRITER_PIPELINE_DRAIN_TRANSITIONS: usize =
                riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
            crate::writer_census::charge(
                crate::writer_census::LOOP_ADMIT_GATE,
                admit_gate_started,
            );
            if pipeline_transitions == 0
                || !command_deferred_eligible
                || journal_suffix_transitions.saturating_add(pipeline_transitions)
                    > WRITER_PIPELINE_DRAIN_TRANSITIONS
            {
                let drain_elapsed = drain_all_completions(
                    &published_rx,
                    &mut in_flight,
                    &mut journal_suffix_transitions,
                    &self.lifecycle,
                );
                self.telemetry
                    .record(CommitTelemetryEvent::CompletionLaneObserved {
                        phase: CompletionLanePhase::Drained,
                        depth: 0,
                        reorder_occupancy: 0,
                        elapsed: drain_elapsed,
                    });
                crate::writer_census::charge_nanos(
                    crate::writer_census::LOOP_DRAIN_ALL_PRE,
                    u64::try_from(drain_elapsed.as_nanos()).unwrap_or(u64::MAX),
                );
                command_evaluation_frontier =
                    crate::command_execution::CommandEvaluationFrontier::Published;
            }
            let idle = last_edge.elapsed();
            let busy_started = Instant::now();
            let unit_enqueued_hint = unit_enqueue_hint(&unit);
            let mut deferred = None;
            // Fence/stop takes effect between units: reject without storage access.
            if matches!(
                lifecycle_state(&self.lifecycle.lifecycle),
                CoordinatorLifecycleState::Fenced | CoordinatorLifecycleState::Stopped
            ) {
                reject_work_unit(unit, &self.lifecycle);
            } else {
                let _panic_guard = ActorMessagePanicGuard::new(self.lifecycle.clone());
                match unit {
                    WorkUnit::CommandGroup(group) => {
                        deferred = runtime.block_on(self.execute_command_group(
                            group,
                            command_pipeline_footprint.flatten(),
                            command_evaluation_frontier,
                        ));
                    }
                    WorkUnit::AuditGroup(group) => {
                        deferred = self.execute_audit_group(group);
                    }
                    WorkUnit::Single(message) => {
                        runtime.block_on(self.execute_single(message));
                    }
                    WorkUnit::Shutdown => {}
                }
            }
            let busy = busy_started.elapsed();
            crate::writer_census::charge_nanos(
                crate::writer_census::UNIT_EXECUTE,
                u64::try_from(busy.as_nanos()).unwrap_or(u64::MAX),
            );
            let post_submit_started = crate::writer_census::stage_start();
            let submitted_unit = deferred.is_some();
            let queue_delay_estimate_micros = self.observe_ewma(unit_enqueued_hint, busy);
            self.telemetry
                .record(CommitTelemetryEvent::WriterUnitCompleted {
                    busy,
                    idle,
                    queue_delay_estimate_micros,
                });
            if let Some(deferred) = deferred {
                if in_flight.len() >= COMPLETION_LANE_CAPACITY {
                    drain_one_completion(
                        &published_rx,
                        &mut in_flight,
                        &mut journal_suffix_transitions,
                        &self.lifecycle,
                    );
                }
                journal_suffix_transitions =
                    journal_suffix_transitions.saturating_add(deferred.transition_count());
                let requires_pipeline_drain = deferred.requires_pipeline_drain();
                let footprint = InFlightWriterUnit {
                    transition_count: deferred.transition_count(),
                    has_command_pipeline_proof: deferred.has_command_pipeline_proof(),
                };
                let submitted_depth =
                    u16::try_from(in_flight.len().saturating_add(1)).unwrap_or(u16::MAX);
                let submitted = SubmittedCompletionUnit {
                    unit: deferred,
                    submitted_at: Instant::now(),
                    submitted_depth,
                };
                if completion_tx.send(submitted).is_err() {
                    self.lifecycle.stop();
                } else {
                    in_flight.push_back(footprint);
                    self.telemetry
                        .record(CommitTelemetryEvent::CompletionLaneObserved {
                            phase: CompletionLanePhase::Submitted,
                            depth: submitted_depth,
                            reorder_occupancy: 0,
                            elapsed: Duration::ZERO,
                        });
                }
                if requires_pipeline_drain {
                    crate::writer_census::charge(
                        crate::writer_census::POST_SUBMIT_ENQUEUE,
                        post_submit_started,
                    );
                    let drain_elapsed = drain_all_completions(
                        &published_rx,
                        &mut in_flight,
                        &mut journal_suffix_transitions,
                        &self.lifecycle,
                    );
                    self.telemetry
                        .record(CommitTelemetryEvent::CompletionLaneObserved {
                            phase: CompletionLanePhase::Drained,
                            depth: 0,
                            reorder_occupancy: 0,
                            elapsed: drain_elapsed,
                        });
                    crate::writer_census::charge_nanos(
                        crate::writer_census::POST_DRAIN_ALL,
                        u64::try_from(drain_elapsed.as_nanos()).unwrap_or(u64::MAX),
                    );
                } else {
                    crate::writer_census::charge(
                        crate::writer_census::POST_SUBMIT_ENQUEUE,
                        post_submit_started,
                    );
                }
            } else {
                crate::writer_census::charge(
                    crate::writer_census::POST_SUBMIT_ENQUEUE,
                    post_submit_started,
                );
            }
            // Capacity 2 with <=1 outstanding unit: send always succeeds while
            // the intake actor is alive. During actor-panic unwind, avoid a
            // second panic from expect on a closed channel.
            let feedback_started = crate::writer_census::stage_start();
            if !std::thread::panicking() {
                feedback_tx
                    .try_send(UnitCompleted)
                    .expect("intake actor must accept writer feedback");
            } else {
                let _ = feedback_tx.try_send(UnitCompleted);
            }
            crate::writer_census::charge(crate::writer_census::POST_FEEDBACK, feedback_started);
            crate::writer_census::end_iteration(
                iteration_started.map_or(0, |started| {
                    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
                }),
                submitted_unit,
            );
            last_edge = Instant::now();
        }
        drop(completion_tx);
        let shutdown_started = Instant::now();
        if let Err(payload) = completion_thread.join() {
            self.lifecycle.stop();
            panic::resume_unwind(payload);
        }
        self.telemetry
            .record(CommitTelemetryEvent::CompletionLaneObserved {
                phase: CompletionLanePhase::Shutdown,
                depth: 0,
                reorder_occupancy: 0,
                elapsed: shutdown_started.elapsed(),
            });
    }

    fn observe_ewma(&mut self, enqueue_hint: Option<Instant>, service: Duration) -> u64 {
        let service_us = duration_micros(service);
        let enqueue_us = enqueue_hint
            .map(|started| duration_micros(started.elapsed().saturating_sub(service)))
            .unwrap_or(0);
        const ALPHA_NUM: u64 = 1;
        const ALPHA_DEN: u64 = 5; // α ≈ 0.2
        if !self.ewma_initialized {
            self.service_ewma_micros = service_us;
            self.enqueue_ewma_micros = enqueue_us;
            self.ewma_initialized = true;
        } else {
            self.service_ewma_micros =
                ewma(self.service_ewma_micros, service_us, ALPHA_NUM, ALPHA_DEN);
            self.enqueue_ewma_micros =
                ewma(self.enqueue_ewma_micros, enqueue_us, ALPHA_NUM, ALPHA_DEN);
        }
        let estimate = self
            .service_ewma_micros
            .saturating_add(self.enqueue_ewma_micros);
        self.queue_delay_estimate_micros
            .store(estimate, Ordering::Relaxed);
        estimate
    }

    async fn execute_single(&mut self, message: CoordinatorMessage) {
        match message {
            CoordinatorMessage::AdministrationAudit {
                submission,
                completion,
            } => match submission {
                AdministrationAuditSubmission::Single(input) => {
                    self.execute_audit(input, completion);
                }
                AdministrationAuditSubmission::FusedPair { started, terminal } => {
                    self.execute_audit_group(vec![(
                        AdministrationAuditSubmission::FusedPair { started, terminal },
                        completion,
                    )]);
                }
            },
            CoordinatorMessage::Command {
                preparation,
                command_id,
                ingress,
                enqueued_at,
                completion,
                ..
            } => {
                self.execute_command(*preparation, command_id, ingress, enqueued_at, completion)
                    .await;
            }
            CoordinatorMessage::ReadOnlyCommand {
                preparation,
                command_id,
                ingress,
                enqueued_at,
                completion,
                ..
            } => {
                self.execute_read_only(*preparation, command_id, ingress, enqueued_at, completion);
            }
            CoordinatorMessage::IdempotencyInspection {
                preparation,
                completion,
            } => {
                self.execute_idempotency_inspection(*preparation, completion);
            }
            CoordinatorMessage::CatalogDeployment {
                preparation,
                completion,
            } => {
                self.execute_catalog_deployment(*preparation, completion);
            }
            CoordinatorMessage::QueryModuleDeployment {
                preparation,
                completion,
            } => {
                self.execute_query_module_deployment(*preparation, completion);
            }
            CoordinatorMessage::ReactiveModulePublication {
                preparation,
                completion,
            } => {
                self.execute_reactive_module_publication(*preparation, completion);
            }
            CoordinatorMessage::CapabilityCreate {
                preparation,
                completion,
            } => {
                self.execute_capability_create(*preparation, completion);
            }
            CoordinatorMessage::CapabilityRevoke {
                preparation,
                completion,
            } => {
                self.execute_capability_revoke(*preparation, completion);
            }
            CoordinatorMessage::CapabilityBootstrap {
                preparation,
                completion,
            } => {
                self.execute_capability_bootstrap(*preparation, completion);
            }
            CoordinatorMessage::CapabilityBootstrapTerminal {
                preparation,
                completion,
            } => {
                self.execute_capability_bootstrap_terminal(*preparation, completion);
            }
            CoordinatorMessage::Shutdown => {}
        }
    }

    fn execute_audit(
        &mut self,
        input: Box<dyn AdministrationAuditInputView>,
        completion: oneshot::Sender<Result<(), AdministrationAuditExecutionError>>,
    ) {
        let result = self.operations.append_audit(input.as_ref());
        match &result {
            Ok(()) => {}
            Err(AdministrationAuditExecutionError::Storage(error))
                if error.kind() == riffdb_storage_api::StorageErrorKind::CommitStatusUnknown =>
            {
                self.lifecycle.fence();
            }
            Err(_) => self.lifecycle.stop(),
        }
        let _receiver_may_be_dropped = completion.send(result);
    }

    fn execute_audit_group(&mut self, group: Vec<AuditGroupItem>) -> Option<SubmittedWriterUnit> {
        // Fused pairs always form a singleton group and use the fused storage path.
        if group.len() == 1 && matches!(group[0].0, AdministrationAuditSubmission::FusedPair { .. })
        {
            let (submission, completion) = group.into_iter().next().expect("len 1");
            let AdministrationAuditSubmission::FusedPair { started, terminal } = submission else {
                unreachable!("matched FusedPair");
            };
            // Publish stop before unwinding drops the receipt sender. The
            // writer-loop guard is outside this call frame, so it cannot order
            // lifecycle publication ahead of these moved completion owners.
            let _panic_guard = ActorMessagePanicGuard::new(self.lifecycle.clone());
            let result = self
                .operations
                .append_audit_fused_pair(started.as_ref(), terminal.as_ref());
            // LimitExceeded is the call-scoped "unsupported fused pair" signal
            // from conformance shells — fail the call without stopping.
            let result = match result {
                Err(AdministrationAuditExecutionError::Storage(error))
                    if error.kind() == riffdb_storage_api::StorageErrorKind::LimitExceeded =>
                {
                    Err(AdministrationAuditExecutionError::PhaseConflict)
                }
                other => other,
            };
            match &result {
                Ok(()) => {}
                Err(AdministrationAuditExecutionError::PhaseConflict) => {}
                Err(AdministrationAuditExecutionError::Storage(error))
                    if error.kind()
                        == riffdb_storage_api::StorageErrorKind::CommitStatusUnknown =>
                {
                    self.lifecycle.fence();
                }
                Err(_) => self.lifecycle.stop(),
            }
            let _ = completion.send(result);
            return None;
        }
        let mut inputs = Vec::with_capacity(group.len());
        let mut completions = Vec::with_capacity(group.len());
        for (submission, completion) in group {
            match submission {
                AdministrationAuditSubmission::Single(input) => {
                    inputs.push(input);
                    completions.push(completion);
                }
                AdministrationAuditSubmission::FusedPair { started, terminal } => {
                    // Mixed groups are fail-closed: fused pairs must be alone.
                    self.lifecycle.stop();
                    let _ =
                        completion.send(Err(AdministrationAuditExecutionError::CoordinatorStopped));
                    for completion in completions {
                        let _ = completion
                            .send(Err(AdministrationAuditExecutionError::CoordinatorStopped));
                    }
                    let _ = (started, terminal);
                    return None;
                }
            }
        }
        // `completions` owns the accepted callers' receipt senders. Keep a
        // guard declared after it so panic unwinding publishes Stopped before
        // any sender drop can wake a caller with stale Accepting state.
        let _panic_guard = ActorMessagePanicGuard::new(self.lifecycle.clone());
        match self.operations.drive_audit_group(&inputs) {
            AuditGroupDriveResult::Complete(results) => {
                self.finish_audit_group(completions, results);
                None
            }
            AuditGroupDriveResult::Submitted(submitted) => Some(SubmittedWriterUnit::Audit {
                submitted: Some(submitted),
                completions,
            }),
        }
    }

    fn finish_audit_group(
        &mut self,
        completions: Vec<oneshot::Sender<Result<(), AdministrationAuditExecutionError>>>,
        results: Vec<Result<(), AdministrationAuditExecutionError>>,
    ) {
        Self::finish_audit_group_with(&self.lifecycle, completions, results);
    }

    fn finish_audit_group_with(
        lifecycle: &ActorLifecyclePublisher,
        completions: Vec<oneshot::Sender<Result<(), AdministrationAuditExecutionError>>>,
        results: Vec<Result<(), AdministrationAuditExecutionError>>,
    ) {
        if results.len() != completions.len() {
            lifecycle.stop();
            for completion in completions {
                let _receiver_may_be_dropped =
                    completion.send(Err(AdministrationAuditExecutionError::CoordinatorStopped));
            }
            return;
        }
        for (completion, result) in completions.into_iter().zip(results) {
            match &result {
                Ok(()) => {}
                Err(AdministrationAuditExecutionError::Storage(error))
                    if error.kind()
                        == riffdb_storage_api::StorageErrorKind::CommitStatusUnknown =>
                {
                    lifecycle.fence();
                }
                Err(_) => lifecycle.stop(),
            }
            let _receiver_may_be_dropped = completion.send(result);
        }
    }

    async fn execute_command(
        &mut self,
        preparation: CommandExecutionPreparation,
        command_id: riffdb_types::CommandId,
        ingress: riffdb_types::ServiceIngressKindV1,
        enqueued_at: Instant,
        completion: oneshot::Sender<Result<CommandExecutionResult, CommandExecutionError>>,
    ) {
        self.telemetry
            .record(CommitTelemetryEvent::StorageQueueCompleted {
                command_id,
                ingress,
                elapsed: enqueued_at.elapsed(),
            });
        let result = self.operations.drive_command(preparation).await;
        if let Ok(CommandExecutionResult::Committed(outcome)) = &result
            && outcome.disposition() == CommittedOutcomeDisposition::FirstCommit
        {
            let sequence = outcome.stored_outcome().commit_sequence();
            let publication_started = Instant::now();
            let publication = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                self.notifications.publish_first_commit(sequence)
            }));
            self.telemetry
                .record(CommitTelemetryEvent::CommandPipelineStageCompleted {
                    stage: CommandPipelineStage::Publication,
                    command_count: 1,
                    elapsed: publication_started.elapsed(),
                });
            if !matches!(publication, Ok(Ok(()))) {
                self.lifecycle.stop();
            }
        }
        self.telemetry
            .record(CommitTelemetryEvent::CommandTerminal {
                command_id,
                ingress,
                terminal: commit_command_terminal(&result),
                elapsed: enqueued_at.elapsed(),
            });
        let _receiver_may_be_dropped = completion.send(result);
    }

    async fn execute_command_group(
        &mut self,
        group: Vec<CommandGroupItem>,
        footprint: Option<crate::command_execution::DeferredPipelineFootprint>,
        evaluation_frontier: crate::command_execution::CommandEvaluationFrontier,
    ) -> Option<SubmittedWriterUnit> {
        crate::writer_census::observe_commands(u64::try_from(group.len()).unwrap_or(u64::MAX));
        let queue_telemetry_started = crate::writer_census::stage_start();
        for (_, command_id, ingress, enqueued_at, _) in &group {
            self.telemetry
                .record(CommitTelemetryEvent::StorageQueueCompleted {
                    command_id: *command_id,
                    ingress: *ingress,
                    elapsed: enqueued_at.elapsed(),
                });
        }
        let (preparations, metadata): (Vec<_>, Vec<_>) = group
            .into_iter()
            .map(
                |(preparation, command_id, ingress, enqueued_at, completion)| {
                    (preparation, (command_id, ingress, enqueued_at, completion))
                },
            )
            .unzip();
        crate::writer_census::charge(
            crate::writer_census::EXEC_QUEUE_TELEMETRY,
            queue_telemetry_started,
        );
        let drive_started = crate::writer_census::stage_start();
        let driven = self
            .operations
            .drive_command_group(preparations, evaluation_frontier)
            .await;
        crate::writer_census::charge(crate::writer_census::DRIVE_TOTAL, drive_started);
        match driven {
            CommandGroupDriveResult::Complete(results) => {
                if evaluation_frontier
                    == crate::command_execution::CommandEvaluationFrontier::WriterPrivate
                {
                    Some(SubmittedWriterUnit::Command {
                        pending: Some(PendingCommandPublication::AfterPredecessor(results)),
                        metadata,
                        footprint,
                    })
                } else {
                    let finish_started = crate::writer_census::stage_start();
                    self.finish_command_group(metadata, results);
                    crate::writer_census::charge(
                        crate::writer_census::EXEC_FINISH_GROUP,
                        finish_started,
                    );
                    None
                }
            }
            CommandGroupDriveResult::Submitted(submitted) => Some(SubmittedWriterUnit::Command {
                pending: Some(PendingCommandPublication::Fence(submitted)),
                metadata,
                footprint,
            }),
        }
    }

    fn finish_command_group(
        &mut self,
        metadata: Vec<CommandGroupMetadata>,
        results: Vec<Result<CommandExecutionResult, CommandExecutionError>>,
    ) {
        Self::finish_command_group_with(
            self.notifications.as_ref(),
            self.telemetry.as_ref(),
            &self.lifecycle,
            metadata,
            results,
        );
    }

    fn finish_command_group_with(
        notifications: &dyn ApplicationCommitNotificationSink,
        telemetry: &dyn CommitTelemetry,
        lifecycle: &ActorLifecyclePublisher,
        metadata: Vec<CommandGroupMetadata>,
        results: Vec<Result<CommandExecutionResult, CommandExecutionError>>,
    ) {
        if results.len() != metadata.len() {
            lifecycle.stop();
            for (_, _, _, completion) in metadata {
                let _receiver_may_be_dropped =
                    completion.send(Err(CommandExecutionError::coordinator_stopped()));
            }
            return;
        };
        let first_commit_sequences = results
            .iter()
            .filter_map(|result| match result {
                Ok(CommandExecutionResult::Committed(outcome))
                    if outcome.disposition() == CommittedOutcomeDisposition::FirstCommit =>
                {
                    Some(outcome.stored_outcome().commit_sequence())
                }
                Ok(CommandExecutionResult::Committed(_))
                | Ok(CommandExecutionResult::ExecutionFailed(_))
                | Ok(CommandExecutionResult::PreparationChanged)
                | Ok(CommandExecutionResult::InputMismatch)
                | Err(_) => None,
            })
            .collect::<Vec<_>>();
        if !first_commit_sequences.is_empty() {
            let publication_started = Instant::now();
            let publication = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                notifications.publish_first_commit_group(&first_commit_sequences)
            }));
            telemetry.record(CommitTelemetryEvent::CommandPipelineStageCompleted {
                stage: CommandPipelineStage::Publication,
                command_count: u16::try_from(first_commit_sequences.len()).unwrap_or(u16::MAX),
                elapsed: publication_started.elapsed(),
            });
            if !matches!(publication, Ok(Ok(()))) {
                lifecycle.stop();
            }
        }
        for ((command_id, ingress, enqueued_at, completion), result) in
            metadata.into_iter().zip(results)
        {
            telemetry.record(CommitTelemetryEvent::CommandTerminal {
                command_id,
                ingress,
                terminal: commit_command_terminal(&result),
                elapsed: enqueued_at.elapsed(),
            });
            let _receiver_may_be_dropped = completion.send(result);
        }
    }

    fn execute_read_only(
        &mut self,
        preparation: ReadOnlyExecutionPreparation,
        command_id: riffdb_types::CommandId,
        ingress: riffdb_types::ServiceIngressKindV1,
        enqueued_at: Instant,
        completion: oneshot::Sender<Result<ReadOnlyExecutionResult, CommandExecutionError>>,
    ) {
        self.telemetry
            .record(CommitTelemetryEvent::StorageQueueCompleted {
                command_id,
                ingress,
                elapsed: enqueued_at.elapsed(),
            });
        let result = self.operations.drive_read_only(preparation);
        self.telemetry
            .record(CommitTelemetryEvent::CommandTerminal {
                command_id,
                ingress,
                terminal: match &result {
                    Ok(_) => CommitCommandTerminal::ReadOnlySucceeded,
                    Err(error) => CommitCommandTerminal::Failed(error.kind()),
                },
                elapsed: enqueued_at.elapsed(),
            });
        let _receiver_may_be_dropped = completion.send(result);
    }

    fn execute_idempotency_inspection(
        &mut self,
        preparation: PreparedCommandIdempotencyInspection,
        completion: oneshot::Sender<
            Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError>,
        >,
    ) {
        let result = self.operations.inspect_idempotency(preparation);
        let _receiver_may_be_dropped = completion.send(result);
    }

    fn execute_catalog_deployment(
        &mut self,
        preparation: CatalogDeploymentPreparation,
        completion: oneshot::Sender<Result<CatalogDeploymentResult, ControlPlaneExecutionError>>,
    ) {
        let result = self.operations.deploy_catalog(preparation);
        let _receiver_may_be_dropped = completion.send(result);
    }

    fn execute_query_module_deployment(
        &mut self,
        preparation: QueryModuleDeploymentPreparation,
        completion: oneshot::Sender<
            Result<QueryModuleDeploymentResult, ControlPlaneExecutionError>,
        >,
    ) {
        let result = self.operations.deploy_query_module(preparation);
        let _receiver_may_be_dropped = completion.send(result);
    }

    fn execute_reactive_module_publication(
        &mut self,
        preparation: ReactiveModulePublicationPreparation,
        completion: oneshot::Sender<
            Result<ReactiveModulePublicationExecutionResult, ControlPlaneExecutionError>,
        >,
    ) {
        let result = self.operations.publish_reactive_module(preparation);
        let _receiver_may_be_dropped = completion.send(result);
    }

    fn execute_capability_create(
        &mut self,
        preparation: CapabilityCreatePreparation,
        completion: oneshot::Sender<
            Result<CapabilityCreateExecutionResult, ControlPlaneExecutionError>,
        >,
    ) {
        let result = self.operations.create_capability(preparation);
        let _receiver_may_be_dropped = completion.send(result);
    }

    fn execute_capability_revoke(
        &mut self,
        preparation: CapabilityRevokePreparation,
        completion: oneshot::Sender<
            Result<CapabilityRevokeExecutionResult, ControlPlaneExecutionError>,
        >,
    ) {
        let result = self.operations.revoke_capability(preparation);
        let _receiver_may_be_dropped = completion.send(result);
    }

    fn execute_capability_bootstrap(
        &mut self,
        preparation: CapabilityBootstrapPreparation,
        completion: oneshot::Sender<
            Result<CapabilityBootstrapExecutionResult, ControlPlaneExecutionError>,
        >,
    ) {
        let result = self.operations.bootstrap_capability(preparation);
        let _receiver_may_be_dropped = completion.send(result);
    }

    fn execute_capability_bootstrap_terminal(
        &mut self,
        preparation: CapabilityBootstrapTerminalPreparation,
        completion: oneshot::Sender<Result<(), ControlPlaneExecutionError>>,
    ) {
        let result = self.operations.append_bootstrap_terminal(preparation);
        let _receiver_may_be_dropped = completion.send(result);
    }
}

/// Pure selection kernel: form the next ordered work unit from the deque front.
///
/// Front-only consumption preserves ADR-0058 anti-starvation and ADR-0060's
/// deferred-before-new / barrier-overtaking prohibitions. The caller performs
/// ADR-0060's bounded sub-millisecond collection, and ADR-0098's contended
/// completion-edge collection, before invoking this pure kernel.
fn form_next_unit(
    pending: &mut VecDeque<CoordinatorMessage>,
) -> Option<(WorkUnit, CommitGroupDispatchReason)> {
    let head_class = command_grouping_class(pending.front()?);
    match head_class {
        CommandGroupingClass::Command => {
            let mut group = Vec::new();
            let mut deferred_obs = VecDeque::new();
            let mut reason = CommitGroupDispatchReason::QueueDrained;
            while let Some(class) = pending.front().map(command_grouping_class) {
                match class {
                    CommandGroupingClass::Command => {
                        if group.len() >= riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
                            reason = CommitGroupDispatchReason::Full;
                            break;
                        }
                        let Some(CoordinatorMessage::Command {
                            preparation,
                            command_id,
                            ingress,
                            enqueued_at,
                            completion,
                            ..
                        }) = pending.pop_front()
                        else {
                            unreachable!("command class must be Command");
                        };
                        group.push((*preparation, command_id, ingress, enqueued_at, completion));
                    }
                    CommandGroupingClass::DeferrableObservation => {
                        deferred_obs.push_back(
                            pending
                                .pop_front()
                                .expect("front present for deferrable observation"),
                        );
                    }
                    CommandGroupingClass::Barrier => {
                        reason = CommitGroupDispatchReason::Barrier;
                        break;
                    }
                }
            }
            while let Some(message) = deferred_obs.pop_back() {
                pending.push_front(message);
            }
            // Head was Command, so the group is non-empty.
            debug_assert!(!group.is_empty());
            Some((WorkUnit::CommandGroup(group), reason))
        }
        CommandGroupingClass::Barrier
            if matches!(
                pending.front(),
                Some(CoordinatorMessage::AdministrationAudit { .. })
            ) =>
        {
            // Fused pairs are their own unit; do not co-group with other audits.
            if matches!(
                pending.front(),
                Some(CoordinatorMessage::AdministrationAudit {
                    submission: AdministrationAuditSubmission::FusedPair { .. },
                    ..
                })
            ) {
                let Some(CoordinatorMessage::AdministrationAudit {
                    submission,
                    completion,
                }) = pending.pop_front()
                else {
                    unreachable!("front was AdministrationAudit");
                };
                // Next non-audit (if any) is a barrier for subsequent formation.
                let reason = if pending
                    .front()
                    .is_some_and(|m| !matches!(m, CoordinatorMessage::AdministrationAudit { .. }))
                {
                    CommitGroupDispatchReason::Barrier
                } else {
                    CommitGroupDispatchReason::QueueDrained
                };
                return Some((WorkUnit::AuditGroup(vec![(submission, completion)]), reason));
            }
            let mut group = Vec::new();
            let mut seen = std::collections::BTreeSet::new();
            let mut reason = CommitGroupDispatchReason::QueueDrained;
            while let Some(CoordinatorMessage::AdministrationAudit {
                submission: AdministrationAuditSubmission::Single(_),
                ..
            }) = pending.front()
            {
                if group.len() >= riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
                    reason = CommitGroupDispatchReason::Full;
                    break;
                }
                let Some(CoordinatorMessage::AdministrationAudit {
                    submission,
                    completion,
                }) = pending.pop_front()
                else {
                    unreachable!("front was AdministrationAudit");
                };
                let AdministrationAuditSubmission::Single(input) = submission else {
                    unreachable!("while matched Single");
                };
                let request = *input.request_id();
                if !seen.insert(request) {
                    pending.push_front(CoordinatorMessage::AdministrationAudit {
                        submission: AdministrationAuditSubmission::Single(input),
                        completion,
                    });
                    // Duplicate request_id is a formation boundary (like Full).
                    reason = CommitGroupDispatchReason::Full;
                    break;
                }
                group.push((AdministrationAuditSubmission::Single(input), completion));
            }
            if reason == CommitGroupDispatchReason::QueueDrained
                && pending
                    .front()
                    .is_some_and(|m| !matches!(m, CoordinatorMessage::AdministrationAudit { .. }))
            {
                reason = CommitGroupDispatchReason::Barrier;
            }
            Some((WorkUnit::AuditGroup(group), reason))
        }
        CommandGroupingClass::DeferrableObservation => {
            let message = pending.pop_front()?;
            Some((
                WorkUnit::Single(message),
                CommitGroupDispatchReason::QueueDrained,
            ))
        }
        CommandGroupingClass::Barrier => {
            let message = pending.pop_front()?;
            if matches!(message, CoordinatorMessage::Shutdown) {
                Some((WorkUnit::Shutdown, CommitGroupDispatchReason::QueueDrained))
            } else {
                Some((
                    WorkUnit::Single(message),
                    CommitGroupDispatchReason::Barrier,
                ))
            }
        }
    }
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn ewma(prior: u64, sample: u64, alpha_num: u64, alpha_den: u64) -> u64 {
    // prior*(1-α) + sample*α with α = alpha_num/alpha_den
    let keep = alpha_den.saturating_sub(alpha_num);
    prior
        .saturating_mul(keep)
        .saturating_add(sample.saturating_mul(alpha_num))
        / alpha_den.max(1)
}

fn unit_enqueue_hint(unit: &WorkUnit) -> Option<Instant> {
    match unit {
        WorkUnit::CommandGroup(group) => group.first().map(|item| item.3),
        WorkUnit::Single(CoordinatorMessage::Command { enqueued_at, .. })
        | WorkUnit::Single(CoordinatorMessage::ReadOnlyCommand { enqueued_at, .. }) => {
            Some(*enqueued_at)
        }
        WorkUnit::AuditGroup(_) | WorkUnit::Single(_) | WorkUnit::Shutdown => None,
    }
}

fn reject_work_unit(unit: WorkUnit, lifecycle: &ActorLifecyclePublisher) {
    let fenced = lifecycle_state(&lifecycle.lifecycle) == CoordinatorLifecycleState::Fenced;
    match unit {
        WorkUnit::CommandGroup(group) => {
            for (_, _, _, _, completion) in group {
                if fenced {
                    let _ = completion.send(Err(CommandExecutionError::coordinator_fenced()));
                } else {
                    let _ = completion.send(Err(CommandExecutionError::coordinator_stopped()));
                }
            }
        }
        WorkUnit::AuditGroup(group) => {
            for (_, completion) in group {
                if fenced {
                    let _ =
                        completion.send(Err(AdministrationAuditExecutionError::CoordinatorFenced));
                } else {
                    let _ =
                        completion.send(Err(AdministrationAuditExecutionError::CoordinatorStopped));
                }
            }
        }
        WorkUnit::Single(message) => reject_message_stopped_or_fenced(message, lifecycle),
        WorkUnit::Shutdown => {}
    }
}

fn reject_message_stopped_or_fenced(
    message: CoordinatorMessage,
    lifecycle: &ActorLifecyclePublisher,
) {
    if lifecycle_state(&lifecycle.lifecycle) == CoordinatorLifecycleState::Fenced {
        reject_message_fenced(message);
    } else {
        reject_message_stopped(message);
    }
}

fn reject_message_fenced(message: CoordinatorMessage) {
    match message {
        CoordinatorMessage::AdministrationAudit { completion, .. } => {
            let _ = completion.send(Err(AdministrationAuditExecutionError::CoordinatorFenced));
        }
        CoordinatorMessage::Command { completion, .. } => {
            let _ = completion.send(Err(CommandExecutionError::coordinator_fenced()));
        }
        CoordinatorMessage::ReadOnlyCommand { completion, .. } => {
            let _ = completion.send(Err(CommandExecutionError::coordinator_fenced()));
        }
        CoordinatorMessage::IdempotencyInspection { completion, .. } => {
            let _ = completion.send(Err(CommandIdempotencyInspectionError::coordinator_fenced()));
        }
        CoordinatorMessage::CatalogDeployment { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_fenced()));
        }
        CoordinatorMessage::QueryModuleDeployment { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_fenced()));
        }
        CoordinatorMessage::ReactiveModulePublication { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_fenced()));
        }
        CoordinatorMessage::CapabilityCreate { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_fenced()));
        }
        CoordinatorMessage::CapabilityRevoke { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_fenced()));
        }
        CoordinatorMessage::CapabilityBootstrap { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_fenced()));
        }
        CoordinatorMessage::CapabilityBootstrapTerminal { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_fenced()));
        }
        CoordinatorMessage::Shutdown => {}
    }
}

fn reject_message_stopped(message: CoordinatorMessage) {
    match message {
        CoordinatorMessage::AdministrationAudit { completion, .. } => {
            let _ = completion.send(Err(AdministrationAuditExecutionError::CoordinatorStopped));
        }
        CoordinatorMessage::Command { completion, .. } => {
            let _ = completion.send(Err(CommandExecutionError::coordinator_stopped()));
        }
        CoordinatorMessage::ReadOnlyCommand { completion, .. } => {
            let _ = completion.send(Err(CommandExecutionError::coordinator_stopped()));
        }
        CoordinatorMessage::IdempotencyInspection { completion, .. } => {
            let _ = completion.send(Err(CommandIdempotencyInspectionError::coordinator_stopped()));
        }
        CoordinatorMessage::CatalogDeployment { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_stopped()));
        }
        CoordinatorMessage::QueryModuleDeployment { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_stopped()));
        }
        CoordinatorMessage::ReactiveModulePublication { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_stopped()));
        }
        CoordinatorMessage::CapabilityCreate { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_stopped()));
        }
        CoordinatorMessage::CapabilityRevoke { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_stopped()));
        }
        CoordinatorMessage::CapabilityBootstrap { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_stopped()));
        }
        CoordinatorMessage::CapabilityBootstrapTerminal { completion, .. } => {
            let _ = completion.send(Err(ControlPlaneExecutionError::coordinator_stopped()));
        }
        CoordinatorMessage::Shutdown => {}
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommandGroupingClass {
    Command,
    DeferrableObservation,
    Barrier,
}

fn command_grouping_class(message: &CoordinatorMessage) -> CommandGroupingClass {
    match message {
        CoordinatorMessage::Command { .. } => CommandGroupingClass::Command,
        CoordinatorMessage::IdempotencyInspection { .. } => {
            CommandGroupingClass::DeferrableObservation
        }
        CoordinatorMessage::AdministrationAudit { .. }
        | CoordinatorMessage::ReadOnlyCommand { .. }
        | CoordinatorMessage::CatalogDeployment { .. }
        | CoordinatorMessage::QueryModuleDeployment { .. }
        | CoordinatorMessage::ReactiveModulePublication { .. }
        | CoordinatorMessage::CapabilityCreate { .. }
        | CoordinatorMessage::CapabilityRevoke { .. }
        | CoordinatorMessage::CapabilityBootstrap { .. }
        | CoordinatorMessage::CapabilityBootstrapTerminal { .. }
        | CoordinatorMessage::Shutdown => CommandGroupingClass::Barrier,
    }
}

#[derive(Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
struct CommandPositionSelection {
    selected: Vec<bool>,
    encountered_barrier: bool,
}

/// Retained for equivalence tests against the pre-pipeline selection model.
#[cfg_attr(not(test), allow(dead_code))]
fn select_command_positions(
    classes: &[CommandGroupingClass],
    available: usize,
) -> CommandPositionSelection {
    let mut selected = Vec::with_capacity(classes.len());
    let mut selected_count = 0usize;
    let mut encountered_barrier = false;
    for class in classes {
        let take = !encountered_barrier
            && selected_count < available
            && *class == CommandGroupingClass::Command;
        selected.push(take);
        if take {
            selected_count += 1;
        }
        if *class == CommandGroupingClass::Barrier {
            encountered_barrier = true;
        }
    }
    CommandPositionSelection {
        selected,
        encountered_barrier,
    }
}

fn commit_command_terminal(
    result: &Result<CommandExecutionResult, CommandExecutionError>,
) -> CommitCommandTerminal {
    match result {
        Ok(CommandExecutionResult::Committed(outcome)) => match outcome.disposition() {
            CommittedOutcomeDisposition::FirstCommit => CommitCommandTerminal::FirstCommit,
            CommittedOutcomeDisposition::Replay => CommitCommandTerminal::OutcomeReplay,
        },
        Ok(CommandExecutionResult::ExecutionFailed(_)) => CommitCommandTerminal::ExecutionFailed,
        Ok(CommandExecutionResult::PreparationChanged) => CommitCommandTerminal::PreparationChanged,
        Ok(CommandExecutionResult::InputMismatch) => CommitCommandTerminal::InputMismatch,
        Err(error) => CommitCommandTerminal::Failed(error.kind()),
    }
}

struct ActorMessagePanicGuard(ActorLifecyclePublisher);

impl ActorMessagePanicGuard {
    fn new(lifecycle: ActorLifecyclePublisher) -> Self {
        Self(lifecycle)
    }
}

impl Drop for ActorMessagePanicGuard {
    fn drop(&mut self) {
        if thread::panicking() {
            self.0.stop();
        }
    }
}

struct SubmissionGate(AtomicUsize);

impl SubmissionGate {
    const fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    fn begin(self: &Arc<Self>) -> Option<ActiveSubmission> {
        let mut observed = self.0.load(Ordering::Acquire);
        loop {
            if observed & SUBMISSION_GATE_CLOSED != 0 {
                return None;
            }
            debug_assert!((observed & SUBMISSION_COUNT_MASK) < u16::MAX.into());
            match self.0.compare_exchange_weak(
                observed,
                observed + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(ActiveSubmission(Arc::clone(self))),
                Err(current) => observed = current,
            }
        }
    }

    fn close(&self) {
        self.0.fetch_or(SUBMISSION_GATE_CLOSED, Ordering::AcqRel);
    }

    fn is_closed(&self) -> bool {
        self.0.load(Ordering::Acquire) & SUBMISSION_GATE_CLOSED != 0
    }
}

struct ActiveSubmission(Arc<SubmissionGate>);

impl Drop for ActiveSubmission {
    fn drop(&mut self) {
        let prior = self.0.0.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(prior & SUBMISSION_COUNT_MASK, 0);
    }
}

struct StoppedLifecycle {
    lifecycle: Arc<AtomicU8>,
    submission_gate: Arc<SubmissionGate>,
}

impl Drop for StoppedLifecycle {
    fn drop(&mut self) {
        let mut observed = self.lifecycle.load(Ordering::Acquire);
        while observed != LIFECYCLE_FENCED && observed != LIFECYCLE_STOPPED {
            match self.lifecycle.compare_exchange_weak(
                observed,
                LIFECYCLE_STOPPED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => observed = current,
            }
        }
        self.submission_gate.close();
        #[cfg(test)]
        {
            let seq = TEST_DROP_SEQ.fetch_add(1, Ordering::AcqRel) + 1;
            TEST_STOPPED_LIFECYCLE_DROP_SEQ.store(seq, Ordering::Release);
        }
    }
}

fn ensure_accepting(lifecycle: &AtomicU8) -> Result<(), AdministrationAuditAdmissionError> {
    match lifecycle.load(Ordering::Acquire) {
        LIFECYCLE_ACCEPTING => Ok(()),
        LIFECYCLE_DRAINING => Err(AdministrationAuditAdmissionError::Draining),
        LIFECYCLE_FENCED => Err(AdministrationAuditAdmissionError::Fenced),
        _ => Err(AdministrationAuditAdmissionError::Stopped),
    }
}

fn lifecycle_error(lifecycle: &AtomicU8) -> AdministrationAuditAdmissionError {
    match lifecycle.load(Ordering::Acquire) {
        LIFECYCLE_DRAINING => AdministrationAuditAdmissionError::Draining,
        LIFECYCLE_FENCED => AdministrationAuditAdmissionError::Fenced,
        LIFECYCLE_ACCEPTING | LIFECYCLE_STOPPED => AdministrationAuditAdmissionError::Stopped,
        _ => AdministrationAuditAdmissionError::Stopped,
    }
}

fn ensure_command_accepting(lifecycle: &AtomicU8) -> Result<(), CommandExecutionAdmissionError> {
    match lifecycle.load(Ordering::Acquire) {
        LIFECYCLE_ACCEPTING => Ok(()),
        LIFECYCLE_DRAINING => Err(CommandExecutionAdmissionError::Draining),
        LIFECYCLE_FENCED => Err(CommandExecutionAdmissionError::Fenced),
        _ => Err(CommandExecutionAdmissionError::Stopped),
    }
}

fn command_lifecycle_error(lifecycle: &AtomicU8) -> CommandExecutionAdmissionError {
    match lifecycle.load(Ordering::Acquire) {
        LIFECYCLE_DRAINING => CommandExecutionAdmissionError::Draining,
        LIFECYCLE_FENCED => CommandExecutionAdmissionError::Fenced,
        LIFECYCLE_ACCEPTING | LIFECYCLE_STOPPED => CommandExecutionAdmissionError::Stopped,
        _ => CommandExecutionAdmissionError::Stopped,
    }
}

fn ensure_control_plane_accepting(
    lifecycle: &AtomicU8,
) -> Result<(), ControlPlaneExecutionAdmissionError> {
    match lifecycle.load(Ordering::Acquire) {
        LIFECYCLE_ACCEPTING => Ok(()),
        LIFECYCLE_DRAINING => Err(ControlPlaneExecutionAdmissionError::Draining),
        LIFECYCLE_FENCED => Err(ControlPlaneExecutionAdmissionError::Fenced),
        _ => Err(ControlPlaneExecutionAdmissionError::Stopped),
    }
}

fn control_plane_lifecycle_error(lifecycle: &AtomicU8) -> ControlPlaneExecutionAdmissionError {
    match lifecycle.load(Ordering::Acquire) {
        LIFECYCLE_DRAINING => ControlPlaneExecutionAdmissionError::Draining,
        LIFECYCLE_FENCED => ControlPlaneExecutionAdmissionError::Fenced,
        LIFECYCLE_ACCEPTING | LIFECYCLE_STOPPED => ControlPlaneExecutionAdmissionError::Stopped,
        _ => ControlPlaneExecutionAdmissionError::Stopped,
    }
}

fn ensure_idempotency_inspection_accepting(
    lifecycle: &AtomicU8,
) -> Result<(), CommandIdempotencyInspectionError> {
    match lifecycle.load(Ordering::Acquire) {
        LIFECYCLE_ACCEPTING => Ok(()),
        LIFECYCLE_FENCED => Err(CommandIdempotencyInspectionError::coordinator_fenced()),
        LIFECYCLE_DRAINING | LIFECYCLE_STOPPED => {
            Err(CommandIdempotencyInspectionError::coordinator_stopped())
        }
        _ => Err(CommandIdempotencyInspectionError::coordinator_stopped()),
    }
}

fn idempotency_inspection_lifecycle_error(
    lifecycle: &AtomicU8,
) -> CommandIdempotencyInspectionError {
    match lifecycle.load(Ordering::Acquire) {
        LIFECYCLE_FENCED => CommandIdempotencyInspectionError::coordinator_fenced(),
        LIFECYCLE_ACCEPTING | LIFECYCLE_DRAINING | LIFECYCLE_STOPPED => {
            CommandIdempotencyInspectionError::coordinator_stopped()
        }
        _ => CommandIdempotencyInspectionError::coordinator_stopped(),
    }
}

fn lifecycle_state(lifecycle: &AtomicU8) -> CoordinatorLifecycleState {
    match lifecycle.load(Ordering::Acquire) {
        LIFECYCLE_ACCEPTING => CoordinatorLifecycleState::Accepting,
        LIFECYCLE_DRAINING => CoordinatorLifecycleState::Draining,
        LIFECYCLE_FENCED => CoordinatorLifecycleState::Fenced,
        _ => CoordinatorLifecycleState::Stopped,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use riffdb_storage_api::{
        ServiceAuditAppendResult, StorageErrorKind, StoredServiceAuditRecordV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, ApprovalId, CapabilityId, RequestId,
        ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
        ServiceOperationV1, Timestamp,
    };

    use super::*;

    struct FixedClock {
        calls: AtomicUsize,
        result: Result<Timestamp, AdministrationClockError>,
    }

    impl AdministrationClock for FixedClock {
        fn now(&self) -> Result<Timestamp, AdministrationClockError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.result
        }
    }

    enum AppendBehavior {
        Append,
        PhaseConflict,
        Fail(StorageError),
    }

    struct RecordingRepository {
        calls: usize,
        group_calls: usize,
        intent: Option<ServiceAuditAppendIntentV1>,
        behavior: AppendBehavior,
    }

    impl ServiceAuditAppendRepository for RecordingRepository {
        fn append_service_audit(
            &mut self,
            intent: &ServiceAuditAppendIntentV1,
        ) -> Result<ServiceAuditAppendResult, StorageError> {
            self.calls += 1;
            self.intent = Some(intent.clone());
            match &self.behavior {
                AppendBehavior::Append => Ok(ServiceAuditAppendResult::Appended(
                    StoredServiceAuditRecordV1::from_intent(
                        AdministrationSequence::first(),
                        intent,
                    ),
                )),
                AppendBehavior::PhaseConflict => Ok(ServiceAuditAppendResult::PhaseConflict),
                AppendBehavior::Fail(error) => Err(error.clone()),
            }
        }

        fn append_service_audit_group(
            &mut self,
            intents: &[ServiceAuditAppendIntentV1],
        ) -> Result<Vec<ServiceAuditAppendResult>, StorageError> {
            self.group_calls += 1;
            match &self.behavior {
                AppendBehavior::Append => intents
                    .iter()
                    .enumerate()
                    .map(|(index, intent)| {
                        let sequence = AdministrationSequence::new(
                            u64::try_from(index + 1).expect("small test group"),
                        )
                        .expect("nonzero sequence");
                        Ok(ServiceAuditAppendResult::Appended(
                            StoredServiceAuditRecordV1::from_intent(sequence, intent),
                        ))
                    })
                    .collect(),
                AppendBehavior::PhaseConflict => {
                    Ok(vec![ServiceAuditAppendResult::PhaseConflict; intents.len()])
                }
                AppendBehavior::Fail(error) => Err(error.clone()),
            }
        }
    }

    struct CheckedInput {
        request_id: RequestId,
        operation: ServiceOperationV1,
        phase: ServiceAuditPhaseV1,
        principal_id: ActorId,
        actor_kind: ActorKind,
        capability_id: CapabilityId,
        capability_revision: NonZeroU64,
        ingress: ServiceIngressKindV1,
        targets: ServiceAuditTargetsV1,
        approval_id: Option<ApprovalId>,
        link: ServiceAuditLinkV1,
    }

    impl AdministrationAuditInputView for CheckedInput {
        fn request_id(&self) -> &RequestId {
            &self.request_id
        }

        fn operation(&self) -> &ServiceOperationV1 {
            &self.operation
        }

        fn phase(&self) -> &ServiceAuditPhaseV1 {
            &self.phase
        }

        fn principal_id(&self) -> &ActorId {
            &self.principal_id
        }

        fn actor_kind(&self) -> &ActorKind {
            &self.actor_kind
        }

        fn capability_id(&self) -> &CapabilityId {
            &self.capability_id
        }

        fn capability_revision(&self) -> &NonZeroU64 {
            &self.capability_revision
        }

        fn ingress(&self) -> &ServiceIngressKindV1 {
            &self.ingress
        }

        fn targets(&self) -> &ServiceAuditTargetsV1 {
            &self.targets
        }

        fn approval_id(&self) -> Option<&ApprovalId> {
            self.approval_id.as_ref()
        }

        fn link(&self) -> &ServiceAuditLinkV1 {
            &self.link
        }
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn input() -> CheckedInput {
        CheckedInput {
            request_id: RequestId::from_bytes(uuid_bytes(0x31)).expect("request UUIDv7"),
            operation: ServiceOperationV1::DeployContract,
            phase: ServiceAuditPhaseV1::Succeeded,
            principal_id: ActorId::new("maintainer").expect("principal"),
            actor_kind: ActorKind::Human,
            capability_id: CapabilityId::from_bytes(uuid_bytes(0x32)).expect("capability UUIDv7"),
            capability_revision: NonZeroU64::new(9).expect("nonzero revision"),
            ingress: ServiceIngressKindV1::Grpc,
            targets: ServiceAuditTargetsV1::empty(),
            approval_id: Some(ApprovalId::new("approved-change").expect("approval")),
            link: ServiceAuditLinkV1::ControlPlane {
                administration_sequence: AdministrationSequence::first(),
            },
        }
    }

    fn clock(result: Result<Timestamp, AdministrationClockError>) -> FixedClock {
        FixedClock {
            calls: AtomicUsize::new(0),
            result,
        }
    }

    fn repository(behavior: AppendBehavior) -> RecordingRepository {
        RecordingRepository {
            calls: 0,
            group_calls: 0,
            intent: None,
            behavior,
        }
    }

    #[test]
    fn one_sample_and_one_append_copy_every_checked_field_without_returning_a_sequence() {
        let input = input();
        let timestamp = Timestamp::new(17, 29).expect("timestamp");
        let clock = clock(Ok(timestamp));
        let mut repository = repository(AppendBehavior::Append);

        assert_eq!(
            append_administration_audit(&mut repository, &clock, &input),
            Ok(())
        );
        assert_eq!(clock.calls.load(Ordering::Relaxed), 1);
        assert_eq!(repository.calls, 1);

        let intent = repository.intent.expect("one captured intent");
        assert_eq!(intent.request_id(), input.request_id);
        assert_eq!(intent.timestamp(), timestamp);
        assert_eq!(intent.operation(), input.operation);
        assert_eq!(intent.phase(), input.phase);
        let principal = intent.principal().expect("authenticated principal");
        assert_eq!(principal.principal_id(), &input.principal_id);
        assert_eq!(principal.actor_kind(), input.actor_kind);
        assert_eq!(principal.capability_id(), input.capability_id);
        assert_eq!(principal.capability_revision(), input.capability_revision);
        assert_eq!(intent.ingress(), input.ingress);
        assert_eq!(intent.targets(), &input.targets);
        assert_eq!(intent.approval_id(), input.approval_id.as_ref());
        assert_eq!(intent.link(), input.link);
    }

    #[test]
    fn compatible_audits_lower_once_each_and_use_one_repository_group() {
        let first = input();
        let mut second = input();
        second.request_id = RequestId::from_bytes(uuid_bytes(0x41)).expect("request UUIDv7");
        let inputs: Vec<Box<dyn AdministrationAuditInputView>> =
            vec![Box::new(first), Box::new(second)];
        let clock = clock(Ok(Timestamp::new(19, 0).expect("timestamp")));
        let mut repository = repository(AppendBehavior::Append);

        let results = append_administration_audit_group(&mut repository, &clock, &inputs);

        assert_eq!(results, vec![Ok(()), Ok(())]);
        assert_eq!(clock.calls.load(Ordering::Relaxed), 2);
        assert_eq!(repository.calls, 0);
        assert_eq!(repository.group_calls, 1);
    }

    #[test]
    fn clock_failure_performs_no_lowering_or_append_and_is_not_retried() {
        let clock = clock(Err(AdministrationClockError));
        let mut repository = repository(AppendBehavior::Append);

        assert_eq!(
            append_administration_audit(&mut repository, &clock, &input()),
            Err(AdministrationAuditExecutionError::Clock(
                AdministrationClockError
            ))
        );
        assert_eq!(clock.calls.load(Ordering::Relaxed), 1);
        assert_eq!(repository.calls, 0);
        assert!(repository.intent.is_none());
    }

    #[test]
    fn invalid_checked_view_fails_closed_before_storage() {
        let timestamp = Timestamp::new(23, 0).expect("timestamp");
        let clock = clock(Ok(timestamp));
        let mut repository = repository(AppendBehavior::Append);
        let mut input = input();
        input.phase = ServiceAuditPhaseV1::Started;

        assert_eq!(
            append_administration_audit(&mut repository, &clock, &input),
            Err(AdministrationAuditExecutionError::InvalidInput(
                StorageValueError::InvalidShape
            ))
        );
        assert_eq!(clock.calls.load(Ordering::Relaxed), 1);
        assert_eq!(repository.calls, 0);
    }

    #[test]
    fn phase_conflict_is_typed_and_attempted_once() {
        let clock = clock(Ok(Timestamp::new(31, 0).expect("timestamp")));
        let mut repository = repository(AppendBehavior::PhaseConflict);

        assert_eq!(
            append_administration_audit(&mut repository, &clock, &input()),
            Err(AdministrationAuditExecutionError::PhaseConflict)
        );
        assert_eq!(clock.calls.load(Ordering::Relaxed), 1);
        assert_eq!(repository.calls, 1);
    }

    #[test]
    fn storage_failure_is_preserved_and_never_retried() {
        let expected = StorageError::new(StorageErrorKind::CommitStatusUnknown, None);
        let clock = clock(Ok(Timestamp::new(37, 0).expect("timestamp")));
        let mut repository = repository(AppendBehavior::Fail(expected.clone()));

        assert_eq!(
            append_administration_audit(&mut repository, &clock, &input()),
            Err(AdministrationAuditExecutionError::Storage(expected))
        );
        assert_eq!(clock.calls.load(Ordering::Relaxed), 1);
        assert_eq!(repository.calls, 1);
    }

    #[test]
    fn command_selection_defers_observations_without_crossing_a_hard_barrier() {
        use CommandGroupingClass::{Barrier, Command, DeferrableObservation};

        let selected = select_command_positions(
            &[
                DeferrableObservation,
                Command,
                DeferrableObservation,
                Command,
                Barrier,
                Command,
            ],
            64,
        );

        assert_eq!(
            selected,
            CommandPositionSelection {
                selected: vec![false, true, false, true, false, false],
                encountered_barrier: true,
            }
        );
    }

    #[test]
    fn command_selection_honors_remaining_group_capacity_without_reordering_tail() {
        use CommandGroupingClass::{Command, DeferrableObservation};

        let selected = select_command_positions(
            &[Command, DeferrableObservation, Command, Command, Command],
            2,
        );

        assert_eq!(
            selected,
            CommandPositionSelection {
                selected: vec![true, false, true, false, false],
                encountered_barrier: false,
            }
        );
    }
}

#[cfg(test)]
#[path = "audit_executor/form_next_unit_tests.rs"]
mod form_next_unit_tests;

#[cfg(test)]
#[path = "audit_executor/actor_tests.rs"]
mod actor_tests;
