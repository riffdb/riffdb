//! Sole-writer coordinator actor and synchronous service-audit lowering.

use std::future::Future;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::{error::Error, fmt, thread};

use riffdb_storage_api::{
    AdmissionRepository, ApplicationCommandTransactionPort, AuditPrincipalV1,
    ExecutionFailureTransitionPort, ServiceAuditAppendIntentV1, ServiceAuditAppendRepository,
    ServiceAuditAppendResult, SnapshotReader, StorageError, StorageValueError,
};
use tokio::runtime;
use tokio::sync::{mpsc, oneshot};

use riffdb_conflict::ConflictManager;

use crate::{
    AdministrationAuditInputView, AdministrationClock, AdministrationClockError, AdmissionClock,
    CommandExecutionPreparation, ProvenanceIdSource,
    command_execution::{
        CommandExecutionError, CommandExecutionLifecycle, CommandExecutionResult,
        CoordinatorDurability, drive_command_execution,
    },
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
    let timestamp = clock
        .now()
        .map_err(AdministrationAuditExecutionError::Clock)?;
    let principal = AuditPrincipalV1::new(
        input.principal_id().clone(),
        *input.actor_kind(),
        *input.capability_id(),
        *input.capability_revision(),
    );
    let intent = ServiceAuditAppendIntentV1::new(
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
    .map_err(AdministrationAuditExecutionError::InvalidInput)?;

    match repository
        .append_service_audit(&intent)
        .map_err(AdministrationAuditExecutionError::Storage)?
    {
        ServiceAuditAppendResult::Appended(_) => Ok(()),
        ServiceAuditAppendResult::PhaseConflict => {
            Err(AdministrationAuditExecutionError::PhaseConflict)
        }
    }
}

const LIFECYCLE_ACCEPTING: u8 = 0;
const LIFECYCLE_DRAINING: u8 = 1;
const LIFECYCLE_FENCED: u8 = 2;
const LIFECYCLE_STOPPED: u8 = 3;
const SUBMISSION_GATE_CLOSED: usize = 1 << (usize::BITS - 1);
const SUBMISSION_COUNT_MASK: usize = !SUBMISSION_GATE_CLOSED;

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
    /// The permanently reserved shutdown slot could not be established.
    ShutdownCapacityUnavailable,
}

impl fmt::Display for CoordinatorStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RuntimeUnavailable => "coordinator runtime is unavailable",
            Self::ThreadUnavailable => "coordinator thread is unavailable",
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
        self.submit_after_admission(input, || {})
    }

    fn submit_after_admission(
        mut self,
        input: Box<dyn AdministrationAuditInputView>,
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
        let _sender = permit.send(CoordinatorMessage::AdministrationAudit { input, completion });
        drop(submission);
        Ok(AdministrationAuditReceipt { receiver })
    }

    #[cfg(test)]
    fn submit_with_hook(
        self,
        input: Box<dyn AdministrationAuditInputView>,
        after_admission: impl FnOnce(),
    ) -> Result<AdministrationAuditReceipt, AdministrationAuditAdmissionError> {
        self.submit_after_admission(input, after_admission)
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

/// Safe rejection before a command preparation is accepted by the coordinator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandExecutionAdmissionError {
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
        if self.submission_gate.is_closed() {
            drop(permit);
            return Err(command_lifecycle_error(&self.lifecycle));
        }
        ensure_command_accepting(&self.lifecycle)?;
        Ok(CommandExecutionCapacityPermit {
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
}

impl CommandExecutionCapacityPermit {
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
        let (completion, receiver) = oneshot::channel();
        let permit = self
            .permit
            .take()
            .expect("move-only command capacity permit is consumed once");
        let _sender = permit.send(CoordinatorMessage::Command {
            preparation: Box::new(preparation),
            completion,
        });
        drop(submission);
        Ok(CommandExecutionReceipt { receiver })
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

/// Owning lifecycle guard for the one extensible sole-writer coordinator actor.
///
/// Call [`Self::shutdown`] to close admission, drain already accepted work, and
/// join the actor. Dropping this guard initiates the same drain but deliberately
/// detaches the join so `Drop` never blocks or risks joining the current thread.
pub struct RunningCommandCoordinator {
    executor: AdministrationAuditExecutor,
    command_executor: CommandExecutor,
    shutdown_permit: Option<mpsc::OwnedPermit<CoordinatorMessage>>,
    actor_thread: Option<thread::JoinHandle<()>>,
    actor_thread_id: thread::ThreadId,
}

impl RunningCommandCoordinator {
    /// Starts one dedicated current-thread actor owning the authoritative repository.
    #[allow(clippy::too_many_arguments)]
    pub fn start<Repository>(
        workload_capacity: CoordinatorWorkloadCapacity,
        durability: CoordinatorDurability,
        repository: Repository,
        conflicts: Arc<dyn ConflictManager>,
        admission_clock: Arc<dyn AdmissionClock>,
        administration_clock: Arc<dyn AdministrationClock>,
        provenance_source: Arc<dyn ProvenanceIdSource>,
    ) -> Result<Self, CoordinatorStartError>
    where
        Repository: AdmissionRepository
            + SnapshotReader
            + ApplicationCommandTransactionPort
            + ExecutionFailureTransitionPort
            + ServiceAuditAppendRepository
            + Send
            + 'static,
    {
        Self::spawn_with_operations(workload_capacity, move |lifecycle| {
            Box::new(ProductionCoordinatorOperations {
                repository,
                conflicts,
                admission_clock,
                administration_clock,
                provenance_source,
                durability,
                lifecycle,
            })
        })
    }

    fn spawn_with_operations(
        workload_capacity: CoordinatorWorkloadCapacity,
        operations: impl FnOnce(ActorLifecyclePublisher) -> Box<dyn CoordinatorActorOperations>,
    ) -> Result<Self, CoordinatorStartError> {
        let channel_capacity = usize::from(workload_capacity.get()) + 1;
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let shutdown_permit = sender
            .clone()
            .try_reserve_owned()
            .map_err(|_| CoordinatorStartError::ShutdownCapacityUnavailable)?;
        let runtime = runtime::Builder::new_current_thread()
            .build()
            .map_err(|_| CoordinatorStartError::RuntimeUnavailable)?;
        let lifecycle = Arc::new(AtomicU8::new(LIFECYCLE_ACCEPTING));
        let submission_gate = Arc::new(SubmissionGate::new());
        let actor_lifecycle = Arc::clone(&lifecycle);
        let actor_submission_gate = Arc::clone(&submission_gate);
        let lifecycle_publisher = ActorLifecyclePublisher {
            lifecycle: Arc::clone(&lifecycle),
            submission_gate: Arc::clone(&submission_gate),
        };
        let actor = CommandCoordinatorActor {
            receiver,
            operations: operations(lifecycle_publisher.clone()),
            lifecycle: lifecycle_publisher,
        };
        let actor_thread = thread::Builder::new()
            .name("riffdb-command-coordinator".to_owned())
            .spawn(move || {
                let _stopped = StoppedLifecycle {
                    lifecycle: actor_lifecycle,
                    submission_gate: actor_submission_gate,
                };
                runtime.block_on(actor.run());
            })
            .map_err(|_| CoordinatorStartError::ThreadUnavailable)?;
        let actor_thread_id = actor_thread.thread().id();
        Ok(Self {
            executor: AdministrationAuditExecutor {
                sender: sender.clone(),
                lifecycle: Arc::clone(&lifecycle),
                submission_gate: Arc::clone(&submission_gate),
            },
            command_executor: CommandExecutor {
                sender,
                lifecycle,
                submission_gate,
            },
            shutdown_permit: Some(shutdown_permit),
            actor_thread: Some(actor_thread),
            actor_thread_id,
        })
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
        Self::spawn_with_operations(workload_capacity, move |_| {
            Box::new(AuditOnlyCoordinatorOperations { repository, clock })
        })
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
        actor_thread
            .join()
            .map_err(|_| CoordinatorShutdownError::ActorPanicked)
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
        self.actor_thread.take();
    }
}

enum CoordinatorMessage {
    AdministrationAudit {
        input: Box<dyn AdministrationAuditInputView>,
        completion: oneshot::Sender<Result<(), AdministrationAuditExecutionError>>,
    },
    Command {
        preparation: Box<CommandExecutionPreparation>,
        completion: oneshot::Sender<Result<CommandExecutionResult, CommandExecutionError>>,
    },
    Shutdown,
}

type LocalCommandFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CommandExecutionResult, CommandExecutionError>> + 'a>>;

trait CoordinatorActorOperations: Send {
    fn append_audit(
        &mut self,
        input: &dyn AdministrationAuditInputView,
    ) -> Result<(), AdministrationAuditExecutionError>;

    fn drive_command(&mut self, preparation: CommandExecutionPreparation)
    -> LocalCommandFuture<'_>;
}

struct ProductionCoordinatorOperations<Repository> {
    repository: Repository,
    conflicts: Arc<dyn ConflictManager>,
    admission_clock: Arc<dyn AdmissionClock>,
    administration_clock: Arc<dyn AdministrationClock>,
    provenance_source: Arc<dyn ProvenanceIdSource>,
    durability: CoordinatorDurability,
    lifecycle: ActorLifecyclePublisher,
}

impl<Repository> CoordinatorActorOperations for ProductionCoordinatorOperations<Repository>
where
    Repository: AdmissionRepository
        + SnapshotReader
        + ApplicationCommandTransactionPort
        + ExecutionFailureTransitionPort
        + ServiceAuditAppendRepository
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

    fn drive_command(
        &mut self,
        preparation: CommandExecutionPreparation,
    ) -> LocalCommandFuture<'_> {
        Box::pin(drive_command_execution(
            &self.repository,
            self.conflicts.as_ref(),
            self.admission_clock.as_ref(),
            self.provenance_source.as_ref(),
            self.durability,
            &self.lifecycle,
            preparation,
        ))
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

    fn drive_command(&mut self, _: CommandExecutionPreparation) -> LocalCommandFuture<'_> {
        Box::pin(async { Err(CommandExecutionError::coordinator_stopped()) })
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

struct CommandCoordinatorActor {
    receiver: mpsc::Receiver<CoordinatorMessage>,
    operations: Box<dyn CoordinatorActorOperations>,
    lifecycle: ActorLifecyclePublisher,
}

impl CommandCoordinatorActor {
    async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                CoordinatorMessage::AdministrationAudit { input, completion } => {
                    if self.execute_audit(input, completion) {
                        self.reject_remaining_after_fence().await;
                        break;
                    }
                }
                CoordinatorMessage::Command {
                    preparation,
                    completion,
                } => {
                    self.execute_command(*preparation, completion).await;
                    if self.reject_after_published_terminal_state().await {
                        break;
                    }
                }
                CoordinatorMessage::Shutdown => {
                    self.receiver.close();
                    while let Some(message) = self.receiver.recv().await {
                        match message {
                            CoordinatorMessage::AdministrationAudit { input, completion } => {
                                let must_fence = self.execute_audit(input, completion);
                                if must_fence {
                                    self.reject_remaining_after_fence().await;
                                    return;
                                }
                            }
                            CoordinatorMessage::Command {
                                preparation,
                                completion,
                            } => {
                                self.execute_command(*preparation, completion).await;
                                if self.reject_after_published_terminal_state().await {
                                    return;
                                }
                            }
                            CoordinatorMessage::Shutdown => {}
                        }
                    }
                    break;
                }
            }
        }
    }

    fn execute_audit(
        &mut self,
        input: Box<dyn AdministrationAuditInputView>,
        completion: oneshot::Sender<Result<(), AdministrationAuditExecutionError>>,
    ) -> bool {
        let result = self.operations.append_audit(input.as_ref());
        let must_fence = matches!(
            &result,
            Err(AdministrationAuditExecutionError::Storage(error))
                if error.kind() == riffdb_storage_api::StorageErrorKind::CommitStatusUnknown
        );
        if must_fence {
            self.lifecycle
                .lifecycle
                .store(LIFECYCLE_FENCED, Ordering::Release);
            self.lifecycle.submission_gate.close();
            self.receiver.close();
        }
        let _receiver_may_be_dropped = completion.send(result);
        must_fence
    }

    async fn execute_command(
        &mut self,
        preparation: CommandExecutionPreparation,
        completion: oneshot::Sender<Result<CommandExecutionResult, CommandExecutionError>>,
    ) {
        let result = self.operations.drive_command(preparation).await;
        let _receiver_may_be_dropped = completion.send(result);
    }

    async fn reject_after_published_terminal_state(&mut self) -> bool {
        match lifecycle_state(&self.lifecycle.lifecycle) {
            CoordinatorLifecycleState::Fenced => {
                self.receiver.close();
                self.reject_remaining_after_fence().await;
                true
            }
            CoordinatorLifecycleState::Stopped => {
                self.receiver.close();
                self.reject_remaining_after_stop().await;
                true
            }
            CoordinatorLifecycleState::Accepting | CoordinatorLifecycleState::Draining => false,
        }
    }

    async fn reject_remaining_after_fence(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                CoordinatorMessage::AdministrationAudit { completion, .. } => {
                    let _receiver_may_be_dropped =
                        completion.send(Err(AdministrationAuditExecutionError::CoordinatorFenced));
                }
                CoordinatorMessage::Command { completion, .. } => {
                    let _receiver_may_be_dropped =
                        completion.send(Err(CommandExecutionError::coordinator_fenced()));
                }
                CoordinatorMessage::Shutdown => {}
            }
        }
    }

    async fn reject_remaining_after_stop(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                CoordinatorMessage::AdministrationAudit { completion, .. } => {
                    let _receiver_may_be_dropped =
                        completion.send(Err(AdministrationAuditExecutionError::CoordinatorStopped));
                }
                CoordinatorMessage::Command { completion, .. } => {
                    let _receiver_may_be_dropped =
                        completion.send(Err(CommandExecutionError::coordinator_stopped()));
                }
                CoordinatorMessage::Shutdown => {}
            }
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
}

#[cfg(test)]
#[path = "audit_executor/actor_tests.rs"]
mod actor_tests;
