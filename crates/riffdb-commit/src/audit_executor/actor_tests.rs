use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc as std_mpsc};
use std::task::{Context, Poll, Waker};

use riffdb_storage_api::{ServiceAuditAppendResult, StorageErrorKind, StoredServiceAuditRecordV1};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, ApprovalId, CapabilityId, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    ServiceOperationV1, Timestamp,
};

use super::*;

#[derive(Clone)]
struct Probe {
    calls: Arc<AtomicUsize>,
    threads: Arc<Mutex<Vec<thread::ThreadId>>>,
    intents: Arc<Mutex<Vec<ServiceAuditAppendIntentV1>>>,
}

impl Probe {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            threads: Arc::new(Mutex::new(Vec::new())),
            intents: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

struct TestClock {
    calls: Arc<AtomicUsize>,
    threads: Arc<Mutex<Vec<thread::ThreadId>>>,
    result: Result<Timestamp, AdministrationClockError>,
}

impl TestClock {
    fn fixed(timestamp: Timestamp) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            threads: Arc::new(Mutex::new(Vec::new())),
            result: Ok(timestamp),
        }
    }

    fn failing() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            threads: Arc::new(Mutex::new(Vec::new())),
            result: Err(AdministrationClockError),
        }
    }
}

impl AdministrationClock for TestClock {
    fn now(&self) -> Result<Timestamp, AdministrationClockError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.threads
            .lock()
            .expect("clock thread probe")
            .push(thread::current().id());
        self.result
    }
}

#[derive(Clone)]
enum RepositoryBehavior {
    Append,
    PhaseConflict,
}

struct RecordingRepository {
    probe: Probe,
    behavior: RepositoryBehavior,
}

impl RecordingRepository {
    fn appending(probe: Probe) -> Self {
        Self {
            probe,
            behavior: RepositoryBehavior::Append,
        }
    }

    fn phase_conflicting(probe: Probe) -> Self {
        Self {
            probe,
            behavior: RepositoryBehavior::PhaseConflict,
        }
    }
}

impl ServiceAuditAppendRepository for RecordingRepository {
    fn append_service_audit(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        let call = self.probe.calls.fetch_add(1, Ordering::Relaxed) + 1;
        self.probe
            .threads
            .lock()
            .expect("repository thread probe")
            .push(thread::current().id());
        self.probe
            .intents
            .lock()
            .expect("intent probe")
            .push(intent.clone());
        match &self.behavior {
            RepositoryBehavior::Append => Ok(ServiceAuditAppendResult::Appended(
                StoredServiceAuditRecordV1::from_intent(
                    AdministrationSequence::try_from(call as u64).expect("nonzero sequence"),
                    intent,
                ),
            )),
            RepositoryBehavior::PhaseConflict => Ok(ServiceAuditAppendResult::PhaseConflict),
        }
    }
}

struct BlockingRepository {
    probe: Probe,
    entered: std_mpsc::SyncSender<()>,
    release_first: Option<std_mpsc::Receiver<()>>,
}

struct BlockingFailureRepository {
    probe: Probe,
    entered: std_mpsc::SyncSender<()>,
    release: Option<std_mpsc::Receiver<()>>,
    error: StorageError,
}

impl ServiceAuditAppendRepository for BlockingFailureRepository {
    fn append_service_audit(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        self.probe.calls.fetch_add(1, Ordering::Relaxed);
        self.probe
            .intents
            .lock()
            .expect("intent probe")
            .push(intent.clone());
        if let Some(release) = self.release.take() {
            self.entered.send(()).expect("report failed append entry");
            release.recv().expect("release failed append");
        }
        Err(self.error.clone())
    }
}

struct BlockingPanicRepository {
    entered: std_mpsc::SyncSender<()>,
    release: Option<std_mpsc::Receiver<()>>,
}

struct BoundaryGroupRepository {
    next_sequence: u64,
    entered: std_mpsc::SyncSender<()>,
    release_first: Option<std_mpsc::Receiver<()>>,
    group_sizes: Arc<Mutex<Vec<usize>>>,
}

impl BoundaryGroupRepository {
    fn append(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        let sequence =
            AdministrationSequence::try_from(self.next_sequence).expect("test sequence is nonzero");
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("bounded test sequence");
        Ok(ServiceAuditAppendResult::Appended(
            StoredServiceAuditRecordV1::from_intent(sequence, intent),
        ))
    }
}

impl ServiceAuditAppendRepository for BoundaryGroupRepository {
    fn append_service_audit(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        if let Some(release) = self.release_first.take() {
            self.entered.send(()).expect("report first append entry");
            release.recv().expect("release first append");
        }
        self.append(intent)
    }

    fn append_service_audit_group(
        &mut self,
        intents: &[ServiceAuditAppendIntentV1],
    ) -> Result<Vec<ServiceAuditAppendResult>, StorageError> {
        self.group_sizes
            .lock()
            .expect("group-size probe")
            .push(intents.len());
        intents.iter().map(|intent| self.append(intent)).collect()
    }
}

impl ServiceAuditAppendRepository for BlockingPanicRepository {
    fn append_service_audit(
        &mut self,
        _: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        if let Some(release) = self.release.take() {
            self.entered.send(()).expect("report panic entry");
            release.recv().expect("release repository panic");
        }
        panic!("intentional coordinator repository panic")
    }
}

impl ServiceAuditAppendRepository for BlockingRepository {
    fn append_service_audit(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        let call = self.probe.calls.fetch_add(1, Ordering::Relaxed) + 1;
        self.probe
            .threads
            .lock()
            .expect("repository thread probe")
            .push(thread::current().id());
        self.probe
            .intents
            .lock()
            .expect("intent probe")
            .push(intent.clone());
        if let Some(release) = self.release_first.take() {
            self.entered.send(()).expect("report first append");
            release.recv().expect("release first append");
        }
        Ok(ServiceAuditAppendResult::Appended(
            StoredServiceAuditRecordV1::from_intent(
                AdministrationSequence::try_from(call as u64).expect("nonzero sequence"),
                intent,
            ),
        ))
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

fn checked_input(fill: u8) -> CheckedInput {
    CheckedInput {
        request_id: RequestId::from_bytes(uuid_bytes(fill)).expect("request UUIDv7"),
        operation: ServiceOperationV1::GetCommit,
        phase: ServiceAuditPhaseV1::Started,
        principal_id: ActorId::new(format!("principal-{fill}")).expect("principal"),
        actor_kind: ActorKind::Human,
        capability_id: CapabilityId::from_bytes(uuid_bytes(fill.wrapping_add(1)))
            .expect("capability UUIDv7"),
        capability_revision: NonZeroU64::new(3).expect("nonzero revision"),
        ingress: ServiceIngressKindV1::Grpc,
        targets: ServiceAuditTargetsV1::empty(),
        approval_id: None,
        link: ServiceAuditLinkV1::None,
    }
}

fn input(fill: u8) -> Box<dyn AdministrationAuditInputView> {
    Box::new(checked_input(fill))
}

fn capacity(value: u16) -> CoordinatorWorkloadCapacity {
    CoordinatorWorkloadCapacity::new(value).expect("nonzero test capacity")
}

fn block_on<Output>(future: impl Future<Output = Output>) -> Output {
    runtime::Builder::new_current_thread()
        .build()
        .expect("test runtime")
        .block_on(future)
}

fn poll_once<Output>(future: Pin<&mut impl Future<Output = Output>>) -> Poll<Output> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    future.poll(&mut context)
}

fn fixed_timestamp() -> Timestamp {
    Timestamp::new(71, 13).expect("canonical timestamp")
}

#[test]
fn workload_capacity_excludes_shutdown_and_cancelled_reservation_releases_position() {
    assert_eq!(CoordinatorWorkloadCapacity::new(0), None);
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(2),
        RecordingRepository::appending(probe),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let command_executor = running.command_executor();
    let first = block_on(executor.reserve_capacity()).expect("first workload slot");
    let second = block_on(command_executor.reserve_capacity()).expect("shared command slot");
    assert_eq!(executor.sender.capacity(), 0);

    let mut pending = Box::pin(executor.reserve_capacity());
    assert!(matches!(poll_once(pending.as_mut()), Poll::Pending));
    drop(pending);

    drop(first);
    let replacement = block_on(executor.reserve_capacity()).expect("released workload slot");
    assert_eq!(executor.sender.capacity(), 0);
    drop(second);
    drop(replacement);
    running.shutdown().expect("clean shutdown");
    assert_eq!(
        format!("{command_executor:?}"),
        "CommandExecutor([REDACTED])"
    );
}

#[test]
fn synchronous_submit_executes_on_the_one_actor_thread() {
    let caller_thread = thread::current().id();
    let probe = Probe::new();
    let clock = TestClock::fixed(fixed_timestamp());
    let clock_calls = Arc::clone(&clock.calls);
    let clock_threads = Arc::clone(&clock.threads);
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(probe.clone()),
        clock,
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let permit = block_on(executor.reserve_capacity()).expect("workload slot");
    let receipt = permit.submit(input(0x31)).expect("synchronous submission");
    assert_eq!(block_on(receipt.completion()), Ok(()));
    running.shutdown().expect("clean shutdown");

    assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
    assert_eq!(clock_calls.load(Ordering::Relaxed), 1);
    let repository_threads = probe.threads.lock().expect("repository threads");
    let clock_threads = clock_threads.lock().expect("clock threads");
    assert_eq!(repository_threads.as_slice(), clock_threads.as_slice());
    assert_eq!(repository_threads.len(), 1);
    assert_ne!(repository_threads[0], caller_thread);
    let intents = probe.intents.lock().expect("captured intent");
    assert_eq!(intents[0].timestamp(), fixed_timestamp());
}

#[test]
fn dropping_receipt_does_not_cancel_accepted_append() {
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(probe.clone()),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let permit = block_on(executor.reserve_capacity()).expect("workload slot");
    let receipt = permit.submit(input(0x41)).expect("accepted append");
    drop(receipt);
    running.shutdown().expect("shutdown drains accepted work");
    assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
}

#[test]
fn shutdown_drains_a_full_workload_queue_without_sleeping() {
    let probe = Probe::new();
    let (entered_sender, entered_receiver) = std_mpsc::sync_channel(0);
    let (release_sender, release_receiver) = std_mpsc::sync_channel(0);
    let repository = BlockingRepository {
        probe: probe.clone(),
        entered: entered_sender,
        release_first: Some(release_receiver),
    };
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(2),
        repository,
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();

    let first = block_on(executor.reserve_capacity())
        .expect("first slot")
        .submit(input(0x51))
        .expect("first submit");
    entered_receiver.recv().expect("actor entered append");
    let second = block_on(executor.reserve_capacity())
        .expect("second slot")
        .submit(input(0x52))
        .expect("second submit");
    let third = block_on(executor.reserve_capacity())
        .expect("third slot")
        .submit(input(0x53))
        .expect("third submit");
    assert_eq!(executor.sender.capacity(), 0);

    let (shutdown_started_sender, shutdown_started_receiver) = std_mpsc::sync_channel(0);
    let shutdown_thread = thread::spawn(move || {
        shutdown_started_sender.send(()).expect("signal shutdown");
        running.shutdown()
    });
    shutdown_started_receiver
        .recv()
        .expect("shutdown thread ready");
    release_sender.send(()).expect("release actor");
    assert_eq!(shutdown_thread.join().expect("shutdown thread"), Ok(()));
    assert_eq!(block_on(first.completion()), Ok(()));
    assert_eq!(block_on(second.completion()), Ok(()));
    assert_eq!(block_on(third.completion()), Ok(()));
    assert_eq!(probe.calls.load(Ordering::Relaxed), 3);
}

#[test]
fn actor_drains_the_exact_64_item_internal_group_boundary_without_waiting() {
    let (entered_sender, entered_receiver) = std_mpsc::sync_channel(0);
    let (release_sender, release_receiver) = std_mpsc::sync_channel(0);
    let group_sizes = Arc::new(Mutex::new(Vec::new()));
    let repository = BoundaryGroupRepository {
        next_sequence: 1,
        entered: entered_sender,
        release_first: Some(release_receiver),
        group_sizes: Arc::clone(&group_sizes),
    };
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(64),
        repository,
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();

    let first = block_on(executor.reserve_capacity())
        .expect("first slot")
        .submit(input(0x10))
        .expect("first accepted append");
    entered_receiver.recv().expect("actor entered first append");

    let queued = (0_u8..64)
        .map(|index| {
            block_on(executor.reserve_capacity())
                .expect("queued slot")
                .submit(input(index.wrapping_add(0x20)))
                .expect("queued accepted append")
        })
        .collect::<Vec<_>>();
    assert_eq!(executor.sender.capacity(), 0);

    release_sender.send(()).expect("release first append");
    assert_eq!(block_on(first.completion()), Ok(()));
    for receipt in queued {
        assert_eq!(block_on(receipt.completion()), Ok(()));
    }
    running.shutdown().expect("clean shutdown");

    assert_eq!(
        group_sizes.lock().expect("group-size probe").as_slice(),
        [64]
    );
}

#[test]
fn held_permit_fails_closed_after_draining_and_releases_shutdown() {
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(probe.clone()),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let permit = block_on(executor.reserve_capacity()).expect("held workload slot");
    let lifecycle = Arc::clone(&executor.lifecycle);
    let shutdown_thread = thread::spawn(move || running.shutdown());
    while lifecycle.load(Ordering::Acquire) == LIFECYCLE_ACCEPTING {
        thread::yield_now();
    }
    assert!(matches!(
        permit.submit(input(0x61)),
        Err(AdministrationAuditAdmissionError::Draining)
    ));
    assert_eq!(shutdown_thread.join().expect("shutdown thread"), Ok(()));
    assert_eq!(probe.calls.load(Ordering::Relaxed), 0);
    assert_eq!(lifecycle.load(Ordering::Acquire), LIFECYCLE_STOPPED);
}

#[test]
fn shutdown_reason_is_visible_while_the_submission_gate_is_still_open() {
    let mut running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(Probe::new()),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let permit = block_on(executor.reserve_capacity()).expect("held workload slot");
    let (published_sender, published_receiver) = std_mpsc::sync_channel(0);
    let (release_sender, release_receiver) = std_mpsc::sync_channel(0);
    let shutdown_thread = thread::spawn(move || {
        running.initiate_shutdown_after_publication(|| {
            published_sender.send(()).expect("report draining state");
            release_receiver.recv().expect("release gate close");
        });
        running.shutdown()
    });
    published_receiver
        .recv()
        .expect("draining reason published before gate close");

    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Draining
    );
    assert!(matches!(
        permit.submit(input(0x60)),
        Err(AdministrationAuditAdmissionError::Draining)
    ));
    release_sender.send(()).expect("allow shutdown gate close");
    assert_eq!(shutdown_thread.join().expect("shutdown thread"), Ok(()));
}

#[test]
fn submission_admitted_before_shutdown_is_drained_even_when_sent_after_shutdown_message() {
    let probe = Probe::new();
    let mut running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(probe.clone()),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let permit = block_on(executor.reserve_capacity()).expect("held workload slot");
    let (admitted_sender, admitted_receiver) = std_mpsc::sync_channel(0);
    let (release_sender, release_receiver) = std_mpsc::sync_channel(0);
    let submit_thread = thread::spawn(move || {
        permit.submit_with_hook(input(0x62), || {
            admitted_sender.send(()).expect("report admitted submit");
            release_receiver.recv().expect("release admitted submit");
        })
    });
    admitted_receiver
        .recv()
        .expect("submission entered atomic gate");

    running.initiate_shutdown();
    release_sender
        .send(())
        .expect("send workload after shutdown message");
    let receipt = submit_thread
        .join()
        .expect("submit thread")
        .expect("pre-shutdown admission remains accepted");
    assert_eq!(block_on(receipt.completion()), Ok(()));
    running.shutdown().expect("drained shutdown");
    assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
}

#[test]
fn explicit_shutdown_waits_for_an_outstanding_permit_to_drop() {
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(probe.clone()),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let permit = block_on(executor.reserve_capacity()).expect("held workload slot");
    let lifecycle = Arc::clone(&executor.lifecycle);
    let (done_sender, done_receiver) = std_mpsc::sync_channel(1);
    let shutdown_thread = thread::spawn(move || {
        done_sender
            .send(running.shutdown())
            .expect("report shutdown result");
    });
    while lifecycle.load(Ordering::Acquire) == LIFECYCLE_ACCEPTING {
        thread::yield_now();
    }
    assert!(matches!(
        done_receiver.try_recv(),
        Err(std_mpsc::TryRecvError::Empty)
    ));
    drop(permit);
    assert_eq!(done_receiver.recv().expect("shutdown completes"), Ok(()));
    shutdown_thread.join().expect("shutdown thread");
    assert_eq!(probe.calls.load(Ordering::Relaxed), 0);
}

#[test]
fn reservation_completed_before_shutdown_is_rejected_by_the_post_reservation_gate() {
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(Probe::new()),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let lifecycle = Arc::clone(&executor.lifecycle);
    let (reserved_sender, reserved_receiver) = std_mpsc::sync_channel(0);
    let (release_sender, release_receiver) = std_mpsc::sync_channel(0);
    let reservation_executor = executor.clone();
    let reservation_thread = thread::spawn(move || {
        block_on(reservation_executor.reserve_capacity_with_hook(|| {
            reserved_sender.send(()).expect("report reserved capacity");
            release_receiver.recv().expect("release reservation check");
        }))
    });
    reserved_receiver
        .recv()
        .expect("capacity reserved before lifecycle check");

    let shutdown_thread = thread::spawn(move || running.shutdown());
    while lifecycle.load(Ordering::Acquire) == LIFECYCLE_ACCEPTING {
        thread::yield_now();
    }
    release_sender
        .send(())
        .expect("run post-reservation lifecycle check");
    assert!(matches!(
        reservation_thread.join().expect("reservation thread"),
        Err(AdministrationAuditAdmissionError::Draining)
    ));
    assert_eq!(shutdown_thread.join().expect("shutdown thread"), Ok(()));
}

#[test]
fn unknown_audit_status_fences_before_completion_and_rejects_queued_work() {
    let probe = Probe::new();
    let (entered_sender, entered_receiver) = std_mpsc::sync_channel(0);
    let (release_sender, release_receiver) = std_mpsc::sync_channel(0);
    let unknown = StorageError::new(StorageErrorKind::CommitStatusUnknown, None);
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(2),
        BlockingFailureRepository {
            probe: probe.clone(),
            entered: entered_sender,
            release: Some(release_receiver),
            error: unknown.clone(),
        },
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let command_executor = running.command_executor();
    let first = block_on(executor.reserve_capacity())
        .expect("first slot")
        .submit(input(0x73))
        .expect("first accepted append");
    entered_receiver.recv().expect("first append entered");
    let queued = block_on(executor.reserve_capacity())
        .expect("queued slot")
        .submit(input(0x74))
        .expect("queued accepted append");

    release_sender.send(()).expect("release unknown append");
    assert_eq!(
        block_on(first.completion()),
        Err(AdministrationAuditExecutionError::Storage(unknown))
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Fenced
    );
    assert!(matches!(
        block_on(executor.reserve_capacity()),
        Err(AdministrationAuditAdmissionError::Fenced)
    ));
    assert!(matches!(
        block_on(command_executor.reserve_capacity()),
        Err(CommandExecutionAdmissionError::Fenced)
    ));
    assert_eq!(
        block_on(queued.completion()),
        Err(AdministrationAuditExecutionError::CoordinatorFenced)
    );
    assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
    running.shutdown().expect("join fenced actor");
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Fenced
    );
}

#[test]
fn unexpected_actor_panic_stops_admission_and_every_queued_receipt() {
    let (entered_sender, entered_receiver) = std_mpsc::sync_channel(0);
    let (release_sender, release_receiver) = std_mpsc::sync_channel(0);
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(2),
        BlockingPanicRepository {
            entered: entered_sender,
            release: Some(release_receiver),
        },
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let command_executor = running.command_executor();
    let first = block_on(executor.reserve_capacity())
        .expect("first slot")
        .submit(input(0x75))
        .expect("first accepted append");
    entered_receiver.recv().expect("panicking append entered");
    let queued = block_on(executor.reserve_capacity())
        .expect("queued slot")
        .submit(input(0x76))
        .expect("queued accepted append");

    release_sender.send(()).expect("release actor panic");
    assert_eq!(
        block_on(first.completion()),
        Err(AdministrationAuditExecutionError::CoordinatorStopped)
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped,
        "panic publication precedes the in-flight sender drop and receipt wake"
    );
    assert_eq!(
        block_on(queued.completion()),
        Err(AdministrationAuditExecutionError::CoordinatorStopped)
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    assert!(matches!(
        block_on(executor.reserve_capacity()),
        Err(AdministrationAuditAdmissionError::Stopped)
    ));
    assert!(matches!(
        block_on(command_executor.reserve_capacity()),
        Err(CommandExecutionAdmissionError::Stopped)
    ));
    assert_eq!(
        running.shutdown(),
        Err(CoordinatorShutdownError::ActorPanicked)
    );
}

#[test]
fn proven_clock_audit_failure_stops_all_admission_without_retry() {
    let clock = TestClock::failing();
    let clock_calls = Arc::clone(&clock.calls);
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(probe.clone()),
        clock,
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let command_executor = running.command_executor();
    let receipt = block_on(executor.reserve_capacity())
        .expect("workload slot")
        .submit(input(0x71))
        .expect("accepted clock-failure attempt");
    assert_eq!(
        block_on(receipt.completion()),
        Err(AdministrationAuditExecutionError::Clock(
            AdministrationClockError
        ))
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    assert!(matches!(
        block_on(executor.reserve_capacity()),
        Err(AdministrationAuditAdmissionError::Stopped)
    ));
    assert!(matches!(
        block_on(command_executor.reserve_capacity()),
        Err(CommandExecutionAdmissionError::Stopped)
    ));
    running.shutdown().expect("clean shutdown");
    assert_eq!(clock_calls.load(Ordering::Relaxed), 1);
    assert_eq!(probe.calls.load(Ordering::Relaxed), 0);
}

#[test]
fn proven_storage_audit_failure_stops_before_completion_and_rejects_queued_work() {
    let expected = StorageError::new(StorageErrorKind::Unavailable, None);
    let (entered_sender, entered_receiver) = std_mpsc::sync_channel(0);
    let (release_sender, release_receiver) = std_mpsc::sync_channel(0);
    let clock = TestClock::fixed(fixed_timestamp());
    let clock_calls = Arc::clone(&clock.calls);
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(2),
        BlockingFailureRepository {
            probe: probe.clone(),
            entered: entered_sender,
            release: Some(release_receiver),
            error: expected.clone(),
        },
        clock,
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let command_executor = running.command_executor();
    let first = block_on(executor.reserve_capacity())
        .expect("first workload slot")
        .submit(input(0x72))
        .expect("accepted storage-failure attempt");
    entered_receiver.recv().expect("failed append entered");
    let queued = block_on(executor.reserve_capacity())
        .expect("queued workload slot")
        .submit(input(0x73))
        .expect("queued accepted append");

    release_sender.send(()).expect("release failed append");
    assert_eq!(
        block_on(first.completion()),
        Err(AdministrationAuditExecutionError::Storage(expected))
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    assert!(matches!(
        block_on(executor.reserve_capacity()),
        Err(AdministrationAuditAdmissionError::Stopped)
    ));
    assert!(matches!(
        block_on(command_executor.reserve_capacity()),
        Err(CommandExecutionAdmissionError::Stopped)
    ));
    assert_eq!(
        block_on(queued.completion()),
        Err(AdministrationAuditExecutionError::CoordinatorStopped)
    );
    running.shutdown().expect("clean shutdown");
    assert_eq!(clock_calls.load(Ordering::Relaxed), 1);
    assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
}

#[test]
fn invalid_or_phase_conflicting_audit_attempt_stops_readiness() {
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(probe.clone()),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let mut invalid = checked_input(0x74);
    invalid.link = ServiceAuditLinkV1::ControlPlane {
        administration_sequence: AdministrationSequence::first(),
    };
    let receipt = block_on(executor.reserve_capacity())
        .expect("workload slot")
        .submit(Box::new(invalid))
        .expect("accepted invalid-input attempt");
    assert_eq!(
        block_on(receipt.completion()),
        Err(AdministrationAuditExecutionError::InvalidInput(
            StorageValueError::InvalidShape
        ))
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    running.shutdown().expect("join stopped coordinator");
    assert_eq!(probe.calls.load(Ordering::Relaxed), 0);

    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::phase_conflicting(probe.clone()),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let executor = running.administration_audit_executor();
    let receipt = block_on(executor.reserve_capacity())
        .expect("workload slot")
        .submit(input(0x75))
        .expect("accepted phase-conflict attempt");
    assert_eq!(
        block_on(receipt.completion()),
        Err(AdministrationAuditExecutionError::PhaseConflict)
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    running.shutdown().expect("join stopped coordinator");
    assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
}

#[test]
fn try_reserve_capacity_returns_overloaded_when_channel_is_full_without_blocking() {
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(probe),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let command = running.command_executor();
    // Workload capacity 1 + shutdown slot = channel 2; one hold fills the sole workload slot.
    let held = command
        .try_reserve_capacity()
        .expect("first non-blocking reservation");
    assert!(matches!(
        command.try_reserve_capacity(),
        Err(CommandExecutionAdmissionError::Overloaded)
    ));
    // Concurrent try must not park: a second call still fails immediately.
    assert!(matches!(
        command.try_reserve_capacity(),
        Err(CommandExecutionAdmissionError::Overloaded)
    ));
    drop(held);
    let released = command
        .try_reserve_capacity()
        .expect("released slot is available again");
    drop(released);
    running.shutdown().expect("clean shutdown");
}

#[test]
fn try_acquire_retained_bytes_returns_overloaded_when_budget_is_exhausted() {
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(2),
        RecordingRepository::appending(probe),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let command = running.command_executor();
    // Exhaust the independent retained-byte semaphore in one shot.
    let total_units = u32::try_from(MAX_QUEUED_COMMAND_BYTES / QUEUED_COMMAND_BYTE_UNIT)
        .expect("byte budget fits u32");
    let held = command
        .try_acquire_retained_bytes(total_units)
        .expect("full budget is available at start");
    assert!(matches!(
        command.try_acquire_retained_bytes(1),
        Err(CommandExecutionAdmissionError::Overloaded)
    ));
    drop(held);
    let recovered = command
        .try_acquire_retained_bytes(1)
        .expect("budget returns on drop");
    drop(recovered);
    running.shutdown().expect("clean shutdown");
}

#[test]
fn undersized_pre_admitted_byte_permit_is_internal_defect_not_silent_accept() {
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(1),
        RecordingRepository::appending(probe),
        TestClock::fixed(fixed_timestamp()),
    )
    .expect("start coordinator");
    let command = running.command_executor();
    let permit = command.try_reserve_capacity().expect("queue slot");
    let byte_permit = command
        .try_acquire_retained_bytes(1)
        .expect("one retained unit");
    let mut permit = permit.with_retained_bytes(byte_permit, 1);
    assert!(matches!(
        permit.take_retained_byte_permit(4),
        Err(CommandExecutionAdmissionError::PermitUnitMismatch)
    ));
    // Queue slot remains held until the permit drops; drop without accept.
    drop(permit);
    running.shutdown().expect("clean shutdown");
}
