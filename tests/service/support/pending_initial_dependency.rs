//! A bounded semantic double for the native Pending-before-audit refusal.
//! The real command actor still owns idempotency and the original outcome.
use super::support::{IncrementingAdministrationClock, ServiceHarness};
use riffdb_commit::{CoordinatorWorkloadCapacity, RunningCommandCoordinator};
use riffdb_service::{
    RequestControl, RequestDeadlineFuture, RequestDeadlineScheduler, RiffDbService, ServiceJob,
    ServiceJobSpawner,
};
use riffdb_storage_api::{
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditAppendResult,
    StorageError, StorageErrorKind, StoredServiceAuditRecordV1,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct PendingInitialDependency {
    ready: AtomicBool,
    admission_probes: AtomicUsize,
    wait_polls: AtomicUsize,
    changed: Notify,
    records: Mutex<Vec<StoredServiceAuditRecordV1>>,
    refused_appends: AtomicUsize,
    fail_appends: AtomicBool,
    probe_error: Mutex<Option<riffdb_service::InitialNativeDependencyError>>,
}
impl PendingInitialDependency {
    pub(crate) async fn wait(&self) {
        self.wait_polls.fetch_add(1, Ordering::SeqCst);
        loop {
            let changed = self.changed.notified();
            if self.ready.load(Ordering::SeqCst) {
                return;
            }
            changed.await;
        }
    }
    pub(crate) fn release(&self) {
        self.ready.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
    pub(crate) fn records(&self) -> Vec<StoredServiceAuditRecordV1> {
        self.records.lock().unwrap().clone()
    }
    pub(crate) fn admission_probes(&self) -> usize {
        self.admission_probes.load(Ordering::SeqCst)
    }
    pub(crate) fn fail_appends(&self) {
        self.fail_appends.store(true, Ordering::SeqCst);
    }
    pub(crate) fn refused_appends(&self) -> usize {
        self.refused_appends.load(Ordering::SeqCst)
    }
    pub(crate) fn refuse_probe(&self, error: riffdb_service::InitialNativeDependencyError) {
        *self.probe_error.lock().unwrap() = Some(error);
    }
    fn append(
        &self,
        intents: &[ServiceAuditAppendIntentV1],
    ) -> Result<Vec<StoredServiceAuditRecordV1>, StorageError> {
        if !self.ready.load(Ordering::SeqCst) || self.fail_appends.load(Ordering::SeqCst) {
            self.refused_appends.fetch_add(1, Ordering::SeqCst);
            return Err(StorageError::new(StorageErrorKind::Unavailable, None));
        }
        assert!(!intents.is_empty() && intents.len() <= 2);
        let mut records = self.records.lock().unwrap();
        assert!(records.len() + intents.len() <= 4, "bounded audit fixture");
        let next: Vec<_> = intents
            .iter()
            .enumerate()
            .map(|(offset, intent)| {
                StoredServiceAuditRecordV1::from_intent(
                    riffdb_types::AdministrationSequence::new((records.len() + offset + 1) as u64)
                        .unwrap(),
                    intent,
                )
            })
            .collect();
        records.extend(next.iter().cloned());
        Ok(next)
    }
}
impl riffdb_service::InitialNativeDependencyPort for PendingInitialDependency {
    fn begin_initial_admission<'a>(
        &'a self,
        _control: &'a RequestControl,
    ) -> Result<
        riffdb_service::InitialNativeDependencyAdmission<'a>,
        riffdb_service::InitialNativeDependencyError,
    > {
        self.admission_probes.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = *self.probe_error.lock().unwrap() {
            return Err(error);
        }
        if self.ready.load(Ordering::SeqCst) {
            Ok(riffdb_service::InitialNativeDependencyAdmission::Ready)
        } else {
            Ok(riffdb_service::InitialNativeDependencyAdmission::Pending(
                Box::pin(async move {
                    self.wait().await;
                    Ok(())
                }),
            ))
        }
    }
}
struct PendingAuditRepository(Arc<PendingInitialDependency>);
impl ServiceAuditAppendRepository for PendingAuditRepository {
    fn append_service_audit(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError> {
        Ok(ServiceAuditAppendResult::Appended(
            self.0.append(std::slice::from_ref(intent))?.pop().unwrap(),
        ))
    }
    fn append_service_audit_fused_pair(
        &mut self,
        started: &ServiceAuditAppendIntentV1,
        terminal: &ServiceAuditAppendIntentV1,
    ) -> Result<(), StorageError> {
        self.0.append(&[started.clone(), terminal.clone()])?;
        Ok(())
    }
}
#[derive(Default)]
pub(crate) struct ManualInitialDependencySpawner(Mutex<Option<ServiceJob>>);
impl ServiceJobSpawner for ManualInitialDependencySpawner {
    fn spawn(&self, job: ServiceJob) {
        let mut pending = self.0.lock().unwrap();
        assert!(pending.is_none(), "one bounded service invocation");
        *pending = Some(job);
    }
}
impl ManualInitialDependencySpawner {
    pub(crate) fn take(&self) -> ServiceJob {
        self.0.lock().unwrap().take().unwrap()
    }
}
struct FormationClock;
impl riffdb_commit::CoordinatorMonotonicClock for FormationClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
            deadline,
        )))
    }
}
struct InitialDeadlineRecorder {
    original: Arc<dyn RequestDeadlineScheduler>,
    deadlines: Mutex<Vec<Instant>>,
}
impl RequestDeadlineScheduler for InitialDeadlineRecorder {
    fn wait_until(&self, deadline: Instant) -> RequestDeadlineFuture<'_> {
        let mut deadlines = self.deadlines.lock().unwrap();
        assert!(
            deadlines.len() < 64,
            "bounded original-deadline observations"
        );
        deadlines.push(deadline);
        drop(deadlines);
        self.original.wait_until(deadline)
    }
}
pub(crate) struct PendingInitialDependencyFixture {
    pub(crate) service: RiffDbService,
    pub(crate) dependency: Arc<PendingInitialDependency>,
    pub(crate) spawner: Arc<ManualInitialDependencySpawner>,
    deadlines: Arc<InitialDeadlineRecorder>,
    actor: Option<RunningCommandCoordinator>,
}
impl PendingInitialDependencyFixture {
    pub(crate) fn new(harness: &ServiceHarness) -> Self {
        let dependency = Arc::new(PendingInitialDependency::default());
        let actor = RunningCommandCoordinator::start_audit_simulation(
            CoordinatorWorkloadCapacity::new(8).unwrap(),
            PendingAuditRepository(Arc::clone(&dependency)),
            IncrementingAdministrationClock::new(),
            Arc::new(FormationClock),
        )
        .unwrap();
        let spawner = Arc::new(ManualInitialDependencySpawner::default());
        let deadlines = Arc::new(InitialDeadlineRecorder {
            original: harness.initial_dependency_fixture_scheduler(),
            deadlines: Mutex::new(Vec::new()),
        });
        let service = harness.initial_dependency_fixture_service(
            actor.administration_audit_executor(),
            Arc::clone(&dependency) as Arc<dyn riffdb_service::InitialNativeDependencyPort>,
            Arc::clone(&spawner) as Arc<dyn ServiceJobSpawner>,
            Arc::clone(&deadlines) as Arc<dyn RequestDeadlineScheduler>,
        );
        Self {
            service,
            dependency,
            spawner,
            deadlines,
            actor: Some(actor),
        }
    }
    pub(crate) fn audit_submissions(&self) -> u64 {
        self.actor
            .as_ref()
            .unwrap()
            .administration_audit_executor()
            .accepted_submission_count()
    }
    pub(crate) fn lifecycle(&self) -> riffdb_commit::CoordinatorLifecycleState {
        self.actor
            .as_ref()
            .unwrap()
            .administration_audit_executor()
            .lifecycle_state()
    }
    pub(crate) fn recorded_deadlines(&self) -> Vec<Instant> {
        self.deadlines.deadlines.lock().unwrap().clone()
    }
    pub(crate) async fn drive_to_dependency_or_audit(&self, job: &mut ServiceJob) {
        struct WakeNotification(Notify);
        impl Wake for WakeNotification {
            fn wake(self: Arc<Self>) {
                self.0.notify_one();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.notify_one();
            }
        }
        let signal = Arc::new(WakeNotification(Notify::new()));
        let waker = Waker::from(Arc::clone(&signal));
        for _ in 0..32 {
            let changed = signal.0.notified();
            let result = job.as_mut().poll(&mut Context::from_waker(&waker));
            if self.audit_submissions() != 0
                || self.dependency.wait_polls.load(Ordering::SeqCst) != 0
            {
                return;
            }
            assert!(
                matches!(result, Poll::Pending),
                "invocation cannot finish before the Pending dependency"
            );
            changed.await;
        }
        panic!("bounded service state transitions did not reach dependency or audit");
    }
}
impl Drop for PendingInitialDependencyFixture {
    fn drop(&mut self) {
        if let Some(actor) = self.actor.take() {
            let _ = actor.shutdown();
        }
    }
}
