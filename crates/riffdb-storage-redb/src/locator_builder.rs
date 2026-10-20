//! One admitted background constructor per pending captured source.
//!
//! Readiness and point reads never walk history. A process-wide worker consumes
//! a fixed queue; each job owns its exact immutable source and pending owner's
//! reservation. Jobs retain no strong reference back to the shared engine.

use std::cell::RefCell;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};

use riffdb_storage_api::{StorageError, StorageErrorKind};

use crate::locator_memory::{
    LOCATOR_MAX_OWNERS, LocatorAllocationClass, LocatorCharge, NativeLocatorPool,
};
use crate::store::{RedbReadAccess, SharedRedb};
use crate::transient::{LocatorGeneration, TransientIndexes};

const BUILDER_STACK_BYTES: usize = 2 * 1024 * 1024;
const BUILDER_CONTROL_BYTES: usize = 64 * 1024;

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct BuilderJob {
    access: RedbReadAccess,
    shared: Option<Weak<SharedRedb>>,
    custody: Option<Arc<crate::startup::ValidatedReadCustody>>,
    registration: Option<crate::locator_build_domain::BuildRegistration>,
    #[cfg(feature = "test-fixtures")]
    lifetime_controller: Option<crate::hooks::RedbTestController>,
    #[cfg(test)]
    panic_during_construction: bool,
    _charge: LocatorCharge,
}
struct BuilderState {
    jobs: Box<[Option<BuilderJob>; LOCATOR_MAX_OWNERS]>,
    read: usize,
    write: usize,
    len: usize,
    enabled: bool,
}
impl BuilderState {
    fn take_domain(
        &mut self,
        domain: &crate::locator_build_domain::LocatorBuildDomain,
    ) -> Option<BuilderJob> {
        for offset in 0..self.len {
            let index = (self.read + offset) % LOCATOR_MAX_OWNERS;
            let matches = self.jobs[index]
                .as_ref()
                .and_then(|job| job.registration.as_ref())
                .is_some_and(|registration| registration.domain().same_domain(domain));
            if !matches {
                continue;
            }
            let removed = self.jobs[index].take();
            for later in offset..self.len - 1 {
                let destination = (self.read + later) % LOCATOR_MAX_OWNERS;
                let source = (destination + 1) % LOCATOR_MAX_OWNERS;
                self.jobs[destination] = self.jobs[source].take();
            }
            self.write = (self.write + LOCATOR_MAX_OWNERS - 1) % LOCATOR_MAX_OWNERS;
            self.jobs[self.write] = None;
            self.len -= 1;
            return removed;
        }
        None
    }
    fn push(&mut self, job: BuilderJob) -> Result<(), BuilderJob> {
        if !self.enabled || self.len == LOCATOR_MAX_OWNERS {
            return Err(job);
        }
        self.jobs[self.write] = Some(job);
        self.write = (self.write + 1) % LOCATOR_MAX_OWNERS;
        self.len += 1;
        Ok(())
    }
    fn pop(&mut self) -> Option<BuilderJob> {
        if self.len == 0 {
            return None;
        }
        let result = self.jobs[self.read].take();
        self.read = (self.read + 1) % LOCATOR_MAX_OWNERS;
        self.len -= 1;
        result
    }
}
struct BuilderQueue {
    state: Mutex<BuilderState>,
    ready: Condvar,
    _charge: LocatorCharge,
}
static QUEUE: OnceLock<Result<Arc<BuilderQueue>, StorageErrorKind>> = OnceLock::new();
impl BuilderQueue {
    fn global() -> Result<Arc<Self>, StorageError> {
        match QUEUE.get_or_init(|| Self::start().map_err(|error| error.kind())) {
            Ok(queue) => Ok(Arc::clone(queue)),
            Err(kind) => Err(StorageError::new(*kind, None)),
        }
    }
    fn start() -> Result<Arc<Self>, StorageError> {
        let pool = NativeLocatorPool::global();
        // The fixed array's construction may occupy a stack temporary as well
        // as the final heap buffer. Admit both before constructing either.
        let queue_bytes = 2 * std::mem::size_of::<[Option<BuilderJob>; LOCATOR_MAX_OWNERS]>()
            + std::mem::size_of::<Self>()
            + 256;
        let queue_charge = pool.admit(queue_bytes, LocatorAllocationClass::Maintenance)?;
        let thread_charge = pool.admit(
            BUILDER_STACK_BYTES + BUILDER_CONTROL_BYTES,
            LocatorAllocationClass::Maintenance,
        )?;
        let queue = Arc::new(Self {
            state: Mutex::new(BuilderState {
                jobs: Box::new(std::array::from_fn(|_| None)),
                read: 0,
                write: 0,
                len: 0,
                enabled: true,
            }),
            ready: Condvar::new(),
            _charge: queue_charge,
        });
        let worker_queue = Arc::clone(&queue);
        let handle = std::thread::Builder::new()
            .name("riffdb-locator-builder".into())
            .stack_size(BUILDER_STACK_BYTES)
            .spawn(move || {
                let _thread_charge = thread_charge;
                // Failure is contained to the original job. A panic cannot
                // publish partial membership or abandon the rest of the queue.
                loop {
                    let job = {
                        let mut state = lock(&worker_queue.state);
                        while state.len == 0 {
                            state = worker_queue
                                .ready
                                .wait(state)
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                        }
                        state.pop()
                    };
                    let Some(job) = job else {
                        continue;
                    };
                    process_and_finish_job(job);
                    std::thread::yield_now();
                }
            })
            .map_err(|_| unavailable())?;
        drop(handle);
        Ok(queue)
    }
    fn enqueue(&self, job: BuilderJob) -> Result<(), StorageError> {
        // Poisoned queue admission refuses; its existing owners remain available
        // to the cleanup worker through the recovering lock above.
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(error) => {
                drop(error);
                finish_job(job);
                return Err(unavailable());
            }
        };
        if job
            .registration
            .as_ref()
            .is_some_and(|registration| registration.is_cancelled())
        {
            drop(state);
            finish_job(job);
            return Err(unavailable());
        }
        match state.push(job) {
            Ok(()) => {
                self.ready.notify_one();
                Ok(())
            }
            Err(job) => {
                drop(state);
                finish_job(job);
                Err(unavailable())
            }
        }
    }
}

fn process_and_finish_job(job: BuilderJob) {
    let capability = job.access.locator_capability().clone();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| process_job(&job)));
    match outcome {
        Ok(Err(error))
            if !matches!(
                error.kind(),
                StorageErrorKind::Unavailable | StorageErrorKind::LimitExceeded
            ) =>
        {
            capability.invalidate(error.kind())
        }
        Err(_) => capability.invalidate(StorageErrorKind::Unavailable),
        Ok(_) => {}
    }
    #[cfg(feature = "test-fixtures")]
    if let Some(controller) = &job.lifetime_controller {
        controller.checkpoint_test_pause(
            crate::hooks::RedbCheckpointTestPoint::BeforeLocatorJobSourceRelease,
        );
    }
    finish_job(job);
}

struct FinishRegistration {
    capability: crate::locator_capability::LocatorCapability,
    registration: Option<crate::locator_build_domain::BuildRegistration>,
}
impl Drop for FinishRegistration {
    fn drop(&mut self) {
        self.capability.release_build_request();
        drop(self.registration.take());
    }
}
fn finish_job(mut job: BuilderJob) {
    let finishing = FinishRegistration {
        capability: job.access.locator_capability().clone(),
        registration: job.registration.take(),
    };
    drop(job);
    drop(finishing);
}
fn root_domain(
    root: &crate::checkpoint_root::CheckpointRoot,
) -> Result<crate::locator_build_domain::LocatorBuildDomain, StorageError> {
    match root.engine_custody() {
        Some(crate::checkpoint_root::CheckpointEngineCustody::Writable { owner }) => {
            Ok(owner.locator_build_domain().clone())
        }
        Some(crate::checkpoint_root::CheckpointEngineCustody::ReadOnly { owner }) => {
            Ok(owner.locator_build_domain().clone())
        }
        None => Err(unavailable()),
    }
}
fn source_domain(
    access: &RedbReadAccess,
) -> Result<crate::locator_build_domain::LocatorBuildDomain, StorageError> {
    match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => root_domain(root),
        RedbReadAccess::Composite(view) => root_domain(&view.checkpoint_root_shared()),
    }
}
pub(crate) fn source_construction_cancelled(access: &RedbReadAccess) -> Result<bool, StorageError> {
    Ok(source_domain(access)?.is_cancelled())
}
pub(crate) fn cancel_queued_for_close(
    domain: &crate::locator_build_domain::LocatorBuildDomain,
    deadline: std::time::Instant,
) -> Result<(), StorageError> {
    let Some(Ok(queue)) = QUEUE.get() else {
        return Ok(());
    };
    for _ in 0..=LOCATOR_MAX_OWNERS {
        if std::time::Instant::now() >= deadline {
            return Err(unavailable());
        }
        let mut state = queue.state.try_lock().map_err(|_| unavailable())?;
        let next = state.take_domain(domain);
        drop(state);
        let Some(job) = next else {
            return Ok(());
        };
        finish_job(job);
    }
    Err(unavailable())
}
fn source_has_external_owner(access: &RedbReadAccess) -> bool {
    match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => !root.is_uniquely_owned(),
        RedbReadAccess::Composite(view) => !view.is_uniquely_owned(),
    }
}

fn job_has_external_owner(job: &BuilderJob) -> bool {
    if job
        .custody
        .as_ref()
        .is_some_and(|custody| Arc::strong_count(custody) == 1)
    {
        return false;
    }
    source_has_external_owner(&job.access)
        || job
            .shared
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|shared| shared.owns_current_locator_source(&job.access))
}

fn verify_job_custody(job: &BuilderJob) -> Result<(), StorageError> {
    match &job.custody {
        Some(custody) => custody.verify(),
        None => Ok(()),
    }
}

fn checked_construction<T>(
    result: Result<T, StorageError>,
    failure: &RefCell<Option<StorageError>>,
) -> Result<T, StorageError> {
    if let Some(error) = failure.borrow_mut().take() {
        return Err(error);
    }
    result
}

fn process_job(job: &BuilderJob) -> Result<(), StorageError> {
    if job
        .registration
        .as_ref()
        .is_some_and(|registration| registration.is_cancelled())
    {
        return Ok(());
    }
    let capability = job.access.locator_capability();
    if !capability.is_pending() {
        return Ok(());
    }
    if !job_has_external_owner(job) {
        return Ok(());
    }
    verify_job_custody(job)?;
    #[cfg(test)]
    if job.panic_during_construction {
        panic!("fixture construction panic while owning the actual source");
    }
    let (database, incarnation, registry) = capability.source_binding()?;
    let owner = capability.construction_owner()?;
    // The captured-source builder admits its native row/frame/protobuf
    // workspace and independently enforces the existing recovery ceiling.
    let custody_failure = RefCell::new(None);
    let cancelled = || {
        if job
            .registration
            .as_ref()
            .is_some_and(|registration| registration.is_cancelled())
            || !job_has_external_owner(job)
            || custody_failure.borrow().is_some()
        {
            return true;
        }
        if let Err(error) = verify_job_custody(job) {
            *custody_failure.borrow_mut() = Some(error);
            return true;
        }
        false
    };
    if job.shared.is_some() {
        let (indexes, _) = checked_construction(
            TransientIndexes::rebuild_from_access_with_owner(
                &job.access,
                owner,
                LocatorAllocationClass::Ordinary,
                cancelled,
            ),
            &custody_failure,
        )?;
        verify_job_custody(job)?;
        if job
            .registration
            .as_ref()
            .is_some_and(|registration| registration.is_cancelled())
            || !job_has_external_owner(job)
        {
            return Ok(());
        }
        if let Some(shared) = job.shared.as_ref().and_then(Weak::upgrade) {
            shared.complete_locator_indexes(&job.access, indexes)?;
        } else {
            let generation = indexes.locator_generation()?;
            generation.validate_database_incarnation(database, incarnation)?;
            capability.verify_source_binding(database, incarnation, registry)?;
            capability.install_generation(generation)?;
        }
    } else {
        let generation = checked_construction(
            LocatorGeneration::rebuild_from_access_with_owner(
                &job.access,
                owner,
                LocatorAllocationClass::Ordinary,
                cancelled,
            ),
            &custody_failure,
        )?;
        verify_job_custody(job)?;
        if job
            .registration
            .as_ref()
            .is_some_and(|registration| registration.is_cancelled())
            || !job_has_external_owner(job)
        {
            return Ok(());
        }
        generation.validate_database_incarnation(database, incarnation)?;
        capability.verify_source_binding(database, incarnation, registry)?;
        capability.install_generation(generation)?;
    }
    Ok(())
}

/// Enqueue one construction for this exact source. Duplicate requests share its
/// pending owner; the queue never clones an index or captures a later source.
pub(crate) fn start_locator_builder(
    access: RedbReadAccess,
    shared: Option<Arc<SharedRedb>>,
) -> Result<(), StorageError> {
    start_locator_builder_with_custody(access, shared, None)
}

pub(crate) fn start_locator_builder_with_custody(
    access: RedbReadAccess,
    shared: Option<Arc<SharedRedb>>,
    custody: Option<Arc<crate::startup::ValidatedReadCustody>>,
) -> Result<(), StorageError> {
    if let Some(custody) = &custody {
        custody.verify()?;
    }
    let capability = access.locator_capability().clone();
    if !capability.is_pending() {
        return capability.require_complete().map(|_| ());
    }
    capability.source_binding()?;
    let domain = source_domain(&access)?;
    let registration = domain.register()?;
    if !capability.request_build() {
        drop(access);
        drop(shared);
        drop(custody);
        drop(registration);
        return Ok(());
    }
    #[cfg(feature = "test-fixtures")]
    let lifetime_controller = shared
        .as_ref()
        .and_then(|shared| shared.locator_test_controller());
    let weak_shared = shared.as_ref().map(Arc::downgrade);
    drop(shared);
    let charge = match NativeLocatorPool::global().admit(
        std::mem::size_of::<BuilderJob>() + 64,
        LocatorAllocationClass::Ordinary,
    ) {
        Ok(charge) => charge,
        Err(error) => {
            drop(access);
            drop(custody);
            capability.release_build_request();
            drop(registration);
            return Err(error);
        }
    };
    let job = BuilderJob {
        access,
        shared: weak_shared,
        custody,
        registration: Some(registration),
        #[cfg(feature = "test-fixtures")]
        lifetime_controller,
        #[cfg(test)]
        panic_during_construction: false,
        _charge: charge,
    };
    let queue = match BuilderQueue::global() {
        Ok(queue) => queue,
        Err(error) => {
            finish_job(job);
            return Err(error);
        }
    };
    queue.enqueue(job)
}

#[cfg(test)]
#[path = "locator_builder_tests.rs"]
mod tests;

#[cfg(all(test, feature = "test-fixtures"))]
pub(crate) fn global_queue_contains_source_for_test(access: &RedbReadAccess) -> bool {
    let Some(Ok(queue)) = QUEUE.get() else {
        return false;
    };
    lock(&queue.state).jobs.iter().flatten().any(|job| {
        job.access
            .locator_capability()
            .same_construction(access.locator_capability())
    })
}
