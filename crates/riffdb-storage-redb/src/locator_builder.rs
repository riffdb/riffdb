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
impl BuilderQueue {
    fn global() -> Result<Arc<Self>, StorageError> {
        static QUEUE: OnceLock<Result<Arc<BuilderQueue>, StorageErrorKind>> = OnceLock::new();
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
                    let capability = job.access.locator_capability().clone();
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        process_job(&job)
                    }));
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
                    drop(job);
                    // Release the active predicate only after its actual
                    // source/engine reference is gone. Temporary pressure and
                    // cancellation leave Pending for a later admitted attempt.
                    capability.release_build_request();
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
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        state.push(job).map_err(|_| unavailable())?;
        self.ready.notify_one();
        Ok(())
    }
}

fn source_has_external_owner(access: &RedbReadAccess) -> bool {
    match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            Arc::strong_count(root) > 1
        }
        RedbReadAccess::Composite(view) => Arc::strong_count(view) > 1,
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
    let capability = job.access.locator_capability();
    if !capability.is_pending() {
        return Ok(());
    }
    if !job_has_external_owner(job) {
        return Ok(());
    }
    verify_job_custody(job)?;
    let (database, incarnation, registry) = capability.source_binding()?;
    let owner = capability.construction_owner()?;
    // The captured-source builder admits its native row/frame/protobuf
    // workspace and independently enforces the existing recovery ceiling.
    let custody_failure = RefCell::new(None);
    let cancelled = || {
        if !job_has_external_owner(job) || custody_failure.borrow().is_some() {
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
        if !job_has_external_owner(job) {
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
        if !job_has_external_owner(job) {
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
    if !capability.request_build() {
        return Ok(());
    }
    let result = (|| {
        let pool = NativeLocatorPool::global();
        let charge = pool.admit(
            std::mem::size_of::<BuilderJob>() + 64,
            LocatorAllocationClass::Ordinary,
        )?;
        let queue = BuilderQueue::global()?;
        queue.enqueue(BuilderJob {
            access,
            shared: shared.as_ref().map(Arc::downgrade),
            custody,
            _charge: charge,
        })
    })();
    if result.is_err() {
        capability.release_build_request();
    }
    result
}

#[cfg(test)]
#[path = "locator_builder_tests.rs"]
mod tests;
