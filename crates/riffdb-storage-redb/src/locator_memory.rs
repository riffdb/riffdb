//! Admitted native locator ownership and bounded, allocation-free final release.
//!
//! Queue descriptors are reserved before constructing an allocation. Its final
//! reference only moves a precreated task into that descriptor. A task may drop
//! other locator handles: their destructors enqueue, rather than recurse.

use std::borrow::Borrow;
use std::cell::Cell;
use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};

use riffdb_storage_api::{StorageError, StorageErrorKind};

pub(crate) const LOCATOR_TOTAL_BYTES: usize = 512 * 1024 * 1024;
pub(crate) const LOCATOR_ORDINARY_BYTES: usize = 448 * 1024 * 1024;
pub(crate) const LOCATOR_MAX_OWNERS: usize = 1024;
pub(crate) const LOCATOR_RETIRE_QUOTA: usize = 256;
const SLOT_CHUNK: usize = 256;
const MAX_CHUNKS: usize = 8192;
const ALLOCATION_OVERHEAD: usize = 64;
const WORKER_STACK_BYTES: usize = 256 * 1024;
const WORKER_CONTROL_BYTES: usize = 64 * 1024;
// Each outstanding owner can close at most one epoch, and admission counts
// owners through physical retirement. One additional epoch stays open for new
// independent work. Its fixed storage is included in the queue's early charge.
const RETIREMENT_EPOCHS: usize = LOCATOR_MAX_OWNERS + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocatorAllocationClass {
    Ordinary,
    Maintenance,
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Reclamation cannot abandon admitted ownership after another thread panics.
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Default)]
struct BudgetState {
    admission_enabled: bool,
    total: usize,
    ordinary: usize,
    owners: usize,
    ordinary_owners: usize,
}
#[derive(Debug)]
struct Budget {
    state: Mutex<BudgetState>,
    total_limit: usize,
    ordinary_limit: usize,
}

#[derive(Debug)]
pub(crate) struct LocatorCharge {
    budget: Arc<Budget>,
    bytes: u32,
    class: LocatorAllocationClass,
    family: Option<riffdb_types::OwnedCapacitySharedV1<LocatorFamily>>,
    ordinal: u64,
}
impl Budget {
    fn admit(
        self: &Arc<Self>,
        bytes: usize,
        class: LocatorAllocationClass,
    ) -> Result<LocatorCharge, StorageError> {
        let charge_bytes = u32::try_from(bytes).map_err(|_| unavailable())?;
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if !state.admission_enabled {
            return Err(unavailable());
        }
        let total = state.total.checked_add(bytes).ok_or_else(unavailable)?;
        let ordinary = if class == LocatorAllocationClass::Ordinary {
            state.ordinary.checked_add(bytes).ok_or_else(unavailable)?
        } else {
            state.ordinary
        };
        if total > self.total_limit || ordinary > self.ordinary_limit {
            return Err(unavailable());
        }
        state.total = total;
        state.ordinary = ordinary;
        Ok(LocatorCharge {
            budget: self.clone(),
            bytes: charge_bytes,
            class,
            family: None,
            ordinal: 0,
        })
    }
}

impl LocatorCharge {
    pub(crate) fn admit_maintenance(&self, bytes: usize) -> Result<LocatorCharge, StorageError> {
        self.budget
            .admit(bytes, LocatorAllocationClass::Maintenance)
    }

    pub(crate) const fn bytes(&self) -> usize {
        self.bytes as usize
    }
}
impl Drop for LocatorCharge {
    fn drop(&mut self) {
        let bytes = self.bytes as usize;
        if let Some(family) = &self.family {
            family
                .accounting
                .release_member_bytes(self.ordinal, u64::from(self.bytes));
        }
        let mut state = lock(&self.budget.state);
        state.total -= bytes;
        if self.class == LocatorAllocationClass::Ordinary {
            state.ordinary -= bytes;
        }
    }
}

// One family contains only allocations shared by immutable successors. It
// retains no source/tree; physical member charges keep its control alive.
const FAMILY_MARKER_BYTES: u64 = 128;
struct LocatorFamily {
    accounting: riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamily,
    // Global-only: charging the family to itself would create an immortal cycle.
    _control: LocatorCharge,
}
impl fmt::Debug for LocatorFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LocatorFamily")
    }
}

pub(crate) struct LocatorFamilyGuard<'a> {
    inner: riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamilyGuard<'a>,
}

pub(crate) struct PreparedLocatorFamilyAdmission<'a>(
    riffdb_types::exact_provider_budget_v1::PreparedPhysicalFamilyAdmission<'a>,
);
impl PreparedLocatorFamilyAdmission<'_> {
    pub(crate) fn commit(self) -> riffdb_types::exact_provider_budget_v1::SharedOwnershipCharge {
        self.0.commit()
    }
}
impl LocatorFamilyGuard<'_> {
    #[cfg(test)]
    pub(crate) fn observation(
        &self,
    ) -> riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamilyObservation {
        self.inner.observation()
    }
    #[cfg(test)]
    pub(crate) fn provider_owned_bytes(&self) -> u64 {
        self.inner.provider_owned_bytes()
    }
    pub(crate) fn provider_ownership_needed(
        &self,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<u64, StorageError> {
        self.inner
            .provider_ownership_needed(budget)
            .map_err(|_| unavailable())
    }
    pub(crate) fn prepare_provider_ownership<'a>(
        &'a mut self,
        work: &mut riffdb_types::exact_provider_budget_v1::Reservation,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<PreparedLocatorFamilyAdmission<'a>, StorageError> {
        self.inner
            .prepare_provider_ownership(work, budget)
            .map(PreparedLocatorFamilyAdmission)
            .map_err(|_| unavailable())
    }
}

/// Original payloads keep only their provider-family bytes here. Their own
/// wrapper/control allocation is admitted separately under its existing native
/// or overlay ceiling; the payload never enters native locator accounting.
pub(crate) struct ProviderOnlyFamilyMember {
    _member: riffdb_types::exact_provider_budget_v1::PhysicalOwnershipMember,
    // Last: the typed ledger and its native control remain alive through release.
    family: riffdb_types::OwnedCapacitySharedV1<LocatorFamily>,
}
impl ProviderOnlyFamilyMember {
    pub(crate) fn same_provider_family(&self, pool: &NativeLocatorPool) -> bool {
        pool.family
            .as_ref()
            .is_some_and(|family| family.same_allocation(&self.family))
    }
}

pub(crate) trait LocatorRetireContents: Send + Sync + 'static {
    /// Extra heap-allocation header allowance beyond the common Arc owner and
    /// boxed retirement task. Payload-owning values retain the original third
    /// allowance unless their closed constructor proves no extra allocation.
    const ADDITIONAL_ALLOCATION_ALLOWANCE: usize = ALLOCATION_OVERHEAD;

    /// Destroy at most one bounded ownership item. Return true only when no
    /// population-sized destruction remains in this value's ordinary Drop.
    fn retire_one(&mut self) -> bool;
    /// Only a final generation owner may close an epoch: its existing quota
    /// reservation bounds the fixed epoch ring through physical retirement.
    fn is_retirement_barrier(&self) -> bool {
        false
    }
}
trait RetirementTask: Send {
    fn retire_one(&mut self) -> bool;
    fn is_retirement_barrier(&self) -> bool;
    fn retire_box(self: Box<Self>);
}
struct RetirementTaskOwner(Option<Box<dyn RetirementTask>>);
impl RetirementTaskOwner {
    fn retire_one(&mut self) -> bool {
        self.0.as_mut().is_some_and(|task| task.retire_one())
    }
    fn is_retirement_barrier(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|task| task.is_retirement_barrier())
    }
}
impl Drop for RetirementTaskOwner {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.retire_box();
        }
    }
}
#[derive(Default)]
struct Slot {
    task: Option<RetirementTaskOwner>,
    next: Option<usize>,
    barrier: bool,
    epoch: usize,
}
struct SlotChunk {
    slots: Box<[Slot; SLOT_CHUNK]>,
    _charge: LocatorCharge,
}
struct RetirementState {
    chunks: Vec<SlotChunk>,
    free: Option<usize>,
    queued: Option<usize>,
    queued_tail: Option<usize>,
    queued_count: usize,
    non_barrier_count: usize,
    epoch_outstanding: [usize; RETIREMENT_EPOCHS],
    open_epoch: usize,
    oldest_epoch: usize,
    // Both queue storage and its admission have process lifetime.
    _directory_charge: LocatorCharge,
}
impl RetirementState {
    fn slot(&mut self, index: usize) -> &mut Slot {
        &mut self.chunks[index / SLOT_CHUNK].slots[index % SLOT_CHUNK]
    }
}

struct RetirementQueue {
    state: Mutex<RetirementState>,
    ready: Condvar,
}

#[derive(Clone, Copy)]
struct RetirementContext {
    queue: &'static RetirementQueue,
    epoch: usize,
}
thread_local! {
    static RETIRING: Cell<Option<RetirementContext>> = const { Cell::new(None) };
    #[cfg(test)]
    static OWNER_QUOTA_RELEASE_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}
struct RetirementContextGuard(Option<RetirementContext>);
impl RetirementContextGuard {
    fn enter(queue: &'static RetirementQueue, epoch: usize) -> Self {
        Self(RETIRING.with(|context| context.replace(Some(RetirementContext { queue, epoch }))))
    }
}
impl Drop for RetirementContextGuard {
    fn drop(&mut self) {
        // This Cell has no destructor or heap state. Restoration also runs
        // during unwinding, before any later independent release on this thread.
        let _ = RETIRING.try_with(|context| context.set(self.0));
    }
}
struct ActiveRetirementEpoch {
    queue: &'static RetirementQueue,
    epoch: usize,
    armed: bool,
}
impl Drop for ActiveRetirementEpoch {
    fn drop(&mut self) {
        if self.armed {
            lock(&self.queue.state).epoch_outstanding[self.epoch] -= 1;
        }
    }
}

#[derive(Clone)]
pub(crate) struct NativeLocatorPool(riffdb_types::OwnedCapacitySharedV1<NativeLocatorPoolInner>);
// The shared payload is private to native ownership. This dereference exposes
// no fields, raw allocation handle, mutation or authority outside this module.
pub(crate) struct NativeLocatorPoolInner {
    budget: Arc<Budget>,
    retirement: &'static RetirementQueue,
    // Serializes workers, not readers or the lifetime of a captured snapshot.
    worker: Arc<Mutex<()>>,
    family: Option<riffdb_types::OwnedCapacitySharedV1<LocatorFamily>>,
}
impl AsRef<NativeLocatorPool> for NativeLocatorPool {
    fn as_ref(&self) -> &NativeLocatorPool {
        self
    }
}
impl Deref for NativeLocatorPool {
    type Target = NativeLocatorPoolInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for NativeLocatorPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.budget.state);
        f.debug_struct("NativeLocatorPool")
            .field("charged_bytes", &state.total)
            .field("generation_owners", &state.owners)
            .finish_non_exhaustive()
    }
}
impl NativeLocatorPool {
    pub(crate) fn source_family(
        &self,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        if self.family.is_some() {
            return Ok((self).clone());
        }
        self.fresh_source_family(class)
    }

    /// Independent construction never imports another family's binding. Only
    /// successors and recovery of an original owner use source_family reuse.
    pub(crate) fn fresh_source_family(
        &self,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let control_bytes = std::mem::size_of::<NativeLocatorPoolInner>()
            + std::mem::size_of::<LocatorFamily>()
            + riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamily::control_payload_bytes(
            ) + 5 * ALLOCATION_OVERHEAD;
        let control = self.admit(control_bytes + FAMILY_MARKER_BYTES as usize, class)?;
        let family = riffdb_types::OwnedCapacitySharedV1::new(LocatorFamily {
            accounting: riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamily::new(
                control_bytes as u64,
            ),
            _control: control,
        });
        Ok(Self(riffdb_types::OwnedCapacitySharedV1::new(
            NativeLocatorPoolInner {
                budget: self.budget.clone(),
                retirement: self.retirement,
                worker: self.worker.clone(),
                family: Some(family),
            },
        )))
    }

    fn admit_retained(
        &self,
        bytes: usize,
        class: LocatorAllocationClass,
    ) -> Result<LocatorCharge, StorageError> {
        let mut charge = self.admit(bytes, class)?;
        if let Some(family) = &self.family {
            let next = family
                .accounting
                .register_member_bytes(bytes as u64)
                .map_err(|_| unavailable())?;
            charge.family = Some((family).clone());
            charge.ordinal = next;
        }
        Ok(charge)
    }

    pub(crate) fn shares_native_budget(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.budget, &other.budget)
    }

    pub(crate) fn same_provider_family(&self, other: &Self) -> bool {
        match (&self.family, &other.family) {
            (Some(first), Some(second)) => first.same_allocation(second),
            _ => false,
        }
    }

    pub(crate) fn provider_family_identity(&self) -> usize {
        self.family
            .as_ref()
            .map_or(0, |family| family.accounting.identity())
    }

    pub(crate) fn lock_provider_family(&self) -> Result<LocatorFamilyGuard<'_>, StorageError> {
        let family = self.family.as_ref().ok_or_else(unavailable)?;
        Ok(LocatorFamilyGuard {
            inner: family.accounting.lock().map_err(|_| unavailable())?,
        })
    }

    pub(crate) fn provider_accounting_family(
        &self,
    ) -> Result<&riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamily, StorageError>
    {
        self.family
            .as_ref()
            .map(|family| &family.accounting)
            .ok_or_else(unavailable)
    }

    pub(crate) fn register_external_provider_member(
        &self,
        bytes: u64,
    ) -> Result<ProviderOnlyFamilyMember, StorageError> {
        let family = self.family.as_ref().ok_or_else(unavailable)?;
        let member = family
            .accounting
            .register_member(bytes)
            .map_err(|_| unavailable())?;
        Ok(ProviderOnlyFamilyMember {
            _member: member,
            family: (family).clone(),
        })
    }

    pub(crate) fn global() -> Self {
        static POOL: OnceLock<NativeLocatorPool> = OnceLock::new();
        (POOL.get_or_init(|| {
            let pool = Self::new(LOCATOR_TOTAL_BYTES, LOCATOR_ORDINARY_BYTES);
            if Self::start_worker(&pool).is_err() {
                lock(&pool.budget.state).admission_enabled = false;
            }
            pool
        }))
        .clone()
    }

    fn new(total_limit: usize, ordinary_limit: usize) -> Self {
        // Fixed directory capacity bounds every subsequent descriptor admission.
        // Charge its complete capacity before allocating it; it never grows.
        let directory_bytes = MAX_CHUNKS * std::mem::size_of::<SlotChunk>()
            + std::mem::size_of::<NativeLocatorPoolInner>()
            + std::mem::size_of::<Mutex<()>>()
            + std::mem::size_of::<Budget>()
            + std::mem::size_of::<RetirementQueue>()
            + 5 * ALLOCATION_OVERHEAD;
        let budget = Arc::new(Budget {
            state: Mutex::new(BudgetState {
                admission_enabled: true,
                total: directory_bytes,
                ..BudgetState::default()
            }),
            total_limit,
            ordinary_limit,
        });
        let directory_charge = LocatorCharge {
            budget: budget.clone(),
            bytes: u32::try_from(directory_bytes).unwrap_or(u32::MAX),
            class: LocatorAllocationClass::Maintenance,
            family: None,
            ordinal: 0,
        };
        // Production has one global pool. Isolated tests use the same explicit
        // process-lifetime queue, so final release never needs a fallback drain.
        let retirement = Box::leak(Box::new(RetirementQueue {
            state: Mutex::new(RetirementState {
                chunks: Vec::with_capacity(MAX_CHUNKS),
                free: None,
                queued: None,
                queued_tail: None,
                queued_count: 0,
                non_barrier_count: 0,
                epoch_outstanding: [0; RETIREMENT_EPOCHS],
                open_epoch: 0,
                oldest_epoch: 0,
                _directory_charge: directory_charge,
            }),
            ready: Condvar::new(),
        }));
        Self(riffdb_types::OwnedCapacitySharedV1::new(
            NativeLocatorPoolInner {
                budget,
                retirement,
                worker: Arc::new(Mutex::new(())),
                family: None,
            },
        ))
    }

    fn start_worker(pool: &Self) -> Result<(), StorageError> {
        // Charge the fixed stack and conservative OS/thread-control reservation
        // before asking the OS to allocate either resource.
        let charge = pool.admit(
            WORKER_STACK_BYTES + WORKER_CONTROL_BYTES,
            LocatorAllocationClass::Maintenance,
        )?;
        let worker_pool = (pool).clone();
        let handle = std::thread::Builder::new()
            .name("riffdb-locator-reaper".into())
            .stack_size(WORKER_STACK_BYTES)
            .spawn(move || {
                let _admission_guard = WorkerAdmissionGuard(worker_pool.clone());
                let _charge = charge;
                loop {
                    let mut state = lock(&worker_pool.retirement.state);
                    while state.queued.is_none() {
                        state = worker_pool
                            .retirement
                            .ready
                            .wait(state)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                    drop(state);
                    loop {
                        let more = worker_pool.drain_retired_step();
                        std::thread::yield_now();
                        if !more {
                            break;
                        }
                    }
                }
            })
            .map_err(|_| unavailable())?;
        // The process-global worker deliberately lives until process exit.
        drop(handle);
        Ok(())
    }

    pub(crate) fn admit(
        &self,
        bytes: usize,
        class: LocatorAllocationClass,
    ) -> Result<LocatorCharge, StorageError> {
        self.budget.admit(bytes, class)
    }

    fn reserve_slot(&self, class: LocatorAllocationClass) -> Result<RetirementSlot, StorageError> {
        let mut state = self.retirement.state.lock().map_err(|_| unavailable())?;
        if state.free.is_none() {
            if state.chunks.len() == MAX_CHUNKS {
                return Err(unavailable());
            }
            let charge = self.admit(
                std::mem::size_of::<[Slot; SLOT_CHUNK]>() + ALLOCATION_OVERHEAD,
                class,
            )?;
            let start = state.chunks.len() * SLOT_CHUNK;
            let slots = Box::new(std::array::from_fn(|offset| Slot {
                task: None,
                barrier: false,
                epoch: 0,
                next: if offset + 1 < SLOT_CHUNK {
                    Some(start + offset + 1)
                } else {
                    None
                },
            }));
            state.chunks.push(SlotChunk {
                slots,
                _charge: charge,
            });
            state.free = Some(start);
        }
        let index = state.free.ok_or_else(unavailable)?;
        state.free = state.slot(index).next.take();
        let index = u32::try_from(index).map_err(|_| unavailable())?;
        Ok(RetirementSlot {
            retirement: self.retirement,
            index,
            armed: true,
        })
    }

    /// Execute no more than 256 charged destruction items. Callers schedule
    /// another step after yielding when this returns true.
    pub(crate) fn drain_retired_step(&self) -> bool {
        let _worker = lock(&self.worker);
        for _ in 0..LOCATOR_RETIRE_QUOTA {
            let next = {
                let mut state = lock(&self.retirement.state);
                let Some(index) = state.queued else {
                    return false;
                };
                state.queued = state.slot(index).next.take();
                if state.queued.is_none() {
                    state.queued_tail = None;
                }
                state.queued_count -= 1;
                let barrier = state.slot(index).barrier;
                let epoch = state.slot(index).epoch;
                let eligible = !barrier
                    || (epoch == state.oldest_epoch && state.epoch_outstanding[epoch] == 0);
                if !barrier {
                    state.non_barrier_count -= 1;
                }
                state
                    .slot(index)
                    .task
                    .take()
                    .map(|task| (index, task, barrier, epoch, eligible))
            };
            let Some((index, task, barrier, epoch, eligible)) = next else {
                continue;
            };
            // A child's final Drop inherits its parent's closed epoch even if
            // the child first enters the queue after that owner's barrier.
            // Keep the active task counted until its physical Drop finishes.
            let context = (!barrier).then(|| RetirementContextGuard::enter(self.retirement, epoch));
            let mut active_epoch = (!barrier).then_some(ActiveRetirementEpoch {
                queue: self.retirement,
                epoch,
                armed: !barrier,
            });
            // Initialize the owning local after the context: on unwinding its
            // physical Drop and descendant enqueue run before TLS restoration.
            let mut task = task;
            let complete = eligible && task.retire_one();
            let mut state = lock(&self.retirement.state);
            if complete {
                if barrier {
                    // Make the closed epoch reusable before physical Owner
                    // Drop makes its quota available to another constructor.
                    state.oldest_epoch = (state.oldest_epoch + 1) % RETIREMENT_EPOCHS;
                }
                // Drop task before descriptor reuse. Its children only enqueue.
                drop(state);
                drop(task);
                state = lock(&self.retirement.state);
                let free = state.free;
                state.slot(index).next = free;
                state.free = Some(index);
            } else {
                state.slot(index).task = Some(task);
                if let Some(active) = &mut active_epoch {
                    // Requeued work still owns its outstanding count.
                    active.armed = false;
                }
                state.slot(index).next = None;
                if let Some(tail) = state.queued_tail {
                    state.slot(tail).next = Some(index);
                } else {
                    state.queued = Some(index);
                }
                state.queued_tail = Some(index);
                state.queued_count += 1;
                if !barrier {
                    state.non_barrier_count += 1;
                }
            }
            drop(state);
            drop(active_epoch);
            drop(context);
        }
        lock(&self.retirement.state).queued.is_some()
    }

    #[cfg(test)]
    pub(crate) fn isolated_for_tests() -> Self {
        Self::new(LOCATOR_TOTAL_BYTES, LOCATOR_ORDINARY_BYTES)
    }

    #[cfg(test)]
    pub(crate) fn exhaust_ordinary_for_tests(&self) -> LocatorCharge {
        let remaining = self.budget.ordinary_limit - lock(&self.budget.state).ordinary;
        self.admit(remaining, LocatorAllocationClass::Ordinary)
            .expect("remaining ordinary capacity")
    }

    #[cfg(test)]
    pub(crate) fn charged_bytes(&self) -> usize {
        lock(&self.budget.state).total
    }
}

struct WorkerAdmissionGuard(NativeLocatorPool);
impl Drop for WorkerAdmissionGuard {
    fn drop(&mut self) {
        // An unexpected worker exit must not leave ordinary construction
        // admitting ownership that can no longer make reclamation progress.
        lock(&self.0.budget.state).admission_enabled = false;
    }
}

struct RetirementSlot {
    retirement: &'static RetirementQueue,
    index: u32,
    armed: bool,
}
impl RetirementSlot {
    fn enqueue(mut self, task: RetirementTaskOwner) {
        self.armed = false;
        let slot_index = self.index as usize;
        let mut state = lock(&self.retirement.state);
        let barrier = task.is_retirement_barrier();
        let epoch = if barrier {
            let closed = state.open_epoch;
            state.open_epoch = (closed + 1) % RETIREMENT_EPOCHS;
            closed
        } else {
            let inherited = RETIRING
                .try_with(|context| {
                    context.get().and_then(|context| {
                        std::ptr::eq(context.queue, self.retirement).then_some(context.epoch)
                    })
                })
                .ok()
                .flatten();
            let epoch = inherited.unwrap_or(state.open_epoch);
            state.epoch_outstanding[epoch] += 1;
            epoch
        };
        let slot = state.slot(slot_index);
        slot.task = Some(task);
        slot.next = None;
        slot.barrier = barrier;
        slot.epoch = epoch;
        if let Some(tail) = state.queued_tail {
            state.slot(tail).next = Some(slot_index);
        } else {
            state.queued = Some(slot_index);
        }
        state.queued_tail = Some(slot_index);
        state.queued_count += 1;
        if !barrier {
            state.non_barrier_count += 1;
        }
        self.retirement.ready.notify_one();
    }
}
impl Drop for RetirementSlot {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let slot_index = self.index as usize;
        let mut state = lock(&self.retirement.state);
        let free = state.free;
        state.slot(slot_index).next = free;
        state.free = Some(slot_index);
    }
}

struct AllocationTaskPayload<T: LocatorRetireContents> {
    value: T,
    _charge: LocatorCharge,
}
struct AllocationTask<T: LocatorRetireContents> {
    payload: Option<AllocationTaskPayload<T>>,
}
impl<T: LocatorRetireContents> RetirementTask for AllocationTask<T> {
    fn retire_one(&mut self) -> bool {
        self.payload
            .as_mut()
            .is_some_and(|payload| payload.value.retire_one())
    }
    fn is_retirement_barrier(&self) -> bool {
        self.payload
            .as_ref()
            .is_some_and(|payload| payload.value.is_retirement_barrier())
    }
    fn retire_box(mut self: Box<Self>) {
        let payload = self.payload.take();
        drop(self);
        // Actual task Box is gone. Value retirement precedes its charge refund.
        drop(payload);
    }
}
struct AllocationTaskOwner<T: LocatorRetireContents>(Option<Box<AllocationTask<T>>>);
impl<T: LocatorRetireContents> AllocationTaskOwner<T> {
    fn value(&self) -> Option<&T> {
        Some(&self.0.as_ref()?.payload.as_ref()?.value)
    }
    fn value_mut(&mut self) -> Option<&mut T> {
        Some(&mut self.0.as_mut()?.payload.as_mut()?.value)
    }
    fn charge(&self) -> Option<&LocatorCharge> {
        Some(&self.0.as_ref()?.payload.as_ref()?._charge)
    }
    fn into_erased(mut self) -> Option<RetirementTaskOwner> {
        self.0.take().map(|task| RetirementTaskOwner(Some(task)))
    }
}
impl<T: LocatorRetireContents> Drop for AllocationTaskOwner<T> {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.retire_box();
        }
    }
}
struct AllocationOwner<T: LocatorRetireContents> {
    // The live value resides only in its original precreated retirement task.
    // A live Arc always owns both task and slot; final Drop moves that Box.
    task: Option<AllocationTaskOwner<T>>,
    slot: Option<RetirementSlot>,
}
impl<T: LocatorRetireContents> Drop for AllocationOwner<T> {
    fn drop(&mut self) {
        if let (Some(slot), Some(task)) = (self.slot.take(), self.task.take())
            && let Some(task) = task.into_erased()
        {
            slot.enqueue(task);
        }
    }
}
pub(crate) struct ChargedLocatorAllocation<T: LocatorRetireContents>(
    riffdb_types::OwnedCapacitySharedV1<AllocationOwner<T>>,
);
impl<T: LocatorRetireContents> Clone for ChargedLocatorAllocation<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T: LocatorRetireContents + fmt::Debug> fmt::Debug for ChargedLocatorAllocation<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(f)
    }
}
impl<T: LocatorRetireContents> Deref for ChargedLocatorAllocation<T> {
    type Target = T;
    #[expect(
        clippy::expect_used,
        reason = "safe shared Arc ownership retains the private retirement value until final release"
    )]
    fn deref(&self) -> &T {
        // Only final Arc destruction takes the private task. A safe shared
        // borrow keeps this Arc alive and cannot overlap queued mutation.
        self.0
            .task
            .as_ref()
            .and_then(AllocationTaskOwner::value)
            .expect("live locator allocation retains its retirement value")
    }
}
impl<T: LocatorRetireContents> ChargedLocatorAllocation<T> {
    pub(crate) fn get_mut(&mut self) -> Option<&mut T> {
        self.0
            .get_mut()?
            .task
            .as_mut()
            .and_then(AllocationTaskOwner::value_mut)
    }

    /// Independent test allowance for exactly the two wrapper allocations.
    #[cfg(test)]
    pub(crate) fn two_allocation_bound_for_tests() -> usize {
        std::mem::size_of::<AllocationOwner<T>>()
            + std::mem::size_of::<AllocationTask<T>>()
            + 2 * ALLOCATION_OVERHEAD
    }

    pub(crate) fn allocation_charge_bound(content_bytes: usize) -> Option<usize> {
        content_bytes
            .checked_add(std::mem::size_of::<AllocationOwner<T>>())
            .and_then(|n| n.checked_add(std::mem::size_of::<AllocationTask<T>>()))
            .and_then(|n| n.checked_add(2 * ALLOCATION_OVERHEAD))
            .and_then(|n| n.checked_add(T::ADDITIONAL_ALLOCATION_ALLOWANCE))
    }

    /// Conservative attribution of one reserved queue descriptor, independent
    /// of process-global backing which a reader does not itself keep alive.
    pub(crate) const fn retirement_descriptor_bound() -> usize {
        std::mem::size_of::<Slot>() + ALLOCATION_OVERHEAD
    }

    pub(crate) fn retained_charge_bound(&self) -> usize {
        // An accessible Arc owner has not begun final Drop, so its precreated
        // task is still present. A violated invariant refuses all admission.
        self.0
            .task
            .as_ref()
            .and_then(AllocationTaskOwner::charge)
            .map_or(usize::MAX, LocatorCharge::bytes)
            .saturating_add(Self::retirement_descriptor_bound())
    }

    pub(crate) fn ptr_eq(left: &Self, right: &Self) -> bool {
        left.0.same_allocation(&right.0)
    }

    pub(crate) fn validate_family_pool(
        &self,
        pool: &NativeLocatorPool,
    ) -> Result<(), StorageError> {
        let charge = self
            .0
            .task
            .as_ref()
            .and_then(AllocationTaskOwner::charge)
            .ok_or_else(unavailable)?;
        match (&charge.family, &pool.family) {
            (Some(first), Some(second)) if first.same_allocation(second) => Ok(()),
            (None, None) => Ok(()),
            _ => Err(StorageError::new(
                StorageErrorKind::InvariantViolation,
                None,
            )),
        }
    }

    pub(crate) fn try_new(
        pool: &NativeLocatorPool,
        class: LocatorAllocationClass,
        content_bytes: usize,
        construct: impl FnOnce() -> Result<T, StorageError>,
    ) -> Result<Self, StorageError> {
        let bytes = Self::allocation_charge_bound(content_bytes).ok_or_else(unavailable)?;
        let charge = pool.admit_retained(bytes, class)?;
        let slot = pool.reserve_slot(class)?;
        let value = construct()?;
        Ok(Self(riffdb_types::OwnedCapacitySharedV1::new(
            AllocationOwner {
                task: Some(AllocationTaskOwner(Some(Box::new(AllocationTask {
                    payload: Some(AllocationTaskPayload {
                        value,
                        _charge: charge,
                    }),
                })))),
                slot: Some(slot),
            },
        )))
    }
}

const INLINE_KEY_CAPACITY: usize = 30;

#[derive(Debug)]
enum KeyAllocation {
    Inline {
        len: u8,
        bytes: [u8; INLINE_KEY_CAPACITY],
    },
    Heap(Box<[u8]>),
}
impl KeyAllocation {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { len, bytes } => &bytes[..usize::from(*len)],
            Self::Heap(bytes) => bytes,
        }
    }
}
impl LocatorRetireContents for KeyAllocation {
    const ADDITIONAL_ALLOCATION_ALLOWANCE: usize = 0;

    fn retire_one(&mut self) -> bool {
        // Any separately charged physical allocation consumes its own step.
        if let Self::Heap(bytes) = self {
            drop(std::mem::take(bytes));
        }
        true
    }
}
#[derive(Clone, Debug)]
pub(crate) struct ChargedLocatorKey(ChargedLocatorAllocation<KeyAllocation>);
impl ChargedLocatorKey {
    pub(crate) fn validate_family_pool(
        &self,
        pool: &NativeLocatorPool,
    ) -> Result<(), StorageError> {
        self.0.validate_family_pool(pool)
    }

    pub(crate) fn retained_charge_bound(&self) -> usize {
        self.0.retained_charge_bound()
    }

    pub(crate) fn try_new(
        pool: &NativeLocatorPool,
        class: LocatorAllocationClass,
        bytes: &[u8],
    ) -> Result<Self, StorageError> {
        // Both any heap byte buffer and the final-release descriptor are admitted
        // before copying a key. Final release only queues its precreated task.
        let content_bytes = if bytes.len() <= INLINE_KEY_CAPACITY {
            0
        } else {
            bytes
                .len()
                .checked_add(ALLOCATION_OVERHEAD)
                .ok_or_else(unavailable)?
        };
        ChargedLocatorAllocation::try_new(pool, class, content_bytes, || {
            if let Ok(len) = u8::try_from(bytes.len())
                && bytes.len() <= INLINE_KEY_CAPACITY
            {
                let mut inline = [0_u8; INLINE_KEY_CAPACITY];
                inline[..bytes.len()].copy_from_slice(bytes);
                Ok(KeyAllocation::Inline { len, bytes: inline })
            } else {
                Ok(KeyAllocation::Heap(bytes.into()))
            }
        })
        .map(Self)
    }
}
impl AsRef<[u8]> for ChargedLocatorKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}
impl Borrow<[u8]> for ChargedLocatorKey {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}
impl PartialEq for ChargedLocatorKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}
impl Eq for ChargedLocatorKey {}
impl PartialOrd for ChargedLocatorKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ChargedLocatorKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

struct OwnerContents {
    budget: Arc<Budget>,
    pool: NativeLocatorPool,
    class: LocatorAllocationClass,
    released: bool,
    provider_charge: Mutex<Option<riffdb_types::exact_provider_budget_v1::SharedOwnershipCharge>>,
    provider_control_charge:
        Mutex<Option<riffdb_types::exact_provider_budget_v1::SharedOwnershipCharge>>,
}
impl LocatorRetireContents for OwnerContents {
    fn retire_one(&mut self) -> bool {
        // The reaper first removes this epoch's barrier, then physically drops
        // the task. Quota and provider ownership remain until that Drop.
        true
    }
    fn is_retirement_barrier(&self) -> bool {
        // The generation quota, as well as any provider charge, follows all
        // queued descendants through physical destruction.
        true
    }
}
impl Drop for OwnerContents {
    fn drop(&mut self) {
        if !self.released {
            self.provider_charge
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            self.provider_control_charge
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            let mut state = lock(&self.budget.state);
            state.owners -= 1;
            if self.class == LocatorAllocationClass::Ordinary {
                state.ordinary_owners -= 1;
            }
            self.released = true;
            drop(state);
            #[cfg(test)]
            OWNER_QUOTA_RELEASE_HOOK.with(|hook| {
                let callback = hook.borrow_mut().take();
                if let Some(callback) = callback {
                    callback();
                }
            });
        }
    }
}
pub(crate) struct LocatorOwner(ChargedLocatorAllocation<OwnerContents>);
impl Clone for LocatorOwner {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl fmt::Debug for LocatorOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LocatorOwner")
    }
}
impl LocatorOwner {
    pub(crate) fn family_pool(&self) -> NativeLocatorPool {
        self.0.pool.clone()
    }

    /// The pending-provider graph marker has its own fixed slot: it must not
    /// conflict with the later complete generation's distinct numerical charge.
    #[cfg(test)]
    pub(crate) fn attach_provider_control_charge(
        &self,
        candidate: riffdb_types::exact_provider_budget_v1::SharedOwnershipCharge,
    ) -> Result<(), StorageError> {
        let mut charge = self
            .0
            .provider_control_charge
            .lock()
            .map_err(|_| unavailable())?;
        if let Some(existing) = &*charge {
            if !existing.same_allocation(&candidate) {
                return Err(unavailable());
            }
        } else {
            *charge = Some(candidate);
        }
        Ok(())
    }

    pub(crate) fn attach_provider_charge(
        &self,
        candidate: riffdb_types::exact_provider_budget_v1::SharedOwnershipCharge,
    ) -> Result<(), StorageError> {
        let mut charge = self.0.provider_charge.lock().map_err(|_| unavailable())?;
        if let Some(existing) = &*charge {
            if !existing.same_allocation(&candidate) {
                return Err(unavailable());
            }
        } else {
            *charge = Some(candidate);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn retained_charge_bound(&self) -> usize {
        self.0.retained_charge_bound()
    }

    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        ChargedLocatorAllocation::ptr_eq(&self.0, &other.0)
    }

    #[cfg(test)]
    pub(crate) fn try_new(
        pool: &NativeLocatorPool,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        Self::try_new_with_bytes(pool, class, 0)
    }

    /// Charge the separately owned generation descriptor before its caller allocates it.
    pub(crate) fn try_new_with_bytes(
        pool: &NativeLocatorPool,
        class: LocatorAllocationClass,
        descriptor_bytes: usize,
    ) -> Result<Self, StorageError> {
        let pool = pool.source_family(class)?;
        // Admit the optional marker's actual Arc allocation before any provider
        // can attach it. Its reservation follows this same owner/task through
        // final queued retirement, even after Completion has been destroyed.
        let content_bytes = descriptor_bytes
            .checked_add(
                std::mem::size_of::<riffdb_types::exact_provider_budget_v1::OwnershipCharge>()
                    + ALLOCATION_OVERHEAD,
            )
            .ok_or_else(unavailable)?;
        {
            let mut state = pool.budget.state.lock().map_err(|_| unavailable())?;
            if !state.admission_enabled
                || state.owners >= LOCATOR_MAX_OWNERS
                || (class == LocatorAllocationClass::Ordinary
                    && state.ordinary_owners >= LOCATOR_MAX_OWNERS - 1)
            {
                return Err(unavailable());
            }
            state.owners += 1;
            if class == LocatorAllocationClass::Ordinary {
                state.ordinary_owners += 1;
            }
        }
        let result = ChargedLocatorAllocation::try_new(&pool, class, content_bytes, || {
            Ok(OwnerContents {
                budget: pool.budget.clone(),
                pool: pool.clone(),
                class,
                released: false,
                provider_charge: Mutex::new(None),
                provider_control_charge: Mutex::new(None),
            })
        });
        match result {
            Ok(owner) => Ok(Self(owner)),
            Err(error) => {
                let mut state = lock(&pool.budget.state);
                state.owners -= 1;
                if class == LocatorAllocationClass::Ordinary {
                    state.ordinary_owners -= 1;
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[path = "locator_memory_tests.rs"]
mod tests;
