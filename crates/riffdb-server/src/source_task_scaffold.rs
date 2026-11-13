//! Inactive private lifecycle scaffold. No Source/public/daemon entry is wired.
//! A concrete predecessor implementation of ClosedSourceBody is still required.
#![allow(
    clippy::collapsible_if,
    clippy::result_large_err,
    clippy::wrong_self_convention
)]

use std::any::Any;
use std::future::Future;
use std::mem::{align_of, size_of, size_of_val};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread, ThreadId};
use std::time::Instant;

use riffdb_types::exact_provider_budget_v1::{
    ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
};
use riffdb_types::owned_allocation_capacity_bound_v1;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::{JoinError, JoinHandle};

const ORDINARY_WORKERS: usize = 8;
static OWNER_LIVE: AtomicBool = AtomicBool::new(false);
// The sole private owner claim prevents a second live or retained scaffold.
// Static inline storage adds no per-request allocation. No source graph escapes it.
static ESCROW: Mutex<Option<RetainedScaffold>> = Mutex::new(None);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    UnsupportedBuild,
    UnsupportedWorkers,
    WrongRoot,
    ForeignLedger,
    Busy,
    TimedOut,
    Panicked,
    Capacity,
    SerialExhausted,
    Invariant,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Poison does not permit an early refund or discard of a live physical owner.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn allocation(bytes: usize, alignment: usize) -> Result<u64, Refusal> {
    owned_allocation_capacity_bound_v1(bytes, alignment).map_err(|_| Refusal::Capacity)
}
fn add(left: u64, right: u64) -> Result<u64, Refusal> {
    left.checked_add(right).ok_or(Refusal::Capacity)
}
fn align_up(bytes: usize, alignment: usize) -> Result<usize, Refusal> {
    if !alignment.is_power_of_two() {
        return Err(Refusal::UnsupportedBuild);
    }
    bytes
        .checked_add(alignment - 1)
        .map(|n| n & !(alignment - 1))
        .ok_or(Refusal::Capacity)
}

// The pinned std ArcInner<T> is repr(C): two AtomicUsize fields, then T.
// This is a layout-only mirror, never an ownership or pointer conversion.
#[repr(C)]
struct ArcLayout<T> {
    strong: AtomicUsize,
    weak: AtomicUsize,
    value: T,
}
fn arc_allocation<T>() -> Result<u64, Refusal> {
    allocation(size_of::<ArcLayout<T>>(), align_of::<ArcLayout<T>>())
}
fn callback_allocation<F: Fn() + Send + Sync>(callback: &F) -> Result<u64, Refusal> {
    let alignment = align_of::<F>().max(align_of::<AtomicUsize>());
    let header = size_of::<AtomicUsize>()
        .checked_mul(2)
        .ok_or(Refusal::Capacity)?;
    let bytes = align_up(header, align_of::<F>())?
        .checked_add(size_of_val(callback))
        .ok_or(Refusal::Capacity)?;
    allocation(align_up(bytes, alignment)?, alignment)
}

// Exact supported-build guard is emitted only by the reviewed build-script draft.
// Unknown compiler/target/Tokio cfg gets no pinned task quote. Ordinary compilation
// is otherwise unchanged; no environment switch can enable this factory.
fn require_task_layout() -> Result<(), Refusal> {
    if cfg!(riffdb_source_tokio_task_layout) {
        Ok(())
    } else {
        Err(Refusal::UnsupportedBuild)
    }
}

// Pinned Tokio 1.54.1: repr(C) Header = State(AtomicUsize), Option<NonNull>,
// vtable reference, Option<NonZeroU64>. Core = Arc<Handle>, Id, repr(C) Stage.
// Trailer = linked-list Pointers(two nullable pointers), Option<Waker>,
// TaskHarnessScheduleHooks(one optional fat Arc callback). tokio_unstable is
// refused, so spawned_at/tracing-ID/taskdump wrappers are absent.
#[repr(C)]
#[expect(
    dead_code,
    reason = "repr(C) layout mirror used via size_of/align_of for Tokio task cell sizing"
)]
enum StageTag {
    Running,
    Finished,
    Consumed,
}
fn task_cell_allocation<F: Future<Output = ()>>() -> Result<u64, Refusal> {
    require_task_layout()?;
    if size_of::<usize>() != 8 || align_of::<F>() > 16 {
        return Err(Refusal::UnsupportedBuild);
    }
    let word = size_of::<usize>();
    let header = word.checked_mul(4).ok_or(Refusal::Capacity)?;
    let stage_alignment = align_of::<F>()
        .max(align_of::<Result<(), JoinError>>())
        .max(align_of::<StageTag>());
    let payload = size_of::<F>().max(size_of::<Result<(), JoinError>>());
    let stage = align_up(
        align_up(size_of::<StageTag>(), stage_alignment)?
            .checked_add(payload)
            .ok_or(Refusal::Capacity)?,
        stage_alignment,
    )?;
    let core_alignment = stage_alignment.max(word);
    let core = align_up(
        align_up(
            word.checked_mul(2).ok_or(Refusal::Capacity)?,
            stage_alignment,
        )?
        .checked_add(stage)
        .ok_or(Refusal::Capacity)?,
        core_alignment,
    )?;
    let trailer = word
        .checked_mul(2)
        .and_then(|n| n.checked_add(size_of::<Option<Waker>>()))
        .and_then(|n| n.checked_add(size_of::<Option<Arc<dyn Fn() + Send + Sync>>>()))
        .ok_or(Refusal::Capacity)?;
    let bytes = align_up(header, core_alignment)?
        .checked_add(core)
        .and_then(|n| n.checked_add(trailer))
        .ok_or(Refusal::Capacity)?;
    // Pinned x86_64 Cell is repr(C, align(128)).
    allocation(align_up(bytes, 128)?, 128)
}

// Sealed by module privacy: no public callback, waker, context, or raw task API.
// Root will add the one concrete owned predecessor implementation here. Its run
// call must consume all non-Send native locals before returning a Send outcome.
trait ClosedSourceBody: Send + 'static {
    type Output: Send + 'static;
    fn actual_budget(&self) -> &ProjectionSourceWorkingBudgetV1;
    fn run(self) -> Self::Output;
}

struct SourceOperationFuture<B> {
    registry: Arc<Registry>,
    serial: u64,
    body: Option<Box<B>>,
}
impl<B: ClosedSourceBody> Future for SourceOperationFuture<B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<()> {
        // No context/waker leaves this method and there is no await/block_in_place.
        let this = self.get_mut();
        let Some(body) = this.body.take() else {
            return Poll::Ready(());
        };
        if !this.registry.begin_actual_poll(this.serial) {
            drop(body);
            this.registry
                .record_refusal(this.serial, Refusal::UnsupportedWorkers);
            return Poll::Ready(());
        }
        let output = (*body).run();
        // This exact Box<Output> allocation is included in the prior task quote.
        let output: Box<dyn Any + Send> = Box::new(output);
        this.registry.store_actual_output(this.serial, output);
        Poll::Ready(())
    }
}
impl<B> Drop for SourceOperationFuture<B> {
    fn drop(&mut self) {
        // Marker alone proves nothing. Ack is later, on this exact worker's park.
        self.registry.finish_actual_poll(self.serial);
    }
}
fn task_quote<B: ClosedSourceBody>() -> Result<u64, Refusal> {
    require_task_layout()?;
    let future = size_of::<SourceOperationFuture<B>>();
    if future > 2048 {
        // This scaffold's named future is deliberately fixed-size. Do not silently
        // acquire Tokio's separate future Box without a reviewed quote branch.
        return Err(Refusal::UnsupportedBuild);
    }
    add(
        add(
            task_cell_allocation::<SourceOperationFuture<B>>()?,
            arc_allocation::<RootWake>()?,
        )?,
        add(
            allocation(size_of::<B>(), align_of::<B>())?,
            allocation(size_of::<B::Output>(), align_of::<B::Output>())?,
        )?,
    )
}

struct RootWake {
    root: Thread,
    #[cfg(test)]
    before_notify: Option<Arc<tests::WakeGate>>,
}
impl Wake for RootWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        #[cfg(test)]
        if let Some(gate) = &self.before_notify {
            gate.before_notify();
        }
        self.root.unpark();
    }
}

struct PendingJob {
    serial: u64,
    worker: Option<ThreadId>,
    finished_serial: Option<u64>,
    acknowledged_serial: Option<u64>,
    join: Option<JoinHandle<()>>,
    joined: Option<Result<(), JoinError>>,
    output: Option<Box<dyn Any + Send>>,
    refusal: Option<Refusal>,
    wake: Option<Arc<RootWake>>,
    // Must be retired explicitly, after task/output/Wake backing is destroyed.
    work: Option<ProjectionSourceWorkReservationV1>,
}
// Only observe_actual_park supplies production identity/event values. This is
// a private state transition, not a detached proof/constructor or public setter.
fn acknowledge_park_slot(pending: &mut PendingJob, worker: ThreadId, serial: Option<u64>) {
    if pending.worker == Some(worker)
        && serial == Some(pending.serial)
        && pending.finished_serial == Some(pending.serial)
    {
        pending.acknowledged_serial = Some(pending.serial);
    }
}
struct RegistryState {
    workers: [Option<ThreadId>; ORDINARY_WORKERS],
    worker_count: usize,
    frozen: bool,
    next_serial: u64,
    pending: Option<PendingJob>,
}
struct Registry {
    root: Thread,
    root_id: ThreadId,
    state: Mutex<RegistryState>,
}
impl Registry {
    fn observe_actual_park(&self) {
        let current = thread::current().id();
        let mut state = lock(&self.state);
        if !state.frozen
            && !state.workers.contains(&Some(current))
            && state.worker_count < ORDINARY_WORKERS
        {
            let index = state.worker_count;
            state.workers[index] = Some(current);
            state.worker_count += 1;
        }
        if let Some(pending) = &mut state.pending {
            let serial = pending.finished_serial;
            acknowledge_park_slot(pending, current, serial);
        }
        // This callback has no wait, spawn, user body or source/native inspection.
        self.root.unpark();
    }
    fn begin_actual_poll(&self, serial: u64) -> bool {
        let current = thread::current().id();
        let mut state = lock(&self.state);
        let supported = state.frozen && state.workers.contains(&Some(current));
        let Some(pending) = &mut state.pending else {
            return false;
        };
        if pending.serial != serial || pending.worker.is_some() {
            return false;
        }
        // Even a refused replacement worker must acknowledge its physical task.
        pending.worker = Some(current);
        supported
    }
    fn finish_actual_poll(&self, serial: u64) {
        let current = thread::current().id();
        let mut state = lock(&self.state);
        if let Some(pending) = &mut state.pending {
            if pending.serial == serial && pending.worker == Some(current) {
                pending.finished_serial = Some(serial);
            }
        }
    }
    fn store_actual_output(&self, serial: u64, output: Box<dyn Any + Send>) {
        let mut state = lock(&self.state);
        if let Some(pending) = &mut state.pending {
            if pending.serial == serial {
                pending.output = Some(output);
                return;
            }
        }
        // No caller can replace/remove the live slot while this task runs.
        drop(output);
    }
    fn record_refusal(&self, serial: u64, refusal: Refusal) {
        let mut state = lock(&self.state);
        if let Some(pending) = &mut state.pending {
            if pending.serial == serial {
                pending.refusal = Some(refusal);
            }
        }
    }
}

// The output Box drops/deallocates BEFORE the original task allowance refunds.
// No into_parts/raw Arc/Weak or uncharged output extraction API is provided.
struct SourceTaskCompletion<T> {
    output: Box<T>,
    _work: ProjectionSourceWorkReservationV1,
}
impl<T> std::ops::Deref for SourceTaskCompletion<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.output
    }
}

struct SourceRuntimeScaffold {
    runtime: Option<Runtime>,
    registry: Option<Arc<Registry>>,
    registry_work: Option<ProjectionSourceWorkReservationV1>,
    callback_work: Option<ProjectionSourceWorkReservationV1>,
    budget: ProjectionSourceWorkingBudgetV1,
    bootstrap_succeeded: bool,
}
struct RetainedScaffold {
    runtime: Option<Runtime>,
    registry: Option<Arc<Registry>>,
    registry_work: Option<ProjectionSourceWorkReservationV1>,
    callback_work: Option<ProjectionSourceWorkReservationV1>,
    _budget: ProjectionSourceWorkingBudgetV1,
    bootstrap_succeeded: bool,
    native_join_verified: bool,
}
impl SourceRuntimeScaffold {
    fn bootstrap_on_actual_ledger(
        budget: &ProjectionSourceWorkingBudgetV1,
    ) -> Result<Self, Refusal> {
        require_task_layout()?;
        let registry_bytes = arc_allocation::<Registry>()?;
        if OWNER_LIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Refusal::Busy);
        }
        let registry_work = match budget.reserve(registry_bytes) {
            Ok(work) => work,
            Err(_) => {
                OWNER_LIVE.store(false, Ordering::Release);
                return Err(Refusal::Capacity);
            }
        };
        // The real daemon integration must supply the already-owned ordinary
        // root Thread. Tests already run on an initialized Rust test thread.
        let root = thread::current();
        let registry = Arc::new(Registry {
            root_id: root.id(),
            root,
            state: Mutex::new(RegistryState {
                workers: [None; ORDINARY_WORKERS],
                worker_count: 0,
                frozen: false,
                next_serial: 1,
                pending: None,
            }),
        });
        let callback_registry = Arc::clone(&registry);
        let callback = move || callback_registry.observe_actual_park();
        let callback_bytes = match callback_allocation(&callback) {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(callback);
                drop(registry);
                drop(registry_work);
                OWNER_LIVE.store(false, Ordering::Release);
                return Err(error);
            }
        };
        let callback_work = match budget.reserve(callback_bytes) {
            Ok(work) => work,
            Err(_) => {
                drop(callback);
                drop(registry);
                drop(registry_work);
                OWNER_LIVE.store(false, Ordering::Release);
                return Err(Refusal::Capacity);
            }
        };
        let mut owner = Self {
            runtime: None,
            registry: Some(registry),
            registry_work: Some(registry_work),
            callback_work: Some(callback_work),
            budget: budget.clone(),
            bootstrap_succeeded: false,
        };
        let mut builder = Builder::new_multi_thread();
        builder
            .worker_threads(ORDINARY_WORKERS)
            .enable_all()
            .on_thread_park(callback);
        let runtime = builder.build();
        // Builder's own callback Arc alias is gone before any retirement test.
        drop(builder);
        match runtime {
            Ok(runtime) => {
                owner.runtime = Some(runtime);
                owner.bootstrap_succeeded = true;
                Ok(owner)
            }
            Err(_) => {
                drop(owner);
                Err(Refusal::Capacity)
            }
        }
    }
    fn registry(&self) -> Result<&Arc<Registry>, Refusal> {
        // All methods require the owned live scaffold; into_retained consumes it.
        self.registry.as_ref().ok_or(Refusal::Invariant)
    }
    fn require_root(&self) -> Result<(), Refusal> {
        if thread::current().id() == self.registry()?.root_id {
            Ok(())
        } else {
            Err(Refusal::WrongRoot)
        }
    }
    fn freeze_after_actual_parks(&self, deadline: Instant) -> Result<(), Refusal> {
        self.require_root()?;
        let registry = self.registry()?;
        loop {
            {
                let mut state = lock(&registry.state);
                if state.worker_count == ORDINARY_WORKERS {
                    state.frozen = true;
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(Refusal::TimedOut);
            }
            thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
        }
    }
    fn start<B: ClosedSourceBody>(&self, body: B) -> Result<(), Refusal> {
        #[cfg(test)]
        {
            self.start_inner(body, None)
        }
        #[cfg(not(test))]
        {
            self.start_inner(body)
        }
    }
    fn start_inner<B: ClosedSourceBody>(
        &self,
        body: B,
        #[cfg(test)] before_notify: Option<Arc<tests::WakeGate>>,
    ) -> Result<(), Refusal> {
        self.require_root()?;
        if !self.budget.shares_owner_with(body.actual_budget()) {
            return Err(Refusal::ForeignLedger);
        }
        let registry = self.registry()?;
        let mut state = lock(&registry.state);
        if !state.frozen {
            return Err(Refusal::UnsupportedWorkers);
        }
        if state.pending.is_some() {
            return Err(Refusal::Busy);
        }
        let serial = state.next_serial;
        state.next_serial = serial.checked_add(1).ok_or(Refusal::SerialExhausted)?;
        let work = self
            .budget
            .reserve(task_quote::<B>()?)
            .map_err(|_| Refusal::Capacity)?;
        let wake = Arc::new(RootWake {
            root: registry.root.clone(),
            #[cfg(test)]
            before_notify,
        });
        state.pending = Some(PendingJob {
            serial,
            worker: None,
            finished_serial: None,
            acknowledged_serial: None,
            join: None,
            joined: None,
            output: None,
            refusal: None,
            wake: Some(wake),
            work: Some(work),
        });
        // This lock excludes source poll/park until the actual handle is installed.
        // Native Source semantics are never run while the parent holds this lock.
        let future = SourceOperationFuture {
            registry: Arc::clone(registry),
            serial,
            body: Some(Box::new(body)),
        };
        let join = self
            .runtime
            .as_ref()
            .ok_or(Refusal::Invariant)?
            .spawn(future);
        state.pending.as_mut().ok_or(Refusal::Invariant)?.join = Some(join);
        Ok(())
    }
    fn poll_once(&self) -> Result<(), Refusal> {
        self.require_root()?;
        let registry = self.registry()?;
        let mut state = lock(&registry.state);
        let pending = state.pending.as_mut().ok_or(Refusal::Invariant)?;
        if pending.joined.is_some() {
            return Ok(());
        }
        let wake = Arc::clone(pending.wake.as_ref().ok_or(Refusal::Invariant)?);
        let waker = Waker::from(wake);
        let mut context = Context::from_waker(&waker);
        if let Poll::Ready(result) =
            Pin::new(pending.join.as_mut().ok_or(Refusal::Invariant)?).poll(&mut context)
        {
            pending.joined = Some(result);
        }
        // Local Waker alias dies before any successful retirement/unique extraction.
        drop(waker);
        Ok(())
    }
    fn wait<T: Send + 'static>(
        &self,
        deadline: Instant,
    ) -> Result<SourceTaskCompletion<T>, Refusal> {
        self.require_root()?;
        let registry = self.registry()?;
        loop {
            self.poll_once()?;
            let ready = {
                let state = lock(&registry.state);
                state
                    .pending
                    .as_ref()
                    .is_some_and(|p| p.joined.is_some() && p.acknowledged_serial == Some(p.serial))
            };
            if ready {
                return self.take_acknowledged();
            }
            if Instant::now() >= deadline {
                return Err(Refusal::TimedOut);
            }
            thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
        }
    }
    fn take_acknowledged<T: Send + 'static>(&self) -> Result<SourceTaskCompletion<T>, Refusal> {
        self.require_root()?;
        let registry = self.registry()?;
        let mut state = lock(&registry.state);
        if !state
            .pending
            .as_ref()
            .is_some_and(|p| p.joined.is_some() && p.acknowledged_serial == Some(p.serial))
        {
            return Err(Refusal::Busy);
        }
        let mut pending = state.pending.take().ok_or(Refusal::Invariant)?;
        drop(state);
        // Post-poll same-worker ack, no exported task/abort/waker handles: now this
        // is the final task reference. Task Cell/Trailer/Waker are destroyed first.
        drop(pending.join.take());
        let wake = pending.wake.take().ok_or(Refusal::Invariant)?;
        match Arc::try_unwrap(wake) {
            Ok(wake) => drop(wake),
            Err(wake) => {
                pending.wake = Some(wake);
                lock(&registry.state).pending = Some(pending);
                return Err(Refusal::Busy);
            }
        }
        let joined = pending.joined.take().ok_or(Refusal::Invariant)?;
        if joined.is_err() {
            drop(joined);
            drop(pending.output.take());
            drop(pending.work.take());
            return Err(Refusal::Panicked);
        }
        if let Some(refusal) = pending.refusal {
            drop(pending.output.take());
            drop(pending.work.take());
            return Err(refusal);
        }
        let output = pending.output.take().ok_or(Refusal::Invariant)?;
        let output = match output.downcast::<T>() {
            Ok(output) => output,
            Err(output) => {
                drop(output);
                drop(pending.work.take());
                return Err(Refusal::Invariant);
            }
        };
        Ok(SourceTaskCompletion {
            output,
            _work: pending.work.take().ok_or(Refusal::Invariant)?,
        })
    }
    fn into_retained(&mut self) -> RetainedScaffold {
        RetainedScaffold {
            runtime: self.runtime.take(),
            registry: self.registry.take(),
            registry_work: self.registry_work.take(),
            callback_work: self.callback_work.take(),
            _budget: self.budget.clone(),
            bootstrap_succeeded: self.bootstrap_succeeded,
            native_join_verified: false,
        }
    }
    fn finish_joined(mut self) -> Result<(), Refusal> {
        self.require_root()?;
        if Handle::try_current().is_ok() {
            return Err(Refusal::Busy);
        }
        let retained = self.into_retained();
        match retained.join_and_retire() {
            Ok(()) => Ok(()),
            Err(retained) => {
                *lock(&ESCROW) = Some(retained);
                Err(Refusal::Busy)
            }
        }
    }
}
impl Drop for SourceRuntimeScaffold {
    fn drop(&mut self) {
        if self.registry.is_none() {
            return;
        }
        // No timeout/cancel/Drop may discard a live JoinHandle or reservation.
        // OWNER_LIVE remains true, blocking every new scaffold/domain bootstrap.
        let retained = self.into_retained();
        let mut escrow = lock(&ESCROW);
        if escrow.is_some() {
            // Constructor's exclusive claim makes this branch unreachable. If an
            // internal invariant breaks, preserve both charges instead of refund.
            std::mem::forget(retained);
            return;
        }
        *escrow = Some(retained);
    }
}
impl RetainedScaffold {
    fn join_and_retire(mut self) -> Result<(), Self> {
        // A failed/panicking Builder may have partially launched workers without
        // returning a Runtime. Neither Arc uniqueness nor a missing Runtime is a
        // native-join witness. Keep this one bootstrap escrow until process exit.
        if !self.bootstrap_succeeded {
            return Err(self);
        }
        {
            let Some(registry) = self.registry.as_ref() else {
                return Err(self);
            };
            if thread::current().id() != registry.root_id
                || Handle::try_current().is_ok()
                || thread::panicking()
            {
                return Err(self);
            }
        }
        // No shutdown_timeout: this is the actual private full native-join path.
        // Caller must first drain ordinary work. The scaffold does not manufacture
        // that daemon lifecycle proof or expose this as an application operation.
        if let Some(runtime) = self.runtime.take() {
            drop(runtime);
            // Set only after actual Runtime Drop/native joins returned normally.
            self.native_join_verified = true;
        }
        if !self.native_join_verified {
            return Err(self);
        }
        let pending = {
            let Some(registry) = self.registry.as_ref() else {
                return Err(self);
            };
            lock(&registry.state).pending.take()
        };
        if let Some(mut pending) = pending {
            // Actual Runtime/native join replaces the absent park acknowledgement.
            drop(pending.join.take());
            drop(pending.joined.take());
            drop(pending.output.take());
            let Some(wake) = pending.wake.take() else {
                return Err(self);
            };
            match Arc::try_unwrap(wake) {
                Ok(wake) => drop(wake),
                Err(wake) => {
                    pending.wake = Some(wake);
                    if let Some(registry) = self.registry.as_ref() {
                        lock(&registry.state).pending = Some(pending);
                    }
                    return Err(self);
                }
            }
            drop(pending.work.take());
        }
        let Some(registry) = self.registry.take() else {
            return Err(self);
        };
        match Arc::try_unwrap(registry) {
            Ok(registry) => drop(registry),
            Err(registry) => {
                self.registry = Some(registry);
                return Err(self);
            }
        }
        // Full runtime/handle/callback destruction happened before these refunds.
        drop(self.callback_work.take());
        drop(self.registry_work.take());
        OWNER_LIVE.store(false, Ordering::Release);
        Ok(())
    }
}
fn reap_retained_after_daemon_drain() -> Result<(), Refusal> {
    let mut escrow = lock(&ESCROW);
    let Some(retained) = escrow.as_ref() else {
        return Ok(());
    };
    if retained
        .registry
        .as_ref()
        .is_none_or(|r| r.root_id != thread::current().id())
        || Handle::try_current().is_ok()
    {
        return Err(Refusal::WrongRoot);
    }
    let retained = escrow.take().ok_or(Refusal::Invariant)?;
    drop(escrow);
    match retained.join_and_retire() {
        Ok(()) => Ok(()),
        Err(retained) => {
            *lock(&ESCROW) = Some(retained);
            Err(Refusal::Busy)
        }
    }
}

#[cfg(test)]
#[path = "source_task_scaffold_tests.rs"]
mod tests;

#[path = "source_execution_startup.rs"]
mod production_startup;
#[cfg_attr(not(test), allow(unused_imports))]
#[cfg(test)]
pub(crate) use production_startup::SourceExecutionStartup;
