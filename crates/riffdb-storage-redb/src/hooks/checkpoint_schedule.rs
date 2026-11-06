//! Closed one-shot pauses for the real checkpoint/journal/publication path.
//! No production open installs a controller and this module is fixture-only.
#![expect(
    clippy::expect_used,
    reason = "fixture-only schedules fail the test on poisoned synchronization or a reused controller"
)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use super::RedbTestController;

/// Independently scheduled storage boundaries; none substitutes an I/O result.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub enum RedbCheckpointTestPoint {
    BeforeMaterialization,
    Materialized,
    BeforeJournalSync,
    BeforePublication,
    BeforeCheckpointCommit,
    BeforeReclaim,
    Reclaimed,
    AfterCommandSeal,
    BeforeCommandApply,
    BeforeSealPublication,
    BeforeSparePreparation,
    BeforeOutboxCheckpointPreparation,
    BeforeConsumerCheckpointPreparation,
    BeforeLocatorJobSourceRelease,
    BeforeFinalCleanCommit,
    LocatorCloseDrainWaiting,
    BeforeLocatorPublicationLease,
    BeforeSourceUsagePublication,
}

#[derive(Default)]
struct Pause {
    state: Mutex<PauseState>,
    changed: Condvar,
}

#[derive(Default)]
struct PauseState {
    armed: bool,
    entered: bool,
    released: bool,
    left: bool,
}

impl Pause {
    fn enter(&self) {
        let mut state = self.state.lock().expect("schedule lock");
        if !state.armed {
            return;
        }
        state.armed = false;
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).expect("schedule release");
        }
        state.left = true;
    }

    fn release(&self) {
        self.state.lock().expect("schedule lock").released = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
pub(super) struct CheckpointScheduleInner {
    points: [Pause; 18],
    requested: AtomicBool,
}

/// Bounded deterministic schedule. Dropping it releases every paused worker,
/// including when an assertion fails; the watchdog is not synchronization.
#[doc(hidden)]
pub struct RedbCheckpointTestSchedule {
    inner: Arc<CheckpointScheduleInner>,
}

impl RedbCheckpointTestSchedule {
    /// Requests one real checkpoint at the next quiescent nonempty suffix.
    pub fn request_checkpoint(&self) {
        assert!(!self.inner.requested.swap(true, Ordering::AcqRel));
    }

    pub fn arm(&self, point: RedbCheckpointTestPoint) {
        let mut state = self.inner.points[point as usize]
            .state
            .lock()
            .expect("schedule lock");
        assert!(!state.armed && (!state.entered || state.left));
        *state = PauseState {
            armed: true,
            ..PauseState::default()
        };
    }

    pub fn wait(&self, point: RedbCheckpointTestPoint) {
        let pause = &self.inner.points[point as usize];
        let (state, _) = pause
            .changed
            .wait_timeout_while(
                pause.state.lock().expect("schedule lock"),
                Duration::from_secs(60),
                |state| !state.entered,
            )
            .expect("schedule observation");
        let entered = state.entered;
        drop(state);
        assert!(entered, "storage did not reach {point:?}");
    }

    // Internal fixture failure handshake. Release is cancellation, never proof
    // of entry; the caller must report its actual writer error when false.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn wait_entered_or_released(&self, point: RedbCheckpointTestPoint) -> bool {
        let pause = &self.inner.points[point as usize];
        let (state, _) = pause
            .changed
            .wait_timeout_while(
                pause.state.lock().expect("schedule lock"),
                Duration::from_secs(60),
                |state| !state.entered && !state.released,
            )
            .expect("schedule observation");
        state.entered
    }

    pub fn release(&self, point: RedbCheckpointTestPoint) {
        self.inner.points[point as usize].release();
    }
}

impl Drop for RedbCheckpointTestSchedule {
    fn drop(&mut self) {
        for pause in &self.inner.points {
            pause.release();
        }
    }
}

impl RedbTestController {
    #[must_use]
    pub fn checkpoint_schedule() -> (Self, RedbCheckpointTestSchedule) {
        Self::observe_index_migration().with_checkpoint_schedule()
    }

    /// Adds a schedule before this controller has been cloned or installed.
    #[must_use]
    pub fn with_checkpoint_schedule(mut self) -> (Self, RedbCheckpointTestSchedule) {
        let inner = Arc::new(CheckpointScheduleInner::default());
        Arc::get_mut(&mut self.inner)
            .expect("fresh test controller")
            .checkpoint_schedule = Some(Arc::clone(&inner));
        (self, RedbCheckpointTestSchedule { inner })
    }

    pub(crate) fn checkpoint_test_pause(&self, point: RedbCheckpointTestPoint) {
        if let Some(schedule) = &self.inner.checkpoint_schedule {
            schedule.points[point as usize].enter();
        }
    }

    pub(crate) fn take_checkpoint_test_request(&self) -> bool {
        self.inner
            .checkpoint_schedule
            .as_ref()
            .is_some_and(|schedule| schedule.requested.swap(false, Ordering::AcqRel))
    }

    pub(crate) fn is_locator_builder_test_point_armed(&self) -> bool {
        self.inner
            .checkpoint_schedule
            .as_ref()
            .is_some_and(|schedule| {
                [
                    RedbCheckpointTestPoint::BeforeLocatorJobSourceRelease,
                    RedbCheckpointTestPoint::BeforeLocatorPublicationLease,
                ]
                .into_iter()
                .any(|point| {
                    schedule.points[point as usize]
                        .state
                        .lock()
                        .map(|state| state.armed)
                        .unwrap_or(false)
                })
            })
    }
}
