//! The existing joined retention worker's wake/cancellation state.
//! This signal has no engine, mutation, source, checkpoint or clock authority.
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

#[derive(Default)]
struct Signals {
    stop: bool,
    wake: bool,
}

/// Internal graph-composition signal shared by the one joined worker and its
/// native Source driver. A signal cannot start work or authorize a database turn.
#[doc(hidden)]
#[derive(Default)]
pub struct RedbJoinedMaintenanceWorkerControl {
    signals: Mutex<Signals>,
    changed: Condvar,
    cancellation: Arc<AtomicBool>,
}
impl RedbJoinedMaintenanceWorkerControl {
    /// The same cancellation allocation already borrowed by native staging.
    #[doc(hidden)]
    pub fn cancellation(&self) -> &Arc<AtomicBool> {
        &self.cancellation
    }
    /// Called by the existing joining owner after retiring source admission.
    #[doc(hidden)]
    pub fn stop(&self) {
        self.cancellation.store(true, Ordering::Release);
        let mut signals = self
            .signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        signals.stop = true;
        self.changed.notify_all();
    }
    /// Wake the existing worker; does not cancel it or shorten a Source deadline.
    #[doc(hidden)]
    pub fn wake(&self) {
        let mut signals = self
            .signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        signals.wake = true;
        self.changed.notify_all();
    }
    /// Actual joined-worker cancellation; no separate driver stop state.
    #[doc(hidden)]
    pub fn stop_requested(&self) -> bool {
        self.signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stop
    }
    /// Wait on the existing worker's condition variable for the caller's already
    /// computed deadline. This signal creates no independent maintenance timer.
    #[doc(hidden)]
    pub fn pause_observation_slice(&self, timeout: Duration) -> (bool, bool) {
        let signals = self
            .signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut signals, _) = self
            .changed
            .wait_timeout_while(signals, timeout, |signals| !signals.stop && !signals.wake)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let woke = signals.wake;
        signals.wake = false;
        (signals.stop, woke)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: PRJ-004, REC-003, END-008
    #[test]
    fn joined_worker_signal_preserves_wake_and_the_same_cancellation_owner() {
        let signal = RedbJoinedMaintenanceWorkerControl::default();
        let cancellation = Arc::clone(signal.cancellation());
        assert!(!signal.stop_requested());
        signal.wake();
        assert_eq!(
            signal.pause_observation_slice(Duration::ZERO),
            (false, true)
        );
        assert_eq!(
            signal.pause_observation_slice(Duration::ZERO),
            (false, false)
        );
        assert!(!cancellation.load(Ordering::Acquire));
        signal.stop();
        assert!(cancellation.load(Ordering::Acquire));
        assert!(Arc::ptr_eq(&cancellation, signal.cancellation()));
        assert_eq!(
            signal.pause_observation_slice(Duration::from_secs(19)),
            (true, false)
        );
    }
    // req: PRJ-004, REC-003, END-008
    #[test]
    fn joined_worker_stop_before_atomic_wait_cannot_be_lost() {
        let signal = Arc::new(RedbJoinedMaintenanceWorkerControl::default());
        let entered = Arc::new(std::sync::Barrier::new(2));
        let receiver = std::thread::spawn({
            let signal = Arc::clone(&signal);
            let entered = Arc::clone(&entered);
            move || {
                let signals = signal.signals.lock().unwrap();
                // The notifier is released while the wait predicate mutex is held.
                // It must either change stop before waiting or notify after atomic
                // Condvar release; no sleep or scheduler timing selects the result.
                entered.wait();
                let (signals, timed) = signal
                    .changed
                    .wait_timeout_while(signals, Duration::from_secs(19), |signals| !signals.stop)
                    .unwrap();
                assert!(signals.stop);
                assert!(!timed.timed_out());
            }
        });
        entered.wait();
        signal.stop();
        receiver.join().unwrap();
    }
}
