//! Bounded operator-only samples of the process-wide exact-provider pool.
// req: OQ-040, PRJ-004
use riffdb_service::{CursorMonotonicClock, CursorTick};
use riffdb_types::exact_provider_budget_v1::{
    BudgetObservation, EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget,
};
use std::{fmt, time::Duration};

pub(crate) enum ProviderDiagnosticSample {
    Available(BudgetObservation),
    Unavailable,
}
impl ProviderDiagnosticSample {
    pub(crate) fn capture(budget: &ProviderBudget) -> Self {
        budget
            .observation()
            .map(Self::Available)
            .unwrap_or(Self::Unavailable)
    }
}
impl fmt::Display for ProviderDiagnosticSample {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => output.write_str("available=false"),
            Self::Available(sample) => write!(
                output,
                "available=true limit_bytes={} fixed_owner_bytes={} owned_bytes={} reserved_bytes={} reserved_work={} allocations={} released_allocations={} active_owners={} retiring_owners={} pinned_owners={} pins={} released_owners={}",
                EXACT_PROVIDER_BUDGET_BYTES,
                sample.fixed_owner_bytes,
                sample.owned_bytes,
                sample.reserved_bytes,
                sample.reservations,
                sample.allocations,
                sample.released_allocations,
                sample.active_owners,
                sample.retiring_owners,
                sample.pinned_owners,
                sample.pins,
                sample.released_owners
            ),
        }
    }
}

#[derive(Default)]
pub(crate) struct ProviderDiagnosticTicker {
    observed: Option<CursorTick>,
    next: Option<CursorTick>,
    unavailable_reported: bool,
}
impl ProviderDiagnosticTicker {
    fn unavailable_once(&mut self) -> Option<ProviderDiagnosticSample> {
        if self.unavailable_reported {
            return None;
        }
        self.unavailable_reported = true;
        Some(ProviderDiagnosticSample::Unavailable)
    }
    pub(crate) fn poll(
        &mut self,
        clock: &dyn CursorMonotonicClock,
        budget: &ProviderBudget,
    ) -> Option<ProviderDiagnosticSample> {
        let observed = match clock.now() {
            Ok(value) => value,
            Err(_) => return self.unavailable_once(),
        };
        if self.observed.is_some_and(|previous| observed < previous) {
            return self.unavailable_once();
        }
        self.observed = Some(observed);
        if self.next.is_some_and(|next| observed < next) {
            return None;
        }
        let Some(next) = observed.checked_add(Duration::from_secs(60)) else {
            return self.unavailable_once();
        };
        self.next = Some(next);
        match budget.observation() {
            Ok(sample) => {
                self.unavailable_reported = false;
                Some(ProviderDiagnosticSample::Available(sample))
            }
            Err(_) => self.unavailable_once(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_service::CursorClockError;
    use std::sync::atomic::{AtomicU64, Ordering};
    struct Clock(AtomicU64);
    impl CursorMonotonicClock for Clock {
        fn now(&self) -> Result<CursorTick, CursorClockError> {
            let seconds = self.0.load(Ordering::SeqCst);
            if seconds == u64::MAX {
                return Err(CursorClockError);
            }
            CursorTick::from_process_elapsed(Duration::from_secs(seconds))
                .map_err(|_| CursorClockError)
        }
    }
    #[test]
    fn lifecycle_diagnostics_are_fixed_numeric_fields_even_at_counter_maxima() {
        let sample = BudgetObservation {
            fixed_owner_bytes: u64::MAX,
            reserved_bytes: u64::MAX,
            owned_bytes: u64::MAX,
            reservations: u64::MAX,
            allocations: u64::MAX,
            released_allocations: u64::MAX,
            active_owners: u64::MAX,
            retiring_owners: u64::MAX,
            pinned_owners: u64::MAX,
            pins: u64::MAX,
            released_owners: u64::MAX,
        };
        let rendered = ProviderDiagnosticSample::Available(sample).to_string();
        assert!(rendered.len() < 1024);
        assert_eq!(rendered.split_whitespace().count(), 13);
        for field in rendered.split_whitespace().skip(1) {
            let (name, number) = field.split_once('=').unwrap();
            assert!(
                name.bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            );
            assert!(number.parse::<u64>().is_ok());
        }
        assert_eq!(
            ProviderDiagnosticSample::Unavailable.to_string(),
            "available=false"
        );
    }
    #[test]
    fn diagnostic_tick_reports_retention_safe_monotonic_samples_and_one_clock_failure() {
        let budget = ProviderBudget::new();
        let clock = Clock(AtomicU64::new(0));
        let mut ticker = ProviderDiagnosticTicker::default();
        assert!(matches!(
            ticker.poll(&clock, &budget),
            Some(ProviderDiagnosticSample::Available(_))
        ));
        clock.0.store(59, Ordering::SeqCst);
        assert!(ticker.poll(&clock, &budget).is_none());
        let mut owner = budget.register_provider_owner().unwrap();
        owner.pin_admitted().unwrap();
        owner.retire().unwrap();
        clock.0.store(60, Ordering::SeqCst);
        let Some(ProviderDiagnosticSample::Available(sample)) = ticker.poll(&clock, &budget) else {
            panic!("expected due sample");
        };
        assert_eq!(
            (
                sample.active_owners,
                sample.retiring_owners,
                sample.pinned_owners,
                sample.pins
            ),
            (0, 1, 1, 1)
        );
        clock.0.store(59, Ordering::SeqCst);
        assert!(matches!(
            ticker.poll(&clock, &budget),
            Some(ProviderDiagnosticSample::Unavailable)
        ));
        assert!(ticker.poll(&clock, &budget).is_none());
        clock.0.store(u64::MAX, Ordering::SeqCst);
        assert!(ticker.poll(&clock, &budget).is_none());
        owner.release_pin().unwrap();
        drop(owner);
        clock.0.store(120, Ordering::SeqCst);
        let Some(ProviderDiagnosticSample::Available(sample)) = ticker.poll(&clock, &budget) else {
            panic!("expected recovered sample");
        };
        assert_eq!(
            (
                sample.retiring_owners,
                sample.pinned_owners,
                sample.released_owners
            ),
            (0, 0, 1)
        );
    }
}

// Workers share one sampler and one monotonic clock origin. Database activation,
// hibernation and clock origins cannot multiply process-wide samples.
struct ProcessSampling {
    origin: std::time::Instant,
    ticker: ProviderDiagnosticTicker,
    budget: ProviderBudget,
    finished: bool,
}

impl ProcessSampling {
    fn poll_at(&mut self, elapsed: Duration) -> Option<ProviderDiagnosticSample> {
        if self.finished {
            return None;
        }
        struct Clock(Duration);
        impl CursorMonotonicClock for Clock {
            fn now(&self) -> Result<CursorTick, riffdb_service::CursorClockError> {
                CursorTick::from_process_elapsed(self.0)
                    .map_err(|_| riffdb_service::CursorClockError)
            }
        }
        self.ticker.poll(&Clock(elapsed), &self.budget)
    }
    fn finish(&mut self) -> Option<ProviderDiagnosticSample> {
        if self.finished {
            return None;
        }
        self.finished = true;
        Some(ProviderDiagnosticSample::capture(&self.budget))
    }
}

static PROCESS_SAMPLING: std::sync::OnceLock<std::sync::Mutex<ProcessSampling>> =
    std::sync::OnceLock::new();

pub(crate) fn poll_process(budget: &ProviderBudget) {
    let state = PROCESS_SAMPLING.get_or_init(|| {
        std::sync::Mutex::new(ProcessSampling {
            origin: std::time::Instant::now(),
            ticker: ProviderDiagnosticTicker::default(),
            budget: budget.clone(),
            finished: false,
        })
    });
    let sample = match state.try_lock() {
        Ok(mut state) => {
            let elapsed = state.origin.elapsed();
            state.poll_at(elapsed)
        }
        Err(std::sync::TryLockError::WouldBlock) => None,
        Err(std::sync::TryLockError::Poisoned(_)) => Some(ProviderDiagnosticSample::Unavailable),
    };
    if let Some(sample) = sample {
        emit_sample(sample);
    }
}

pub(crate) fn finish_process_sampling() {
    let Some(state) = PROCESS_SAMPLING.get() else {
        return;
    };
    let sample = match state.lock() {
        Ok(mut state) => state.finish(),
        Err(_) => Some(ProviderDiagnosticSample::Unavailable),
    };
    if let Some(sample) = sample {
        emit_sample(sample);
    }
}

fn emit_sample(sample: ProviderDiagnosticSample) {
    let observation = match sample {
        ProviderDiagnosticSample::Available(sample) => Some(sample),
        ProviderDiagnosticSample::Unavailable => None,
    };
    crate::operational_logging::emit(crate::operational_logging::OperationalEvent::Provider(
        observation,
    ));
}

#[cfg(test)]
mod process_tests {
    use super::*;
    use std::sync::{Arc, Barrier, Mutex};

    // req: END-007, END-008, ID-005
    #[test]
    fn multi_database_provider_samples_once_per_process() {
        let state = Arc::new(Mutex::new(ProcessSampling {
            origin: std::time::Instant::now(),
            ticker: ProviderDiagnosticTicker::default(),
            budget: ProviderBudget::new(),
            finished: false,
        }));
        let barrier = Arc::new(Barrier::new(3));
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let state = state.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    state
                        .try_lock()
                        .ok()
                        .and_then(|mut s| s.poll_at(Duration::ZERO))
                        .is_some()
                })
            })
            .collect();
        barrier.wait();
        assert_eq!(
            workers
                .into_iter()
                .map(|w| usize::from(w.join().unwrap()))
                .sum::<usize>(),
            1
        );
        let mut state = state.lock().unwrap();
        assert!(state.poll_at(Duration::from_secs(59)).is_none());
        assert!(state.poll_at(Duration::from_secs(60)).is_some());
        assert!(state.finish().is_some());
        assert!(state.finish().is_none());
        assert!(state.poll_at(Duration::from_secs(120)).is_none());
    }
}

/// One process owner keeps sampling even when every database is hibernating.
pub(crate) struct RunningProviderSamples {
    stop: std::sync::mpsc::SyncSender<()>,
    finished: std::sync::mpsc::Receiver<()>,
    task: Option<std::thread::JoinHandle<()>>,
}

impl RunningProviderSamples {
    pub(crate) fn start() -> Result<Self, ()> {
        let budget = crate::exact_provider_graph::process_budget();
        let (stop, receiver) = std::sync::mpsc::sync_channel(1);
        let (completed, finished) = std::sync::mpsc::sync_channel(1);
        let task = std::thread::Builder::new()
            .name("riffdb-provider-samples".to_owned())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                poll_process(&budget);
                while matches!(
                    receiver.recv_timeout(Duration::from_secs(60)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                ) {
                    poll_process(&budget);
                }
                finish_process_sampling();
                let _ = completed.try_send(());
            })
            .map_err(|_| ())?;
        Ok(Self {
            stop,
            finished,
            task: Some(task),
        })
    }

    pub(crate) fn shutdown(mut self, timeout: Duration) -> bool {
        let _ = self.stop.try_send(());
        if self.finished.recv_timeout(timeout).is_err() {
            return false;
        }
        if let Some(task) = self.task.take() {
            return task.join().is_ok();
        }
        true
    }
}

impl Drop for RunningProviderSamples {
    fn drop(&mut self) {
        let _ = self.stop.try_send(());
    }
}
