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
