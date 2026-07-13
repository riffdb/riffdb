use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Waker;

const MAX_CANCELLATION_REGISTRATIONS: usize = 1_024;

/// A cloneable, runtime-neutral cancellation signal for lock acquisition.
///
/// Cancellation is monotonic. Registering a waker after cancellation wakes it
/// immediately, so a cancellation concurrent with waiter registration cannot
/// be lost.
#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: Arc<CancellationState>,
}

impl CancellationToken {
    /// Creates a signal in its initial, non-cancelled state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the signal cancelled and wakes every registered acquisition.
    pub fn cancel(&self) {
        let wakers = {
            let mut state = lock_unpoisoned(&self.inner.state);
            if state.cancelled {
                return;
            }
            state.cancelled = true;
            std::mem::take(&mut state.wakers)
        };

        for (_, waker) in wakers {
            waker.wake();
        }
    }

    /// Reports whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        lock_unpoisoned(&self.inner.state).cancelled
    }

    pub(crate) fn register(
        &self,
        registration: &mut Option<u64>,
        waker: &Waker,
    ) -> Result<(), CancellationRegistrationError> {
        let wake_now = {
            let mut state = lock_unpoisoned(&self.inner.state);
            if state.cancelled {
                true
            } else {
                let id = if let Some(id) = *registration {
                    id
                } else {
                    if state.wakers.len() >= MAX_CANCELLATION_REGISTRATIONS {
                        return Err(CancellationRegistrationError::CapacityExceeded {
                            maximum: MAX_CANCELLATION_REGISTRATIONS,
                        });
                    }
                    let id = state.next_registration();
                    *registration = Some(id);
                    id
                };
                state.wakers.insert(id, waker.clone());
                false
            }
        };
        if wake_now {
            waker.wake_by_ref();
        }
        Ok(())
    }

    pub(crate) fn unregister(&self, registration: &mut Option<u64>) {
        if let Some(id) = registration.take() {
            lock_unpoisoned(&self.inner.state).wakers.remove(&id);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CancellationRegistrationError {
    CapacityExceeded { maximum: usize },
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct CancellationState {
    state: Mutex<CancellationData>,
}

struct CancellationData {
    cancelled: bool,
    wakers: BTreeMap<u64, Waker>,
    next_registration: u64,
}

impl Default for CancellationData {
    fn default() -> Self {
        Self {
            cancelled: false,
            wakers: BTreeMap::new(),
            next_registration: 1,
        }
    }
}

impl CancellationData {
    fn next_registration(&mut self) -> u64 {
        loop {
            let candidate = self.next_registration;
            self.next_registration = self.next_registration.checked_add(1).unwrap_or(1);
            if !self.wakers.contains_key(&candidate) {
                return candidate;
            }
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use super::CancellationToken;

    struct CounterWaker(AtomicUsize);

    impl Wake for CounterWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn cancellation_is_idempotent_and_wakes_registered_waiters_once() {
        let token = CancellationToken::new();
        let counter = Arc::new(CounterWaker(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut registration = None;
        token
            .register(&mut registration, &waker)
            .expect("first registration");
        token
            .register(&mut registration, &waker)
            .expect("replacement registration");

        token.cancel();
        token.cancel();

        assert!(token.is_cancelled());
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn registration_after_cancellation_wakes_immediately() {
        let token = CancellationToken::new();
        token.cancel();
        let counter = Arc::new(CounterWaker(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut registration = None;

        token
            .register(&mut registration, &waker)
            .expect("cancelled registration only wakes");

        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unregister_removes_a_live_waker() {
        let token = CancellationToken::new();
        let counter = Arc::new(CounterWaker(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut registration = None;
        token
            .register(&mut registration, &waker)
            .expect("live registration");

        token.unregister(&mut registration);
        token.cancel();

        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn registration_collection_has_a_checked_hard_bound() {
        let token = CancellationToken::new();
        let counter = Arc::new(CounterWaker(AtomicUsize::new(0)));
        let waker = Waker::from(counter);
        let mut registrations = (0..super::MAX_CANCELLATION_REGISTRATIONS)
            .map(|_| None)
            .collect::<Vec<_>>();

        for registration in &mut registrations {
            token
                .register(registration, &waker)
                .expect("registration below the hard bound");
        }
        let mut overflow = None;
        assert_eq!(
            token.register(&mut overflow, &waker),
            Err(super::CancellationRegistrationError::CapacityExceeded {
                maximum: super::MAX_CANCELLATION_REGISTRATIONS,
            })
        );

        token.unregister(&mut registrations[0]);
        token
            .register(&mut overflow, &waker)
            .expect("released capacity is reusable");
    }
}
