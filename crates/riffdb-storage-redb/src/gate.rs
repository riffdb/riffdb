#![expect(
    clippy::expect_used,
    reason = "an owned redb exclusion grant retains its mutex state until release"
)]

//! Owned exclusion used by redb startup and mutation transactions.

use std::sync::{Arc, Condvar, Mutex};

use riffdb_storage_api::{StorageError, StorageErrorKind};

mod source_admission;
pub(crate) use source_admission::SourceDrainRefusal;

#[derive(Default)]
pub(crate) struct ExclusiveGate {
    inner: Arc<GateInner>,
}

#[derive(Default)]
struct GateInner {
    state: Mutex<GateState>,
    released: Condvar,
}

#[derive(Default)]
struct GateState {
    next_ticket: u128,
    serving: u128,
    held: bool,
    source: source_admission::State,
}

pub(crate) struct ExclusiveLease {
    inner: Arc<GateInner>,
    source_started: Option<std::time::Instant>,
}

impl ExclusiveGate {
    /// Bounded maintenance admission. Existing queued tickets always win;
    /// refusal creates no waiter and does not consume a ticket.
    pub(crate) fn try_acquire(&self) -> Result<Option<ExclusiveLease>, StorageError> {
        let mut state = self.inner.state.lock().map_err(|_| poisoned_gate())?;
        if state.held || state.serving != state.next_ticket {
            return Ok(None);
        }
        state.next_ticket = state.next_ticket.checked_add(1).ok_or_else(poisoned_gate)?;
        state.held = true;
        drop(state);
        Ok(Some(ExclusiveLease {
            inner: Arc::clone(&self.inner),
            source_started: None,
        }))
    }

    pub(crate) fn acquire(&self) -> Result<ExclusiveLease, StorageError> {
        let mut state = self.inner.state.lock().map_err(|_| poisoned_gate())?;
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.checked_add(1).ok_or_else(poisoned_gate)?;
        self.inner.released.notify_all();
        while state.held || state.serving != ticket {
            state = self
                .inner
                .released
                .wait(state)
                .map_err(|_| poisoned_gate())?;
        }
        state.held = true;
        drop(state);
        Ok(ExclusiveLease {
            inner: Arc::clone(&self.inner),
            source_started: None,
        })
    }

    pub(crate) fn is_busy(&self) -> Result<bool, StorageError> {
        self.inner
            .state
            .lock()
            .map(|state| state.held)
            .map_err(|_| poisoned_gate())
    }

    #[cfg(test)]
    pub(crate) fn is_held(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .held
    }

    /// Total tickets ever issued, granted, queued or cancelled.
    ///
    /// Every write transaction and every indexed read lease takes exactly one
    /// ticket, so an unchanged count across an operation proves the operation
    /// never contended the exclusive gate.
    #[cfg(test)]
    pub(crate) fn tickets_issued(&self) -> u128 {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_ticket
    }

    #[cfg(test)]
    fn wait_for_queued(&self, expected: u128) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while queued_waiters(&state) < expected {
            state = self
                .inner
                .released
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(test)]
impl ExclusiveLease {
    /// Observes the same gate without cloning or retaining its granted lease.
    /// Tests can prove real refusal and subsequent admission across proof drop.
    pub(crate) fn gate_for_test(&self) -> ExclusiveGate {
        ExclusiveGate {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for ExclusiveLease {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(
            state.held,
            "an exclusive lease must own the gate it releases"
        );
        if let Some(started) = self.source_started {
            state.source.note_held(started.elapsed());
        }
        state.held = false;
        state.serving = state
            .serving
            .checked_add(1)
            .expect("a granted ticket always has a representable successor");
        state.skip_cancelled_source();
        self.inner.released.notify_all();
    }
}

#[cfg(test)]
fn queued_waiters(state: &GateState) -> u128 {
    (state.next_ticket - state.serving) - u128::from(state.held)
}

fn poisoned_gate() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    #[test]
    fn bounded_admission_cannot_barge_a_queued_writer_or_issue_refused_tickets() {
        let gate = Arc::new(ExclusiveGate::default());
        let initial = gate.acquire().unwrap();
        let issued = gate.tickets_issued();
        assert!(gate.try_acquire().unwrap().is_none());
        assert_eq!(gate.tickets_issued(), issued);
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let queued = gate.clone();
        let writer = std::thread::spawn(move || {
            let lease = queued.acquire().unwrap();
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(lease);
        });
        gate.wait_for_queued(1);
        drop(initial);
        // Whether the queued writer has woken or still owns the lease, the
        // queued ticket wins. Refusal does not insert another maintenance waiter.
        assert!(gate.try_acquire().unwrap().is_none());
        ready_rx.recv().unwrap();
        assert!(gate.try_acquire().unwrap().is_none());
        release_tx.send(()).unwrap();
        writer.join().unwrap();
        let admitted = gate.try_acquire().unwrap().unwrap();
        assert!(gate.is_held());
        assert_eq!(gate.tickets_issued(), issued + 2);
        drop(admitted);
        assert!(!gate.is_held());
    }

    #[test]
    fn owned_lease_releases_the_gate_on_drop() {
        let gate = ExclusiveGate::default();
        let lease = gate.acquire().expect("acquire gate");
        assert!(gate.is_held());
        drop(lease);
        assert!(!gate.is_held());
        drop(gate.acquire().expect("reacquire gate"));
    }

    #[test]
    fn queued_acquisitions_are_granted_in_fifo_order_without_starvation() {
        let gate = Arc::new(ExclusiveGate::default());
        let initial = gate.acquire().expect("acquire initial lease");
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();

        let first_gate = Arc::clone(&gate);
        let first_acquired = acquired_tx.clone();
        let first = std::thread::spawn(move || {
            let lease = first_gate.acquire().expect("acquire first queued lease");
            first_acquired.send(1).expect("report first acquisition");
            release_first_rx.recv().expect("release first lease");
            drop(lease);
        });
        gate.wait_for_queued(1);

        let second_gate = Arc::clone(&gate);
        let second = std::thread::spawn(move || {
            let _lease = second_gate.acquire().expect("acquire second queued lease");
            acquired_tx.send(2).expect("report second acquisition");
        });
        gate.wait_for_queued(2);

        drop(initial);
        assert_eq!(acquired_rx.recv().expect("first acquisition"), 1);
        assert!(acquired_rx.try_recv().is_err());
        release_first_tx
            .send(())
            .expect("release first acquisition");
        assert_eq!(acquired_rx.recv().expect("second acquisition"), 2);

        first.join().expect("first waiter");
        second.join().expect("second waiter");
    }
}
