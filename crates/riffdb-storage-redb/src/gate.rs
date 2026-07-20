//! Owned exclusion used by redb startup and mutation transactions.

use std::sync::{Arc, Condvar, Mutex};

use riffdb_storage_api::{StorageError, StorageErrorKind};

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
}

pub(crate) struct ExclusiveLease {
    inner: Arc<GateInner>,
}

impl ExclusiveGate {
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
        })
    }

    #[cfg(test)]
    pub(crate) fn is_held(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .held
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
        state.held = false;
        state.serving = state
            .serving
            .checked_add(1)
            .expect("a granted ticket always has a representable successor");
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
