//! Owned exclusion used by redb startup and mutation transactions.

use std::sync::{Arc, Condvar, Mutex};

use riffdb_storage_api::{StorageError, StorageErrorKind};

#[derive(Default)]
pub(crate) struct ExclusiveGate {
    inner: Arc<GateInner>,
}

#[derive(Default)]
struct GateInner {
    held: Mutex<bool>,
    released: Condvar,
}

pub(crate) struct ExclusiveLease {
    inner: Arc<GateInner>,
}

impl ExclusiveGate {
    pub(crate) fn acquire(&self) -> Result<ExclusiveLease, StorageError> {
        let mut held = self.inner.held.lock().map_err(|_| poisoned_gate())?;
        while *held {
            held = self
                .inner
                .released
                .wait(held)
                .map_err(|_| poisoned_gate())?;
        }
        *held = true;
        drop(held);
        Ok(ExclusiveLease {
            inner: Arc::clone(&self.inner),
        })
    }

    #[cfg(test)]
    pub(crate) fn is_held(&self) -> bool {
        *self
            .inner
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for ExclusiveLease {
    fn drop(&mut self) {
        let mut held = self
            .inner
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(*held, "an exclusive lease must own the gate it releases");
        *held = false;
        self.inner.released.notify_all();
    }
}

fn poisoned_gate() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}

#[cfg(test)]
mod tests {
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
}
