//! Command/control admission drain; service-audit submission is independent.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

const PAUSING: usize = 1 << (usize::BITS - 1);
const FENCED: usize = 1 << (usize::BITS - 2);
const COUNT_MASK: usize = !(PAUSING | FENCED);
const MAX_SUBMISSIONS: usize = u16::MAX as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrimaryAdmissionRefusal {
    Draining,
    Fenced,
}

/// Process-local scheduling state, never durable source evidence. Ordinary
/// admission cannot reopen Fenced. Only the consumed drained owner may seal it
/// after the coordinator establishes its durable outcome. Required audit has a
/// separate submission path and never acquires this gate.
pub(crate) struct PrimaryAdmissionGate {
    state: AtomicUsize,
    drained: Notify,
}
impl PrimaryAdmissionGate {
    pub(crate) fn from_repository(
        repository: &impl riffdb_storage_api::ReplicationPrimaryAdmissionReadPort,
    ) -> Result<Self, riffdb_storage_api::StorageError> {
        let admission = repository.read_replication_primary_admission()?;
        Ok(Self {
            state: AtomicUsize::new(if admission.fence().is_some() {
                FENCED
            } else {
                0
            }),
            drained: Notify::new(),
        })
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            drained: Notify::new(),
        }
    }
    pub(crate) fn begin(self: &Arc<Self>) -> Result<PrimarySubmission, PrimaryAdmissionRefusal> {
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            if observed & FENCED != 0 {
                return Err(PrimaryAdmissionRefusal::Fenced);
            }
            if observed & PAUSING != 0 || observed & COUNT_MASK >= MAX_SUBMISSIONS {
                return Err(PrimaryAdmissionRefusal::Draining);
            }
            match self.state.compare_exchange_weak(
                observed,
                observed + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(PrimarySubmission(Arc::clone(self))),
                Err(current) => observed = current,
            }
        }
    }
    /// One fence submitter owns the pause before enqueuing its barrier. A second
    /// submitter cannot borrow that pause or mistake it for a committed fence.
    pub(crate) fn pause(self: &Arc<Self>) -> Result<PrimaryPause, PrimaryAdmissionRefusal> {
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            if observed & FENCED != 0 {
                return Err(PrimaryAdmissionRefusal::Fenced);
            }
            if observed & PAUSING != 0 {
                return Err(PrimaryAdmissionRefusal::Draining);
            }
            match self.state.compare_exchange_weak(
                observed,
                observed | PAUSING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(PrimaryPause {
                        gate: Arc::clone(self),
                        release: true,
                    });
                }
                Err(current) => observed = current,
            }
        }
    }

    /// Schedules a fence attempt without reopening an already fenced source.
    /// A retry owns no releasable pause: cancellation or refusal must preserve
    /// Fenced. Current policy and exact durable replay are still checked by the
    /// transaction owner; this process-local state is not fence evidence.
    pub(crate) fn pause_for_fence(
        self: &Arc<Self>,
    ) -> Result<PrimaryPause, PrimaryAdmissionRefusal> {
        match self.pause() {
            Err(PrimaryAdmissionRefusal::Fenced) => Ok(PrimaryPause {
                gate: Arc::clone(self),
                release: false,
            }),
            other => other,
        }
    }
}

/// Held through the synchronous queue send, so drain observes actual insertion,
/// not an earlier capacity reservation. Dropping before send cancels admission.
pub(crate) struct PrimarySubmission(Arc<PrimaryAdmissionGate>);
impl Drop for PrimarySubmission {
    fn drop(&mut self) {
        let prior = self.0.state.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(prior & COUNT_MASK, 0);
        if prior & PAUSING != 0 && prior & COUNT_MASK == 1 {
            self.0.drained.notify_waiters();
        }
    }
}

pub(crate) struct PrimaryPause {
    gate: Arc<PrimaryAdmissionGate>,
    release: bool,
}
impl PrimaryPause {
    /// Cancellation owns and drops the pause. Register the wake before checking
    /// the counter to cover a last sender completing between those two actions.
    pub(crate) async fn drain(self) -> DrainedPrimaryPause {
        loop {
            let notified = self.gate.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.gate.state.load(Ordering::Acquire) & COUNT_MASK == 0 {
                break;
            }
            notified.await;
        }
        DrainedPrimaryPause(self)
    }
}
impl Drop for PrimaryPause {
    fn drop(&mut self) {
        if self.release {
            // Only remove this nondurable pause; never clear a committed fence.
            self.gate.state.fetch_and(!PAUSING, Ordering::AcqRel);
        }
    }
}

/// The barrier may be enqueued only after every previously admitted sender has
/// sent or cancelled. The queued fence owner must retain this until its durable
/// outcome is established; dropping the response must not drop that owner.
pub(crate) struct DrainedPrimaryPause(PrimaryPause);
impl DrainedPrimaryPause {
    /// Used only by the closed coordinator fence owner after durable fencing.
    /// No public operator option or raw transaction can call this method.
    pub(crate) fn finish_fenced(mut self) {
        self.0.gate.state.store(FENCED, Ordering::Release);
        self.0.release = false;
    }
}

#[cfg(test)]
#[path = "primary_admission_gate_tests.rs"]
mod tests;
