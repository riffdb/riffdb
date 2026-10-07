//! One fixed FIFO source-maintenance slot; no general cancellable waiter list.
use super::*;
use std::time::{Duration, Instant};

const ADMISSION_WAIT: Duration = Duration::from_millis(100);

#[derive(Default)]
pub(super) struct State {
    slot: Option<Slot>,
    retired: bool,
    // One nondurable hint for an ordinary writer's existing checkpoint. The
    // same gate mutex couples it to irreversible source-owner retirement.
    checkpoint_requested: bool,
    // queued, granted, expired, cancelled, capacity/refused, wait us,
    // full lease residence us, bounded-drain yields. Diagnostic only.
    census: [u64; 8],
}

struct Slot {
    ticket: u128,
    cancelled: bool,
}

impl State {
    fn add(&mut self, index: usize, amount: u64) {
        self.census[index] = self.census[index].saturating_add(amount);
    }
    pub(super) fn note_held(&mut self, elapsed: Duration) {
        self.add(6, micros(elapsed));
    }
}

impl GateState {
    pub(super) fn skip_cancelled_source(&mut self) {
        if !self.held
            && self
                .source
                .slot
                .as_ref()
                .is_some_and(|slot| slot.cancelled && slot.ticket == self.serving)
        {
            self.serving = self
                .serving
                .checked_add(1)
                .expect("a queued source ticket has a representable successor");
            self.source.slot = None;
        }
    }
    fn cancel_source_ticket(&mut self, ticket: u128, reason: usize) {
        if let Some(slot) = self.source.slot.as_mut()
            && slot.ticket == ticket
            && !slot.cancelled
        {
            slot.cancelled = true;
            self.source.add(reason, 1);
        }
        self.skip_cancelled_source();
    }
}

impl ExclusiveGate {
    pub(crate) fn acquire_source(&self) -> Result<Option<ExclusiveLease>, StorageError> {
        let Some(request) = self.source_request_at(Instant::now())? else {
            return Ok(None);
        };
        request.wait()
    }

    fn source_request_at(&self, now: Instant) -> Result<Option<Request>, StorageError> {
        let deadline = now.checked_add(ADMISSION_WAIT).ok_or_else(poisoned_gate)?;
        let mut state = self.inner.state.lock().map_err(|_| poisoned_gate())?;
        if state.source.retired || state.source.slot.is_some() {
            state.source.add(4, 1);
            return Ok(None);
        }
        let ticket = state.next_ticket;
        state.next_ticket = ticket.checked_add(1).ok_or_else(poisoned_gate)?;
        state.source.slot = Some(Slot {
            ticket,
            cancelled: false,
        });
        state.source.add(0, 1);
        self.inner.released.notify_all();
        Ok(Some(Request {
            inner: Arc::clone(&self.inner),
            ticket,
            began: now,
            deadline,
            finished: false,
        }))
    }

    /// Retirement is irreversible for this graph. An already granted atomic
    /// turn finishes; no pending or subsequent source turn can gain a lease.
    pub(crate) fn retire_source_admission(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.source.retired = true;
        state.source.checkpoint_requested = false;
        if let Some(ticket) = state.source.slot.as_ref().map(|slot| slot.ticket) {
            state.cancel_source_ticket(ticket, 3);
        }
        self.inner.released.notify_all();
    }

    pub(crate) fn source_census(&self) -> Result<[u64; 8], StorageError> {
        self.inner
            .state
            .lock()
            .map(|state| state.source.census)
            .map_err(|_| poisoned_gate())
    }

    pub(crate) fn note_source_checkpoint_request(&self) -> Result<(), StorageError> {
        let mut state = self.inner.state.lock().map_err(|_| poisoned_gate())?;
        if !state.source.retired {
            state.source.checkpoint_requested = true;
        }
        Ok(())
    }

    pub(crate) fn source_checkpoint_requested(&self) -> Result<bool, StorageError> {
        self.inner
            .state
            .lock()
            .map(|state| state.source.checkpoint_requested)
            .map_err(|_| poisoned_gate())
    }

    pub(crate) fn clear_source_checkpoint_request(&self) -> Result<(), StorageError> {
        self.inner
            .state
            .lock()
            .map_err(|_| poisoned_gate())?
            .source
            .checkpoint_requested = false;
        Ok(())
    }

    pub(crate) fn note_source_drain_yield(&self) -> Result<(), StorageError> {
        self.inner
            .state
            .lock()
            .map_err(|_| poisoned_gate())?
            .source
            .add(7, 1);
        Ok(())
    }
}

struct Request {
    inner: Arc<GateInner>,
    ticket: u128,
    began: Instant,
    deadline: Instant,
    finished: bool,
}

enum Poll {
    Pending,
    Finished(Option<ExclusiveLease>),
}

impl Request {
    fn poll_locked(&mut self, state: &mut GateState, now: Instant) -> Poll {
        let eligible = !state.source.retired
            && state
                .source
                .slot
                .as_ref()
                .is_some_and(|slot| slot.ticket == self.ticket && !slot.cancelled);
        if !eligible || now >= self.deadline {
            state.cancel_source_ticket(self.ticket, if eligible { 2 } else { 3 });
            self.finished = true;
            state
                .source
                .add(5, micros(now.saturating_duration_since(self.began)));
            self.inner.released.notify_all();
            return Poll::Finished(None);
        }
        if state.held || state.serving != self.ticket {
            return Poll::Pending;
        }
        state.source.slot = None;
        state.held = true;
        state.source.add(1, 1);
        state
            .source
            .add(5, micros(now.saturating_duration_since(self.began)));
        self.finished = true;
        Poll::Finished(Some(ExclusiveLease {
            inner: Arc::clone(&self.inner),
            source_started: Some(now),
        }))
    }

    fn wait(mut self) -> Result<Option<ExclusiveLease>, StorageError> {
        let inner = Arc::clone(&self.inner);
        let mut state = inner.state.lock().map_err(|_| poisoned_gate())?;
        loop {
            match self.poll_locked(&mut state, Instant::now()) {
                Poll::Finished(lease) => return Ok(lease),
                Poll::Pending => {
                    let remaining = self.deadline.saturating_duration_since(Instant::now());
                    let (next, _) = inner
                        .released
                        .wait_timeout(state, remaining)
                        .map_err(|_| poisoned_gate())?;
                    state = next;
                }
            }
        }
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        if !self.finished {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.cancel_source_ticket(self.ticket, 3);
            state.source.add(5, micros(self.began.elapsed()));
            self.inner.released.notify_all();
        }
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "source_admission_tests.rs"]
mod tests;
