//! Admitted per-engine locator construction and finite explicit close drain.
//! A registration is released only after its real source and active predicate.

use crate::locator_memory::{
    LOCATOR_MAX_OWNERS, LocatorAllocationClass, LocatorCharge, NativeLocatorPool,
};
use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::exact_provider_budget_v1::{
    PreparedSharedPhysicalOwnership, ProviderBudget, Reservation, SharedPhysicalOwnershipCell,
};
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

pub(crate) const LOCATOR_DRAIN_LIMIT: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Open,
    Closing,
    Closed,
    Invalid,
}

struct State {
    phase: Phase,
    epoch: u64,
    registrations: usize,
    close_attempt: bool,
}

#[derive(Clone)]
pub(crate) struct LocatorBuildDomain(riffdb_types::OwnedCapacitySharedV1<LocatorBuildDomainInner>);
pub(crate) struct LocatorBuildDomainInner {
    state: Mutex<State>,
    changed: Condvar,
    cancelled: AtomicBool,
    provider: SharedPhysicalOwnershipCell,
    // The actual control remains admitted through its last physical owner.
    native_charge: LocatorCharge,
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

impl AsRef<LocatorBuildDomain> for LocatorBuildDomain {
    fn as_ref(&self) -> &LocatorBuildDomain {
        self
    }
}
impl std::ops::Deref for LocatorBuildDomain {
    type Target = LocatorBuildDomainInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl LocatorBuildDomain {
    pub(crate) fn same_domain(&self, other: &Self) -> bool {
        self.0.same_allocation(&other.0)
    }
    pub(crate) fn try_new(
        pool: &NativeLocatorPool,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let native_charge =
            pool.admit(std::mem::size_of::<LocatorBuildDomainInner>() + 128, class)?;
        Ok(Self(riffdb_types::OwnedCapacitySharedV1::new(
            LocatorBuildDomainInner {
                state: Mutex::new(State {
                    phase: Phase::Open,
                    epoch: 0,
                    registrations: 0,
                    close_attempt: false,
                }),
                changed: Condvar::new(),
                cancelled: AtomicBool::new(false),
                provider: SharedPhysicalOwnershipCell::new(),
                native_charge,
            },
        )))
    }

    pub(crate) fn register(&self) -> Result<BuildRegistration, StorageError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.phase != Phase::Open || state.registrations == LOCATOR_MAX_OWNERS {
            return Err(unavailable());
        }
        state.registrations += 1;
        Ok(BuildRegistration {
            domain: self.clone(),
            epoch: state.epoch,
        })
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    // Call only after source/writable/private-validation preflight succeeds.
    // Deadline originates at the OUTER close entry, before queued extraction.
    pub(crate) fn begin_close(&self, deadline: Instant) -> Result<CloseAttempt, StorageError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.close_attempt || state.phase == Phase::Invalid {
            return Err(unavailable());
        }
        state.close_attempt = true;
        if state.phase == Phase::Open {
            state.phase = Phase::Closing;
        }
        self.cancelled.store(true, Ordering::Release);
        self.changed.notify_all();
        Ok(CloseAttempt {
            domain: self.clone(),
            epoch: state.epoch,
            deadline,
        })
    }

    // Caller must own the ORIGINAL activation/mutation lease and have just
    // committed the existing verified DIRTY transition. This grants no writer
    // admission and cannot be called by a query or ordinary request.
    pub(crate) fn rearm_after_verified_dirty(&self) -> Result<(), StorageError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state.phase == Phase::Open {
            return Ok(());
        }
        if state.registrations != 0 || state.phase == Phase::Invalid {
            return Err(unavailable());
        }
        if state.phase != Phase::Open {
            state.epoch = state.epoch.checked_add(1).ok_or_else(unavailable)?;
            state.phase = Phase::Open;
            state.close_attempt = false;
            self.cancelled.store(false, Ordering::Release);
            self.changed.notify_all();
        }
        Ok(())
    }

    pub(crate) fn rearm_precondition(&self) -> Result<(), StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        if state.phase == Phase::Open || (state.phase != Phase::Invalid && state.registrations == 0)
        {
            Ok(())
        } else {
            Err(unavailable())
        }
    }
    #[cfg(test)]
    pub(crate) fn registrations_for_test(&self) -> usize {
        self.state.lock().unwrap().registrations
    }
    #[cfg(test)]
    pub(crate) fn provider_charge_bytes(&self) -> u64 {
        self.provider.owned_bytes().unwrap()
    }

    fn provider_bound(&self) -> Result<u64, StorageError> {
        u64::try_from(self.native_charge.bytes()).map_err(|_| unavailable())
    }

    pub(crate) fn provider_ownership_needed(
        &self,
        budget: &ProviderBudget,
    ) -> Result<u64, StorageError> {
        self.provider
            .provider_ownership_needed(self.provider_bound()?, budget)
            .map_err(|_| unavailable())
    }

    pub(crate) fn prepare_provider_ownership_from(
        &self,
        work: &mut Reservation,
        budget: &ProviderBudget,
    ) -> Result<PreparedSharedPhysicalOwnership<'_>, StorageError> {
        self.provider
            .prepare_provider_ownership_from(self.provider_bound()?, work, budget)
            .map_err(|_| unavailable())
    }
}

pub(crate) struct BuildRegistration {
    domain: LocatorBuildDomain,
    epoch: u64,
}

impl BuildRegistration {
    pub(crate) fn domain(&self) -> &LocatorBuildDomain {
        &self.domain
    }
    pub(crate) fn is_cancelled(&self) -> bool {
        self.domain.is_cancelled()
    }
}

impl Drop for BuildRegistration {
    fn drop(&mut self) {
        // Cleanup cannot abandon its physically owned registration after poison.
        let mut state = self
            .domain
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.epoch == self.epoch && state.registrations != 0 {
            state.registrations -= 1;
            if state.registrations == 0 && state.phase == Phase::Closing {
                state.phase = Phase::Closed;
            }
        } else {
            // An impossible stale acknowledgment must never open admission.
            state.phase = Phase::Invalid;
            self.domain.cancelled.store(true, Ordering::Release);
        }
        self.domain.changed.notify_all();
    }
}

pub(crate) struct CloseAttempt {
    domain: LocatorBuildDomain,
    epoch: u64,
    deadline: Instant,
}

impl CloseAttempt {
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    // First extract all matching queued work without owning mutation_gate.
    // The fixture pause couples the observed nonzero predicate to the real wait.
    #[cfg(any(test, not(feature = "test-fixtures")))]
    pub(crate) fn await_quiescence(self) -> Result<ClosedLocatorOwner, StorageError> {
        #[cfg(feature = "test-fixtures")]
        {
            self.await_quiescence_inner(None)
        }
        #[cfg(not(feature = "test-fixtures"))]
        {
            self.await_quiescence_inner()
        }
    }
    #[cfg(feature = "test-fixtures")]
    pub(crate) fn await_quiescence_with_controller(
        self,
        controller: Option<&crate::hooks::RedbTestController>,
    ) -> Result<ClosedLocatorOwner, StorageError> {
        self.await_quiescence_inner(controller)
    }
    fn await_quiescence_inner(
        self,
        #[cfg(feature = "test-fixtures")] controller: Option<&crate::hooks::RedbTestController>,
    ) -> Result<ClosedLocatorOwner, StorageError> {
        let mut state = self.domain.state.lock().map_err(|_| unavailable())?;
        loop {
            if state.epoch != self.epoch || !state.close_attempt || state.phase == Phase::Invalid {
                return Err(unavailable());
            }
            if Instant::now() >= self.deadline {
                return Err(unavailable());
            }
            if state.registrations == 0 {
                state.phase = Phase::Closed;
                drop(state);
                return Ok(ClosedLocatorOwner { attempt: self });
            }
            #[cfg(feature = "test-fixtures")]
            if let Some(controller) = controller {
                controller.checkpoint_test_pause(
                    crate::hooks::RedbCheckpointTestPoint::LocatorCloseDrainWaiting,
                );
            }
            let remaining = self
                .deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(unavailable)?;
            let (next, _) = self
                .domain
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| unavailable())?;
            state = next;
        }
    }
}

impl Drop for CloseAttempt {
    fn drop(&mut self) {
        let mut state = self
            .domain
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.epoch == self.epoch {
            state.close_attempt = false;
        }
        // The fence never reopens, including timeout, poison or CLEAN failure.
        self.domain.changed.notify_all();
    }
}

pub(crate) struct ClosedLocatorOwner {
    attempt: CloseAttempt,
}

impl ClosedLocatorOwner {
    // Recheck AFTER acquiring the original mutation lease; a legitimate newer
    // private DIRTY activation invalidates this proof by changing the epoch.
    pub(crate) fn verify_under_mutation_lease(&self) -> Result<(), StorageError> {
        let state = self
            .attempt
            .domain
            .state
            .lock()
            .map_err(|_| unavailable())?;
        if state.epoch != self.attempt.epoch
            || state.phase != Phase::Closed
            || state.registrations != 0
            || !state.close_attempt
        {
            return Err(unavailable());
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "locator_build_domain_tests.rs"]
mod tests;
