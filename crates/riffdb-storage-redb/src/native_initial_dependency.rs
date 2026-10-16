//! Request-owned admission for an incomplete current native command directory.
//!
//! Waiting holds no writer lease. The service owns its original cancellation
//! and deadline; this substrate adds neither a deadline nor an operation retry.

use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

use riffdb_storage_api::{StorageError, StorageErrorKind};

use crate::locator_capability::LocatorCapability;
use crate::locator_memory::{LocatorAllocationClass, LocatorCharge, NativeLocatorPool};
use crate::store::{RedbOperationalPorts, RedbReadAccess, SharedRedb};

struct DependencyControl {
    shared: Weak<SharedRedb>,
    initial: Mutex<Option<RedbReadAccess>>,
    current: Mutex<LocatorCapability>,
    cancelled: AtomicBool,
    // Actual source and engine references disappear before their control's
    // charge. Cancellation aliases retain the same physical allocation.
    _charge: LocatorCharge,
}

/// One admitted initial dependency, consumed by the existing blocking driver.
/// This owns no authorization result, audit permit, or writer lease.
pub struct OwnedNativeDependencyWait {
    control: Arc<DependencyControl>,
}

/// Wakes one initial dependency when its request is dropped or cancelled.
/// Clones share the original admitted physical control.
#[derive(Clone)]
pub struct NativeDependencyCancellation {
    control: Arc<DependencyControl>,
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

fn unidentified_source_error(error: StorageError) -> StorageError {
    if error.kind() == StorageErrorKind::Unavailable {
        // No healthy Pending source was identified. Do not turn a missing
        // binding, absent proof or failed capture into unaudited backpressure.
        StorageError::new(StorageErrorKind::InvariantViolation, None)
    } else {
        error
    }
}

fn initial_source_is_pending(capability: &LocatorCapability) -> Result<bool, StorageError> {
    if capability.is_pending() {
        capability
            .source_binding()
            .map_err(unidentified_source_error)?;
        Ok(true)
    } else {
        capability
            .require_complete_initial_dependency()
            .map_err(unidentified_source_error)?;
        Ok(false)
    }
}

impl NativeDependencyCancellation {
    /// Cancellation is idempotent and cannot lose a wake during source change.
    pub fn cancel(&self) {
        self.control.cancelled.store(true, Ordering::Release);
        let capability = self
            .control
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        capability.wake_initial_dependency_waiters();
    }
}

impl OwnedNativeDependencyWait {
    fn try_new(
        shared: Arc<SharedRedb>,
        initial: RedbReadAccess,
        pool: &Arc<NativeLocatorPool>,
    ) -> Result<Self, StorageError> {
        let bytes = std::mem::size_of::<DependencyControl>()
            .checked_add(4 * std::mem::size_of::<usize>())
            // Reserve fixed per-waiter headroom for a possible weak-only
            // Shared allocation after its final strong owner disappears. This
            // is conservative reservation headroom, not attribution of a new
            // physical Shared allocation per waiter, and pins no future roots.
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<SharedRedb>()))
            .and_then(|bytes| bytes.checked_add(4 * std::mem::size_of::<usize>()))
            .ok_or_else(unavailable)?;
        let charge = pool.admit(bytes, LocatorAllocationClass::Ordinary)?;
        let capability = initial.locator_capability().clone();
        Ok(Self {
            control: Arc::new(DependencyControl {
                shared: Arc::downgrade(&shared),
                initial: Mutex::new(Some(initial)),
                current: Mutex::new(capability),
                cancelled: AtomicBool::new(false),
                _charge: charge,
            }),
        })
    }

    /// Create the cancellation guard before handing this owner to the driver.
    #[must_use]
    pub fn cancellation(&self) -> NativeDependencyCancellation {
        NativeDependencyCancellation {
            control: Arc::clone(&self.control),
        }
    }

    /// Wait for the complete current source under the caller's cancellation.
    /// A failed construction of an unchanged source is a typed refusal, rather
    /// than permission to rebuild or resubmit the operation indefinitely.
    pub fn wait(self) -> Result<(), StorageError> {
        self.wait_with_sources(
            || {
                self.control
                    .shared
                    .upgrade()
                    .ok_or_else(unavailable)?
                    .begin_composite_operational_read()
            },
            |access| {
                crate::locator_builder::start_locator_builder(
                    access,
                    Some(self.control.shared.upgrade().ok_or_else(unavailable)?),
                )
            },
        )
    }

    fn wait_with_sources(
        &self,
        mut capture: impl FnMut() -> Result<RedbReadAccess, StorageError>,
        mut request: impl FnMut(RedbReadAccess) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        let mut source = self
            .control
            .initial
            .lock()
            .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))?
            .take()
            .ok_or_else(|| StorageError::new(StorageErrorKind::InvariantViolation, None))?;
        loop {
            *self
                .control
                .current
                .lock()
                .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))? =
                source.locator_capability().clone();
            let outcome = source
                .locator_capability()
                .wait_for_initial_dependency(&self.control.cancelled);
            if self.control.cancelled.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            if let Err(error) = &outcome
                && !matches!(
                    error.kind(),
                    StorageErrorKind::Unavailable | StorageErrorKind::LimitExceeded
                )
            {
                return Err(error.clone());
            }
            // Do not hold the completion predicate while capturing publication
            // or requesting the next causally distinct admitted construction.
            let current = capture().map_err(unidentified_source_error)?;
            if self.control.cancelled.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            if current
                .locator_capability()
                .same_construction(source.locator_capability())
            {
                outcome?;
                return current
                    .locator_capability()
                    .require_complete_initial_dependency()
                    .map(|_| ());
            }
            if !initial_source_is_pending(current.locator_capability())? {
                return Ok(());
            }
            request(current.clone())?;
            source = current;
        }
    }
}

impl RedbOperationalPorts {
    /// Probe before checking prestart cancellation: a complete source preserves
    /// the ordinary request/audit path. Only genuine initial Pending may wait.
    pub fn initial_native_dependency(
        &self,
    ) -> Result<Option<OwnedNativeDependencyWait>, StorageError> {
        let source = self
            .shared
            .begin_composite_operational_read()
            .map_err(unidentified_source_error)?;
        if !initial_source_is_pending(source.locator_capability())? {
            return Ok(None);
        }
        let waiter = OwnedNativeDependencyWait::try_new(
            Arc::clone(&self.shared),
            source.clone(),
            &NativeLocatorPool::global(),
        )?;
        crate::locator_builder::start_locator_builder(source, Some(Arc::clone(&self.shared)))?;
        Ok(Some(waiter))
    }

    /// Test-fixture synchronization with exactly one captured source. This
    /// writes no lifecycle certificate and never retries or recaptures a source.
    #[cfg(feature = "test-fixtures")]
    #[doc(hidden)]
    pub fn wait_for_exact_locator_fixture_completion(&self) -> Result<(), StorageError> {
        let source = self.shared.begin_composite_operational_read()?;
        if source.locator_capability().is_pending() {
            crate::locator_builder::start_locator_builder(
                source.clone(),
                Some(Arc::clone(&self.shared)),
            )?;
        }
        source.locator_capability().wait_for_builder_for_test()
    }
}

#[cfg(test)]
#[path = "native_initial_dependency_tests.rs"]
mod tests;
