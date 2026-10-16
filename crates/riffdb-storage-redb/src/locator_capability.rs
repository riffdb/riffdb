//! Admitted completion state retained by one authoritative source owner.
//!
//! A pending directory is never a negative membership proof. Completion is
//! write-once, and callers may install only evidence derived from this owner's
//! captured source or a causally prepared successor with the same membership.

use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock,
    atomic::{AtomicBool, Ordering},
};

use riffdb_storage_api::{StorageError, StorageErrorKind};

use crate::locator_memory::{
    ChargedLocatorAllocation, LocatorAllocationClass, LocatorOwner, LocatorRetireContents,
    NativeLocatorPool,
};
use crate::transient::LocatorGeneration;
#[cfg(test)]
use riffdb_types::exact_provider_budget_v1::Reservation;
use riffdb_types::exact_provider_budget_v1::{OwnershipCharge, ProviderBudget};
use riffdb_types::{DatabaseId, SchemaHash};

static NATIVE_PROVIDER_ADMISSION: Mutex<()> = Mutex::new(());

fn initial_dependency_proof_error(kind: StorageErrorKind) -> StorageError {
    // The existing worker contains a panic by invalidating its proof with
    // Unavailable. It is not healthy Pending capacity backpressure. Keep the
    // generic storage policy unchanged while initial admission signals the
    // existing systemic integrity handling for that invalidated proof.
    StorageError::new(
        if kind == StorageErrorKind::Unavailable {
            StorageErrorKind::InvariantViolation
        } else {
            kind
        },
        None,
    )
}

/// One bounded numerical plan covers both provider discovery and future
/// completion. Lock order is global admission, stable sharing-family guards,
/// then capability/engine attachment guards and provider accounting.
pub(crate) fn native_provider_admission_gate() -> Result<MutexGuard<'static, ()>, StorageError> {
    NATIVE_PROVIDER_ADMISSION
        .lock()
        .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))
}

struct ProviderBinding {
    budget: ProviderBudget,
    marker: Arc<OwnershipCharge>,
}

/// Holds the captured capability's binding slot while its complete family/
/// engine plan is prepared. Final assignment is infallible and allocation-free.
pub(crate) struct PreparedLocatorProviderBinding<'a> {
    provider: std::sync::MutexGuard<'a, Option<ProviderBinding>>,
    budget: ProviderBudget,
}
impl PreparedLocatorProviderBinding<'_> {
    /// Marker comes from this plan's same-budget prepared family receipt.
    pub(crate) fn commit(mut self, marker: Arc<OwnershipCharge>) {
        if self.provider.is_none() {
            *self.provider = Some(ProviderBinding {
                budget: self.budget.clone(),
                marker,
            });
        }
    }
}

struct Completion {
    // Pending construction itself consumes an owner slot. Its reservation
    // remains charged until bounded retirement, including failed construction.
    generation: OnceLock<Result<LocatorGeneration, StorageErrorKind>>,
    build_started: AtomicBool,
    binding: OnceLock<(DatabaseId, u64, SchemaHash)>,
    provider: Mutex<Option<ProviderBinding>>,
    changed: std::sync::Condvar,
    // Release roots before the reserved final-owner retirement barrier.
    _owner: LocatorOwner,
}

impl LocatorRetireContents for Completion {
    fn retire_one(&mut self) -> bool {
        // A generation has seven fixed root handles. Their final releases
        // enqueue admitted tasks; no population is traversed here.
        self.generation.take();
        true
    }
}

#[derive(Clone)]
pub(crate) struct LocatorCapability {
    completion: Option<ChargedLocatorAllocation<Completion>>,
}

impl LocatorCapability {
    /// No allocation and no authority. Used when even a pending owner cannot
    /// be admitted; ordinary point reads report typed unavailable.
    #[cfg(test)]
    pub(crate) const fn invalid() -> Self {
        Self { completion: None }
    }

    pub(crate) fn try_pending(class: LocatorAllocationClass) -> Result<Self, StorageError> {
        let pool = NativeLocatorPool::global();
        let owner =
            LocatorOwner::try_new_with_bytes(&pool, class, LocatorGeneration::descriptor_bytes())?;
        Self::allocate_completion(owner, OnceLock::new(), class)
    }

    fn allocate_completion(
        owner: LocatorOwner,
        generation: OnceLock<Result<LocatorGeneration, StorageErrorKind>>,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let pool = owner.family_pool();
        let completion = ChargedLocatorAllocation::try_new(
            &pool,
            class,
            std::mem::size_of::<crate::checkpoint_root::CheckpointRoot>()
                + std::mem::size_of::<crate::composite_view::RedbCompositeReadView>()
                + crate::checkpoint_root::ROOT_ALLOCATION_CONTROL_BYTES
                + 128,
            || {
                Ok(Completion {
                    _owner: owner,
                    generation,
                    build_started: AtomicBool::new(false),
                    binding: OnceLock::new(),
                    provider: Mutex::new(None),
                    changed: std::sync::Condvar::new(),
                })
            },
        )?;
        Ok(Self {
            completion: Some(completion),
        })
    }

    pub(crate) fn try_complete(
        generation: LocatorGeneration,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        // A complete capability reuses the generation's one quota reservation.
        // It admits its distinct physical descriptor, but needs no second owner.
        let owner = generation.owner_handle();
        Self::allocate_completion(owner, OnceLock::from(Ok(generation)), class)
    }

    pub(crate) fn construction_owner(&self) -> Result<LocatorOwner, StorageError> {
        let state = self
            .completion
            .as_ref()
            .ok_or_else(|| StorageError::new(StorageErrorKind::Unavailable, None))?;
        if state.generation.get().is_some() {
            return Err(StorageError::new(
                StorageErrorKind::InvariantViolation,
                None,
            ));
        }
        Ok(state._owner.clone())
    }

    pub(crate) fn request_build(&self) -> bool {
        self.completion.as_ref().is_some_and(|state| {
            state.generation.get().is_none()
                && state
                    .build_started
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
        })
    }

    pub(crate) fn release_build_request(&self) {
        if let Some(state) = &self.completion {
            // Completion waiters observe the complete process_job handoff,
            // including ancillary Ready, rather than only its OnceLock write.
            let _predicate = state
                .provider
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.build_started.store(false, Ordering::Release);
            state.changed.notify_all();
        }
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) fn wait_for_builder_for_test(&self) -> Result<(), StorageError> {
        let state = self
            .completion
            .as_ref()
            .ok_or_else(|| StorageError::new(StorageErrorKind::Unavailable, None))?;
        let predicate = state
            .provider
            .lock()
            .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))?;
        let (_predicate, _) = state
            .changed
            .wait_timeout_while(predicate, std::time::Duration::from_secs(60), |_| {
                state.build_started.load(Ordering::Acquire)
            })
            .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))?;
        if state.build_started.load(Ordering::Acquire) {
            return Err(StorageError::new(StorageErrorKind::Unavailable, None));
        }
        self.require_complete().map(|_| ())
    }

    /// Both pending control and complete native directories use the original
    /// construction family. Caller retains the captured access before locking.
    pub(crate) fn provider_family_pool(&self) -> Result<Arc<NativeLocatorPool>, StorageError> {
        self.completion
            .as_ref()
            .map(|state| state._owner.family_pool())
            .ok_or_else(|| StorageError::new(StorageErrorKind::Unavailable, None))
    }

    pub(crate) fn prepare_provider_binding(
        &self,
        budget: &ProviderBudget,
    ) -> Result<PreparedLocatorProviderBinding<'_>, StorageError> {
        let state = self
            .completion
            .as_ref()
            .ok_or_else(|| StorageError::new(StorageErrorKind::Unavailable, None))?;
        if let Some(Err(kind)) = state.generation.get() {
            return Err(StorageError::new(*kind, None));
        }
        let provider = state
            .provider
            .lock()
            .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))?;
        if provider
            .as_ref()
            .is_some_and(|binding| !binding.marker.belongs_to(budget))
        {
            return Err(StorageError::new(StorageErrorKind::Unavailable, None));
        }
        Ok(PreparedLocatorProviderBinding {
            provider,
            budget: budget.clone(),
        })
    }

    /// Compatibility preflight. The global source plan instead holds the family
    /// guard throughout preflight and private preparation to make its cut exact.
    #[cfg(test)]
    pub(crate) fn provider_ownership_needed(
        &self,
        budget: &ProviderBudget,
    ) -> Result<u64, StorageError> {
        // Never hold a capability binding mutex while acquiring a family guard.
        drop(self.prepare_provider_binding(budget)?);
        self.provider_family_pool()?
            .lock_provider_family()?
            .provider_ownership_needed(budget)
    }

    #[cfg(test)]
    pub(crate) fn admit_provider_ownership_from(
        &self,
        reservation: &mut Reservation,
        budget: &ProviderBudget,
    ) -> Result<Option<Arc<OwnershipCharge>>, StorageError> {
        let pool = self.provider_family_pool()?;
        let mut family = pool.lock_provider_family()?;
        let binding = self.prepare_provider_binding(budget)?;
        let prepared = family.prepare_provider_ownership(reservation, budget)?;
        let marker = prepared.commit();
        binding.commit(Arc::clone(&marker));
        Ok(Some(marker))
    }

    pub(crate) fn source_binding(&self) -> Result<(DatabaseId, u64, SchemaHash), StorageError> {
        self.completion
            .as_ref()
            .and_then(|state| state.binding.get())
            .copied()
            .ok_or_else(|| StorageError::new(StorageErrorKind::Unavailable, None))
    }

    pub(crate) fn bind_source(
        &self,
        database: DatabaseId,
        incarnation: u64,
        registry: SchemaHash,
    ) -> Result<(), StorageError> {
        let state = self
            .completion
            .as_ref()
            .ok_or_else(|| StorageError::new(StorageErrorKind::Unavailable, None))?;
        if let Some(Ok(generation)) = state.generation.get() {
            generation.validate_database_incarnation(database, incarnation)?;
        }
        let binding = (database, incarnation, registry);
        if state.binding.set(binding).is_err() && state.binding.get() != Some(&binding) {
            return Err(StorageError::new(StorageErrorKind::CorruptData, None));
        }
        Ok(())
    }

    pub(crate) fn verify_source_binding(
        &self,
        database: DatabaseId,
        incarnation: u64,
        registry: SchemaHash,
    ) -> Result<(), StorageError> {
        if self.source_binding()? != (database, incarnation, registry) {
            return Err(StorageError::new(StorageErrorKind::CorruptData, None));
        }
        Ok(())
    }

    pub(crate) fn try_complete_from_source(
        generation: LocatorGeneration,
        source: &Self,
        class: LocatorAllocationClass,
    ) -> Result<Self, StorageError> {
        let (database, incarnation, registry) = source.source_binding()?;
        let result = Self::try_complete(generation, class)?;
        result.bind_source(database, incarnation, registry)?;
        Ok(result)
    }

    /// Private publication preparation may inspect a generation before its
    /// source metadata has been bound. Point readers must use require_complete.
    pub(crate) fn prepared_generation(&self) -> Result<&LocatorGeneration, StorageError> {
        match self
            .completion
            .as_ref()
            .and_then(|state| state.generation.get())
        {
            Some(Ok(generation)) => Ok(generation),
            Some(Err(kind)) => Err(StorageError::new(*kind, None)),
            None => Err(StorageError::new(StorageErrorKind::Unavailable, None)),
        }
    }

    pub(crate) fn require_complete(&self) -> Result<&LocatorGeneration, StorageError> {
        let (database, incarnation, _) = self.source_binding()?;
        let generation = self.prepared_generation()?;
        generation.validate_database_incarnation(database, incarnation)?;
        Ok(generation)
    }

    /// Initial admission distinguishes an invalidated proof from an attempt
    /// that ended under ordinary capacity pressure and remained Pending.
    pub(crate) fn require_complete_initial_dependency(
        &self,
    ) -> Result<&LocatorGeneration, StorageError> {
        if let Some(Err(kind)) = self
            .completion
            .as_ref()
            .and_then(|state| state.generation.get())
        {
            return Err(initial_dependency_proof_error(*kind));
        }
        self.require_complete()
    }

    pub(crate) fn install_generation(
        &self,
        generation: LocatorGeneration,
    ) -> Result<(), StorageError> {
        let _admission = native_provider_admission_gate()?;
        let state = self
            .completion
            .as_ref()
            .ok_or_else(|| StorageError::new(StorageErrorKind::Unavailable, None))?;
        let provider = state
            .provider
            .lock()
            .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))?;
        if state.generation.get().is_some() {
            return Err(StorageError::new(
                StorageErrorKind::InvariantViolation,
                None,
            ));
        }
        if !state._owner.same_owner(&generation.owner_handle()) {
            return Err(StorageError::new(
                StorageErrorKind::InvariantViolation,
                None,
            ));
        }
        if let Some((database, incarnation, _)) = state.binding.get() {
            generation.validate_database_incarnation(*database, *incarnation)?;
        }
        let provider_budget = provider.as_ref().map(|binding| binding.budget.clone());
        drop(provider);
        if let Some(budget) = provider_budget {
            // A private built generation is not yet reachable from the pinned
            // source. Refuse before the write-once publication if this existing
            // provider graph cannot retain its complete physical ownership.
            let needed = generation.provider_ownership_needed(&budget)?;
            // Keep a small parent control allowance because split deliberately
            // refuses consuming the complete parent reservation.
            let total = needed
                .checked_add(128)
                .ok_or_else(|| StorageError::new(StorageErrorKind::Unavailable, None))?;
            let mut work = budget
                .reserve(total)
                .map_err(|_| StorageError::new(StorageErrorKind::Unavailable, None))?;
            generation.admit_provider_ownership_from(&mut work, &budget)?;
        }
        state
            .generation
            .set(Ok(generation))
            .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))?;
        state.changed.notify_all();
        Ok(())
    }

    pub(crate) fn invalidate(&self, kind: StorageErrorKind) {
        if let Some(state) = &self.completion {
            let _predicate = state
                .provider
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let _ = state.generation.set(Err(kind));
            state.changed.notify_all();
        }
    }

    /// Cancellation and completion share the predicate mutex so a wake cannot
    /// fall between checking the request flag and entering the condition wait.
    pub(crate) fn wake_initial_dependency_waiters(&self) {
        if let Some(state) = &self.completion {
            let _predicate = state
                .provider
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.changed.notify_all();
        }
    }

    pub(crate) fn wait_for_initial_dependency(
        &self,
        cancelled: &AtomicBool,
    ) -> Result<(), StorageError> {
        self.wait_for_initial_dependency_with(cancelled, || {})
    }

    pub(crate) fn wait_for_initial_dependency_with(
        &self,
        cancelled: &AtomicBool,
        mut before_wait: impl FnMut(),
    ) -> Result<(), StorageError> {
        let state = self
            .completion
            .as_ref()
            .ok_or_else(|| StorageError::new(StorageErrorKind::InvariantViolation, None))?;
        let mut predicate = state
            .provider
            .lock()
            .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))?;
        loop {
            if cancelled.load(Ordering::Acquire) {
                return Err(StorageError::new(StorageErrorKind::Unavailable, None));
            }
            match state.generation.get() {
                Some(Err(kind)) => return Err(initial_dependency_proof_error(*kind)),
                Some(Ok(_)) if !state.build_started.load(Ordering::Acquire) => {
                    return self.require_complete().map(|_| ());
                }
                None if !state.build_started.load(Ordering::Acquire) => {
                    // This exact attempt ended without a complete directory.
                    // A dependency wait never restarts an unchanged owner.
                    return Err(StorageError::new(StorageErrorKind::Unavailable, None));
                }
                _ => {}
            }
            before_wait();
            predicate = state
                .changed
                .wait(predicate)
                .map_err(|_| StorageError::new(StorageErrorKind::InvariantViolation, None))?;
        }
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.completion
            .as_ref()
            .is_some_and(|state| state.generation.get().is_none())
    }

    pub(crate) fn same_construction(&self, other: &Self) -> bool {
        match (&self.completion, &other.completion) {
            (Some(left), Some(right)) => ChargedLocatorAllocation::ptr_eq(left, right),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: SEC-005, STO-032, REC-001
    #[test]
    fn initial_dependency_predicate_poison_is_integrity_and_cleanup_still_wakes() {
        let pending = bound_pending();
        assert!(pending.request_build());
        let state = pending.completion.as_ref().unwrap();
        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _predicate = state.provider.lock().unwrap();
            panic!("deterministic initial completion predicate poison");
        }));
        assert!(poison.is_err());
        assert_eq!(
            pending
                .wait_for_initial_dependency(&AtomicBool::new(false))
                .unwrap_err()
                .kind(),
            StorageErrorKind::InvariantViolation,
        );
        // Final cleanup must notify and release ownership even after poison;
        // it does not grant an incomplete generation membership authority.
        pending.wake_initial_dependency_waiters();
        pending.release_build_request();
        assert!(pending.is_pending());
        assert_eq!(
            pending.require_complete().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
    }

    fn bound_pending() -> LocatorCapability {
        let pending = LocatorCapability::try_pending(LocatorAllocationClass::Ordinary).unwrap();
        pending
            .bind_source(
                DatabaseId::from_unix_milliseconds_and_random(1, [7; 10]).unwrap(),
                1,
                SchemaHash::from_bytes([8; 32]),
            )
            .unwrap();
        pending
    }

    fn pending_with_canonical_segment() -> (
        LocatorCapability,
        Arc<riffdb_storage_api::StoredCommandSegmentV1>,
    ) {
        let command =
            crate::derived::follower_indexes::capsule(riffdb_types::CommitSequence::first());
        let (segment, _) = crate::derived::follower_indexes::segment(vec![command], None);
        let pending = LocatorCapability::try_pending(LocatorAllocationClass::Ordinary).unwrap();
        pending
            .bind_source(
                segment.database_id(),
                segment.history_incarnation(),
                SchemaHash::from_bytes([8; 32]),
            )
            .unwrap();
        (pending, Arc::new(segment))
    }

    fn exact_family_owned(capability: &LocatorCapability) -> u64 {
        let pool = capability.provider_family_pool().unwrap();
        let family = pool.lock_provider_family().unwrap();
        family.provider_owned_bytes()
    }

    fn drop_pending_and_check_final_release(pending: LocatorCapability, budget: &ProviderBudget) {
        let pool = pending.provider_family_pool().unwrap();
        drop(pending);
        while pool.drain_retired_step() {}
        drop(pool);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    fn attach_provider(
        capability: &LocatorCapability,
        budget: &ProviderBudget,
    ) -> Arc<OwnershipCharge> {
        let _admission = native_provider_admission_gate().unwrap();
        let needed = capability.provider_ownership_needed(budget).unwrap();
        let mut work = budget.reserve(needed + 128).unwrap();
        capability
            .admit_provider_ownership_from(&mut work, budget)
            .unwrap()
            .unwrap()
    }

    // req: TXN-042, STO-032
    #[test]
    fn locator_completion_wait_observes_finished_worker_without_lost_notification() {
        let pending = bound_pending();
        assert!(pending.request_build());
        let generation = LocatorGeneration::try_empty_with_owner(
            pending.construction_owner().unwrap(),
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        pending.install_generation(generation).unwrap();
        let entering_wait = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let captured = pending.clone();
            let entering = Arc::clone(&entering_wait);
            let waiter = scope.spawn(move || {
                let state = captured.completion.as_ref().unwrap();
                let predicate = state.provider.lock().unwrap();
                // Holding the predicate mutex forces worker completion to
                // follow Condvar's atomic unlock-and-wait transition.
                entering.wait();
                let (_predicate, timeout) = state
                    .changed
                    .wait_timeout_while(predicate, std::time::Duration::from_secs(60), |_| {
                        state.build_started.load(Ordering::Acquire)
                    })
                    .unwrap();
                assert!(!timeout.timed_out());
                captured.require_complete().unwrap();
            });
            entering_wait.wait();
            pending.release_build_request();
            waiter.join().unwrap();
        });
        pending.wait_for_builder_for_test().unwrap();
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn pending_checkpoint_provider_binding_refuses_foreign_graph_and_charges_future_completion() {
        let (pending, segment) = pending_with_canonical_segment();
        let budget = ProviderBudget::new();
        let marker = attach_provider(&pending, &budget);
        assert!(pending.is_pending());
        assert_eq!(
            pending.require_complete().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        let initially_owned = exact_family_owned(&pending);
        assert_eq!(budget.observation().unwrap().owned_bytes, initially_owned);
        assert!(initially_owned > marker.bytes());
        assert_eq!(pending.provider_ownership_needed(&budget).unwrap(), 0);
        let foreign = ProviderBudget::new();
        assert_eq!(
            pending
                .provider_ownership_needed(&foreign)
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
        // The first cut covers only Pending controls. Build genuine canonical
        // members afterwards with that same original construction owner.
        let generation = LocatorGeneration::rebuild_test_segments_with_owner(
            [Arc::clone(&segment)],
            pending.construction_owner().unwrap(),
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        // Remove only unreferenced construction paths before measuring the
        // exact live increment; the captured generation itself stays pinned.
        while generation.provider_family_pool().drain_retired_step() {}
        let new_physical = generation.provider_ownership_needed(&budget).unwrap();
        assert!(new_physical > 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, initially_owned);
        pending.install_generation(generation).unwrap();
        let completed = pending.require_complete().unwrap();
        assert_eq!(completed.coverage(), Some(segment.last_commit_sequence()));
        assert!(
            completed
                .capture_command_at(segment.first_commit_sequence())
                .unwrap()
                .is_some()
        );
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            initially_owned + new_physical
        );
        assert_eq!(exact_family_owned(&pending), initially_owned + new_physical);
        assert_eq!(
            pending
                .provider_ownership_needed(&foreign)
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );
        drop(marker);
        drop_pending_and_check_final_release(pending, &budget);
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn pending_checkpoint_completion_refuses_before_provider_growth_and_retries_same_owner() {
        use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;
        let (pending, segment) = pending_with_canonical_segment();
        let budget = ProviderBudget::new();
        let marker = attach_provider(&pending, &budget);
        let generation = LocatorGeneration::rebuild_test_segments_with_owner(
            [Arc::clone(&segment)],
            pending.construction_owner().unwrap(),
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        // Remove only unreferenced construction paths before measuring the
        // exact live increment; the captured generation itself stays pinned.
        while generation.provider_family_pool().drain_retired_step() {}
        let new_physical = generation.provider_ownership_needed(&budget).unwrap();
        assert!(
            new_physical > 0,
            "canonical members were allocated after the initial cut"
        );
        let before = budget.observation().unwrap();
        let pressure = budget
            .reserve(
                EXACT_PROVIDER_BUDGET_BYTES
                    - before.fixed_owner_bytes
                    - before.owned_bytes
                    - before.reserved_bytes,
            )
            .unwrap();
        assert_eq!(
            pending
                .install_generation(generation.clone())
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert!(pending.is_pending());
        assert_eq!(
            pending.require_complete().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            generation.provider_ownership_needed(&budget).unwrap(),
            new_physical,
            "refused capacity cannot cover the newly allocated physical members"
        );
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            before.owned_bytes
        );
        assert_eq!(exact_family_owned(&pending), before.owned_bytes);
        drop(pressure);
        assert_eq!(budget.observation().unwrap(), before);
        assert!(
            pending
                .construction_owner()
                .unwrap()
                .same_owner(&generation.owner_handle())
        );
        pending.install_generation(generation).unwrap();
        let completed = pending.require_complete().unwrap();
        assert_eq!(completed.coverage(), Some(segment.last_commit_sequence()));
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            before.owned_bytes + new_physical
        );
        assert_eq!(
            exact_family_owned(&pending),
            before.owned_bytes + new_physical
        );
        drop(marker);
        drop_pending_and_check_final_release(pending, &budget);
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn pending_checkpoint_failed_provider_plan_does_not_bind_a_graph() {
        use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;
        let pending = bound_pending();
        let budget = ProviderBudget::new();
        let before = budget.observation().unwrap();
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - before.fixed_owner_bytes)
            .unwrap();
        {
            let _admission = native_provider_admission_gate().unwrap();
            let needed = pending.provider_ownership_needed(&budget).unwrap();
            assert!(budget.reserve(needed + 128).is_err());
        }
        assert!(pending.is_pending());
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        let another = ProviderBudget::new();
        let marker = attach_provider(&pending, &another);
        assert!(
            marker.belongs_to(&another),
            "refused admission must leave attachment available"
        );
        assert_eq!(
            pending
                .provider_ownership_needed(&budget)
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );
        drop(pressure);
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn pending_checkpoint_provider_attachment_and_completion_have_deterministic_orders() {
        for admission_first in [true, false] {
            let pending = bound_pending();
            let budget = ProviderBudget::new();
            let generation = LocatorGeneration::try_empty_with_owner(
                pending.construction_owner().unwrap(),
                LocatorAllocationClass::Ordinary,
            )
            .unwrap();
            let entered = Arc::new(std::sync::Barrier::new(2));
            if admission_first {
                let admission = native_provider_admission_gate().unwrap();
                std::thread::scope(|scope| {
                    let work = pending.clone();
                    let signal = Arc::clone(&entered);
                    let installer = scope.spawn(move || {
                        signal.wait();
                        work.install_generation(generation).unwrap();
                    });
                    entered.wait();
                    let needed = pending.provider_ownership_needed(&budget).unwrap();
                    let mut reservation = budget.reserve(needed + 128).unwrap();
                    let marker = pending
                        .admit_provider_ownership_from(&mut reservation, &budget)
                        .unwrap()
                        .unwrap();
                    assert!(
                        pending.is_pending(),
                        "completion cannot pass the held admission gate"
                    );
                    drop(reservation);
                    drop(admission);
                    installer.join().unwrap();
                    assert_eq!(
                        budget.observation().unwrap().owned_bytes,
                        exact_family_owned(&pending)
                    );
                    assert!(budget.observation().unwrap().owned_bytes > marker.bytes());
                });
            } else {
                let published = Arc::new(std::sync::Barrier::new(2));
                std::thread::scope(|scope| {
                    let work = pending.clone();
                    let graph = budget.clone();
                    let signal = Arc::clone(&entered);
                    let completed = Arc::clone(&published);
                    let provider = scope.spawn(move || {
                        signal.wait();
                        completed.wait();
                        attach_provider(&work, &graph)
                    });
                    entered.wait();
                    // The explicit publication barrier prevents provider
                    // discovery until completion has installed its generation.
                    pending.install_generation(generation).unwrap();
                    pending.require_complete().unwrap();
                    published.wait();
                    drop(provider.join().unwrap());
                    assert_eq!(
                        budget.observation().unwrap().owned_bytes,
                        exact_family_owned(&pending)
                    );
                });
            }
            pending.require_complete().unwrap();
            assert_eq!(pending.provider_ownership_needed(&budget).unwrap(), 0);
            drop_pending_and_check_final_release(pending, &budget);
        }
    }

    // req: TXN-042, STO-032, REC-001
    #[test]
    fn locator_generation_pending_never_proves_absence() {
        let pending = LocatorCapability::try_pending(LocatorAllocationClass::Ordinary).unwrap();
        let captured = pending.clone();
        let database = DatabaseId::from_unix_milliseconds_and_random(1, [8; 10]).unwrap();
        let registry = SchemaHash::from_bytes([1; 32]);
        pending.bind_source(database, 1, registry).unwrap();
        assert_eq!(
            captured.require_complete().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        assert!(captured.same_construction(&pending));
        let generation = LocatorGeneration::try_empty_with_owner(
            pending.construction_owner().unwrap(),
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        pending.install_generation(generation).unwrap();
        assert!(captured.require_complete().unwrap().coverage().is_none());
        let replacement = LocatorGeneration::try_empty(LocatorAllocationClass::Ordinary).unwrap();
        assert_eq!(
            pending.install_generation(replacement).unwrap_err().kind(),
            StorageErrorKind::InvariantViolation
        );
        assert_eq!(
            LocatorCapability::invalid()
                .require_complete()
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        let invalid = LocatorCapability::try_pending(LocatorAllocationClass::Ordinary).unwrap();
        invalid.bind_source(database, 1, registry).unwrap();
        invalid.invalidate(StorageErrorKind::CorruptData);
        assert_eq!(
            invalid.require_complete().err().unwrap().kind(),
            StorageErrorKind::CorruptData
        );
    }
    // req: TXN-042, STO-032, REC-001
    #[test]
    fn locator_capability_source_binding_is_immutable_and_required_for_answers() {
        let generation = LocatorGeneration::try_empty(LocatorAllocationClass::Ordinary).unwrap();
        let capability =
            LocatorCapability::try_complete(generation, LocatorAllocationClass::Ordinary).unwrap();
        assert_eq!(
            capability.require_complete().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        let database = DatabaseId::from_unix_milliseconds_and_random(1, [3; 10]).unwrap();
        let other_database = DatabaseId::from_unix_milliseconds_and_random(1, [4; 10]).unwrap();
        let registry = SchemaHash::from_bytes([5; 32]);
        capability.bind_source(database, 2, registry).unwrap();
        capability.bind_source(database, 2, registry).unwrap();
        capability.require_complete().unwrap();
        assert_eq!(
            capability
                .bind_source(other_database, 2, registry)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            capability
                .verify_source_binding(database, 3, registry)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            capability
                .verify_source_binding(database, 2, SchemaHash::from_bytes([6; 32]))
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            capability.source_binding().unwrap(),
            (database, 2, registry)
        );
        capability.require_complete().unwrap();
    }

    // req: TXN-042, STO-032, REC-001
    #[test]
    fn locator_pending_completion_refuses_foreign_owner_before_original_completion() {
        let pending = LocatorCapability::try_pending(LocatorAllocationClass::Ordinary).unwrap();
        let foreign = LocatorGeneration::try_empty(LocatorAllocationClass::Ordinary).unwrap();
        assert_eq!(
            pending.install_generation(foreign).unwrap_err().kind(),
            StorageErrorKind::InvariantViolation
        );
        assert!(pending.is_pending());
        let original = pending.construction_owner().unwrap();
        let generation = LocatorGeneration::try_empty_with_owner(
            original.clone(),
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        assert!(original.same_owner(&generation.owner_handle()));
        pending.install_generation(generation).unwrap();
        assert!(!pending.is_pending());
    }
}
