//! One actual engine and its physical cache ownership across every read pin.
use std::ops::{Deref, DerefMut};

use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::exact_provider_budget_v1::{
    PreparedSharedPhysicalOwnership, ProviderBudget, Reservation, SharedPhysicalOwnershipCell,
};

use crate::locator_memory::{LocatorAllocationClass, LocatorCharge, NativeLocatorPool};

pub(crate) struct EngineOwner<T> {
    // Destroy the actual engine/cache before its final numerical ownership.
    database: T,
    cache_bytes: usize,
    // Fixed source context data drops before the enclosing native control charge.
    native_origin_context: crate::native_source_fresh::OriginContextCellV1,
    _control: LocatorCharge,
    provider: SharedPhysicalOwnershipCell,
    locator_build_domain: crate::locator_build_domain::LocatorBuildDomain,
    source_working_domain: crate::source_working_domain::SourceWorkingDomain,
}

// Own every partially constructed field so all failure paths retire the
// actual native database before either numerical domain or control allowance.
struct EngineOwnerConstruction<T> {
    database: T,
    control: Option<LocatorCharge>,
    locator_build_domain: Option<crate::locator_build_domain::LocatorBuildDomain>,
    source_working_domain: Option<crate::source_working_domain::SourceWorkingDomain>,
}

// The actual primary owner, not a caller-selected numeric token, selects the
// domain inherited by each derived owner. The entry survives binding and
// remains held by the caller through publication or failed native teardown.
pub(crate) struct OrdinaryPairedEngineConstructionV1<'owner, T> {
    _owner: &'owner EngineOwner<T>,
    source_working_domain: Option<crate::source_working_domain::SourceWorkingDomain>,
    _entry: crate::source_working_domain::SourceOrdinaryDerivedEntryV1,
}
impl<T> OrdinaryPairedEngineConstructionV1<'_, T> {
    pub(crate) fn bind<U>(
        &mut self,
        database: U,
        cache_bytes: usize,
        class: LocatorAllocationClass,
    ) -> Result<riffdb_types::OwnedCapacitySharedV1<EngineOwner<U>>, StorageError> {
        let mut pending = EngineOwnerConstruction {
            database,
            control: None,
            locator_build_domain: None,
            source_working_domain: None,
        };
        pending.source_working_domain =
            Some(self.source_working_domain.take().ok_or_else(unavailable)?);
        EngineOwner::<U>::finish_construction(
            pending,
            cache_bytes,
            class,
            &NativeLocatorPool::global(),
        )
    }
}

impl<T> EngineOwner<T> {
    pub(crate) fn try_new(
        database: T,
        cache_bytes: usize,
        class: LocatorAllocationClass,
    ) -> Result<riffdb_types::OwnedCapacitySharedV1<Self>, StorageError> {
        Self::try_new_in_pool(database, cache_bytes, class, &NativeLocatorPool::global())
    }

    fn try_new_in_pool(
        database: T,
        cache_bytes: usize,
        class: LocatorAllocationClass,
        pool: &NativeLocatorPool,
    ) -> Result<riffdb_types::OwnedCapacitySharedV1<Self>, StorageError> {
        Self::try_new_in_pool_with_source_domain(database, cache_bytes, class, pool, None)
    }

    pub(crate) fn try_new_configured(
        database: T,
        cache_bytes: usize,
        class: LocatorAllocationClass,
        source_working_domain: crate::source_working_domain::SourceWorkingDomain,
    ) -> Result<riffdb_types::OwnedCapacitySharedV1<Self>, StorageError> {
        Self::try_new_in_pool_with_source_domain(
            database,
            cache_bytes,
            class,
            &NativeLocatorPool::global(),
            Some(source_working_domain),
        )
    }
    fn try_new_in_pool_with_source_domain(
        database: T,
        cache_bytes: usize,
        class: LocatorAllocationClass,
        pool: &NativeLocatorPool,
        source_working_domain: Option<crate::source_working_domain::SourceWorkingDomain>,
    ) -> Result<riffdb_types::OwnedCapacitySharedV1<Self>, StorageError> {
        Self::finish_construction(
            EngineOwnerConstruction {
                database,
                control: None,
                locator_build_domain: None,
                source_working_domain,
            },
            cache_bytes,
            class,
            pool,
        )
    }

    fn finish_construction(
        mut pending: EngineOwnerConstruction<T>,
        cache_bytes: usize,
        class: LocatorAllocationClass,
        pool: &NativeLocatorPool,
    ) -> Result<riffdb_types::OwnedCapacitySharedV1<Self>, StorageError> {
        pending.control = Some(pool.admit(std::mem::size_of::<Self>() + 128, class)?);
        pending.locator_build_domain = Some(
            crate::locator_build_domain::LocatorBuildDomain::try_new(pool, class)?,
        );
        if pending.source_working_domain.is_none() {
            pending.source_working_domain =
                Some(crate::source_working_domain::SourceWorkingDomain::try_new()?);
        }
        // Check the entire construction before moving any native/control field.
        // Successful transfer is infallible; failure destroys the intact owner.
        match pending {
            EngineOwnerConstruction {
                database,
                control: Some(control),
                locator_build_domain: Some(locator_build_domain),
                source_working_domain: Some(source_working_domain),
            } => Ok(riffdb_types::OwnedCapacitySharedV1::new(Self {
                database,
                cache_bytes,
                native_origin_context: crate::native_source_fresh::OriginContextCellV1::new(),
                _control: control,
                provider: SharedPhysicalOwnershipCell::new(),
                locator_build_domain,
                source_working_domain,
            })),
            pending => {
                drop(pending);
                Err(unavailable())
            }
        }
    }

    pub(crate) fn begin_ordinary_derived_construction(
        &self,
    ) -> Result<OrdinaryPairedEngineConstructionV1<'_, T>, StorageError> {
        let entry = self
            .source_working_domain
            .begin_ordinary_derived_construction()?;
        // Preadmit the paired inline header before any ordinary native Builder,
        // create or initialization write. No fresh ledger/domain is substituted.
        let source_working_domain = self.source_working_domain.try_share()?;
        Ok(OrdinaryPairedEngineConstructionV1 {
            _owner: self,
            source_working_domain: Some(source_working_domain),
            _entry: entry,
        })
    }

    pub(crate) fn locator_build_domain(&self) -> &crate::locator_build_domain::LocatorBuildDomain {
        &self.locator_build_domain
    }
    // Heap-bearing authoritative source DTOs use this ledger. Fixed-scalar
    // command metadata validation must not depend on worker capacity.
    pub(crate) fn source_working_budget(
        &self,
    ) -> &riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1 {
        self.source_working_domain.budget()
    }
    pub(crate) fn native_origin_context(&self) -> &crate::native_source_fresh::OriginContextCellV1 {
        &self.native_origin_context
    }
    pub(crate) fn source_usage_domain(&self) -> &crate::source_usage_domain::SourceUsageDomain {
        self.source_working_domain.usage()
    }

    pub(crate) fn source_prefix_pins(&self) -> &crate::source_prefix_pin::SourcePrefixPins {
        self.source_working_domain.prefix_pins()
    }
    pub(crate) fn database(&self) -> &T {
        &self.database
    }

    /// Bounded maintenance decode work shares the actual engine control pool.
    /// It does not consume Source-worker capacity or create a new native pool.
    pub(crate) fn admit_maintenance_decode(
        &self,
        bytes: usize,
    ) -> Result<LocatorCharge, StorageError> {
        self._control.admit_maintenance(bytes)
    }

    #[cfg(test)]
    pub(crate) fn provider_charge_bytes(&self) -> u64 {
        let own = self.provider.owned_bytes().unwrap()
            + self.locator_build_domain.provider_charge_bytes();
        if self.source_working_domain.is_paired_derived() {
            own
        } else {
            own + self.source_working_domain.provider_execution_charge_bytes()
                + self.source_prefix_pins().provider_charge_bytes()
                + self
                    .source_working_budget()
                    .provider_control_owned_bytes()
                    .unwrap()
        }
    }

    fn provider_bound(&self) -> Result<u64, StorageError> {
        self.cache_bytes
            .checked_add(self._control.bytes())
            .and_then(|bytes| bytes.checked_add(128))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(unavailable)
    }

    // Both operations belong to the existing complete-plan admission gate.
    pub(crate) fn provider_ownership_needed(
        &self,
        budget: &ProviderBudget,
    ) -> Result<u64, StorageError> {
        let engine = self
            .provider
            .provider_ownership_needed(self.provider_bound()?, budget)
            .map_err(|_| unavailable())?;
        let engine = engine
            .checked_add(
                self.locator_build_domain
                    .provider_ownership_needed(budget)?,
            )
            .ok_or_else(unavailable)?;
        if self.source_working_domain.is_paired_derived() {
            return Ok(engine);
        }
        let engine = engine
            .checked_add(
                self.source_prefix_pins()
                    .provider_ownership_needed(budget)?,
            )
            .ok_or_else(unavailable)?;
        let engine = engine
            .checked_add(
                self.source_working_domain
                    .provider_execution_ownership_needed(budget)?,
            )
            .ok_or_else(unavailable)?;
        engine
            .checked_add(
                self.source_working_budget()
                    .provider_control_ownership_needed(budget)
                    .map_err(|_| unavailable())?,
            )
            .ok_or_else(unavailable)
    }

    #[cfg(test)]
    pub(crate) fn admit_provider_ownership_from(
        &self,
        work: &mut Reservation,
        budget: &ProviderBudget,
    ) -> Result<(), StorageError> {
        self.prepare_provider_ownership_from(work, budget)?.commit();
        Ok(())
    }

    pub(crate) fn prepare_provider_ownership_from(
        &self,
        work: &mut Reservation,
        budget: &ProviderBudget,
    ) -> Result<PreparedEngineProviderOwnership<'_>, StorageError> {
        let engine = self
            .provider
            .prepare_provider_ownership_from(self.provider_bound()?, work, budget)
            .map_err(|_| unavailable())?;
        let domain = self
            .locator_build_domain
            .prepare_provider_ownership_from(work, budget)?;
        let (execution, source_accountant, source_pins) =
            if self.source_working_domain.is_paired_derived() {
                (None, None, None)
            } else {
                let execution = self
                    .source_working_domain
                    .prepare_provider_execution_ownership_from(work, budget)?;
                let source_accountant = self
                    .source_working_budget()
                    .prepare_provider_control_ownership_from(work, budget)
                    .map_err(|_| unavailable())?;
                let source_pins = self
                    .source_prefix_pins()
                    .prepare_provider_ownership_from(work, budget)?;
                (execution, Some(source_accountant), Some(source_pins))
            };
        Ok(PreparedEngineProviderOwnership {
            execution,
            source_pins,
            engine,
            domain,
            source_accountant,
        })
    }
}

pub(crate) struct PreparedEngineProviderOwnership<'a> {
    execution: Option<PreparedSharedPhysicalOwnership<'a>>,
    source_pins: Option<PreparedSharedPhysicalOwnership<'a>>,
    engine: PreparedSharedPhysicalOwnership<'a>,
    domain: PreparedSharedPhysicalOwnership<'a>,
    source_accountant: Option<PreparedSharedPhysicalOwnership<'a>>,
}
impl PreparedEngineProviderOwnership<'_> {
    pub(crate) fn commit(self) {
        self.engine.commit();
        if let Some(source_pins) = self.source_pins {
            source_pins.commit();
        }
        self.domain.commit();
        if let Some(source_accountant) = self.source_accountant {
            source_accountant.commit();
        }
        if let Some(execution) = self.execution {
            execution.commit();
        }
    }
}

impl<T> Deref for EngineOwner<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.database
    }
}

impl<T> DerefMut for EngineOwner<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.database
    }
}

impl<T> std::fmt::Debug for EngineOwner<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EngineOwner")
            .field("cache_bytes", &self.cache_bytes)
            .finish_non_exhaustive()
    }
}

fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

#[cfg(test)]
#[path = "engine_owner_maintenance_decode_tests.rs"]
mod maintenance_decode_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct Cache {
        bytes: Vec<u8>,
        budget: ProviderBudget,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for Cache {
        fn drop(&mut self) {
            assert!(
                self.budget.observation().unwrap().owned_bytes >= self.bytes.capacity() as u64,
                "cache bytes remain charged throughout actual engine destruction"
            );
            self.dropped.store(true, Ordering::Release);
        }
    }

    fn admit<T>(engine: &EngineOwner<T>, budget: &ProviderBudget) -> Result<(), StorageError> {
        let _gate = crate::locator_capability::native_provider_admission_gate()?;
        let needed = engine.provider_ownership_needed(budget)?;
        if needed == 0 {
            return Ok(());
        }
        let mut work = budget.reserve(needed + 1).map_err(|_| unavailable())?;
        engine.admit_provider_ownership_from(&mut work, budget)
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn engine_cache_charge_follows_actual_final_engine_drop_and_shared_aliases() {
        let budget = ProviderBudget::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let cache = Cache {
            bytes: vec![0; 128 * 1024],
            budget: budget.clone(),
            dropped: Arc::clone(&dropped),
        };
        let capacity = cache.bytes.capacity();
        let engine =
            EngineOwner::try_new(cache, capacity, LocatorAllocationClass::Ordinary).unwrap();
        admit(&engine, &budget).unwrap();
        let charged = budget.observation().unwrap().owned_bytes;
        let alias = (engine).clone();
        admit(&alias, &budget).unwrap();
        assert_eq!(budget.observation().unwrap().owned_bytes, charged);
        drop(engine);
        assert!(!dropped.load(Ordering::Acquire));
        assert_eq!(budget.observation().unwrap().owned_bytes, charged);
        drop(alias);
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn engine_cache_failed_admission_leaves_no_binding_and_live_foreign_binding_refuses() {
        let engine =
            EngineOwner::try_new((), 128 * 1024, LocatorAllocationClass::Ordinary).unwrap();
        let first = ProviderBudget::new();
        let occupied = first
            .reserve(
                EXACT_PROVIDER_BUDGET_BYTES
                    - first.observation().unwrap().fixed_owner_bytes
                    - 64 * 1024,
            )
            .unwrap();
        assert_eq!(
            admit(&engine, &first).unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(first.observation().unwrap().owned_bytes, 0);
        drop(occupied);
        let second = ProviderBudget::new();
        admit(&engine, &second).unwrap();
        assert_eq!(
            admit(&engine, &first).unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(first.observation().unwrap().owned_bytes, 0);
        drop(engine);
        assert_eq!(second.observation().unwrap().owned_bytes, 0);
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn cancelled_private_engine_plan_releases_bytes_without_attaching_a_graph() {
        let engine =
            EngineOwner::try_new((), 128 * 1024, LocatorAllocationClass::Ordinary).unwrap();
        let first = ProviderBudget::new();
        {
            let _gate = crate::locator_capability::native_provider_admission_gate().unwrap();
            let needed = engine.provider_ownership_needed(&first).unwrap();
            let mut work = first.reserve(needed + 1).unwrap();
            let prepared = engine
                .prepare_provider_ownership_from(&mut work, &first)
                .unwrap();
            assert!(first.observation().unwrap().owned_bytes > 0);
            drop(prepared);
            assert_eq!(first.observation().unwrap().owned_bytes, 0);
        }
        assert_eq!(first.observation().unwrap().reserved_bytes, 0);
        let later_graph = ProviderBudget::new();
        admit(&engine, &later_graph).unwrap();
        assert_eq!(first.observation().unwrap().owned_bytes, 0);
        drop(engine);
        assert_eq!(later_graph.observation().unwrap().owned_bytes, 0);
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn maintenance_engine_control_uses_reserved_headroom_without_relaxing_ordinary_open() {
        let pool = NativeLocatorPool::isolated_for_tests();
        let occupied = pool.exhaust_ordinary_for_tests();
        assert_eq!(
            EngineOwner::try_new_in_pool((), 0, LocatorAllocationClass::Ordinary, &pool)
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        let before = pool.charged_bytes();
        let maintenance =
            EngineOwner::try_new_in_pool((), 0, LocatorAllocationClass::Maintenance, &pool)
                .unwrap();
        assert!(pool.charged_bytes() > before);
        assert!(pool.charged_bytes() <= crate::locator_memory::LOCATOR_TOTAL_BYTES);
        drop(maintenance);
        assert_eq!(pool.charged_bytes(), before);
        drop(occupied);
    }
    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn domain_provider_charge_outlives_engine_until_final_registered_control_drop() {
        let budget = ProviderBudget::new();
        let engine = EngineOwner::try_new((), 0, LocatorAllocationClass::Ordinary).unwrap();
        admit(&engine, &budget).unwrap();
        let owner = engine.locator_build_domain().clone();
        let domain_bytes = owner.provider_charge_bytes();
        assert!(domain_bytes > 0);
        let work = owner.register().unwrap();
        drop(engine);
        assert_eq!(budget.observation().unwrap().owned_bytes, domain_bytes);
        drop(owner);
        assert_eq!(budget.observation().unwrap().owned_bytes, domain_bytes);
        drop(work);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[cfg(test)]
#[path = "source_working_domain_tests.rs"]
mod source_working_domain_tests;

#[cfg(unix)]
pub(crate) mod source_reopen;
mod weak_backing;
pub(crate) use weak_backing::MaintenanceWeakBackingControlV1;
