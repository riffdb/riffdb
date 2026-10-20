//! Source-ledger numerical ownership for the private native resource pool.
//!
//! Draft integration component: no writer, plan, engine, callback, raw Arc or
//! Weak owner is exposed. The native epoch transition is a separate required
//! integration boundary; byte counts alone must never authorize retirement.
#![cfg_attr(not(test), allow(dead_code))]

use std::{
    mem::{align_of, size_of},
    sync::{Arc, Mutex},
};

use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
use riffdb_types::{ProjectionSourceWorkReservationV1, owned_allocation_capacity_bound_v1};

/// The concrete T retained by the native numerical carrier. Cloning performs
/// no allocation. Every strong reference uses the same final-release protocol.
pub(super) struct SourceNativeNumericalOwnerV1(Option<Arc<NumericalState>>);

struct NumericalState {
    // Numerical accounting only. This contains no engine/native pool, budget
    // acquisition capability, Weak owner, callback or worker backreference.
    // All concrete same-ledger increments are admitted BEFORE this lock.
    admission: Mutex<ProjectionSourceWorkReservationV1>,
    // Pool Arc allocation plus native counter/erased-carrier allocations must
    // remain admitted even when no native payload or metadata buffer is live.
    control_floor: u64,
}

impl Clone for SourceNativeNumericalOwnerV1 {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Drop for SourceNativeNumericalOwnerV1 {
    fn drop(&mut self) {
        if let Some(state) = self.0.take() {
            // On the final strong release, the actual Arc allocation is freed
            // before its returned state refunds the reservation. There are no
            // Weak references, and concurrent final drops use into_inner too.
            drop(Arc::into_inner(state));
        }
    }
}

impl SourceNativeNumericalOwnerV1 {
    /// Quote the concrete pool allocation before constructing it. Native must
    /// separately quote its actual carrier T=SourceNativeNumericalOwnerV1.
    pub(super) fn pool_control_bound() -> Result<u64, BudgetUnavailable> {
        let layout = size_of::<NumericalState>()
            .checked_add(2 * size_of::<usize>())
            .ok_or(BudgetUnavailable)?;
        owned_allocation_capacity_bound_v1(
            layout,
            align_of::<NumericalState>().max(align_of::<usize>()),
        )
        .map_err(|_| BudgetUnavailable)
    }

    /// Consumes already admitted full peak capacity. This does not acquire new
    /// allowance, settle an ordinary baseline, or grant mutation authority.
    pub(super) fn from_prepared(
        admission: ProjectionSourceWorkReservationV1,
        native_control_bound: u64,
    ) -> Result<Self, BudgetUnavailable> {
        let control_floor = Self::pool_control_bound()?
            .checked_add(native_control_bound)
            .ok_or(BudgetUnavailable)?;
        if native_control_bound == 0 || admission.bytes() < control_floor {
            return Err(BudgetUnavailable);
        }
        Ok(Self(Some(Arc::new(NumericalState {
            admission: Mutex::new(admission),
            control_floor,
        }))))
    }

    fn state(&self) -> Result<&NumericalState, BudgetUnavailable> {
        self.0.as_deref().ok_or(BudgetUnavailable)
    }

    fn admitted_bytes(&self) -> Result<u64, BudgetUnavailable> {
        Ok(self
            .state()?
            .admission
            .lock()
            .map_err(|_| BudgetUnavailable)?
            .bytes())
    }
}

/// Numerical native capacity only. A complete native plan must separately
/// validate primary/root/epoch and all retained metadata before using this pool.
/// No writer, transaction, callback or operational conversion is exposed here.
pub(super) struct SourceNativeNumericalPoolV1 {
    // Native Counter/erased carrier and every alias die before this controller's
    // last numerical reference. Escaping native guards retain another actual T.
    native: redb::NativeNumericalPool,
    owner: SourceNativeNumericalOwnerV1,
}

impl Clone for SourceNativeNumericalPoolV1 {
    fn clone(&self) -> Self {
        Self {
            native: self.native.clone(),
            owner: self.owner.clone(),
        }
    }
}

impl SourceNativeNumericalPoolV1 {
    pub(super) fn control_capacity_bound() -> Result<u64, BudgetUnavailable> {
        let native =
            redb::NativeNumericalPool::control_capacity_bytes::<SourceNativeNumericalOwnerV1>()
                .map_err(|_| BudgetUnavailable)?;
        SourceNativeNumericalOwnerV1::pool_control_bound()?
            .checked_add(native)
            .ok_or(BudgetUnavailable)
    }

    /// Full admission has already happened in the original Source ledger.
    /// This private constructor installs the ACTUAL closed reservation owner,
    /// with the native carrier quote for that concrete T, never a unit marker.
    pub(super) fn from_prepared(
        admission: ProjectionSourceWorkReservationV1,
    ) -> Result<Self, BudgetUnavailable> {
        let native =
            redb::NativeNumericalPool::control_capacity_bytes::<SourceNativeNumericalOwnerV1>()
                .map_err(|_| BudgetUnavailable)?;
        let owner = SourceNativeNumericalOwnerV1::from_prepared(admission, native)?;
        let limit = owner
            .admitted_bytes()?
            .checked_sub(owner.state()?.control_floor)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or(BudgetUnavailable)?;
        let native = redb::NativeNumericalPool::from_retained_data(limit, owner.clone())
            .map_err(|_| BudgetUnavailable)?;
        Ok(Self { native, owner })
    }

    pub(super) fn native_pool(&self) -> &redb::NativeNumericalPool {
        &self.native
    }

    /// The complete fixed owner calls this ONLY with its full additional peak
    /// already reserved from the originating ledger. Absorb checks that actual
    /// ledger identity. There is no native limit change/refund in this method.
    /// A subsequent native epoch transition may use the returned total ceiling;
    /// on its refusal this additional allowance stays owned until a fully
    /// joined complete physical completion permits settlement.
    pub(super) fn absorb_prepared_increment(
        &self,
        increment: ProjectionSourceWorkReservationV1,
    ) -> Result<usize, BudgetUnavailable> {
        let state = self.owner.state()?;
        let mut admission = state.admission.lock().map_err(|_| BudgetUnavailable)?;
        let total = admission
            .bytes()
            .checked_add(increment.bytes())
            .ok_or(BudgetUnavailable)?;
        let ceiling = total
            .checked_sub(state.control_floor)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or(BudgetUnavailable)?;
        admission.absorb(increment)?;
        Ok(ceiling)
    }

    pub(super) fn already_admitted_ceiling(&self) -> Result<usize, BudgetUnavailable> {
        self.owner
            .admitted_bytes()?
            .checked_sub(self.owner.state()?.control_floor)
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or(BudgetUnavailable)
    }
    // Deliberately NO trim/current-only settlement method. The native complete
    // owner must first consume all prepared/IO/error work, close this exact
    // pool's serial, join all fifteen guarded physical families and lower its
    // ceiling. That final public numerical seam is held for exact root review.
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::ProjectionSourceWorkingBudgetV1;

    // req: STO-032
    #[test]
    fn actual_source_reservation_survives_controller_drop_until_last_native_pool_owner() {
        let budget = ProjectionSourceWorkingBudgetV1::new();
        let controls = SourceNativeNumericalPoolV1::control_capacity_bound().unwrap();
        let maximum = controls.checked_add(8192).unwrap();
        let pool =
            SourceNativeNumericalPoolV1::from_prepared(budget.reserve(maximum).unwrap()).unwrap();
        assert_eq!(pool.native_pool().reserved_bytes(), 0);
        let native_alias = pool.native_pool().clone();
        let remaining = pool.clone();
        drop(pool);
        drop(remaining);
        assert_eq!(budget.observation().unwrap().reserved_bytes, maximum);
        assert_eq!(budget.observation().unwrap().reservations, 1);
        drop(native_alias);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().reservations, 0);
    }

    // req: STO-032
    #[test]
    fn actual_source_reservation_refuses_missing_concrete_pool_control_before_construction() {
        let budget = ProjectionSourceWorkingBudgetV1::new();
        let controls = SourceNativeNumericalPoolV1::control_capacity_bound().unwrap();
        assert!(controls > 1);
        let prepared = budget.reserve(controls - 1).unwrap();
        assert!(SourceNativeNumericalPoolV1::from_prepared(prepared).is_err());
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().reservations, 0);
    }

    // req: STO-032
    #[test]
    fn actual_source_reservation_remains_held_when_native_pool_owner_is_forgotten() {
        let budget = ProjectionSourceWorkingBudgetV1::new();
        let controls = SourceNativeNumericalPoolV1::control_capacity_bound().unwrap();
        let maximum = controls.checked_add(4096).unwrap();
        let pool =
            SourceNativeNumericalPoolV1::from_prepared(budget.reserve(maximum).unwrap()).unwrap();
        let forgotten_native_alias = pool.native_pool().clone();
        drop(pool);
        std::mem::forget(forgotten_native_alias);
        assert_eq!(budget.observation().unwrap().reserved_bytes, maximum);
        assert_eq!(budget.observation().unwrap().reservations, 1);
    }

    // req: STO-032
    #[test]
    fn actual_source_reservation_absorbs_same_ledger_increment_and_refuses_foreign_ledger() {
        let budget = ProjectionSourceWorkingBudgetV1::new();
        let foreign = ProjectionSourceWorkingBudgetV1::new();
        let controls = SourceNativeNumericalPoolV1::control_capacity_bound().unwrap();
        let initial = controls.checked_add(4096).unwrap();
        let pool =
            SourceNativeNumericalPoolV1::from_prepared(budget.reserve(initial).unwrap()).unwrap();
        assert_eq!(pool.already_admitted_ceiling().unwrap(), 4096);
        assert!(
            pool.absorb_prepared_increment(foreign.reserve(2048).unwrap())
                .is_err()
        );
        assert_eq!(foreign.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, initial);
        assert_eq!(budget.observation().unwrap().reservations, 1);

        let extra = budget.reserve(2048).unwrap();
        assert_eq!(budget.observation().unwrap().reservations, 2);
        assert_eq!(pool.absorb_prepared_increment(extra).unwrap(), 6144);
        assert_eq!(pool.already_admitted_ceiling().unwrap(), 6144);
        assert_eq!(
            budget.observation().unwrap().reserved_bytes,
            initial.checked_add(2048).unwrap()
        );
        assert_eq!(budget.observation().unwrap().reservations, 1);
        drop(pool);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().reservations, 0);
    }
}
