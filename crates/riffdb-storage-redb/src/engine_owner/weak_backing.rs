//! Private numerical backing for genuine weak ownership relationships.
//! No engine/resolver/back-reference/callable is stored in the controls.
use super::*;
use riffdb_types::{
    OwnedCapacitySharedV1, OwnedCapacityWeakV1, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

// The pinned std Arc layout has two reference-count words before aligned T.
// repr(C) includes both header-to-T padding and final allocation padding.
// This prices only the residual Arc block, not any live T-owned heap graph.
#[repr(C)]
struct WeakBackingLayoutV1<T> {
    strong: usize,
    weak: usize,
    value: T,
}

pub(crate) struct MaintenanceWeakBackingControlV1 {
    _charge: LocatorCharge,
}

fn backing_bytes<T, C>() -> Result<u64, StorageError> {
    let observed = owned_allocation_capacity_bound_v1(
        size_of::<WeakBackingLayoutV1<T>>(),
        align_of::<WeakBackingLayoutV1<T>>(),
    )
    .map_err(|_| unavailable())?;
    let numeric = owned_allocation_capacity_bound_v1(
        size_of::<WeakBackingLayoutV1<C>>(),
        align_of::<WeakBackingLayoutV1<C>>(),
    )
    .map_err(|_| unavailable())?;
    // Actual factory controls: its weak result, numerical payload and closed
    // control handle. The caller separately admits its exact local/TLS Option
    // slots; their discriminants are not inferred from a generic weak count.
    let inline = size_of::<OwnedCapacityWeakV1<T, C>>()
        .checked_add(size_of::<C>())
        .and_then(|bytes| bytes.checked_add(size_of::<OwnedCapacitySharedV1<C>>()))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(unavailable)?;
    observed
        .checked_add(numeric)
        .and_then(|bytes| bytes.checked_add(inline))
        .ok_or_else(unavailable)
}

impl<T> EngineOwner<T> {
    pub(crate) fn admit_maintenance_weak_backing<V>(
        &self,
        observed: &OwnedCapacitySharedV1<V>,
    ) -> Result<OwnedCapacityWeakV1<V, MaintenanceWeakBackingControlV1>, StorageError> {
        let bytes = usize::try_from(backing_bytes::<V, MaintenanceWeakBackingControlV1>()?)
            .map_err(|_| unavailable())?;
        // Same existing actual-engine Maintenance counter. No fresh pool,
        // Source ledger, owner slot or family is constructed here.
        let charge = self.admit_maintenance_decode(bytes)?;
        let control =
            OwnedCapacitySharedV1::new(MaintenanceWeakBackingControlV1 { _charge: charge });
        Ok(observed.downgrade_with_control(control))
    }
}
