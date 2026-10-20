//! Trusted, bounded inspection of non-durable allocation ownership.
//!
//! Callers reserve `OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1` before inspection.
//! Shared allocation addresses are process-local bookkeeping, never durable keys.

use std::any::Any;
use std::fmt;
use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::exact_provider_budget_v1::Reservation;

/// Proof that the full existing census allowance is still held by this caller.
#[doc(hidden)]
pub struct OwnedCapacityInspectionScopeV1<'scope>(pub(crate) &'scope mut Reservation);
impl OwnedCapacityInspectionScopeV1<'_> {
    /// Sequentially lend the same allowance; another preview cannot overlap.
    pub fn reborrow(&mut self) -> OwnedCapacityInspectionScopeV1<'_> {
        OwnedCapacityInspectionScopeV1(&mut *self.0)
    }
}
// External numerical scratch cannot supply lifecycle carriers or settlement
// authority. Only Types' fixed scratch owner implements this private trait.
trait PreallocatedInspectionScratchV1: Send + Sync {
    fn take_observations(
        &mut self,
    ) -> Result<Vec<SharedAllocationObservationV1>, OwnedCapacityInspectionErrorV1>;
}

/// Fixed numerical preview scratch owned by a caller's existing physical pool.
/// No budget factory, semantic owner, callback or ownership attachment exists.
/// This generic control is a numerical lifetime retainer, NOT an authenticated
/// Types Reservation or Source admission proof. The closed caller must preadmit
/// construction_upper_bound before try_new on its existing physical family.
/// No preview from this owner can supply an Owned report or lifecycle carrier.
/// Control retires after buffers and is borrowed through report destruction.
#[doc(hidden)]
pub struct OwnedCapacityInspectionScratchV1<C> {
    observations: Option<Vec<SharedAllocationObservationV1>>,
    control: C,
}
impl<C: Send + Sync> OwnedCapacityInspectionScratchV1<C> {
    pub fn construction_upper_bound() -> Result<u64, OwnedCapacityInspectionErrorV1> {
        let bytes = MAX_SHARED_ALLOCATIONS
            .checked_mul(size_of::<SharedAllocationObservationV1>())
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        allocation_upper_bound(bytes, align_of::<SharedAllocationObservationV1>())?
            .checked_add(
                u64::try_from(size_of::<Self>()).map_err(|_| OwnedCapacityInspectionErrorV1)?,
            )
            .ok_or(OwnedCapacityInspectionErrorV1)
    }
    pub fn try_new(control: C) -> Result<Self, OwnedCapacityInspectionErrorV1> {
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(MAX_SHARED_ALLOCATIONS)
            .map_err(|_| OwnedCapacityInspectionErrorV1)?;
        Ok(Self {
            observations: Some(observations),
            control,
        })
    }
    pub fn preview<'scope, T: InspectOwnedCapacityV1 + ?Sized>(
        &'scope mut self,
        value: &T,
    ) -> Result<OwnedCapacityPreviewV1<'scope>, OwnedCapacityInspectionErrorV1> {
        OwnedCapacityInspectionV1::run(InspectionMode::Preallocated(self), |inspection| {
            inspection.visit(value)
        })
        .map(OwnedCapacityPreviewV1)
    }
    pub fn into_control(self) -> C {
        let Self {
            observations,
            control,
        } = self;
        drop(observations);
        control
    }
}
impl<C: Send + Sync> PreallocatedInspectionScratchV1 for OwnedCapacityInspectionScratchV1<C> {
    fn take_observations(
        &mut self,
    ) -> Result<Vec<SharedAllocationObservationV1>, OwnedCapacityInspectionErrorV1> {
        self.observations
            .take()
            .ok_or(OwnedCapacityInspectionErrorV1)
    }
}

enum InspectionMode<'scope> {
    Preview(OwnedCapacityInspectionScopeV1<'scope>),
    Owned(Reservation),
    Preallocated(&'scope mut dyn PreallocatedInspectionScratchV1),
}

/// Numerical report, lifetime-bound to actual preadmitted census scratch.
#[doc(hidden)]
pub struct OwnedCapacityPreviewV1<'scope>(OwnedCapacityReportV1<'scope>);
impl OwnedCapacityPreviewV1<'_> {
    pub fn unshared_bytes(&self) -> u64 {
        self.0.unshared_bytes()
    }
    pub fn shared_allocations(&self) -> &[SharedAllocationObservationV1] {
        self.0.shared_allocations()
    }
}
impl fmt::Debug for OwnedCapacityPreviewV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityPreviewV1([REDACTED])")
    }
}
impl Drop for OwnedCapacityPreviewV1<'_> {
    fn drop(&mut self) {}
}
impl Drop for OwnedCapacityReportV1<'_> {
    fn drop(&mut self) {}
}

// This trait is private and implemented only by the concrete carrier below.
// Erasure cannot be supplied by callers and never exposes a raw semantic Arc.
trait Carrier: Send + Sync {
    fn lifecycle(&self) -> Option<&crate::OwnedCapacityLifecycleV1>;
    fn retire(self: Box<Self>);
}
struct CarrierPayload<T> {
    handle: crate::OwnedCapacitySharedV1<T>,
    _work: Reservation,
}
struct NativeCarrier<T>(Option<CarrierPayload<T>>);
impl<T: InspectOwnedCapacityV1> Carrier for NativeCarrier<T> {
    fn lifecycle(&self) -> Option<&crate::OwnedCapacityLifecycleV1> {
        self.0.as_ref()?.handle.as_ref().allocation_lifecycle()
    }
    fn retire(mut self: Box<Self>) {
        let payload = self.0.take();
        drop(self);
        // Carrier storage is physically gone; handle then reservation retire.
        drop(payload);
    }
}
struct ErasedCarrier(Option<Box<dyn Carrier>>);
impl ErasedCarrier {
    fn lifecycle(&self) -> Option<&crate::OwnedCapacityLifecycleV1> {
        self.0.as_ref()?.lifecycle()
    }
}
impl Drop for ErasedCarrier {
    fn drop(&mut self) {
        if let Some(carrier) = self.0.take() {
            carrier.retire();
        }
    }
}

pub(crate) const MAX_SHARED_ALLOCATIONS: usize = 4096;
/// Per-shared-allocation attachment bookkeeping in the numerical census.
pub(crate) const SHARED_ALLOCATION_BOOKKEEPING_BYTES: u64 = 1024;
const MAX_DEPTH: u16 = 256;
const MAX_VISITS: u64 = 4_194_304;
/// Closed scratch ceiling for trusted allocation inspection, including proxy owners.
#[doc(hidden)]
pub const OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1: u64 = 4 * 1024 * 1024;

/// A numerical ownership observation; not an application budget selector.
#[doc(hidden)]
pub trait InspectOwnedCapacityV1: Any + Send + Sync {
    /// False only for types that cannot retain a heap allocation.
    const HAS_NESTED_ALLOCATIONS: bool = true;
    /// Present only when final data destruction owns the attached byte charge.
    fn allocation_lifecycle(&self) -> Option<&crate::OwnedCapacityLifecycleV1> {
        None
    }

    /// Adds this value's owned heap capacity, excluding its inline representation.
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1>;
}

/// Opaque identity for one shared allocation during this process lifetime.
#[doc(hidden)]
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct SharedAllocationIdentityV1(usize);

impl fmt::Debug for SharedAllocationIdentityV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SharedAllocationIdentityV1([REDACTED])")
    }
}

/// One original allocation and its conservative capacity charge.
#[doc(hidden)]
pub struct SharedAllocationObservationV1 {
    identity: SharedAllocationIdentityV1,
    bytes: u64,
    carrier: Option<ErasedCarrier>,
}

impl SharedAllocationObservationV1 {
    /// Process-local identity, never serialized or reported to applications.
    pub const fn identity(&self) -> SharedAllocationIdentityV1 {
        self.identity
    }
    /// Includes capacity, alignment, allocation and ownership overhead.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
    /// Borrow only the actual closed semantic owner retained by this report.
    /// Numeric previews have no attachment authority.
    pub fn lifecycle(&self) -> Option<crate::OwnedCapacityLifecycleReferenceV1<'_>> {
        self.carrier
            .as_ref()?
            .lifecycle()
            .map(|lifecycle| lifecycle.reference())
    }
}

impl fmt::Debug for SharedAllocationObservationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SharedAllocationObservationV1([REDACTED])")
    }
}

/// Refuses overflow, excessive recursion, work, or a partial ownership walk.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedCapacityInspectionErrorV1;

impl fmt::Display for OwnedCapacityInspectionErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("owned capacity inspection unavailable")
    }
}
impl std::error::Error for OwnedCapacityInspectionErrorV1 {}

/// Complete sealed observation; failed or partial walks cannot produce this value.
#[doc(hidden)]
pub struct OwnedCapacityReportV1<'scope> {
    unshared_bytes: u64,
    shared: Vec<SharedAllocationObservationV1>,
    _metadata_work: Option<Reservation>,
    _scope: InspectionMode<'scope>,
}

impl fmt::Debug for OwnedCapacityReportV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityReportV1([REDACTED])")
    }
}

impl OwnedCapacityReportV1<'_> {
    /// Nonshared heap capacity; inline bytes belong to the containing owner.
    pub const fn unshared_bytes(&self) -> u64 {
        self.unshared_bytes
    }
    /// Distinct shared allocations, in bounded discovery order.
    pub fn shared_allocations(&self) -> &[SharedAllocationObservationV1] {
        &self.shared
    }
}

/// Bounded trusted visitor; no incomplete state can become a report.
#[doc(hidden)]
pub struct OwnedCapacityInspectionV1<'scope> {
    unshared_bytes: u64,
    shared: Vec<SharedAllocationObservationV1>,
    current: Option<usize>,
    depth: u16,
    visits: u64,
    refused: bool,
    metadata_work: Option<Reservation>,
    mode: InspectionMode<'scope>,
}

impl fmt::Debug for OwnedCapacityInspectionV1<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityInspectionV1([REDACTED])")
    }
}

impl<'scope> OwnedCapacityInspectionV1<'scope> {
    /// Copy-only fields cannot own a destructible allocation. A field changing
    /// to an owning representation fails compilation at its exhaustive inventory.
    pub fn inline<T: Copy>(&mut self, _: &T) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if self.refused {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        Ok(())
    }
    /// Numerical sizing under the actual caller's prior census reservation.
    pub fn preview<T: InspectOwnedCapacityV1 + ?Sized>(
        value: &T,
        scope: OwnedCapacityInspectionScopeV1<'scope>,
    ) -> Result<OwnedCapacityPreviewV1<'scope>, OwnedCapacityInspectionErrorV1> {
        Self::preview_with(scope, |inspection| inspection.visit(value))
    }

    pub fn preview_with(
        scope: OwnedCapacityInspectionScopeV1<'scope>,
        inspect: impl FnOnce(&mut Self) -> Result<(), OwnedCapacityInspectionErrorV1>,
    ) -> Result<OwnedCapacityPreviewV1<'scope>, OwnedCapacityInspectionErrorV1> {
        scope.0.validate_inspection_scope()?;
        Self::run(InspectionMode::Preview(scope), inspect).map(OwnedCapacityPreviewV1)
    }

    /// Borrows the actual input through report destruction and consumes census
    /// admission. A report cannot extend underconstruction input lifetime.
    /// Every erased carrier owns a split from
    /// this same reservation and retires its Box before its closed handle.
    pub fn inspect_owned<T: InspectOwnedCapacityV1 + ?Sized>(
        value: &'scope T,
        work: Reservation,
    ) -> Result<OwnedCapacityReportV1<'scope>, OwnedCapacityInspectionErrorV1> {
        Self::inspect_owned_with(value, work, |value, inspection| inspection.visit(value))
    }

    pub fn inspect_owned_with<R: ?Sized>(
        root: &'scope R,
        work: Reservation,
        inspect: impl FnOnce(&'scope R, &mut Self) -> Result<(), OwnedCapacityInspectionErrorV1>
        + 'scope,
    ) -> Result<OwnedCapacityReportV1<'scope>, OwnedCapacityInspectionErrorV1> {
        work.validate_inspection_scope()?;
        Self::run(InspectionMode::Owned(work), |inspection| {
            inspect(root, inspection)
        })
    }

    fn run(
        mut mode: InspectionMode<'scope>,
        inspect: impl FnOnce(&mut Self) -> Result<(), OwnedCapacityInspectionErrorV1>,
    ) -> Result<OwnedCapacityReportV1<'scope>, OwnedCapacityInspectionErrorV1> {
        if let InspectionMode::Preview(scope) = &mode {
            scope.0.validate_inspection_scope()?;
        }
        let metadata_work = if let InspectionMode::Owned(work) = &mut mode {
            let bytes = MAX_SHARED_ALLOCATIONS
                .checked_mul(size_of::<SharedAllocationObservationV1>())
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            let bound = allocation_upper_bound(bytes, align_of::<SharedAllocationObservationV1>())?;
            Some(
                work.split(bound)
                    .map_err(|_| OwnedCapacityInspectionErrorV1)?,
            )
        } else {
            None
        };
        let shared = match &mut mode {
            InspectionMode::Preallocated(scratch) => scratch.take_observations()?,
            _ => Vec::with_capacity(MAX_SHARED_ALLOCATIONS),
        };
        let mut inspection = Self {
            unshared_bytes: 0,
            shared,
            current: None,
            depth: 0,
            visits: 0,
            refused: false,
            metadata_work,
            mode,
        };
        inspect(&mut inspection)?;
        if inspection.refused {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        Ok(OwnedCapacityReportV1 {
            unshared_bytes: inspection.unshared_bytes,
            shared: inspection.shared,
            _metadata_work: inspection.metadata_work,
            _scope: inspection.mode,
        })
    }

    /// Visits one owned field with bounded recursion and total work.
    pub fn visit<T: InspectOwnedCapacityV1 + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if self.refused || self.depth >= MAX_DEPTH || self.visits >= MAX_VISITS {
            self.refused = true;
            return Err(OwnedCapacityInspectionErrorV1);
        }
        self.depth += 1;
        self.visits += 1;
        let result = value.inspect_owned_capacity(self);
        self.depth -= 1;
        if result.is_err() {
            self.refused = true;
        }
        result
    }

    /// Inspect a prospective B-tree using the same conservative node layout as
    /// the retained-tree inventory, before its entries are allocated.
    pub fn prospective_btree<K, V>(
        &mut self,
        length: usize,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        crate::owned_capacity_collections::tree_storage::<K, V>(length, self)
    }

    /// Charges a requested allocation with conservative alignment/allocator slack.
    pub fn allocation(
        &mut self,
        bytes: usize,
        alignment: usize,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if self.refused {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        if bytes == 0 {
            return Ok(());
        }
        let bound = match allocation_upper_bound(bytes, alignment) {
            Ok(bound) => bound,
            Err(error) => {
                self.refused = true;
                return Err(error);
            }
        };
        self.add(bound)
    }

    fn add(&mut self, bytes: u64) -> Result<(), OwnedCapacityInspectionErrorV1> {
        let target = match self.current {
            Some(index) => {
                &mut self
                    .shared
                    .get_mut(index)
                    .ok_or(OwnedCapacityInspectionErrorV1)?
                    .bytes
            }
            None => &mut self.unshared_bytes,
        };
        let Some(next) = target.checked_add(bytes) else {
            self.refused = true;
            return Err(OwnedCapacityInspectionErrorV1);
        };
        *target = next;
        Ok(())
    }

    pub(crate) fn shared<T: InspectOwnedCapacityV1>(
        &mut self,
        value: &Arc<T>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        self.refuse_raw_owned()?;
        let identity = SharedAllocationIdentityV1(Arc::as_ptr(value).cast::<()>() as usize);
        if self.shared_seen(identity)? {
            return Ok(());
        }
        self.shared_inner(
            identity,
            size_of::<T>(),
            align_of::<T>(),
            None,
            |inspection| inspection.visit(value.as_ref()),
        )
    }

    pub(crate) fn shared_closed<T: InspectOwnedCapacityV1>(
        &mut self,
        value: &crate::OwnedCapacitySharedV1<T>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        let identity = SharedAllocationIdentityV1(value.allocation_address());
        if self.shared_seen(identity)? {
            return Ok(());
        }
        let carrier = if let InspectionMode::Owned(work) = &mut self.mode {
            if value.as_ref().allocation_lifecycle().is_none() {
                return Err(OwnedCapacityInspectionErrorV1);
            }
            let bound = allocation_upper_bound(
                size_of::<NativeCarrier<T>>(),
                align_of::<NativeCarrier<T>>(),
            )?;
            let work = work
                .split(bound)
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            let payload = CarrierPayload {
                handle: value.clone(),
                _work: work,
            };
            let carrier = NativeCarrier(Some(payload));
            Some(ErasedCarrier(Some(Box::new(carrier))))
        } else {
            None
        };
        self.shared_inner(
            identity,
            size_of::<T>(),
            align_of::<T>(),
            carrier,
            |inspection| inspection.visit(value.as_ref()),
        )
    }

    pub(crate) fn shared_slice<T: InspectOwnedCapacityV1>(
        &mut self,
        value: &Arc<[T]>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        self.shared_slice_with_lifecycle(value, None)
    }
    pub(crate) fn shared_slice_with_lifecycle<T: InspectOwnedCapacityV1>(
        &mut self,
        value: &Arc<[T]>,
        _lifecycle: Option<crate::OwnedCapacityLifecycleReferenceV1<'_>>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        self.refuse_raw_owned()?;
        let identity = SharedAllocationIdentityV1(Arc::as_ptr(value).cast::<()>() as usize);
        if self.shared_seen(identity)? {
            return Ok(());
        }
        self.shared_inner(
            identity,
            std::mem::size_of_val(value.as_ref()),
            align_of::<T>(),
            None,
            |inspection| inspection.visit(value.as_ref()),
        )
    }
    pub(crate) fn shared_str(
        &mut self,
        value: &Arc<str>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        self.shared_str_with_lifecycle(value, None)
    }
    pub(crate) fn shared_str_with_lifecycle(
        &mut self,
        value: &Arc<str>,
        _lifecycle: Option<crate::OwnedCapacityLifecycleReferenceV1<'_>>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        self.refuse_raw_owned()?;
        let identity = SharedAllocationIdentityV1(Arc::as_ptr(value).cast::<()>() as usize);
        if self.shared_seen(identity)? {
            return Ok(());
        }
        self.shared_inner(identity, value.len(), align_of::<usize>(), None, |_| Ok(()))
    }
    fn refuse_raw_owned(&mut self) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if matches!(&self.mode, InspectionMode::Owned(_)) {
            self.refused = true;
            return Err(OwnedCapacityInspectionErrorV1);
        }
        Ok(())
    }

    fn shared_seen(
        &mut self,
        identity: SharedAllocationIdentityV1,
    ) -> Result<bool, OwnedCapacityInspectionErrorV1> {
        if self.refused {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        if self.shared.iter().any(|entry| entry.identity == identity) {
            return Ok(true);
        }
        if self.shared.len() >= MAX_SHARED_ALLOCATIONS {
            self.refused = true;
            return Err(OwnedCapacityInspectionErrorV1);
        }
        Ok(false)
    }

    fn shared_inner(
        &mut self,
        identity: SharedAllocationIdentityV1,
        inline: usize,
        alignment: usize,
        carrier: Option<ErasedCarrier>,
        visit: impl FnOnce(&mut Self) -> Result<(), OwnedCapacityInspectionErrorV1>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if self.refused || self.shared.len() >= MAX_SHARED_ALLOCATIONS {
            self.refused = true;
            return Err(OwnedCapacityInspectionErrorV1);
        }
        let parent = self.current;
        let index = self.shared.len();
        self.shared.push(SharedAllocationObservationV1 {
            identity,
            bytes: SHARED_ALLOCATION_BOOKKEEPING_BYTES,
            carrier,
        });
        self.current = Some(index);
        let result = inline
            .checked_add(2 * size_of::<usize>())
            .ok_or(OwnedCapacityInspectionErrorV1)
            .and_then(|bytes| self.allocation(bytes, alignment))
            .and_then(|()| visit(self));
        self.current = parent;
        if result.is_err() {
            self.refused = true;
        }
        result
    }
}

/// Exhaustive field inventory: adding a field requires updating its ownership proof.
#[doc(hidden)]
#[macro_export]
macro_rules! inspect_owned_capacity_struct_v1 {
    ($ty:ty { owned: [$($owned:ident),* $(,)?], inline: [$($inline:ident),* $(,)?], lifecycle: $lifecycle:ident }) => {
        impl $crate::InspectOwnedCapacityV1 for $ty {
            fn allocation_lifecycle(&self) -> Option<&$crate::OwnedCapacityLifecycleV1> { Some(&self.$lifecycle) }
            fn inspect_owned_capacity(&self, inspection: &mut $crate::OwnedCapacityInspectionV1) -> Result<(), $crate::OwnedCapacityInspectionErrorV1> {
                let Self { $($owned,)* $($inline,)* $lifecycle } = self;
                $(inspection.visit($owned)?;)*
                $(inspection.inline($inline)?;)*
                inspection.visit($lifecycle)
            }
        }
    };
    ($ty:ty { owned: [$($owned:ident),* $(,)?], inline: [$($inline:ident),* $(,)?] }) => {
        impl $crate::InspectOwnedCapacityV1 for $ty {
            fn inspect_owned_capacity(&self, inspection: &mut $crate::OwnedCapacityInspectionV1) -> Result<(), $crate::OwnedCapacityInspectionErrorV1> {
                let Self { $($owned,)* $($inline,)* } = self;
                $(inspection.visit($owned)?;)*
                $(inspection.inline($inline)?;)*
                Ok(())
            }
        }
    };
}

/// Exhaustive tuple-newtype inventory with a checked inner representation.
#[doc(hidden)]
#[macro_export]
macro_rules! inspect_owned_capacity_tuple_v1 {
    ($ty:ty, $inner:ty) => {
        impl $crate::InspectOwnedCapacityV1 for $ty {
            const HAS_NESTED_ALLOCATIONS: bool =
                <$inner as $crate::InspectOwnedCapacityV1>::HAS_NESTED_ALLOCATIONS;
            fn inspect_owned_capacity(
                &self,
                inspection: &mut $crate::OwnedCapacityInspectionV1,
            ) -> Result<(), $crate::OwnedCapacityInspectionErrorV1> {
                let Self(value) = self;
                let value: &$inner = value;
                inspection.visit(value)
            }
        }
    };
}

/// Copy-only semantic values have no destructible heap ownership.
#[doc(hidden)]
#[macro_export]
macro_rules! inspect_owned_capacity_copy_v1 {
    ($ty:ty) => {
        impl $crate::InspectOwnedCapacityV1 for $ty {
            const HAS_NESTED_ALLOCATIONS: bool = false;
            fn inspect_owned_capacity(
                &self,
                inspection: &mut $crate::OwnedCapacityInspectionV1,
            ) -> Result<(), $crate::OwnedCapacityInspectionErrorV1> {
                inspection.inline(self)
            }
        }
    };
}

/// Allocation-free allowance for one requested allocation, using the exact same
/// alignment and allocator accounting as the trusted ownership inspector.
/// Invalid alignment and arithmetic overflow refuse; this grants no budget.
#[doc(hidden)]
pub fn owned_allocation_capacity_bound_v1(
    bytes: usize,
    alignment: usize,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    if !alignment.is_power_of_two() {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    allocation_upper_bound(bytes, alignment)
}

/// One admitted allocator allowance shared by decoded sizing and owned inspection.
pub(crate) fn allocation_upper_bound(
    bytes: usize,
    alignment: usize,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    if bytes == 0 {
        return Ok(0);
    }
    u64::try_from(bytes)
        .ok()
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| {
            u64::try_from(alignment)
                .ok()
                .and_then(|alignment| alignment.checked_mul(2))
                .and_then(|alignment| bytes.checked_add(alignment))
        })
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or(OwnedCapacityInspectionErrorV1)
}

#[cfg(test)]
#[path = "owned_capacity_retirement_tests.rs"]
mod retirement_tests;
