//! Trusted, bounded inspection of non-durable allocation ownership.
//!
//! Callers reserve `OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1` before inspection.
//! Shared allocation addresses are process-local bookkeeping, never durable keys.

use std::any::Any;
use std::fmt;
use std::mem::{align_of, size_of};
use std::sync::Arc;

pub(crate) const MAX_SHARED_ALLOCATIONS: usize = 4096;
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
    keepalive: Arc<dyn Any + Send + Sync>,
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
    /// Transfers the original allocation reference to the budget owner.
    pub fn into_parts(self) -> (SharedAllocationIdentityV1, u64, Arc<dyn Any + Send + Sync>) {
        (self.identity, self.bytes, self.keepalive)
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
pub struct OwnedCapacityReportV1 {
    unshared_bytes: u64,
    shared: Vec<SharedAllocationObservationV1>,
}

impl fmt::Debug for OwnedCapacityReportV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityReportV1([REDACTED])")
    }
}

impl OwnedCapacityReportV1 {
    /// Nonshared heap capacity; inline bytes belong to the containing owner.
    pub const fn unshared_bytes(&self) -> u64 {
        self.unshared_bytes
    }
    /// Distinct shared allocations, in bounded discovery order.
    pub fn shared_allocations(&self) -> &[SharedAllocationObservationV1] {
        &self.shared
    }
    /// Transfers only a completed observation and its original references.
    pub fn into_parts(self) -> (u64, Vec<SharedAllocationObservationV1>) {
        (self.unshared_bytes, self.shared)
    }
}

/// Bounded trusted visitor; no incomplete state can become a report.
#[doc(hidden)]
pub struct OwnedCapacityInspectionV1 {
    unshared_bytes: u64,
    shared: Vec<SharedAllocationObservationV1>,
    current: Option<usize>,
    depth: u16,
    visits: u64,
    refused: bool,
}

impl fmt::Debug for OwnedCapacityInspectionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityInspectionV1([REDACTED])")
    }
}

impl OwnedCapacityInspectionV1 {
    /// Copy-only fields cannot own a destructible allocation. A field changing
    /// to an owning representation fails compilation at its exhaustive inventory.
    pub fn inline<T: Copy>(&mut self, _: &T) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if self.refused {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        Ok(())
    }
    /// Requires prior scratch reservation by the closed ownership caller.
    pub fn inspect<T: InspectOwnedCapacityV1 + ?Sized>(
        value: &T,
    ) -> Result<OwnedCapacityReportV1, OwnedCapacityInspectionErrorV1> {
        let mut inspection = Self {
            unshared_bytes: 0,
            shared: Vec::with_capacity(MAX_SHARED_ALLOCATIONS),
            current: None,
            depth: 0,
            visits: 0,
            refused: false,
        };
        inspection.visit(value)?;
        if inspection.refused {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        Ok(OwnedCapacityReportV1 {
            unshared_bytes: inspection.unshared_bytes,
            shared: inspection.shared,
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
        let bound = u64::try_from(bytes)
            .ok()
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| {
                u64::try_from(alignment)
                    .ok()
                    .and_then(|alignment| alignment.checked_mul(2))
                    .and_then(|alignment| bytes.checked_add(alignment))
            })
            .and_then(|bytes| bytes.checked_add(256));
        let Some(bound) = bound else {
            self.refused = true;
            return Err(OwnedCapacityInspectionErrorV1);
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
        let identity = SharedAllocationIdentityV1(Arc::as_ptr(value).cast::<()>() as usize);
        if self.shared_seen(identity)? {
            return Ok(());
        }
        let proof: Arc<dyn Any + Send + Sync> = value.clone();
        self.shared_inner(
            identity,
            size_of::<T>(),
            align_of::<T>(),
            proof,
            |inspection| inspection.visit(value.as_ref()),
        )
    }

    pub(crate) fn shared_slice<T: InspectOwnedCapacityV1>(
        &mut self,
        value: &Arc<[T]>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        let identity = SharedAllocationIdentityV1(Arc::as_ptr(value).cast::<()>() as usize);
        if self.shared_seen(identity)? {
            return Ok(());
        }
        let proof: Arc<dyn Any + Send + Sync> = Arc::new(value.clone());
        self.shared_inner(
            identity,
            std::mem::size_of_val(value.as_ref()),
            align_of::<T>(),
            proof,
            |inspection| inspection.visit(value.as_ref()),
        )
    }

    pub(crate) fn shared_str(
        &mut self,
        value: &Arc<str>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        let identity = SharedAllocationIdentityV1(Arc::as_ptr(value).cast::<()>() as usize);
        if self.shared_seen(identity)? {
            return Ok(());
        }
        let proof: Arc<dyn Any + Send + Sync> = Arc::new(value.clone());
        self.shared_inner(
            identity,
            value.len(),
            align_of::<usize>(),
            proof,
            |_| Ok(()),
        )
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
        proof: Arc<dyn Any + Send + Sync>,
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
            bytes: 256,
            keepalive: proof,
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
