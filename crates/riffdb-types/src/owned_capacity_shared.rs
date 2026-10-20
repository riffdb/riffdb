//! Closed strong ownership for shared semantic allocations.
//!
//! No raw Arc or Weak can escape. Without a closed weak handle, final Arc
//! storage retires before its semantic value and lifecycle charge. A closed
//! weak handle separately retains prior allowance for the complete residual
//! allocation until the final weak handle is destroyed.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::{Arc, Weak};

/// First-party shared ownership; this handle grants no database authority.
#[doc(hidden)]
pub struct OwnedCapacitySharedV1<T>(Option<Arc<T>>);

impl<T> OwnedCapacitySharedV1<T> {
    pub(crate) fn from_allocation_handle(allocation: Option<Arc<T>>) -> Self {
        Self(allocation)
    }

    /// Construction must occur under the containing owner's prior admission.
    pub fn new(value: T) -> Self {
        Self(Some(Arc::new(value)))
    }

    /// Tests process-local allocation identity without exposing a raw pointer.
    pub fn same_allocation(&self, other: &Self) -> bool {
        Arc::ptr_eq(self.inner(), other.inner())
    }

    /// Creates a weak relationship without exposing either native handle.
    /// The caller must first admit the complete residual allocation and the
    /// control allocation on the actual containing owner's physical family.
    /// This operation and subsequent clones allocate no memory.
    pub fn downgrade_with_control<C>(
        &self,
        control: OwnedCapacitySharedV1<C>,
    ) -> OwnedCapacityWeakV1<T, C> {
        OwnedCapacityWeakV1 {
            weak: Some(Arc::downgrade(self.inner())),
            control,
        }
    }

    /// Erases only the borrowed downcast type; retains this same physical Arc.
    pub fn into_erased(mut self) -> OwnedCapacityErasedV1
    where
        T: std::any::Any + Send + Sync,
    {
        OwnedCapacityErasedV1(self.0.take().map(|owner| ErasedOwner::Shared(owner)))
    }

    /// Exact old strong-count observation only. This cannot mint a transfer proof;
    /// mutation/extraction still validates ownership with the checked operations.
    pub fn is_uniquely_owned(&self) -> bool {
        Arc::strong_count(self.inner()) == 1
    }

    /// Mutable access exists only while this is the actual sole strong owner.
    pub fn get_mut(&mut self) -> Option<&mut T> {
        Arc::get_mut(self.0.as_mut()?)
    }

    /// Atomically releases this owner and extracts T only for the final owner.
    /// Used by iterative immutable ancestry retirement; no failed-owner race.
    pub fn into_value_on_final_owner(mut self) -> Option<T> {
        self.0.take().and_then(Arc::into_inner)
    }

    /// Consumes the actual sole strong owner. Outstanding closed weak handles
    /// can retain the separately admitted residual allocation after extraction.
    /// An alias refusal returns the same closed allocation without modifying T.
    pub fn try_into_value(mut self) -> Result<T, Self> {
        let Some(allocation) = self.0.take() else {
            return Err(self);
        };
        match Arc::try_unwrap(allocation) {
            Ok(value) => Ok(value),
            Err(allocation) => {
                self.0 = Some(allocation);
                Err(self)
            }
        }
    }

    pub(crate) fn allocation_address(&self) -> usize {
        Arc::as_ptr(self.inner()).cast::<()>() as usize
    }

    fn inner(&self) -> &Arc<T> {
        self.0.as_ref().expect("live closed shared owner")
    }
}

impl<T> Clone for OwnedCapacitySharedV1<T> {
    fn clone(&self) -> Self {
        Self(Some(self.inner().clone()))
    }
}

impl<T> Deref for OwnedCapacitySharedV1<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.inner()
    }
}

impl<T> AsRef<T> for OwnedCapacitySharedV1<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T> Drop for OwnedCapacitySharedV1<T> {
    fn drop(&mut self) {
        if let Some(allocation) = self.0.take() {
            // into_inner releases this strong owner before returning T. Any
            // closed weak backing remains charged by its own control owner.
            drop(Arc::into_inner(allocation));
        }
    }
}

/// Closed weak ownership of an already admitted allocation. This handle grants
/// no database authority and exposes no raw Arc, Weak, address or mutable value.
/// The numerical control must not contain a back-reference to the observed T.
#[doc(hidden)]
pub struct OwnedCapacityWeakV1<T, C> {
    // Drop the actual native weak handle before the backing allowance.
    weak: Option<Weak<T>>,
    control: OwnedCapacitySharedV1<C>,
}

impl<T, C> OwnedCapacityWeakV1<T, C> {
    /// Tests only process-local allocation identity, including after T dies.
    pub fn same_allocation(&self, other: &OwnedCapacitySharedV1<T>) -> bool {
        self.weak
            .as_ref()
            .is_some_and(|weak| std::ptr::eq(weak.as_ptr(), Arc::as_ptr(other.inner())))
    }

    /// A live T still retains its original admission. This creates no new
    /// allocation, control or authority; it only retains that same strong owner.
    pub fn upgrade(&self) -> Option<OwnedCapacitySharedV1<T>> {
        self.weak
            .as_ref()?
            .upgrade()
            .map(|owner| OwnedCapacitySharedV1::from_allocation_handle(Some(owner)))
    }
}

impl<T, C> Clone for OwnedCapacityWeakV1<T, C> {
    fn clone(&self) -> Self {
        Self {
            weak: self.weak.clone(),
            control: self.control.clone(),
        }
    }
}

impl<T, C> Drop for OwnedCapacityWeakV1<T, C> {
    fn drop(&mut self) {
        // Native weak retirement precedes the closed control's final retirement.
        // The latter uses the existing into_inner protocol, not ordinary Arc Drop.
        drop(self.weak.take());
    }
}

impl<T: fmt::Debug> fmt::Debug for OwnedCapacitySharedV1<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(formatter)
    }
}
impl<T: PartialEq> PartialEq for OwnedCapacitySharedV1<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}
impl<T: Eq> Eq for OwnedCapacitySharedV1<T> {}
impl<T: PartialOrd> PartialOrd for OwnedCapacitySharedV1<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl<T: Ord> Ord for OwnedCapacitySharedV1<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deref().cmp(other.deref())
    }
}
impl<T: Hash> Hash for OwnedCapacitySharedV1<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.deref().hash(state)
    }
}

/// Sized ownership shell for immutable text and its exact destruction control.
#[doc(hidden)]
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OwnedCapacityTextV1(OwnedCapacitySharedV1<TextValue>);
#[derive(Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TextValue {
    text: Box<str>,
    lifecycle: crate::OwnedCapacityLifecycleV1,
}
crate::inspect_owned_capacity_struct_v1!(TextValue {
    owned: [text],
    inline: [],
    lifecycle: lifecycle
});
impl OwnedCapacityTextV1 {
    /// Pure retained-storage quote for this concrete closed text representation.
    /// Includes its Arc shell, boxed UTF-8, lifecycle control and the inspector's
    /// shared attachment bookkeeping. Construction scratch is quoted by callers.
    pub fn retained_allocation_upper_bound(
        text_bytes: usize,
    ) -> Result<u64, crate::OwnedCapacityInspectionErrorV1> {
        use crate::owned_capacity::{SHARED_ALLOCATION_BOOKKEEPING_BYTES, allocation_upper_bound};
        use std::mem::{align_of, size_of};

        let inline = size_of::<TextValue>()
            .checked_add(2 * size_of::<usize>())
            .ok_or(crate::OwnedCapacityInspectionErrorV1)?;
        let shell = allocation_upper_bound(inline, align_of::<TextValue>())?;
        let text = allocation_upper_bound(text_bytes, align_of::<u8>())?;
        let lifecycle = crate::OwnedCapacityLifecycleV1::control_allocation_upper_bound()?;
        SHARED_ALLOCATION_BOOKKEEPING_BYTES
            .checked_add(shell)
            .and_then(|bytes| bytes.checked_add(text))
            .and_then(|bytes| bytes.checked_add(lifecycle))
            .ok_or(crate::OwnedCapacityInspectionErrorV1)
    }

    pub fn new(text: String) -> Self {
        Self(OwnedCapacitySharedV1::new(TextValue {
            text: text.into_boxed_str(),
            lifecycle: Default::default(),
        }))
    }
}
impl Deref for OwnedCapacityTextV1 {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0.text
    }
}
impl AsRef<str> for OwnedCapacityTextV1 {
    fn as_ref(&self) -> &str {
        self
    }
}
impl fmt::Debug for OwnedCapacityTextV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.deref(), formatter)
    }
}
impl fmt::Display for OwnedCapacityTextV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.deref(), formatter)
    }
}
impl crate::InspectOwnedCapacityV1 for OwnedCapacityTextV1 {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut crate::OwnedCapacityInspectionV1,
    ) -> Result<(), crate::OwnedCapacityInspectionErrorV1> {
        inspection.shared_closed(&self.0)
    }
}

/// Sized ownership shell for one immutable slice and its destruction control.
#[doc(hidden)]
pub struct OwnedCapacitySliceV1<T>(OwnedCapacitySharedV1<SliceValue<T>>);
struct SliceValue<T> {
    values: Box<[T]>,
    lifecycle: crate::OwnedCapacityLifecycleV1,
}
impl<T: crate::InspectOwnedCapacityV1> crate::InspectOwnedCapacityV1 for SliceValue<T> {
    fn allocation_lifecycle(&self) -> Option<&crate::OwnedCapacityLifecycleV1> {
        Some(&self.lifecycle)
    }
    fn inspect_owned_capacity(
        &self,
        inspection: &mut crate::OwnedCapacityInspectionV1,
    ) -> Result<(), crate::OwnedCapacityInspectionErrorV1> {
        let Self { values, lifecycle } = self;
        inspection.visit(values)?;
        inspection.visit(lifecycle)
    }
}
impl<T> OwnedCapacitySliceV1<T> {
    pub fn same_allocation(&self, other: &Self) -> bool {
        self.0.same_allocation(&other.0)
    }
    pub fn new(values: Vec<T>) -> Self {
        Self(OwnedCapacitySharedV1::new(SliceValue {
            values: values.into_boxed_slice(),
            lifecycle: Default::default(),
        }))
    }
}
impl<T> Clone for OwnedCapacitySliceV1<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> Deref for OwnedCapacitySliceV1<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.0.values
    }
}
impl<T> AsRef<[T]> for OwnedCapacitySliceV1<T> {
    fn as_ref(&self) -> &[T] {
        self
    }
}
impl<T: fmt::Debug> fmt::Debug for OwnedCapacitySliceV1<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(formatter)
    }
}
impl<T: PartialEq> PartialEq for OwnedCapacitySliceV1<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}
impl<T: Eq> Eq for OwnedCapacitySliceV1<T> {}
impl<T: PartialOrd> PartialOrd for OwnedCapacitySliceV1<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl<T: Ord> Ord for OwnedCapacitySliceV1<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deref().cmp(other.deref())
    }
}
impl<T: Hash> Hash for OwnedCapacitySliceV1<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.deref().hash(state)
    }
}
impl<T: crate::InspectOwnedCapacityV1> crate::InspectOwnedCapacityV1 for OwnedCapacitySliceV1<T> {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut crate::OwnedCapacityInspectionV1,
    ) -> Result<(), crate::OwnedCapacityInspectionErrorV1> {
        inspection.shared_closed(&self.0)
    }
}

impl<T> From<Vec<T>> for OwnedCapacitySliceV1<T> {
    fn from(values: Vec<T>) -> Self {
        Self::new(values)
    }
}
impl<T> From<Box<[T]>> for OwnedCapacitySliceV1<T> {
    fn from(values: Box<[T]>) -> Self {
        Self::new(values.into_vec())
    }
}
impl<T, const N: usize> From<[T; N]> for OwnedCapacitySliceV1<T> {
    fn from(values: [T; N]) -> Self {
        Self::new(Vec::from(values))
    }
}
impl<T: Clone> From<&[T]> for OwnedCapacitySliceV1<T> {
    fn from(values: &[T]) -> Self {
        Self::new(values.to_vec())
    }
}

// Only these private vtables may consume an erased Arc. Each concrete method
// retires its strong handle before its value and charge. Outstanding closed
// weak backing retains its separate control; it grants no erased access.
trait SharedErasure: Send + Sync {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync);
    fn retire(self: Arc<Self>);
}
impl<T: std::any::Any + Send + Sync> SharedErasure for T {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
    fn retire(self: Arc<Self>) {
        drop(Arc::into_inner(self));
    }
}
trait AllocationErasure: Send + Sync {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync);
    fn retire(self: Arc<Self>);
}
impl<T: std::any::Any + Send + Sync> AllocationErasure
    for crate::exact_provider_budget_v1::ProviderAllocation<T>
{
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        &**self
    }
    fn retire(self: Arc<Self>) {
        drop(Arc::into_inner(self));
    }
}
enum ErasedOwner {
    Shared(Arc<dyn SharedErasure>),
    Allocation(Arc<dyn AllocationErasure>),
}
/// A closed erased owner with borrow-only downcasts and concrete retirement.
#[doc(hidden)]
pub struct OwnedCapacityErasedV1(Option<ErasedOwner>);
impl OwnedCapacityErasedV1 {
    pub(crate) fn from_allocation<T: std::any::Any + Send + Sync>(
        allocation: Option<Arc<crate::exact_provider_budget_v1::ProviderAllocation<T>>>,
    ) -> Self {
        Self(allocation.map(|owner| ErasedOwner::Allocation(owner)))
    }
    /// Borrows semantic T; a provider allocation's numerical shell stays private.
    pub fn downcast_ref<T: std::any::Any>(&self) -> Option<&T> {
        match self.0.as_ref()? {
            ErasedOwner::Shared(owner) => owner.as_ref().as_any().downcast_ref(),
            ErasedOwner::Allocation(owner) => owner.as_ref().as_any().downcast_ref(),
        }
    }
}
impl Clone for OwnedCapacityErasedV1 {
    fn clone(&self) -> Self {
        Self(self.0.as_ref().map(|owner| match owner {
            ErasedOwner::Shared(owner) => ErasedOwner::Shared(owner.clone()),
            ErasedOwner::Allocation(owner) => ErasedOwner::Allocation(owner.clone()),
        }))
    }
}
impl Drop for OwnedCapacityErasedV1 {
    fn drop(&mut self) {
        match self.0.take() {
            Some(ErasedOwner::Shared(owner)) => owner.retire(),
            Some(ErasedOwner::Allocation(owner)) => owner.retire(),
            None => {}
        }
    }
}
impl fmt::Debug for OwnedCapacityErasedV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnedCapacityErasedV1([REDACTED])")
    }
}
// External crates can only provide the two concrete closed handles below.
mod erased_seal {
    pub trait Sealed {}
}
/// Closed zero-allocation type erasure for admitted first-party owners.
#[doc(hidden)]
pub trait IntoOwnedCapacityErasedV1: erased_seal::Sealed {
    fn into_owned_capacity_erased(self) -> OwnedCapacityErasedV1;
}
impl<T: std::any::Any + Send + Sync> erased_seal::Sealed for OwnedCapacitySharedV1<T> {}
impl<T: std::any::Any + Send + Sync> IntoOwnedCapacityErasedV1 for OwnedCapacitySharedV1<T> {
    fn into_owned_capacity_erased(self) -> OwnedCapacityErasedV1 {
        self.into_erased()
    }
}
impl<T: std::any::Any + Send + Sync> erased_seal::Sealed
    for crate::exact_provider_budget_v1::ProviderAllocationOwner<T>
{
}
impl<T: std::any::Any + Send + Sync> IntoOwnedCapacityErasedV1
    for crate::exact_provider_budget_v1::ProviderAllocationOwner<T>
{
    fn into_owned_capacity_erased(self) -> OwnedCapacityErasedV1 {
        self.into_erased()
    }
}

#[cfg(test)]
#[path = "owned_capacity_weak_tests.rs"]
mod weak_tests;
