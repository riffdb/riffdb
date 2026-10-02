//! Allocation bounds for containers; initialized children are walked separately.

use crate::{InspectOwnedCapacityV1, OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem::{align_of, size_of};
use std::sync::Arc;

macro_rules! scalar {
    ($($ty:ty),*) => { $(impl InspectOwnedCapacityV1 for $ty {
        const HAS_NESTED_ALLOCATIONS: bool = false;
        fn inspect_owned_capacity(&self, _: &mut OwnedCapacityInspectionV1) -> Result<(), OwnedCapacityInspectionErrorV1> { Ok(()) }
    })* };
}
scalar!(
    (),
    bool,
    char,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    std::num::NonZeroU8,
    std::num::NonZeroU16,
    std::num::NonZeroU32,
    std::num::NonZeroU64,
    std::num::NonZeroUsize
);

impl InspectOwnedCapacityV1 for String {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.allocation(self.capacity(), align_of::<u8>())
    }
}
impl InspectOwnedCapacityV1 for str {
    const HAS_NESTED_ALLOCATIONS: bool = false;
    fn inspect_owned_capacity(
        &self,
        _: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        Ok(())
    }
}
impl<T: InspectOwnedCapacityV1> InspectOwnedCapacityV1 for [T] {
    const HAS_NESTED_ALLOCATIONS: bool = T::HAS_NESTED_ALLOCATIONS;
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if T::HAS_NESTED_ALLOCATIONS {
            for value in self {
                inspection.visit(value)?;
            }
        }
        Ok(())
    }
}
impl<T: InspectOwnedCapacityV1, const N: usize> InspectOwnedCapacityV1 for [T; N] {
    const HAS_NESTED_ALLOCATIONS: bool = T::HAS_NESTED_ALLOCATIONS;
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.visit(self.as_slice())
    }
}
impl<T: InspectOwnedCapacityV1> InspectOwnedCapacityV1 for Vec<T> {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        let bytes = self
            .capacity()
            .checked_mul(size_of::<T>())
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        inspection.allocation(bytes, align_of::<T>())?;
        inspection.visit(self.as_slice())
    }
}
impl<T: InspectOwnedCapacityV1> InspectOwnedCapacityV1 for VecDeque<T> {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        let bytes = self
            .capacity()
            .checked_mul(size_of::<T>())
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        inspection.allocation(bytes, align_of::<T>())?;
        if T::HAS_NESTED_ALLOCATIONS {
            for value in self {
                inspection.visit(value)?;
            }
        }
        Ok(())
    }
}
impl<T: InspectOwnedCapacityV1> InspectOwnedCapacityV1 for Option<T> {
    const HAS_NESTED_ALLOCATIONS: bool = T::HAS_NESTED_ALLOCATIONS;
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if let Some(value) = self {
            inspection.visit(value)?;
        }
        Ok(())
    }
}
impl<T: InspectOwnedCapacityV1> InspectOwnedCapacityV1 for Box<T> {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.allocation(size_of::<T>(), align_of::<T>())?;
        inspection.visit(self.as_ref())
    }
}
impl<T: InspectOwnedCapacityV1> InspectOwnedCapacityV1 for Arc<T> {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.shared(self)
    }
}
impl<T: InspectOwnedCapacityV1> InspectOwnedCapacityV1 for Arc<[T]> {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.shared_slice(self)
    }
}
impl InspectOwnedCapacityV1 for Arc<str> {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.shared_str(self)
    }
}
impl<A: InspectOwnedCapacityV1, B: InspectOwnedCapacityV1> InspectOwnedCapacityV1 for (A, B) {
    const HAS_NESTED_ALLOCATIONS: bool = A::HAS_NESTED_ALLOCATIONS || B::HAS_NESTED_ALLOCATIONS;
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        inspection.visit(&self.0)?;
        inspection.visit(&self.1)
    }
}

pub(crate) fn tree_storage<K, V>(
    length: usize,
    inspection: &mut OwnedCapacityInspectionV1,
) -> Result<(), OwnedCapacityInspectionErrorV1> {
    // Admitted std B-tree layout has eleven key/value slots and twelve edges.
    // Charge a full internal node per entry, plus one possibly empty root.
    // This deliberately avoids using occupancy to tighten ownership admission.
    let node = size_of::<K>()
        .checked_add(size_of::<V>())
        .and_then(|bytes| bytes.checked_mul(11))
        .and_then(|bytes| bytes.checked_add(256))
        .and_then(|bytes| bytes.checked_add(align_of::<K>()))
        .and_then(|bytes| bytes.checked_add(align_of::<V>()))
        .ok_or(OwnedCapacityInspectionErrorV1)?;
    let bytes = length
        .checked_add(1)
        .and_then(|count| count.checked_mul(node))
        .ok_or(OwnedCapacityInspectionErrorV1)?;
    inspection.allocation(
        bytes,
        align_of::<K>()
            .max(align_of::<V>())
            .max(align_of::<usize>()),
    )
}
impl<K: InspectOwnedCapacityV1 + Ord, V: InspectOwnedCapacityV1> InspectOwnedCapacityV1
    for BTreeMap<K, V>
{
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        tree_storage::<K, V>(self.len(), inspection)?;
        if K::HAS_NESTED_ALLOCATIONS || V::HAS_NESTED_ALLOCATIONS {
            for (key, value) in self {
                if K::HAS_NESTED_ALLOCATIONS {
                    inspection.visit(key)?;
                }
                if V::HAS_NESTED_ALLOCATIONS {
                    inspection.visit(value)?;
                }
            }
        }
        Ok(())
    }
}
impl<T: InspectOwnedCapacityV1 + Ord> InspectOwnedCapacityV1 for BTreeSet<T> {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        tree_storage::<T, ()>(self.len(), inspection)?;
        if T::HAS_NESTED_ALLOCATIONS {
            for value in self {
                inspection.visit(value)?;
            }
        }
        Ok(())
    }
}
