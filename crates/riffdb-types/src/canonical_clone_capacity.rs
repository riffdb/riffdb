//! Allocation-free capacity admission for cloning an existing canonical graph.
//!
//! This measures additional clone work; the borrowed input needs its own owner.
use crate::{
    CanonicalRecord, CanonicalValue, MAX_NESTING_DEPTH, OwnedCapacityInspectionErrorV1,
    owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

// Match the existing bounded ownership census. A valid depth alone does not
// bound total work when independently constructed collections form a wide tree.
const MAX_CLONE_CAPACITY_VISITS: u64 = 4_194_304;

/// Additional heap allowance for the existing semantic Clone implementation.
/// No input, output, scratch buffer or collection is allocated by this walk.
/// The caller must reserve this allowance before cloning and retain it until
/// the clone physically drops or transfers to a separately admitted owner.
#[doc(hidden)]
pub fn canonical_value_clone_work_bound_v1(
    value: &CanonicalValue,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let mut walk = CloneCapacityWalk {
        bytes: 0,
        visits: 0,
    };
    walk.value(value, 0)?;
    Ok(walk.bytes)
}

/// Record-shaped counterpart with the same admission and ownership contract.
#[doc(hidden)]
pub fn canonical_record_clone_work_bound_v1(
    record: &CanonicalRecord,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let mut walk = CloneCapacityWalk {
        bytes: 0,
        visits: 0,
    };
    walk.record(record, 0)?;
    Ok(walk.bytes)
}

struct CloneCapacityWalk {
    bytes: u64,
    visits: u64,
}

impl CloneCapacityWalk {
    fn visit(&mut self, depth: usize) -> Result<(), OwnedCapacityInspectionErrorV1> {
        if depth > MAX_NESTING_DEPTH || self.visits >= MAX_CLONE_CAPACITY_VISITS {
            return Err(OwnedCapacityInspectionErrorV1);
        }
        self.visits += 1;
        Ok(())
    }

    fn allocation<T>(&mut self, count: usize) -> Result<(), OwnedCapacityInspectionErrorV1> {
        let requested = count
            .checked_mul(size_of::<T>())
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        let bytes = owned_allocation_capacity_bound_v1(requested, align_of::<T>())?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        Ok(())
    }

    fn record(
        &mut self,
        record: &CanonicalRecord,
        depth: usize,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        self.visit(depth)?;
        self.allocation::<(crate::FieldId, CanonicalValue)>(record.len())?;
        for (_, value) in record.fields() {
            self.value(value, depth + 1)?;
        }
        Ok(())
    }

    fn value(
        &mut self,
        value: &CanonicalValue,
        depth: usize,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        self.visit(depth)?;
        match value {
            CanonicalValue::String(value) => self.allocation::<u8>(value.as_str().len()),
            CanonicalValue::Bytes(value) => self.allocation::<u8>(value.len()),
            CanonicalValue::List(value) => {
                self.allocation::<CanonicalValue>(value.len())?;
                for item in value.values() {
                    self.value(item, depth + 1)?;
                }
                Ok(())
            }
            CanonicalValue::Record(value) => self.record(value, depth),
            CanonicalValue::Vector(value) => self.allocation::<f32>(value.components().len()),
            CanonicalValue::Null
            | CanonicalValue::Bool(_)
            | CanonicalValue::I64(_)
            | CanonicalValue::U64(_)
            | CanonicalValue::Decimal(_)
            | CanonicalValue::Money(_)
            | CanonicalValue::Timestamp(_)
            | CanonicalValue::Date(_)
            | CanonicalValue::Uuid(_)
            | CanonicalValue::Enum { .. } => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "canonical_clone_capacity_tests.rs"]
mod tests;
