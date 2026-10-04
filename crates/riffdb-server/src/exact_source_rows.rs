//! Closed private source conversion workspace, admitted before allocation.
use super::*;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1,
    exact_provider_budget_v1::{ProviderBudget, Reservation},
};
use std::{
    mem::{align_of, size_of},
    ops::Deref,
};

pub(super) struct SourceRowsWork {
    budget: ProviderBudget,
    // Last: callers' guarded source data is destroyed before work release.
    work: Reservation,
}
pub(super) struct GuardedSourceRows<T> {
    rows: T,
    work: SourceRowsWork,
}
impl<T> GuardedSourceRows<T> {
    pub(super) fn into_parts(self) -> (T, SourceRowsWork) {
        (self.rows, self.work)
    }
}
impl<T> Deref for GuardedSourceRows<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.rows
    }
}

fn tree<K, V>(count: usize) -> Result<u64, RebuildFailure> {
    let bytes = size_of::<K>()
        .checked_add(size_of::<V>())
        .and_then(|bytes| bytes.checked_mul(11))
        .and_then(|bytes| bytes.checked_add(256 + align_of::<K>() + align_of::<V>()))
        .and_then(|node| {
            count
                .checked_add(1)
                .and_then(|count| count.checked_mul(node))
        })
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(4096))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(RebuildFailure::Transient)?;
    Ok(bytes)
}
impl SourceRowsWork {
    pub(super) fn new(budget: &ProviderBudget) -> Result<Self, RebuildFailure> {
        let work = budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 32 * 1024)
            .map_err(|_| RebuildFailure::Transient)?;
        Ok(Self {
            budget: budget.clone(),
            work,
        })
    }
    fn grow(&mut self, bytes: u64) -> Result<(), RebuildFailure> {
        let additional = self
            .budget
            .reserve(bytes)
            .map_err(|_| RebuildFailure::Transient)?;
        self.work
            .absorb(additional)
            .map_err(|_| RebuildFailure::Transient)
    }
    pub(super) fn reserve_tree<K, V>(&mut self, count: usize) -> Result<(), RebuildFailure> {
        self.grow(tree::<K, V>(count)?)
    }
    pub(super) fn key_decode(
        &self,
        key_bytes: usize,
        components: usize,
    ) -> Result<Reservation, RebuildFailure> {
        // Decoding builds bounded scalar components and a copied entity key;
        // subsequent temporary key/target clones live inside this work scope.
        let bytes = components
            .checked_mul(size_of::<CanonicalValue>())
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| {
                key_bytes
                    .checked_mul(8)
                    .and_then(|keys| bytes.checked_add(keys))
            })
            .and_then(|bytes| bytes.checked_add(32 * 1024))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(RebuildFailure::Transient)?;
        self.budget
            .reserve(bytes)
            .map_err(|_| RebuildFailure::Transient)
    }
    pub(super) fn reserve_row(
        &mut self,
        fields: &CanonicalRecord,
        profiles: usize,
        outputs: usize,
        key_bytes: usize,
    ) -> Result<(), RebuildFailure> {
        // Census scratch was admitted by new(). Canonical values have no shared
        // children; inspect capacities independently of their encoded size.
        let report =
            OwnedCapacityInspectionV1::inspect(fields).map_err(|_| RebuildFailure::Transient)?;
        if !report.shared_allocations().is_empty() {
            return Err(RebuildFailure::Transient);
        }
        let heap = report.unshared_bytes();
        drop(report);
        // Values dictionary, reference cells, projected output, text/filter
        // copies and mutation conversion overlap within the same bounded pass.
        // Six complete native value forests cover their private deep copies.
        let bytes = heap
            .checked_mul(6)
            .and_then(|bytes| {
                bytes.checked_add(tree::<FieldId, CanonicalValue>(fields.len()).ok()?)
            })
            .and_then(|bytes| {
                bytes.checked_add(tree::<FieldId, ExactReferenceCellV1>(profiles).ok()?)
            })
            .and_then(|bytes| {
                outputs
                    .checked_mul(size_of::<(FieldId, CanonicalValue)>())
                    .and_then(|slots| slots.checked_mul(4))
                    .and_then(|slots| u64::try_from(slots).ok())
                    .and_then(|slots| bytes.checked_add(slots))
            })
            .and_then(|bytes| bytes.checked_add(tree::<EntityKey, ExactTextSourceRow>(2).ok()?))
            .and_then(|bytes| {
                bytes.checked_add(tree::<EntityKey, ExactPredicateProviderRowV1>(2).ok()?)
            })
            .and_then(|bytes| {
                key_bytes
                    .checked_mul(8)
                    .and_then(|keys| u64::try_from(keys).ok())
                    .and_then(|keys| bytes.checked_add(keys))
            })
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(RebuildFailure::Transient)?;
        self.grow(bytes)
    }
    pub(super) fn reserve_keys<'a>(
        &mut self,
        keys: impl Iterator<Item = &'a EntityKey>,
    ) -> Result<(), RebuildFailure> {
        for key in keys {
            self.reserve_key(key.as_bytes().len())?;
        }
        Ok(())
    }
    pub(super) fn reserve_key(&mut self, key_bytes: usize) -> Result<(), RebuildFailure> {
        let bytes =
            tree::<EntityKey, Option<riffdb_storage_api::AdmittedProviderEntityRecordV1>>(2)?
                .checked_add(
                    u64::try_from(size_of::<EntityKey>() * 8)
                        .map_err(|_| RebuildFailure::Transient)?,
                )
                .and_then(|bytes| {
                    u64::try_from(key_bytes)
                        .ok()
                        .and_then(|keys| keys.checked_mul(8))
                        .and_then(|keys| bytes.checked_add(keys))
                })
                .ok_or(RebuildFailure::Transient)?;
        self.grow(bytes)
    }
    pub(super) fn guard<T>(self, rows: T) -> GuardedSourceRows<T> {
        GuardedSourceRows { rows, work: self }
    }
}
