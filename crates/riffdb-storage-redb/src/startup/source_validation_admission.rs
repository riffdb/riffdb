//! Actual-engine maintenance admission for Source scan bytes and decoded values.
//! No Source worker ledger or per-scan provider budget is constructed here.
use super::*;
use crate::locator_memory::LocatorCharge;
use riffdb_storage_api::proto_codec::{
    ProjectionSourceCallerDecodeWorkV1, ProjectionSourceRecordDecodeErrorV1,
};
use riffdb_types::{
    InspectOwnedCapacityV1, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    OwnedCapacityInspectionScratchV1, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

pub(super) struct RawStructuralRowV1 {
    phase: usize,
    index: u64,
    key: Vec<u8>,
    value: Vec<u8>,
    // Data is physically destroyed before the exact underlying pool charge.
    _source_work: Option<LocatorCharge>,
}
impl RawStructuralRowV1 {
    pub(super) fn existing(phase: usize, index: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            phase,
            index,
            key,
            value,
            _source_work: None,
        }
    }
    pub(super) fn source<O: EvidenceScanReadOwnerV1>(
        owner: &O,
        phase: usize,
        index: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<Self, StorageError> {
        if key.len() > riffdb_types::MAX_KEY_BYTES
            || key
                .len()
                .checked_add(value.len())
                .is_none_or(|bytes| bytes > riffdb_storage_api::MAX_SCAN_PAGE_BYTES)
        {
            return Err(corrupt());
        }
        // Role-specific framing/control limits and canonical-tree sizing are
        // borrowed checks, before even the raw input copies are allocated.
        source_validation::inspect_work_for_phase(phase, value)?;
        let key_bytes = owned_allocation_capacity_bound_v1(key.len(), align_of::<u8>())
            .map_err(|_| limit_exceeded())?;
        let value_bytes = owned_allocation_capacity_bound_v1(value.len(), align_of::<u8>())
            .map_err(|_| limit_exceeded())?;
        let maximum = key_bytes
            .checked_add(value_bytes)
            .ok_or_else(limit_exceeded)?;
        let maximum = usize::try_from(maximum)
            .ok()
            .and_then(|b| b.checked_add(size_of::<Self>()))
            .ok_or_else(limit_exceeded)?;
        let work = owner.admit_source_maintenance_decode(maximum)?;
        let mut key_copy = Vec::new();
        key_copy
            .try_reserve_exact(key.len())
            .map_err(|_| limit_exceeded())?;
        key_copy.extend_from_slice(key);
        let mut value_copy = Vec::new();
        value_copy
            .try_reserve_exact(value.len())
            .map_err(|_| limit_exceeded())?;
        value_copy.extend_from_slice(value);
        Ok(Self {
            phase,
            index,
            key: key_copy,
            value: value_copy,
            _source_work: Some(work),
        })
    }
    // The existing audit parallel path only owns old uncharged raw records.
    // Charged Source values cannot leave their closed data-before-charge owner.
    pub(super) fn into_existing_parts(
        mut self,
    ) -> Result<(usize, u64, Vec<u8>, Vec<u8>), StorageError> {
        if self.phase >= source_validation::FIRST_PHASE || self._source_work.is_some() {
            return Err(invariant());
        }
        Ok((
            self.phase,
            self.index,
            std::mem::take(&mut self.key),
            std::mem::take(&mut self.value),
        ))
    }
    pub(super) fn parts(&self) -> (usize, u64, &[u8], &[u8]) {
        (self.phase, self.index, &self.key, &self.value)
    }
}
impl Drop for RawStructuralRowV1 {
    fn drop(&mut self) {}
}

pub(super) struct MaintenanceDecodedSourceV1<T> {
    value: riffdb_storage_api::EncodedPageItem<T>,
    _scratch: Vec<u8>,
    _work: LocatorCharge,
}
impl<T> MaintenanceDecodedSourceV1<T> {
    pub(super) fn value(&self) -> &T {
        self.value.value()
    }
}
impl<T> Drop for MaintenanceDecodedSourceV1<T> {
    fn drop(&mut self) {}
}

#[allow(clippy::type_complexity)]
pub(super) fn decode<T: InspectOwnedCapacityV1, O: EvidenceScanReadOwnerV1>(
    owner: &O,
    encoded: &[u8],
    inspect: fn(
        &[u8],
    )
        -> Result<ProjectionSourceCallerDecodeWorkV1, ProjectionSourceRecordDecodeErrorV1>,
    reconstruct: fn(
        &[u8],
        &mut Vec<u8>,
    ) -> Result<
        riffdb_storage_api::EncodedPageItem<T>,
        ProjectionSourceRecordDecodeErrorV1,
    >,
) -> Result<MaintenanceDecodedSourceV1<T>, StorageError> {
    let plan = inspect(encoded).map_err(decode_error)?;
    let maximum = usize::try_from(plan.maximum_work_bytes()).map_err(|_| limit_exceeded())?;
    let maximum = maximum
        .checked_add(size_of::<MaintenanceDecodedSourceV1<T>>())
        .ok_or_else(limit_exceeded)?;
    // The existing canonical decode plan already includes the full census
    // allowance. Check the Types-owned fixed metadata/control quote fits that
    // allowance before constructing any census buffer or decoded graph.
    if OwnedCapacityInspectionScratchV1::<LocatorCharge>::construction_upper_bound()
        .map_err(|_| limit_exceeded())?
        > OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
    {
        return Err(limit_exceeded());
    }
    let work = owner.admit_source_maintenance_decode(maximum)?;
    let mut census =
        OwnedCapacityInspectionScratchV1::try_new(work).map_err(|_| limit_exceeded())?;
    // The same reservation predates scratch, generated DTOs, semantic values,
    // mapper temporary buffers and the existing exhaustive capacity inspector.
    let mut scratch = Vec::new();
    scratch
        .try_reserve_exact(plan.scratch_capacity())
        .map_err(|_| limit_exceeded())?;
    let value = reconstruct(encoded, &mut scratch).map_err(decode_error)?;
    let observed = census
        .preview(value.value())
        .map_err(|_| limit_exceeded())?;
    let retained = observed
        .shared_allocations()
        .iter()
        .try_fold(observed.unshared_bytes(), |total, allocation| {
            total.checked_add(allocation.bytes())
        })
        .ok_or_else(limit_exceeded)?;
    let scratch_bytes = owned_allocation_capacity_bound_v1(scratch.capacity(), align_of::<u8>())
        .map_err(|_| limit_exceeded())?;
    let retained = retained
        .checked_add(scratch_bytes)
        .and_then(|n| n.checked_add(size_of::<MaintenanceDecodedSourceV1<T>>() as u64))
        .ok_or_else(limit_exceeded)?;
    if retained > maximum as u64 {
        return Err(limit_exceeded());
    }
    drop(observed);
    // Report metadata has physically retired before extracting the original
    // actual Maintenance charge into the closed data-before-control result.
    let work = census.into_control();
    Ok(MaintenanceDecodedSourceV1 {
        value,
        _scratch: scratch,
        _work: work,
    })
}
pub(super) fn decode_error(error: ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    match error {
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
            storage_error(StorageErrorKind::Unavailable)
        }
        ProjectionSourceRecordDecodeErrorV1::Codec(_) => corrupt(),
    }
}

#[cfg(test)]
#[path = "source_validation_admission_tests.rs"]
mod tests;
