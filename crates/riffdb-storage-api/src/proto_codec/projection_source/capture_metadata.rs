//! Bounded current application-pointer custody for one actual source capture.
//! Canonical values alone never establish engine, plan or publication authority.
use super::*;
use crate::ActiveCatalogPointerV1;

/// Move-only semantic admission made before the native capture's writer lease.
/// Its private originating ledger and reservation cannot be replaced separately.
/// Full canonical/semantic reconstruction still happens outside that lease.
#[doc(hidden)]
pub struct ProjectionSourceActiveCatalogDecodeAdmissionV1 {
    budget: ProjectionSourceWorkingBudgetV1,
    work: riffdb_types::ProjectionSourceWorkReservationV1,
}
impl ProjectionSourceActiveCatalogDecodeAdmissionV1 {
    /// Admits the complete closed control maximum and final ownership census.
    /// No stored bytes, catalog selection, native mutation or parser runs here.
    pub fn new(
        budget: &ProjectionSourceWorkingBudgetV1,
    ) -> Result<Self, ProjectionSourceRecordDecodeErrorV1> {
        let transport=ProjectionSourceDecodeV1::<wire::ActiveCatalogPointerV1>::maximum_control_transport_work_bound_v1()?;
        // Active pointers have exactly a lineage String, fixed hash byte Vec,
        // and version scalar, with no canonical group/measure containers. This
        // is the zero-container specialization of semantic_work_bound below.
        let maximum = transport
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .and_then(|bytes| bytes.checked_add(128 * 1024))
            .ok_or(BudgetUnavailable)?;
        let work = budget.reserve(maximum)?;
        Ok(Self {
            budget: budget.clone(),
            work,
        })
    }
    /// Reuses the complete existing mapper and canonical checks under the same
    /// admitted ledger. Returned physical graph charge lasts to its final alias.
    pub fn decode(
        self,
        encoded: &[u8],
    ) -> Result<
        EncodedPageItem<ProjectionSourceWorkAllocationV1<ActiveCatalogPointerV1>>,
        ProjectionSourceRecordDecodeErrorV1,
    > {
        let input = inspect_projection_source_decode_v1::<wire::ActiveCatalogPointerV1>(encoded)
            .map_err(DurableCodecError::from_decode_envelope)?;
        if semantic_work_bound(&input)? > self.work.bytes() {
            return Err(BudgetUnavailable.into());
        }
        let Self { budget, work } = self;
        reconstruct_admitted_with_work(
            encoded.len(),
            input,
            &budget,
            work,
            super::super::catalog::active_from_proto,
        )
    }
}

/// Source-only complete canonical application pointer reconstruction. Ordinary
/// catalog reads remain quota-independent and retain their existing decoder.
#[doc(hidden)]
pub fn decode_projection_source_active_catalog_admitted_v1(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    EncodedPageItem<ProjectionSourceWorkAllocationV1<ActiveCatalogPointerV1>>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    ProjectionSourceActiveCatalogDecodeAdmissionV1::new(budget)?.decode(encoded)
}
#[cfg(test)]
#[path = "capture_metadata_tests.rs"]
mod tests;
