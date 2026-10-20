//! One Source-owned row at a time; complete-command arithmetic stays here.
use super::*;
use riffdb_storage_api::ProjectionSourceRowV1;
use riffdb_types::{
    CanonicalValue, FieldId, OwnedCapacityInspectionV1, ProjectionSourceLocatorV1,
    ProjectionSourceWorkAllocationV1, canonical_record_clone_work_bound_v1,
    canonical_record_encoded_len, canonical_value_clone_work_bound_v1, canonical_value_encoded_len,
    owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

impl SourceAdmittedProjectionCommitV1<'_> {
    /// Accounting affinity only; no independent source/publication authority.
    #[doc(hidden)]
    pub fn belongs_to(&self, budget: &ProjectionSourceWorkingBudgetV1) -> bool {
        self.budget.shares_owner_with(budget)
    }

    /// Reparses one evaluated canonical key with fresh Source-owned identity
    /// storage. Foreign catalog Arc lifecycle bindings are never settled here.
    #[doc(hidden)]
    pub fn prepare_source_group_key_v1(
        &self,
        key: &ProjectionGroupKey,
    ) -> Result<ProjectionSourceWorkAllocationV1<ProjectionGroupKey>, ProjectionEvaluationError>
    {
        self.evaluated
            .grouped_deltas()
            .get(key)
            .ok_or_else(integrity)?;
        // Admit census before inspecting any shared typed control. Reuse this
        // same reservation as settlement scratch instead of admitting it twice.
        let mut work = self
            .budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .map_err(|_| unavailable())?;
        let mut bytes = key_reparse_work(key, work.inspection_scope().map_err(|_| unavailable())?)?;
        add(&mut bytes, settlement_root::<ProjectionGroupKey>()?)?;
        work.absorb(self.budget.reserve(bytes).map_err(|_| unavailable())?)
            .map_err(|_| unavailable())?;
        let independent =
            ProjectionGroupKey::from_bytes(key.as_bytes().to_vec()).map_err(|_| integrity())?;
        if &independent != key {
            return Err(integrity());
        }
        work.settle_shared_value(independent)
            .map_err(|_| unavailable())
    }

    /// Prepares the exact complete-command replacement, validating the optional
    /// admitted current row. The returned fresh row owns no catalog allocations.
    /// Locator syntax is non-authoritative; native capture binds its expectation.
    #[doc(hidden)]
    pub fn prepare_source_row_v1(
        &self,
        key: &ProjectionSourceWorkAllocationV1<ProjectionGroupKey>,
        current: Option<&ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>>,
        locator: ProjectionSourceLocatorV1,
    ) -> Result<ProjectionSourceWorkAllocationV1<ProjectionSourceRowV1>, ProjectionEvaluationError>
    {
        if !key.belongs_to(self.budget) {
            return Err(integrity());
        }
        let delta = self
            .evaluated
            .grouped_deltas()
            .get(&**key)
            .ok_or_else(integrity)?;
        if let Some(row) = current
            && (!row.belongs_to(self.budget)
                || row.identity() != self.identity()
                || row.locator() != locator
                || row.group_values() != key.components()
                || row.last_changed_sequence() >= self.sequence())
        {
            return Err(integrity());
        }
        let mut work = self
            .budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .map_err(|_| unavailable())?;
        let mut bytes = key_reparse_work(key, work.inspection_scope().map_err(|_| unavailable())?)?;
        add(&mut bytes, settlement_root::<ProjectionSourceRowV1>()?)?;
        add(
            &mut bytes,
            canonical_record_clone_work_bound_v1(delta).map_err(|_| unavailable())?,
        )?;
        add(
            &mut bytes,
            allocation::<u8>(canonical_record_encoded_len(delta).map_err(|_| unavailable())?)?,
        )?;
        if let Some(row) = current {
            add(
                &mut bytes,
                allocation::<u8>(
                    canonical_record_encoded_len(row.measures()).map_err(|_| unavailable())?,
                )?,
            )?;
            // The ordinary complete-command combiner creates one numeric
            // record and, for Mean, a temporary cloned key for its totals lookup.
            add(&mut bytes, numeric_record_work(delta.len())?)?;
            add(&mut bytes, key_clone_work(key)?)?;
        }
        work.absorb(self.budget.reserve(bytes).map_err(|_| unavailable())?)
            .map_err(|_| unavailable())?;
        let measures = match current {
            Some(row) => {
                self.evaluated
                    .schema()
                    .validate_measure_record(row.measures())
                    .map_err(|_| integrity())?;
                self.evaluated.combine_with_prior(key, row.measures())?
            }
            None => delta.clone(),
        };
        let independent =
            ProjectionGroupKey::from_bytes(key.as_bytes().to_vec()).map_err(|_| integrity())?;
        let row = ProjectionSourceRowV1::new(
            self.evaluated.schema(),
            locator,
            self.sequence(),
            independent,
            measures,
            self.sequence(),
        )
        .map_err(|_| integrity())?;
        work.settle_shared_value(row).map_err(|_| unavailable())
    }
}

// Clone copies the encoded bytes and component Vec. The immutable identity
// retains its existing closed shared backing without allocating another shell.
fn key_clone_work(key: &ProjectionGroupKey) -> Result<u64, ProjectionEvaluationError> {
    let mut bytes = allocation::<u8>(key.as_bytes().len())?;
    add(
        &mut bytes,
        allocation::<CanonicalValue>(key.components().len())?,
    )?;
    for value in key.components() {
        add(
            &mut bytes,
            canonical_value_clone_work_bound_v1(value).map_err(|_| unavailable())?,
        )?;
    }
    Ok(bytes)
}

// ProjectionGroupKey::from_bytes consumes the copied bytes and allocates one
// new ContractLineage (temporary String, closed text owner), a growing
// component Vec, decoded scalar payloads and one canonical re-encode at a time.
// Inspect only after the caller admits the existing census scratch. The lineage
// report conservatively includes all final lifecycle/control bytes; it never
// transfers its existing ownership attachment into the fresh decoded lineage.
fn key_reparse_work(
    key: &ProjectionGroupKey,
    scope: riffdb_types::OwnedCapacityInspectionScopeV1<'_>,
) -> Result<u64, ProjectionEvaluationError> {
    let mut bytes = allocation::<u8>(key.as_bytes().len())?;
    let lineage = key.identity().contract_lineage();
    let report = OwnedCapacityInspectionV1::preview(lineage, scope).map_err(|_| unavailable())?;
    add(&mut bytes, report.unshared_bytes())?;
    for shared in report.shared_allocations() {
        add(&mut bytes, shared.bytes())?;
    }
    drop(report);
    add(&mut bytes, allocation::<u8>(lineage.as_bytes().len())?)?;
    let count = key.components().len();
    if count != 0 {
        let capacity = count
            .max(4)
            .checked_next_power_of_two()
            .ok_or_else(unavailable)?;
        add(&mut bytes, allocation::<CanonicalValue>(capacity)?)?;
        add(&mut bytes, allocation::<CanonicalValue>(capacity / 2)?)?;
    }
    let mut encode = 0;
    for value in key.components() {
        add(
            &mut bytes,
            canonical_value_clone_work_bound_v1(value).map_err(|_| unavailable())?,
        )?;
        encode = encode.max(canonical_value_encoded_len(value).map_err(|_| unavailable())?);
    }
    add(&mut bytes, allocation::<u8>(encode)?)?;
    Ok(bytes)
}
// The actual settlement owner's sole capacity formula includes all root/control
// allocations; keep this aligned with the current owning API, not an old DTO layout.
fn settlement_root<T>() -> Result<u64, ProjectionEvaluationError> {
    riffdb_types::exact_provider_budget_v1::ProviderAllocationOwner::<T>::settlement_root_capacity_upper_bound()
        .map_err(|_| unavailable())
}
fn numeric_record_work(count: usize) -> Result<u64, ProjectionEvaluationError> {
    if count == 0 {
        return Ok(0);
    }
    let capacity = count
        .max(4)
        .checked_next_power_of_two()
        .ok_or_else(unavailable)?;
    let mut bytes = allocation::<(FieldId, CanonicalValue)>(capacity)?;
    add(
        &mut bytes,
        allocation::<(FieldId, CanonicalValue)>(capacity / 2)?,
    )?;
    add(&mut bytes, allocation::<(FieldId, CanonicalValue)>(count)?)?;
    Ok(bytes)
}
fn allocation<T>(count: usize) -> Result<u64, ProjectionEvaluationError> {
    owned_allocation_capacity_bound_v1(
        count.checked_mul(size_of::<T>()).ok_or_else(unavailable)?,
        align_of::<T>(),
    )
    .map_err(|_| unavailable())
}
fn add(total: &mut u64, bytes: u64) -> Result<(), ProjectionEvaluationError> {
    *total = total.checked_add(bytes).ok_or_else(unavailable)?;
    Ok(())
}
fn integrity() -> ProjectionEvaluationError {
    ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ProjectionStateIntegrity)
}
