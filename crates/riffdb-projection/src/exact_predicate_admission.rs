//! Borrowed preflight admits owned programs, source copies, and sparse indexes.
// req: OQ-040
use super::*;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use riffdb_types::{
    InspectOwnedCapacityV1, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1, canonical_record_encoded_len,
};
use std::mem::{align_of, size_of};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum ExactPredicateAdmissionErrorV1 {
    CapacityUnavailable,
    Provider(ExactPredicateProviderErrorV1),
}
impl From<BudgetUnavailable> for ExactPredicateAdmissionErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::CapacityUnavailable
    }
}
impl fmt::Display for ExactPredicateAdmissionErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityUnavailable => {
                formatter.write_str("exact predicate capacity unavailable")
            }
            Self::Provider(error) => error.fmt(formatter),
        }
    }
}
impl Error for ExactPredicateAdmissionErrorV1 {}

macro_rules! admitted_rebuild {
    ($provider:ty, $binding:ty, $program:ty, $profiles:ident) => {
        impl $provider {
            /// Admit before cloning the owned program and rows or deriving indexes.
            #[doc(hidden)]
            pub fn rebuild_admitted_v1(
                binding: $binding,
                program: &$program,
                rows: &[ExactPredicateProviderRowV1],
                budget: &ProviderBudget,
            ) -> Result<Arc<ProviderAllocation<Self>>, ExactPredicateAdmissionErrorV1> {
                let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
                let profiles =
                    $profiles(program).map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                let maximum = construction_maximum(
                    program,
                    program.orders().len(),
                    profiles.len(),
                    rows.len(),
                    rows.iter(),
                )
                .map_err(|_| BudgetUnavailable)?;
                drop(profiles);
                drop(scratch);
                let work = budget.reserve(maximum)?;
                let value = Self::rebuild(binding, program.clone(), rows.to_vec())
                    .map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                work.settle_owned_value(value).map_err(Into::into)
            }

            /// Admit a private successor before any deep predecessor clone.
            /// Ordinary apply still validates duplicate keys and frontier order.
            #[doc(hidden)]
            pub fn apply_admitted_v1(
                &self,
                epoch: CommitSequence,
                mutations: &[ExactPredicateIndexMutationV1],
                budget: &ProviderBudget,
            ) -> Result<Arc<ProviderAllocation<Self>>, ExactPredicateAdmissionErrorV1> {
                let scratch = budget.reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)?;
                let profiles =
                    $profiles(&self.program).map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                let count = self
                    .rows
                    .len()
                    .checked_add(mutations.len())
                    .ok_or(BudgetUnavailable)?;
                let rows = self
                    .rows
                    .iter()
                    .chain(mutations.iter().filter_map(|mutation| match mutation {
                        ExactPredicateIndexMutationV1::Upsert(row) => Some(row),
                        ExactPredicateIndexMutationV1::Delete(_) => None,
                    }));
                let construction = construction_maximum(
                    &self.program,
                    self.program.orders().len(),
                    profiles.len(),
                    count,
                    rows,
                )
                .map_err(|_| BudgetUnavailable)?;
                let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
                    // apply temporarily keeps the private predecessor, cloned
                    // row map, seen-key tree, and replacement indexes together.
                    inspection.visit(self)?;
                    inspection.visit(self)?;
                    inspection.prospective_btree::<Vec<u8>, ExactPredicateProviderRowV1>(count)?;
                    inspection.prospective_btree::<Vec<u8>, ()>(mutations.len())?;
                    for row in &self.rows {
                        inspection.visit(&row.key)?;
                    }
                    for mutation in mutations {
                        inspection.visit(mutation.key())?;
                        inspection.visit(mutation.key())?;
                    }
                    Ok(())
                })
                .map_err(|_| BudgetUnavailable)?;
                if !report.shared_allocations().is_empty() {
                    return Err(BudgetUnavailable.into());
                }
                let maximum = construction
                    .checked_add(report.unshared_bytes())
                    .ok_or(BudgetUnavailable)?;
                drop(report);
                drop(profiles);
                drop(scratch);
                let work = budget.reserve(maximum)?;
                let mut value = self.clone();
                value
                    .apply(epoch, mutations)
                    .map_err(ExactPredicateAdmissionErrorV1::Provider)?;
                work.settle_owned_value(value).map_err(Into::into)
            }
        }
    };
}
admitted_rebuild!(
    ExactPredicatePartitionIndexV4,
    ExactPredicateProviderBindingV1,
    ExactPredicateProgramV1,
    referenced_profiles
);
admitted_rebuild!(
    ExactPredicatePartitionIndexV5,
    ExactPredicateProviderBindingV2,
    ExactPredicateProgramV2,
    referenced_profiles_v2
);

fn construction_maximum<'a>(
    program: &impl InspectOwnedCapacityV1,
    orders: usize,
    fields: usize,
    row_count: usize,
    rows: impl Clone + Iterator<Item = &'a ExactPredicateProviderRowV1>,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let report = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        // Deep owned program/row clones, canonical verification output and
        // descriptor construction coexist with the final indexes.
        for _ in 0..4 {
            inspection.visit(program)?;
            for row in rows.clone() {
                inspection.visit(row)?;
            }
            inspection.allocation(
                row_count
                    .checked_mul(size_of::<ExactPredicateProviderRowV1>())
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                align_of::<ExactPredicateProviderRowV1>(),
            )?;
        }
        for row in rows.clone() {
            let encoded = canonical_record_encoded_len(&row.output)
                .map_err(|_| OwnedCapacityInspectionErrorV1)?;
            inspection.allocation(
                encoded
                    .checked_mul(4)
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                1,
            )?;
        }
        inspection.prospective_btree::<FieldId, ExactComparisonProfileV1>(fields)?;
        inspection.allocation(
            orders
                .checked_mul(4 * size_of::<ExactOrderIndexV1>())
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<ExactOrderIndexV1>(),
        )?;
        for _ in 0..orders {
            inspection.prospective_btree::<FieldId, SparseFieldIndexV1>(fields)?;
            inspection.allocation(
                row_count
                    .checked_mul(4 * size_of::<usize>())
                    .ok_or(OwnedCapacityInspectionErrorV1)?,
                align_of::<usize>(),
            )?;
            for row in rows.clone() {
                for cell in row.fields.values() {
                    // Empty, singleton, and geometrically grown state vectors.
                    inspection.allocation(12 * size_of::<usize>(), align_of::<usize>())?;
                    if let ExactReferenceCellV1::Value(value) = cell {
                        inspection.visit(value)?;
                        // Counting a separate root per row is conservative even
                        // when values and posting vectors later deduplicate.
                        inspection.prospective_btree::<ExactScalarV1, Vec<usize>>(1)?;
                        inspection.allocation(4 * size_of::<usize>(), align_of::<usize>())?;
                        if let ExactScalarV1::String(value) = value {
                            let bytes = value.len();
                            let suffix_bytes = bytes
                                .checked_add(1)
                                .and_then(|next| bytes.checked_mul(next))
                                .map(|total| total / 2)
                                .ok_or(OwnedCapacityInspectionErrorV1)?;
                            for count in [1, 1, bytes] {
                                inspection.prospective_btree::<Vec<u8>, Vec<usize>>(count)?;
                                inspection.allocation(
                                    count
                                        .checked_mul(4 * size_of::<usize>())
                                        .ok_or(OwnedCapacityInspectionErrorV1)?,
                                    align_of::<usize>(),
                                )?;
                            }
                            let keys = bytes
                                .checked_mul(4)
                                .and_then(|base| {
                                    suffix_bytes
                                        .checked_mul(2)
                                        .and_then(|suffix| base.checked_add(suffix))
                                })
                                .ok_or(OwnedCapacityInspectionErrorV1)?;
                            inspection.allocation(keys, 1)?;
                        }
                    }
                }
            }
        }
        Ok(())
    })?;
    if !report.shared_allocations().is_empty() {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    report
        .unshared_bytes()
        .checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .and_then(|bytes| bytes.checked_add(32768))
        .ok_or(OwnedCapacityInspectionErrorV1)
}
