//! Private provider conversion buffers need admission through their final drop.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

#[test]
fn source_rows_work_refuses_growth_before_copy_at_budget_saturation() {
    let budget = ProviderBudget::new();
    let mut work = SourceRowsWork::new(&budget).unwrap();
    let before = budget.observation().unwrap();
    let pressure = budget
        .reserve(
            EXACT_PROVIDER_BUDGET_BYTES
                - before.fixed_owner_bytes
                - before.reserved_bytes
                - before.owned_bytes,
        )
        .unwrap();
    let before = budget.observation().unwrap();
    let fields = CanonicalRecord::new(vec![(
        FieldId::first(),
        CanonicalValue::string("synthetic payload").unwrap(),
    )])
    .unwrap();
    assert!(matches!(
        work.reserve_row(&fields, 2, 3, 64),
        Err(RebuildFailure::Transient)
    ));
    assert_eq!(budget.observation().unwrap(), before);
    drop(work);
    drop(pressure);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn source_rows_work_native_bounds_cover_conversion_forests_and_outlive_data_drop() {
    let budget = ProviderBudget::new();
    let mut work = SourceRowsWork::new(&budget).unwrap();
    let fields = CanonicalRecord::new(vec![(
        FieldId::first(),
        CanonicalValue::string("x".repeat(262144)).unwrap(),
    )])
    .unwrap();
    work.reserve_row(&fields, 1024, 1024, 4096).unwrap();
    let values = fields.fields().iter().cloned().collect::<BTreeMap<_, _>>();
    let output = fields.clone();
    let reference_fields = (1..=1024)
        .map(|id| (FieldId::new(id).unwrap(), ExactReferenceCellV1::Null))
        .collect::<BTreeMap<_, _>>();
    let report = riffdb_types::OwnedCapacityInspectionV1::inspect_with(|inspection| {
        inspection.visit(&values)?;
        inspection.visit(&output)?;
        inspection.visit(&reference_fields)
    })
    .unwrap();
    assert!(report.unshared_bytes() < budget.observation().unwrap().reserved_bytes);
    drop(values);
    drop(output);
    drop(reference_fields);
    struct DropProbe(ProviderBudget);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            assert!(self.0.observation().unwrap().reserved_bytes > 0);
        }
    }
    let guarded = work.guard(DropProbe(budget.clone()));
    drop(guarded);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
