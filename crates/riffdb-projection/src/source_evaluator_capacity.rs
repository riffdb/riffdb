//! Complete incremental allocation census for the unchanged source evaluator.
use riffdb_catalog::ResolvedProjectionPlan;
use riffdb_contract_ir::{ProjectionAggregation, ValueTypeTag};
use riffdb_storage_api::{CheckedProjectionSchema, StoredCommitRecordV1};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, FieldId, MAX_KEY_BYTES, MAX_PROJECTION_ROW_UPDATES,
    OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1, ProjectionGroupKey,
    owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

pub(super) fn work_bound(
    resolved: &ResolvedProjectionPlan,
    schema: &CheckedProjectionSchema,
    commit: &StoredCommitRecordV1,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let plan = resolved.projection_plan();
    let max_key = schema.maximum_complete_key_bytes();
    if max_key > MAX_KEY_BYTES {
        return Err(OwnedCapacityInspectionErrorV1);
    }
    let mut total = report_bytes(OwnedCapacityInspectionV1::inspect(schema)?)?;
    add(
        &mut total,
        riffdb_invariant::source_expression_control_work_bound_v1(plan.expressions())?,
    )?;
    let mut matching = 0_usize;
    let mut transient = 0_u64;
    let mut retained_entry = 0_u64;
    for event in commit
        .events()
        .iter()
        .filter(|event| event.event_type_id() == plan.source_event())
    {
        matching = matching
            .checked_add(1)
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        let mut event_work =
            resolved.source_event_materialization_work_bound_v1(commit.plan(), event)?;
        add(
            &mut event_work,
            riffdb_invariant::source_expression_values_work_bound_v1(
                plan.expressions(),
                event.payload(),
            )?,
        )?;
        add(
            &mut event_work,
            growing_vector::<CanonicalValue>(plan.key_expressions().len())?,
        )?;
        let mut key_clones = 0_u64;
        for expression in plan.key_expressions() {
            add(
                &mut key_clones,
                riffdb_invariant::source_expression_result_clone_work_bound_v1(
                    plan.expressions(),
                    *expression,
                    event.payload(),
                )?,
            )?;
        }
        // EvaluationBatch clones key values, then group_key clones them again.
        add(&mut event_work, key_clones)?;
        add(&mut event_work, key_clones)?;
        add(
            &mut event_work,
            growing_vector::<CanonicalValue>(plan.key_expressions().len())?,
        )?;
        // Native key: final doubled byte capacity + old growth allocation,
        // separate identity serialization and one canonical component encoding.
        add_allocation::<u8>(&mut event_work, 2 * max_key)?;
        add_allocation::<u8>(&mut event_work, max_key)?;
        add_allocation::<u8>(&mut event_work, max_key)?;
        add_allocation::<u8>(&mut event_work, max_key)?;
        let measure_count = plan.measures().len();
        let mut encoded_measures = 6_usize;
        for measure in plan.measures() {
            match measure.field().value_type().tag() {
                ValueTypeTag::I64
                | ValueTypeTag::U64
                | ValueTypeTag::Decimal
                | ValueTypeTag::Money => {}
                ValueTypeTag::Bool
                | ValueTypeTag::String
                | ValueTypeTag::Bytes
                | ValueTypeTag::Timestamp
                | ValueTypeTag::Date
                | ValueTypeTag::Uuid
                | ValueTypeTag::Enum
                | ValueTypeTag::Optional
                | ValueTypeTag::List
                | ValueTypeTag::Record
                | ValueTypeTag::Vector => return Err(OwnedCapacityInspectionErrorV1),
            }
            let max_value = measure
                .field()
                .value_type()
                .maximum_canonical_bytes()
                .map_err(|_| OwnedCapacityInspectionErrorV1)?
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            encoded_measures = encoded_measures
                .checked_add(4)
                .and_then(|v| v.checked_add(max_value))
                .ok_or(OwnedCapacityInspectionErrorV1)?;
            match measure.aggregation() {
                ProjectionAggregation::Count => {}
                ProjectionAggregation::Sum => add(
                    &mut event_work,
                    riffdb_invariant::source_expression_result_clone_work_bound_v1(
                        plan.expressions(),
                        measure.expression().ok_or(OwnedCapacityInspectionErrorV1)?,
                        event.payload(),
                    )?,
                )?,
            }
        }
        // Count/Sum retained fields and checked-add results are numeric inline
        // scalars under the checked plan. Existing field-type bounds include
        // malformed-result temporary encoding before ordinary validation refuses.
        add(&mut event_work, record_work(measure_count)?)?;
        add_allocation::<u8>(&mut event_work, encoded_measures)?;
        // Existing grouped replacement retains old/current/delta while building
        // one new record, including Result<Vec> growth and stable-sort scratch.
        add(&mut event_work, record_work(measure_count)?)?;
        transient = transient.max(event_work);
        let mut entry = 0_u64;
        add_allocation::<u8>(&mut entry, 2 * max_key)?;
        add(
            &mut entry,
            retained_vector::<CanonicalValue>(plan.key_expressions().len())?,
        )?;
        add(&mut entry, key_clones)?;
        add(
            &mut entry,
            retained_vector::<(FieldId, CanonicalValue)>(measure_count)?,
        )?;
        retained_entry = retained_entry.max(entry);
    }
    let groups = matching.min(MAX_PROJECTION_ROW_UPDATES);
    let tree = OwnedCapacityInspectionV1::inspect_with(|inspection| {
        inspection.prospective_btree::<ProjectionGroupKey, CanonicalRecord>(groups)
    })?;
    add(&mut total, report_bytes(tree)?)?;
    add(
        &mut total,
        retained_entry
            .checked_mul(u64::try_from(groups).map_err(|_| OwnedCapacityInspectionErrorV1)?)
            .ok_or(OwnedCapacityInspectionErrorV1)?,
    )?;
    // The max event transient includes a fully materialized candidate key/delta
    // before the existing4096th-group limit check, even when it will be refused.
    add(&mut total, transient)?;
    Ok(total)
}

fn record_work(count: usize) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let mut bytes = growing_vector::<(FieldId, CanonicalValue)>(count)?;
    add_allocation::<(FieldId, CanonicalValue)>(&mut bytes, count)?;
    Ok(bytes)
}
fn retained_vector<T>(count: usize) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    if count == 0 {
        return Ok(0);
    }
    let capacity = count
        .max(4)
        .checked_next_power_of_two()
        .ok_or(OwnedCapacityInspectionErrorV1)?;
    owned_allocation_capacity_bound_v1(
        capacity
            .checked_mul(size_of::<T>())
            .ok_or(OwnedCapacityInspectionErrorV1)?,
        align_of::<T>(),
    )
}
fn growing_vector<T>(count: usize) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let mut bytes = retained_vector::<T>(count)?;
    if count != 0 {
        let capacity = count
            .max(4)
            .checked_next_power_of_two()
            .ok_or(OwnedCapacityInspectionErrorV1)?;
        add_allocation::<T>(&mut bytes, capacity / 2)?;
    }
    Ok(bytes)
}
fn report_bytes(
    report: riffdb_types::OwnedCapacityReportV1,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let mut bytes = report.unshared_bytes();
    for shared in report.shared_allocations() {
        add(&mut bytes, shared.bytes())?;
    }
    Ok(bytes)
}
fn add_allocation<T>(total: &mut u64, count: usize) -> Result<(), OwnedCapacityInspectionErrorV1> {
    add(
        total,
        owned_allocation_capacity_bound_v1(
            count
                .checked_mul(size_of::<T>())
                .ok_or(OwnedCapacityInspectionErrorV1)?,
            align_of::<T>(),
        )?,
    )
}
fn add(total: &mut u64, bytes: u64) -> Result<(), OwnedCapacityInspectionErrorV1> {
    *total = total
        .checked_add(bytes)
        .ok_or(OwnedCapacityInspectionErrorV1)?;
    Ok(())
}
