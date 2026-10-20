//! Cloned graph semantics and ownership admission, independent of source authority.
use super::*;
use crate::{
    CanonicalBytes, CanonicalList, CanonicalString, CanonicalVector, Date, Decimal, DecimalSpec,
    FieldId, Money, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityInspectionV1,
    ProjectionSourceWorkingBudgetV1, Timestamp,
};

fn all_values() -> Vec<CanonicalValue> {
    let decimal = Decimal::new(DecimalSpec::new(12, 2).unwrap(), 123).unwrap();
    vec![
        CanonicalValue::Null,
        CanonicalValue::Bool(true),
        CanonicalValue::I64(-10),
        CanonicalValue::U64(10),
        CanonicalValue::Decimal(decimal),
        CanonicalValue::Money(Money::new(
            crate::CurrencyCode::new("USD").unwrap(),
            decimal,
        )),
        CanonicalValue::String(CanonicalString::new("ticket").unwrap()),
        CanonicalValue::Bytes(CanonicalBytes::new(vec![1, 2, 3]).unwrap()),
        CanonicalValue::Timestamp(Timestamp::new(-1, 123).unwrap()),
        CanonicalValue::Date(Date::from_days_since_unix_epoch(-10)),
        CanonicalValue::Uuid([7; 16]),
        CanonicalValue::Enum {
            type_id: crate::EnumTypeId::first(),
            variant_id: crate::EnumVariantId::first(),
        },
        CanonicalValue::List(
            CanonicalList::new(vec![CanonicalValue::Null, CanonicalValue::Bool(false)]).unwrap(),
        ),
        CanonicalValue::Record(
            CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(1))]).unwrap(),
        ),
        CanonicalValue::Vector(CanonicalVector::new(vec![0.0, 1.25]).unwrap()),
    ]
}

// req: STO-020, PRJ-004
#[test]
fn borrowed_clone_bound_covers_every_tag_and_preserves_canonical_bytes() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let mut _census = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    for value in all_values() {
        let bound = canonical_value_clone_work_bound_v1(&value).unwrap();
        let work = (bound != 0).then(|| budget.reserve(bound).unwrap());
        let cloned = value.clone();
        let actual =
            OwnedCapacityInspectionV1::preview(&cloned, _census.inspection_scope().unwrap())
                .unwrap();
        assert!(actual.shared_allocations().is_empty());
        assert!(actual.unshared_bytes() <= bound);
        assert_eq!(value, cloned);
        assert_eq!(
            crate::encode_canonical_value(&value).unwrap(),
            crate::encode_canonical_value(&cloned).unwrap()
        );
        drop((actual, cloned, work));
    }
}

// req: STO-020, PRJ-004
#[test]
fn borrowed_record_clone_bound_admits_nested_graph_and_releases_after_drop() {
    let record = CanonicalRecord::new(vec![(
        FieldId::first(),
        CanonicalValue::List(CanonicalList::new(all_values()).unwrap()),
    )])
    .unwrap();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let before = budget.observation().unwrap();
    let bound = canonical_record_clone_work_bound_v1(&record).unwrap();
    let free = crate::PROJECTION_SOURCE_WORKING_MEMORY_BYTES - before.fixed_owner_bytes;
    let pressure = budget.reserve(free - bound + 1).unwrap();
    assert!(budget.reserve(bound).is_err());
    drop(pressure);
    let work = budget.reserve(bound).unwrap();
    let cloned = record.clone();
    let mut census = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let actual =
        OwnedCapacityInspectionV1::preview(&cloned, census.inspection_scope().unwrap()).unwrap();
    assert!(actual.unshared_bytes() <= work.bytes());
    assert_eq!(record, cloned);
    assert_eq!(
        crate::encode_canonical_record(&record).unwrap(),
        crate::encode_canonical_record(&cloned).unwrap()
    );
    drop(actual);
    drop(cloned);
    drop(census);
    drop(work);
    assert_eq!(budget.observation().unwrap(), before);
}

// req: STO-020
#[test]
fn clone_bound_counts_new_buffers_separately_from_retained_input_capacity() {
    let mut text = String::with_capacity(8192);
    text.push_str("ticket");
    let value = CanonicalValue::String(CanonicalString::new(text).unwrap());
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let mut _census = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let input =
        OwnedCapacityInspectionV1::preview(&value, _census.inspection_scope().unwrap()).unwrap();
    let input_bytes = input.unshared_bytes();
    drop(input);
    let bound = canonical_value_clone_work_bound_v1(&value).unwrap();
    assert!(bound < input_bytes);
    let _work = budget.reserve(bound).unwrap();
    let clone = value.clone();
    let output =
        OwnedCapacityInspectionV1::preview(&clone, _census.inspection_scope().unwrap()).unwrap();
    assert!(output.unshared_bytes() <= bound);
    assert_eq!(clone, value);
}
