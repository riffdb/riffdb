// req: OQ-040
//! Capacity inspection proves ownership bounds, never payload or identity output.

use super::*;
use std::sync::Arc;

#[test]
fn retained_capacity_is_charged_even_when_payload_is_empty() {
    let value = Vec::<u64>::with_capacity(4096);
    let report = OwnedCapacityInspectionV1::inspect(&value).unwrap();
    assert!(report.unshared_bytes() >= 4096 * 8);
    let text = String::with_capacity(8192);
    assert!(
        OwnedCapacityInspectionV1::inspect(&text)
            .unwrap()
            .unshared_bytes()
            >= 8192
    );
}

#[test]
fn shared_allocations_are_reported_once_across_multiple_references() {
    let value = Arc::new(String::with_capacity(8192));
    let report = OwnedCapacityInspectionV1::inspect(&[value.clone(), value.clone()]).unwrap();
    assert_eq!(report.shared_allocations().len(), 1);
    assert!(report.shared_allocations()[0].bytes() >= 8192);
    assert_eq!(report.unshared_bytes(), 0);
    assert!(!format!("{report:?}").contains("8192"));
    assert_eq!(
        format!("{:?}", report.shared_allocations()[0].identity()),
        "SharedAllocationIdentityV1([REDACTED])"
    );
}

#[test]
fn nested_shared_ownership_is_not_flattened_into_two_charges() {
    let child = Arc::new(String::with_capacity(8192));
    let parent = Arc::new(vec![child.clone(), child.clone()]);
    let report = OwnedCapacityInspectionV1::inspect(&parent).unwrap();
    assert_eq!(report.shared_allocations().len(), 2);
    assert!(
        report
            .shared_allocations()
            .iter()
            .map(|entry| entry.bytes())
            .sum::<u64>()
            >= 8192
    );
    assert_eq!(report.unshared_bytes(), 0);
}

#[test]
fn retained_tree_root_and_nested_values_are_charged_after_removal() {
    let mut tree = std::collections::BTreeMap::new();
    tree.insert(1_u64, String::with_capacity(8192));
    let populated = OwnedCapacityInspectionV1::inspect(&tree)
        .unwrap()
        .unshared_bytes();
    assert!(populated >= 8192);
    tree.remove(&1);
    let emptied = OwnedCapacityInspectionV1::inspect(&tree)
        .unwrap()
        .unshared_bytes();
    assert!(emptied > 0);
    assert!(emptied < populated);
}

#[test]
fn canonical_value_inspection_charges_private_buffer_capacity() {
    let text = CanonicalValue::string(String::with_capacity(8192)).unwrap();
    let report = OwnedCapacityInspectionV1::inspect(&text).unwrap();
    assert!(report.unshared_bytes() >= 8192);
}

struct IgnoresOverflow;
impl InspectOwnedCapacityV1 for IgnoresOverflow {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        assert!(inspection.allocation(usize::MAX, 8).is_err());
        Ok(())
    }
}

#[test]
fn a_failed_walk_cannot_produce_a_partial_report_even_if_the_caller_ignores_error() {
    assert!(OwnedCapacityInspectionV1::inspect(&IgnoresOverflow).is_err());
}

struct Nested {
    child: Option<Box<Nested>>,
}
crate::inspect_owned_capacity_struct_v1!(Nested {
    owned: [child],
    inline: []
});

#[test]
fn excessive_recursion_refuses_without_an_unbounded_stack_walk() {
    let mut node = Nested { child: None };
    for _ in 0..32 {
        node = Nested {
            child: Some(Box::new(node)),
        };
    }
    assert!(OwnedCapacityInspectionV1::inspect(&node).is_ok());
    for _ in 0..128 {
        node = Nested {
            child: Some(Box::new(node)),
        };
    }
    assert!(OwnedCapacityInspectionV1::inspect(&node).is_err());
}

#[test]
fn shared_census_refuses_a_successor_instead_of_growing_inspection_scratch() {
    let values = (0..=crate::owned_capacity::MAX_SHARED_ALLOCATIONS)
        .map(|_| Arc::new(1_u64))
        .collect::<Vec<_>>();
    assert!(OwnedCapacityInspectionV1::inspect(&values).is_err());
    assert!(
        OwnedCapacityInspectionV1::inspect(
            &values[..crate::owned_capacity::MAX_SHARED_ALLOCATIONS]
        )
        .is_ok()
    );
}
