// req: OQ-040
//! Capacity inspection proves ownership bounds, never payload or identity output.

use super::*;
use std::sync::Arc;

#[test]
fn retained_capacity_is_charged_even_when_payload_is_empty() {
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let mut inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let value = Vec::<u64>::with_capacity(4096);
    let report =
        OwnedCapacityInspectionV1::preview(&value, inspection_work.inspection_scope().unwrap())
            .unwrap();
    assert!(report.unshared_bytes() >= 4096 * 8);
    drop(report);
    let text = String::with_capacity(8192);
    assert!(
        OwnedCapacityInspectionV1::preview(&text, inspection_work.inspection_scope().unwrap())
            .unwrap()
            .unshared_bytes()
            >= 8192
    );
}

#[test]
fn shared_allocations_are_reported_once_across_multiple_references() {
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let mut inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let value = Arc::new(String::with_capacity(8192));
    let report = OwnedCapacityInspectionV1::preview(
        &[value.clone(), value.clone()],
        inspection_work.inspection_scope().unwrap(),
    )
    .unwrap();
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
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let mut inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let child = Arc::new(String::with_capacity(8192));
    let parent = Arc::new(vec![child.clone(), child.clone()]);
    let report =
        OwnedCapacityInspectionV1::preview(&parent, inspection_work.inspection_scope().unwrap())
            .unwrap();
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
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let mut inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let mut tree = std::collections::BTreeMap::new();
    tree.insert(1_u64, String::with_capacity(8192));
    let populated =
        OwnedCapacityInspectionV1::preview(&tree, inspection_work.inspection_scope().unwrap())
            .unwrap()
            .unshared_bytes();
    assert!(populated >= 8192);
    tree.remove(&1);
    let emptied =
        OwnedCapacityInspectionV1::preview(&tree, inspection_work.inspection_scope().unwrap())
            .unwrap()
            .unshared_bytes();
    assert!(emptied > 0);
    assert!(emptied < populated);
}

#[test]
fn canonical_value_inspection_charges_private_buffer_capacity() {
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let mut inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let text = CanonicalValue::string(String::with_capacity(8192)).unwrap();
    let report =
        OwnedCapacityInspectionV1::preview(&text, inspection_work.inspection_scope().unwrap())
            .unwrap();
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
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let mut inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    assert!(
        OwnedCapacityInspectionV1::preview(
            &IgnoresOverflow,
            inspection_work.inspection_scope().unwrap()
        )
        .is_err()
    );
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
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let mut inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let mut node = Nested { child: None };
    for _ in 0..32 {
        node = Nested {
            child: Some(Box::new(node)),
        };
    }
    assert!(
        OwnedCapacityInspectionV1::preview(&node, inspection_work.inspection_scope().unwrap())
            .is_ok()
    );
    for _ in 0..128 {
        node = Nested {
            child: Some(Box::new(node)),
        };
    }
    assert!(
        OwnedCapacityInspectionV1::preview(&node, inspection_work.inspection_scope().unwrap())
            .is_err()
    );
}

#[test]
fn shared_census_refuses_a_successor_instead_of_growing_inspection_scratch() {
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let mut inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let values = (0..=crate::owned_capacity::MAX_SHARED_ALLOCATIONS)
        .map(|_| Arc::new(1_u64))
        .collect::<Vec<_>>();
    assert!(
        OwnedCapacityInspectionV1::preview(&values, inspection_work.inspection_scope().unwrap())
            .is_err()
    );
    assert!(
        OwnedCapacityInspectionV1::preview(
            &values[..crate::owned_capacity::MAX_SHARED_ALLOCATIONS],
            inspection_work.inspection_scope().unwrap()
        )
        .is_ok()
    );
}

#[test]
fn shared_string_inspection_retains_destruction_ownership_through_its_last_capture() {
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let value = ContractLineage::new("captured_lineage").unwrap();
    let cloned = value.clone();
    let report = OwnedCapacityInspectionV1::inspect_owned(&value, inspection_work).unwrap();
    let lifecycle = report.shared_allocations()[0].lifecycle().unwrap();
    let charge = budget.reserve(128).unwrap().commit().unwrap();
    drop(lifecycle.attach(charge).unwrap());
    drop(cloned);
    assert_eq!(budget.observation().unwrap().owned_bytes, 128);
    drop(report);
    assert_eq!(budget.observation().unwrap().owned_bytes, 128);
    drop(value);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().released_allocations, 1);
}

#[test]
fn destruction_bookkeeping_does_not_change_key_hash_order_or_debug() {
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    fn hashed(value: &impl Hash) -> u64 {
        let mut hash = DefaultHasher::new();
        value.hash(&mut hash);
        hash.finish()
    }
    let lineage = ContractLineage::new("stable_lineage").unwrap();
    let separately_constructed = ContractLineage::new("stable_lineage").unwrap();
    let before = hashed(&lineage);
    let report = OwnedCapacityInspectionV1::inspect_owned(&lineage, inspection_work).unwrap();
    let budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    drop(
        report.shared_allocations()[0]
            .lifecycle()
            .unwrap()
            .attach(budget.reserve(128).unwrap().commit().unwrap())
            .unwrap(),
    );
    assert_eq!(lineage, separately_constructed);
    assert_eq!(
        lineage.cmp(&separately_constructed),
        std::cmp::Ordering::Equal
    );
    assert_eq!(hashed(&lineage), before);
    assert_eq!(hashed(&lineage), hashed(&"stable_lineage"));
    assert_eq!(
        format!("{lineage:?}"),
        "ContractLineage(\"stable_lineage\")"
    );
    let environment = Environment::new("test").unwrap();
    assert_eq!(format!("{environment:?}"), "Environment(\"test\")");
    let actor = ActorId::new("reader").unwrap();
    assert_eq!(format!("{actor:?}"), "ActorId([REDACTED])");
    assert_eq!(hashed(&actor), hashed(&"reader"));
}

#[test]
fn boxed_slice_counts_element_storage_and_nested_capacity_and_shared_aliases() {
    let inspection_budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let mut inspection_work = inspection_budget
        .reserve(crate::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let budget = crate::exact_provider_budget_v1::ProviderBudget::new();
    let _scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let values = vec![String::with_capacity(8192), String::with_capacity(4096)].into_boxed_slice();
    let report =
        OwnedCapacityInspectionV1::preview(&values, inspection_work.inspection_scope().unwrap())
            .unwrap();
    assert!(
        report.unshared_bytes() >= 2 * (8192 + 4096 + 2 * std::mem::size_of::<String>()) as u64
    );
    drop(report);
    let shared = Arc::new(String::with_capacity(8192));
    let aliases = vec![shared.clone(), shared].into_boxed_slice();
    let report =
        OwnedCapacityInspectionV1::preview(&aliases, inspection_work.inspection_scope().unwrap())
            .unwrap();
    assert!(report.unshared_bytes() >= 2 * (2 * std::mem::size_of::<Arc<String>>()) as u64);
    assert_eq!(report.shared_allocations().len(), 1);
    assert!(report.shared_allocations()[0].bytes() >= 2 * 8192);
}

// req: PRJ-004
#[test]
fn closed_text_retained_quote_covers_actual_shell_text_and_lifecycle_control() {
    use crate::exact_provider_budget_v1::ProviderBudget;
    use crate::{OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, OwnedCapacityTextV1};

    for length in [0, 1, 8, 256] {
        let quoted = OwnedCapacityTextV1::retained_allocation_upper_bound(length).unwrap();
        let budget = ProviderBudget::new();
        let mut work = budget
            .reserve(
                quoted
                    .checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
                    .unwrap(),
            )
            .unwrap();
        let text = OwnedCapacityTextV1::new("x".repeat(length));
        let report =
            OwnedCapacityInspectionV1::preview(&text, work.inspection_scope().unwrap()).unwrap();
        assert_eq!(report.unshared_bytes(), 0);
        assert_eq!(report.shared_allocations().len(), 1);
        assert!(report.shared_allocations()[0].bytes() <= quoted);
        drop(report);
        drop(text);
        drop(work);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
    assert!(OwnedCapacityTextV1::retained_allocation_upper_bound(usize::MAX).is_err());
}
