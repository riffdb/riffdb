//! Provider encoding must precede allocation with exact borrowed sizing.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

#[test]
fn provider_bundle_encoding_matches_frozen_bytes_and_retains_final_output_charge() {
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/production-embedding/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/cascade/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/vector-ann/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/text-index/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/initialized-state-transition/bundle.bin")
            .as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret-reveal/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/indexed-selection/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/command-decision/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/aggregate-collection-budget/bundle.bin")
            .as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let mut counter = Writer::counting(MAX_BUNDLE_BYTES);
        encode_bundle_to(&mut counter, &bundle).unwrap();
        assert_eq!(counter.encoded_len(), bytes.len());
        assert_eq!(
            counter.finish().capacity(),
            0,
            "sizing retains no encoding buffer"
        );
        let budget = ProviderBudget::new();
        let root = bundle
            .compute_provider_plan_root_hash_admitted_v1(&budget)
            .unwrap();
        assert_eq!(root, bundle.plan_root_hash());
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        let encoded = bundle.encode_provider_admitted_v1(&budget).unwrap();
        assert_eq!(encoded.as_ref().as_slice(), bytes);
        assert_eq!(
            encoded.as_ref().capacity(),
            bytes.len(),
            "one exact output allocation"
        );
        let observation = budget.observation().unwrap();
        assert_eq!(observation.reserved_bytes, 0);
        assert!(observation.owned_bytes >= 2 * encoded.as_ref().capacity() as u64);
        let escaped = encoded.clone();
        drop(encoded);
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            observation.owned_bytes
        );
        drop(escaped);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }
}

#[test]
fn provider_bundle_encoding_refuses_before_sizing_and_before_output() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    for remaining in [128, 64 * 1024] {
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            bundle.encode_provider_admitted_v1(&budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}

#[test]
fn provider_root_hash_refuses_pressure_and_preserves_changed_semantic_hash() {
    let mut bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let budget = ProviderBudget::new();
    let fixed = budget.observation().unwrap().fixed_owner_bytes;
    for remaining in [128, 32 * 1024] {
        let pressure = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - remaining)
            .unwrap();
        let before = budget.observation().unwrap();
        assert!(matches!(
            bundle.compute_provider_plan_root_hash_admitted_v1(&budget),
            Err(ProviderBundleEncodingErrorV1::Unavailable)
        ));
        assert_eq!(budget.observation().unwrap(), before);
        drop(pressure);
    }
    let expected = bundle
        .compute_provider_plan_root_hash_admitted_v1(&budget)
        .unwrap();
    assert_eq!(expected, bundle.plan_root_hash());
    assert!(bundle.projections.pop().is_some());
    let changed = bundle
        .compute_provider_plan_root_hash_admitted_v1(&budget)
        .unwrap();
    let ordinary = compute_plan_root_hash_versioned(
        &bundle.schema,
        &bundle.workflows,
        &bundle.row_policies,
        &bundle.commands,
        &bundle.projections,
        bundle.ir_version,
    )
    .unwrap();
    assert_eq!(changed, ordinary);
    assert_ne!(changed, expected);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}
