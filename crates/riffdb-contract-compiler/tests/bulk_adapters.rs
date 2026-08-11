//! Adapter-shaped positive corpus for ADR-0107 bounded collection plans.

use riffdb_contract_compiler::compile_contract_source;

#[test]
fn adapter_collection_sources_compile_to_one_bounded_v5_plan() {
    for (name, source) in [
        (
            "openfga",
            include_str!("../../../fixtures/contracts/bulk/openfga-tuples.riff"),
        ),
        (
            "mlflow",
            include_str!("../../../fixtures/contracts/bulk/mlflow-metrics.riff"),
        ),
        (
            "payload",
            include_str!("../../../fixtures/contracts/bulk/payload-document-graph.riff"),
        ),
        (
            "woodpecker",
            include_str!("../../../fixtures/contracts/bulk/woodpecker-pipeline-steps.riff"),
        ),
    ] {
        let bundle = compile_contract_source(source).unwrap_or_else(|error| {
            panic!("{name} bounded collection source must compile: {error}")
        });
        assert_eq!(bundle.ir_version(), 5, "{name}");
        assert_eq!(bundle.commands().len(), 1, "{name}");
        let expansion = bundle.commands()[0]
            .collection_expansion()
            .unwrap_or_else(|| panic!("{name} collection expansion"));
        assert!(expansion.maximum_elements() <= 128, "{name}");
        assert!(
            expansion
                .maximum_elements()
                .checked_mul(expansion.binding_count())
                .is_some_and(|instances| instances <= 256),
            "{name}"
        );
    }
}
