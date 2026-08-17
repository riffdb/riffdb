//! Pay-once descriptor/catalog validation architecture test.

use std::num::{NonZeroU16, NonZeroU32};
use std::sync::Arc;

use riffdb_catalog::ProjectionProviderCatalogV1;
use riffdb_query_module::{
    ProjectionResultSetBindingV1, ProjectionResultSetPlanV1, ResultSetOutputShapeV1,
    ResultSetWindowV1,
};
use riffdb_types::{
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1, QueryOperationName,
};

fn binding(name: &str) -> ProjectionResultSetBindingV1 {
    let provider = ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::Columnar,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::FILTER
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        ProjectionProviderPolicyModeV1::PartitionAligned,
        ProjectionProviderStaticBoundsV1 {
            max_candidates: 500,
            max_output_rows: 50,
            max_measures: 0,
            max_input_bytes: 1_024,
            max_work_units: 50_000,
            max_state_bytes_per_row: 1_024,
            max_diagnostic_bytes: 512,
            retained_epochs: 100,
            max_catchup_lag: 10,
            max_epoch_lease_steps: 100,
        },
        ProjectionProviderStateIdentityV1::new(NonZeroU32::new(1).unwrap(), [9; 32]),
    )
    .unwrap();
    let plan = ProjectionResultSetPlanV1::new(
        provider,
        true,
        true,
        false,
        ResultSetWindowV1::Top {
            limit: NonZeroU16::new(50).unwrap(),
        },
        ResultSetOutputShapeV1::TypedRows,
    )
    .unwrap();
    ProjectionResultSetBindingV1::new(QueryOperationName::new(name).unwrap(), plan)
}

#[test]
fn descriptor_validation_is_once_per_catalog_generation_not_per_lookup() {
    let canonical = vec![binding("BoardPage").to_canonical_bytes()];
    let catalog = ProjectionProviderCatalogV1::open(17, &canonical).unwrap();
    assert_eq!(catalog.validation_count(), 1);
    let name = QueryOperationName::new("BoardPage").unwrap();
    let first = catalog.get(&name).unwrap();
    for _ in 0..10_000 {
        let observed = catalog.get(&name).unwrap();
        assert!(Arc::ptr_eq(&first, &observed));
    }
    assert_eq!(catalog.validation_count(), 1);
}

#[test]
fn foundation_has_no_full_text_bridge_or_composite_runtime_dependency() {
    let manifests = [
        include_str!("../Cargo.toml"),
        include_str!("../../riffdb-query-ir/Cargo.toml"),
        include_str!("../../riffdb-query-module/Cargo.toml"),
        include_str!("../../riffdb-projection/Cargo.toml"),
        include_str!("../../riffdb-columnar/Cargo.toml"),
    ]
    .join("\n");
    for forbidden_dependency in ["tantivy", "roaring", "bm25", "fulltext"] {
        assert!(
            !manifests
                .to_ascii_lowercase()
                .contains(forbidden_dependency),
            "foundation gained forbidden provider/bridge dependency {forbidden_dependency}"
        );
    }

    let plan_source = include_str!("../../riffdb-query-ir/src/result_set.rs");
    assert!(!plan_source.contains("Vec<ProjectionProviderDescriptorV1>"));
    assert!(!plan_source.contains("HashMap"));
    assert!(!plan_source.contains("HashSet"));
}
