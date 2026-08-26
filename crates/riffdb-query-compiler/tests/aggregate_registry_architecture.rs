//! Static ownership and inventory guards for ADR-0152's semantic registry.

use std::fs;
use std::path::PathBuf;

use riffdb_query_ir::source_aggregate_semantic_identity;
use riffdb_riffql_syntax::{format_query, parse_query};
use riffdb_types::{AggregateSemanticIdentityV1, aggregate_semantic_registry_v1};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

#[test]
fn one_registry_is_consumed_by_every_existing_semantic_layer() {
    let root = workspace_root();
    let registry = fs::read_to_string(root.join("crates/riffdb-types/src/aggregate.rs"))
        .expect("aggregate registry source");
    assert_eq!(
        registry
            .matches("const AGGREGATE_SEMANTIC_REGISTRY_V1:")
            .count(),
        1,
        "aggregate semantics have one registry owner"
    );

    for (path, marker) in [
        (
            "crates/riffdb-query-ir/src/resolver.rs",
            "let semantic = source_aggregate_semantic_identity(measure.function.value)",
        ),
        (
            "crates/riffdb-query-ir/src/operational.rs",
            "pub const fn semantic_identity",
        ),
        ("crates/riffdb-query-compiler/src/lib.rs", ".descriptor()"),
        (
            "crates/riffdb-query-executor/src/lib.rs",
            "let semantic = measure.function().semantic_identity()",
        ),
        (
            "crates/riffdb-query-module/src/lib.rs",
            ".source_spelling()",
        ),
        (
            "crates/riffdb-columnar/src/query.rs",
            "agg.semantic_identity().descriptor().input_class()",
        ),
    ] {
        let source = fs::read_to_string(root.join(path)).expect("semantic consumer source");
        assert!(
            source.contains(marker),
            "aggregate semantic consumer {path} lost registry marker {marker}"
        );
    }
}

#[test]
fn unchanged_parser_and_formatter_cover_the_registry_exactly() {
    let source = r#"
query AggregateInventory() {
    many rows from Row where organization_id == organization_id order by row_id asc take 1
    aggregate result from rows {
        count() as count_value
        exact_count() as exact_count_value
        sum(amount) as sum_value
        min(amount) as min_value
        max(amount) as max_value
    }
    return Found { result: result { count_value exact_count_value sum_value min_value max_value } }
    outcomes Found
}
"#;
    let document = parse_query(source).expect("aggregate inventory parses");
    let measures = &document.body.aggregates[0].measures;
    assert_eq!(measures.len(), aggregate_semantic_registry_v1().len());
    for (measure, descriptor) in measures.iter().zip(aggregate_semantic_registry_v1()) {
        assert_eq!(
            source_aggregate_semantic_identity(measure.function.value),
            descriptor.identity()
        );
        assert!(
            format_query(&document).contains(&format!("{}(", descriptor.source_spelling())),
            "formatter omitted registry spelling {}",
            descriptor.source_spelling()
        );
    }
    assert_eq!(
        source_aggregate_semantic_identity(measures[1].function.value),
        AggregateSemanticIdentityV1::ExactCount
    );
}

#[test]
fn inventory_accounts_for_result_wire_generation_and_future_shapes() {
    let root = workspace_root();
    let evidence =
        fs::read_to_string(root.join("docs/architecture/aggregate-semantic-registry-v1.md"))
            .expect("aggregate registry evidence");

    for existing in [
        "`count`",
        "`exact_count`",
        "`sum`",
        "`min`",
        "`max`",
        "`group by`",
        "QueryAggregateCell",
        "AggregateValue",
        "ProjectionAggregation::{Count, Sum}",
        "service/wire",
        "Rust/Go/TypeScript/Python",
    ] {
        assert!(
            evidence.contains(existing),
            "aggregate inventory omitted {existing}"
        );
    }

    for sketch in [
        "### Exact variance sketch",
        "### Approximate cardinality sketch",
        "### Percentile and histogram sketch",
    ] {
        let section = evidence
            .split(sketch)
            .nth(1)
            .expect("required descriptor sketch");
        for field in [
            "posture:",
            "numerical algorithm:",
            "merge state:",
            "quality/error statement:",
            "empty/null behavior:",
            "determinism:",
            "bounds:",
            "provider epoch:",
            "policy partition:",
            "durable-state identity:",
        ] {
            assert!(section.contains(field), "{sketch} omitted {field}");
        }
    }

    for classification in [
        "transient compiler vocabulary",
        "neither serialized nor hashed",
        "release-significant under ADR-0124",
        "no version-topology node",
        "pay-once",
    ] {
        assert!(
            evidence.contains(classification),
            "durability/pay-once evidence omitted {classification}"
        );
    }
}

#[test]
fn deferred_sketches_have_no_runtime_or_dependency_arm() {
    let root = workspace_root();
    for path in [
        "crates/riffdb-types/src/aggregate.rs",
        "crates/riffdb-riffql-syntax/src/syntax.rs",
        "crates/riffdb-query-ir/src/operational.rs",
        "crates/riffdb-query-executor/src/lib.rs",
        "crates/riffdb-columnar/src/query.rs",
    ] {
        let source = fs::read_to_string(root.join(path)).expect("aggregate runtime source");
        for forbidden in ["Variance", "HyperLogLog", "Percentile", "Histogram"] {
            assert!(
                !source.contains(forbidden),
                "paper-only aggregate {forbidden} entered runtime source {path}"
            );
        }
    }
}
