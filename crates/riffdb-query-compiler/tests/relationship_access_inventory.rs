//! Executable inventory for ADR-0150's accepted relationship composition.

use std::fs;
use std::path::{Path, PathBuf};

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_ir::{
    MAX_QUERY_ARTIFACT_BYTES, QueryAccessKind, QueryPredicateOperator, QueryPredicateValue,
    SymbolicCatalog,
};
use riffdb_riffql_syntax::parse_query;

struct Domain<'a> {
    name: &'a str,
    contract: &'a str,
    queries: &'a str,
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

// req: OQ-033, OQ-039, OQ-041
#[test]
fn every_real_point_dependency_and_dependent_batch_matches_the_frozen_inventory() {
    let root = workspace_root();
    let domains = [
        Domain {
            name: "ticketdesk",
            contract: "examples/app-baseline/contracts/ticketdesk.riff",
            queries: "queries/ticketdesk",
        },
        Domain {
            name: "agent-blog",
            contract: "examples/agent-alpha/domains/agent-blog/domain/contract.riff",
            queries: "queries/agent-alpha/blog",
        },
        Domain {
            name: "agent-orders",
            contract: "examples/agent-alpha/domains/agent-orders/domain/contract.riff",
            queries: "queries/agent-alpha/orders",
        },
    ];
    let mut inventory = Vec::new();

    for domain in domains {
        let contract = fs::read_to_string(root.join(domain.contract)).expect("contract source");
        let bundle = compile_contract_source(&contract).expect("contract compiles");
        let catalog = SymbolicCatalog::from_bundle(&bundle).expect("symbolic catalog");
        let mut query_paths = fs::read_dir(root.join(domain.queries))
            .expect("query directory")
            .map(|entry| entry.expect("query entry").path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "riffq")
            })
            .collect::<Vec<_>>();
        query_paths.sort_unstable();

        for path in query_paths {
            let source = fs::read_to_string(&path).expect("query source");
            let document = parse_query(&source).expect("query parses");
            let program = compile_query(&document, &catalog).expect("query compiles");
            let query = path
                .file_stem()
                .and_then(|name| name.to_str())
                .expect("query name");

            for (step_index, step) in program.steps().iter().enumerate() {
                if matches!(step.access(), QueryAccessKind::Nearest { .. }) {
                    continue;
                }
                for predicate in step.predicates() {
                    let (source_binding, source_field, collection) = match predicate.value() {
                        QueryPredicateValue::BindingField { binding, field } => {
                            (binding, field, false)
                        }
                        QueryPredicateValue::BindingFieldSet { binding, field } => {
                            (binding, field, true)
                        }
                        QueryPredicateValue::Parameter(_)
                        | QueryPredicateValue::Literal(_)
                        | QueryPredicateValue::EnumVariant { .. }
                        | QueryPredicateValue::CandidateBinding { .. } => continue,
                    };
                    let source_step = program.steps()[..step_index]
                        .iter()
                        .find(|candidate| candidate.binding() == source_binding)
                        .expect("dependency is an earlier step");
                    assert_eq!(
                        source_step.internal_partition_key_schema().components(),
                        step.internal_partition_key_schema().components(),
                        "relationship dependency must retain one type-exact partition route",
                    );
                    assert!(step.dependencies().contains(source_binding));
                    assert_eq!(
                        predicate.operator(),
                        if collection {
                            QueryPredicateOperator::In
                        } else {
                            QueryPredicateOperator::Equal
                        }
                    );
                    let (shape, access_key_bytes, maximum_probes) = match step.access() {
                        QueryAccessKind::Point { key_fields } => {
                            assert!(!collection);
                            assert!(key_fields.contains(&predicate.field().to_owned()));
                            (
                                "point",
                                step.internal_entity_key_schema().maximum_encoded_bytes(),
                                1,
                            )
                        }
                        QueryAccessKind::DependentPointBatch {
                            key_fields,
                            source_binding: declared_binding,
                            source_field: declared_field,
                        } => {
                            assert!(collection);
                            assert_eq!(declared_binding, source_binding);
                            assert_eq!(declared_field, source_field);
                            assert!(key_fields.contains(&predicate.field().to_owned()));
                            assert!(step.maximum_rows() <= source_step.maximum_rows());
                            (
                                "dependent-batch",
                                step.internal_entity_key_schema().maximum_encoded_bytes(),
                                step.maximum_rows(),
                            )
                        }
                        QueryAccessKind::Index { .. } => {
                            assert!(!collection);
                            (
                                "bounded-index",
                                step.internal_index_key_schema()
                                    .expect("index access key schema")
                                    .maximum_encoded_bytes(),
                                step.maximum_rows().saturating_add(1),
                            )
                        }
                        QueryAccessKind::ExpansionIndex { .. } => {
                            assert!(!collection);
                            (
                                "bounded-expansion-index",
                                step.internal_index_key_schema()
                                    .expect("expansion index access key schema")
                                    .maximum_encoded_bytes(),
                                step.maximum_rows().saturating_add(1),
                            )
                        }
                        QueryAccessKind::PartitionSetIndex { .. } => {
                            unreachable!("relationship corpus has no partition-set route")
                        }
                        QueryAccessKind::Nearest { .. } => unreachable!("filtered above"),
                        QueryAccessKind::CandidateRootHydration { .. } => {
                            unreachable!("relationship corpus has no candidate root")
                        }
                        QueryAccessKind::LongPatternCandidate { .. } => {
                            unreachable!("relationship corpus has no pattern provider")
                        }
                    };
                    let total_key_bytes = access_key_bytes
                        .checked_mul(usize::try_from(step.maximum_rows()).expect("bounded rows"))
                        .expect("bounded key-byte product");
                    assert!(total_key_bytes <= MAX_QUERY_ARTIFACT_BYTES);
                    let missing = step.absence_outcome().unwrap_or_else(|| {
                        if matches!(step.access(), QueryAccessKind::Index { .. }) {
                            "empty"
                        } else {
                            "optional-none"
                        }
                    });
                    let cursor = if step.cursor_parameter().is_some() {
                        "opaque"
                    } else {
                        "none"
                    };
                    let authority = program
                        .authorization()
                        .iter()
                        .find(|access| access.entity() == step.entity())
                        .expect("dependent target has compiler-derived authority");
                    assert!(authority.maximum_rows() >= step.maximum_rows());
                    assert!(
                        step.selected_fields()
                            .iter()
                            .chain(step.predicate_fields())
                            .all(|field| authority.fields().contains(field)),
                        "selected and dependency fields must be authorized",
                    );
                    let explain = program.explain().lines().join("\n");
                    let expected_explain = if collection {
                        format!("dependent primary-key batch from {source_binding}.{source_field}")
                    } else {
                        match step.access() {
                            QueryAccessKind::Point { .. } => "primary-key".to_owned(),
                            QueryAccessKind::Index { index, .. }
                            | QueryAccessKind::ExpansionIndex { index, .. } => {
                                format!("index {index}")
                            }
                            _ => unreachable!("relationship inventory shape handled above"),
                        }
                    };
                    assert!(explain.contains(&expected_explain));
                    let cost = program.cost();
                    inventory.push(format!(
                        "{}/{} {}.{} <- {}.{} shape={} partition=shared driver_rows<={} fanout_rows<={} probes<={} access_key_bytes<={} query_intermediates<={} query_projected_values<={} query_result_bytes<={} missing={} cursor={} authority=sealed explain=sealed plan_identity=sealed",
                        domain.name,
                        query,
                        step.binding(),
                        predicate.field(),
                        source_binding,
                        source_field,
                        shape,
                        source_step.maximum_rows(),
                        step.maximum_rows(),
                        maximum_probes,
                        total_key_bytes,
                        cost.intermediate_rows(),
                        cost.projected_values(),
                        cost.encoded_result_bytes(),
                        missing,
                        cursor,
                    ));
                    if matches!(
                        step.access(),
                        QueryAccessKind::Point { .. } | QueryAccessKind::DependentPointBatch { .. }
                    ) {
                        assert!(step.cursor_parameter().is_none());
                    }
                }
            }
        }
    }

    inventory.sort_unstable();
    let actual = format!("{}\n", inventory.join("\n"));
    assert_eq!(
        actual,
        include_str!("../../../fixtures/riffql/relationship-access-inventory-v1.txt")
    );
}

// req: OQ-042
#[test]
fn relationship_runtime_contains_no_unaccepted_join_operator() {
    let root = workspace_root();
    let ir = fs::read_to_string(root.join("crates/riffdb-query-ir/src/plan.rs"))
        .expect("query IR source");
    assert!(ir.contains("DependentPointBatch"));
    for forbidden in [
        "SemiJoin",
        "CorrelatedExists",
        "OneToManyExpansion",
        "CartesianProduct",
        "CrossPartitionJoin",
        "RecursiveTraversal",
        "JoinOptimizer",
    ] {
        assert!(
            !ir.contains(forbidden),
            "relationship runtime unexpectedly contains {forbidden}"
        );
    }
    if ir.contains("ExpansionIndex") {
        let amendment = fs::read_to_string(
            root.join("adr/0185-relational-operators-in-the-candidate-algebra.md"),
        )
        .expect("accepted successor relationship amendment");
        assert!(amendment.contains("status: accepted"));
        assert!(amendment.contains("**One-to-many expansion.**"));
    }
}

// req: OQ-041, OQ-043
#[test]
fn redb_uses_the_position_preserving_policy_aware_batch_contract() {
    let root = workspace_root();
    for path in ["crates/riffdb-storage-redb/src/query.rs"] {
        let source = fs::read_to_string(root.join(path)).expect("storage query source");
        // Follow a diagnostic wrapper to the implementation. `riffdb-storage-redb`
        // wraps this method to time the storage read-view call and delegates to
        // `dependent_point_batch_inner`; slicing to the trait method alone found
        // only the wrapper and silently stopped checking anything. Prefer the
        // `_inner` body when it exists so the invariants below keep applying to
        // the code that actually batches.
        let (start, marker) = source
            .find("    fn dependent_point_batch_inner(")
            .map(|index| (index, "    fn dependent_point_batch_inner("))
            .or_else(|| {
                source
                    .find("    fn dependent_point_batch(")
                    .map(|index| (index, "    fn dependent_point_batch("))
            })
            .expect("dependent batch implementation");
        let tail = &source[start + marker.len()..];
        let end = tail.find("\n    fn ").expect("next query method");
        let body = &tail[..end];
        assert!(
            body.contains("RowMaterializePlan::for_step"),
            "{path}: the sliced body does not look like the batch implementation; \
             a rename or a new wrapper has broken this test's source slice"
        );
        assert_eq!(
            body.matches("RowMaterializePlan::for_step").count(),
            1,
            "{path} must construct one materialization plan per batch"
        );
        assert!(
            body.contains(
                ".map(|predicates| self.point_with_plan(step, predicates, &plan, policy))"
            ),
            "{path} must preserve input position and apply the same policy to every target"
        );
        for forbidden in ["sort", "dedup", "begin_read", "begin_composite_read"] {
            assert!(
                !body.contains(forbidden),
                "{path} must not add batch-local {forbidden} semantics"
            );
        }
    }
}
