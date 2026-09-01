//! ADR-0175 bounded partition-set locality, identity, and diagnostic coverage.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{PlannerDiagnosticCode, compile_operational_query_family};
use riffdb_query_ir::{
    QUERY_IR_VERSION_PARTITION_SET_V1, QueryAccessKind, QueryPartitionRouteV1, SymbolicCatalog,
};
use riffdb_riffql_syntax::{RIFFQL_LANGUAGE_VERSION_PARTITION_SET_V1, parse_query};

const CONTRACT: &str = r#"
contract PartitionSetRuns version 1 {
  entity Run {
    key (experiment_id: u64, run_id: string<32>)
    field start_time: i64
    index by_start (experiment_id, start_time, run_id)
  }

  aggregate Runs {
    root Run
    partition_by experiment_id
    conflict_key (experiment_id, run_id)
  }
}
"#;

const QUERY: &str = r#"
query SearchRuns(
    $experiment_ids: Set<Run.experiment_id, 1000>,
    $limit: Limit<50> = 50,
    $after: Cursor?,
) {
    many runs from Run
        where experiment_id in $experiment_ids
        order by start_time desc, run_id asc
        take $limit after $after
        else IntegrityFailure

    return Found { runs: runs { experiment_id run_id start_time } }
    outcomes Found | IntegrityFailure
}
"#;

fn catalog() -> SymbolicCatalog {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    SymbolicCatalog::from_bundle(&bundle).expect("catalog")
}

#[test]
fn bounded_partition_membership_is_the_one_compiler_sealed_route() {
    let document = parse_query(QUERY).expect("query");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_PARTITION_SET_V1
    );
    let family = compile_operational_query_family(&document, &catalog())
        .expect("bounded partition-set route compiles");
    let program = family.select(&[]).expect("only member").program();
    assert_eq!(program.ir_version(), QUERY_IR_VERSION_PARTITION_SET_V1);
    assert_eq!(
        program.partition_route(),
        &QueryPartitionRouteV1::FiniteSet {
            parameter: "experiment_ids".to_owned(),
            maximum: 1_000,
        }
    );
    assert!(program.explain().lines()[0].contains("within 1000"));
}

#[test]
fn legacy_set_does_not_silently_become_a_partition_route() {
    let source = QUERY.replace("Set<Run.experiment_id, 1000>", "Set<Run.experiment_id>");
    let error = compile_operational_query_family(&parse_query(&source).expect("query"), &catalog())
        .expect_err("legacy set is not an ADR-0175 route");
    let diagnostic = &error.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::NonLocal);
    assert_eq!(diagnostic.symbol_path(), ["Run", "experiment_id"]);
    assert!(diagnostic.primary().start < diagnostic.primary().end);
}

#[test]
fn every_binding_must_share_the_exact_same_bounded_route() {
    let source = QUERY.replace(
        "return Found",
        "many other from Run\n        where experiment_id == 1\n        order by start_time asc, run_id asc\n        take 1\n        else IntegrityFailure\n\n    return Found",
    );
    let error = compile_operational_query_family(&parse_query(&source).expect("query"), &catalog())
        .expect_err("different scalar route must fail");
    let diagnostic = &error.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::NonLocal);
    assert!(diagnostic.primary().start < diagnostic.primary().end);
}

#[test]
fn uniform_heap_plan_supports_pages_well_above_the_predecessor_499_limit() {
    let source = QUERY.replace("Limit<50>", "Limit<5000>").replace(
        "order by start_time desc, run_id asc",
        "order by start_time desc, run_id desc",
    );
    let family =
        compile_operational_query_family(&parse_query(&source).expect("query"), &catalog())
            .expect("large bounded page compiles");
    let program = family.select(&[]).expect("member").program();
    let QueryAccessKind::PartitionSetIndex { scan_ceiling, .. } = program.steps()[0].access()
    else {
        panic!("partition-set index")
    };
    assert_eq!(
        *scan_ceiling,
        riffdb_query_ir::MAX_QUERY_SCANNED_ROWS as u32
    );
    assert!(program.cost().scanned_index_rows() >= 4_194_304);
}

#[test]
fn mlflow_mixed_order_supports_large_pages_across_one_thousand_routes() {
    let source = QUERY.replace("Limit<50>", "Limit<5000>");
    let family =
        compile_operational_query_family(&parse_query(&source).expect("query"), &catalog())
            .expect("large mixed-order page compiles");
    let program = family.select(&[]).expect("member").program();
    let QueryAccessKind::PartitionSetIndex { scan_ceiling, .. } = program.steps()[0].access()
    else {
        panic!("partition-set index")
    };
    assert_eq!(*scan_ceiling, 65_535);
    assert_eq!(program.cost().scanned_index_rows(), 65_535_000);
}
