#![forbid(unsafe_code)]

//! ADR-0177 high-cardinality collection compilation and compatibility boundaries.

use riffdb_contract_compiler::{
    CompilerBoundResource, CompilerDiagnosticCode, compile_contract_source,
};
use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V23, ContractBundle, EXECUTABLE_IR_VERSION_V23, GRAMMAR_VERSION_V23,
};

const METRICS: &str =
    include_str!("../../../fixtures/contracts/bulk/high-cardinality-metrics.riff");

fn four_mutation_templates(maximum: usize, fixed_root: bool) -> String {
    let root_binding = if fixed_root {
        "mutate Summary(run_id) as summary else RunMissing {}"
    } else {
        "read Summary(run_id) as summary else RunMissing {}"
    };
    let root_effect = if fixed_root {
        "set summary.revision = summary.revision + 1"
    } else {
        ""
    };
    format!(
        r#"
contract MutationBoundary version 1 {{
  entity Summary {{
    key (run_id: string<32>)
    field revision: u64
  }}
  entity Candidate {{ key (run_id: string<32>, item_id: u64) }}
  entity ItemA {{ key (run_id: string<32>, item_id: u64) }}
  entity ItemB {{ key (run_id: string<32>, item_id: u64) }}
  entity ItemC {{ key (run_id: string<32>, item_id: u64) }}
  entity ItemD {{ key (run_id: string<32>, item_id: u64) }}
  aggregate Items {{
    root Summary
    child Candidate
    child ItemA
    child ItemB
    child ItemC
    child ItemD
    partition_by run_id
    conflict_key (run_id)
  }}
  bulk command ApplyCandidates {{
    input request_id: string<128>
    input run_id: string<32>
    input candidates: list<Candidate, 1..{maximum}> aggregate_bytes <= 1048576
    idempotency_key request_id
    {root_binding}
    for candidate in candidates {{
      create ItemA(run_id, candidate.item_id) as a else ExistsA {{}}
      create ItemB(run_id, candidate.item_id) as b else ExistsB {{}}
      create ItemC(run_id, candidate.item_id) as c else ExistsC {{}}
      create ItemD(run_id, candidate.item_id) as d else ExistsD {{}}
    }}
    {root_effect}
    return Applied {{}}
  }}
}}
"#
    )
}

fn one_mutation_template(maximum: usize) -> String {
    METRICS.replace("1..1000", &format!("1..{maximum}"))
}

fn element_dependent_conflicts(maximum: usize) -> String {
    format!(
        r#"
contract ConflictBoundary version 1 {{
  entity Row {{
    key (tenant_id: uuid, row_id: u64)
  }}
  aggregate Rows {{
    root Row
    partition_by tenant_id
    conflict_key (tenant_id, row_id)
  }}
  bulk command PutRows {{
    input request_id: uuid
    input tenant_id: uuid
    input rows: list<Row, 1..{maximum}> aggregate_bytes <= 1048576
    idempotency_key request_id
    for row in rows {{
      create Row(tenant_id, row.row_id) as stored else Exists {{}}
    }}
    return Written {{}}
  }}
}}
"#
    )
}

#[test]
fn thousand_elements_plus_root_selects_v23_and_round_trips() {
    let bundle = compile_contract_source(METRICS).expect("1,000 elements plus root compile");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V23);
    assert_eq!(bundle.grammar_version(), GRAMMAR_VERSION_V23);
    assert_eq!(bundle.ir_version(), EXECUTABLE_IR_VERSION_V23);

    let plan = &bundle.commands()[0];
    assert_eq!(
        plan.collection_expansion().unwrap().maximum_elements(),
        1_000
    );
    assert_eq!(plan.maximum_mutation_instances(), 1_001);
    assert!(plan.requires_ir_v23());

    let decoded = ContractBundle::decode(bundle.canonical_bytes()).expect("V23 bundle decodes");
    assert_eq!(decoded.canonical_bytes(), bundle.canonical_bytes());
    assert_eq!(decoded.commands()[0].maximum_mutation_instances(), 1_001);
}

#[test]
fn v22_reader_identity_refuses_a_v23_high_cardinality_plan() {
    let bundle = compile_contract_source(METRICS).expect("V23 contract compiles");
    let mut bytes = bundle.canonical_bytes().to_vec();
    let version_offset = b"RIFFDB-BUNDLE\0".len();
    for version_index in 0..3 {
        let offset = version_offset + version_index * u32::BITS as usize / 8;
        bytes[offset..offset + 4].copy_from_slice(&22_u32.to_be_bytes());
    }
    assert!(
        ContractBundle::decode(&bytes).is_err(),
        "V22 must not reinterpret V23 collection bounds"
    );
}

#[test]
fn legacy_256_element_plan_keeps_its_pre_v23_identity() {
    let source = one_mutation_template(256)
        .replace(
            "mutate RunSummary(run_id) as summary else RunMissing {}",
            "read RunSummary(run_id) as summary else RunMissing {}",
        )
        .replace("set summary.revision = summary.revision + 1", "");
    let bundle = compile_contract_source(&source).expect("legacy cardinality compiles");
    let plan = &bundle.commands()[0];
    assert_eq!(bundle.ir_version(), 22);
    assert_eq!(plan.maximum_mutation_instances(), 256);
    assert!(!plan.requires_ir_v23());
}

#[test]
fn first_successor_element_and_mutation_counts_select_v23_independently() {
    let element_source = one_mutation_template(257)
        .replace(
            "mutate RunSummary(run_id) as summary else RunMissing {}",
            "read RunSummary(run_id) as summary else RunMissing {}",
        )
        .replace("set summary.revision = summary.revision + 1", "");
    let element_bundle =
        compile_contract_source(&element_source).expect("257 elements compile under V23");
    assert_eq!(element_bundle.ir_version(), EXECUTABLE_IR_VERSION_V23);
    assert_eq!(
        element_bundle.commands()[0].maximum_mutation_instances(),
        257
    );

    let mutation_source = one_mutation_template(256);
    let mutation_bundle =
        compile_contract_source(&mutation_source).expect("257 mutations compile under V23");
    assert_eq!(mutation_bundle.ir_version(), EXECUTABLE_IR_VERSION_V23);
    assert_eq!(
        mutation_bundle.commands()[0].maximum_mutation_instances(),
        257
    );
}

#[test]
fn element_ceiling_has_exact_source_spanned_diagnostic() {
    let source = one_mutation_template(1_025);
    let error = compile_contract_source(&source).expect_err("1,025 elements must fail");
    let diagnostic = &error.semantic().expect("semantic failure").as_slice()[0];
    assert_eq!(diagnostic.code(), CompilerDiagnosticCode::BoundExceeded);
    let bound = diagnostic.bound().expect("closed count observation");
    assert_eq!(
        bound.resource(),
        CompilerBoundResource::CollectionCommandElements
    );
    assert_eq!(bound.actual(), 1_025);
    assert_eq!(bound.maximum(), 1_024);
    let span = diagnostic.primary_span();
    assert_eq!(
        &source[span.start() as usize..span.end() as usize],
        "list<Metric, 1..1025>"
    );
}

#[test]
fn four_thousand_ninety_six_mutations_are_admitted() {
    let source = four_mutation_templates(1_024, false);
    let bundle = compile_contract_source(&source).expect("4,096 mutations compile");
    let plan = &bundle.commands()[0];
    assert_eq!(plan.maximum_mutation_instances(), 4_096);
    assert!(plan.requires_ir_v23());
}

#[test]
fn four_thousand_ninety_seven_mutations_fail_at_the_command_span() {
    let source = four_mutation_templates(1_024, true);
    let error = compile_contract_source(&source).expect_err("4,097 mutations must fail");
    let diagnostic = error
        .semantic()
        .expect("semantic failure")
        .as_slice()
        .iter()
        .find(|diagnostic| {
            diagnostic.bound().is_some_and(|bound| {
                bound.resource() == CompilerBoundResource::CollectionCommandMutationInstances
            })
        })
        .expect("closed mutation observation");
    assert_eq!(diagnostic.code(), CompilerDiagnosticCode::BoundExceeded);
    let bound = diagnostic.bound().unwrap();
    assert_eq!(bound.actual(), 4_097);
    assert_eq!(bound.maximum(), 4_096);
    let span = diagnostic.primary_span();
    assert!(source[span.start() as usize..span.end() as usize].starts_with("for candidate"));
}

#[test]
fn element_dependent_conflict_keys_retain_the_independent_256_ceiling() {
    let source = element_dependent_conflicts(1_000);
    let error = compile_contract_source(&source).expect_err("1,000 distinct conflicts must fail");
    let diagnostic = error
        .semantic()
        .expect("semantic failure")
        .as_slice()
        .iter()
        .find(|diagnostic| {
            diagnostic
                .bound()
                .is_some_and(|bound| bound.resource() == CompilerBoundResource::CommandConflictKeys)
        })
        .expect("closed conflict observation");
    assert_eq!(diagnostic.code(), CompilerDiagnosticCode::BoundExceeded);
    let bound = diagnostic.bound().unwrap();
    assert_eq!(bound.actual(), 1_000);
    assert_eq!(bound.maximum(), 256);
    let span = diagnostic.primary_span();
    assert!(source[span.start() as usize..span.end() as usize].starts_with("for row"));
}
