#![forbid(unsafe_code)]

//! Nearest-step compilation shape, mandatory organization scope, and honest
//! scan-fuel accounting (VEC-005/VEC-006, fix round M7/S1).
//!
//! The WP-593 wiring landed with no test on this path: the compiler charged
//! K as the scan cost and the executor charged no fuel at all for a nearest
//! step. These tests pin the honest accounting on both sides and prove a
//! scopeless nearest query is uncompilable.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{PlannerDiagnosticCode, compile_query};
use riffdb_query_executor::{
    BoundPredicate, QueryExecutionError, QueryNearestPage, QueryParameters, QueryReadView,
    QueryRow, QueryScanPage, execute_in_snapshot,
};
use riffdb_query_ir::{
    MAX_QUERY_SCANNED_ROWS, QueryAccessKind, QueryAccessStep, QueryDiagnosticCode, SymbolicCatalog,
    resolve_query_surface,
};
use riffdb_riffql_syntax::{Span, parse_query};
use riffdb_types::{CanonicalValue, CanonicalVector};

const CONTRACT: &str = r#"
contract Docs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: string<65536>
    field nearest: u64
    vector_field embedding(3, cosine, (title, body), staleness_slo 60)
    index by_doc (org_id, doc_id)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
}
"#;

const NEAREST_QUERY: &str = r#"
query SimilarDocuments(
    $org_id: Document.org_id,
    $query_vec: Document.embedding,
) {
    many results from Document
        where org_id == $org_id
        nearest(embedding, $query_vec, 10)
    return Found { results: results { title } }
    outcomes Found
}
"#;

/// The same query as `NEAREST_QUERY` with EXACTLY ONE difference: the
/// predicate field is `doc_id` instead of the partition field `org_id`. The
/// parameter list, nearest clause, return shape, and outcomes are identical,
/// and `doc_id == $org_id` type-checks (both sides are uuid) — so the ONLY
/// check that can reject this query is the partition-route check, and the
/// test below asserts its typed code and span rather than a bare `is_err()`.
/// (Omitting `where` entirely is already a parse error, an even earlier
/// unrepresentability.)
const SCOPELESS_NEAREST_QUERY: &str = r#"
query SimilarDocuments(
    $org_id: Document.org_id,
    $query_vec: Document.embedding,
) {
    many results from Document
        where doc_id == $org_id
        nearest(embedding, $query_vec, 10)
    return Found { results: results { title } }
    outcomes Found
}
"#;

fn nearest_program() -> riffdb_query_ir::QueryAccessProgramV1 {
    let bundle = compile_contract_source(CONTRACT).expect("vector contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    compile_query(&parse_query(NEAREST_QUERY).expect("parse"), &catalog).expect("program")
}

/// The compiled plan carries the Nearest access kind with the declared K,
/// and the program-level organization scope every access kind shares: a
/// non-empty partition parameter routing the whole query to one org.
#[test]
fn nearest_program_carries_k_and_the_mandatory_partition_parameter() {
    let program = nearest_program();
    assert!(
        !program.partition_parameter().is_empty(),
        "every program carries exactly one partition (org) parameter"
    );
    assert_eq!(program.partition_parameter(), "org_id");
    let step = &program.steps()[0];
    match step.access() {
        QueryAccessKind::Nearest {
            vector_field,
            vector_parameter,
            k,
        } => {
            assert_eq!(vector_field, "embedding");
            assert_eq!(vector_parameter, "query_vec");
            assert_eq!(*k, 10);
        }
        other => panic!("expected a Nearest access kind, found {other:?}"),
    }
}

/// VEC-006: organization scope is unrepresentable to omit. A nearest query
/// without the partition predicate does not compile — there is no scopeless
/// plan to execute (M7). The query differs from the compilable control in
/// its predicate field ONLY, and the assertion pins the partition-route
/// diagnostic (`NonLocal`) and its span: a rejection from any other cause
/// (parameter resolution, index selection, typing) fails this test.
#[test]
fn scopeless_nearest_query_is_uncompilable() {
    let bundle = compile_contract_source(CONTRACT).expect("vector contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let diagnostics = compile_query(
        &parse_query(SCOPELESS_NEAREST_QUERY).expect("parse"),
        &catalog,
    )
    .expect_err("a nearest query without an org-scope (partition) predicate must be rejected");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::NonLocal);
    assert_eq!(
        diagnostic.summary(),
        "query access is not routed by an exact partition parameter"
    );
    let predicate = "doc_id == $org_id";
    let start = SCOPELESS_NEAREST_QUERY
        .find(predicate)
        .expect("predicate text present") as u32;
    assert_eq!(
        diagnostic.primary(),
        Span {
            start,
            end: start + predicate.len() as u32,
        },
        "the diagnostic points at the non-partition predicate"
    );
}

/// S1 static side: a nearest step charges the partition-scan ceiling as its
/// scan cost, not K. Charging K under-billed a whole-partition scan by
/// orders of magnitude.
#[test]
fn nearest_static_cost_charges_the_partition_scan_ceiling_not_k() {
    let program = nearest_program();
    assert_eq!(
        program.cost().scanned_index_rows(),
        MAX_QUERY_SCANNED_ROWS,
        "static scan charge must be the partition-scan ceiling"
    );
    assert!(program.cost().scanned_index_rows() > 10, "and never just K");
}

struct NearestView {
    rows: Vec<QueryRow>,
    scanned_rows: u64,
    reported_calls: usize,
}

impl QueryReadView for NearestView {
    type Error = ();

    fn fault(&self, (): &Self::Error) -> riffdb_query_executor::QueryBackendFault {
        riffdb_query_executor::QueryBackendFault::Integrity
    }

    fn application_head(&self) -> u64 {
        1
    }

    fn point(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, Self::Error> {
        Err(())
    }

    fn dependent_point_batch(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[Vec<BoundPredicate>],
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        Err(())
    }

    fn scan(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _limit: u64,
        _after: Option<&[u8]>,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        Err(())
    }

    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        self.reported_calls += 1;
        Ok(QueryNearestPage {
            rows: self.rows.clone(),
            scanned_rows: self.scanned_rows,
        })
    }
}

fn parameters() -> QueryParameters {
    QueryParameters::checked(BTreeMap::from([
        ("org_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        (
            "query_vec".to_owned(),
            CanonicalValue::Vector(CanonicalVector::new(vec![1.0, 0.0, 0.0]).expect("finite")),
        ),
    ]))
    .expect("parameters")
}

fn result_row() -> QueryRow {
    QueryRow::checked(
        "Document".to_owned(),
        BTreeMap::from([(
            "title".to_owned(),
            CanonicalValue::string("doc").expect("s"),
        )]),
    )
    .expect("row")
}

/// S1 runtime side (within budget): the adapter's honest scan count is
/// charged against fuel and the query succeeds.
#[test]
fn nearest_execution_charges_reported_scan_work_and_succeeds_within_budget() {
    let program = nearest_program();
    let mut view = NearestView {
        rows: vec![result_row()],
        scanned_rows: MAX_QUERY_SCANNED_ROWS,
        reported_calls: 0,
    };
    let snapshot = execute_in_snapshot(&program, &parameters(), &mut view)
        .expect("a full-ceiling scan is funded by the honest static charge");
    assert_eq!(view.reported_calls, 1);
    drop(snapshot);
}

/// S1 runtime side (beyond budget): an adapter reporting a scan above the
/// physical ceiling is a typed refusal, never accepted silently.
#[test]
fn nearest_execution_refuses_scans_beyond_the_ceiling() {
    let program = nearest_program();
    let mut view = NearestView {
        rows: vec![result_row()],
        scanned_rows: MAX_QUERY_SCANNED_ROWS + 1,
        reported_calls: 0,
    };
    let error = execute_in_snapshot(&program, &parameters(), &mut view)
        .expect_err("an over-ceiling scan must be refused");
    assert!(matches!(error, QueryExecutionError::BoundExceeded));
}

/// More rows than K (or than the scan itself examined) is a bound violation.
#[test]
fn nearest_execution_refuses_more_rows_than_k() {
    let program = nearest_program();
    let mut view = NearestView {
        rows: (0..11).map(|_| result_row()).collect(),
        scanned_rows: 11,
        reported_calls: 0,
    };
    let error = execute_in_snapshot(&program, &parameters(), &mut view)
        .expect_err("more rows than K must be refused");
    assert!(matches!(error, QueryExecutionError::BoundExceeded));
}

// ─── K ceiling (S2/N14): K inherits the 499 page-take ceiling ───

fn nearest_query_with_k(k: &str) -> String {
    NEAREST_QUERY.replace(
        "nearest(embedding, $query_vec, 10)",
        &format!("nearest(embedding, $query_vec, {k})"),
    )
}

/// K at the ceiling compiles: 499 is the maximum page bound, and the
/// compiled step carries it verbatim.
#[test]
fn nearest_k_at_the_499_ceiling_compiles() {
    let bundle = compile_contract_source(CONTRACT).expect("vector contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program = compile_query(
        &parse_query(&nearest_query_with_k("499")).expect("parse"),
        &catalog,
    )
    .expect("k = 499 is within the page-take ceiling");
    match program.steps()[0].access() {
        QueryAccessKind::Nearest { k, .. } => assert_eq!(*k, 499),
        other => panic!("expected a Nearest access kind, found {other:?}"),
    }
}

/// K one past the ceiling is refused with a typed diagnostic at the K token
/// (previously enforced but unpinned: every nearest test used k = 10, so
/// nothing would red if the nearest binding stopped inheriting the ceiling).
#[test]
fn nearest_k_over_the_499_ceiling_is_a_typed_refusal() {
    let bundle = compile_contract_source(CONTRACT).expect("vector contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let source = nearest_query_with_k("500");
    let document = parse_query(&source).expect("parse");

    // The resolver names the exact bound, code, and K-token span.
    let diagnostics =
        resolve_query_surface(&document, &catalog).expect_err("k = 500 must be refused");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), QueryDiagnosticCode::ArtifactLimit);
    assert_eq!(
        diagnostic.summary(),
        "nearest k exceeds the maximum page bound of 499 (K is the binding's checked page bound and shares the take ceiling)"
    );
    let start = source.find(", 500)").expect("k literal present") as u32 + 2;
    assert_eq!(
        diagnostic.primary(),
        Span {
            start,
            end: start + 3,
        },
        "the diagnostic points at the K literal"
    );

    // The public front door refuses the same query.
    assert!(
        compile_query(&document, &catalog).is_err(),
        "compile_query must refuse k = 500"
    );

    // And a colossal K is refused identically, not accepted with saturation.
    assert!(
        compile_query(
            &parse_query(&nearest_query_with_k("1000000")).expect("parse"),
            &catalog,
        )
        .is_err(),
        "compile_query must refuse k = 1_000_000"
    );
}

/// K = 0 never reaches the resolver: it is a parse error.
#[test]
fn nearest_k_zero_is_rejected_at_parse() {
    assert!(
        parse_query(&nearest_query_with_k("0")).is_err(),
        "k = 0 must be rejected at parse"
    );
}

/// A contract field named `nearest` is queryable end-to-end. The contract
/// language never reserved the word, so `field nearest: u64` is legal;
/// RiffQL previously hard-reserved it, making the field unnameable in any
/// query — the one genuine contract/query keyword collision.
#[test]
fn contract_field_named_nearest_is_queryable() {
    let bundle = compile_contract_source(CONTRACT).expect("vector contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let source = r#"
query NearestField(
    $org_id: Document.org_id,
    $doc_id: Document.doc_id,
) {
    one doc from Document
        where org_id == $org_id && doc_id == $doc_id
        else NotFound
    return Found { doc: doc { nearest } }
    outcomes Found | NotFound
}
"#;
    compile_query(&parse_query(source).expect("parse"), &catalog)
        .expect("a field named nearest compiles through the full query pipeline");
}

// ─── Fuel charge (S1/N7): examined rows are charged against shared scan fuel ───

/// A two-step query: one indexed listing page plus one nearest step. The
/// static cost funds the listing at its take-derived budget and the nearest
/// step at the partition-scan ceiling; both runtime reports are charged
/// against the SAME scan-fuel pool.
const LISTING_AND_NEAREST_QUERY: &str = r#"
query ListingAndSimilar(
    $org_id: Document.org_id,
    $query_vec: Document.embedding,
) {
    many listing from Document
        where org_id == $org_id
        order by doc_id asc
        take 5
    many results from Document
        where org_id == $org_id
        nearest(embedding, $query_vec, 10)
    return Found { listing: listing { title }, results: results { title } }
    outcomes Found
}
"#;

/// Answers both the indexed scan and the nearest step with configurable
/// self-reported scan work.
struct TwoStepView {
    scan_scanned: u64,
    nearest_scanned: u64,
}

impl QueryReadView for TwoStepView {
    type Error = ();

    fn fault(&self, (): &Self::Error) -> riffdb_query_executor::QueryBackendFault {
        riffdb_query_executor::QueryBackendFault::Integrity
    }

    fn application_head(&self) -> u64 {
        1
    }

    fn point(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
    ) -> Result<Option<QueryRow>, Self::Error> {
        Err(())
    }

    fn dependent_point_batch(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[Vec<BoundPredicate>],
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        Err(())
    }

    fn scan(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _limit: u64,
        _after: Option<&[u8]>,
    ) -> Result<QueryScanPage, Self::Error> {
        QueryScanPage::reported(vec![result_row()], 1, self.scan_scanned, 1, None).ok_or(())
    }

    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
    ) -> Result<QueryNearestPage, Self::Error> {
        Ok(QueryNearestPage {
            rows: vec![result_row()],
            scanned_rows: self.nearest_scanned,
        })
    }
}

fn listing_and_nearest_program() -> riffdb_query_ir::QueryAccessProgramV1 {
    let bundle = compile_contract_source(CONTRACT).expect("vector contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    compile_query(
        &parse_query(LISTING_AND_NEAREST_QUERY).expect("parse"),
        &catalog,
    )
    .expect("program")
}

/// S1/N7 runtime side, the charge itself: the nearest step's examined rows
/// are genuinely burned against the shared scan fuel. Each step's report is
/// individually within the per-page ceiling — the refusal guard passes both
/// times — but the CUMULATIVE charge exceeds the plan-funded budget, so the
/// execution must die of `FuelExhausted`. Deleting the nearest arm's
/// `fuel.scans(page.scanned_rows)` makes this execution succeed, which is
/// exactly the deletion the previous refusal-only tests could not observe.
#[test]
fn nearest_scan_work_is_charged_against_shared_fuel() {
    let program = listing_and_nearest_program();
    // Precondition for the arithmetic below: the plan funds strictly less
    // than two full-ceiling scans (listing is funded at its take-derived
    // budget, nearest at the ceiling).
    assert!(
        program.cost().scanned_index_rows() + program.cost().access_steps()
            < 2 * MAX_QUERY_SCANNED_ROWS,
        "plan must fund less than two full-ceiling scans"
    );
    let mut view = TwoStepView {
        scan_scanned: MAX_QUERY_SCANNED_ROWS,
        nearest_scanned: MAX_QUERY_SCANNED_ROWS,
    };
    let error = execute_in_snapshot(&program, &parameters(), &mut view)
        .expect_err("two guard-passing full-ceiling reports must exhaust the shared scan fuel");
    assert!(
        matches!(error, QueryExecutionError::FuelExhausted),
        "expected FuelExhausted, found {error:?}"
    );
}

/// The complement: honest per-step reports fit the same budget, so the
/// charge is not an overcount.
#[test]
fn honest_two_step_reports_stay_within_the_funded_budget() {
    let program = listing_and_nearest_program();
    let mut view = TwoStepView {
        scan_scanned: 1,
        nearest_scanned: MAX_QUERY_SCANNED_ROWS,
    };
    execute_in_snapshot(&program, &parameters(), &mut view)
        .expect("honest reports are funded by the static charge");
}
