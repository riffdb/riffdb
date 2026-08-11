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
use riffdb_query_compiler::compile_query;
use riffdb_query_executor::{
    BoundPredicate, QueryExecutionError, QueryNearestPage, QueryParameters, QueryReadView,
    QueryRow, QueryScanPage, execute_in_snapshot,
};
use riffdb_query_ir::{MAX_QUERY_SCANNED_ROWS, QueryAccessKind, QueryAccessStep, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;
use riffdb_types::{CanonicalValue, CanonicalVector};

const CONTRACT: &str = r#"
contract Docs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: string<65536>
    vector_field embedding(3, cosine, (title, body), staleness_slo 60)
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

/// The same query with the organization-scope predicate replaced by a
/// non-partition predicate. (Omitting `where` entirely is already a parse
/// error, an even earlier unrepresentability.)
const SCOPELESS_NEAREST_QUERY: &str = r#"
query SimilarDocuments(
    $doc_id: Document.doc_id,
    $query_vec: Document.embedding,
) {
    many results from Document
        where doc_id == $doc_id
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
/// plan to execute (M7).
#[test]
fn scopeless_nearest_query_is_uncompilable() {
    let bundle = compile_contract_source(CONTRACT).expect("vector contract compiles");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let result = compile_query(
        &parse_query(SCOPELESS_NEAREST_QUERY).expect("parse"),
        &catalog,
    );
    assert!(
        result.is_err(),
        "a nearest query without an org-scope (partition) predicate must be rejected"
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
