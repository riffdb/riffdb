//! Closed-program composite snapshot semantics.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_executor::{
    BoundPredicate, QueryExecutionError, QueryParameters, QueryReadView, QueryResultValue,
    QueryRow, QueryScanPage, execute_in_snapshot,
};
use riffdb_query_ir::QueryAccessStep;
use riffdb_query_ir::SymbolicCatalog;
use riffdb_riffql_syntax::parse_query;
use riffdb_types::{CanonicalValue, EnumTypeId, EnumVariantId, Timestamp};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const TICKET_PAGE: &str = include_str!("../../../queries/ticketdesk/ticket_page.riffq");

struct FakeView {
    head: u64,
    rows: BTreeMap<String, Vec<QueryRow>>,
    point_calls: usize,
    scan_calls: usize,
}

impl QueryReadView for FakeView {
    type Error = ();

    fn application_head(&self) -> u64 {
        self.head
    }

    fn point(
        &mut self,
        step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
    ) -> Result<Option<QueryRow>, Self::Error> {
        self.point_calls += 1;
        Ok(self
            .rows
            .get(step.binding())
            .and_then(|rows| rows.first())
            .cloned())
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
    ) -> Result<QueryScanPage, Self::Error> {
        self.scan_calls += 1;
        Ok(QueryScanPage::exact_end(
            self.rows.get(step.binding()).cloned().unwrap_or_default(),
            7,
        ))
    }
}

#[test]
fn all_accesses_use_one_owned_snapshot_and_respect_cardinality() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program =
        compile_query(&parse_query(TICKET_PAGE).expect("query"), &catalog).expect("program");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("ticket_id".to_owned(), CanonicalValue::Uuid([2; 16])),
    ]))
    .expect("parameters");
    let timestamp = CanonicalValue::Timestamp(Timestamp::new(10, 0).expect("timestamp"));
    let mut view = FakeView {
        head: 9,
        rows: BTreeMap::from([
            (
                "ticket".to_owned(),
                vec![row(
                    "Ticket",
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("ticket_id", CanonicalValue::Uuid([2; 16])),
                        ("reporter_id", CanonicalValue::Uuid([3; 16])),
                        ("assignee_id", CanonicalValue::Uuid([4; 16])),
                        (
                            "status",
                            CanonicalValue::Enum {
                                type_id: EnumTypeId::first(),
                                variant_id: EnumVariantId::first(),
                            },
                        ),
                        ("title", CanonicalValue::string("ticket").expect("text")),
                        ("created_at", timestamp.clone()),
                        ("updated_at", timestamp.clone()),
                    ],
                )],
            ),
            (
                "reporter".to_owned(),
                vec![row(
                    "AppUser",
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("user_id", CanonicalValue::Uuid([3; 16])),
                        (
                            "display_name",
                            CanonicalValue::string("reporter").expect("text"),
                        ),
                    ],
                )],
            ),
            (
                "assignee".to_owned(),
                vec![row(
                    "AppUser",
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("user_id", CanonicalValue::Uuid([4; 16])),
                        (
                            "display_name",
                            CanonicalValue::string("assignee").expect("text"),
                        ),
                    ],
                )],
            ),
            (
                "comments".to_owned(),
                vec![row(
                    "Comment",
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("ticket_id", CanonicalValue::Uuid([2; 16])),
                        ("comment_id", CanonicalValue::Uuid([5; 16])),
                        ("author_id", CanonicalValue::Uuid([3; 16])),
                        ("body", CanonicalValue::string("body").expect("text")),
                        ("created_at", timestamp),
                    ],
                )],
            ),
        ]),
        point_calls: 0,
        scan_calls: 0,
    };
    let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("snapshot");
    assert_eq!(snapshot.application_head(), 9);
    assert_eq!(snapshot.outcome(), "Found");
    assert_eq!(view.point_calls, 3);
    assert_eq!(view.scan_calls, 1);
    assert_eq!(snapshot.index_epochs().get("Comment.by_ticket"), Some(&7));
    assert!(matches!(
        snapshot.fields().get("comments"),
        Some(QueryResultValue::Many(rows)) if rows.len() == 1
    ));
}

#[test]
fn closed_errors_do_not_debug_business_values() {
    let error = QueryExecutionError::MissingParameter {
        parameter: "organization_id".to_owned(),
    };
    assert!(!format!("{error:?}").contains("[1, 1"));
    let _execute = execute_in_snapshot::<FakeView>;
}

fn row<const N: usize>(entity: &str, fields: [(&str, CanonicalValue); N]) -> QueryRow {
    QueryRow::checked(
        entity.to_owned(),
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
    .expect("row")
}
