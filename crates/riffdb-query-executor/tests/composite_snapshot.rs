//! Closed-program composite snapshot semantics.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_executor::{
    BoundPredicate, QueryContinuation, QueryExecutionError, QueryParameters, QueryReadView,
    QueryResultValue, QueryRow, QueryScanPage, execute_in_snapshot, execute_page_in_snapshot,
};
use riffdb_query_ir::QueryAccessStep;
use riffdb_query_ir::SymbolicCatalog;
use riffdb_riffql_syntax::parse_query;
use riffdb_types::{CanonicalValue, EnumTypeId, EnumVariantId, Timestamp};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const TICKET_PAGE: &str = include_str!("../../../queries/ticketdesk/ticket_page.riffq");
const PROJECT_MEMBERS: &str = include_str!("../../../queries/ticketdesk/project_members.riffq");
const OPEN_TICKETS: &str = r#"
query OpenTickets(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == TicketStatus.Open
        order by ticket_id asc
        take 5
    return Found { tickets: tickets { ticket_id status } }
    outcomes Found
}
"#;

struct FakeView {
    head: u64,
    rows: BTreeMap<String, Vec<QueryRow>>,
    point_calls: usize,
    scan_calls: usize,
    last_limit: Option<u64>,
    last_after: Option<Vec<u8>>,
    scan_epoch: u64,
    continue_first_scan: bool,
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
        limit: u64,
        after: Option<&[u8]>,
    ) -> Result<QueryScanPage, Self::Error> {
        self.scan_calls += 1;
        self.last_limit = Some(limit);
        self.last_after = after.map(<[u8]>::to_vec);
        let rows = self.rows.get(step.binding()).cloned().unwrap_or_default();
        if self.continue_first_scan && after.is_none() {
            QueryScanPage::continued(rows, self.scan_epoch, 1, vec![0x44]).ok_or(())
        } else {
            Ok(QueryScanPage::exact_end(rows, self.scan_epoch))
        }
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
        last_limit: None,
        last_after: None,
        scan_epoch: 7,
        continue_first_scan: false,
    };
    let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("snapshot");
    assert_eq!(snapshot.application_head(), 9);
    assert_eq!(snapshot.outcome(), "Found");
    assert_eq!(view.point_calls, 3);
    assert_eq!(view.scan_calls, 1);
    assert_eq!(view.last_limit, Some(50));
    assert_eq!(snapshot.index_epochs().get("Comment.by_ticket"), Some(&7));
    assert!(matches!(
        snapshot.fields().get("comments"),
        Some(QueryResultValue::Many(rows)) if rows.len() == 1
    ));
}

#[test]
fn continuation_resumes_the_named_binding_and_rejects_a_stale_epoch() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program =
        compile_query(&parse_query(PROJECT_MEMBERS).expect("query"), &catalog).expect("program");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
    ]))
    .expect("parameters");
    let membership = row(
        "ProjectMember",
        [
            ("organization_id", CanonicalValue::Uuid([1; 16])),
            ("project_id", CanonicalValue::Uuid([2; 16])),
            ("user_id", CanonicalValue::Uuid([3; 16])),
            ("role", CanonicalValue::string("member").expect("role")),
        ],
    );
    let mut first_view = FakeView {
        head: 11,
        rows: BTreeMap::from([("memberships".to_owned(), vec![membership.clone()])]),
        point_calls: 0,
        scan_calls: 0,
        last_limit: None,
        last_after: None,
        scan_epoch: 7,
        continue_first_scan: true,
    };
    let first = execute_in_snapshot(&program, &parameters, &mut first_view).expect("first page");
    assert_eq!(first.continuation_binding(), Some("memberships"));
    let cursor = QueryContinuation::checked(
        first
            .continuation_binding()
            .expect("continuation binding")
            .to_owned(),
        first.continuation().expect("continuation").to_vec(),
        first.index_epochs().clone(),
    )
    .expect("cursor");

    let mut second_view = FakeView {
        head: 12,
        rows: BTreeMap::from([("memberships".to_owned(), vec![membership.clone()])]),
        point_calls: 0,
        scan_calls: 0,
        last_limit: None,
        last_after: None,
        scan_epoch: 7,
        continue_first_scan: false,
    };
    let second = execute_page_in_snapshot(&program, &parameters, Some(&cursor), &mut second_view)
        .expect("second page");
    assert_eq!(second.application_head(), 12);
    assert_eq!(second_view.last_after.as_deref(), Some(&[0x44][..]));

    let mut stale_view = FakeView {
        head: 13,
        rows: BTreeMap::from([("memberships".to_owned(), vec![membership])]),
        point_calls: 0,
        scan_calls: 0,
        last_limit: None,
        last_after: None,
        scan_epoch: 8,
        continue_first_scan: false,
    };
    assert_eq!(
        execute_page_in_snapshot(&program, &parameters, Some(&cursor), &mut stale_view,),
        Err(QueryExecutionError::StaleCursor)
    );
}

#[test]
fn exact_enum_symbols_execute_as_internal_canonical_values() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let status = catalog.enumeration("TicketStatus").expect("status enum");
    let open = CanonicalValue::Enum {
        type_id: status.internal_id(),
        variant_id: status.variant("Open").expect("Open"),
    };
    let program =
        compile_query(&parse_query(OPEN_TICKETS).expect("query"), &catalog).expect("program");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
    ]))
    .expect("parameters");
    let mut view = FakeView {
        head: 14,
        rows: BTreeMap::from([(
            "tickets".to_owned(),
            vec![row(
                "Ticket",
                [
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("project_id", CanonicalValue::Uuid([2; 16])),
                    ("ticket_id", CanonicalValue::Uuid([3; 16])),
                    ("status", open),
                ],
            )],
        )]),
        point_calls: 0,
        scan_calls: 0,
        last_limit: None,
        last_after: None,
        scan_epoch: 9,
        continue_first_scan: false,
    };
    let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("snapshot");
    assert!(matches!(
        snapshot.fields().get("tickets"),
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
