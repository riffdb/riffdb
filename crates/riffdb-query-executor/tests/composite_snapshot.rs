//! Closed-program composite snapshot semantics.

use std::collections::BTreeMap;
use std::time::Instant;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_executor::{
    BoundPredicate, QueryContinuation, QueryExecutionError, QueryNearestPage, QueryParameters,
    QueryReadView, QueryResultValue, QueryRow, QueryScanPage, bind_live_query_dependencies,
    execute_in_snapshot, execute_page_in_snapshot,
};
use riffdb_query_ir::{
    LiveInvalidationPrecisionV1, LiveQueryPlanError, LiveQueryPlanV1, LiveUpdateStrategyV1,
    QueryAccessStep, ReactiveUpdateModeV1, SymbolicCatalog,
};
use riffdb_riffql_syntax::parse_query;
use riffdb_types::{CanonicalValue, EnumTypeId, EnumVariantId, Timestamp};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const TICKET_PAGE: &str = include_str!("../../../queries/ticketdesk/ticket_page.riffq");
const TICKET_PAGE_PAGED: &str = include_str!("../../../queries/ticketdesk/ticket_page_paged.riffq");
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
const EXACT_TICKET: &str = r#"
query ExactTicket(
    $organization_id: Organization.organization_id,
    $ticket_id: Ticket.ticket_id,
) {
    one ticket from Ticket
        where organization_id == $organization_id
            && ticket_id == $ticket_id
        else NotFound
    return Found { ticket: ticket { organization_id ticket_id status } }
    outcomes Found | NotFound
}
"#;
const PATCHABLE_TICKETS: &str = r#"
query PatchableTickets(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == TicketStatus.Open
        order by ticket_id asc
        take 5
    return Found { tickets: tickets { organization_id ticket_id status } }
    outcomes Found
}
"#;

fn contract_without_cover() -> String {
    CONTRACT.replace(" cover (title, reporter_id, assignee_id)", "")
}

struct FakeView {
    head: u64,
    rows: BTreeMap<String, Vec<QueryRow>>,
    point_calls: usize,
    batch_calls: usize,
    missing_batch_target: bool,
    scan_calls: usize,
    last_limit: Option<u64>,
    last_after: Option<Vec<u8>>,
    scan_epoch: u64,
    continue_first_scan: bool,
}

#[test]
fn live_plans_bind_exact_points_and_keep_scans_conservative() {
    let bundle = compile_contract_source(&contract_without_cover()).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let exact =
        compile_query(&parse_query(EXACT_TICKET).expect("query"), &catalog).expect("exact program");
    let patchable = compile_query(&parse_query(PATCHABLE_TICKETS).expect("query"), &catalog)
        .expect("patchable program");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
        ("ticket_id".to_owned(), CanonicalValue::Uuid([3; 16])),
    ]))
    .expect("parameters");

    let exact_plan =
        LiveQueryPlanV1::derive(&exact, ReactiveUpdateModeV1::Reset, &[]).expect("exact live plan");
    assert_eq!(
        exact_plan.dependencies()[0].precision(),
        LiveInvalidationPrecisionV1::ExactEntity
    );
    let exact_dependencies =
        bind_live_query_dependencies(&exact, &parameters).expect("bound exact dependency");
    assert!(exact_dependencies[0].entity_key().is_some());

    let patch_plan = LiveQueryPlanV1::derive(
        &patchable,
        ReactiveUpdateModeV1::Patch,
        &["organization_id".to_owned(), "ticket_id".to_owned()],
    )
    .expect("keyed patch plan");
    assert_eq!(
        patch_plan.dependencies()[0].precision(),
        LiveInvalidationPrecisionV1::PartitionEntity
    );
    assert!(matches!(
        patch_plan.update_strategy(),
        LiveUpdateStrategyV1::Patch { result_field, key_fields }
            if result_field == "tickets"
                && key_fields == &["organization_id".to_owned(), "ticket_id".to_owned()]
    ));
    let scan_dependencies =
        bind_live_query_dependencies(&patchable, &parameters).expect("bound scan dependency");
    assert!(scan_dependencies[0].entity_key().is_none());
    assert_eq!(
        scan_dependencies[0].partition_hash(),
        exact_dependencies[0].partition_hash()
    );

    let complete = compile_query(&parse_query(TICKET_PAGE).expect("query"), &catalog)
        .expect("complete watchable program");
    assert!(LiveQueryPlanV1::derive(&complete, ReactiveUpdateModeV1::Reset, &[]).is_ok());

    let paginated = compile_query(&parse_query(TICKET_PAGE_PAGED).expect("query"), &catalog)
        .expect("paginated program");
    assert_eq!(
        LiveQueryPlanV1::derive(&paginated, ReactiveUpdateModeV1::Reset, &[]),
        Err(LiveQueryPlanError::PaginatedQuery)
    );
    assert_eq!(
        LiveQueryPlanV1::derive(
            &patchable,
            ReactiveUpdateModeV1::Patch,
            &["not_selected".to_owned()],
        ),
        Err(LiveQueryPlanError::InvalidPatchShape)
    );
}

impl QueryReadView for FakeView {
    type Error = ();

    fn fault(&self, _error: &Self::Error) -> riffdb_query_executor::QueryBackendFault {
        riffdb_query_executor::QueryBackendFault::Unavailable
    }

    fn application_head(&self) -> u64 {
        self.head
    }

    fn point(
        &mut self,
        step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, Self::Error> {
        self.point_calls += 1;
        Ok(self
            .rows
            .get(step.binding())
            .and_then(|rows| rows.first())
            .cloned())
    }

    fn dependent_point_batch(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        self.batch_calls += 1;
        if self.missing_batch_target {
            return Ok((0..predicates.len()).map(|_| None).collect());
        }
        Ok(self
            .rows
            .get(step.binding())
            .into_iter()
            .flatten()
            .cloned()
            .map(Some)
            .collect())
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
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

    fn nearest(
        &mut self,
        step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        let rows = self.rows.get(step.binding()).cloned().unwrap_or_default();
        let scanned_rows = rows.len() as u64;
        Ok(QueryNearestPage { rows, scanned_rows })
    }
}

#[test]
fn all_accesses_use_one_owned_snapshot_and_respect_cardinality() {
    let bundle = compile_contract_source(&contract_without_cover()).expect("contract");
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
                        ("project_id", CanonicalValue::Uuid([6; 16])),
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
                "project".to_owned(),
                vec![row(
                    "Project",
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("project_id", CanonicalValue::Uuid([6; 16])),
                        ("name", CanonicalValue::string("project").expect("text")),
                    ],
                )],
            ),
            (
                "organization".to_owned(),
                vec![row(
                    "Organization",
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        (
                            "name",
                            CanonicalValue::string("organization").expect("text"),
                        ),
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
            (
                "ticket_labels".to_owned(),
                vec![row(
                    "TicketLabel",
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("ticket_id", CanonicalValue::Uuid([2; 16])),
                        ("label_id", CanonicalValue::Uuid([7; 16])),
                    ],
                )],
            ),
            (
                "labels".to_owned(),
                vec![row(
                    "Label",
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("label_id", CanonicalValue::Uuid([7; 16])),
                        ("name", CanonicalValue::string("urgent").expect("text")),
                    ],
                )],
            ),
        ]),
        point_calls: 0,
        batch_calls: 0,
        missing_batch_target: false,
        scan_calls: 0,
        last_limit: None,
        last_after: None,
        scan_epoch: 7,
        continue_first_scan: false,
    };
    let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("snapshot");
    assert_eq!(snapshot.application_head(), 9);
    assert_eq!(snapshot.outcome(), "Found");
    assert_eq!(view.point_calls, 5);
    assert_eq!(view.batch_calls, 1);
    assert_eq!(view.scan_calls, 2);
    assert_eq!(view.last_limit, Some(50));
    assert_eq!(snapshot.index_epochs().get("Comment.by_ticket"), Some(&7));
    assert!(matches!(
        snapshot.fields().get("comments"),
        Some(QueryResultValue::Many(rows)) if rows.len() == 1
    ));
    assert!(matches!(
        snapshot.fields().get("labels"),
        Some(QueryResultValue::Many(rows)) if rows.len() == 1
    ));

    view.missing_batch_target = true;
    let missing = execute_in_snapshot(&program, &parameters, &mut view).expect("declared outcome");
    assert_eq!(missing.outcome(), "IntegrityFailure");
    assert!(missing.fields().is_empty());

    view.missing_batch_target = false;
    view.rows.insert("ticket_labels".to_owned(), Vec::new());
    view.rows.insert("labels".to_owned(), Vec::new());
    let empty = execute_in_snapshot(&program, &parameters, &mut view).expect("empty collection");
    assert!(matches!(
        empty.fields().get("labels"),
        Some(QueryResultValue::Many(rows)) if rows.is_empty()
    ));

    let ticket_label = |label_id| {
        row(
            "TicketLabel",
            [
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([2; 16])),
                ("label_id", label_id),
            ],
        )
    };
    view.rows.insert(
        "ticket_labels".to_owned(),
        vec![
            ticket_label(CanonicalValue::Uuid([7; 16])),
            ticket_label(CanonicalValue::Uuid([7; 16])),
        ],
    );
    assert!(matches!(
        execute_in_snapshot(&program, &parameters, &mut view),
        Err(QueryExecutionError::InvalidDependentKey { .. })
    ));
    view.rows.insert(
        "ticket_labels".to_owned(),
        vec![ticket_label(CanonicalValue::Null)],
    );
    assert!(matches!(
        execute_in_snapshot(&program, &parameters, &mut view),
        Err(QueryExecutionError::InvalidDependentKey { .. })
    ));
    view.rows.insert(
        "ticket_labels".to_owned(),
        vec![
            ticket_label(CanonicalValue::Uuid([8; 16])),
            ticket_label(CanonicalValue::Uuid([7; 16])),
        ],
    );
    assert!(matches!(
        execute_in_snapshot(&program, &parameters, &mut view),
        Err(QueryExecutionError::InvalidDependentKey { .. })
    ));
    view.rows.insert(
        "ticket_labels".to_owned(),
        (1_u8..=50)
            .map(|value| ticket_label(CanonicalValue::Uuid([value; 16])))
            .collect(),
    );
    view.rows.insert(
        "labels".to_owned(),
        (1_u8..=50)
            .map(|value| {
                row(
                    "Label",
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("label_id", CanonicalValue::Uuid([value; 16])),
                        (
                            "name",
                            CanonicalValue::string(format!("label-{value}")).expect("label name"),
                        ),
                    ],
                )
            })
            .collect(),
    );
    let maximum = execute_in_snapshot(&program, &parameters, &mut view).expect("maximum batch");
    assert!(matches!(
        maximum.fields().get("labels"),
        Some(QueryResultValue::Many(rows)) if rows.len() == 50
    ));

    view.rows.insert(
        "ticket_labels".to_owned(),
        (0_u8..51)
            .map(|value| ticket_label(CanonicalValue::Uuid([value; 16])))
            .collect(),
    );
    assert_eq!(
        execute_in_snapshot(&program, &parameters, &mut view),
        Err(QueryExecutionError::BoundExceeded)
    );
}

#[test]
fn continuation_resumes_the_named_binding_and_rejects_a_stale_epoch() {
    let bundle = compile_contract_source(&contract_without_cover()).expect("contract");
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
        batch_calls: 0,
        missing_batch_target: false,
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
        batch_calls: 0,
        missing_batch_target: false,
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
        batch_calls: 0,
        missing_batch_target: false,
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
    let bundle = compile_contract_source(&contract_without_cover()).expect("contract");
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
        batch_calls: 0,
        missing_batch_target: false,
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

struct ReportedWorkView {
    row: QueryRow,
    scanned_rows: u64,
    point_reads: u64,
    continuation: bool,
}

impl QueryReadView for ReportedWorkView {
    type Error = ();

    fn fault(&self, _error: &Self::Error) -> riffdb_query_executor::QueryBackendFault {
        riffdb_query_executor::QueryBackendFault::Unavailable
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
        QueryScanPage::reported(
            vec![self.row.clone()],
            1,
            self.scanned_rows,
            self.point_reads,
            self.continuation.then(|| vec![0x55]),
        )
        .ok_or(())
    }

    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        Err(())
    }
}

#[test]
fn backend_work_is_reconciled_with_whole_query_fuel_before_release() {
    let bundle = compile_contract_source(&contract_without_cover()).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program =
        compile_query(&parse_query(OPEN_TICKETS).expect("query"), &catalog).expect("program");
    assert_eq!(program.cost().scanned_index_rows(), 5);
    let status = catalog.enumeration("TicketStatus").expect("status enum");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
    ]))
    .expect("parameters");
    let ticket = row(
        "Ticket",
        [
            ("organization_id", CanonicalValue::Uuid([1; 16])),
            ("project_id", CanonicalValue::Uuid([2; 16])),
            ("ticket_id", CanonicalValue::Uuid([3; 16])),
            (
                "status",
                CanonicalValue::Enum {
                    type_id: status.internal_id(),
                    variant_id: status.variant("Open").expect("Open"),
                },
            ),
        ],
    );

    let mut exact = ReportedWorkView {
        row: ticket.clone(),
        scanned_rows: 5,
        point_reads: 1,
        continuation: false,
    };
    assert!(execute_in_snapshot(&program, &parameters, &mut exact).is_ok());

    // Legitimate continuation peek: scan limit+1 against plan limit of 5.
    let mut peek = ReportedWorkView {
        row: ticket.clone(),
        scanned_rows: 6,
        point_reads: 1,
        continuation: true,
    };
    assert!(
        execute_in_snapshot(&program, &parameters, &mut peek).is_ok(),
        "limit+1 continuation peek is within runtime scan headroom"
    );

    let mut one_over = ReportedWorkView {
        row: ticket.clone(),
        scanned_rows: 7,
        point_reads: 1,
        continuation: true,
    };
    assert_eq!(
        execute_in_snapshot(&program, &parameters, &mut one_over),
        Err(QueryExecutionError::FuelExhausted)
    );

    let mut under_report = ReportedWorkView {
        row: ticket,
        scanned_rows: 0,
        point_reads: 0,
        continuation: false,
    };
    assert_eq!(
        execute_in_snapshot(&program, &parameters, &mut under_report),
        Err(QueryExecutionError::BoundExceeded)
    );
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

const STATIC_MAX_PAGE: &str = r#"
query BoardPageMax(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == TicketStatus.Open
        order by ticket_id asc
        take 499
    return Found { tickets: tickets { ticket_id } }
    outcomes Found
}
"#;

const PARAM_PAGE: &str = r#"
query ParamPage(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $limit: Limit = 25,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == TicketStatus.Open
        order by ticket_id asc
        take $limit
    return Found { tickets: tickets { ticket_id } }
    outcomes Found
}
"#;

/// Adapter-style view that mirrors real storage: fetch limit+1, charge the probe
/// to scanned_rows, return at most `limit` rows with a continuation when more exist.
struct ProbeScanView {
    available: usize,
    scan_calls: usize,
    open_status: CanonicalValue,
}

impl QueryReadView for ProbeScanView {
    type Error = ();

    fn fault(&self, _error: &Self::Error) -> riffdb_query_executor::QueryBackendFault {
        riffdb_query_executor::QueryBackendFault::Unavailable
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
        step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        limit: u64,
        _after: Option<&[u8]>,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        self.scan_calls += 1;
        let page_limit = usize::try_from(limit).expect("limit fits usize");
        let fetch = page_limit.saturating_add(1);
        let scanned = self.available.min(fetch);
        let has_more = self.available > page_limit;
        let return_count = if has_more { page_limit } else { scanned };
        let rows = (0..return_count)
            .map(|index| {
                row(
                    step.entity(),
                    [
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("project_id", CanonicalValue::Uuid([2; 16])),
                        (
                            "ticket_id",
                            CanonicalValue::Uuid({
                                let mut id = [0_u8; 16];
                                id[0] = (index / 256) as u8;
                                id[1] = (index % 256) as u8;
                                id[15] = 3;
                                id
                            }),
                        ),
                        ("status", self.open_status.clone()),
                    ],
                )
            })
            .collect::<Vec<_>>();
        let scanned_rows = u64::try_from(scanned).expect("scanned fits u64");
        if has_more {
            QueryScanPage::continued(rows, 1, scanned_rows.max(1), vec![0xAB]).ok_or(())
        } else {
            Ok(QueryScanPage::exact_end(rows, 1))
        }
    }

    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        Err(())
    }
}

fn open_status_value(catalog: &SymbolicCatalog) -> CanonicalValue {
    let status = catalog.enumeration("TicketStatus").expect("status enum");
    CanonicalValue::Enum {
        type_id: status.internal_id(),
        variant_id: status.variant("Open").expect("Open"),
    }
}

#[test]
fn max_page_take_executes_with_continuation_probe_against_full_range() {
    use riffdb_query_executor::max_query_page_take;

    let bundle = compile_contract_source(&contract_without_cover()).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program =
        compile_query(&parse_query(STATIC_MAX_PAGE).expect("query"), &catalog).expect("program");
    assert_eq!(program.steps()[0].maximum_rows(), max_query_page_take());
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
    ]))
    .expect("parameters");
    // ≥ max_query_page_take() rows in range so the adapter probes one past the page.
    let mut view = ProbeScanView {
        available: max_query_page_take() as usize + 10,
        scan_calls: 0,
        open_status: open_status_value(&catalog),
    };
    let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("execute max take");
    assert_eq!(view.scan_calls, 1);
    match snapshot.fields().get("tickets") {
        Some(QueryResultValue::Many(rows)) => {
            assert_eq!(rows.len() as u64, max_query_page_take());
        }
        other => panic!("expected many tickets, got {other:?}"),
    }
    assert!(
        snapshot.continuation().is_some(),
        "range larger than take must mint a continuation after the probe"
    );
}

#[test]
fn parameterized_limit_over_max_page_take_is_invalid_parameter_with_zero_scan() {
    use riffdb_query_executor::{MAX_QUERY_SCANNED_ROWS, max_query_page_take};

    let bundle = compile_contract_source(&contract_without_cover()).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program =
        compile_query(&parse_query(PARAM_PAGE).expect("query"), &catalog).expect("program");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
        (
            "limit".to_owned(),
            CanonicalValue::U64(MAX_QUERY_SCANNED_ROWS),
        ),
    ]))
    .expect("parameters");
    let mut view = ProbeScanView {
        available: 1_000,
        scan_calls: 0,
        open_status: open_status_value(&catalog),
    };
    assert_eq!(
        execute_in_snapshot(&program, &parameters, &mut view),
        Err(QueryExecutionError::InvalidParameter {
            parameter: "limit".to_owned(),
        })
    );
    assert_eq!(
        view.scan_calls, 0,
        "over-bound Limit must not execute a scan"
    );

    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
        (
            "limit".to_owned(),
            CanonicalValue::U64(max_query_page_take()),
        ),
    ]))
    .expect("parameters");
    let mut view = ProbeScanView {
        available: max_query_page_take() as usize + 5,
        scan_calls: 0,
        open_status: open_status_value(&catalog),
    };
    let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("at-bound limit");
    assert_eq!(view.scan_calls, 1);
    match snapshot.fields().get("tickets") {
        Some(QueryResultValue::Many(rows)) => {
            assert_eq!(rows.len() as u64, max_query_page_take());
        }
        other => panic!("expected many tickets, got {other:?}"),
    }
}

#[test]
fn scan_ceiling_breach_is_bound_exceeded_not_internal() {
    use riffdb_query_executor::MAX_QUERY_SCANNED_ROWS;

    let bundle = compile_contract_source(&contract_without_cover()).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program =
        compile_query(&parse_query(OPEN_TICKETS).expect("query"), &catalog).expect("program");
    let status = catalog.enumeration("TicketStatus").expect("status enum");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
    ]))
    .expect("parameters");
    let ticket = row(
        "Ticket",
        [
            ("organization_id", CanonicalValue::Uuid([1; 16])),
            ("project_id", CanonicalValue::Uuid([2; 16])),
            ("ticket_id", CanonicalValue::Uuid([3; 16])),
            (
                "status",
                CanonicalValue::Enum {
                    type_id: status.internal_id(),
                    variant_id: status.variant("Open").expect("Open"),
                },
            ),
        ],
    );
    // Force an over-scan page (take 5 plan, synthetic scanned = MAX+1) through
    // the public constructor so defense-in-depth classifies BoundExceeded.
    let mut over = ReportedWorkView {
        row: ticket,
        scanned_rows: MAX_QUERY_SCANNED_ROWS + 1,
        point_reads: 1,
        continuation: true,
    };
    assert_eq!(
        execute_in_snapshot(&program, &parameters, &mut over),
        Err(QueryExecutionError::BoundExceeded)
    );
}

/// Report-only micro-bench: 450-row materialize+project loop. Prints elapsed for
/// controller comparison against the pre-change baseline; does not gate CI.
#[test]
fn report_only_board_scale_materialize_project_loop() {
    let bundle = compile_contract_source(&contract_without_cover()).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    // take 499 so the closed program admits a 450-row board page.
    let program =
        compile_query(&parse_query(STATIC_MAX_PAGE).expect("query"), &catalog).expect("program");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
    ]))
    .expect("parameters");
    let tickets: Vec<QueryRow> = (0u16..450)
        .map(|n| {
            let mut id = [0u8; 16];
            id[0] = (n >> 8) as u8;
            id[1] = n as u8;
            row(
                "Ticket",
                [
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("project_id", CanonicalValue::Uuid([2; 16])),
                    ("ticket_id", CanonicalValue::Uuid(id)),
                    (
                        "status",
                        CanonicalValue::Enum {
                            type_id: EnumTypeId::first(),
                            variant_id: EnumVariantId::first(),
                        },
                    ),
                ],
            )
        })
        .collect();
    let mut view = FakeView {
        head: 1,
        rows: BTreeMap::from([("tickets".to_owned(), tickets)]),
        point_calls: 0,
        batch_calls: 0,
        missing_batch_target: false,
        scan_calls: 0,
        last_limit: None,
        last_after: None,
        scan_epoch: 1,
        continue_first_scan: false,
    };
    let started = Instant::now();
    let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("execute");
    let elapsed = started.elapsed();
    match snapshot.fields().get("tickets") {
        Some(QueryResultValue::Many(rows)) => assert_eq!(rows.len(), 450),
        other => panic!("expected 450 tickets, got {other:?}"),
    }
    eprintln!(
        "row-pipeline microbench (report-only): 450-row materialize+project in {:?} ({:.1} ns/row)",
        elapsed,
        elapsed.as_nanos() as f64 / 450.0
    );
}
