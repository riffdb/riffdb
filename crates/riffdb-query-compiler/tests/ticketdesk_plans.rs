//! Stable TicketDesk planner acceptance fixtures.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_ir::{
    QUERY_IR_VERSION_BOUNDED_LIMIT_V1, QUERY_IR_VERSION_COVERED_RESULT_V1, QUERY_IR_VERSION_V1,
    QueryAccessKind, QueryAccessProgramV1, QueryAccessStep, QueryPredicateValue, QueryRowLimit,
    SymbolicCatalog,
};
use riffdb_riffql_syntax::parse_query;
use riffdb_types::QueryCostVectorV1;

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");

fn query(name: &str) -> &'static str {
    QUERIES
        .iter()
        .find_map(|(query_name, source)| (*query_name == name).then_some(*source))
        .unwrap_or_else(|| panic!("missing query fixture {name}"))
}

const QUERIES: &[(&str, &str)] = &[
    (
        "get_ticket",
        include_str!("../../../queries/ticketdesk/get_ticket.riffq"),
    ),
    (
        "get_user",
        include_str!("../../../queries/ticketdesk/get_user.riffq"),
    ),
    (
        "list_comments",
        include_str!("../../../queries/ticketdesk/list_comments.riffq"),
    ),
    (
        "list_tickets",
        include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
    ),
    (
        "list_tickets_by_assignee",
        include_str!("../../../queries/ticketdesk/list_tickets_by_assignee.riffq"),
    ),
    (
        "ticket_page",
        include_str!("../../../queries/ticketdesk/ticket_page.riffq"),
    ),
    (
        "project_members",
        include_str!("../../../queries/ticketdesk/project_members.riffq"),
    ),
    (
        "project_summary",
        include_str!("../../../queries/ticketdesk/project_summary.riffq"),
    ),
];

const BOARD_PAGE_450: &str = include_str!("../../../queries/ticketdesk/board_page_450.riffq");

const BOUNDED_RUNTIME_BOARD_PAGE: &str = r#"query BoundedBoardPage(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $status: TicketStatus,
    $limit: Limit<100> = 50,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take $limit
    return Found { tickets: tickets { ticket_id title } }
    outcomes Found
}
"#;

const CURSOR_BOUNDED_RUNTIME_BOARD_PAGE: &str = r#"query CursorBoundedBoardPage(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $status: TicketStatus,
    $limit: Limit<100> = 50,
    $after: Cursor?,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take $limit after $after
    return Found { tickets: tickets { ticket_id title } }
    outcomes Found
}
"#;

const MIXED_CURSOR_AND_UNPAGED_LIMIT: &str = r#"query MixedCursorAndUnpagedLimit(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $status: TicketStatus,
    $limit: Limit<100> = 50,
    $after: Cursor?,
) {
    many first from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take $limit after $after
    many second from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take $limit
    return Found {
        first: first { ticket_id title }
        second: second { ticket_id title }
    }
    outcomes Found
}
"#;

const TWO_BOUNDED_COLLECTIONS: &str = r#"query TwoBoundedCollections(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
    $status: TicketStatus,
    $limit: Limit<100> = 50,
) {
    many first from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take $limit
    many second from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == $status
        order by ticket_id asc
        take $limit
    return Found {
        first: first { ticket_id title }
        second: second { ticket_id title }
    }
    outcomes Found
}
"#;

#[test]
fn bounded_runtime_limit_drives_the_plan_and_whole_request_cost() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let document = parse_query(BOUNDED_RUNTIME_BOARD_PAGE).expect("bounded source");
    let plan = compile_query(&document, &catalog).expect("bounded plan");
    assert_eq!(plan.ir_version(), QUERY_IR_VERSION_BOUNDED_LIMIT_V1);
    assert_eq!(plan.steps()[0].maximum_rows(), 100);
    assert_eq!(plan.cost().scanned_index_rows(), 100);
    assert!(matches!(
        plan.steps()[0].row_limit(),
        QueryRowLimit::BoundedParameter {
            name,
            maximum: 100,
            default: Some(50),
        } if name == "limit"
    ));
}

#[test]
fn bounded_runtime_limit_cost_is_cumulative_for_every_reference() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let document = parse_query(TWO_BOUNDED_COLLECTIONS).expect("bounded source");
    let plan = compile_query(&document, &catalog).expect("bounded plan");
    assert_eq!(plan.steps().len(), 2);
    assert!(plan.steps().iter().all(|step| step.maximum_rows() == 100));
    assert_eq!(plan.cost().scanned_index_rows(), 200);
}

#[test]
fn only_exclusively_cursor_paged_limits_are_page_cardinality_parameters() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");

    let cursor_document =
        parse_query(CURSOR_BOUNDED_RUNTIME_BOARD_PAGE).expect("cursor-paged source");
    let cursor_plan = compile_query(&cursor_document, &catalog).expect("cursor-paged plan");
    assert_eq!(cursor_plan.cursor_page_cardinality_parameters(), ["limit"]);

    let unpaged_document = parse_query(BOUNDED_RUNTIME_BOARD_PAGE).expect("unpaged source");
    let unpaged_plan = compile_query(&unpaged_document, &catalog).expect("unpaged plan");
    assert!(unpaged_plan.cursor_page_cardinality_parameters().is_empty());

    let mixed_document = parse_query(MIXED_CURSOR_AND_UNPAGED_LIMIT).expect("mixed-use source");
    let mixed_plan = compile_query(&mixed_document, &catalog).expect("mixed-use plan");
    assert!(mixed_plan.cursor_page_cardinality_parameters().is_empty());
}

#[test]
fn complete_cover_seals_board_page_layout_and_incomplete_cover_does_not() {
    let covered_contract = CONTRACT.to_owned();
    let bundle = compile_contract_source(&covered_contract).expect("covered contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("covered catalog");
    let covered = compile_query(&parse_query(BOARD_PAGE_450).expect("board parse"), &catalog)
        .expect("covered board plan");
    let step = &covered.steps()[0];
    assert!(matches!(
        step.access(),
        QueryAccessKind::Index { index, .. } if index == "by_board_project_status"
    ));
    let layout = step
        .covered_result_layout()
        .expect("complete cover seals layout");
    assert_eq!(layout.entity(), "Ticket");
    assert_eq!(layout.index(), "by_board_project_status");
    assert_eq!(
        layout
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        [
            "assignee_id",
            "organization_id",
            "project_id",
            "reporter_id",
            "status",
            "ticket_id",
            "title",
        ]
    );
    assert_eq!(covered.ir_version(), QUERY_IR_VERSION_COVERED_RESULT_V1);

    let incomplete_contract = CONTRACT.replace(
        "    index by_board_project_status (organization_id, project_id, status, ticket_id) cover (title, reporter_id, assignee_id)",
        "    index by_board_project_status (organization_id, project_id, status, ticket_id) cover (title, reporter_id)",
    );
    let bundle = compile_contract_source(&incomplete_contract).expect("incomplete contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("incomplete catalog");
    let generic = compile_query(&parse_query(BOARD_PAGE_450).expect("board parse"), &catalog)
        .expect("generic board plan");
    assert!(generic.steps()[0].covered_result_layout().is_none());
    assert_eq!(generic.ir_version(), QUERY_IR_VERSION_V1);

    let insertion = covered_contract.rfind('}').expect("contract closing brace");
    let policy_contract = format!(
        "{}  row policy TicketRead on Ticket {{ allow read when true }}\n{}",
        &covered_contract[..insertion],
        &covered_contract[insertion..]
    );
    let bundle = compile_contract_source(&policy_contract).expect("policy contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("policy catalog");
    let policy_plan = compile_query(&parse_query(BOARD_PAGE_450).expect("board parse"), &catalog)
        .expect("policy board plan");
    assert!(
        policy_plan.steps()[0].covered_result_layout().is_none(),
        "a cover is not eligible until the complete row-policy proof is represented"
    );
}

#[test]
fn every_ticketdesk_query_has_one_stable_bounded_same_partition_program() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    for (name, source) in QUERIES {
        let document = parse_query(source).expect("parse");
        let first = compile_query(&document, &catalog)
            .unwrap_or_else(|diagnostics| panic!("{name}: {diagnostics:?}"));
        let second = compile_query(&document, &catalog).expect("deterministic second compile");
        assert_eq!(first, second, "{name}");
        assert_eq!(first.partition_parameter(), "organization_id", "{name}");
        assert!(!first.steps().is_empty(), "{name}");
        assert!(first.steps().iter().all(|step| step.maximum_rows() <= 500));
        assert_eq!(first.identity(), second.identity(), "{name}");
        assert!(
            !first
                .explain()
                .lines()
                .iter()
                .any(|line| line.contains("Id("))
        );
    }
}

#[test]
fn list_and_detail_choose_expected_physical_accesses() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let list = compile_query(
        &parse_query(query("list_tickets")).expect("parse"),
        &catalog,
    )
    .expect("plan");
    assert_eq!(
        format!("{}\n", list.explain().lines().join("\n")),
        include_str!("../../../fixtures/riffql/list_tickets.plan")
    );
    assert!(matches!(
        list.steps()[0].access(),
        QueryAccessKind::Index { index, .. } if index == "by_board_project_status"
    ));

    let detail =
        compile_query(&parse_query(query("ticket_page")).expect("parse"), &catalog).expect("plan");
    let step = |binding: &str| {
        detail
            .steps()
            .iter()
            .find(|step| step.binding() == binding)
            .unwrap_or_else(|| panic!("missing {binding}"))
    };
    assert!(matches!(
        step("ticket").access(),
        QueryAccessKind::Point { .. }
    ));
    assert!(matches!(
        step("comments").access(),
        QueryAccessKind::Index { index, .. } if index == "by_ticket"
    ));
    assert!(matches!(
        step("ticket_labels").access(),
        QueryAccessKind::Index { index, .. } if index == "by_ticket"
    ));
    assert!(matches!(
        step("labels").access(),
        QueryAccessKind::DependentPointBatch {
            source_binding,
            source_field,
            ..
        } if source_binding == "ticket_labels" && source_field == "label_id"
    ));
    let expected_scans = detail
        .steps()
        .iter()
        .filter(|step| matches!(step.access(), QueryAccessKind::Index { .. }))
        .map(QueryAccessStep::maximum_rows)
        .sum::<u64>();
    let expected_points = detail
        .steps()
        .iter()
        .map(|step| match step.access() {
            QueryAccessKind::Point { .. } => 1,
            QueryAccessKind::Index { .. } | QueryAccessKind::DependentPointBatch { .. } => {
                step.maximum_rows()
            }
            QueryAccessKind::Nearest { .. } => 0,
        })
        .sum::<u64>();
    let expected_dependent_keys = detail
        .steps()
        .iter()
        .filter(|step| matches!(step.access(), QueryAccessKind::DependentPointBatch { .. }))
        .map(QueryAccessStep::maximum_rows)
        .sum::<u64>();
    assert_eq!(detail.cost().access_steps(), detail.steps().len() as u64);
    assert_eq!(detail.cost().scanned_index_rows(), expected_scans);
    assert_eq!(detail.cost().point_reads(), expected_points);
    assert_eq!(detail.cost().dependent_keys(), expected_dependent_keys);
    assert_eq!(
        detail.cost().intermediate_rows(),
        detail
            .steps()
            .iter()
            .map(QueryAccessStep::maximum_rows)
            .sum::<u64>()
    );
    let ticket = &detail.steps()[0];
    assert!(
        ["project_id", "reporter_id", "assignee_id"]
            .iter()
            .all(|field| ticket.predicate_fields().iter().any(|read| read == field)),
        "downstream join fields must be fetched from the source binding"
    );
    assert!(
        ["reporter_id", "assignee_id"].iter().all(|field| !ticket
            .selected_fields()
            .iter()
            .any(|selected| selected == field)),
        "dependency-only fields must not leak into the public result"
    );

    let mut reordered_steps = detail.steps().to_vec();
    let source = reordered_steps
        .iter()
        .position(|candidate| candidate.binding() == "ticket_labels")
        .expect("dependent source");
    let target = reordered_steps
        .iter()
        .position(|candidate| candidate.binding() == "labels")
        .expect("dependent target");
    reordered_steps.swap(source, target);
    assert!(
        QueryAccessProgramV1::checked(
            detail.contract().clone(),
            detail.surface().clone(),
            detail.name().map(str::to_owned),
            detail.partition_parameter().to_owned(),
            reordered_steps,
            detail.authorization().to_vec(),
            detail.cost(),
        )
        .is_none(),
        "a dependent target cannot precede its compiler-declared driver"
    );

    let reassigned_reporter = query("ticket_page").replace(
        "user_id == ticket.reporter_id",
        "user_id == ticket.assignee_id",
    );
    let reassigned_reporter = compile_query(
        &parse_query(&reassigned_reporter).expect("reassigned parse"),
        &catalog,
    )
    .expect("reassigned plan");
    assert_ne!(
        detail.identity(),
        reassigned_reporter.identity(),
        "the exact relationship mapping participates in plan identity"
    );
}

#[test]
fn collection_dependencies_are_not_implicitly_scalar_or_unbounded() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let ticket_page = query("ticket_page");
    let scalar = ticket_page.replace(
        "label_id in ticket_labels.label_id",
        "label_id == ticket_labels.label_id",
    );
    let diagnostics =
        compile_query(&parse_query(&scalar).expect("parse"), &catalog).expect_err("reject scalar");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );
    assert_eq!(
        diagnostics.as_slice()[0].symbol_path(),
        &["ticket_labels", "label_id"]
    );
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(
        format!(
            "{}|{}..{}|{}|{}|{}\n",
            diagnostic.code().as_str(),
            diagnostic.primary().start,
            diagnostic.primary().end,
            diagnostic.symbol_path().join("."),
            diagnostic.summary(),
            diagnostic.suggested_index().unwrap_or("")
        ),
        include_str!("../../../fixtures/riffql/cardinality.snapshot")
    );

    let singular_set = ticket_page.replace(
        "label_id in ticket_labels.label_id",
        "label_id in ticket.ticket_id",
    );
    let diagnostics = compile_query(&parse_query(&singular_set).expect("parse"), &catalog)
        .expect_err("reject singular set");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );

    let no_outcome = ticket_page.replace(
        "        take 50\n        else IntegrityFailure\n\n    return Found",
        "        take 50\n\n    return Found",
    );
    let diagnostics = compile_query(&parse_query(&no_outcome).expect("parse"), &catalog)
        .expect_err("reject implicit missing targets");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );

    let target_exceeds_source = ticket_page.replacen(
        "        take 50\n\n    many labels",
        "        take 25\n\n    many labels",
        1,
    );
    let diagnostics = compile_query(
        &parse_query(&target_exceeds_source).expect("parse"),
        &catalog,
    )
    .expect_err("reject target bound above source");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );

    let noncanonical_source = ticket_page.replace(
        "order by label_id asc\n        take 50\n\n    many labels",
        "order by label_id desc\n        take 50\n\n    many labels",
    );
    let diagnostics = compile_query(&parse_query(&noncanonical_source).expect("parse"), &catalog)
        .expect_err("reject reverse collection source");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        riffdb_query_compiler::PlannerDiagnosticCode::Cardinality
    );
}

#[test]
fn predicate_inputs_are_closed_and_participate_in_plan_identity() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let open = query("project_summary");
    let closed = open.replace("status == $status", "status == TicketStatus.Closed");
    let open = compile_query(&parse_query(open).expect("parse"), &catalog).expect("plan");
    let closed = compile_query(&parse_query(&closed).expect("parse"), &catalog).expect("plan");
    assert_ne!(open.identity(), closed.identity());
    assert!(matches!(
        open.steps()[1].predicates()[2].value(),
        QueryPredicateValue::Parameter(name) if name == "status"
    ));
    assert!(matches!(
        closed.steps()[1].predicates()[2].value(),
        QueryPredicateValue::EnumVariant {
            enumeration,
            variant,
            ..
        }
            if enumeration == "TicketStatus" && variant == "Closed"
    ));
}

#[test]
fn complete_cost_vector_participates_in_plan_identity() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let original =
        compile_query(&parse_query(query("get_ticket")).expect("parse"), &catalog).expect("plan");
    let cost = original.cost();
    let changed_cost = QueryCostVectorV1::new(
        cost.access_steps(),
        cost.scanned_index_rows(),
        cost.point_reads(),
        cost.dependent_keys(),
        cost.intermediate_rows(),
        cost.projected_values(),
        cost.encoded_result_bytes() + 1,
    )
    .expect("one-byte-larger bounded cost");
    let changed = QueryAccessProgramV1::checked(
        original.contract().clone(),
        original.surface().clone(),
        original.name().map(str::to_owned),
        original.partition_parameter().to_owned(),
        original.steps().to_vec(),
        original.authorization().to_vec(),
        changed_cost,
    )
    .expect("checked program");
    assert_ne!(original.identity(), changed.identity());
    assert_ne!(original.canonical_bytes(), changed.canonical_bytes());
}
