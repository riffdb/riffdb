//! Finite operational plan-family safety and identity acceptance.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{
    PlannerDiagnosticCode, compile_operational_query_family, compile_query,
};
use riffdb_query_ir::{
    AccessDirection, MAX_OPERATIONAL_PRESENCE_PARAMETERS, NamedTypeSchema,
    OperationalAggregateFunctionV1, PageBound, QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1,
    QueryAccessKind, QueryDiagnosticCode, QueryPredicateOperator, SourceSymbolKind,
    SymbolicCatalog, resolve_query_surface,
};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = r#"
contract Operational version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: string<32>
    field priority: string<32>
    field story_points: i64
    field deleted_at: optional<timestamp>
    field created_at: timestamp
    field updated_at: timestamp
    index a_status_priority (organization_id, status, priority, updated_at, ticket_id)
    index b_status (organization_id, status, updated_at, ticket_id)
    index c_priority (organization_id, priority, updated_at, ticket_id)
    index z_all (organization_id, updated_at, ticket_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}
"#;

const QUERY: &str = r#"
query SearchTickets(
    $organization_id: Ticket.organization_id,
    $status: Ticket.status?,
    $priority: Ticket.priority?
) {
    many tickets from Ticket
        where organization_id == $organization_id
          && when $status { status == $status }
          && when $priority { priority == $priority }
        order by updated_at asc, ticket_id asc
        take 25
    return Found { tickets: tickets { ticket_id status priority updated_at } }
    outcomes Found
}
"#;

const UNINDEXED_NULL_QUERY: &str = r#"
query DeletedTickets($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id && deleted_at is null
        order by updated_at asc, ticket_id asc
        take 25
    return Found { tickets: tickets { ticket_id deleted_at updated_at } }
    outcomes Found
}
"#;

const EXACT_IDENTITY_CONTRACT: &str = r#"
contract ExactIdentity version 1 {
  entity Account {
    key (organization_id: uuid, user_id: uuid, account_id: uuid)
    field external_id: string<256>
    field provider_id: string<64>
    unique provider_identity (organization_id, provider_id, external_id)
    index by_provider (organization_id, provider_id, external_id, user_id, account_id)
    index by_user (organization_id, user_id, account_id)
  }
  aggregate Accounts {
    root Account
    partition_by organization_id
    conflict_key (organization_id, user_id)
  }
}
"#;

const EXACT_IDENTITY_QUERY: &str = r#"
query AccountByProvider(
    $organization_id: Account.organization_id,
    $external_id: Account.external_id,
    $provider_id: Account.provider_id,
) {
    many accounts from Account
        where organization_id == $organization_id
          && provider_id == $provider_id
          && external_id == $external_id
        order by user_id asc, account_id asc
        take 1
    return Found { accounts: accounts { user_id account_id external_id provider_id } }
    outcomes Found
}
"#;

const BINARY_ORDER_CONTRACT: &str = r#"
contract BinaryOrder version 1 {
  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field title: string<64>
    index by_title (organization_id, title, document_id) text_key(title, binary_utf8_v1)
  }
  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
  }
}
"#;

const BINARY_ORDER_QUERY: &str = r#"
query DocumentsByBinaryTitle(
    $organization_id: Document.organization_id,
    $after: Cursor?,
) {
    many documents from Document
        where organization_id == $organization_id
        order by title asc, document_id asc
        take 1 after $after
    return Found { documents: documents { document_id title } }
    outcomes Found
}
"#;

const SHARED_BINARY_COMPONENT_CONTRACT: &str = r#"
contract SharedBinaryComponents version 1 {
  entity Tuple {
    key (store_id: uuid, object_type: string<64>, object_id: string<128>, relation: string<64>, user: string<256>)
    field active: bool
    index by_object (store_id, active, object_type, object_id, relation, user)
      text_key(relation, binary_utf8_v1)
      text_key(user, binary_utf8_v1)
  }
  aggregate Tuples {
    root Tuple
    partition_by store_id
    conflict_key (store_id, object_type, object_id, relation, user)
  }
}
"#;

const SHARED_BINARY_ORDER_QUERY: &str = r#"
query TuplesByObject(
    $store_id: Tuple.store_id,
    $active: Tuple.active,
    $object_type: Tuple.object_type,
    $object_id: Tuple.object_id,
    $after: Cursor?,
) {
    many tuples from Tuple
        where store_id == $store_id
          && active == $active
          && object_type == $object_type
          && object_id == $object_id
        order by relation asc, user asc
        take 1 after $after
    return Found { tuples: tuples { relation user } }
    outcomes Found
}
"#;

const SHARED_BINARY_EQUALITY_QUERY: &str = r#"
query TuplesByObjectAndRelation(
    $store_id: Tuple.store_id,
    $active: Tuple.active,
    $object_type: Tuple.object_type,
    $object_id: Tuple.object_id,
    $relation: Tuple.relation,
    $after: Cursor?,
) {
    many tuples from Tuple
        where store_id == $store_id
          && active == $active
          && object_type == $object_type
          && object_id == $object_id
          && relation == $relation
        order by user asc
        take 1 after $after
    return Found { tuples: tuples { relation user } }
    outcomes Found
}
"#;

const SHARED_BINARY_MEMBERSHIP_QUERY: &str = r#"
query TuplesByObjects(
    $store_id: Tuple.store_id,
    $active: Tuple.active,
    $object_type: Tuple.object_type,
    $object_ids: Set<Tuple.object_id>,
    $after: Cursor?,
) {
    many tuples from Tuple
        where store_id == $store_id
          && active == $active
          && object_type == $object_type
          && object_id in $object_ids
        order by object_id asc, relation asc, user asc
        take 1 after $after
    return Found { tuples: tuples { object_id relation user } }
    outcomes Found
}
"#;

fn catalog(source: &str) -> SymbolicCatalog {
    let bundle = compile_contract_source(source).expect("contract");
    SymbolicCatalog::from_bundle(&bundle).expect("catalog")
}

#[test]
fn binary_text_key_proves_ordinary_forward_and_reverse_cursor_order_without_prefix() {
    let catalog = catalog(BINARY_ORDER_CONTRACT);
    for (source, expected_direction) in [
        (BINARY_ORDER_QUERY.to_owned(), AccessDirection::Forward),
        (
            BINARY_ORDER_QUERY
                .replace("title asc", "title desc")
                .replace("document_id asc", "document_id desc"),
            AccessDirection::Reverse,
        ),
    ] {
        let family = compile_operational_query_family(
            &parse_query(&source).expect("binary-order query"),
            &catalog,
        )
        .expect("declared binary text key proves the ordinary total order");
        let program = family.select(&[]).expect("sole family member").program();
        assert!(matches!(
            program.steps()[0].access(),
            QueryAccessKind::Index {
                index,
                direction,
                ..
            } if index == "by_title" && *direction == expected_direction
        ));
        assert_eq!(program.steps()[0].cursor_parameter(), Some("after"));
    }
}

#[test]
fn one_binary_text_index_proves_ordered_and_exact_equality_prefix_shapes() {
    let catalog = catalog(SHARED_BINARY_COMPONENT_CONTRACT);
    for source in [SHARED_BINARY_ORDER_QUERY, SHARED_BINARY_EQUALITY_QUERY] {
        let family = compile_operational_query_family(
            &parse_query(source).expect("shared binary-component query"),
            &catalog,
        )
        .expect("one text-key index proves both compiled shapes");
        let program = family.select(&[]).expect("sole family member").program();
        assert!(matches!(
            program.steps()[0].access(),
            QueryAccessKind::Index { index, direction: AccessDirection::Forward, .. }
                if index == "by_object"
        ));
        assert_eq!(program.steps()[0].cursor_parameter(), Some("after"));
    }
}

#[test]
fn binary_text_key_proves_bounded_membership_and_bytewise_order() {
    let contract = SHARED_BINARY_COMPONENT_CONTRACT.replace(
        "    index by_object (store_id, active, object_type, object_id, relation, user)",
        concat!(
            "    index by_object (store_id, active, object_type, object_id, relation, user)\n",
            "      text_key(object_id, binary_utf8_v1)"
        ),
    );
    let family = compile_operational_query_family(
        &parse_query(SHARED_BINARY_MEMBERSHIP_QUERY).expect("binary membership query"),
        &catalog(&contract),
    )
    .expect("one binary text-key index proves bounded membership and order");
    let program = family.select(&[]).expect("sole family member").program();
    assert!(matches!(
        program.steps()[0].access(),
        QueryAccessKind::Index { index, direction: AccessDirection::Forward, .. }
            if index == "by_object"
    ));
    assert!(program.steps()[0].predicates().iter().any(|predicate| {
        predicate.field() == "object_id" && predicate.operator() == QueryPredicateOperator::In
    }));
    assert_eq!(program.steps()[0].cursor_parameter(), Some("after"));
}

#[test]
fn binary_text_order_does_not_reinterpret_other_typed_predicates() {
    let source = SHARED_BINARY_EQUALITY_QUERY
        .replace("relation == $relation", "relation >= $relation")
        .replace("order by user asc", "order by relation asc, user asc");
    let diagnostics = compile_operational_query_family(
        &parse_query(&source).expect("binary range query"),
        &catalog(SHARED_BINARY_COMPONENT_CONTRACT),
    )
    .expect_err("binary text order cannot reinterpret canonical string range semantics");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::Unindexed);
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
        include_str!("../../../fixtures/riffql/binary-text-range-unindexed.snapshot")
    );
}

#[test]
fn ordinary_range_predicate_cannot_be_deferred_until_after_the_page_bound() {
    let source = r#"
query TicketsUpdatedAfter(
    $organization_id: Ticket.organization_id,
    $updated_at: Ticket.updated_at,
) {
    many tickets from Ticket
        where organization_id == $organization_id && updated_at >= $updated_at
        order by updated_at asc, ticket_id asc
        take 25
    return Found { tickets: tickets { ticket_id updated_at } }
    outcomes Found
}
"#;
    let diagnostics = compile_operational_query_family(
        &parse_query(source).expect("range query"),
        &catalog(CONTRACT),
    )
    .expect_err("an unlowered interval cannot be evaluated after the page bound");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::Unindexed);
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
        include_str!("../../../fixtures/riffql/canonical-range-unavailable.snapshot")
    );
}

#[test]
fn planner_never_selects_an_ordered_index_that_omits_a_filter_predicate() {
    let program = compile_query(
        &parse_query(EXACT_IDENTITY_QUERY).expect("query"),
        &catalog(EXACT_IDENTITY_CONTRACT),
    )
    .expect("declared filter-and-order index is a complete access path");
    assert!(matches!(
        program.steps()[0].access(),
        riffdb_query_ir::QueryAccessKind::Index { index, .. }
            if index == "by_provider"
    ));

    let without_filter_index = EXACT_IDENTITY_CONTRACT.replace(
        "    index by_provider (organization_id, provider_id, external_id, user_id, account_id)\n",
        "",
    );
    let diagnostics = compile_query(
        &parse_query(EXACT_IDENTITY_QUERY).expect("query"),
        &catalog(&without_filter_index),
    )
    .expect_err("an order-only index cannot defer filters until after take");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::Unindexed);
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
        include_str!("../../../fixtures/riffql/residual-predicate-unindexed.snapshot")
    );
}

#[test]
fn every_presence_mask_selects_one_precompiled_bounded_member() {
    let catalog = catalog(CONTRACT);
    let document = parse_query(QUERY).expect("query");
    let first = compile_operational_query_family(&document, &catalog).expect("family");
    let second = compile_operational_query_family(&document, &catalog).expect("same family");

    assert_eq!(first, second);
    assert_eq!(first.identity(), second.identity());
    assert_eq!(first.presence_parameters(), &["priority", "status"]);
    assert_eq!(first.members().len(), 4);
    assert_eq!(
        first.select(&[false, false]).expect("00").presence_mask(),
        0
    );
    assert_eq!(first.select(&[true, false]).expect("01").presence_mask(), 1);
    assert_eq!(first.select(&[false, true]).expect("10").presence_mask(), 2);
    assert_eq!(first.select(&[true, true]).expect("11").presence_mask(), 3);
    assert!(first.select(&[true]).is_none());

    let indexes = first.authorization_union()[0].indexes();
    let member_indexes = first
        .members()
        .iter()
        .map(|member| {
            (
                member.presence_mask(),
                member.program().authorization()[0].indexes().to_vec(),
                member.program().steps()[0].predicates().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    for expected in ["a_status_priority", "b_status", "c_priority", "z_all"] {
        assert!(
            indexes.iter().any(|candidate| candidate == expected),
            "{expected}: {indexes:?}; {member_indexes:?}"
        );
    }
    for member in first.members() {
        assert!(first.maximum_cost().covers(member.program().cost()));
        assert_eq!(member.program().partition_parameter(), "organization_id");
        assert_eq!(
            member.program().surface().schemas(),
            first.members()[0].program().surface().schemas()
        );
    }
}

#[test]
fn ordinary_compiler_rejects_operational_source_instead_of_ignoring_it() {
    let catalog = catalog(CONTRACT);
    let document = parse_query(QUERY).expect("query");
    let diagnostics = compile_query(&document, &catalog).expect_err("closed V1 compiler");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        PlannerDiagnosticCode::OperationalFamilyRequired
    );
}

#[test]
fn null_and_existence_require_a_declared_discriminator_index() {
    let catalog = catalog(CONTRACT);
    for predicate in [
        "deleted_at is null",
        "deleted_at is not null",
        "exists deleted_at",
    ] {
        let source = QUERY.replace(
            "&& when $priority { priority == $priority }",
            &format!("&& {predicate}"),
        );
        let document = parse_query(&source).expect("null/existence syntax");
        let diagnostics = compile_operational_query_family(&document, &catalog)
            .expect_err("ordinary index must not collapse missing, null, and non-null");
        let diagnostic = &diagnostics.as_slice()[0];
        assert_eq!(diagnostic.code(), PlannerDiagnosticCode::Unindexed);
        assert!(diagnostic.primary().start < diagnostic.primary().end);
        assert!(
            diagnostic
                .suggested_index()
                .is_some_and(|suggestion| suggestion.contains("presence"))
        );
    }
}

#[test]
fn unindexed_null_diagnostic_has_a_frozen_span_and_operational_remediation() {
    let diagnostics = compile_operational_query_family(
        &parse_query(UNINDEXED_NULL_QUERY).expect("null syntax"),
        &catalog(CONTRACT),
    )
    .expect_err("ordinary index cannot answer explicit null");
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
        include_str!("../../../fixtures/riffql/unindexed-null.snapshot")
    );
}

#[test]
fn prefix_requires_a_declared_versioned_text_key_index() {
    let catalog = catalog(CONTRACT);
    let source = QUERY
        .replace("$priority: Ticket.priority?", "$priority: Ticket.priority")
        .replace(
            "&& when $priority { priority == $priority }",
            "&& priority prefix $priority",
        );
    let document = parse_query(&source).expect("prefix syntax");
    let diagnostics = compile_operational_query_family(&document, &catalog)
        .expect_err("ordinary string index has no versioned text-key identity");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::Unindexed);
    assert!(diagnostic.primary().start < diagnostic.primary().end);
    assert!(
        diagnostic
            .suggested_index()
            .is_some_and(|suggestion| suggestion.contains("text_key"))
    );
}

#[test]
fn declared_operational_indexes_lower_to_sealed_predicates() {
    let contract = CONTRACT.replace(
        "    index z_all (organization_id, updated_at, ticket_id)",
        concat!(
            "    index z_all (organization_id, updated_at, ticket_id)\n",
            "    index y_deleted (organization_id, deleted_at, updated_at, ticket_id) ",
            "presence(deleted_at)\n",
            "    index x_priority_text (organization_id, priority, updated_at, ticket_id) ",
            "text_key(priority, binary_utf8_v1)",
        ),
    );
    let catalog = catalog(&contract);
    let cases = [
        (
            "deleted_at is null",
            "order by updated_at asc, ticket_id asc",
            "y_deleted",
            QueryPredicateOperator::IsNull,
        ),
        (
            "exists deleted_at",
            "order by deleted_at asc nulls first, updated_at asc, ticket_id asc",
            "y_deleted",
            QueryPredicateOperator::Exists,
        ),
        (
            "priority prefix $priority",
            "order by priority asc, updated_at asc, ticket_id asc",
            "x_priority_text",
            QueryPredicateOperator::Prefix,
        ),
    ];
    for (predicate, order, expected_index, expected_operator) in cases {
        let source = QUERY
            .replace("    $status: Ticket.status?,\n", "")
            .replace("&& when $status { status == $status }\n          ", "")
            .replace("$priority: Ticket.priority?", "$priority: Ticket.priority")
            .replace(
                "&& when $priority { priority == $priority }",
                &format!("&& {predicate}"),
            )
            .replace("order by updated_at asc, ticket_id asc", order);
        let family = compile_operational_query_family(
            &parse_query(&source).expect("operational syntax"),
            &catalog,
        )
        .expect("declared operational index lowers");
        for member in family.members() {
            let step = &member.program().steps()[0];
            assert!(matches!(
                step.access(),
                riffdb_query_ir::QueryAccessKind::Index { index, .. }
                    if index == expected_index
            ));
            assert!(
                step.predicates()
                    .iter()
                    .any(|predicate| predicate.operator() == expected_operator)
            );
        }
    }
}

#[test]
fn one_unindexed_presence_member_rejects_the_complete_family() {
    let contract = CONTRACT.replace(
        "    index z_all (organization_id, updated_at, ticket_id)\n",
        "",
    );
    let catalog = catalog(&contract);
    let document = parse_query(QUERY).expect("query");
    let diagnostics =
        compile_operational_query_family(&document, &catalog).expect_err("mask zero unindexed");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::Unindexed);
    assert!(diagnostic.primary().start < diagnostic.primary().end);
    assert!(diagnostic.suggested_index().is_some());
}

#[test]
fn optional_inputs_are_usable_only_in_matching_top_level_guards() {
    let catalog = catalog(CONTRACT);
    let cases = [
        (
            QUERY.replace("when $status { status == $status }", "status == $status"),
            PlannerDiagnosticCode::OperationalFamilyRequired,
        ),
        (
            QUERY.replace(
                "when $status { status == $status }",
                "true || when $status { status == $status }",
            ),
            PlannerDiagnosticCode::OperationalFamilyRequired,
        ),
        (
            QUERY.replace("$status: Ticket.status?", "$status: Ticket.status"),
            PlannerDiagnosticCode::TypeMismatch,
        ),
        (
            QUERY.replace(
                "when $status { status == $status }",
                "when $status { status == \"Open\" }",
            ),
            PlannerDiagnosticCode::OperationalFamilyRequired,
        ),
    ];
    for (source, expected) in cases {
        let document = parse_query(&source).expect("syntax");
        let diagnostics = compile_operational_query_family(&document, &catalog)
            .expect_err("unsafe optional shape");
        assert_eq!(diagnostics.as_slice()[0].code(), expected, "{source}");
        assert!(
            diagnostics.as_slice()[0].primary().start < diagnostics.as_slice()[0].primary().end
        );
    }
}

#[test]
fn presence_dimension_count_is_hard_bounded_before_enumeration() {
    let catalog = catalog(CONTRACT);
    let parameter_count = MAX_OPERATIONAL_PRESENCE_PARAMETERS + 1;
    let parameters = (0..parameter_count)
        .map(|index| format!("$status_{index}: Ticket.status?"))
        .collect::<Vec<_>>()
        .join(",\n    ");
    let guards = (0..parameter_count)
        .map(|index| format!("&& when $status_{index} {{ status == $status_{index} }}"))
        .collect::<Vec<_>>()
        .join("\n          ");
    let source = format!(
        r#"query TooMany(
    $organization_id: Ticket.organization_id,
    {parameters}
) {{
    many tickets from Ticket
        where organization_id == $organization_id
          {guards}
        order by updated_at asc, ticket_id asc
        take 25
    return Found {{ tickets: tickets {{ ticket_id }} }}
    outcomes Found
}}"#
    );
    let document = parse_query(&source).expect("syntax");
    let diagnostics =
        compile_operational_query_family(&document, &catalog).expect_err("too many members");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        PlannerDiagnosticCode::Unbounded
    );
}

#[test]
fn aggregate_syntax_lowers_to_a_bounded_authorized_family() {
    let catalog = catalog(CONTRACT);
    let source = QUERY.replace(
        "    return Found { tickets: tickets { ticket_id status priority updated_at } }",
        r#"    aggregate summary from tickets {
        group by status, priority
        count() as ticket_count
        sum(story_points) as total_points
        min(created_at) as earliest
        max(updated_at) as latest
    }
    return Found {
        tickets: tickets { ticket_id status priority updated_at }
        summary: summary { status priority ticket_count total_points earliest latest }
    }"#,
    );
    let document = parse_query(&source).expect("aggregate syntax");
    let expected_start = source.find("summary from").expect("aggregate symbol") as u32;

    let diagnostics = compile_query(&document, &catalog).expect_err("ordinary compiler rejects");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(
        diagnostic.code(),
        PlannerDiagnosticCode::OperationalFamilyRequired
    );
    assert_eq!(diagnostic.primary().start, expected_start);
    assert_eq!(diagnostic.symbol_path(), &["summary"]);

    let family = compile_operational_query_family(&document, &catalog).expect("aggregate family");
    for (bytes, magic) in [
        (
            family.canonical_bytes(),
            b"RIFFDB-OPERATIONAL-QUERY-FAMILY\0".as_slice(),
        ),
        (
            family.surface().canonical_bytes(),
            b"RIFFDB-QUERY-SURFACE\0".as_slice(),
        ),
    ] {
        assert_eq!(&bytes[..magic.len()], magic);
        assert_eq!(
            u32::from_be_bytes(
                bytes[magic.len()..magic.len() + 4]
                    .try_into()
                    .expect("version bytes")
            ),
            QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1
        );
    }
    let [aggregate] = family.aggregates() else {
        panic!("one aggregate descriptor expected")
    };
    assert_eq!(aggregate.name(), "summary");
    assert_eq!(aggregate.source_binding(), "tickets");
    assert_eq!(aggregate.source_entity(), "Ticket");
    assert_eq!(aggregate.maximum_groups(), &PageBound::Literal(25));
    assert_eq!(
        aggregate
            .group_keys()
            .iter()
            .map(|key| key.field())
            .collect::<Vec<_>>(),
        ["status", "priority"]
    );
    assert_eq!(
        aggregate
            .measures()
            .iter()
            .map(|measure| (measure.alias(), measure.function(), measure.input_field()))
            .collect::<Vec<_>>(),
        [
            ("ticket_count", OperationalAggregateFunctionV1::Count, None),
            (
                "total_points",
                OperationalAggregateFunctionV1::Sum,
                Some("story_points")
            ),
            (
                "earliest",
                OperationalAggregateFunctionV1::Min,
                Some("created_at")
            ),
            (
                "latest",
                OperationalAggregateFunctionV1::Max,
                Some("updated_at")
            ),
        ]
    );
    assert_eq!(
        aggregate.measures()[1].result_type(),
        &NamedTypeSchema::Scalar("decimal<39,0>".to_owned())
    );
    assert_eq!(
        aggregate.measures()[2].result_type(),
        &NamedTypeSchema::Optional(Box::new(NamedTypeSchema::Scalar("timestamp".to_owned())))
    );

    for member in family.members() {
        let fields = member.program().steps()[0].selected_fields();
        for expected in [
            "created_at",
            "priority",
            "status",
            "story_points",
            "ticket_id",
            "updated_at",
        ] {
            assert!(fields.iter().any(|field| field == expected), "{fields:?}");
            assert!(
                member.program().authorization()[0]
                    .fields()
                    .iter()
                    .any(|field| field == expected),
                "{:?}",
                member.program().authorization()[0].fields()
            );
        }
        assert!(member.program().cost().projected_values() > 100);
    }
    assert!(
        family
            .surface()
            .source_map()
            .entries()
            .iter()
            .any(|entry| entry.kind() == SourceSymbolKind::Aggregate
                && entry.symbolic_path() == ["summary"])
    );
    assert!(
        family
            .surface()
            .source_map()
            .entries()
            .iter()
            .any(|entry| entry.kind() == SourceSymbolKind::AggregateMeasure
                && entry.symbolic_path() == ["summary", "total_points"])
    );
}

#[test]
fn whole_set_aggregate_has_one_result_and_changes_family_identity() {
    let catalog = catalog(CONTRACT);
    let count_source = QUERY.replace(
        "    return Found",
        "    aggregate summary from tickets { count() as ticket_count }\n    return Found",
    );
    let sum_source = count_source.replace(
        "count() as ticket_count",
        "sum(story_points) as total_points",
    );
    let count = compile_operational_query_family(
        &parse_query(&count_source).expect("count syntax"),
        &catalog,
    )
    .expect("count family");
    let sum =
        compile_operational_query_family(&parse_query(&sum_source).expect("sum syntax"), &catalog)
            .expect("sum family");
    assert_eq!(
        count.aggregates()[0].maximum_groups(),
        &PageBound::Literal(1)
    );
    assert_ne!(count.identity(), sum.identity());
}

#[test]
fn aggregate_result_aliases_fail_with_a_source_spanned_semantic_diagnostic() {
    let catalog = catalog(CONTRACT);
    let source = QUERY.replace(
        "    return Found { tickets: tickets { ticket_id status priority updated_at } }",
        r#"    aggregate summary from tickets { count() as ticket_count }
    return Found { renamed: summary { ticket_count } }"#,
    );
    let document = parse_query(&source).expect("aggregate alias syntax");
    let diagnostics = resolve_query_surface(&document, &catalog)
        .expect_err("v1 aggregate result alias must fail closed");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), QueryDiagnosticCode::InvalidPath);
    assert_eq!(diagnostic.symbol_path(), &["summary"]);
    assert_eq!(
        &source[diagnostic.primary().start as usize..diagnostic.primary().end as usize],
        "renamed"
    );
    let rendered = format!(
        "{}|{:?}|{}..{}|{}|{}|{}\n",
        diagnostic.code().as_str(),
        diagnostic.stage(),
        diagnostic.primary().start,
        diagnostic.primary().end,
        diagnostic.symbol_path().join("."),
        diagnostic.summary(),
        diagnostic.help().unwrap_or("none"),
    );
    assert_eq!(
        rendered,
        include_str!("../../../fixtures/riffql/aggregate_result_alias.snapshot")
    );
}
