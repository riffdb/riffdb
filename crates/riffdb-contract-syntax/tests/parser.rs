//! Parser corpus, AST, diagnostic, and safety-bound tests.

use proptest::prelude::*;
use riffdb_contract_syntax::ast::{
    BinaryOperator, Binding, CommandKind, Declaration, DeletePolicyDeclaration, Effect, EntityItem,
    Expression, Literal, RowPolicyExpression, RowPolicyOperation, ServiceValueKind, TypeExpression,
};
use riffdb_contract_syntax::diagnostic::SyntaxDiagnosticCode;
use riffdb_contract_syntax::limits::{MAX_EXPECTED_TOKENS, MAX_SYNTAX_DIAGNOSTICS};
use riffdb_contract_syntax::span::Span;
use riffdb_contract_syntax::{parse_contract, parse_contract_bytes};
use std::fmt::Write as _;

const LEGAL_SPEND: &str = include_str!("../../../contracts/parser-fixtures/valid/legal_spend.riff");
const FULL_SURFACE: &str =
    include_str!("../../../contracts/parser-fixtures/valid/full_surface.riff");
const RELATIONSHIPS: &str =
    include_str!("../../../contracts/parser-fixtures/valid/relationships.riff");
const SPEC: &str = include_str!("../../../SPEC.md");
const WORKFLOW_SURFACE: &str =
    include_str!("../../../fixtures/workflows/compiler/valid/workflow_surface.riff");
const ROW_POLICY_SURFACE: &str =
    include_str!("../../../fixtures/compiler/row-policy/valid/document-access.riff");

#[test]
fn parses_one_explicit_bulk_iteration_and_checked_delete_with_exact_spans() {
    let source = r#"
contract BulkSurface version 1 {
  entity Item {
    key (tenant_id: uuid, item_id: uuid)
    field value: u64
  }
  bulk command ReplaceItems {
    input tenant_id: uuid
    input item_ids: list<uuid, 1..256>
    idempotency_key tenant_id
    for item_id in item_ids {
      delete Item(tenant_id, item_id) as item else Missing { item_id: item_id }
    }
    return Replaced {}
  }
}
"#;
    let document = parse_contract(source).expect("bulk surface parses");
    let Declaration::Command(command) = &document.contract.value.declarations[1].value else {
        panic!("second declaration must be bulk command");
    };
    assert_eq!(command.kind, CommandKind::Bulk);
    let iteration = command.bulk_iteration.as_ref().expect("one iteration");
    assert_eq!(iteration.value.element.value, "item_id");
    assert_eq!(iteration.value.collection.value, "item_ids");
    assert!(matches!(
        iteration.value.bindings[0].value,
        Binding::Delete(_)
    ));
    assert_eq!(
        iteration.span.start() as usize,
        source.find("for item_id").expect("iteration source")
    );
    let TypeExpression::List {
        minimum, maximum, ..
    } = &command.inputs[1].value.field.ty.value
    else {
        panic!("second input must be the bounded collection");
    };
    assert_eq!(
        minimum.as_ref().map(|value| value.value.as_str()),
        Some("1")
    );
    assert_eq!(maximum.value, "256");
}

#[test]
fn parses_closed_delete_policies_and_rejects_nested_collection_expansion() {
    let source = r#"
contract DeletePolicies version 1 {
  entity Parent {
    key (tenant_id: uuid, parent_id: uuid)
    delete_policy no_inbound
  }
  entity Referenced {
    key (tenant_id: uuid, referenced_id: uuid)
    delete_policy restrict Child.by_referenced
  }
  entity Child {
    key (tenant_id: uuid, child_id: uuid)
    field referenced_id: uuid
    index by_referenced (tenant_id, referenced_id)
  }
}
"#;
    let document = parse_contract(source).expect("closed delete policies parse");
    let Declaration::Entity(parent) = &document.contract.value.declarations[0].value else {
        panic!("first declaration must be Parent");
    };
    assert!(matches!(
        parent.items[1].value,
        EntityItem::DeletePolicy(DeletePolicyDeclaration::NoInbound)
    ));
    let Declaration::Entity(referenced) = &document.contract.value.declarations[1].value else {
        panic!("second declaration must be Referenced");
    };
    let EntityItem::DeletePolicy(DeletePolicyDeclaration::Restrict {
        source_entity,
        index,
    }) = &referenced.items[1].value
    else {
        panic!("second entity must carry restrict policy");
    };
    assert_eq!(source_entity.value, "Child");
    assert_eq!(index.value, "by_referenced");

    let nested = r#"
contract NestedBulk version 1 {
  entity Row { key (tenant_id: uuid, row_id: uuid) }
  bulk command Invalid {
    input tenant_id: uuid
    input row_ids: list<uuid, 1..8>
    idempotency_key tenant_id
    for row_id in row_ids {
      for nested_id in row_ids {
        delete Row(tenant_id, nested_id) as row else Missing {}
      }
    }
    return Done {}
  }
}
"#;
    let error = parse_contract(nested).expect_err("nested expansion is not grammar");
    assert!(
        error
            .as_slice()
            .iter()
            .any(|diagnostic| diagnostic.code() == SyntaxDiagnosticCode::UnexpectedToken)
    );
}

#[test]
fn parses_principal_facts_and_closed_row_policy_rules_with_exact_spans() {
    let document = parse_contract(ROW_POLICY_SURFACE).expect("row-policy surface parses");
    let Declaration::PrincipalFact(fact) = &document.contract.value.declarations[1].value else {
        panic!("second declaration must be the principal fact");
    };
    assert_eq!(fact.name.value, "team_ids");

    let policy = document
        .contract
        .value
        .declarations
        .iter()
        .find_map(|declaration| match &declaration.value {
            Declaration::RowPolicy(policy) => Some(policy),
            _ => None,
        })
        .expect("row policy declaration");
    assert_eq!(policy.name.value, "DocumentAccess");
    assert_eq!(policy.entity.value, "Document");
    assert_eq!(policy.rules.len(), 4);
    assert_eq!(
        policy.rules[0].value.operation.value,
        RowPolicyOperation::Read
    );
    assert!(matches!(
        policy.rules[0].value.expression.value,
        RowPolicyExpression::Binary { .. }
    ));
    let start = ROW_POLICY_SURFACE
        .find("row policy DocumentAccess")
        .expect("policy source");
    assert_eq!(
        policy.name.span.start() as usize,
        start + "row policy ".len()
    );
}

#[test]
fn parses_compiler_visible_workflow_and_service_values_with_exact_spans() {
    let document = parse_contract(WORKFLOW_SURFACE).expect("workflow surface parses");
    let Declaration::Workflow(workflow) = &document.contract.value.declarations[3].value else {
        panic!("fourth declaration must be the workflow");
    };
    assert_eq!(workflow.name.value, "WorkLifecycle");
    assert_eq!(workflow.entity.value, "WorkItem");
    assert_eq!(workflow.state_field.value, "state");
    assert_eq!(workflow.transitions.len(), 2);
    assert_eq!(workflow.transitions[0].value.name.value, "Start");
    assert_eq!(
        workflow.transitions[0]
            .value
            .source_states
            .iter()
            .map(|state| state.value.as_str())
            .collect::<Vec<_>>(),
        ["Queued"]
    );
    assert_eq!(workflow.transitions[0].value.destination.value, "Running");
    let lease = workflow.lease.as_ref().expect("workflow lease");
    assert_eq!(lease.value.name.value, "execution");
    assert_eq!(lease.value.minimum_duration_seconds.value, "5");
    assert_eq!(lease.value.maximum_duration_seconds.value, "900");

    let Declaration::Command(command) = &document.contract.value.declarations[4].value else {
        panic!("fifth declaration must be the command");
    };
    assert_eq!(command.service_values.len(), 2);
    assert_eq!(
        command.service_values[0].value.kind.value,
        ServiceValueKind::TransactionTime
    );
    assert_eq!(
        command.service_values[1].value.kind.value,
        ServiceValueKind::UuidV7
    );
    let Effect::WorkflowTransition(transition) = &command.effects[0].value else {
        panic!("first effect must be an exact workflow transition");
    };
    assert_eq!(transition.transition.value, "Start");
    assert_eq!(transition.binding.value, "work");
    assert_eq!(transition.stale.value.name.value, "StaleRevision");
    assert_eq!(transition.illegal.value.name.value, "IllegalState");

    let start = WORKFLOW_SURFACE
        .find("transition Start")
        .expect("transition text");
    assert_eq!(workflow.transitions[0].span.start() as usize, start);
}

#[test]
fn parses_the_closed_fenced_lease_operation_family() {
    let source = r#"
contract LeaseEffects version 1 {
  enum State { Ready, Running }
  entity Work {
    key (tenant: uuid, work_id: uuid)
    field state: State
    field owner: optional<uuid>
    field expiry: optional<timestamp>
    field fence: u64
    field attempts: u64
  }
  aggregate Works { root Work partition_by tenant conflict_key (tenant, work_id) }
  workflow Lifecycle {
    entity Work
    state state
    transition Start from (Ready) to Running
    lease execution {
      owner owner
      expires_at expiry
      fencing_token fence
      attempts attempts
      duration_seconds (5, 60)
    }
  }
  command Exercise {
    input request_key: string<32>
    input tenant: uuid
    input work_id: uuid
    input owner_id: uuid
    input duration: u64
    input revision: u64
    input token: u64
    idempotency_key request_key
    mutate Work(tenant, work_id) as work else Missing {}
    lease claim execution on work owner owner_id duration_seconds duration revision revision
      stale Stale {} unavailable Busy {} invalid InvalidDuration {} exhausted Exhausted {}
    lease renew execution on work owner owner_id fencing_token token duration_seconds duration
      revision revision stale Stale {} invalid StaleLease {} expired Expired {} exhausted Exhausted {}
    lease release execution on work owner owner_id fencing_token token revision revision
      stale Stale {} invalid StaleLease {}
    lease expire execution on work revision revision stale Stale {} active Active {}
    lease fence execution on work owner owner_id fencing_token token revision revision
      stale Stale {} invalid StaleLease {} expired Expired {}
    return Done {}
  }
}
"#;

    let document = parse_contract(source).expect("lease operations parse");
    let Declaration::Command(command) = &document.contract.value.declarations[4].value else {
        panic!("fifth declaration must be the command")
    };
    assert_eq!(command.effects.len(), 5);
    assert!(matches!(
        command.effects[0].value,
        Effect::WorkflowLease(ref effect)
            if matches!(effect.operation, riffdb_contract_syntax::ast::WorkflowLeaseOperation::Claim { .. })
    ));
    assert!(matches!(
        command.effects[4].value,
        Effect::WorkflowLease(ref effect)
            if matches!(effect.operation, riffdb_contract_syntax::ast::WorkflowLeaseOperation::Fence { .. })
    ));
}

#[test]
fn parses_required_same_partition_reference_with_exact_spans() {
    let source = concat!(
        "contract C version 1 { ",
        "entity Ticket { key (tenant_id: uuid, ticket_id: uuid) ",
        "field project_id: uuid ",
        "reference project (tenant_id, project_id) -> Project(tenant_id, project_id) } ",
        "entity Project { key (tenant_id: uuid, project_id: uuid) } }"
    );
    let document = parse_contract(source).expect("required reference parses");
    let Declaration::Entity(ticket) = &document.contract.value.declarations[0].value else {
        panic!("ticket entity");
    };
    let EntityItem::Reference(reference) = &ticket.items[2].value else {
        panic!("reference item");
    };
    assert_eq!(reference.name.value, "project");
    assert_eq!(
        reference
            .source_fields
            .iter()
            .map(|field| field.value.as_str())
            .collect::<Vec<_>>(),
        ["tenant_id", "project_id"]
    );
    assert_eq!(reference.target_entity.value, "Project");
    assert_eq!(
        &source[reference.target_entity.span.start() as usize
            ..reference.target_entity.span.end() as usize],
        "Project"
    );
}

#[test]
fn parses_declared_same_partition_unique_key_with_exact_spans() {
    let source = include_str!("../../../contracts/parser-fixtures/valid/uniqueness.riff");
    let document = parse_contract(source).expect("unique fixture parses");
    let Declaration::Entity(user) = &document.contract.value.declarations[1].value else {
        panic!("second declaration must be User");
    };
    let EntityItem::Unique(unique) = &user.items[2].value else {
        panic!("third User item must be unique");
    };
    assert_eq!(unique.name.value, "user_email");
    assert_eq!(
        unique
            .fields
            .iter()
            .map(|field| field.value.as_str())
            .collect::<Vec<_>>(),
        ["organization_id", "email"]
    );
    assert_eq!(
        &source[unique.name.span.start() as usize..unique.name.span.end() as usize],
        "user_email"
    );
}

#[test]
fn invalid_corpus_matches_golden_diagnostics() {
    for (source, golden) in [
        (
            include_str!("../../../contracts/parser-fixtures/invalid/block_comment.riff"),
            include_str!("../../../contracts/parser-fixtures/invalid/block_comment.diag"),
        ),
        (
            include_str!("../../../contracts/parser-fixtures/invalid/command_phase.riff"),
            include_str!("../../../contracts/parser-fixtures/invalid/command_phase.diag"),
        ),
        (
            include_str!("../../../contracts/parser-fixtures/invalid/deferred_query.riff"),
            include_str!("../../../contracts/parser-fixtures/invalid/deferred_query.diag"),
        ),
        (
            include_str!("../../../contracts/parser-fixtures/invalid/deferred_state_machine.riff"),
            include_str!("../../../contracts/parser-fixtures/invalid/deferred_state_machine.diag"),
        ),
        (
            include_str!("../../../contracts/parser-fixtures/invalid/generic_call.riff"),
            include_str!("../../../contracts/parser-fixtures/invalid/generic_call.diag"),
        ),
        (
            include_str!("../../../contracts/parser-fixtures/invalid/missing_binding_else.riff"),
            include_str!("../../../contracts/parser-fixtures/invalid/missing_binding_else.diag"),
        ),
    ] {
        let diagnostics = parse_contract(source).expect_err("invalid fixture must fail");
        assert_eq!(diagnostic_snapshot(&diagnostics), golden);
    }
}

fn diagnostic_snapshot(diagnostics: &riffdb_contract_syntax::SyntaxDiagnostics) -> String {
    let mut snapshot = String::new();
    for diagnostic in diagnostics.as_slice() {
        snapshot.push_str(&format!(
            "code={}\nspan={}..{}\nexpected={:?}\n",
            diagnostic.code().as_str(),
            diagnostic.span().start(),
            diagnostic.span().end(),
            diagnostic.expected(),
        ));
    }
    snapshot
}

#[test]
fn parses_the_checked_valid_corpus() {
    let legal_spend = parse_contract(LEGAL_SPEND).expect("LegalSpend must parse");
    assert_eq!(legal_spend.contract.value.name.value, "LegalSpend");
    assert_eq!(legal_spend.contract.value.version.value, "1");
    assert_eq!(legal_spend.contract.value.declarations.len(), 6);

    let full_surface = parse_contract(FULL_SURFACE).expect("full grammar surface must parse");
    assert_eq!(full_surface.contract.value.name.value, "Inventory");
    assert_eq!(full_surface.contract.value.declarations.len(), 8);
    assert!(matches!(
        full_surface.contract.value.declarations[0].value,
        Declaration::Enum(_)
    ));
    let relationships = parse_contract(RELATIONSHIPS).expect("relationship grammar must parse");
    assert_eq!(relationships.contract.value.name.value, "Relationships");
}

#[test]
fn checked_legal_spend_matches_both_authoritative_spec_copies() {
    let blocks = SPEC
        .match_indices("contract LegalSpend version 1 {")
        .map(|(start, _)| {
            let source = &SPEC[start..];
            let close = source
                .find("\n}\n```")
                .expect("LegalSpend code block must have a closing fence");
            &source[..close + "\n}\n".len()]
        })
        .collect::<Vec<_>>();

    assert_eq!(blocks.len(), 2, "SPEC must contain two canonical copies");
    for block in blocks {
        assert_eq!(block, LEGAL_SPEND);
    }
}

#[test]
fn every_binding_mode_has_one_explicit_failure_outcome() {
    let legal_spend = parse_contract(LEGAL_SPEND).expect("LegalSpend must parse");
    let full_surface = parse_contract(FULL_SURFACE).expect("full surface must parse");

    let mut observed = Vec::new();
    for document in [&legal_spend, &full_surface] {
        for declaration in &document.contract.value.declarations {
            let Declaration::Command(command) = &declaration.value else {
                continue;
            };
            for binding in &command.bindings {
                let (mode, binding) = match &binding.value {
                    Binding::Read(binding) => ("read", binding),
                    Binding::Mutate(binding) => ("mutate", binding),
                    Binding::Create(binding) => ("create", binding),
                    Binding::Delete(binding) => ("delete", binding),
                };
                observed.push((mode, binding.failure.value.name.value.as_str()));
            }
        }
    }

    assert!(observed.contains(&("read", "ItemNotFound")));
    assert!(observed.contains(&("mutate", "BudgetNotFound")));
    assert!(observed.contains(&("create", "BudgetAlreadyExists")));
    assert!(observed.contains(&("create", "ItemAlreadyExists")));
}

#[test]
fn missing_else_is_invalid_for_every_binding_mode() {
    for mode in ["read", "mutate", "create"] {
        let source = format!(
            "contract C version 1 {{ command C {{ {mode} E(id) as record return Done {{}} }} }}"
        );
        let diagnostics = parse_contract(&source).expect_err("binding else must be mandatory");
        let diagnostic = &diagnostics.as_slice()[0];
        assert_eq!(diagnostic.code(), SyntaxDiagnosticCode::UnexpectedToken);
        assert_eq!(diagnostic.expected(), &["else"]);
    }
}

#[test]
fn preserves_exact_half_open_source_spans() {
    let source = "contract A version 7 { event E { value: i64 } }";
    let document = parse_contract(source).expect("small contract must parse");
    assert_eq!(document.contract.span.start(), 0);
    assert_eq!(document.contract.span.end() as usize, source.len());
    assert_eq!(document.contract.value.name.span.start(), 9);
    assert_eq!(document.contract.value.name.span.end(), 10);
    assert_eq!(document.contract.value.version.span.start(), 19);
    assert_eq!(document.contract.value.version.span.end(), 20);
}

#[test]
fn date_is_contextual_only_as_a_transaction_path_segment() {
    let path_source = concat!(
        "contract C version 1 { command C { ",
        "return Done { value: tx.date } } }"
    );
    assert!(parse_contract(path_source).is_ok());

    let field_source = "contract C version 1 { event E { date: date } }";
    let diagnostics = parse_contract(field_source).expect_err("date cannot be a field name");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        SyntaxDiagnosticCode::UnexpectedToken
    );
}

#[test]
fn expression_precedence_is_fixed_and_left_associative() {
    let source = concat!(
        "contract P version 1 { command C { ",
        "return Done { value: 1 + 2 * 3 - 4 == 3 && !false || null == null } ",
        "} }"
    );
    let document = parse_contract(source).expect("precedence contract must parse");
    let Declaration::Command(command) = &document.contract.value.declarations[0].value else {
        panic!("expected command declaration");
    };
    let expression = &command
        .return_clause
        .value
        .outcome
        .value
        .payload
        .value
        .fields[0]
        .value
        .value;

    let Expression::Binary { operator, left, .. } = &expression.value else {
        panic!("expected outer binary expression");
    };
    assert_eq!(operator.value, BinaryOperator::Or);
    let Expression::Binary {
        operator: and_operator,
        left: equality,
        ..
    } = &left.value
    else {
        panic!("expected and expression");
    };
    assert_eq!(and_operator.value, BinaryOperator::And);
    let Expression::Binary {
        operator: equality_operator,
        left: subtraction,
        ..
    } = &equality.value
    else {
        panic!("expected equality expression");
    };
    assert_eq!(equality_operator.value, BinaryOperator::Equal);
    let Expression::Binary {
        operator: subtraction_operator,
        left: addition,
        ..
    } = &subtraction.value
    else {
        panic!("expected subtraction expression");
    };
    assert_eq!(subtraction_operator.value, BinaryOperator::Subtract);
    let Expression::Binary {
        operator: addition_operator,
        right: multiplication,
        ..
    } = &addition.value
    else {
        panic!("expected addition expression");
    };
    assert_eq!(addition_operator.value, BinaryOperator::Add);
    assert!(matches!(
        multiplication.value,
        Expression::Binary { ref operator, .. } if operator.value == BinaryOperator::Multiply
    ));
}

#[test]
fn parses_source_spelling_without_decoding_or_typing_literals() {
    let source = concat!(
        "contract L version 0001 { command C { ",
        "return Done { unsigned: 00042, decimal_value: 001.2300, text: \"a\\n\\u0021\" } ",
        "} }"
    );
    let document = parse_contract(source).expect("literal contract must parse");
    assert_eq!(document.contract.value.version.value, "0001");
    let Declaration::Command(command) = &document.contract.value.declarations[0].value else {
        panic!("expected command declaration");
    };
    let fields = &command
        .return_clause
        .value
        .outcome
        .value
        .payload
        .value
        .fields;
    assert!(matches!(
        fields[0].value.value.value,
        Expression::Literal(ref literal) if literal.value == Literal::UInt("00042".to_owned())
    ));
    assert!(matches!(
        fields[1].value.value.value,
        Expression::Literal(ref literal)
            if literal.value == Literal::FixedDecimal("001.2300".to_owned())
    ));
    assert!(matches!(
        fields[2].value.value.value,
        Expression::Literal(ref literal)
            if literal.value == Literal::String("\"a\\n\\u0021\"".to_owned())
    ));
}

#[test]
fn enforces_command_phases_and_rejects_generic_calls() {
    for fixture in [
        include_str!("../../../contracts/parser-fixtures/invalid/command_phase.riff"),
        include_str!("../../../contracts/parser-fixtures/invalid/generic_call.riff"),
    ] {
        let diagnostics = parse_contract(fixture).expect_err("invalid grammar must fail");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.as_slice()[0].code(),
            SyntaxDiagnosticCode::UnexpectedToken
        );
    }
}

#[test]
fn deferred_syntax_and_block_comments_have_a_closed_failure_code() {
    for fixture in [
        include_str!("../../../contracts/parser-fixtures/invalid/block_comment.riff"),
        include_str!("../../../contracts/parser-fixtures/invalid/deferred_query.riff"),
        include_str!("../../../contracts/parser-fixtures/invalid/deferred_state_machine.riff"),
    ] {
        let diagnostics = parse_contract(fixture).expect_err("deferred grammar must fail");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.as_slice()[0].code(),
            SyntaxDiagnosticCode::UnsupportedSyntax
        );
    }
}

#[test]
fn validates_currency_as_exact_uppercase_ascii() {
    assert!(parse_contract("contract C version 1 { event E { price: money<USD> } }").is_ok());
    for currency in ["usd", "US", "USDD", "U1D"] {
        let source = format!("contract C version 1 {{ event E {{ price: money<{currency}> }} }}");
        let diagnostics = parse_contract(&source).expect_err("invalid currency must fail");
        assert_eq!(
            diagnostics.as_slice()[0].code(),
            SyntaxDiagnosticCode::InvalidToken
        );
    }
}

#[test]
fn enforces_expression_and_collection_bounds() {
    let at_depth = format!(
        "contract C version 1 {{ command C {{ return Done {{ value: {}true }} }} }}",
        "!".repeat(32)
    );
    assert!(parse_contract(&at_depth).is_ok());

    let above_depth = format!(
        "contract C version 1 {{ command C {{ return Done {{ value: {}true }} }} }}",
        "!".repeat(33)
    );
    let diagnostics = parse_contract(&above_depth).expect_err("deep expression must fail");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        SyntaxDiagnosticCode::NestingLimit
    );

    let arguments = (0..=1024).map(|_| "value").collect::<Vec<_>>().join(",");
    let above_list = format!(
        "contract C version 1 {{ command C {{ read E({arguments}) as e else Missing {{}} return Done {{}} }} }}"
    );
    let diagnostics = parse_contract(&above_list).expect_err("large list must fail");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        SyntaxDiagnosticCode::CollectionLimit
    );

    let long_path = (0..=1024)
        .map(|index| format!("segment{index}"))
        .collect::<Vec<_>>()
        .join(".");
    let path_source =
        format!("contract C version 1 {{ command C {{ return Done {{ value: {long_path} }} }} }}");
    assert!(
        parse_contract(&path_source).is_ok(),
        "path segments are governed by node bounds, not tuple-list bounds"
    );
}

#[test]
fn incomplete_input_has_bounded_static_expected_tokens() {
    let source = "contract C version 1 { event E { value:";
    let diagnostics = parse_contract(source).expect_err("incomplete source must fail");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), SyntaxDiagnosticCode::UnexpectedEnd);
    assert!(diagnostic.span().is_empty());
    assert_eq!(diagnostic.span().start() as usize, source.len());
    assert!(!diagnostic.expected().is_empty());
    assert!(diagnostic.expected().len() <= MAX_EXPECTED_TOKENS);
}

#[test]
fn eof_span_includes_trailing_skipped_bytes() {
    for source in [
        "contract C version 1 { event E { value:   ",
        "contract C version 1 { event E { value: // trailing comment é",
    ] {
        let diagnostics = parse_contract(source).expect_err("incomplete source must fail");
        let diagnostic = &diagnostics.as_slice()[0];
        assert_eq!(diagnostic.code(), SyntaxDiagnosticCode::UnexpectedEnd);
        assert_eq!(diagnostic.span().start() as usize, source.len());
        assert_eq!(diagnostic.span().end() as usize, source.len());
    }
}

#[test]
fn invalid_utf8_is_rejected_at_the_first_invalid_sequence() {
    let source = b"contract C version 1 {\xff}";
    let diagnostics = parse_contract_bytes(source).expect_err("invalid UTF-8 must fail");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), SyntaxDiagnosticCode::InvalidToken);
    assert_eq!(
        (diagnostic.span().start(), diagnostic.span().end()),
        (22, 23)
    );
}

#[test]
fn declaration_and_item_limits_are_checked_before_ast_construction() {
    let declarations_at_limit = repeated_event_declarations(4096);
    assert!(parse_contract(&declarations_at_limit).is_ok());
    assert_collection_error_at(&repeated_event_declarations(4097), "event E4096");

    let entity_items_at_limit = repeated_entity_fields(4096);
    assert!(parse_contract(&entity_items_at_limit).is_ok());
    assert_collection_error_at(&repeated_entity_fields(4097), "field f4096");

    let projection_at_limit = repeated_projection_measures(4093);
    assert!(parse_contract(&projection_at_limit).is_ok());
    assert_collection_error_at(&repeated_projection_measures(4094), "frontier");
}

#[test]
fn each_bounded_collection_accepts_1024_and_rejects_1025_entries() {
    for (at_limit, above_limit, failing_lexeme) in [
        repeated_enum_variants(1024),
        repeated_object_fields(1024),
        repeated_key_fields(1024),
        repeated_index_fields(1024),
        repeated_arguments(1024),
    ] {
        assert!(parse_contract(&at_limit).is_ok());
        assert_collection_error_at(&above_limit, &failing_lexeme);
    }
}

fn assert_collection_error_at(source: &str, lexeme: &str) {
    let diagnostics =
        parse_contract(source).expect_err("source above a collection limit must fail");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), SyntaxDiagnosticCode::CollectionLimit);
    let start = source
        .rfind(lexeme)
        .expect("failing lexeme must be present in generated source");
    assert_eq!(
        diagnostic.span().start() as usize,
        start,
        "wrong collection diagnostic span for {lexeme:?} in {:?}",
        &source[..source.len().min(80)]
    );
}

fn repeated_event_declarations(count: usize) -> String {
    let mut source = String::from("contract C version 1 {");
    for index in 0..count {
        write!(source, " event E{index} {{}}").expect("String writes are infallible");
    }
    source.push('}');
    source
}

fn repeated_entity_fields(count: usize) -> String {
    let mut source = String::from("contract C version 1 { entity E {");
    for index in 0..count {
        write!(source, " field f{index}: bool").expect("String writes are infallible");
    }
    source.push_str("} }");
    source
}

fn repeated_projection_measures(count: usize) -> String {
    let mut source =
        String::from("contract C version 1 { projection P { source event E key (group)");
    for index in 0..count {
        write!(source, " measure m{index} = count()").expect("String writes are infallible");
    }
    source.push_str(" frontier transactionally_ordered } }");
    source
}

fn repeated_enum_variants(count: usize) -> (String, String, String) {
    let build = |count| {
        let values = (0..count)
            .map(|index| format!("V{index}"))
            .collect::<Vec<_>>()
            .join(",");
        format!("contract C version 1 {{ enum E {{ {values} }} }}")
    };
    (build(count), build(count + 1), format!("V{count}"))
}

fn repeated_object_fields(count: usize) -> (String, String, String) {
    let build = |count| {
        let values = (0..count)
            .map(|index| format!("f{index}: true"))
            .collect::<Vec<_>>()
            .join(",");
        format!("contract C version 1 {{ command C {{ return Done {{ {values} }} }} }}")
    };
    (build(count), build(count + 1), ":".to_owned())
}

fn repeated_key_fields(count: usize) -> (String, String, String) {
    let build = |count| {
        let values = (0..count)
            .map(|index| format!("f{index}: decimal<28,2>"))
            .collect::<Vec<_>>()
            .join(",");
        format!("contract C version 1 {{ entity E {{ key ({values}) }} }}")
    };
    (build(count), build(count + 1), format!("f{count}:"))
}

fn repeated_index_fields(count: usize) -> (String, String, String) {
    let build = |count| {
        let values = (0..count)
            .map(|index| format!("f{index}"))
            .collect::<Vec<_>>()
            .join(",");
        format!("contract C version 1 {{ entity E {{ index I ({values}) }} }}")
    };
    (build(count), build(count + 1), format!("f{count}"))
}

fn repeated_arguments(count: usize) -> (String, String, String) {
    let build = |count| {
        let values = (0..count)
            .map(|index| format!("a{index}"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "contract C version 1 {{ command C {{ read E({values}) as e else Missing {{}} return Done {{}} }} }}"
        )
    };
    (build(count), build(count + 1), format!("a{count}"))
}

#[test]
fn diagnostics_never_include_source_or_parser_debug_text() {
    let canary = "DO_NOT_EXPOSE_THIS_CANARY";
    let source =
        format!("contract C version 1 {{ command C {{ return Done {{ value: {canary}(1) }} }} }}");
    let diagnostics = parse_contract(&source).expect_err("call syntax must fail");
    let rendered = diagnostics.to_string();
    assert!(!rendered.contains(canary));
    assert!(!rendered.contains("ParseError"));
}

proptest! {
    #[test]
    fn arbitrary_utf8_never_panics_or_escapes_diagnostic_bounds(source in any::<String>()) {
        if let Err(diagnostics) = parse_contract(&source) {
            prop_assert!(!diagnostics.is_empty());
            prop_assert!(diagnostics.len() <= MAX_SYNTAX_DIAGNOSTICS);
            for diagnostic in diagnostics.as_slice() {
                prop_assert!(diagnostic.expected().len() <= MAX_EXPECTED_TOKENS);
                prop_assert!(diagnostic.span().start() <= diagnostic.span().end());
                prop_assert!((diagnostic.span().end() as usize) <= source.len());
            }
        }
    }


    #[test]
    fn arbitrary_bytes_exercise_utf8_validation_and_parser_bounds(
        source in proptest::collection::vec(any::<u8>(), 0..4096)
    ) {
        if let Err(diagnostics) = parse_contract_bytes(&source) {
            prop_assert!(!diagnostics.is_empty());
            prop_assert!(diagnostics.len() <= MAX_SYNTAX_DIAGNOSTICS);
            for diagnostic in diagnostics.as_slice() {
                prop_assert!(diagnostic.expected().len() <= MAX_EXPECTED_TOKENS);
                prop_assert!(diagnostic.span().start() <= diagnostic.span().end());
                prop_assert!((diagnostic.span().end() as usize) <= source.len());
            }
        }
    }
}

#[test]
fn all_effect_variants_are_represented_in_the_full_fixture() {
    let document = parse_contract(FULL_SURFACE).expect("full grammar surface must parse");
    let command = document
        .contract
        .value
        .declarations
        .iter()
        .find_map(|declaration| match &declaration.value {
            Declaration::Command(command) if command.name.value == "CreateItem" => Some(command),
            _ => None,
        })
        .expect("fixture contains a command");
    assert!(
        command
            .effects
            .iter()
            .any(|effect| matches!(effect.value, Effect::Set(_)))
    );
    assert!(
        command
            .effects
            .iter()
            .any(|effect| matches!(effect.value, Effect::Emit(_)))
    );
}

// ─── Vector keywords are contextual (fix round S10) ───

/// The metric names and `staleness_slo` are ordinary identifiers outside a
/// `vector_field` declaration: the WP-591 hard keywords broke contracts
/// declaring fields with these names while claiming additive compatibility.
#[test]
fn metric_and_slo_names_remain_valid_field_identifiers() {
    let source = r#"
contract Compat version 1 {
  entity Row {
    key (id: uuid)
    field cosine: u64
    field euclidean: u64
    field dot_product: u64
    field staleness_slo: u64
  }
}
"#;
    let document = parse_contract(source).expect("metric names must stay usable as field names");
    let Declaration::Entity(entity) = &document.contract.value.declarations[0].value else {
        panic!("entity declaration");
    };
    let field_names: Vec<&str> = entity
        .items
        .iter()
        .filter_map(|item| match &item.value {
            EntityItem::Field(field) => Some(field.name.value.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        field_names,
        ["cosine", "euclidean", "dot_product", "staleness_slo"]
    );
}

/// An unknown metric identifier inside `vector_field` is rejected at its
/// exact span.
#[test]
fn unknown_vector_metric_is_rejected_at_its_span() {
    let source = r#"
contract Invalid version 1 {
  entity Document {
    key (id: uuid)
    field title: string<256>
    vector_field embedding(128, manhattan, (title), staleness_slo 60)
  }
}
"#;
    let diagnostics = parse_contract(source).expect_err("unknown metric must be rejected");
    let start = source.find("manhattan").expect("metric span");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(
        diagnostic.span(),
        Span::new(start, start + 9).expect("span")
    );
}

/// A misplaced keyword in the `staleness_slo` position is rejected at its
/// exact span.
#[test]
fn wrong_staleness_keyword_is_rejected_at_its_span() {
    let source = r#"
contract Invalid version 1 {
  entity Document {
    key (id: uuid)
    field title: string<256>
    vector_field embedding(128, cosine, (title), freshness_slo 60)
  }
}
"#;
    let diagnostics = parse_contract(source).expect_err("wrong keyword must be rejected");
    let start = source.find("freshness_slo").expect("keyword span");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(
        diagnostic.span(),
        Span::new(start, start + 13).expect("span")
    );
}

// ─── Secret field classification is contextual (ADR-0118, WP-597) ───

/// `field secret name: type` records the classification with the modifier's
/// exact source span; an unclassified field records `None`.
#[test]
fn parses_secret_field_classification_with_exact_spans() {
    let source = r#"
contract Auth version 1 {
  entity Session {
    key (id: uuid)
    field secret token_hash: string<256>
    field expires_at: timestamp
  }
}
"#;
    let document = parse_contract(source).expect("secret field classification parses");
    let Declaration::Entity(entity) = &document.contract.value.declarations[0].value else {
        panic!("entity declaration");
    };
    let EntityItem::Field(token_hash) = &entity.items[1].value else {
        panic!("classified field item");
    };
    assert_eq!(token_hash.name.value, "token_hash");
    let secret = token_hash.secret.expect("classification span");
    assert_eq!(
        &source[secret.start() as usize..secret.end() as usize],
        "secret"
    );
    let EntityItem::Field(expires_at) = &entity.items[2].value else {
        panic!("plain field item");
    };
    assert_eq!(expires_at.name.value, "expires_at");
    assert_eq!(expires_at.secret, None);
}

/// `secret` stays an ordinary identifier everywhere except the classifier
/// position: a field named `secret` and a secret-classified field named
/// `secret` both parse (additive compatibility, the WP-591 S10 lesson).
#[test]
fn secret_remains_a_valid_field_identifier() {
    let source = r#"
contract Compat version 1 {
  entity Row {
    key (id: uuid)
    field secret: string<64>
    field secret secret: string<64>
  }
}
"#;
    let document = parse_contract(source).expect("`secret` must stay usable as a field name");
    let Declaration::Entity(entity) = &document.contract.value.declarations[0].value else {
        panic!("entity declaration");
    };
    let EntityItem::Field(plain) = &entity.items[1].value else {
        panic!("plain field item");
    };
    assert_eq!(plain.name.value, "secret");
    assert_eq!(plain.secret, None);
    let EntityItem::Field(classified) = &entity.items[2].value else {
        panic!("classified field item");
    };
    assert_eq!(classified.name.value, "secret");
    assert!(classified.secret.is_some());
}

/// An unknown classification modifier is rejected at its exact span with the
/// closed invalid-token code.
#[test]
fn unknown_field_classification_is_rejected_at_its_span() {
    let source = r#"
contract Invalid version 1 {
  entity Session {
    key (id: uuid)
    field hidden token_hash: string<256>
  }
}
"#;
    let diagnostics = parse_contract(source).expect_err("unknown modifier must be rejected");
    let start = source.find("hidden").expect("modifier span");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), SyntaxDiagnosticCode::InvalidToken);
    assert_eq!(diagnostic.span(), Span::new(start, start + 6).expect("span"));
}

/// The classification is unrepresentable outside stored entity fields: key
/// fields, event fields, and command inputs reject the modifier position
/// outright.
#[test]
fn secret_classification_is_rejected_outside_stored_entity_fields() {
    for source in [
        r#"
contract Invalid version 1 {
  entity Session {
    key (secret id: uuid)
  }
}
"#,
        r#"
contract Invalid version 1 {
  event TokenIssued {
    secret token_hash: string<256>
  }
}
"#,
        r#"
contract Invalid version 1 {
  command IssueToken {
    input secret token_hash: string<256>
    return issued {}
  }
}
"#,
    ] {
        parse_contract(source)
            .expect_err("the classification position must not exist outside entity fields");
    }
}
