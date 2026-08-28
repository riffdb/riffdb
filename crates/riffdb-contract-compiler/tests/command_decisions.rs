//! Compiler-sealed command-decision conformance (ADR-0163).

use std::fmt::Write as _;

use riffdb_contract_compiler::{CompilerDiagnosticCode, compile_contract_source};
use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V18, BindingMode, CommandDecisionActionV1, ContractBundle,
    EXECUTABLE_IR_VERSION_V18, GRAMMAR_VERSION_V18,
};

const SOURCE: &str = r#"
contract SealedState version 1 {
  entity State {
    key (organization_id: uuid, state_id: uuid)
    field active: bool
    field revision: u64
  }
  entity Change {
    key (organization_id: uuid, state_id: uuid, change_id: uuid)
    field recorded: bool
  }
  event StateApplied { state_id: uuid revision: u64 }
  aggregate States {
    root State
    child Change
    partition_by organization_id
    conflict_key (organization_id, state_id)
  }
  command ApplyState {
    input request_id: uuid
    input organization_id: uuid
    input state_id: uuid
    input change_id: uuid
    idempotency_key request_id
    observe_or_initialize State(organization_id, state_id) as state initialize {
      active: false,
      revision: 0,
    }
    decide state {
      when state.active == false => apply {
        create Change(organization_id, state_id, change_id) as change else ChangeExists {}
        set state.active = true
        set state.revision = state.revision + 1
        set change.recorded = true
        emit StateApplied { state_id: state.state_id, revision: state.revision }
      }
      when state.active == true => no_effect
      else => reject StateConflict { state_id: state_id }
    }
    return Applied {}
  }
  bulk command ApplyStates {
    input request_id: uuid
    input states: list<State, 1..100>
    idempotency_key request_id
    for input_state in states {
      observe_or_initialize State(input_state.organization_id, input_state.state_id)
        as stored initialize {
          active: false,
          revision: 0,
        }
      decide stored {
        when stored.active == false => apply {
          set stored.active = true
          set stored.revision = stored.revision + 1
        }
        else => no_effect
      }
    }
    return Applied {}
  }
}
"#;

#[test]
fn sealed_decision_selects_v18_and_round_trips_canonically() {
    let bundle = compile_contract_source(SOURCE).expect("sealed decision compiles");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V18);
    assert_eq!(bundle.grammar_version(), GRAMMAR_VERSION_V18);
    assert_eq!(bundle.ir_version(), EXECUTABLE_IR_VERSION_V18);
    assert_eq!(bundle.commands().len(), 2);
    let command = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "ApplyState")
        .expect("ordinary decision command");
    assert!(command.requires_ir_v18());
    assert_eq!(command.decisions().len(), 1);
    assert_eq!(
        command.bindings()[command.decisions()[0].binding().get() as usize].mode(),
        BindingMode::ObserveOrInitialize
    );
    assert!(matches!(
        command.decisions()[0].when_arms()[0].action(),
        CommandDecisionActionV1::Apply { .. }
    ));
    let bulk = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "ApplyStates")
        .expect("bulk decision command");
    assert!(bulk.decisions()[0].collection_local());
    assert!(matches!(
        command.decisions()[0].when_arms()[1].action(),
        CommandDecisionActionV1::NoEffect
    ));
    assert!(matches!(
        command.decisions()[0].else_action(),
        CommandDecisionActionV1::Reject(_)
    ));
    assert_eq!(
        ContractBundle::decode(bundle.canonical_bytes()).expect("V18 round trip"),
        bundle
    );
}

#[test]
fn checked_in_v18_fixture_is_byte_exact_and_decodes_canonically() {
    let source = include_str!("../../../fixtures/compiler/command-decision/contract.riff");
    let expected = include_bytes!("../../../fixtures/compiler/command-decision/bundle.bin");
    let expected_hash =
        include_str!("../../../fixtures/compiler/command-decision/bundle-hash.txt").trim_end();
    let bundle = compile_contract_source(source).expect("checked V18 fixture compiles");
    assert_eq!(bundle.canonical_bytes(), expected);
    assert_eq!(
        bundle
            .bundle_hash()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        expected_hash
    );
    assert_eq!(
        ContractBundle::decode(expected).expect("checked V18 fixture decodes"),
        bundle
    );
}

#[test]
fn equivalent_later_predicate_is_rejected_at_its_exact_span() {
    let source = SOURCE.replace(
        "when state.active == true => no_effect",
        "when state.active == false => no_effect",
    );
    let expected = source
        .rfind("state.active == false")
        .expect("second equivalent predicate");
    let error = compile_contract_source(&source).expect_err("equivalent later arm must reject");
    let diagnostics = error.semantic().expect("compiler diagnostics").as_slice();
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidExpression)
        .expect("equivalent predicate diagnostic");
    assert_eq!(diagnostic.primary_span().start() as usize, expected);
    assert_eq!(
        diagnostic.primary_span().end() as usize,
        expected + "state.active == false".len()
    );
    assert!(diagnostic.related_span().is_some());
}

#[test]
fn effects_outside_sealed_arms_reject_at_the_effect_target() {
    let source = SOURCE.replacen(
        "    return Applied {}",
        "    set state.active = false\n    return Applied {}",
        1,
    );
    let expected = source.rfind("set state.active").expect("common effect");
    let error = compile_contract_source(&source).expect_err("effect outside arms must reject");
    let diagnostics = error.semantic().expect("compiler diagnostics").as_slice();
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidMutation)
        .expect("common effect diagnostic");
    assert_eq!(diagnostic.primary_span().start() as usize, expected);
}

#[test]
fn mutually_exclusive_arm_graph_bytes_are_charged_as_a_maximum() {
    let mut large_fields = String::new();
    let mut left_assignments = String::new();
    let mut right_assignments = String::new();
    for field in 0..16 {
        writeln!(large_fields, "    field payload_{field}: bytes<524288>").expect("field source");
        writeln!(
            left_assignments,
            "          set left.payload_{field} = payload"
        )
        .expect("left assignment");
        writeln!(
            right_assignments,
            "          set right.payload_{field} = payload"
        )
        .expect("right assignment");
    }
    let source = format!(
        r#"
contract DecisionCost version 1 {{
  entity State {{
    key (organization_id: uuid, state_id: uuid)
    field active: bool
    field revision: u64
  }}
  entity LeftGraph {{
    key (organization_id: uuid, state_id: uuid, graph_id: uuid)
{large_fields}  }}
  entity RightGraph {{
    key (organization_id: uuid, state_id: uuid, graph_id: uuid)
{large_fields}  }}
  aggregate States {{
    root State
    child LeftGraph
    child RightGraph
    partition_by organization_id
    conflict_key (organization_id, state_id)
  }}
  bulk command Apply {{
    input request_id: uuid
    input graph_id: uuid
    input payload: bytes<524288>
    input states: list<State, 1..1>
    idempotency_key request_id
    for submitted in states {{
      observe_or_initialize State(submitted.organization_id, submitted.state_id)
        as state initialize {{ active: false, revision: 0 }}
      decide state {{
        when state.active == false => apply {{
          create LeftGraph(submitted.organization_id, submitted.state_id, graph_id)
            as left else LeftExists {{}}
          set state.active = true
          set state.revision = state.revision + 1
{left_assignments}        }}
        when state.active == true => apply {{
          create RightGraph(submitted.organization_id, submitted.state_id, graph_id)
            as right else RightExists {{}}
          set state.active = false
          set state.revision = state.revision + 1
{right_assignments}        }}
        else => no_effect
      }}
    }}
    return Applied {{}}
  }}
}}
"#
    );
    let bundle = compile_contract_source(&source)
        .expect("each selected arm is below the graph ceiling even though their sum is not");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V18);
    let aggregate_bounded = source.replace(
        "input states: list<State, 1..1>",
        "input states: list<State, 1..1> aggregate_bytes <= 1024",
    );
    let aggregate_bundle = compile_contract_source(&aggregate_bounded)
        .expect("aggregate-bounded graph proof also charges the maximum selected arm");
    assert_eq!(aggregate_bundle.format_version(), BUNDLE_FORMAT_VERSION_V18);
}
