#![forbid(unsafe_code)]

//! Framework-agnostic identity/session acceptance profile (ADR-0117, WP-598).

use riffdb_contract_compiler::{compile_contract_source, validate_contract_source};
use riffdb_contract_ir::{BUNDLE_FORMAT_VERSION_V9, ContractBundle, ExpressionKind, Instruction};
use riffdb_types::CanonicalValue;

const PROFILE: &str =
    include_str!("../../../fixtures/adapters/framework-profile/riffdb/contract.riff");

const ORDINARY_WORKFLOW_CREATE: &str = r#"
contract OrdinaryWorkflowCreate version 1 {
  enum State { Ready, Revoked }
  entity Session {
    key (tenant_id: uuid, session_id: uuid)
    field state: State
    field label: string<64>
  }
  aggregate Sessions {
    root Session
    partition_by tenant_id
    conflict_key (tenant_id, session_id)
  }
  workflow Lifecycle {
    entity Session
    state state
    initial Ready
    transition Revoke from (Ready) to Revoked
  }
  command CreateSession {
    input request_id: uuid
    input tenant_id: uuid
    input session_id: uuid
    input label: string<64>
    idempotency_key request_id
    create Session(tenant_id, session_id) as session else Exists {}
    set session.label = label
    return Created { session: session }
  }
}
"#;

#[test]
fn workflow_initialization_and_self_transition_are_closed_v9_ir() {
    let bundle = compile_contract_source(PROFILE).expect("generic framework profile compiles");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V9);

    let workflow = bundle
        .workflows()
        .workflows()
        .iter()
        .find(|workflow| workflow.name() == "SessionLifecycle")
        .expect("session workflow");
    let initial = workflow
        .initial_state()
        .expect("compiler-owned initial state");
    let refresh = workflow
        .transitions()
        .iter()
        .find(|transition| transition.name() == "Refresh")
        .expect("refresh transition");
    assert_eq!(refresh.destination(), initial);
    assert_eq!(refresh.source_states(), [initial]);

    let create = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "CreateUserAccountSessions")
        .expect("atomic signup command");
    let state_field = workflow.state_field();
    let initializer = create
        .instructions()
        .iter()
        .find_map(|instruction| match instruction {
            Instruction::SetField {
                binding,
                field,
                value,
            } if *field == state_field
                && create.bindings()[binding.get() as usize].entity_type() == workflow.entity() =>
            {
                Some(*value)
            }
            _ => None,
        })
        .expect("compiler injected workflow initial state");
    assert!(matches!(
        create.expressions().get(initializer).map(|node| node.kind()),
        Some(ExpressionKind::Constant(CanonicalValue::Enum { variant_id, .. }))
            if *variant_id == initial
    ));

    let decoded = ContractBundle::decode(bundle.canonical_bytes()).expect("v9 bundle round trip");
    assert_eq!(decoded, bundle);
}

#[test]
fn checked_in_workflow_initial_bundle_pins_the_v9_compatibility_boundary() {
    let bytes: &[u8] = include_bytes!("../../../fixtures/compiler/workflow-initial/bundle.bin");
    let pinned_hash =
        include_str!("../../../fixtures/compiler/workflow-initial/bundle-hash.txt").trim_end();
    let decoded = ContractBundle::decode(bytes).expect("the pinned v9 fixture must decode");
    assert_eq!(decoded.format_version(), BUNDLE_FORMAT_VERSION_V9);
    let workflow = decoded
        .workflows()
        .workflows()
        .iter()
        .find(|workflow| workflow.name() == "SessionLifecycle")
        .expect("session workflow");
    let initial = workflow.initial_state().expect("pinned initial state");
    assert!(workflow.transitions().iter().any(|transition| {
        transition.name() == "Refresh"
            && transition.source_states() == [initial]
            && transition.destination() == initial
    }));
    let rendered: String = decoded
        .bundle_hash()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(rendered, pinned_hash, "the pinned fixture hash must match");
}

#[test]
fn ordinary_create_receives_the_same_compiler_owned_initial_state() {
    let bundle =
        compile_contract_source(ORDINARY_WORKFLOW_CREATE).expect("ordinary create compiles");
    let workflow = &bundle.workflows().workflows()[0];
    let create = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "CreateSession")
        .expect("create command");
    let initial = workflow.initial_state().expect("initial state");
    assert!(create.instructions().iter().any(|instruction| {
        let Instruction::SetField { field, value, .. } = instruction else {
            return false;
        };
        *field == workflow.state_field()
            && matches!(
                create.expressions().get(*value).map(|node| node.kind()),
                Some(ExpressionKind::Constant(CanonicalValue::Enum { variant_id, .. }))
                    if *variant_id == initial
            )
    }));
}

#[test]
fn workflow_without_initial_remains_ineligible_for_ordinary_create() {
    let source = ORDINARY_WORKFLOW_CREATE.replacen("    initial Ready\n", "", 1);
    let error = validate_contract_source(&source)
        .expect_err("ordinary create cannot invent an undeclared workflow state");
    assert!(
        error
            .semantic()
            .expect("semantic diagnostic")
            .as_slice()
            .iter()
            .any(|diagnostic| diagnostic.code().as_str() == "RDB-C012"),
        "definite assignment must reject the missing workflow state"
    );
}

#[test]
fn unknown_initial_state_has_a_source_spanned_semantic_error() {
    let source = PROFILE.replacen("initial Active", "initial MissingState", 1);
    let expected_start = source.find("MissingState").expect("fixture marker");
    let error = validate_contract_source(&source).expect_err("unknown state must fail");
    let diagnostic = error
        .semantic()
        .expect("semantic diagnostic")
        .as_slice()
        .iter()
        .find(|diagnostic| diagnostic.code().as_str() == "RDB-C034")
        .expect("workflow diagnostic");
    assert_eq!(diagnostic.primary_span().start() as usize, expected_start);
    assert_eq!(
        diagnostic.primary_span().end() as usize,
        expected_start + 12
    );
}

#[test]
fn caller_authored_workflow_state_assignment_remains_rejected() {
    let source = PROFILE.replacen(
        "set session.token_digest = signup.token_digest",
        "set session.state = SessionState.Active\n      set session.token_digest = signup.token_digest",
        1,
    );
    let expected_start = source
        .find("session.state")
        .expect("forbidden assignment marker");
    let error = validate_contract_source(&source).expect_err("caller state assignment must fail");
    let diagnostic = error
        .semantic()
        .expect("semantic diagnostic")
        .as_slice()
        .iter()
        .find(|diagnostic| diagnostic.code().as_str() == "RDB-C034")
        .expect("workflow diagnostic");
    assert_eq!(diagnostic.primary_span().start() as usize, expected_start);
}
