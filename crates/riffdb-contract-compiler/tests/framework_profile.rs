#![forbid(unsafe_code)]

//! Framework-agnostic identity/session acceptance profile (ADR-0117, WP-598).

use riffdb_contract_compiler::{compile_contract_source, validate_contract_source};
use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V9, BUNDLE_FORMAT_VERSION_V11, BindingMode, ContractBundle,
    ExpressionKind, Instruction,
};
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
fn workflow_initialization_and_self_transition_survive_v11_reveal_ir() {
    let bundle = compile_contract_source(PROFILE).expect("generic framework profile compiles");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V11);

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
fn token_handout_flow_carries_the_exact_static_secret_reveal() {
    // ADR-0118 Amendment 1 / WP-600: the issuance outcome hands out the
    // secret-classified digest exactly once, with one compiler-checked reveal
    // over the same direct bound-field expression.
    let bundle = compile_contract_source(PROFILE).expect("generic framework profile compiles");
    let token = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "VerificationToken")
        .expect("verification token entity");
    let digest_field = token
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "token_digest")
        .expect("token digest field")
        .id();
    assert!(
        bundle.schema().is_secret_field(token.id(), digest_field),
        "the stored digest must stay secret-classified"
    );

    let issue = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "IssueVerificationToken")
        .expect("issuance command");
    let handout = issue
        .instructions()
        .iter()
        .find_map(|instruction| match instruction {
            Instruction::Return(construction) => Some(construction),
            _ => None,
        })
        .expect("issuance success return");
    let outcome = issue
        .outcomes()
        .iter()
        .find(|outcome| outcome.name() == "VerificationTokenIssued")
        .expect("issued outcome");
    assert_eq!(handout.outcome_id(), outcome.id());
    let outcome_digest = outcome
        .payload()
        .fields()
        .iter()
        .find(|field| field.name() == "token_digest")
        .expect("outcome handout field")
        .id();
    let flow = handout
        .payload()
        .fields()
        .iter()
        .find(|field| field.field_id() == outcome_digest)
        .expect("handout construction field");
    let token_binding = issue
        .bindings()
        .iter()
        .position(|binding| binding.entity_type() == token.id())
        .expect("token binding");
    assert!(
        matches!(
            issue.expressions().get(flow.expression()).map(|node| node.kind()),
            Some(ExpressionKind::BoundField { binding, field })
                if binding.get() as usize == token_binding && *field == digest_field
        ),
        "the handout must stay one direct secret-field read"
    );
    let reveal = issue
        .secret_reveals()
        .iter()
        .find(|reveal| reveal.expression() == flow.expression())
        .expect("one-time digest handout reveal");
    assert_eq!(reveal.source_binding().get() as usize, token_binding);
    assert_eq!(reveal.source_field(), digest_field);
    assert_eq!(
        reveal.destination(),
        riffdb_contract_ir::SecretRevealDestinationV1::OutcomeField {
            outcome: outcome.id(),
            field: outcome_digest,
        }
    );
    assert_eq!(bundle.ir_version(), 11);
}

#[test]
fn unique_entity_delete_compiles_without_an_input_computable_release_conflict() {
    // WP-607 unseals the profile's session sign-out-by-delete shape. The
    // delete removes the exact snapshot-derived unique entry in the
    // authoritative write transaction; it must not invent a caller-computable
    // conflict for a value that is visible only in the deleted row.
    let source = PROFILE
        .replacen(
            "    unique session_token (organization_id, token_digest)",
            "    unique session_token (organization_id, token_digest)\n    delete_policy no_inbound",
            1,
        )
        .replacen(
            "  command RevokeSession {",
            concat!(
                "  bulk command DeleteSessions {\n",
                "    input request_id: uuid\n",
                "    input organization_id: uuid\n",
                "    input user_id: uuid\n",
                "    input session_ids: list<uuid, 1..8>\n",
                "    idempotency_key request_id\n",
                "    for session_id in session_ids {\n",
                "      delete Session(organization_id, user_id, session_id) as session\n",
                "        else DeleteSessionMissing {}\n",
                "    }\n",
                "    return SessionsDeleted {}\n",
                "  }\n\n",
                "  command RevokeSession {",
            ),
            1,
        );
    let bundle = compile_contract_source(&source)
        .expect("unique-carrying entity delete compiles after the audited unseal");
    let delete = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "DeleteSessions")
        .expect("DeleteSessions plan");
    let binding = delete
        .bindings()
        .iter()
        .find(|binding| binding.mode() == BindingMode::Delete)
        .expect("DeleteSessions delete binding");
    let session = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Session")
        .expect("Session schema");
    assert_eq!(binding.entity_type(), session.id());
    assert!(
        delete.unique_conflicts().is_empty(),
        "snapshot-derived release is not represented as a caller-computable conflict"
    );

    let unsafe_source = source.replacen("\n    delete_policy no_inbound", "", 1);
    let error = validate_contract_source(&unsafe_source)
        .expect_err("unsealed unique delete still requires an explicit deletion policy");
    let diagnostic = error
        .semantic()
        .expect("semantic delete-policy diagnostic")
        .as_slice()
        .iter()
        .find(|diagnostic| diagnostic.code().as_str() == "RDB-C045")
        .expect("unsafe delete has the retained policy-specific diagnostic");
    let rejected_delete = unsafe_source
        .find("delete Session(organization_id, user_id, session_id)")
        .expect("unsafe delete source marker")
        + "delete ".len();
    assert_eq!(diagnostic.primary_span().start() as usize, rejected_delete);
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
