//! Atomic unary delete/preimage compiler conformance.

use riffdb_contract_compiler::{CompilerDiagnosticCode, compile_contract_source};
use riffdb_contract_ir::{BindingMode, ContractBundle, SecretRevealDestinationV1};

const UNARY_DELETE_PREIMAGE_SOURCE: &str = r#"
contract UnaryDeletePreimage version 1 {
  entity OneTimeToken {
    key (organization_id: uuid, token_id: uuid)
    field identifier: string<256>
    field secret value: string<512>
    delete_policy no_inbound
  }

  aggregate OneTimeTokens {
    root OneTimeToken
    partition_by organization_id
    conflict_key (organization_id, token_id)
  }

  command ConsumeToken {
    input request_id: uuid
    input organization_id: uuid
    input token_id: uuid
    idempotency_key request_id

    delete OneTimeToken(organization_id, token_id)
      as token
      else TokenMissing {}

    return TokenConsumed {
      token_id: token.token_id,
      identifier: token.identifier,
      value: token.value reveals token.value
    }
  }
}
"#;

#[test]
fn unary_no_inbound_delete_returns_the_declared_preimage() {
    let bundle = compile_contract_source(UNARY_DELETE_PREIMAGE_SOURCE)
        .expect("one exact-key no_inbound delete must compile");
    let command = bundle.commands().first().expect("consume command");
    assert!(command.collection_expansion().is_none());
    assert_eq!(command.bindings().len(), 1);
    let binding = &command.bindings()[0];
    assert_eq!(binding.mode(), BindingMode::Delete);
    assert_eq!(binding.accessed_fields().len(), 3);
    assert_eq!(command.secret_reveals().len(), 1);
    assert!(matches!(
        command.secret_reveals()[0].destination(),
        SecretRevealDestinationV1::OutcomeField { .. }
    ));
    assert!(command.requires_ir_v5());
    assert!(!command.requires_ir_v16());
    assert_eq!(
        ContractBundle::decode(bundle.canonical_bytes()).expect("v5 round trip"),
        bundle,
    );
}

#[test]
fn unary_delete_complete_record_access_remains_explicit() {
    let source = UNARY_DELETE_PREIMAGE_SOURCE.replace(
        concat!(
            "      token_id: token.token_id,\n",
            "      identifier: token.identifier,\n",
            "      value: token.value reveals token.value\n",
        ),
        "      token: token reveals token.value\n",
    );
    let bundle = compile_contract_source(&source).expect("complete preimage return compiles");
    let binding = &bundle.commands()[0].bindings()[0];
    assert!(binding.complete_record_access());
}

#[test]
fn multiple_ordinary_deletes_are_a_source_diagnostic_not_invalid_ir() {
    let source = UNARY_DELETE_PREIMAGE_SOURCE.replace(
        "    delete OneTimeToken(organization_id, token_id)\n      as token\n      else TokenMissing {}",
        concat!(
            "    delete OneTimeToken(organization_id, token_id)\n",
            "      as token\n",
            "      else TokenMissing {}\n",
            "    delete OneTimeToken(organization_id, token_id)\n",
            "      as duplicate\n",
            "      else DuplicateMissing {}",
        ),
    );
    let rejected = source
        .rfind("OneTimeToken(organization_id, token_id)")
        .expect("second delete target");
    assert_source_diagnostic_at(
        &source,
        CompilerDiagnosticCode::InvalidDeletePolicy,
        rejected,
    );
}

#[test]
fn a_second_alias_for_the_deleted_target_is_rejected_at_source() {
    let source = UNARY_DELETE_PREIMAGE_SOURCE.replace(
        "      else TokenMissing {}",
        concat!(
            "      else TokenMissing {}\n",
            "    read OneTimeToken(organization_id, token_id)\n",
            "      as duplicate\n",
            "      else DuplicateMissing {}",
        ),
    );
    let rejected = source
        .rfind("OneTimeToken(organization_id, token_id)")
        .expect("second target alias");
    assert_source_diagnostic_at(&source, CompilerDiagnosticCode::InvalidBinding, rejected);
}

#[test]
fn a_delete_preimage_cannot_be_a_set_target() {
    let source = UNARY_DELETE_PREIMAGE_SOURCE.replace(
        "    return TokenConsumed {",
        "    set token.identifier = token.identifier\n    return TokenConsumed {",
    );
    let rejected = source.find("token.identifier =").expect("set target");
    assert_source_diagnostic_at(&source, CompilerDiagnosticCode::InvalidMutation, rejected);
}

#[test]
fn ordinary_restrict_delete_is_rejected_at_the_delete_source() {
    let source = r#"
contract UnaryRestrictDelete version 1 {
  entity Parent {
    key (tenant_id: uuid, parent_id: uuid)
    delete_policy restrict Child.by_parent
  }
  entity Child {
    key (tenant_id: uuid, parent_id: uuid, child_id: uuid)
    index by_parent (tenant_id, parent_id)
    reference parent (tenant_id, parent_id) -> Parent(tenant_id, parent_id)
  }
  aggregate Family {
    root Parent
    child Child
    partition_by tenant_id
    conflict_key (tenant_id)
  }
  command DeleteParent {
    input request_id: uuid
    input tenant_id: uuid
    input parent_id: uuid
    idempotency_key request_id
    delete Parent(tenant_id, parent_id) as parent else Missing {} restrict Referenced {}
    return Deleted { parent_id: parent.parent_id }
  }
}
"#;
    let rejected = source
        .find("Parent(tenant_id, parent_id) as parent")
        .expect("delete target");
    assert_source_diagnostic_at(
        source,
        CompilerDiagnosticCode::InvalidDeletePolicy,
        rejected,
    );
}

#[test]
fn ordinary_cascade_delete_is_rejected_at_the_delete_source() {
    let source = r#"
contract UnaryCascadeDelete version 1 {
  entity Parent {
    key (tenant_id: uuid, parent_id: uuid)
    delete_policy cascade {
      relationship Child.parent using Child.by_parent maximum 8
    }
  }
  entity Child {
    key (tenant_id: uuid, parent_id: uuid, child_id: uuid)
    index by_parent (tenant_id, parent_id)
    reference parent (tenant_id, parent_id) -> Parent(tenant_id, parent_id)
    delete_policy no_inbound
  }
  aggregate Family {
    root Parent
    child Child
    partition_by tenant_id
    conflict_key (tenant_id)
  }
  command DeleteParent {
    input request_id: uuid
    input tenant_id: uuid
    input parent_id: uuid
    idempotency_key request_id
    delete Parent(tenant_id, parent_id) as parent else Missing {} cascade LimitExceeded {}
    return Deleted { parent_id: parent.parent_id }
  }
}
"#;
    let rejected = source
        .find("Parent(tenant_id, parent_id) as parent")
        .expect("delete target");
    assert_source_diagnostic_at(
        source,
        CompilerDiagnosticCode::InvalidDeletePolicy,
        rejected,
    );
}

fn assert_source_diagnostic_at(
    source: &str,
    expected: CompilerDiagnosticCode,
    expected_start: usize,
) {
    let error = compile_contract_source(source).expect_err("source shape must be rejected");
    let diagnostics = error.semantic().expect("semantic diagnostics").as_slice();
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code() == expected)
        .unwrap_or_else(|| panic!("expected {expected:?}, got {diagnostics:?}"));
    assert_eq!(diagnostic.primary_span().start() as usize, expected_start);
    assert!(
        diagnostics
            .iter()
            .all(|diagnostic| diagnostic.code() != CompilerDiagnosticCode::InvalidIr),
        "valid source rejection must not surface RDB-C023: {diagnostics:?}",
    );
}
