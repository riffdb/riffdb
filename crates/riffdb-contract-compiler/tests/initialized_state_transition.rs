//! Compiler-sealed initialized-state transition conformance (ADR-0153).

use riffdb_contract_compiler::{CompilerDiagnosticCode, compile_contract_source};
use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V17, BindingMode, ContractBundle, EXECUTABLE_IR_VERSION_V17,
    GRAMMAR_VERSION_V17,
};

const SOURCE: &str = r#"
contract InitializedState version 1 {
  entity State {
    key (organization_id: uuid, state_id: uuid)
    field active: bool
    field revision: u64
    field payload: bytes<64>
  }

  aggregate States {
    root State
    partition_by organization_id
    conflict_key (organization_id, state_id)
  }

  command PutState {
    input request_id: uuid
    input organization_id: uuid
    input state_id: uuid
    input payload: bytes<64>
    idempotency_key request_id
    init_or_mutate State(organization_id, state_id) as state initialize {
      active: false,
      revision: 0,
    }
    require current_revision: state.revision == 0 else Changed {}
    set state.active = true
    set state.payload = payload
    set state.revision = state.revision + 1
    return Written {}
  }

  bulk command PutStates {
    input request_id: uuid
    input states: list<State, 1..100>
    idempotency_key request_id
    for state_input in states {
      init_or_mutate State(state_input.organization_id, state_input.state_id)
        as state initialize {
          active: false,
          revision: 0,
        }
      require current_revision: state.revision == 0 else Changed {}
      set state.active = true
      set state.payload = state_input.payload
      set state.revision = state.revision + 1
    }
    return Written {}
  }
}
"#;

#[test]
fn ordinary_and_bulk_initialized_mutation_select_v17_and_round_trip() {
    let bundle = compile_contract_source(SOURCE).expect("initialized mutation compiles");
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V17);
    assert_eq!(bundle.grammar_version(), GRAMMAR_VERSION_V17);
    assert_eq!(bundle.ir_version(), EXECUTABLE_IR_VERSION_V17);
    assert_eq!(bundle.commands().len(), 2);
    for command in bundle.commands() {
        let binding = &command.bindings()[0];
        assert_eq!(binding.mode(), BindingMode::InitOrMutate);
        assert_eq!(binding.initializer().len(), 2);
        assert!(binding.failure().is_none());
        assert!(command.requires_ir_v17());
    }
    assert_eq!(
        ContractBundle::decode(bundle.canonical_bytes()).expect("v17 round trip"),
        bundle,
    );
}

#[test]
fn checked_in_v17_fixture_is_byte_exact_and_decodes_canonically() {
    let source =
        include_str!("../../../fixtures/compiler/initialized-state-transition/contract.riff");
    let expected =
        include_bytes!("../../../fixtures/compiler/initialized-state-transition/bundle.bin");
    let expected_hash =
        include_str!("../../../fixtures/compiler/initialized-state-transition/bundle-hash.txt")
            .trim_end();
    let bundle = compile_contract_source(source).expect("checked V17 fixture compiles");
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
        ContractBundle::decode(expected).expect("checked V17 fixture decodes"),
        bundle
    );
}

#[test]
fn initializer_cannot_read_bound_state() {
    let source = SOURCE.replace("active: false,", "active: state.active,");
    assert_source_diagnostic(&source, CompilerDiagnosticCode::UnknownName, "state.active");
}

#[test]
fn initializer_rejects_key_unknown_duplicate_and_mistyped_fields() {
    for (replacement, code, excerpt) in [
        (
            "organization_id: organization_id,\n      active: false,",
            CompilerDiagnosticCode::InvalidCreation,
            "organization_id: organization_id",
        ),
        (
            "unknown: false,\n      active: false,",
            CompilerDiagnosticCode::InvalidCreation,
            "unknown",
        ),
        (
            "active: false,\n      active: false,",
            CompilerDiagnosticCode::InvalidCreation,
            "active",
        ),
        ("active: 1,", CompilerDiagnosticCode::TypeMismatch, "1"),
    ] {
        let source = SOURCE.replacen("active: false,", replacement, 1);
        assert_source_diagnostic(&source, code, excerpt);
    }
}

#[test]
fn absent_path_reads_require_initializer_or_dominating_assignment() {
    let source = SOURCE.replacen("      revision: 0,\n", "", 1);
    assert_source_diagnostic(
        &source,
        CompilerDiagnosticCode::InvalidCreation,
        "state.revision == 0",
    );
}

#[test]
fn empty_initializer_is_valid_when_common_effects_complete_the_absent_postimage() {
    let source = SOURCE
        .replace("active: false,\n", "")
        .replace("revision: 0,\n", "")
        .replace(
            "set state.revision = state.revision + 1",
            "set state.revision = 1",
        )
        .replace(
            "require current_revision: state.revision == 0 else Changed {}\n",
            "",
        );
    let bundle = compile_contract_source(&source).expect("common effects complete the record");
    assert!(
        bundle
            .commands()
            .iter()
            .all(|command| command.bindings()[0].initializer().is_empty())
    );
}

#[test]
fn initializer_values_drive_relationship_and_unique_proofs() {
    let source = r#"
contract InitializedProofs version 1 {
  entity Organization {
    key (organization_id: uuid)
  }
  entity Parent {
    key (organization_id: uuid, parent_id: uuid)
  }
  entity Child {
    key (organization_id: uuid, child_id: uuid)
    field parent_id: uuid
    field email: string<128>
    unique child_email (organization_id, email)
    reference parent (organization_id, parent_id) -> Parent(organization_id, parent_id)
  }
  aggregate Families {
    root Organization
    child Parent
    child Child
    partition_by organization_id
    conflict_key (organization_id)
  }
  command PutChild {
    input request_id: uuid
    input organization_id: uuid
    input parent_id: uuid
    input child_id: uuid
    input email: string<128>
    idempotency_key request_id
    read Parent(organization_id, parent_id) as parent else ParentMissing {}
    init_or_mutate Child(organization_id, child_id) as stored initialize {
      parent_id: parent_id,
      email: email,
    }
    return Written {}
  }
}
"#;
    let bundle = compile_contract_source(source).expect("initializer proofs compile");
    let command = &bundle.commands()[0];
    assert_eq!(command.relationship_checks().len(), 1);
    assert_eq!(command.unique_conflicts().len(), 1);
}

fn assert_source_diagnostic(
    source: &str,
    expected: CompilerDiagnosticCode,
    expected_excerpt: &str,
) {
    let error = compile_contract_source(source).expect_err("source must fail closed");
    let diagnostics = error.semantic().expect("semantic diagnostics").as_slice();
    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code() == expected)
        .unwrap_or_else(|| panic!("expected {expected:?}, got {diagnostics:?}"));
    let span = diagnostic.primary_span();
    assert_eq!(
        &source[span.start() as usize..span.end() as usize],
        expected_excerpt,
        "diagnostic must retain its exact source span"
    );
}
