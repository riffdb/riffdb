//! Codes that refuse for more than one reason must say which reason.
//!
//! Two adapters lost time to diagnostics that named the construct but not the
//! rule it broke, because the rules have different repairs and the message did
//! not distinguish them. Each arm below breaks one rule and asserts the
//! attached cause identifies it.

use riffdb_contract_compiler::{
    CompilerDiagnosticCause, CompilerDiagnosticCode, compile_contract_source,
};

fn cause_for(source: &str, code: CompilerDiagnosticCode) -> Option<CompilerDiagnosticCause> {
    let error = compile_contract_source(source).expect_err("source is refused");
    error
        .semantic()
        .expect("semantic diagnostics")
        .as_slice()
        .iter()
        .find(|diagnostic| diagnostic.code() == code)
        .expect("expected diagnostic code")
        .cause()
}

const UNIQUE_OUTSIDE_ROUTE: &str = r#"
contract Registry version 1 {
  entity Item {
    key (item_id: u64)
    field name: string<100>
    unique by_name (name)
  }
  aggregate Items { root Item partition_by item_id conflict_key (item_id) }
}
"#;

const UNIQUE_OVER_OPTIONAL: &str = r#"
contract Registry version 1 {
  entity Item {
    key (tenant_id: uuid, item_id: u64)
    field name: optional<string<100>>
    unique by_name (tenant_id, name)
  }
  aggregate Items { root Item partition_by tenant_id conflict_key (tenant_id, item_id) }
}
"#;

#[test]
fn the_two_unique_key_rules_are_distinguished() {
    assert_eq!(
        cause_for(
            UNIQUE_OUTSIDE_ROUTE,
            CompilerDiagnosticCode::InvalidUniqueKey
        ),
        Some(CompilerDiagnosticCause::UniqueKeyOutsidePartitionRoute)
    );
    assert_eq!(
        cause_for(
            UNIQUE_OVER_OPTIONAL,
            CompilerDiagnosticCode::InvalidUniqueKey
        ),
        Some(CompilerDiagnosticCause::UniqueKeyFieldNotKeyCompatible)
    );
}

const ASSIGN_KEY_FIELD: &str = r#"
contract Registry version 1 {
  entity Item {
    key (tenant_id: uuid, item_id: u64)
    field name: string<100>
  }
  aggregate Items { root Item partition_by tenant_id conflict_key (tenant_id, item_id) }
  command Rekey {
    input request_id: string<128>
    input tenant_id: uuid
    input item_id: u64
    input other_id: u64
    idempotency_key request_id
    mutate Item(tenant_id, item_id) as item else Missing {}
    set item.item_id = other_id
    return Done { item: item }
  }
}
"#;

const ASSIGN_TWICE: &str = r#"
contract Registry version 1 {
  entity Item {
    key (tenant_id: uuid, item_id: u64)
    field name: string<100>
  }
  aggregate Items { root Item partition_by tenant_id conflict_key (tenant_id, item_id) }
  command Rename {
    input request_id: string<128>
    input tenant_id: uuid
    input item_id: u64
    input name: string<100>
    input other: string<100>
    idempotency_key request_id
    mutate Item(tenant_id, item_id) as item else Missing {}
    set item.name = name
    set item.name = other
    return Done { item: item }
  }
}
"#;

#[test]
fn assigning_a_key_field_is_named_as_key_immutability() {
    assert_eq!(
        cause_for(ASSIGN_KEY_FIELD, CompilerDiagnosticCode::InvalidMutation),
        Some(CompilerDiagnosticCause::AssignmentToKeyField)
    );
}

#[test]
fn assigning_a_field_twice_is_named_as_duplication() {
    assert_eq!(
        cause_for(ASSIGN_TWICE, CompilerDiagnosticCode::InvalidMutation),
        Some(CompilerDiagnosticCause::DuplicateFieldAssignment)
    );
}

/// The point of the vocabulary is discrimination. Two different rules under one
/// code must not produce the same cause, or the split achieved nothing.
#[test]
fn the_two_mutation_rules_do_not_share_a_cause() {
    let key = cause_for(ASSIGN_KEY_FIELD, CompilerDiagnosticCode::InvalidMutation);
    let twice = cause_for(ASSIGN_TWICE, CompilerDiagnosticCode::InvalidMutation);
    assert!(key.is_some() && twice.is_some());
    assert_ne!(key, twice);
}
