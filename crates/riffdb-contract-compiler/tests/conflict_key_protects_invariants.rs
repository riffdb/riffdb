//! A declared conflict key cannot fail to protect the invariants it must.
//!
//! `SPEC.md` §8.3 once illustrated a diagnostic, `ADB-C042`, for an invariant
//! "not protected by the declared conflict key". No such pass exists, and
//! ADR-0254 records why one is not needed: the condition is unreachable.
//!
//! An aggregate invariant is evaluated against the aggregate root. Two commands
//! can jointly violate it only if they can write the same root while deriving
//! *different* conflict key values, so that they never serialize against each
//! other. A conflict key component may only name a field of the aggregate
//! root's primary key, so every conflict key is a function of the root key, and
//! two writes to one root always derive one conflict value.
//!
//! These tests enumerate the categories of name a conflict key could contain.
//! If the grammar later admits a component outside the root key -- a child key
//! field, a stored root field, another aggregate's field -- one of them fails,
//! and `ADB-C042` becomes necessary rather than redundant.
// req: POC-003, CMP-014

use riffdb_contract_compiler::{CompilerDiagnosticCode, compile_contract_source};

fn ledger(conflict: &str) -> String {
    format!(
        r#"
contract Ledger version 1 {{
  entity Account {{
    key (tenant: uuid, account_id: uuid)
    field balance: i64
  }}
  entity Entry {{
    key (tenant: uuid, account_id: uuid, entry_id: uuid)
    field amount: i64
  }}
  entity Note {{
    key (tenant: uuid, note_id: uuid)
    field body: i64
  }}
  aggregate Accounts {{
    root Account
    child Entry
    partition_by tenant
    conflict_key {conflict}
    invariant non_negative: balance >= 0
  }}
  aggregate Notes {{
    root Note
    partition_by tenant
    conflict_key (tenant, note_id)
  }}
  command AddEntry {{
    input request_key: string<128>
    input tenant: uuid
    input account_id: uuid
    input entry_id: uuid
    input amount: i64
    idempotency_key request_key
    mutate Entry(tenant, account_id, entry_id) as entry else EntryMissing {{}}
    set entry.amount = amount
    return Added {{ entry: entry }}
  }}
}}
"#
    )
}

fn rejection(conflict: &str) -> CompilerDiagnosticCode {
    let error = compile_contract_source(&ledger(conflict))
        .expect_err("a conflict key outside the aggregate root key is not declarable");
    let riffdb_contract_compiler::CompilationError::Semantic(diagnostics) = error else {
        panic!("expected a semantic rejection for conflict_key {conflict}");
    };
    diagnostics
        .as_slice()
        .first()
        .expect("one diagnostic")
        .code()
}

/// Every subset of the aggregate root's key fields is a legal conflict key, and
/// every one of them is constant across writes to one root.
// req: POC-003, CMP-014
#[test]
fn a_conflict_key_may_name_the_aggregate_root_key_fields() {
    for conflict in [
        "(tenant, account_id)",
        "(account_id, tenant)",
        "(tenant)",
        "(account_id)",
    ] {
        assert!(
            compile_contract_source(&ledger(conflict)).is_ok(),
            "conflict_key {conflict} names only root key fields and must compile"
        );
    }
}

/// A child key field would let two writes to one root derive different conflict
/// values. It is not a name a conflict key can resolve.
// req: POC-003, CMP-014
#[test]
fn a_conflict_key_may_not_name_a_child_key_field() {
    assert_eq!(
        rejection("(tenant, entry_id)"),
        CompilerDiagnosticCode::UnknownName,
        "a child key field is not in scope for the aggregate's conflict key"
    );
}

/// A stored root field is not derivable from validated inputs, so it cannot
/// name a conflict component either (`RDB-C016`).
// req: POC-003, CMP-014
#[test]
fn a_conflict_key_may_not_name_a_stored_root_field() {
    assert_eq!(
        rejection("(tenant, balance)"),
        CompilerDiagnosticCode::ConflictNotInputComputable,
        "a conflict key must be computable from validated inputs before the read"
    );
}

/// Nor a field belonging to some other aggregate in the same contract.
// req: POC-003, CMP-014
#[test]
fn a_conflict_key_may_not_name_another_aggregates_field() {
    assert_eq!(
        rejection("(tenant, note_id)"),
        CompilerDiagnosticCode::UnknownName,
        "another aggregate's key field is not in scope"
    );
}
