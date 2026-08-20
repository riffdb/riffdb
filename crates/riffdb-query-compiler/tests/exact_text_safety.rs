//! Exact-result policy activation and predicate-completeness safety gates.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{PlannerDiagnosticCode, compile_exact_text_query_v1};
use riffdb_query_ir::SymbolicCatalog;
use riffdb_riffql_syntax::parse_query;
use riffdb_types::{ExactTextOperatorV1, ProjectionProviderPolicyModeV1};

const PROTECTED_CONTRACT: &str = r#"
contract ProtectedExactUsers version 1 {
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field name: string<128>
    field active: bool
    index by_name (organization_id, name, user_id) text_key(name, binary_utf8_v1)
    index by_active_name (organization_id, active, name, user_id) text_key(name, binary_utf8_v1)
  }
  aggregate Users {
    root User
    partition_by organization_id
    conflict_key (organization_id, user_id)
  }
  row policy UserAccess on User {
    allow read when user_id == principal.id
  }
}
"#;

const PROTECTED_QUERY: &str = r#"
query SearchUsers(
  $organization_id: User.organization_id,
  $needle: User.name,
  $limit: Limit = 50,
  $offset: u64 = 0
) {
  many users from User
    where organization_id == $organization_id && name contains $needle
    order by name asc, user_id asc
    take $limit offset $offset
  aggregate total from users { exact_count() as value }
  return Found { users: users { user_id name } total: total { value } }
  outcomes Found
}
"#;

const FILTERED_QUERY: &str = r#"
query SearchActiveUsers(
  $organization_id: User.organization_id,
  $needle: User.name,
  $active: User.active?,
  $limit: Limit = 50,
  $offset: u64 = 0
) {
  many users from User
    where organization_id == $organization_id
      && when $active { active == $active }
      && name contains $needle
    order by name desc, user_id asc
    take $limit offset $offset
  aggregate total from users { exact_count() as value }
  return Found { users: users { user_id name active } total: total { value } }
  outcomes Found
}
"#;

fn catalog() -> SymbolicCatalog {
    let bundle = compile_contract_source(PROTECTED_CONTRACT).expect("protected contract");
    SymbolicCatalog::from_bundle(&bundle).expect("catalog")
}

#[test]
fn protected_exact_query_selects_bounded_row_admission() {
    let document = parse_query(PROTECTED_QUERY).expect("exact query");
    let compiled =
        compile_exact_text_query_v1(&document, &catalog()).expect("protected exact plan");
    assert_eq!(
        compiled.family.descriptor().policy_mode(),
        ProjectionProviderPolicyModeV1::BoundedRowAdmission
    );
}

#[test]
fn exact_equality_is_distinct_from_the_required_partition_equality() {
    let source = PROTECTED_QUERY.replace("name contains $needle", "name == $needle");
    let document = parse_query(&source).expect("exact equality query");
    let compiled = compile_exact_text_query_v1(&document, &catalog()).expect("exact equality plan");
    assert_eq!(compiled.operator, ExactTextOperatorV1::Equals);
}

#[test]
fn exact_query_rejects_any_predicate_the_provider_does_not_execute() {
    let query = PROTECTED_QUERY
        .replace(
            "&& name contains $needle",
            "&& active == $active && name contains $needle",
        )
        .replace(
            "$needle: User.name,",
            "$needle: User.name,\n  $active: User.active,",
        );
    let document = parse_query(&query).expect("exact query with typed filter");
    let diagnostics = compile_exact_text_query_v1(&document, &catalog())
        .expect_err("unimplemented exact filter must never be silently omitted");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), PlannerDiagnosticCode::ExactTextProvider);
    assert_eq!(
        diagnostic.summary(),
        "exact result predicate is not implemented by the selected provider"
    );
    assert!(diagnostic.primary().start < diagnostic.primary().end);
}

#[test]
fn one_optional_typed_equality_filter_is_sealed_into_provider_v3() {
    let document = parse_query(FILTERED_QUERY).expect("filtered exact query");
    let compiled = compile_exact_text_query_v1(&document, &catalog()).expect("filtered exact plan");
    let filter = compiled.filter.expect("compiled filter");
    assert_eq!(filter.field_name(), "active");
    assert_eq!(filter.parameter(), "active");
    assert_eq!(
        compiled.order,
        riffdb_types::ExactTextOrderV1::ValueDescEntityKey
    );
    assert_eq!(
        compiled
            .family
            .descriptor()
            .state_identity()
            .layout_version()
            .get(),
        3
    );
}
