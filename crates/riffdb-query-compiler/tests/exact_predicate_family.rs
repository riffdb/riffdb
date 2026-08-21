//! ADR-0134 semantic-family compiler acceptance without provider activation.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{PlannerDiagnosticCode, compile_exact_predicate_query_v1};
use riffdb_query_ir::{
    ExactPredicateOperatorV1, QUERY_IR_VERSION_EXACT_PREDICATE_V1, SymbolicCatalog,
};
use riffdb_riffql_syntax::{RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1, parse_query};
use riffdb_types::EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V4;
use riffdb_types::ProjectionProviderPolicyModeV1;

const CONTRACT: &str = r#"
contract Directory version 1 {
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field email: string<320>
    field state: string<32>
    field created_at: u64
    index by_email (organization_id, email, user_id) text_key(email, binary_utf8_v1)
    index by_state (organization_id, state, user_id)
    index by_created (organization_id, created_at, user_id)
  }
  aggregate Users {
    root User
    partition_by organization_id
    conflict_key (organization_id, user_id)
  }
}
"#;

const QUERY: &str = r#"
query SearchUsers(
  $organization_id: User.organization_id,
  $needle: User.email,
  $states: Set<User.state>,
  $after: User.created_at?,
  $limit: Limit,
  $offset: u64
) {
  many users from User
    where organization_id == $organization_id
      && email contains $needle
      && state not_in $states
      && when $after { created_at >= $after }
    order by created_at desc, user_id asc
    take $limit offset $offset
  aggregate totals from users { exact_count() as total }
  return Found { users: users { user_id email state created_at } totals: totals { total } }
  outcomes Found
}
"#;

fn catalog(source: &str) -> SymbolicCatalog {
    let bundle = compile_contract_source(source).expect("contract");
    SymbolicCatalog::from_bundle(&bundle).expect("catalog")
}

#[test]
fn rich_family_is_finite_canonical_and_provider_complete() {
    let document = parse_query(QUERY).expect("query");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1
    );
    assert_eq!(QUERY_IR_VERSION_EXACT_PREDICATE_V1, 9);

    let first = compile_exact_predicate_query_v1(&document, &catalog(CONTRACT)).expect("family");
    let second = compile_exact_predicate_query_v1(&document, &catalog(CONTRACT)).expect("family");
    assert_eq!(first, second);
    assert_eq!(first.program().members().len(), 2);
    assert_eq!(
        first.value_parameters(),
        &["after", "needle", "organization_id", "states"]
    );
    assert_eq!(first.presence_parameters(), &["after"]);
    assert_eq!(first.limit_parameter(), "limit");
    assert_eq!(first.offset_parameter(), "offset");
    assert_eq!(
        first.program().provider_requirement().policy_mode(),
        ProjectionProviderPolicyModeV1::PartitionAligned
    );
    let descriptor = first
        .program()
        .provider_descriptor()
        .expect("V4 provider descriptor");
    assert_eq!(descriptor.state_identity().layout_version().get(), 4);
    assert_eq!(
        descriptor.state_identity().schema_hash(),
        EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V4
    );
    assert!(
        first
            .program()
            .canonical_bytes()
            .windows(1)
            .any(|byte| byte == [ExactPredicateOperatorV1::NotIn as u8])
    );
}

#[test]
fn one_missing_index_rejects_the_complete_family_with_a_value_free_span() {
    let contract = CONTRACT.replace(
        "    index by_created (organization_id, created_at, user_id)\n",
        "",
    );
    let error =
        compile_exact_predicate_query_v1(&parse_query(QUERY).expect("query"), &catalog(&contract))
            .expect_err("missing order provider must reject");
    assert_eq!(
        error.as_slice()[0].code(),
        PlannerDiagnosticCode::ExactTextProvider
    );
    assert_eq!(error.as_slice()[0].primary().start, 195);
    assert_eq!(error.as_slice()[0].primary().end, 199);
    assert_eq!(
        error.as_slice()[0].summary(),
        "exact predicate family member lacks a declared provider index"
    );
}

#[test]
fn caller_structure_and_incomplete_orders_remain_unrepresentable() {
    let query = QUERY.replace(
        "order by created_at desc, user_id asc",
        "order by created_at desc",
    );
    let error =
        compile_exact_predicate_query_v1(&parse_query(&query).expect("query"), &catalog(CONTRACT))
            .expect_err("key tie breaker must be complete");
    assert_eq!(
        error.as_slice()[0].summary(),
        "exact predicate order requires the complete ascending entity key"
    );

    let literal = QUERY.replace("email contains $needle", "email contains \"secret\"");
    let error = compile_exact_predicate_query_v1(
        &parse_query(&literal).expect("query"),
        &catalog(CONTRACT),
    )
    .expect_err("runtime literal cannot enter the semantic program");
    assert_eq!(
        error.as_slice()[0].summary(),
        "exact predicate values must be typed parameters"
    );
}
