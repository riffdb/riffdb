//! Query-module identity and compatibility for ADR-0134 semantic families.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_BOUNDED_LIMIT_V1,
    QUERY_MODULE_FORMAT_VERSION_EXACT_PREDICATE_V1,
    QUERY_MODULE_FORMAT_VERSION_NULLABLE_EXACT_ORDER_V1, QueryModule, QueryModuleCandidate,
    generate_go_client, generate_python_client, generate_rust_client, generate_typescript_client,
};
use riffdb_types::{QueryModuleName, QueryModuleVersion};

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
  $limit: Limit,
  $offset: u64
) {
  many users from User
    where organization_id == $organization_id
      && email contains $needle
      && state not_in $states
    order by created_at desc, user_id asc
    take $limit offset $offset
  aggregate totals from users { exact_count() as total }
  return Found { users: users { user_id email state created_at } totals: totals { total } }
  outcomes Found
}
"#;

#[test]
fn semantic_family_rotates_module_identity_and_round_trips_by_recompilation() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("directory").expect("module name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchUsers", QUERY).expect("source")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_EXACT_PREDICATE_V1
    );
    let exact = module.queries()[0]
        .exact_predicate_result()
        .expect("exact predicate family");
    assert_eq!(exact.program().members().len(), 1);
    assert_eq!(
        exact.value_parameters(),
        &["needle", "organization_id", "states"]
    );
    assert_eq!(exact.limit_parameter(), "limit");
    assert_eq!(exact.offset_parameter(), "offset");

    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("strict recompile");
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
    assert_eq!(decoded.identity(), module.identity());

    let rust = generate_rust_client(&module, &contract);
    let typescript = generate_typescript_client(&module, &contract);
    let go = generate_go_client(&module, &contract);
    let python = generate_python_client(&module, &contract).expect("Python client");
    for generated in [&rust, &typescript, &go, &python] {
        assert!(generated.contains("SearchUsers"));
        assert!(!generated.contains("ExactPredicateNodeV1"));
        assert!(!generated.contains("provider_requirement"));
    }
}

#[test]
fn bounded_limit_composes_with_present_and_nullable_exact_predicate_families() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let bounded = QUERY.replace("$limit: Limit,", "$limit: Limit<100>,");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("bounded_directory").expect("module name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchUsers", bounded).expect("source")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("bounded module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_BOUNDED_LIMIT_V1
    );
    assert_eq!(
        module.queries()[0]
            .exact_predicate_result()
            .expect("exact predicate")
            .program()
            .max_limit(),
        100
    );

    let nullable_contract = compile_contract_source(
        &CONTRACT
            .replace("field created_at: u64", "field created_at: optional<u64>")
            .replace(
                "index by_created (organization_id, created_at, user_id)",
                "index by_created (organization_id, created_at, user_id) presence(created_at)",
            ),
    )
    .expect("nullable contract");
    let nullable = QUERY
        .replace("$limit: Limit,", "$limit: Limit<100>,")
        .replace(
            "order by created_at desc, user_id asc",
            "order by created_at desc nulls last, user_id asc",
        );
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("bounded_nullable_directory").expect("module name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchUsers", nullable).expect("source")],
    )
    .expect("candidate");
    let module =
        QueryModule::compile(candidate, &nullable_contract).expect("bounded nullable module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_BOUNDED_LIMIT_V1
    );
    assert_eq!(
        module.queries()[0]
            .nullable_exact_predicate_result()
            .expect("nullable exact predicate")
            .program()
            .max_limit(),
        100
    );
}

#[test]
fn explicit_nullable_order_uses_v10_and_old_module_bytes_remain_stable() {
    let contract_source = CONTRACT
        .replace("field created_at: u64", "field created_at: optional<u64>")
        .replace(
            "index by_created (organization_id, created_at, user_id)",
            "index by_created (organization_id, created_at, user_id) presence(created_at)",
        );
    let query = QUERY.replace(
        "order by created_at desc, user_id asc",
        "order by created_at desc nulls last, user_id asc",
    );
    let contract = compile_contract_source(&contract_source).expect("nullable contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("directory").expect("module name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchUsers", query).expect("source")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("nullable module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_NULLABLE_EXACT_ORDER_V1
    );
    let exact = module.queries()[0]
        .nullable_exact_predicate_result()
        .expect("nullable exact predicate family");
    assert_eq!(exact.program().members().len(), 1);
    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("strict nullable recompile");
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
    for generated in [
        generate_rust_client(&module, &contract),
        generate_typescript_client(&module, &contract),
        generate_go_client(&module, &contract),
        generate_python_client(&module, &contract).expect("Python client"),
    ] {
        assert!(generated.contains("SearchUsers"));
        assert!(!generated.contains("NullsLastV1"));
        assert!(!generated.contains("null_placement"));
    }

    let old_contract = compile_contract_source(CONTRACT).expect("old contract");
    let old_candidate = QueryModuleCandidate::new(
        QueryModuleName::new("directory").expect("module name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchUsers", QUERY).expect("source")],
    )
    .expect("old candidate");
    let old_module = QueryModule::compile(old_candidate, &old_contract).expect("old module");
    assert_eq!(
        old_module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_EXACT_PREDICATE_V1
    );
}

#[test]
fn successor_omission_uses_v9_only_with_the_complete_lineage_proof() {
    let predecessor = compile_contract_source(CONTRACT).expect("predecessor");
    let successor =
        compile_contract_source(&CONTRACT.replace("version 1", "version 2")).expect("successor");
    let candidate = || {
        QueryModuleCandidate::new(
            QueryModuleName::new("directory").expect("module name"),
            QueryModuleVersion::new(2).expect("version"),
            vec![NamedQuerySource::new("SearchUsers", QUERY).expect("source")],
        )
        .expect("candidate")
    };
    assert!(QueryModule::compile(candidate(), &successor).is_err());
    let module = QueryModule::compile_with_lineage(candidate(), &[predecessor, successor.clone()])
        .expect("complete lineage proof");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_EXACT_PREDICATE_V1
    );
    let decoded = QueryModule::decode_and_validate_with_lineage(
        module.canonical_bytes(),
        &[
            compile_contract_source(CONTRACT).expect("predecessor"),
            successor,
        ],
    )
    .expect("lineage-aware strict decode");
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
}
