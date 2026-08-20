#![forbid(unsafe_code)]

//! Exact source-to-module activation and strict V5 codec coverage.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    CompiledNamedQueryPlan, NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1,
    QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
};
use riffdb_types::{ExactTextOperatorV1, ExactTextOrderV1};

const CONTRACT: &str = r#"
contract ExactUsers version 1 {
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field name: string<128>
    index by_name (organization_id, name, user_id) text_key(name, binary_utf8_v1)
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

fn module() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("exact_users").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchUsers", QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("exact module");
    (contract, module)
}

#[test]
fn exact_source_compiles_to_one_sealed_provider_plan_and_v5_round_trips() {
    let (contract, module) = module();
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_EXACT_RESULT_SET_V1
    );
    let query = module.query("SearchUsers").expect("query");
    let CompiledNamedQueryPlan::ExactTextResultV1(exact) = query.plan() else {
        panic!("exact plan kind");
    };
    assert_eq!(exact.operator(), ExactTextOperatorV1::Contains);
    assert_eq!(exact.order(), ExactTextOrderV1::ValueAscEntityKey);
    assert_eq!(exact.needle_parameter(), "needle");
    assert_eq!(exact.limit_parameter(), "limit");
    assert_eq!(exact.offset_parameter(), "offset");
    assert_eq!(exact.authorization()[0].entity(), "User");
    assert!(exact.cost().scanned_index_rows() >= 4_096);

    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("strict exact module decode");
    assert_eq!(decoded.identity(), module.identity());
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
}
