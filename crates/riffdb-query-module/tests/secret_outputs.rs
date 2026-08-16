#![forbid(unsafe_code)]

//! Query-module and application-role secret-output derivation (ADR-0128).

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationSourceManifest, NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_SECRET_OUTPUT_V1,
    QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    ReactiveModuleCompilationError, compile_application_role, compile_reactive_source,
    generate_go_client, generate_mcp_tools, generate_python_client, generate_rust_client,
    generate_typescript_client,
};

const CONTRACT: &str = r#"
contract AuthShape version 1 {
  entity Account {
    key (org_id: uuid)
  }
  entity Session {
    key (org_id: uuid, session_id: uuid)
    field secret token_hash: string<256>
    field expires_at: timestamp
  }
  aggregate AccountRoot {
    root Account
    child Session
    partition_by org_id
    conflict_key (org_id)
  }
}
"#;

const SECRET_QUERY: &str = r#"query GetSession($org: Session.org_id, $id: Session.session_id) {
    one session from Session where org_id == $org && session_id == $id else NotFound
    return Found { session: session { token_hash reveals session.token_hash } }
    outcomes Found | NotFound
}"#;

const PUBLIC_QUERY: &str = r#"query GetSessionExpiry($org: Session.org_id, $id: Session.session_id) {
    one session from Session where org_id == $org && session_id == $id else NotFound
    return Found { session: session { expires_at } }
    outcomes Found | NotFound
}"#;

fn module(contract: &riffdb_contract_ir::ContractBundle) -> QueryModule {
    QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("auth").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![
                NamedQuerySource::new("GetSession", SECRET_QUERY).expect("secret query"),
                NamedQuerySource::new("GetSessionExpiry", PUBLIC_QUERY).expect("public query"),
            ],
        )
        .expect("candidate"),
        contract,
    )
    .expect("module")
}

fn manifest(
    contract: &riffdb_contract_ir::ContractBundle,
    module: &QueryModule,
) -> riffdb_query_module::ApplicationManifest {
    let source = r#"{
  "application": "auth-shape",
  "contract": {"lineage": "AuthShape", "source": "contract.riff", "version": 1},
  "generation": {"go": "generated/go/client.go", "mcp": "generated/mcp/tools.json", "python": "generated/python/client.py", "rust": "generated/rust/client.rs", "typescript": "generated/typescript/client.ts"},
  "migrations": [],
  "query_modules": [{"name": "auth", "queries": [{"name": "GetSession", "source": "queries/get_session.riffq"}, {"name": "GetSessionExpiry", "source": "queries/get_session_expiry.riffq"}], "version": 1}],
  "reactive_modules": [],
  "roles": [
    {"agent_subscriptions": [], "commands": [], "environment": "development", "event_streams": [], "name": "SecretReader", "queries": ["GetSession"], "row_policies": [], "tenant_scope": "global", "watch_queries": []},
    {"agent_subscriptions": [], "commands": [], "environment": "development", "event_streams": [], "name": "PublicReader", "queries": ["GetSessionExpiry"], "row_policies": [], "tenant_scope": "global", "watch_queries": []}
  ],
  "schema": "riffdb.application-source/v6",
  "seed_inputs": []
}"#;
    ApplicationSourceManifest::parse(source)
        .expect("application source")
        .exact_manifest_v2(contract, std::slice::from_ref(module), &[])
        .expect("exact manifest")
}

#[test]
fn module_v4_round_trips_and_mcp_omits_secret_queries() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let module = module(&contract);
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_SECRET_OUTPUT_V1
    );
    let query = module.query("GetSession").expect("secret query");
    assert_eq!(query.plan().secret_outputs().len(), 1);
    assert!(
        query
            .explain_lines()
            .iter()
            .any(|line| { line == "secret_output.Found.session.token_hash=Session.token_hash" })
    );
    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("strict V4 decode");
    assert_eq!(decoded.identity(), module.identity());

    let tools = generate_mcp_tools(&module).expect("safe MCP catalog");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].operation_name, "GetSessionExpiry");
}

#[test]
fn selected_query_derives_secret_visibility_and_role_hash_atom() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let module = module(&contract);
    let manifest = manifest(&contract, &module);
    let secret = compile_application_role(
        &manifest,
        "SecretReader",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("secret role");
    let public = compile_application_role(
        &manifest,
        "PublicReader",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("public role");

    assert_eq!(secret.secret_outputs().len(), 1);
    assert_eq!(secret.secret_outputs()[0].query(), "GetSession");
    assert_eq!(secret.secret_outputs()[0].entity(), "Session");
    assert_eq!(secret.secret_outputs()[0].field(), "token_hash");
    let visibility = secret
        .internal_grant()
        .field_visibility()
        .iter()
        .find(|entry| entry.entity_type() == secret.secret_outputs()[0].internal_entity_id())
        .expect("Session visibility");
    assert_eq!(
        visibility.secret_fields(),
        [secret.secret_outputs()[0].internal_field_id()]
    );
    assert!(
        !visibility
            .fields()
            .contains(&secret.secret_outputs()[0].internal_field_id())
    );
    assert!(public.secret_outputs().is_empty());
    assert!(
        public
            .internal_grant()
            .field_visibility()
            .iter()
            .all(|entry| entry.secret_fields().is_empty())
    );
    assert_ne!(secret.identity(), public.identity());
}

#[test]
fn reactive_watch_cannot_reference_a_secret_output_query() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let module = module(&contract);
    let source = r#"reactive AuthActivity version 1 {
  watch SessionWatch($org: Session.org_id, $id: Session.session_id) query GetSession updates reset;
}"#;
    let error = compile_reactive_source(source, &contract, std::slice::from_ref(&module))
        .expect_err("secret query is absent from reactive catalog");
    let ReactiveModuleCompilationError::Semantic(diagnostics) = error else {
        panic!("expected source-spanned semantic diagnostic");
    };
    assert_eq!(
        diagnostics[0].code(),
        riffdb_query_compiler::ReactiveCompileDiagnosticCode::UnknownSymbol
    );
    let query_start = source.find("GetSession").expect("query reference");
    assert!(diagnostics[0].span().start() <= query_start);
    assert!(diagnostics[0].span().end() >= query_start + "GetSession".len());
}

#[test]
fn generated_languages_publish_secret_metadata_and_redacted_debug_surfaces() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let module = module(&contract);
    let rust = generate_rust_client(&module, &contract);
    let go = generate_go_client(&module, &contract);
    let typescript = generate_typescript_client(&module, &contract);
    let python = generate_python_client(&module, &contract).expect("Python client");

    assert!(rust.contains("GET_SESSION_SECRET_OUTPUTS"));
    assert!(rust.contains("<secret outputs redacted>"));
    assert!(!rust.contains("derive(Clone, Debug, Eq, PartialEq)]\npub struct GetSessionFound"));

    assert!(go.contains("GetSessionSecretOutputs"));
    assert!(go.contains("func (GetSessionFound) GoString() string"));
    assert!(go.contains("<secret outputs redacted>"));

    assert!(typescript.contains("GET_SESSION_SECRET_OUTPUTS"));
    assert!(typescript.contains("redactGetSessionResult"));
    assert!(typescript.contains("redactedSecretOutputs"));

    assert!(python.contains("GET_SESSION_SECRET_OUTPUTS"));
    assert!(python.contains("token_hash"));
    assert!(python.contains("session: GetSessionFoundSession = field(repr=False)"));
}
