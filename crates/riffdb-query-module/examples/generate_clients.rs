#![forbid(unsafe_code)]

//! Regenerates the checked TicketDesk Rust and TypeScript clients.

use std::env;
use std::fs;
use std::path::PathBuf;

use riffdb_contract_compiler::{
    compile_contract_source, compile_contract_successor, compile_migration_source,
};
use riffdb_query_module::{
    ApplicationLock, ApplicationMigrationLockInput, ApplicationSourceManifest,
    GeneratedApplicationArtifact, GeneratedApplicationArtifactKind, NamedQuerySource, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, compile_application_role,
    compile_application_role_v2, generate_go_client, generate_mcp_commands, generate_mcp_tools,
    generate_python_client, generate_rust_client, generate_typescript_client,
};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const APPLICATION_SOURCE: &str = r#"{
  "schema":"riffdb.application-source/v1",
  "application":"ticketdesk",
  "contract":{"source":"examples/app-baseline/contracts/ticketdesk.riff","lineage":"TicketDesk","version":1},
  "generation":{"mcp":"fixtures/query-modules/ticketdesk.mcp.json","rust":"fixtures/query-modules/ticketdesk.rs","typescript":"clients/typescript/ticketdesk/client.ts"},
  "query_modules":[{"name":"ticketdesk","version":1,"queries":[
    {"name":"BoardPage200","source":"queries/ticketdesk/board_page_200.riffq"},
    {"name":"BoardPage450","source":"queries/ticketdesk/board_page_450.riffq"},
    {"name":"BoardPage50","source":"queries/ticketdesk/board_page_50.riffq"},
    {"name":"GetTicket","source":"queries/ticketdesk/get_ticket.riffq"},
    {"name":"GetUser","source":"queries/ticketdesk/get_user.riffq"},
    {"name":"ListComments","source":"queries/ticketdesk/list_comments.riffq"},
    {"name":"ListTickets","source":"queries/ticketdesk/list_tickets.riffq"},
    {"name":"ListTicketsByAssignee","source":"queries/ticketdesk/list_tickets_by_assignee.riffq"},
    {"name":"ProjectMembers","source":"queries/ticketdesk/project_members.riffq"},
    {"name":"ProjectSummary","source":"queries/ticketdesk/project_summary.riffq"},
    {"name":"TicketPage","source":"queries/ticketdesk/ticket_page.riffq"},
    {"name":"TicketPagePaged","source":"queries/ticketdesk/ticket_page_paged.riffq"},
    {"name":"TicketQueue","source":"queries/ticketdesk/ticket_queue.riffq"}
  ]}],
  "roles":[
    {"name":"TicketDeskAgent","environment":"development","tenant_scope":"global","queries":["BoardPage200","BoardPage450","BoardPage50","GetTicket","GetUser","ListComments","ListTickets","ListTicketsByAssignee","ProjectMembers","ProjectSummary","TicketPage","TicketPagePaged","TicketQueue"],"commands":["AddProjectMember","AttachLabel","CloseTicketWithComment","CreateComment","CreateLabel","CreateOrganization","CreateProject","CreateTicket","CreateUser","OpenTicketWithLabels","SwapMemberRoles"]},
    {"name":"TicketDeskApplication","environment":"development","tenant_scope":"global","queries":["BoardPage200","BoardPage450","BoardPage50","GetTicket","GetUser","ListComments","ListTickets","ListTicketsByAssignee","ProjectMembers","ProjectSummary","TicketPage","TicketPagePaged","TicketQueue"],"commands":["AddProjectMember","AttachLabel","CloseTicketWithComment","CreateComment","CreateLabel","CreateOrganization","CreateProject","CreateTicket","CreateUser","OpenTicketWithLabels","SwapMemberRoles"]}
  ],
  "seed_inputs":["examples/ticketdesk/seed/dev.jsonl"]
}"#;
const QUERIES: [(&str, &str); 13] = [
    (
        "BoardPage200",
        include_str!("../../../queries/ticketdesk/board_page_200.riffq"),
    ),
    (
        "BoardPage450",
        include_str!("../../../queries/ticketdesk/board_page_450.riffq"),
    ),
    (
        "BoardPage50",
        include_str!("../../../queries/ticketdesk/board_page_50.riffq"),
    ),
    (
        "GetTicket",
        include_str!("../../../queries/ticketdesk/get_ticket.riffq"),
    ),
    (
        "GetUser",
        include_str!("../../../queries/ticketdesk/get_user.riffq"),
    ),
    (
        "ListComments",
        include_str!("../../../queries/ticketdesk/list_comments.riffq"),
    ),
    (
        "ListTickets",
        include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
    ),
    (
        "ListTicketsByAssignee",
        include_str!("../../../queries/ticketdesk/list_tickets_by_assignee.riffq"),
    ),
    (
        "ProjectMembers",
        include_str!("../../../queries/ticketdesk/project_members.riffq"),
    ),
    (
        "ProjectSummary",
        include_str!("../../../queries/ticketdesk/project_summary.riffq"),
    ),
    (
        "TicketPage",
        include_str!("../../../queries/ticketdesk/ticket_page.riffq"),
    ),
    (
        "TicketPagePaged",
        include_str!("../../../queries/ticketdesk/ticket_page_paged.riffq"),
    ),
    (
        "TicketQueue",
        include_str!("../../../queries/ticketdesk/ticket_queue.riffq"),
    ),
];

fn main() {
    let output = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("."), PathBuf::from);
    let contract = compile_contract_source(CONTRACT).expect("compile TicketDesk contract");
    let source = ApplicationSourceManifest::parse(APPLICATION_SOURCE)
        .expect("TicketDesk application source");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("ticketdesk").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        QUERIES
            .into_iter()
            .map(|(name, source)| NamedQuerySource::new(name, source).expect("query source"))
            .collect(),
    )
    .expect("module candidate");
    let module = QueryModule::compile(candidate, &contract).expect("compile query module");
    let application = source
        .exact_manifest(&contract, std::slice::from_ref(&module))
        .expect("exact TicketDesk application manifest");
    assert_eq!(
        application.contract().lineage(),
        contract.lineage().as_str()
    );
    assert_eq!(
        application.contract().version(),
        contract.contract_version().get()
    );
    assert_eq!(application.contract().bundle_hash(), contract.bundle_hash());
    assert_eq!(
        application.query_modules()[0].module_hash(),
        module.identity()
    );
    let role = compile_application_role(
        &application,
        "TicketDeskAgent",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("compile TicketDesk role");
    fs::create_dir_all(output.join("fixtures/query-modules")).expect("fixture directory");
    fs::create_dir_all(output.join("fixtures/application-manifests"))
        .expect("manifest fixture directory");
    fs::create_dir_all(output.join("fixtures/application-locks")).expect("lock fixture directory");
    fs::create_dir_all(output.join("clients/typescript/ticketdesk")).expect("client directory");
    fs::create_dir_all(output.join("clients/python/ticketdesk")).expect("client directory");
    fs::create_dir_all(output.join("clients/go/ticketdesk")).expect("client directory");
    fs::write(
        output.join("fixtures/application-manifests/ticketdesk-v1.json"),
        application.canonical_bytes(),
    )
    .expect("manifest fixture");
    fs::write(
        output.join("fixtures/query-modules/ticketdesk.rs"),
        generate_rust_client(&module, &contract),
    )
    .expect("Rust fixture");
    fs::write(
        output.join("clients/typescript/ticketdesk/client.ts"),
        generate_typescript_client(&module, &contract),
    )
    .expect("TypeScript fixture");
    fs::write(
        output.join("clients/python/ticketdesk/generated.py"),
        generate_python_client(&module, &contract).expect("generate Python client"),
    )
    .expect("Python fixture");
    fs::write(
        output.join("clients/go/ticketdesk/client.go"),
        generate_go_client(&module, &contract),
    )
    .expect("Go fixture");
    let tools = generate_mcp_tools(&module).expect("generate MCP tools");
    let commands = generate_mcp_commands(&module, &contract).expect("generate MCP commands");
    let manifest = serde_json::json!({
        "schema": "riffdb-generated-application-operations/v2",
        "application_manifest_hash": hex(application.identity().as_bytes()),
        "tools": tools
            .iter()
            .map(|tool| serde_json::json!({
                "name": tool.name,
                "operation_name": tool.operation_name,
                "title": tool.title,
                "description": tool.description,
                "module_hash": hex(&tool.module_hash),
                "input_schema": serde_json::from_str::<serde_json::Value>(&tool.input_schema)
                    .expect("input schema"),
                "result_schema": serde_json::from_str::<serde_json::Value>(&tool.result_schema)
                    .expect("result schema"),
                "annotations": {
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false,
                },
            }))
            .collect::<Vec<_>>(),
        "commands": commands
            .iter()
            .map(|command| serde_json::json!({
                "name": command.name,
                "operation_name": command.operation_name,
                "title": command.title,
                "description": command.description,
                "contract_bundle_hash": hex(&command.contract_bundle_hash),
                "plan_hash": hex(&command.plan_hash),
                "input_schema": serde_json::from_str::<serde_json::Value>(&command.input_schema)
                    .expect("input schema"),
                "result_schema": serde_json::from_str::<serde_json::Value>(&command.result_schema)
                    .expect("result schema"),
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": true,
                    "idempotentHint": true,
                    "openWorldHint": false,
                },
            }))
            .collect::<Vec<_>>(),
    });
    fs::write(
        output.join("fixtures/query-modules/ticketdesk.mcp.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&manifest).expect("MCP manifest")
        ),
    )
    .expect("MCP fixture");
    fs::write(
        output.join("fixtures/application-manifests/ticketdesk-v1.identity"),
        format!("{}\n", hex(application.identity().as_bytes())),
    )
    .expect("manifest identity fixture");
    fs::write(
        output.join("fixtures/application-manifests/ticketdesk-agent-role-v1.identity"),
        format!("{}\n", hex(role.identity().as_bytes())),
    )
    .expect("role identity fixture");
    let role_description = serde_json::json!({
        "application": role.application_name(),
        "application_manifest_hash": hex(role.manifest_hash().as_bytes()),
        "contract": {
            "bundle_hash": hex(role.contract_hash().as_bytes()),
            "lineage": role.contract_lineage().as_str(),
            "version": role.contract_version().get(),
        },
        "environment": role.environment().as_str(),
        "query_module_hashes": role.module_hashes().iter()
            .map(|hash| hex(hash.as_bytes()))
            .collect::<Vec<_>>(),
        "role": role.role_name(),
        "role_hash": hex(role.identity().as_bytes()),
        "tenant_scope": "global",
        "operations": role.operations().iter().map(|operation| serde_json::json!({
            "kind": match operation.kind() {
                riffdb_query_module::ApplicationRoleOperationKind::Query => "query",
                riffdb_query_module::ApplicationRoleOperationKind::Command => "command",
                riffdb_query_module::ApplicationRoleOperationKind::EventStream => "event_stream",
                riffdb_query_module::ApplicationRoleOperationKind::QueryWatch => "watch_query",
                riffdb_query_module::ApplicationRoleOperationKind::AgentSubscription => "agent_subscription",
            },
            "name": operation.name(),
        })).collect::<Vec<_>>(),
    });
    fs::write(
        output.join("fixtures/application-manifests/ticketdesk-agent-role-v1.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&role_description).expect("role description")
        ),
    )
    .expect("role description fixture");
    generate_migration_application_fixtures(&output);
    generate_row_policy_application_fixtures(&output);
}

fn generate_row_policy_application_fixtures(output: &std::path::Path) {
    let contract_source =
        include_str!("../../../fixtures/compiler/row-policy/valid/document-access.riff");
    let contract = compile_contract_source(contract_source).expect("row-policy contract");
    let query_source = r#"
query GetDocument(
    $organization_id: Document.organization_id,
    $document_id: Document.document_id,
) {
    one document from Document
        where organization_id == $organization_id
          && document_id == $document_id
        else NotFound
    return Found { document: document { document_id owner_id team_id visibility } }
    outcomes Found | NotFound
}
"#;
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("policy_surface").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("GetDocument", query_source).expect("query")],
        )
        .expect("candidate"),
        &contract,
    )
    .expect("row-policy module");
    let source_json = serde_json::json!({
        "application": "policy-surface",
        "contract": {"lineage": "PolicySurface", "source": "contract.riff", "version": 1},
        "generation": {
            "go": "generated/go/client.go",
            "mcp": "generated/mcp/tools.json",
            "python": "generated/python/client.py",
            "rust": "generated/rust/client.rs",
            "typescript": "generated/typescript/client.ts",
        },
        "migrations": [],
        "query_modules": [{
            "name": "policy_surface",
            "queries": [{"name": "GetDocument", "source": "queries/get_document.riffq"}],
            "version": 1,
        }],
        "reactive_modules": [],
        "roles": [{
            "agent_subscriptions": [],
            "commands": [],
            "environment": "development",
            "event_streams": [],
            "name": "DocumentReader",
            "queries": ["GetDocument"],
            "row_policies": ["DocumentAccess"],
            "tenant_scope": "global",
            "watch_queries": [],
        }],
        "schema": "riffdb.application-source/v6",
        "seed_inputs": [],
    });
    let source = ApplicationSourceManifest::parse(
        &serde_json::to_string(&source_json).expect("source JSON"),
    )
    .expect("source V6");
    let exact = source
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .expect("exact manifest V4");
    let role = compile_application_role_v2(
        &exact,
        "DocumentReader",
        None,
        &contract,
        std::slice::from_ref(&module),
        &[],
    )
    .expect("policy role");
    let rust = generate_rust_client(&module, &contract);
    let typescript = generate_typescript_client(&module, &contract);
    let python = generate_python_client(&module, &contract).expect("Python client");
    let go = generate_go_client(&module, &contract);
    let mcp = serde_json::to_vec(&serde_json::json!({
        "commands": generate_mcp_commands(&module, &contract)
            .expect("MCP commands")
            .iter()
            .map(|command| command.name.as_str())
            .collect::<Vec<_>>(),
        "schema": "riffdb-row-policy-fixture-mcp/v1",
        "tools": generate_mcp_tools(&module)
            .expect("MCP tools")
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
    }))
    .expect("MCP fixture");
    let artifacts = [
        (
            GeneratedApplicationArtifactKind::Manifest,
            "generated/riffdb.application.exact.json",
            exact.canonical_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Rust,
            "generated/rust/client.rs",
            rust.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::TypeScript,
            "generated/typescript/client.ts",
            typescript.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Python,
            "generated/python/client.py",
            python.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Go,
            "generated/go/client.go",
            go.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Mcp,
            "generated/mcp/tools.json",
            mcp.as_slice(),
        ),
        (
            GeneratedApplicationArtifactKind::ContractBundle,
            riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
            contract.canonical_bytes(),
        ),
    ]
    .into_iter()
    .map(|(kind, path, bytes)| {
        GeneratedApplicationArtifact::new(kind, path, bytes).expect("generated artifact")
    })
    .collect::<Vec<_>>();
    let lock = ApplicationLock::compile_v7(
        &source,
        &exact,
        &contract,
        std::slice::from_ref(&module),
        &[],
        &artifacts,
        &[],
    )
    .expect("application lock V7");
    fs::write(
        output.join("fixtures/application-manifests/policy-surface-v6.json"),
        source.canonical_bytes(),
    )
    .expect("source fixture");
    fs::write(
        output.join("fixtures/application-manifests/policy-surface-exact-v4.json"),
        exact.canonical_bytes(),
    )
    .expect("exact fixture");
    fs::write(
        output.join("fixtures/application-locks/policy-surface-v7.json"),
        lock.canonical_bytes(),
    )
    .expect("lock fixture");
    let receipt = serde_json::json!({
        "after": {
            "application_lock_schema": lock.schema(),
            "application_manifest_schema": exact.schema(),
            "application_role_definition_format": 3,
            "application_source_schema": source.schema(),
            "contract_bundle_format": contract.format_version(),
            "contract_grammar_version": contract.grammar_version(),
            "contract_ir_version": contract.ir_version(),
            "identities": {
                "application_lock_hash": hex(lock.identity().as_bytes()),
                "application_manifest_hash": hex(exact.identity().as_bytes()),
                "application_role_hash": hex(role.identity().as_bytes()),
                "contract_bundle_hash": hex(contract.bundle_hash().as_bytes()),
                "query_module_hash": hex(module.identity().as_bytes()),
                "source_hash": hex(source.identity().as_bytes()),
            },
        },
        "before": {
            "application_lock_fixture": "fixtures/application-locks/ticketdesk-migration-v4.json",
            "application_manifest_fixture": "fixtures/application-manifests/ticketdesk-v1.json",
            "preserved": true,
        },
        "classification": {
            "old_fixtures_preserved": true,
            "partial_rotation_is_success": false,
            "row_policy_identity_is_additive": true,
        },
        "decision": "ADR-0111",
        "repository_closure": [
            "fixtures/application-manifests/policy-surface-v6.json",
            "fixtures/application-manifests/policy-surface-exact-v4.json",
            "fixtures/application-locks/policy-surface-v7.json",
        ],
        "schema": "riffdb-row-policy-identity-rotation/v1",
        "work_package": "WP-570",
    });
    fs::write(
        output.join("fixtures/application-locks/row-policy-rotation-v1.json"),
        format!(
            "{}\n",
            serde_json::to_string_pretty(&receipt).expect("rotation receipt")
        ),
    )
    .expect("rotation receipt fixture");
}

fn generate_migration_application_fixtures(output: &std::path::Path) {
    let parent = compile_contract_source(CONTRACT).expect("compile migration parent");
    let successor_source = CONTRACT.replacen("version 1", "version 2", 1).replace(
        "index by_assignee_status (organization_id, assignee_id, status, ticket_id)",
        concat!(
            "index by_assignee_status (organization_id, assignee_id, status, ticket_id)\n",
            "    index by_title (organization_id, title, ticket_id)"
        ),
    );
    let successor = compile_contract_successor(&successor_source, &parent)
        .expect("compile migration successor");
    let migration_source = "migration TicketDesk from 1 to 2 {}\n";
    let migration = compile_migration_source(migration_source, &parent, &successor)
        .expect("compile migration fixture");
    let query_source = QUERIES
        .iter()
        .find_map(|(name, source)| (*name == "GetTicket").then_some(*source))
        .expect("GetTicket source");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("ticketdesk").expect("module name"),
            QueryModuleVersion::new(2).expect("module version"),
            vec![NamedQuerySource::new("GetTicket", query_source).expect("query source")],
        )
        .expect("module candidate"),
        &successor,
    )
    .expect("compile successor query module");
    let source_json = serde_json::json!({
        "application": "ticketdesk-migration-fixture",
        "contract": {
            "lineage": "TicketDesk",
            "source": "riffdb/contract.riff",
            "version": 2,
        },
        "generation": {
            "mcp": "generated/mcp/tools.json",
            "python": "generated/python/client.py",
            "rust": "generated/rust/client.rs",
            "typescript": "generated/typescript/client.ts",
        },
        "migrations": [{
            "parent_bundle": "retained/ticketdesk-v1.riffdb.contract.bundle",
            "source": "riffdb/migrations/ticketdesk-v1-to-v2.riffm",
        }],
        "query_modules": [{
            "name": "ticketdesk",
            "queries": [{
                "name": "GetTicket",
                "source": "riffdb/queries/get_ticket.riffq",
            }],
            "version": 2,
        }],
        "roles": [{
            "commands": ["CreateTicket"],
            "environment": "development",
            "name": "TicketDeskMigrationAgent",
            "queries": ["GetTicket"],
            "tenant_scope": "global",
        }],
        "schema": "riffdb.application-source/v3",
        "seed_inputs": [],
    });
    let source = ApplicationSourceManifest::parse(
        &serde_json::to_string(&source_json).expect("application source JSON"),
    )
    .expect("application source V3");
    let exact = source
        .exact_manifest(&successor, std::slice::from_ref(&module))
        .expect("exact manifest");
    let rust = generate_rust_client(&module, &successor);
    let typescript = generate_typescript_client(&module, &successor);
    let python = generate_python_client(&module, &successor).expect("Python client");
    let mcp = serde_json::to_vec(&serde_json::json!({
        "commands": generate_mcp_commands(&module, &successor)
            .expect("MCP commands")
            .iter()
            .map(|command| command.name.as_str())
            .collect::<Vec<_>>(),
        "schema": "riffdb-migration-fixture-mcp/v1",
        "tools": generate_mcp_tools(&module)
            .expect("MCP tools")
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
    }))
    .expect("MCP fixture");
    let artifacts = [
        (
            GeneratedApplicationArtifactKind::Manifest,
            "generated/riffdb.application.exact.json",
            exact.canonical_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Rust,
            "generated/rust/client.rs",
            rust.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::TypeScript,
            "generated/typescript/client.ts",
            typescript.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Python,
            "generated/python/client.py",
            python.as_bytes(),
        ),
        (
            GeneratedApplicationArtifactKind::Mcp,
            "generated/mcp/tools.json",
            mcp.as_slice(),
        ),
        (
            GeneratedApplicationArtifactKind::ContractBundle,
            riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
            successor.canonical_bytes(),
        ),
    ]
    .into_iter()
    .map(|(kind, path, bytes)| {
        GeneratedApplicationArtifact::new(kind, path, bytes).expect("generated artifact")
    })
    .collect::<Vec<_>>();
    let migration_input = ApplicationMigrationLockInput::new(
        "riffdb/migrations/ticketdesk-v1-to-v2.riffm",
        migration_source.as_bytes(),
        "retained/ticketdesk-v1.riffdb.contract.bundle",
        &parent,
        "generated/migrations/1-to-2.riffdb.migration.bundle",
        &migration,
    )
    .expect("migration lock input");
    let lock = ApplicationLock::compile_v4(
        &source,
        &exact,
        &successor,
        std::slice::from_ref(&module),
        &artifacts,
        &[migration_input],
    )
    .expect("application lock V4");

    fs::write(
        output.join("fixtures/application-manifests/ticketdesk-migration-v3.json"),
        source.canonical_bytes(),
    )
    .expect("source V3 fixture");
    fs::write(
        output.join("fixtures/application-locks/ticketdesk-migration-v4.json"),
        lock.canonical_bytes(),
    )
    .expect("lock V4 fixture");
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("string");
    }
    output
}
