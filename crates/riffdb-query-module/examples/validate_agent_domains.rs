#![forbid(unsafe_code)]

//! Compiles and deterministically generates the two WP-335 unfamiliar-domain corpora.

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::ContractBundle;
use riffdb_query_compiler::compile_query;
use riffdb_query_ir::SymbolicCatalog;
use riffdb_query_module::{
    ApplicationManifest, ApplicationRoleOperationKind, CompiledApplicationRole,
    GeneratedMcpCommand, GeneratedMcpTool, NamedQuerySource, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion, compile_application_role, generate_mcp_commands,
    generate_mcp_tools, generate_rust_client, generate_typescript_client,
};
use riffdb_riffql_syntax::parse_query;
use serde_json::json;

const BLOG_CONTRACT: &str = include_str!("../../../contracts/agent-alpha/blog.riff");
const BLOG_QUERIES: [(&str, &str); 4] = [
    (
        "ModerationQueue",
        include_str!("../../../queries/agent-alpha/blog/moderation_queue.riffq"),
    ),
    (
        "PostBySlug",
        include_str!("../../../queries/agent-alpha/blog/post_by_slug.riffq"),
    ),
    (
        "PostPage",
        include_str!("../../../queries/agent-alpha/blog/post_page.riffq"),
    ),
    (
        "PublicFeed",
        include_str!("../../../queries/agent-alpha/blog/public_feed.riffq"),
    ),
];

const ORDERS_CONTRACT: &str = include_str!("../../../contracts/agent-alpha/orders.riff");
const ORDERS_QUERIES: [(&str, &str); 4] = [
    (
        "CustomerHistory",
        include_str!("../../../queries/agent-alpha/orders/customer_history.riffq"),
    ),
    (
        "InventoryDashboard",
        include_str!("../../../queries/agent-alpha/orders/inventory_dashboard.riffq"),
    ),
    (
        "OpenOrders",
        include_str!("../../../queries/agent-alpha/orders/open_orders.riffq"),
    ),
    (
        "OrderPage",
        include_str!("../../../queries/agent-alpha/orders/order_page.riffq"),
    ),
];
const BLOG_UNINDEXED: &str =
    include_str!("../../../fixtures/agent-alpha/negative/blog-unindexed-title.riffq");
const BLOG_CROSS_PARTITION: &str =
    include_str!("../../../fixtures/agent-alpha/negative/blog-cross-partition.riffq");
const ORDERS_COLLECTION_AS_SCALAR: &str =
    include_str!("../../../fixtures/agent-alpha/negative/orders-collection-as-scalar.riffq");
const ORDERS_UNBOUNDED: &str =
    include_str!("../../../fixtures/agent-alpha/negative/orders-unbounded-many.riffq");

struct Domain<'a> {
    name: &'a str,
    application: &'a str,
    role: &'a str,
    contract_source: &'a str,
    queries: &'a [(&'a str, &'a str)],
    commands: &'a [&'a str],
    seeds: &'a [(&'a str, &'a str)],
}

const BLOG_COMMANDS: &[&str] = &[
    "AttachTag",
    "CreateAuthor",
    "CreateComment",
    "CreatePost",
    "CreatePostRoute",
    "CreateSite",
    "CreateTag",
];
const BLOG_SEEDS: &[(&str, &str)] = &[
    (
        "01-CreateSite.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-blog/seed/01-CreateSite.jsonl"),
    ),
    (
        "02-CreateAuthor.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-blog/seed/02-CreateAuthor.jsonl"),
    ),
    (
        "03-CreatePost.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-blog/seed/03-CreatePost.jsonl"),
    ),
    (
        "04-CreatePostRoute.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-blog/seed/04-CreatePostRoute.jsonl"),
    ),
    (
        "05-CreateComment.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-blog/seed/05-CreateComment.jsonl"),
    ),
    (
        "06-CreateTag.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-blog/seed/06-CreateTag.jsonl"),
    ),
    (
        "07-AttachTag.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-blog/seed/07-AttachTag.jsonl"),
    ),
];
const ORDERS_COMMANDS: &[&str] = &[
    "AddOrderLine",
    "CreateCustomer",
    "CreateInventory",
    "CreateOrder",
    "CreateProduct",
    "CreateStore",
    "ReserveInventory",
];
const ORDERS_SEEDS: &[(&str, &str)] = &[
    (
        "01-CreateStore.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-orders/seed/01-CreateStore.jsonl"),
    ),
    (
        "02-CreateCustomer.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-orders/seed/02-CreateCustomer.jsonl"),
    ),
    (
        "03-CreateProduct.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-orders/seed/03-CreateProduct.jsonl"),
    ),
    (
        "04-CreateInventory.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-orders/seed/04-CreateInventory.jsonl"),
    ),
    (
        "05-CreateOrder.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-orders/seed/05-CreateOrder.jsonl"),
    ),
    (
        "06-AddOrderLine.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-orders/seed/06-AddOrderLine.jsonl"),
    ),
    (
        "07-ReserveInventory.jsonl",
        include_str!("../../../fixtures/agent-alpha/agent-orders/seed/07-ReserveInventory.jsonl"),
    ),
];

fn compile(domain: &Domain<'_>) -> (ContractBundle, QueryModule) {
    let name = domain.name;
    let contract_source = domain.contract_source;
    let queries = domain.queries;
    let contract = compile_contract_source(contract_source)
        .unwrap_or_else(|diagnostics| panic!("{name} contract: {diagnostics:#?}"));
    let catalog = SymbolicCatalog::from_bundle(&contract).expect("symbolic catalog");
    for (query_name, source) in queries {
        let document = parse_query(source)
            .unwrap_or_else(|diagnostics| panic!("{name}.{query_name} syntax: {diagnostics:#?}"));
        compile_query(&document, &catalog)
            .unwrap_or_else(|diagnostics| panic!("{name}.{query_name} plan: {diagnostics:#?}"));
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new(name).expect("domain module name"),
            QueryModuleVersion::new(1).expect("domain module version"),
            vec![NamedQuerySource::new(*query_name, *source).expect("bounded query source")],
        )
        .expect("bounded domain module candidate");
        QueryModule::compile(candidate, &contract)
            .unwrap_or_else(|diagnostics| panic!("{name}.{query_name}: {diagnostics:#?}"));
    }
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new(name).expect("domain module name"),
        QueryModuleVersion::new(1).expect("domain module version"),
        queries
            .iter()
            .map(|(query_name, source)| {
                NamedQuerySource::new(*query_name, *source).expect("bounded query source")
            })
            .collect(),
    )
    .expect("bounded domain module candidate");
    let module = QueryModule::compile(candidate, &contract)
        .unwrap_or_else(|diagnostics| panic!("{name} queries: {diagnostics:#?}"));
    (contract, module)
}

fn main() {
    let domains = [
        Domain {
            name: "agent_blog",
            application: "agent-blog",
            role: "BlogApplication",
            contract_source: BLOG_CONTRACT,
            queries: &BLOG_QUERIES,
            commands: BLOG_COMMANDS,
            seeds: BLOG_SEEDS,
        },
        Domain {
            name: "agent_orders",
            application: "agent-orders",
            role: "OrdersApplication",
            contract_source: ORDERS_CONTRACT,
            queries: &ORDERS_QUERIES,
            commands: ORDERS_COMMANDS,
            seeds: ORDERS_SEEDS,
        },
    ];
    let mut arguments = env::args_os().skip(1);
    let output = match arguments.next() {
        None => None,
        Some(flag) if flag == "--write" => Some(
            arguments
                .next()
                .map_or_else(|| PathBuf::from("."), PathBuf::from),
        ),
        Some(_) => panic!("usage: validate_agent_domains [--write OUTPUT_ROOT]"),
    };
    assert!(arguments.next().is_none(), "unexpected trailing argument");
    let mut compiled = Vec::with_capacity(domains.len());
    for domain in &domains {
        let (contract, module) = compile(domain);
        if let Some(root) = &output {
            generate_domain(root, domain, &contract, &module);
        }
        compiled.push((contract, module));
    }
    let diagnostics = vec![
        planner_rejection(
            "blog-unindexed-title",
            "index",
            BLOG_UNINDEXED,
            &compiled[0].0,
            "RDB-QP003",
            "Declare an index beginning (site_id, title), or use the existing status feed.",
        ),
        planner_rejection(
            "blog-cross-partition",
            "locality",
            BLOG_CROSS_PARTITION,
            &compiled[0].0,
            "RDB-QP002",
            "Supply site_id and keep the author lookup inside that partition.",
        ),
        planner_rejection(
            "orders-collection-as-scalar",
            "cardinality",
            ORDERS_COLLECTION_AS_SCALAR,
            &compiled[1].0,
            "RDB-QP007",
            "Use the bounded dependent-key batch form: product_id in lines.product_id.",
        ),
        syntax_rejection(
            "orders-unbounded-many",
            "bounds",
            ORDERS_UNBOUNDED,
            "RDB-QS009",
            "Add an explicit positive take bound and optional cursor.",
        ),
    ];
    if let Some(root) = &output {
        write(
            root,
            "fixtures/agent-alpha/gap-report-v1.json",
            pretty(json!({
                "conclusion": "Both domains complete without a new grammar or IR construct.",
                "diagnostics": diagnostics,
                "language_addition": null,
                "schema": "riffdb-agent-domain-gap-report/v1",
            }))
            .as_bytes(),
        );
    }
}

fn planner_rejection(
    shape: &str,
    classification: &str,
    source: &str,
    contract: &ContractBundle,
    expected_code: &str,
    remedy: &str,
) -> serde_json::Value {
    let document = parse_query(source).expect("negative planner fixture must parse");
    let catalog = SymbolicCatalog::from_bundle(contract).expect("negative symbolic catalog");
    let diagnostics = compile_query(&document, &catalog).expect_err("shape must be rejected");
    let diagnostic = diagnostics
        .as_slice()
        .first()
        .expect("one planner diagnostic");
    assert_eq!(diagnostic.code().as_str(), expected_code, "{shape}");
    json!({
        "classification": classification,
        "code": diagnostic.code().as_str(),
        "remedy": remedy,
        "shape": shape,
        "source_span": {
            "end": diagnostic.primary().end,
            "start": diagnostic.primary().start,
        },
        "suggested_index": diagnostic.suggested_index(),
        "summary": diagnostic.summary(),
        "symbol_path": diagnostic.symbol_path(),
    })
}

fn syntax_rejection(
    shape: &str,
    classification: &str,
    source: &str,
    expected_code: &str,
    remedy: &str,
) -> serde_json::Value {
    let diagnostics = parse_query(source).expect_err("shape must be rejected");
    let diagnostic = diagnostics
        .as_slice()
        .first()
        .expect("one syntax diagnostic");
    assert_eq!(diagnostic.code().as_str(), expected_code, "{shape}");
    json!({
        "classification": classification,
        "code": diagnostic.code().as_str(),
        "remedy": remedy,
        "shape": shape,
        "source_span": {
            "end": diagnostic.span().end,
            "start": diagnostic.span().start,
        },
        "summary": diagnostic.summary(),
    })
}

fn generate_domain(
    root: &Path,
    domain: &Domain<'_>,
    contract: &ContractBundle,
    module: &QueryModule,
) {
    let fixture_root = format!("fixtures/agent-alpha/{}", domain.application);
    let generated_root = format!("examples/agent-alpha/domains/{}", domain.application);
    let manifest_source = serde_json::to_string(&json!({
        "application": domain.application,
        "contract": {
            "bundle_hash": hex(contract.bundle_hash().as_bytes()),
            "lineage": contract.lineage().as_str(),
            "source": "domain/contract.riff",
            "version": contract.contract_version().get(),
        },
        "generation": {
            "mcp": "generated/mcp/tools.json",
            "rust": "generated/rust/client.rs",
            "typescript": "generated/typescript/client.ts",
        },
        "query_modules": [{
            "module_hash": hex(module.identity().as_bytes()),
            "name": domain.name,
            "queries": domain.queries.iter().map(|(name, _)| json!({
                "name": name,
                "source": format!("domain/queries/{}.riffq", snake(name)),
            })).collect::<Vec<_>>(),
            "version": 1,
        }],
        "roles": [{
            "commands": domain.commands,
            "environment": "development",
            "name": domain.role,
            "queries": domain.queries.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            "tenant_scope": "global",
        }],
        "schema": "riffdb.application-manifest/v1",
        "seed_inputs": domain.seeds.iter()
            .map(|(name, _)| format!("domain/seed/{name}"))
            .collect::<Vec<_>>(),
    }))
    .expect("serialize application manifest");
    let manifest = ApplicationManifest::parse(&manifest_source).expect("application manifest");
    let role = compile_application_role(
        &manifest,
        domain.role,
        None,
        contract,
        std::slice::from_ref(module),
    )
    .expect("application role");
    let tools = generate_mcp_tools(module).expect("MCP query tools");
    let commands = generate_mcp_commands(module, contract).expect("MCP command tools");
    write(
        root,
        &format!("{fixture_root}/application-manifest.json"),
        manifest.canonical_bytes(),
    );
    write(
        root,
        &format!("{generated_root}/riffdb.application.json"),
        manifest.canonical_bytes(),
    );
    write(
        root,
        &format!("{generated_root}/domain/contract.riff"),
        domain.contract_source.as_bytes(),
    );
    for (name, source) in domain.queries {
        write(
            root,
            &format!("{generated_root}/domain/queries/{}.riffq", snake(name)),
            source.as_bytes(),
        );
    }
    for (name, source) in domain.seeds {
        write(
            root,
            &format!("{generated_root}/domain/seed/{name}"),
            source.as_bytes(),
        );
    }
    write(
        root,
        &format!("{fixture_root}/role.json"),
        role_json(&role).as_bytes(),
    );
    write(
        root,
        &format!("{fixture_root}/golden-observations.json"),
        observations_json(domain).as_bytes(),
    );
    write(
        root,
        &format!("{generated_root}/generated/rust/client.rs"),
        generate_rust_client(module, contract).as_bytes(),
    );
    write(
        root,
        &format!("{generated_root}/generated/typescript/client.ts"),
        generate_typescript_client(module, contract).as_bytes(),
    );
    write(
        root,
        &format!("{generated_root}/generated/mcp/tools.json"),
        mcp_json(&manifest, &tools, &commands).as_bytes(),
    );
}

fn role_json(role: &CompiledApplicationRole) -> String {
    pretty(json!({
        "application": role.application_name(),
        "application_manifest_hash": hex(role.manifest_hash().as_bytes()),
        "contract": {
            "bundle_hash": hex(role.contract_hash().as_bytes()),
            "lineage": role.contract_lineage().as_str(),
            "version": role.contract_version().get(),
        },
        "environment": role.environment().as_str(),
        "operations": role.operations().iter().map(|operation| json!({
            "kind": match operation.kind() {
                ApplicationRoleOperationKind::Query => "query",
                ApplicationRoleOperationKind::Command => "command",
                ApplicationRoleOperationKind::EventStream => "event_stream",
                ApplicationRoleOperationKind::QueryWatch => "watch_query",
                ApplicationRoleOperationKind::AgentSubscription => "agent_subscription",
            },
            "name": operation.name(),
        })).collect::<Vec<_>>(),
        "query_module_hashes": role.module_hashes().iter()
            .map(|hash| hex(hash.as_bytes()))
            .collect::<Vec<_>>(),
        "role": role.role_name(),
        "role_hash": hex(role.identity().as_bytes()),
        "tenant_scope": "global",
    }))
}

fn observations_json(domain: &Domain<'_>) -> String {
    pretty(json!({
        "application": domain.application,
        "languages": ["rust", "typescript"],
        "observations": domain.queries.iter().map(|(name, _)| json!({
            "application_rpc_count": 1,
            "operation": name,
            "result": "typed_declared_outcome",
            "snapshot": "single_engine_snapshot",
            "transport_glue_lines": 0,
        })).collect::<Vec<_>>(),
        "schema": "riffdb-agent-domain-observations/v1",
        "writes": domain.commands.iter().map(|name| json!({
            "idempotency": "required",
            "operation": name,
            "result": "typed_declared_outcome",
            "transport_glue_lines": 0,
        })).collect::<Vec<_>>(),
    }))
}

fn mcp_json(
    manifest: &ApplicationManifest,
    tools: &[GeneratedMcpTool],
    commands: &[GeneratedMcpCommand],
) -> String {
    pretty(json!({
        "application_manifest_hash": hex(manifest.identity().as_bytes()),
        "commands": commands.iter().map(|command| json!({
            "annotations": {
                "destructiveHint": true,
                "idempotentHint": true,
                "openWorldHint": false,
                "readOnlyHint": false,
            },
            "contract_bundle_hash": hex(&command.contract_bundle_hash),
            "description": command.description,
            "input_schema": serde_json::from_str::<serde_json::Value>(&command.input_schema)
                .expect("command input schema"),
            "name": command.name,
            "plan_hash": hex(&command.plan_hash),
            "result_schema": serde_json::from_str::<serde_json::Value>(&command.result_schema)
                .expect("command result schema"),
            "title": command.title,
        })).collect::<Vec<_>>(),
        "schema": "riffdb-generated-mcp-tools-v1",
        "tools": tools.iter().map(|tool| json!({
            "annotations": {
                "destructiveHint": false,
                "idempotentHint": true,
                "openWorldHint": false,
                "readOnlyHint": true,
            },
            "description": tool.description,
            "input_schema": serde_json::from_str::<serde_json::Value>(&tool.input_schema)
                .expect("query input schema"),
            "module_hash": hex(&tool.module_hash),
            "name": tool.name,
            "result_schema": serde_json::from_str::<serde_json::Value>(&tool.result_schema)
                .expect("query result schema"),
            "title": tool.title,
        })).collect::<Vec<_>>(),
    }))
}

fn write(root: &Path, relative: &str, bytes: &[u8]) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().expect("generated parent")).expect("generated directory");
    fs::write(path, bytes).expect("generated artifact");
}

fn pretty(value: serde_json::Value) -> String {
    let mut output = serde_json::to_string_pretty(&value).expect("pretty JSON");
    output.push('\n');
    output
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("string");
    }
    output
}

fn snake(name: &str) -> String {
    let mut output = String::with_capacity(name.len() + 4);
    for (index, character) in name.chars().enumerate() {
        if character.is_ascii_uppercase() && index > 0 {
            output.push('_');
        }
        output.push(character.to_ascii_lowercase());
    }
    output
}
