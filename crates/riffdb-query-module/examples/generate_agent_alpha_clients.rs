//! Regenerates agent-alpha application Rust clients checked into examples/.
//!
//! Owned outputs (do not hand-edit):
//! - `examples/agent-alpha/generated/rust/client.rs`
//! - `examples/agent-alpha/domains/agent-blog/generated/rust/client.rs`
//! - `examples/agent-alpha/domains/agent-orders/generated/rust/client.rs`
//!
//! Drift protection: `./scripts/generate-query-clients --check` diffs these
//! three paths against a fresh generator run.

#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::ContractBundle;
use riffdb_query_module::{
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_rust_client,
};

struct App<'a> {
    relative_client: &'a str,
    module_name: &'a str,
    contract: &'a str,
    queries: &'a [(&'a str, &'a str)],
}

fn main() {
    let output = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("."), PathBuf::from);

    let apps = [
        App {
            relative_client: "examples/agent-alpha/generated/rust/client.rs",
            module_name: "agent_alpha",
            contract: include_str!("../../../examples/agent-alpha/riffdb/contract.riff"),
            queries: &[(
                "ItemPage",
                include_str!("../../../examples/agent-alpha/riffdb/queries/item_page.riffq"),
            )],
        },
        App {
            relative_client: "examples/agent-alpha/domains/agent-blog/generated/rust/client.rs",
            module_name: "agent_blog",
            contract: include_str!("../../../contracts/agent-alpha/blog.riff"),
            queries: &[
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
            ],
        },
        App {
            relative_client: "examples/agent-alpha/domains/agent-orders/generated/rust/client.rs",
            module_name: "agent_orders",
            contract: include_str!("../../../contracts/agent-alpha/orders.riff"),
            queries: &[
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
            ],
        },
    ];

    for app in apps {
        write_rust_client(&output, app);
    }
}

fn write_rust_client(root: &Path, app: App<'_>) {
    let contract = compile_contract_source(app.contract).expect("compile agent-alpha contract");
    let module = compile_module(app.module_name, app.queries, &contract);
    let path = root.join(app.relative_client);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("generated parent");
    }
    fs::write(path, generate_rust_client(&module, &contract)).expect("write rust client");
}

fn compile_module(name: &str, queries: &[(&str, &str)], contract: &ContractBundle) -> QueryModule {
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new(name).expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        queries
            .iter()
            .map(|(query_name, source)| {
                NamedQuerySource::new(*query_name, *source).expect("query source")
            })
            .collect(),
    )
    .expect("module candidate");
    QueryModule::compile(candidate, contract).expect("compile query module")
}
