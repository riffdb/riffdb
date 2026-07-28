#![forbid(unsafe_code)]

//! Process fixture that exposes the real bounded MCP stdio transport.

#[allow(dead_code)]
#[path = "conformance_support/backend.rs"]
mod backend;

use backend::ConformanceBackend;
use riffdb_api_mcp::{McpStdioClientActivity, serve_mcp_stdio};

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("conformance stdio runtime");
    let backend = ConformanceBackend::default();
    runtime
        .block_on(serve_mcp_stdio(
            backend.clone(),
            backend,
            McpStdioClientActivity::new(),
        ))
        .expect("conformance stdio server");
}
