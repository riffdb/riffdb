//! Exact generated-catalog authority tests.

use std::fs;

use riffdb_driver_host::{ApplicationCatalog, CatalogError, OperationKind};

#[test]
fn generated_catalog_is_the_only_dispatch_authority() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let lock = fs::read(root.join("examples/agent-alpha/riffdb.application.lock.json"))
        .expect("application lock");
    let tools = fs::read(root.join("examples/agent-alpha/generated/mcp/tools.json"))
        .expect("generated operation catalog");
    let manifest =
        fs::read(root.join("examples/agent-alpha/generated/riffdb.application.exact.json"))
            .expect("exact application manifest");

    let catalog = ApplicationCatalog::from_exact_artifacts(
        &lock,
        &manifest,
        &tools,
        "default",
        "AgentAlphaApplication",
    )
    .expect("exact catalog");
    let command = catalog
        .operation("agent_alpha_create_item")
        .expect("generated command");
    assert_eq!(command.kind(), OperationKind::Command);
    assert_eq!(command.symbol(), "CreateItem");
    assert!(catalog.operation("GetEntity").is_none());
    assert!(catalog.operation("ScanIndex").is_none());
    assert!(catalog.operation("contract_deploy").is_none());
}

#[test]
fn catalog_rejects_manifest_or_plan_drift() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let lock = fs::read(root.join("examples/agent-alpha/riffdb.application.lock.json"))
        .expect("application lock");
    let mut tools: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("examples/agent-alpha/generated/mcp/tools.json"))
            .expect("generated catalog"),
    )
    .expect("json");
    let manifest =
        fs::read(root.join("examples/agent-alpha/generated/riffdb.application.exact.json"))
            .expect("exact application manifest");
    tools["commands"][0]["plan_hash"] = serde_json::Value::String("00".repeat(32));

    assert_eq!(
        ApplicationCatalog::from_exact_artifacts(
            &lock,
            &manifest,
            &serde_json::to_vec(&tools).expect("json"),
            "default",
            "AgentAlphaApplication",
        )
        .expect_err("drift must fail closed"),
        CatalogError::IdentityMismatch,
    );
}

#[test]
fn v3_catalog_dispatches_sdk_only_secret_query_and_v2_refuses_it() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture = root.join("fixtures/driver/conformance-app");
    let lock = fs::read(fixture.join("riffdb.application.lock.json")).expect("application lock");
    let tools = fs::read(fixture.join("generated/mcp/tools.json")).expect("operation catalog");
    let manifest = fs::read(fixture.join("generated/riffdb.application.exact.json"))
        .expect("exact application manifest");

    let catalog = ApplicationCatalog::from_exact_artifacts(
        &lock,
        &manifest,
        &tools,
        "default",
        "DriverConformanceApplication",
    )
    .expect("V3 SDK query catalog");
    let secret = catalog
        .operation("driver_conformance_item_secret")
        .expect("SDK-only secret query");
    assert_eq!(secret.kind(), OperationKind::Query);
    assert_eq!(secret.symbol(), "ItemSecret");

    let tools_value: serde_json::Value = serde_json::from_slice(&tools).expect("catalog JSON");
    assert!(
        tools_value["tools"]
            .as_array()
            .expect("MCP tools")
            .iter()
            .all(|tool| tool["operation_name"] != "ItemSecret")
    );
    assert!(
        tools_value["sdk_tools"]
            .as_array()
            .expect("SDK-only tools")
            .iter()
            .any(|tool| tool["operation_name"] == "ItemSecret")
    );

    let mut downgraded_tools = tools_value;
    downgraded_tools["schema"] =
        serde_json::Value::String("riffdb-generated-application-operations/v2".to_owned());
    let downgraded_tools = serde_json::to_vec(&downgraded_tools).expect("catalog JSON");
    let mut matching_lock: serde_json::Value = serde_json::from_slice(&lock).expect("lock JSON");
    let mcp_artifact = matching_lock["artifacts"]
        .as_array_mut()
        .expect("artifacts")
        .iter_mut()
        .find(|artifact| artifact["kind"] == "mcp")
        .expect("MCP artifact");
    mcp_artifact["content_hash"] = serde_json::Value::String(hex(
        riffdb_types::hash_generated_artifact(&downgraded_tools).as_bytes(),
    ));
    let matching_lock = serde_json::to_vec(&matching_lock).expect("lock JSON");

    assert_eq!(
        ApplicationCatalog::from_exact_artifacts(
            &matching_lock,
            &manifest,
            &downgraded_tools,
            "default",
            "DriverConformanceApplication",
        )
        .expect_err("a V2-only reader must refuse the V3 SDK registry"),
        CatalogError::IdentityMismatch,
    );
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("write to String");
    }
    output
}
