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
