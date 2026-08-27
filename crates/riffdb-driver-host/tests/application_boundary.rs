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
fn v8_go_only_lock_accepts_a_compiler_runtime_catalog_without_an_mcp_artifact() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture = root.join("fixtures/driver/conformance-app");
    let mut lock: serde_json::Value = serde_json::from_slice(
        &fs::read(fixture.join("riffdb.application.lock.json")).expect("application lock"),
    )
    .expect("lock JSON");
    let mut manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(fixture.join("generated/riffdb.application.exact.json"))
            .expect("exact application manifest"),
    )
    .expect("manifest JSON");
    let mut catalog: serde_json::Value = serde_json::from_slice(
        &fs::read(fixture.join("generated/mcp/tools.json")).expect("operation catalog"),
    )
    .expect("catalog JSON");

    lock["schema"] = serde_json::Value::String("riffdb.application-lock/v8".to_owned());
    lock["artifacts"]
        .as_array_mut()
        .expect("artifacts")
        .retain(|artifact| artifact["kind"] != "mcp");
    manifest["schema"] = serde_json::Value::String("riffdb.application-manifest/v5".to_owned());
    manifest["generation"] = serde_json::json!({"go": "generated/go/client.go"});
    let manifest = serde_json::to_vec(&manifest).expect("manifest JSON");
    let manifest_hash = hex(riffdb_types::hash_generated_artifact(&manifest).as_bytes());
    lock["exact_manifest_hash"] = serde_json::Value::String(manifest_hash.clone());
    let manifest_artifact = lock["artifacts"]
        .as_array_mut()
        .expect("artifacts")
        .iter_mut()
        .find(|artifact| artifact["kind"] == "manifest")
        .expect("manifest artifact");
    manifest_artifact["content_hash"] = serde_json::Value::String(manifest_hash.clone());
    catalog["application_manifest_hash"] = serde_json::Value::String(manifest_hash);
    let catalog = serde_json::to_vec(&catalog).expect("catalog JSON");
    let lock = serde_json::to_vec(&lock).expect("lock JSON");

    let loaded = ApplicationCatalog::from_exact_artifacts(
        &lock,
        &manifest,
        &catalog,
        "default",
        "DriverConformanceApplication",
    )
    .expect("compiler runtime catalog");
    assert!(loaded.operation("driver_conformance_item_page").is_some());

    let mut declared_manifest: serde_json::Value =
        serde_json::from_slice(&manifest).expect("manifest JSON");
    declared_manifest["generation"]["mcp"] =
        serde_json::Value::String("generated/mcp/tools.json".to_owned());
    let declared_manifest = serde_json::to_vec(&declared_manifest).expect("manifest JSON");
    let declared_manifest_hash =
        hex(riffdb_types::hash_generated_artifact(&declared_manifest).as_bytes());
    let mut declared_lock: serde_json::Value = serde_json::from_slice(&lock).expect("lock JSON");
    declared_lock["exact_manifest_hash"] =
        serde_json::Value::String(declared_manifest_hash.clone());
    declared_lock["artifacts"]
        .as_array_mut()
        .expect("artifacts")
        .iter_mut()
        .find(|artifact| artifact["kind"] == "manifest")
        .expect("manifest artifact")["content_hash"] =
        serde_json::Value::String(declared_manifest_hash.clone());
    let declared_lock = serde_json::to_vec(&declared_lock).expect("lock JSON");
    let mut declared_catalog: serde_json::Value =
        serde_json::from_slice(&catalog).expect("catalog JSON");
    declared_catalog["application_manifest_hash"] =
        serde_json::Value::String(declared_manifest_hash);
    let declared_catalog = serde_json::to_vec(&declared_catalog).expect("catalog JSON");
    assert_eq!(
        ApplicationCatalog::from_exact_artifacts(
            &declared_lock,
            &declared_manifest,
            &declared_catalog,
            "default",
            "DriverConformanceApplication",
        )
        .expect_err("a declared MCP surface remains lock-anchored"),
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
