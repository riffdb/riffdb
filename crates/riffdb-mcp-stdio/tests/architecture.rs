#![forbid(unsafe_code)]

//! Architecture guards for the public-client-only stdio process.

use std::fs;
use std::path::PathBuf;

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn section<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    let start = source
        .find(start)
        .unwrap_or_else(|| panic!("missing {start}"));
    let remainder = &source[start..];
    let end = remainder
        .find(end)
        .unwrap_or_else(|| panic!("missing {end} after {start}"));
    &remainder[..end]
}

fn assert_in_order(source: &str, tokens: &[&str]) {
    let mut remainder = source;
    for token in tokens {
        let position = remainder
            .find(token)
            .unwrap_or_else(|| panic!("missing ordered token {token}"));
        remainder = &remainder[position + token.len()..];
    }
}

#[test]
fn binary_uses_current_thread_runtime_and_stdout_only_for_doctor_result() {
    let source = fs::read_to_string(crate_root().join("src/main.rs")).expect("read main source");
    assert!(source.contains("#[tokio::main(flavor = \"current_thread\")]"));
    assert!(
        !source
            .lines()
            .any(|line| line.trim_start().starts_with("println!"))
    );
    assert!(
        !source
            .lines()
            .any(|line| line.trim_start().starts_with("print!("))
    );
    assert_eq!(source.matches("std::io::stdout").count(), 1);
    let doctor = section(
        &source,
        "if std::env::args_os().nth(1)",
        "match riffdb_mcp_stdio::run().await",
    );
    assert!(doctor.contains("riffdb_mcp_stdio::doctor().await"));
    assert!(doctor.contains("std::io::stdout"));
    assert!(source.contains("eprintln!"));
    assert!(source.contains("riffdb_mcp_stdio::run().await"));
}

#[test]
fn telemetry_install_is_fixed_stderr_non_ansi_and_non_reloadable() {
    let source =
        fs::read_to_string(crate_root().join("src/telemetry.rs")).expect("read telemetry source");
    assert!(source.contains("riffdb_api_mcp::allows_transport_trace_target"));
    assert!(source.contains("riffdb_api_mcp::is_safe_transport_trace_target"));
    assert!(source.contains(".with_writer(io::stderr)"));
    assert!(source.contains(".with_ansi(false)"));
    assert!(source.contains(".with_filter(safe_target_filter)"));
    assert!(source.contains(".with(formatting)\n        .with(hard_filter)"));
    for forbidden in [
        "EnvFilter",
        ".reload(",
        "io::stdout",
        "RIFFDB_",
        "target.starts_with",
    ] {
        assert!(!source.contains(forbidden), "{forbidden}");
    }
}

#[test]
fn production_graph_has_no_local_authority_or_server_imports() {
    let manifest =
        fs::read_to_string(crate_root().join("Cargo.toml")).expect("read crate manifest");
    for forbidden in [
        "riffdb-auth",
        "riffdb-service",
        "riffdb-policy",
        "riffdb-server",
        "riffdb-runtime",
        "riffdb-commit",
        "riffdb-catalog",
        "riffdb-storage",
        "riffdb-api-grpc",
        "rt-multi-thread",
    ] {
        assert!(!manifest.contains(forbidden), "{forbidden}");
    }
    assert!(manifest.contains("features = [\"stdio\"]"));
    assert!(manifest.contains("features = [\"macros\", \"rt\"]"));
}

#[test]
fn config_surface_names_only_the_accepted_flags_and_environment() {
    let source =
        fs::read_to_string(crate_root().join("src/config.rs")).expect("read config source");
    let production = source
        .split("#[cfg(test)]")
        .next()
        .expect("production config source");
    for required in [
        "RIFFDB_MCP_CONFIG",
        "RIFFDB_MCP_ENDPOINT",
        "RIFFDB_MCP_CAPABILITY_TOKEN",
        "RIFFDB_MCP_CREDENTIAL_FILE",
        "\"--config\"",
        "\"--endpoint\"",
        "load_protected_bearer_credential",
    ] {
        assert!(production.contains(required), "{required}");
    }
    for forbidden in [
        "\"--token\"",
        "\"--credential-file\"",
        "\"--audience\"",
        "\"--tenant\"",
        "riffdb_auth",
    ] {
        assert!(!production.contains(forbidden), "{forbidden}");
    }
}

#[test]
fn only_the_client_request_backend_receives_the_stdio_activity_handle() {
    let startup =
        fs::read_to_string(crate_root().join("src/startup.rs")).expect("read startup source");
    assert!(startup.contains("let client_activity = McpStdioClientActivity::new();"));
    assert!(startup.contains("PublicGrpcMcpBackend::with_client_activity("));
    assert!(startup.contains("client_activity.clone(),"));
    assert!(startup.contains("PublicGrpcMcpObserverBackend::new(client, metadata),"));
    assert!(startup.contains("client_activity,\n    )"));

    let observer = fs::read_to_string(crate_root().join("src/observer_backend.rs"))
        .expect("read observer backend source");
    assert!(!observer.contains("McpStdioClientActivity"));
    assert!(!observer.contains("with_client_activity"));
}

#[test]
fn observer_physical_calls_are_charged_before_every_stdio_dispatch() {
    let observer = fs::read_to_string(crate_root().join("src/observer_backend.rs"))
        .expect("read observer backend source");
    let compact = section(
        &observer,
        "fn discover_compact<'a>",
        "fn observe_subscribed_resource<'a>",
    );
    assert_eq!(compact.matches(".compact_discovery()").count(), 1);
    assert_eq!(compact.matches(".charge()").count(), 1);
    assert_in_order(
        compact,
        &[
            ".compact_discovery()",
            ".charge()",
            "match inventory",
            ".discover_command_tools(",
        ],
    );
    assert_in_order(
        compact,
        &[
            ".compact_discovery()",
            ".charge()",
            "match inventory",
            ".discover_resources(",
        ],
    );

    let subscription = section(
        &observer,
        "fn observe_subscribed_resource<'a>",
        "fn compact_tool_request(",
    );
    assert_in_order(
        subscription,
        &[
            ".subscribed_resource()",
            "let observation = match locator",
            ".observe_command_plan(",
        ],
    );
    assert_in_order(
        subscription,
        &[
            ".subscribed_resource()",
            "let observation = match locator",
            ".read_resource_locator(",
        ],
    );
    for required in [
        "McpResourceLocator::ActiveContract",
        "McpResourceLocator::CommandPlan",
        "McpResourceLocator::ProjectionStatus",
        "McpResourceLocator::ServerHealth",
        "McpResourceLocator::ContractVersion",
        "McpResourceLocator::EntitySchema",
        "McpResourceLocator::CommandDocumentation",
        "McpResourceLocator::Outcome",
        "McpResourceLocator::Commit",
        "McpResourceLocator::Provenance",
    ] {
        assert!(subscription.contains(required), "{required}");
    }
    assert!(!subscription.contains("\n                locator =>"));

    let backend =
        fs::read_to_string(crate_root().join("src/backend.rs")).expect("read backend source");
    for (start, end, dispatch) in [
        (
            "McpResourceLocator::ActiveContract =>",
            "McpResourceLocator::ContractVersion",
            ".get_active_contract(",
        ),
        (
            "McpResourceLocator::ProjectionStatus {",
            "McpResourceLocator::ServerHealth =>",
            ".get_projection_status(",
        ),
        (
            "McpResourceLocator::ServerHealth =>",
            "McpResourceLocator::ReactiveWakeup =>",
            ".health(",
        ),
        (
            "McpResourceLocator::ReactiveWakeup =>",
            "pub(crate) async fn observe_command_plan",
            ".get_reactive_wakeup(",
        ),
    ] {
        let resource = section(&backend, start, end);
        assert_eq!(
            resource
                .matches("invocation.charge_observer_physical_call()?;")
                .count(),
            1,
            "{start}"
        );
        assert_in_order(
            resource,
            &["invocation.charge_observer_physical_call()?;", dispatch],
        );
    }

    let command_plan = section(
        &backend,
        "async fn authorize_command_resource(",
        "async fn resolve_dynamic(",
    );
    assert_eq!(
        command_plan
            .matches("invocation.charge_observer_physical_call()?;")
            .count(),
        3
    );
    assert_eq!(command_plan.matches(".discover_resources(").count(), 2);
    assert_eq!(command_plan.matches(".explain_command(").count(), 1);
    assert_in_order(
        command_plan,
        &[
            "invocation.charge_observer_physical_call()?;",
            ".discover_resources(",
            "invocation.charge_observer_physical_call()?;",
            ".explain_command(",
            "invocation.charge_observer_physical_call()?;",
            ".discover_resources(",
        ],
    );
}

#[test]
fn removed_stdio_parity_probe_cannot_reappear() {
    let backend =
        fs::read_to_string(crate_root().join("src/backend.rs")).expect("read backend source");
    for forbidden in [
        "ProbeState",
        "ensure_parity_surface_probe",
        "run_parity_surface_probe",
        "scan_complete_tool_inventory",
        "scan_complete_resource_inventory",
    ] {
        assert!(!backend.contains(forbidden), "{forbidden}");
    }

    let tool_discovery = section(
        &backend,
        "async fn discover_tool_page(",
        "async fn discover_resource_page(",
    );
    assert_eq!(
        tool_discovery.matches(".discover_command_tools(").count(),
        1
    );
    let resource_discovery = section(
        &backend,
        "async fn discover_resource_page(",
        "pub(crate) async fn read_resource_locator(",
    );
    assert_eq!(
        resource_discovery.matches(".discover_resources(").count(),
        1
    );
}
