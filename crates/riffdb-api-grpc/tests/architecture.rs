#![forbid(unsafe_code)]

//! Architecture assertions for the gRPC adapter feature and authority graph.

#[test]
fn grpc_adapter_has_no_lower_semantic_authority_dependency() {
    let manifest = include_str!("../Cargo.toml");
    for banned in [
        "riffdb-catalog",
        "riffdb-commit",
        "riffdb-conflict",
        "riffdb-policy",
        "riffdb-runtime",
        "riffdb-storage-api",
        "riffdb-storage-memory",
        "riffdb-storage-redb",
    ] {
        assert!(
            !manifest
                .lines()
                .any(|line| line.trim_start().starts_with(banned)),
            "gRPC adapter must not depend on {banned}"
        );
    }
}

#[test]
fn tonic_runtime_features_remain_narrow_and_default_disabled() {
    let manifest = include_str!("../Cargo.toml");
    let runtime_dependencies = manifest
        .split("[build-dependencies]")
        .next()
        .expect("runtime dependency section");
    assert!(manifest.contains("default = []"));
    assert!(manifest.contains("client = [\"tonic/channel\"]"));
    assert!(manifest.contains("\"tonic/router\","));
    assert!(manifest.contains("\"tonic/server\","));
    assert!(runtime_dependencies.contains("features = [\"codegen\"]"));
    assert!(!runtime_dependencies.contains("features = [\"transport\"]"));
    for forbidden in ["tls-", "gzip", "deflate", "zstd"] {
        assert!(!manifest.contains(forbidden));
    }
}

#[test]
fn client_feature_does_not_activate_server_semantic_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    let client = manifest
        .lines()
        .find(|line| line.starts_with("client ="))
        .expect("client feature declaration");
    assert_eq!(client, "client = [\"tonic/channel\"]");

    for dependency in [
        "riffdb-auth",
        "riffdb-errors",
        "riffdb-service",
        "riffdb-types",
    ] {
        let line = manifest
            .lines()
            .find(|line| line.starts_with(dependency))
            .expect("server dependency declaration");
        assert!(
            line.contains("optional = true"),
            "{dependency} must be optional"
        );
        assert!(
            manifest.contains(&format!("\"dep:{dependency}\"")),
            "server feature must explicitly own {dependency}"
        );
    }

    let source = include_str!("../src/lib.rs");
    for module in ["authentication", "conversion", "error", "server"] {
        assert!(
            source.contains(&format!("#[cfg(feature = \"server\")]\nmod {module};")),
            "{module} must remain server-only"
        );
    }
}

#[test]
fn generated_servers_are_wrapped_with_exact_public_message_limits() {
    let source = include_str!("../src/server.rs");
    assert_eq!(
        source
            .matches(".max_decoding_message_size(MAX_PUBLIC_REQUEST_BYTES)")
            .count(),
        5
    );
    assert_eq!(
        source
            .matches(".max_encoding_message_size(MAX_PUBLIC_RESPONSE_BYTES)")
            .count(),
        5
    );
    assert!(!source.contains("accept_compressed"));
    assert!(!source.contains("send_compressed"));
}

#[test]
fn staged_route_does_not_require_a_full_service_at_construction() {
    let source = include_str!("../src/server.rs");
    let application = source
        .split("pub struct GrpcApplication")
        .nth(1)
        .and_then(|tail| tail.split("impl GrpcApplication").next())
        .expect("GrpcApplication declaration");
    assert!(!application.contains("Arc<dyn ApplicationService>"));
    assert!(application.contains("Arc<dyn GrpcLifecycleRoute>"));
}

#[test]
fn capability_cancellation_guard_outlives_invocation_construction() {
    let source = include_str!("../src/server.rs");
    let handler = source
        .split("async fn create_capability(")
        .nth(1)
        .and_then(|tail| tail.split("async fn revoke_capability(").next())
        .expect("create-capability handler");
    assert!(handler.contains("let (invocation, _cancellation) = match"));
    assert!(!handler.contains("let (context, _cancellation)"));
}
