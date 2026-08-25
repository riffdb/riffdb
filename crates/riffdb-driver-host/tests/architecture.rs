//! Static pins for the application-only local protocol boundary.

use std::fs;

#[test]
fn host_has_no_direct_kernel_or_grpc_protocol_dependency() {
    let manifest = fs::read_to_string(format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR")))
        .expect("driver manifest");
    let production = manifest
        .split("[dev-dependencies]")
        .next()
        .expect("production manifest");
    for forbidden in ["riffdb-proto", "riffdb-api-grpc", "tonic ="] {
        assert!(
            !production.contains(forbidden),
            "driver host directly names {forbidden}"
        );
    }
}

#[test]
fn local_request_inventory_stays_closed() {
    let source = fs::read_to_string(format!("{}/src/protocol.rs", env!("CARGO_MANIFEST_DIR")))
        .expect("protocol source");
    for required in ["Handshake {", "Invoke {", "Batch {", "Cancel {"] {
        assert!(
            source.contains(required),
            "missing closed request: {required}"
        );
    }
    for forbidden in [
        "GetEntity",
        "ScanIndex",
        "DeployContract",
        "raw_protobuf",
        "capability_token",
        "bearer_credential",
    ] {
        assert!(
            !source.contains(forbidden),
            "local protocol grew forbidden surface: {forbidden}"
        );
    }
}

#[test]
fn established_local_sessions_are_long_lived_and_bounded_by_admission_not_idle_time() {
    let source = fs::read_to_string(format!("{}/src/socket.rs", env!("CARGO_MANIFEST_DIR")))
        .expect("socket source");
    assert_eq!(
        source
            .matches("tokio::time::timeout(HANDSHAKE_TIMEOUT")
            .count(),
        1,
        "only an unauthenticated pre-handshake peer may be timed out"
    );
    assert!(
        source.contains(
            "request=FrameCodec::read_request(&mut reader)=>match request{Ok(request)=>request,Err(_)=>break}"
        ),
        "an authenticated generated client must remain attached across arbitrarily quiet role intervals"
    );
    assert!(
        !source.contains("IDLE_CONNECTION_TIMEOUT"),
        "the bounded connection inventory replaces an established-session idle timeout"
    );
}

#[test]
fn python_binding_has_no_parallel_value_query_command_or_error_core() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let manifest =
        fs::read_to_string(workspace.join("crates/riffdb-client-python-native/Cargo.toml"))
            .expect("Python native manifest");
    assert!(
        !manifest.contains("riffdb-client-rust"),
        "the binding must reach the application client only through the protocol core"
    );
    let source =
        fs::read_to_string(workspace.join("crates/riffdb-client-python-native/src/lib.rs"))
            .expect("Python native source");
    for forbidden in [
        "fn parse_value(",
        "fn parse_query(",
        "fn parse_command(",
        "MAX_BRIDGE_DEPTH",
        "MAX_BRIDGE_VALUES",
        "fn non_application_client_error_kind(",
        "fn public_application_error_json(",
        "fn application_error_json(",
    ] {
        assert!(
            !source.contains(forbidden),
            "Python binding retained duplicated protocol rule: {forbidden}"
        );
    }
    for required in [
        "parse_in_process_query",
        "parse_in_process_command",
        "normalize_python_value",
        "classify_application_client_error",
        "classify_client_error",
    ] {
        assert!(
            source.contains(required),
            "Python binding does not consume shared core rule: {required}"
        );
    }
}
