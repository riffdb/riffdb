#![forbid(unsafe_code)]
#![allow(missing_docs)]

use std::fs;
use std::path::PathBuf;

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn native_bridge_keeps_transport_and_waits_in_rust() {
    let source = fs::read_to_string(crate_root().join("src/lib.rs")).expect("read native source");

    assert!(source.starts_with("#![forbid(unsafe_code)]"));
    assert!(source.contains("StableApplicationClient"));
    assert!(source.contains("pyo3_async_runtimes::tokio::future_into_py"));
    assert!(source.contains(".detach(||"));
    assert!(!source.contains("allow(unsafe_code)"));
    assert!(!source.contains("Python::with_gil"));
}

#[test]
fn native_bridge_has_no_kernel_or_storage_dependency() {
    let manifest =
        fs::read_to_string(crate_root().join("Cargo.toml")).expect("read native manifest");

    assert!(manifest.contains("riffdb-client-rust"));
    for forbidden in [
        "riffdb-storage-",
        "riffdb-commit",
        "riffdb-runtime",
        "riffdb-command-runtime",
        "riffdb-admin",
        "riffdb-kernel",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "forbidden dependency {forbidden}"
        );
    }
}

#[test]
fn public_python_package_has_no_transport_fallback() {
    let package_root = crate_root().join("../../clients/python/runtime/src/riffdb_application");
    let init = fs::read_to_string(package_root.join("__init__.py")).expect("read runtime facade");
    let stub = fs::read_to_string(package_root.join("_native.pyi")).expect("read native stub");

    assert!(init.contains("from . import _native"));
    assert!(!init.contains("import grpc"));
    assert!(!init.contains("import requests"));
    assert!(!init.contains("import httpx"));
    assert!(!stub.contains("protobuf"));
    assert!(!stub.contains("kernel"));
}

#[test]
fn native_errors_and_credentials_are_closed() {
    let source = fs::read_to_string(crate_root().join("src/lib.rs")).expect("read native source");

    assert!(source.contains("BearerCredential([REDACTED])"));
    assert!(source.contains("bearer credentials cannot be serialized"));
    assert!(source.contains("load_protected_bearer_credential"));
    assert!(source.contains("NativeError"));
    assert!(source.contains("public_application_error_json"));
    assert!(source.contains("ClientError::Public(_) | ClientError::Application(_) =>"));
    assert!(source.contains("handled by semantic/public guards"));
    assert!(!source.contains("format!(\"{error}"));
    assert!(!source.contains("error.to_string()"));
}
