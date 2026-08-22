#![allow(missing_docs)]

use std::fs;
use std::path::PathBuf;

#[test]
fn application_adapter_has_no_transport_or_crypto_dependency() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let source = fs::read_to_string(manifest).expect("read adapter manifest");
    for forbidden in [
        "tonic",
        "hyper",
        "http",
        "tokio",
        "rustls",
        "ring",
        "riffdb-api-grpc",
        "riffdb-api-frame",
    ] {
        assert!(
            !source.lines().any(|line| {
                let line = line.trim_start();
                line.starts_with(forbidden)
                    && line[forbidden.len()..]
                        .chars()
                        .next()
                        .is_some_and(|next| next.is_whitespace() || next == '=')
            }),
            "API-neutral adapter must not depend on {forbidden}"
        );
    }
}
