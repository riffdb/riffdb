#![forbid(unsafe_code)]

//! Production dependency-policy guards for the hosted P1 server.

const MANIFEST: &str = include_str!("../Cargo.toml");
const LOCKFILE: &str = include_str!("../../../Cargo.lock");

fn production_dependencies() -> &'static str {
    MANIFEST
        .split_once("[dependencies]")
        .expect("server dependencies section")
        .1
        .split_once("[dev-dependencies]")
        .expect("server dev-dependencies boundary")
        .0
}

fn development_dependencies() -> &'static str {
    MANIFEST
        .split_once("[dev-dependencies]")
        .expect("server dev-dependencies section")
        .1
        .split_once("[[test]]")
        .expect("server external-test boundary")
        .0
}

#[test]
fn production_transport_features_are_exact_and_default_disabled() {
    let production = production_dependencies();
    assert!(MANIFEST.contains("[features]\ndefault = []"));
    assert!(production.contains(
        "riffdb-api-grpc = { version = \"0.1.0\", path = \"../riffdb-api-grpc\", default-features = false, features = [\"server\"] }"
    ));
    assert!(production.contains(
        "tokio = { version = \"=1.52.0\", default-features = false, features = [\"macros\", \"rt-multi-thread\", \"sync\", \"time\"] }"
    ));
    assert!(production.contains(
        "tonic = { version = \"=0.14.6\", default-features = false, features = [\"router\", \"server\"] }"
    ));

    for forbidden in [
        "riffdb-client-rust",
        "riffdb-proto",
        "riffdb-storage-memory",
        "base64 =",
        "features = [\"transport\"]",
        "channel",
        "tls",
        "compression",
        "gzip",
        "zstd",
    ] {
        assert!(
            !production.contains(forbidden),
            "forbidden production dependency capability: {forbidden}"
        );
    }
}

#[test]
fn client_and_public_message_helpers_remain_test_only() {
    let development = development_dependencies();
    assert!(development.contains("riffdb-client-rust"));
    assert!(development.contains("riffdb-proto"));
}

#[test]
fn lockfile_has_one_base64_and_no_tls_or_compression_stack() {
    assert_eq!(LOCKFILE.matches("name = \"base64\"").count(), 1);
    let base64 = LOCKFILE
        .split_once("name = \"base64\"")
        .expect("locked base64 package")
        .1;
    assert!(base64.starts_with("\nversion = \"0.22.1\""));

    for forbidden_package in [
        "rustls",
        "tokio-rustls",
        "native-tls",
        "openssl",
        "ring",
        "aws-lc-rs",
        "aws-lc-sys",
        "flate2",
        "zstd",
        "brotli",
    ] {
        assert!(
            !LOCKFILE.contains(&format!("name = \"{forbidden_package}\"")),
            "forbidden locked transport package: {forbidden_package}"
        );
    }
}
