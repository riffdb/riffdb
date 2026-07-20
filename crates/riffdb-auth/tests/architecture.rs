#![forbid(unsafe_code)]

//! Dependency, secret-custody, and bootstrap-module boundary guards.

use std::{fs, path::PathBuf};

const MANIFEST: &str = include_str!("../Cargo.toml");
const LOCKFILE: &str = include_str!("../../../Cargo.lock");
const LIB_ROOT: &str = include_str!("../src/lib.rs");
const BOOTSTRAP_SOURCE: &str = include_str!("../src/bootstrap_secret.rs");

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn production_dependency_owners(dependency: &str) -> Vec<String> {
    let root = crate_root();
    let crates = root.parent().expect("workspace crates directory");
    let mut owners = fs::read_dir(crates)
        .expect("read workspace crates")
        .filter_map(|entry| {
            let path = entry.expect("crate entry").path();
            let manifest = fs::read_to_string(path.join("Cargo.toml")).ok()?;
            let owns_dependency = manifest
                .lines()
                .skip_while(|line| *line != "[dependencies]")
                .skip(1)
                .take_while(|line| !line.starts_with('['))
                .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
                .any(|name| name == dependency);
            owns_dependency.then(|| {
                path.file_name()
                    .expect("crate directory name")
                    .to_string_lossy()
                    .into_owned()
            })
        })
        .collect::<Vec<_>>();
    owners.sort();
    owners
}

#[test]
fn direct_dependency_slice_is_exact() {
    let dependency_lines = MANIFEST
        .lines()
        .skip_while(|line| *line != "[dependencies]")
        .skip(1)
        .take_while(|line| !line.starts_with('['))
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    assert_eq!(
        dependency_lines,
        [
            "base64 = { version = \"=0.22.1\", default-features = false, features = [\"alloc\"] }",
            "getrandom = { version = \"=0.3.4\", default-features = false }",
            "riffdb-storage-api = { version = \"0.1.0\", path = \"../riffdb-storage-api\", default-features = false }",
            "riffdb-types = { version = \"0.1.0\", path = \"../riffdb-types\", default-features = false }",
            "zeroize = { version = \"=1.8.1\", default-features = false, features = [\"alloc\"] }",
        ]
    );
    for forbidden in [
        "hmac =",
        "sha2 =",
        "rand =",
        "tokio =",
        "serde =",
        "tracing =",
        "riffdb-policy =",
    ] {
        assert!(
            !dependency_lines.iter().any(|line| line.contains(forbidden)),
            "unreviewed direct auth dependency: {forbidden}"
        );
    }
}

#[test]
fn reviewed_dependency_owners_and_lock_entries_are_frozen() {
    for dependency in ["base64", "getrandom", "zeroize"] {
        assert_eq!(
            production_dependency_owners(dependency),
            ["riffdb-auth"],
            "WP-110 direct owner changed for {dependency}"
        );
    }
    for exact_entry in [
        "name = \"base64\"\nversion = \"0.22.1\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"72b3254f16251a8381aa12e40e3c4d2f0199f8c6508fbecb9d91f575e0fbb8c6\"",
        "name = \"getrandom\"\nversion = \"0.3.4\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"899def5c37c4fd7b2664648c28120ecec138e4d395b459e5ca34f9cce2dd77fd\"",
        "name = \"zeroize\"\nversion = \"1.8.1\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"ced3678a2879b30306d323f4542626697a464a97c0a07c9aebf7ebca65cd4dde\"",
    ] {
        assert!(
            LOCKFILE.contains(exact_entry),
            "reviewed dependency lock entry changed: {exact_entry}"
        );
    }
}

#[test]
fn first_party_auth_source_is_safe_rust_and_uses_central_hashing() {
    assert!(LIB_ROOT.starts_with("#![forbid(unsafe_code)]"));
    let mut source_paths = fs::read_dir(crate_root().join("src"))
        .expect("read auth source directory")
        .map(|entry| entry.expect("source entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect::<Vec<_>>();
    source_paths.sort();
    let sources = source_paths
        .iter()
        .map(|path| fs::read_to_string(path).expect("auth source is UTF-8"))
        .collect::<Vec<_>>()
        .join("\n");
    for forbidden in ["hmac::", "sha2::", "rand::", "unsafe {", "unsafe fn"] {
        assert!(
            !sources.contains(forbidden),
            "auth bypasses reviewed implementation boundary: {forbidden}"
        );
    }
    assert!(sources.contains("hash_capability_token_secret"));
    assert!(sources.contains("keyed_hash_secret"));
}

#[test]
fn bootstrap_module_remains_an_isolated_secret_helper() {
    for forbidden in [
        "digest_keys",
        "CapabilityDigestKeyProvider",
        "IdempotencyDigestKeyProvider",
        "DigestKeyProviders",
        "CredentialAuthenticator",
        "AuthenticatedPrincipal",
        "riffdb_storage",
        "riffdb_policy",
        "storage_api",
        "authorizer",
        "capability record",
    ] {
        assert!(
            !BOOTSTRAP_SOURCE.contains(forbidden),
            "bootstrap_secret crosses its isolation boundary: {forbidden}"
        );
    }
    assert!(BOOTSTRAP_SOURCE.contains("generate_bootstrap_credential"));
    assert!(BOOTSTRAP_SOURCE.contains("load_bootstrap_credential_file"));
    assert!(BOOTSTRAP_SOURCE.contains("read_bootstrap_credential"));
    assert!(BOOTSTRAP_SOURCE.contains("CapabilityId::from_unix_milliseconds_and_random"));
}
