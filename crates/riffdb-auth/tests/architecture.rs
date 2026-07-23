#![forbid(unsafe_code)]

//! Dependency, secret-custody, and bootstrap-module boundary guards.

use std::{fs, path::PathBuf};

const MANIFEST: &str = include_str!("../Cargo.toml");
const LOCKFILE: &str = include_str!("../../../Cargo.lock");
const LIB_ROOT: &str = include_str!("../src/lib.rs");
const AUTHENTICATOR_SOURCE: &str = include_str!("../src/authenticator.rs");
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

fn owners_with_reviewed_row(allowed: &[&str], row: &str) -> Vec<String> {
    let auth_crate = crate_root();
    let crates = auth_crate.parent().expect("workspace crates directory");
    let mut owners = allowed
        .iter()
        .filter_map(|owner| {
            let manifest = fs::read_to_string(crates.join(owner).join("Cargo.toml"))
                .expect("read reviewed owner manifest");
            manifest
                .lines()
                .any(|line| line == row)
                .then(|| (*owner).to_owned())
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
    let registries = [
        (
            "base64",
            "base64 = { version = \"=0.22.1\", default-features = false, features = [\"alloc\"] }",
            [
                "riffdb-api-mcp",
                "riffdb-auth",
                "riffdb-cli",
                "riffdb-proto",
                "riffdb-service",
            ]
            .as_slice(),
            ["riffdb-auth", "riffdb-proto", "riffdb-service"].as_slice(),
        ),
        (
            "zeroize",
            "zeroize = { version = \"=1.8.1\", default-features = false, features = [\"alloc\"] }",
            ["riffdb-auth", "riffdb-cli", "riffdb-client-rust"].as_slice(),
            ["riffdb-auth", "riffdb-client-rust"].as_slice(),
        ),
    ];
    for (dependency, row, allowed, required_now) in registries {
        let actual = production_dependency_owners(dependency);
        assert_eq!(
            actual,
            owners_with_reviewed_row(allowed, row),
            "direct owner or exact reviewed row changed for {dependency}"
        );
        for required in required_now {
            assert!(
                actual.iter().any(|owner| owner == required),
                "WP-137 requires the reviewed {dependency} edge in {required}"
            );
        }
    }
    assert_eq!(
        production_dependency_owners("getrandom"),
        ["riffdb-auth", "riffdb-client-rust", "riffdb-server"],
        "reviewed ADR-0018 getrandom owners changed"
    );
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

#[test]
fn retained_opaque_credential_stays_auth_owned_and_non_exposing() {
    for required in [
        "const RETAINED_OPAQUE_CREDENTIAL_BYTES: usize = 43;",
        "pub struct RetainedOpaqueCredential",
        "Zeroizing<[u8; RETAINED_OPAQUE_CREDENTIAL_BYTES]>",
        "pub fn borrow(&self) -> OpaqueCredential<'_>",
        "RetainedOpaqueCredential([REDACTED])",
    ] {
        assert!(
            AUTHENTICATOR_SOURCE.contains(required),
            "retained credential boundary changed: {required}"
        );
    }
    for forbidden in [
        "impl Clone for RetainedOpaqueCredential",
        "impl serde::Serialize for RetainedOpaqueCredential",
        "impl Serialize for RetainedOpaqueCredential",
        "impl AsRef<",
        "impl std::ops::Deref for RetainedOpaqueCredential",
        "impl Borrow<",
        "impl From<RetainedOpaqueCredential",
        "impl Into<",
    ] {
        assert!(
            !AUTHENTICATOR_SOURCE.contains(forbidden),
            "retained credential exposes an unreviewed capability: {forbidden}"
        );
    }
    let declaration = AUTHENTICATOR_SOURCE
        .find("pub struct RetainedOpaqueCredential")
        .expect("retained credential declaration");
    let declaration_attributes = AUTHENTICATOR_SOURCE[..declaration]
        .rsplit_once("\n\n")
        .map_or(&AUTHENTICATOR_SOURCE[..declaration], |(_, suffix)| suffix);
    assert!(
        !declaration_attributes.contains("Clone")
            && !declaration_attributes.contains("Serialize")
            && !declaration_attributes.contains("serde"),
        "retained credential gained a clone or serialization attribute"
    );
    let fields = AUTHENTICATOR_SOURCE
        .split_once("pub struct RetainedOpaqueCredential {")
        .and_then(|(_, suffix)| suffix.split_once("\n}\n\nimpl RetainedOpaqueCredential"))
        .map(|(fields, _)| fields)
        .expect("retained credential fields");
    assert_eq!(
        fields, "\n    bytes: Zeroizing<[u8; RETAINED_OPAQUE_CREDENTIAL_BYTES]>,\n    len: u8,",
        "retained credential fields or visibility changed"
    );
    let implementation = AUTHENTICATOR_SOURCE
        .split_once("impl RetainedOpaqueCredential {")
        .and_then(|(_, suffix)| suffix.split_once("\n}\n\nimpl fmt::Debug"))
        .map(|(implementation, _)| implementation)
        .expect("retained credential implementation");
    assert_eq!(
        implementation.matches("pub fn ").count(),
        2,
        "retained credential gained a public method"
    );
    assert!(implementation.contains("pub fn new(bytes: &[u8])"));
    assert!(implementation.contains("pub fn borrow(&self) -> OpaqueCredential<'_>"));
    let zeroize_row =
        "zeroize = { version = \"=1.8.1\", default-features = false, features = [\"alloc\"] }";
    let allowed = ["riffdb-auth", "riffdb-cli", "riffdb-client-rust"];
    let actual = production_dependency_owners("zeroize");
    assert_eq!(
        actual,
        owners_with_reviewed_row(&allowed, zeroize_row),
        "retained credentials must not add a new direct zeroize owner"
    );
    assert!(
        !actual.iter().any(|owner| owner == "riffdb-api-mcp"),
        "MCP must retain credentials through the auth-owned wrapper"
    );
}
