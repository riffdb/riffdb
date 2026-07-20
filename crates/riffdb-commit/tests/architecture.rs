//! Dependency and authority checks for the commit orchestration boundary.

use std::{fs, path::PathBuf};

const LOCKFILE: &str = include_str!("../../../Cargo.lock");
const MANIFEST: &str = include_str!("../Cargo.toml");
const AUDIT_SOURCE: &str = include_str!("../src/audit.rs");
const LIB_SOURCE: &str = include_str!("../src/lib.rs");
const CLOCK_SOURCE: &str = include_str!("../src/clock.rs");
const INITIALIZATION_SOURCE: &str = include_str!("../src/initialization.rs");
const OUTCOME_SOURCE: &str = include_str!("../src/outcome.rs");
const PROVENANCE_SOURCE: &str = include_str!("../src/provenance.rs");

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
fn manifest_has_only_the_foundational_dependencies_needed_by_this_slice() {
    let dependency_names = MANIFEST
        .lines()
        .filter_map(|line| line.split_once(" = ").map(|(name, _)| name))
        .filter(|name| name.starts_with("riffdb-"))
        .collect::<Vec<_>>();

    assert_eq!(dependency_names, vec!["riffdb-storage-api", "riffdb-types"]);

    for forbidden in [
        "riffdb-service",
        "riffdb-proto",
        "riffdb-api-grpc",
        "riffdb-api-mcp",
        "riffdb-storage-memory",
        "riffdb-storage-redb",
        "riffdb-runtime",
        "riffdb-conflict",
        "riffdb-idempotency",
        "riffdb-catalog",
        "riffdb-policy",
        "riffdb-auth",
        "tonic",
        "rmcp",
        "redb",
        "getrandom",
        "rand",
        "uuid",
    ] {
        assert!(
            !MANIFEST.contains(forbidden),
            "commit manifest contains forbidden dependency {forbidden}"
        );
    }

    assert_eq!(
        MANIFEST.lines().find(|line| line.starts_with("tokio =")),
        Some(
            "tokio = { version = \"=1.52.0\", default-features = false, features = [\"rt\", \"sync\"] }"
        ),
        "Tokio must retain the exact reviewed current-thread channel feature graph"
    );
}

#[test]
fn reviewed_tokio_owner_and_lock_graph_are_frozen() {
    assert_eq!(
        production_dependency_owners("tokio"),
        ["riffdb-commit"],
        "a new production Tokio owner requires dependency and feature-unification review"
    );
    for exact_entry in [
        "name = \"tokio\"\nversion = \"1.52.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"a91135f59b1cbf38c91e73cf3386fca9bb77915c45ce2771460c9d92f0f3d776\"\ndependencies = [\n \"pin-project-lite\",\n]",
        "name = \"pin-project-lite\"\nversion = \"0.2.17\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"a89322df9ebe1c1578d689c92318e070967d1042b512afbe49518723f4e6d5cd\"",
    ] {
        assert!(
            LOCKFILE.contains(exact_entry),
            "reviewed coordinator dependency lock entry changed: {exact_entry}"
        );
    }
}

#[test]
fn this_slice_has_no_concrete_clock_entropy_transport_or_engine_authority() {
    let sources = [
        LIB_SOURCE,
        AUDIT_SOURCE,
        CLOCK_SOURCE,
        INITIALIZATION_SOURCE,
        OUTCOME_SOURCE,
        PROVENANCE_SOURCE,
    ]
    .join("\n");

    for forbidden in [
        "std::time::",
        "SystemTime",
        "UNIX_EPOCH",
        "getrandom::",
        "rand::",
        "tokio::",
        "tonic::",
        "rmcp::",
        "redb::",
        "riffdb_service",
        "riffdb_proto",
        "riffdb_storage_memory",
        "riffdb_storage_redb",
    ] {
        assert!(
            !sources.contains(forbidden),
            "commit source contains forbidden authority {forbidden}"
        );
    }
}

#[test]
fn audit_view_is_borrowed_and_has_no_durable_or_policy_authority() {
    let production_source = AUDIT_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(AUDIT_SOURCE, |(production, _)| production);

    assert!(production_source.contains("pub trait AdministrationAuditInputView: Send + Sync"));
    let trait_body = production_source
        .split_once("pub trait AdministrationAuditInputView: Send + Sync {")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("audit view trait body");
    assert_eq!(
        trait_body.matches("    fn ").count(),
        11,
        "audit view must expose exactly the approved borrowed fields"
    );
    assert_eq!(
        production_source
            .matches("impl AdministrationAuditInputView for")
            .count(),
        0,
        "commit must not own the concrete service audit input"
    );
    for signature in [
        "fn request_id(&self) -> &RequestId",
        "fn operation(&self) -> &ServiceOperationV1",
        "fn phase(&self) -> &ServiceAuditPhaseV1",
        "fn principal_id(&self) -> &ActorId",
        "fn actor_kind(&self) -> &ActorKind",
        "fn capability_id(&self) -> &CapabilityId",
        "fn capability_revision(&self) -> &NonZeroU64",
        "fn ingress(&self) -> &ServiceIngressKindV1",
        "fn targets(&self) -> &ServiceAuditTargetsV1",
        "fn approval_id(&self) -> Option<&ApprovalId>",
        "fn link(&self) -> &ServiceAuditLinkV1",
    ] {
        assert!(
            production_source.contains(signature),
            "audit view is missing borrowed field {signature}"
        );
    }

    for forbidden in [
        "fn administration_sequence(&self)",
        "fn audit_record_sequence(&self)",
        "fn assigned_sequence(&self)",
        "Timestamp",
        "ServiceAuditAppendIntentV1",
        "ServiceAuditAppendRepository",
        "append_service_audit",
        "StorageEngine",
        "StorageWrite",
        "PolicyDecision",
        "AuthorizationDecision",
        "RawCredential",
        "riffdb_service",
        "serde::",
        "prost::",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "audit interface crosses forbidden authority through {forbidden}"
        );
    }
}

#[test]
fn bootstrap_audit_proof_is_sealed_and_nonserializable() {
    let production_source = AUDIT_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(AUDIT_SOURCE, |(production, _)| production);

    assert!(production_source.contains("pub struct BootstrapCompoundAuditProof"));
    assert!(production_source.contains("_private: BootstrapCompoundAuditProofSeal"));
    for forbidden in [
        "derive(Clone",
        "derive(Copy",
        "derive(Default",
        "impl Default for BootstrapCompoundAuditProof",
        "Serialize",
        "Deserialize",
        "Message",
        "Encode",
        "Decode",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "bootstrap proof gains forbidden capability through {forbidden}"
        );
    }
}

#[test]
fn initialization_transition_has_one_private_call_site_and_no_handle_escape() {
    let production_source = INITIALIZATION_SOURCE
        .split_once("#[cfg(test)]")
        .map_or(INITIALIZATION_SOURCE, |(production, _)| production);
    assert_eq!(
        production_source.matches(".initialize_database(").count(),
        1
    );
    assert!(production_source.contains("pub fn probe(self)"));
    assert!(production_source.contains("Existing(InitializedDatabase<Storage>)"));
    assert!(production_source.contains("Storage: StructuralEvidenceOpen"));
    assert_eq!(
        production_source
            .matches("pub fn begin_structural_evidence(")
            .count(),
        1
    );
    for forbidden in [
        "impl DatabaseInitializationPort for",
        "into_inner",
        "into_storage",
        "storage_mut",
        "pub fn storage",
    ] {
        assert!(
            !production_source.contains(forbidden),
            "initialization wrapper leaks or duplicates storage authority through {forbidden}"
        );
    }
}

#[test]
fn response_wrapper_does_not_introduce_a_persisted_replay_record() {
    assert!(OUTCOME_SOURCE.contains("stored_outcome: StoredOutcomeV1"));
    assert!(OUTCOME_SOURCE.contains("CommittedOutcomeDisposition"));
    for forbidden in [
        "StoredReplay",
        "StoredCommittedOutcome",
        "StoredEnvelope",
        "riffdb_proto",
        "prost::",
        "encode_to_vec",
        "decode(",
    ] {
        assert!(
            !OUTCOME_SOURCE.contains(forbidden),
            "response wrapper crosses a durable-format boundary through {forbidden}"
        );
    }
}
