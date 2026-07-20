//! Dependency and authority checks for the commit orchestration boundary.

const MANIFEST: &str = include_str!("../Cargo.toml");
const LIB_SOURCE: &str = include_str!("../src/lib.rs");
const CLOCK_SOURCE: &str = include_str!("../src/clock.rs");
const INITIALIZATION_SOURCE: &str = include_str!("../src/initialization.rs");
const OUTCOME_SOURCE: &str = include_str!("../src/outcome.rs");
const PROVENANCE_SOURCE: &str = include_str!("../src/provenance.rs");

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
        "tokio",
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
}

#[test]
fn this_slice_has_no_concrete_clock_entropy_transport_or_engine_authority() {
    let sources = [
        LIB_SOURCE,
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
