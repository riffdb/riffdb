#![forbid(unsafe_code)]

//! Dependency, authority, and opacity checks for input-derived command facts.

const MANIFEST: &str = include_str!("../Cargo.toml");
const LIB_ROOT: &str = include_str!("../src/lib.rs");
const INPUT_FACTS_SOURCE: &str = include_str!("../src/input_facts.rs");

#[test]
fn input_facts_keep_the_existing_pure_dependency_boundary() {
    assert!(LIB_ROOT.starts_with("#![forbid(unsafe_code)]"));
    for allowed in ["riffdb-contract-ir", "riffdb-types"] {
        assert!(MANIFEST.contains(allowed));
    }
    for forbidden in [
        "riffdb-storage-api",
        "riffdb-catalog",
        "riffdb-policy",
        "riffdb-service",
        "riffdb-proto",
        "serde",
        "prost",
        "tokio",
    ] {
        assert!(!MANIFEST.contains(forbidden));
    }
}

#[test]
fn proof_is_opaque_move_only_nonserializable_and_redacted() {
    assert!(INPUT_FACTS_SOURCE.contains("pub struct InputDerivedCommandFacts"));
    assert!(INPUT_FACTS_SOURCE.contains("InputDerivedCommandFacts([REDACTED])"));
    assert!(INPUT_FACTS_SOURCE.contains("normalized_input: CanonicalRecord"));
    assert!(!INPUT_FACTS_SOURCE.contains("pub normalized_input:"));
    assert!(!INPUT_FACTS_SOURCE.contains("fn normalized_input("));
    assert!(!INPUT_FACTS_SOURCE.contains("impl Clone for InputDerivedCommandFacts"));
    for forbidden in [
        "derive(Clone",
        "derive(Copy",
        "derive(Default",
        "impl Default for InputDerivedCommandFacts",
        "Serialize",
        "Deserialize",
        "prost::",
        "Message",
    ] {
        assert!(!INPUT_FACTS_SOURCE.contains(forbidden));
    }
}

#[test]
fn derivation_has_no_io_time_entropy_or_acquisition_policy() {
    for forbidden in [
        "std::fs",
        "std::net",
        "SystemTime",
        "Instant",
        "Random",
        "thread_rng",
        "async fn",
        ".await",
        ".sort",
        ".dedup",
    ] {
        assert!(!INPUT_FACTS_SOURCE.contains(forbidden));
    }
    assert!(INPUT_FACTS_SOURCE.contains("ExpressionEvaluator::new(plan.expressions())"));
    assert!(INPUT_FACTS_SOURCE.contains("for derivation in plan.locality().conflict_keys()"));
    assert!(INPUT_FACTS_SOURCE.contains("pub fn matches_command("));
}

#[test]
fn proof_can_cross_the_bounded_executor_boundary() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<riffdb_invariant::InputDerivedCommandFacts>();
}
