#![forbid(unsafe_code)]

//! Dependency and durable-ownership guards for command idempotency.

const MANIFEST: &str = include_str!("../Cargo.toml");
const INSPECTION_SOURCE: &str = include_str!("../src/inspection.rs");
const PREPARATION_SOURCE: &str = include_str!("../src/prepare.rs");
const RECHECK_SOURCE: &str = include_str!("../src/recheck.rs");
const LIB_ROOT: &str = include_str!("../src/lib.rs");

#[test]
fn production_dependencies_are_exact_and_exclude_authentication() {
    let dependencies = MANIFEST
        .lines()
        .skip_while(|line| *line != "[dependencies]")
        .skip(1)
        .take_while(|line| !line.starts_with('['))
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    assert_eq!(
        dependencies,
        [
            "riffdb-storage-api = { version = \"0.1.0\", path = \"../riffdb-storage-api\", default-features = false }",
            "riffdb-types = { version = \"0.1.0\", path = \"../riffdb-types\", default-features = false }",
        ]
    );
    assert!(!MANIFEST.contains("riffdb-auth"));
}

#[test]
fn preparation_api_excludes_deployment_and_invocation_versions_by_shape() {
    assert!(!PREPARATION_SOURCE.contains("ContractVersion"));
    assert!(!PREPARATION_SOURCE.contains("RequestId"));
    assert!(!PREPARATION_SOURCE.contains("CommittedOutcome"));
}

#[test]
fn crate_root_keeps_safe_rust_mandatory() {
    assert!(LIB_ROOT.starts_with("#![forbid(unsafe_code)]"));
}

#[test]
fn inspection_is_read_only_and_keeps_full_observations_private() {
    assert!(INSPECTION_SOURCE.contains("repository.lookup_admission(candidates)?"));
    assert!(!INSPECTION_SOURCE.contains("repository.admit_or_resolve"));
    assert!(INSPECTION_SOURCE.contains("observation: AdmissionLookupResultV1"));
    assert!(!INSPECTION_SOURCE.contains("pub observation:"));
    assert!(!INSPECTION_SOURCE.contains("pub prepared_command:"));
    assert!(!INSPECTION_SOURCE.contains("impl Clone for InspectedIdempotencyV1"));
    assert!(!INSPECTION_SOURCE.contains("impl Clone for ConfirmedIdempotencyInspectionV1"));
    assert!(INSPECTION_SOURCE.contains("normalized_input: CanonicalRecord"));
}

#[test]
fn recheck_is_single_read_only_and_authority_values_are_move_only() {
    assert_eq!(RECHECK_SOURCE.matches(".lookup_admission(").count(), 1);
    assert!(!RECHECK_SOURCE.contains(".admit_or_resolve("));
    assert!(!RECHECK_SOURCE.contains("SystemTime"));
    assert!(!RECHECK_SOURCE.contains("Instant"));
    assert!(!RECHECK_SOURCE.contains("Random"));
    assert!(!RECHECK_SOURCE.contains("impl Clone for PreparedIdempotencyRecheckV1"));
    assert!(!RECHECK_SOURCE.contains("impl Clone for VacantIdempotencyAdmissionV1"));
    assert!(!RECHECK_SOURCE.contains("impl Clone for RecheckedPendingAdmissionV1"));
    assert!(RECHECK_SOURCE.contains("original_observation: AdmissionLookupResultV1"));
    assert!(RECHECK_SOURCE.contains("selected_plan: ExecutablePlanRef"));
    assert!(!RECHECK_SOURCE.contains("pub original_observation:"));
    assert!(!RECHECK_SOURCE.contains("pub selected_plan:"));
}

#[test]
fn recheck_preparation_matcher_is_exact_and_does_not_expose_retained_values() {
    let preparation_impl = RECHECK_SOURCE
        .split_once("impl PreparedIdempotencyRecheckV1 {")
        .expect("preparation implementation")
        .1
        .split_once("impl fmt::Debug for PreparedIdempotencyRecheckV1")
        .expect("bounded preparation implementation")
        .0;

    assert!(preparation_impl.contains("pub fn matches_preparation("));
    assert!(preparation_impl.contains("&self.selected_plan == selected_plan"));
    assert!(preparation_impl.contains("&self.normalized_input == normalized_input"));
    assert!(preparation_impl.contains("pub fn matches_scope("));
    assert!(preparation_impl.contains(".lookup_candidates()"));
    assert!(preparation_impl.contains(".as_slice()"));
    assert!(preparation_impl.contains(".all(|identity|"));
    assert!(!preparation_impl.contains("CommandIdempotencyScopeV1"));
    assert!(!preparation_impl.contains("caller_key_digest"));
    for exact_comparison in [
        "identity.database_id() == database_id",
        "identity.environment() == environment",
        "identity.tenant_scope() == tenant_scope",
        "identity.principal_id() == principal_id",
        "identity.contract_lineage() == contract_lineage",
        "identity.command_id() == command_id",
    ] {
        assert!(
            preparation_impl.contains(exact_comparison),
            "scope matcher is missing exact comparison {exact_comparison}"
        );
    }
    assert!(!preparation_impl.contains("pub fn selected_plan("));
    assert!(!preparation_impl.contains("pub const fn selected_plan("));
    assert!(!preparation_impl.contains("pub fn normalized_input("));
    assert!(!preparation_impl.contains("pub const fn normalized_input("));
    assert!(!preparation_impl.contains("pub fn original_observation("));
    assert!(!preparation_impl.contains("pub fn prepared_command("));
    assert!(!preparation_impl.contains("pub fn lookup_candidates("));
    assert!(!preparation_impl.contains("pub fn current_identity("));
}

#[test]
fn plan_comparison_is_source_adjacent_before_input_hash_comparison() {
    let plan_comparison = RECHECK_SOURCE
        .find("state_plan(&current) != &selected_plan")
        .expect("exact plan comparison");
    let hash_comparison = RECHECK_SOURCE
        .find("state_input_hash(&current) != prepared_command.canonical_input_hash()")
        .expect("exact input-hash comparison");
    assert!(plan_comparison < hash_comparison);
}
