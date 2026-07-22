#![forbid(unsafe_code)]

//! Compile-time and source-level guards for the API-neutral service boundary.

use riffdb_service::{
    AdministrationApplication, CommandApplication, CommitApplication, ContractApplication,
    CreateCapabilityInvocation, DiscoveryApplication, QueryApplication,
};

const MANIFEST: &str = include_str!("../Cargo.toml");
const ADMINISTRATION_SOURCE: &str = include_str!("../src/administration_operations.rs");
const COMMIT_SOURCE: &str = include_str!("../src/commit_operations.rs");
const CONTEXT_SOURCE: &str = include_str!("../src/context.rs");
const QUERY_SOURCE: &str = include_str!("../src/query_discovery_operations.rs");
const SERVICE_SOURCE: &str = include_str!("../src/service.rs");
const TRAITS_SOURCE: &str = include_str!("../src/application.rs");

fn assert_object_safe(
    _contract: &dyn ContractApplication,
    _command: &dyn CommandApplication,
    _query: &dyn QueryApplication,
    _commit: &dyn CommitApplication,
    _administration: &dyn AdministrationApplication,
    _discovery: &dyn DiscoveryApplication,
) {
}

fn exhaust_create_invocation(invocation: CreateCapabilityInvocation) {
    match invocation {
        CreateCapabilityInvocation::Normal { .. }
        | CreateCapabilityInvocation::Bootstrap { .. } => {}
    }
}

#[test]
fn six_service_traits_are_object_safe_and_create_mode_is_closed() {
    let _ = assert_object_safe;
    let _ = exhaust_create_invocation;
}

#[test]
fn initializing_service_exposes_only_health_before_consuming_activation() {
    let initialization = SERVICE_SOURCE
        .split_once("impl InitializingRiffDbService")
        .expect("initializing service implementation exists")
        .1
        .split_once("impl fmt::Debug for InitializingRiffDbService")
        .expect("initializing service implementation has a closed boundary")
        .0;

    assert_eq!(initialization.matches("pub fn health(").count(), 1);
    for forbidden in [
        "execute_command",
        "deploy_contract",
        "get_entity",
        "scan_index",
        "create_capability",
        "storage",
        "catalog",
        "policy",
    ] {
        assert!(
            !initialization.contains(forbidden),
            "initializing service acquired forbidden surface {forbidden}"
        );
    }
    assert!(SERVICE_SOURCE.contains("pub fn activate(\n        self,"));
}

#[test]
fn service_dependency_graph_has_no_storage_runtime_or_transport_edge() {
    let production_dependencies = MANIFEST
        .split_once("[dependencies]")
        .expect("service manifest declares production dependencies")
        .1
        .split_once("\n[")
        .map_or_else(|| MANIFEST, |(dependencies, _)| dependencies);
    for forbidden in [
        "riffdb-storage-api",
        "riffdb-storage-memory",
        "riffdb-storage-redb",
        "riffdb-runtime",
        "riffdb-conflict",
        "riffdb-idempotency",
        "riffdb-proto",
        "tonic",
        "prost",
        "rmcp",
        "redb",
    ] {
        assert!(
            !production_dependencies.contains(forbidden),
            "riffdb-service has a forbidden production dependency on {forbidden}"
        );
    }
}

#[test]
fn grpc_context_construction_fixes_ingress_and_hides_policy_claim_vocabulary() {
    let constructor = CONTEXT_SOURCE
        .split_once("pub const fn from_authenticated_grpc(")
        .and_then(|(_, remainder)| remainder.split_once("\n    }"))
        .map(|(body, _)| body)
        .expect("bounded authenticated gRPC constructor");
    assert!(constructor.contains("ServiceIngressKindV1::Grpc"));
    assert!(constructor.contains("UntrustedInvocationClaims::new(None, None, None, None, None)"));
    assert!(!constructor.contains("claims:"));
    assert!(!constructor.contains("ingress:"));
}

#[test]
fn catalog_owns_capability_partition_decoding_and_service_validates_output_keys() {
    assert!(ADMINISTRATION_SOURCE.contains("validate_capability_partition_scope("));
    assert!(!ADMINISTRATION_SOURCE.contains(".decode_partition("));
    assert!(!ADMINISTRATION_SOURCE.contains("struct CapabilityPartitionValidationError"));

    let normal_create = ADMINISTRATION_SOURCE
        .split_once("async fn create_capability_normal(")
        .expect("normal create implementation")
        .1
        .split_once("async fn create_capability_bootstrap(")
        .expect("bootstrap follows normal create")
        .0;
    let validation = normal_create
        .find("validate_capability_partition_scope(")
        .expect("normal create calls the catalog validator");
    let policy = normal_create
        .find(".begin_capability_mutation(")
        .expect("normal create begins policy and audit orchestration");
    let token = normal_create
        .find(".token_issuer.issue()")
        .expect("normal create issues a capability token");
    let control_plane = normal_create
        .find(".control_plane.reserve_capacity()")
        .expect("normal create eventually reserves control-plane capacity");
    let submission = normal_create
        .find(".submit_capability_create(preparation)")
        .expect("normal create eventually submits the checked preparation");
    assert!(
        validation < policy
            && policy < token
            && token < control_plane
            && control_plane < submission
    );
    for boundary in [
        "validate_capability_partition_scope(",
        ".begin_capability_mutation(",
        ".token_issuer.issue()",
        ".control_plane.reserve_capacity()",
        ".submit_capability_create(preparation)",
    ] {
        assert_eq!(
            normal_create.matches(boundary).count(),
            1,
            "normal create boundary drifted for {boundary}"
        );
    }

    assert!(COMMIT_SOURCE.contains("prepare_contract_version("));
    assert!(COMMIT_SOURCE.contains(".decode_entity(affected.key())"));
}

#[test]
fn query_components_materialize_after_schema_selection_and_before_policy_or_lower_access() {
    for (start, end) in [
        ("async fn scan_index(", "async fn query_projection("),
        (
            "async fn query_projection(",
            "async fn get_projection_status(",
        ),
    ] {
        let operation = QUERY_SOURCE
            .split_once(start)
            .expect("query operation exists")
            .1
            .split_once(end)
            .expect("query operation has a closed source boundary")
            .0;
        let schema = operation
            .find("prepare_selected_contract(")
            .expect("selected contract is loaded");
        let materialization = operation
            .find("materialize_query_components(")
            .expect("submitted components are materialized");
        let policy = operation
            .find(".begin_invocation(")
            .expect("policy and audit invocation begins");
        let lower = operation
            .find(".reserve_")
            .expect("lower-port capacity is eventually reserved");
        assert!(schema < materialization && materialization < policy && policy < lower);
        assert_eq!(operation.matches("request.leading_components()").count(), 1);
    }
}

#[test]
fn operation_specific_traits_expose_the_closed_twenty_two_method_inventory() {
    let methods = [
        "fn validate_contract(",
        "fn explain_command(",
        "fn deploy_contract(",
        "fn get_active_contract(",
        "fn get_contract_version(",
        "fn execute_command(",
        "fn resolve_command_outcome(",
        "fn get_entity(",
        "fn scan_index(",
        "fn query_projection(",
        "fn get_projection_status(",
        "fn get_commit(",
        "fn scan_commits(",
        "fn subscribe_to_commits(",
        "fn trace_provenance(",
        "fn health(",
        "fn statistics(",
        "fn create_capability(",
        "fn revoke_capability(",
        "fn list_pending_outbox_deliveries(",
        "fn discover_command_tools(",
        "fn discover_resources(",
    ];

    for method in methods {
        assert_eq!(
            TRAITS_SOURCE.matches(method).count(),
            1,
            "service method inventory drifted for {method}"
        );
    }

    assert!(!TRAITS_SOURCE.contains("fn execute("));
    assert!(!TRAITS_SOURCE.contains("fn generic_read("));
    assert!(!TRAITS_SOURCE.contains("fn generic_mutation("));
}
