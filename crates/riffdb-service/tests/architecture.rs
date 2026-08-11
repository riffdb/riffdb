#![forbid(unsafe_code)]

//! Compile-time and source-level guards for the API-neutral service boundary.

use riffdb_service::{
    AdministrationApplication, CommandApplication, CommitApplication, ContractApplication,
    CreateCapabilityInvocation, DiscoveryApplication, EventServiceApplication,
    OfflineMaintenanceApplication, QueryApplication, RecoveryOfflineMaintenanceApplication,
    RestoreRetryOfflineMaintenanceApplication,
};

const MANIFEST: &str = include_str!("../Cargo.toml");
const ADMINISTRATION_SOURCE: &str = include_str!("../src/administration_operations.rs");
const COMMAND_SOURCE: &str = include_str!("../src/command_operations.rs");
const COMMIT_SOURCE: &str = include_str!("../src/commit_operations.rs");
const CONSUMER_SOURCE: &str = include_str!("../src/consumer_operations.rs");
const CONTEXT_SOURCE: &str = include_str!("../src/context.rs");
const DTO_SOURCE: &str = include_str!("../src/dto.rs");
const EVENT_SOURCE: &str = include_str!("../src/event_operations.rs");
const MAINTENANCE_SOURCE: &str = include_str!("../src/maintenance_operations.rs");
const ORCHESTRATION_SOURCE: &str = include_str!("../src/orchestration.rs");
const PORTS_SOURCE: &str = include_str!("../src/ports.rs");
const QUERY_SOURCE: &str = include_str!("../src/query_discovery_operations.rs");
const SERVICE_SOURCE: &str = include_str!("../src/service.rs");
const SYMBOLIC_QUERY_SOURCE: &str = include_str!("../src/symbolic_query.rs");
const TRAITS_SOURCE: &str = include_str!("../src/application.rs");

fn assert_data_path_object_safe(
    _contract: &dyn ContractApplication,
    _command: &dyn CommandApplication,
    _query: &dyn QueryApplication,
    _commit: &dyn CommitApplication,
    _event: &dyn EventServiceApplication,
) {
}

fn assert_control_path_object_safe(
    _administration: &dyn AdministrationApplication,
    _maintenance: &dyn OfflineMaintenanceApplication,
    _discovery: &dyn DiscoveryApplication,
) {
}

fn assert_recovery_object_safe(_recovery: &dyn RecoveryOfflineMaintenanceApplication) {}

fn assert_restore_retry_object_safe(_retry: &dyn RestoreRetryOfflineMaintenanceApplication) {}

fn exhaust_create_invocation(invocation: CreateCapabilityInvocation) {
    match invocation {
        CreateCapabilityInvocation::Normal { .. }
        | CreateCapabilityInvocation::Bootstrap { .. } => {}
    }
}

#[test]
fn eight_service_traits_are_object_safe_and_create_mode_is_closed() {
    let _ = assert_data_path_object_safe;
    let _ = assert_control_path_object_safe;
    let _ = assert_recovery_object_safe;
    let _ = assert_restore_retry_object_safe;
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
fn hosted_mcp_context_construction_fixes_ingress_and_hides_policy_claim_vocabulary() {
    let remainder = CONTEXT_SOURCE
        .split_once("pub const fn from_authenticated_mcp_http(")
        .map(|(_, remainder)| remainder)
        .expect("bounded authenticated hosted MCP constructor");
    let parameters = remainder
        .split_once(") -> Self {")
        .map(|(parameters, _)| parameters)
        .expect("hosted MCP constructor has a concrete return type");
    let compact_parameters: String = parameters
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    assert_eq!(
        compact_parameters,
        "request_id:RequestId,principal:AuthenticatedPrincipal,control:RequestControl,trace:Option<TraceContext>,"
    );

    let constructor = remainder
        .split_once("\n    }")
        .map(|(body, _)| body)
        .expect("bounded authenticated hosted MCP constructor body");
    assert!(constructor.contains("request_id: RequestId"));
    assert!(constructor.contains("principal: AuthenticatedPrincipal"));
    assert!(constructor.contains("control: RequestControl"));
    assert!(constructor.contains("trace: Option<TraceContext>"));
    assert!(constructor.contains("ServiceIngressKindV1::McpHttp"));
    assert!(constructor.contains("UntrustedInvocationClaims::new(None, None, None, None, None)"));
    assert!(!constructor.contains("claims:"));
    assert!(!constructor.contains("ingress:"));
    assert!(!constructor.contains("credential"));
    assert!(!constructor.contains("session"));
    assert!(!constructor.contains("&[u8]"));
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

/// Returns one top-level function body, from its signature to its column-zero brace.
fn function_body<'source>(source: &'source str, signature: &str) -> &'source str {
    source
        .split_once(signature)
        .unwrap_or_else(|| panic!("{signature} is defined in this source"))
        .1
        .split_once("\n}\n")
        .unwrap_or_else(|| panic!("{signature} has a closed body"))
        .0
}

/// A sub-floor admission budget stays a typed overload at both admission stages.
///
/// Rejecting because the remaining client budget cannot host the bounded
/// pre-admission wait is a capacity shed of an unadmitted request, so it must
/// surface `Overloaded` (`RDB-CAPACITY-0101`) and record a capacity-rejection
/// stage — never details-free `DeadlineExceeded`, which would tell the caller
/// its own deadline elapsed. `AdmissionBudget::failure` owns that mapping and
/// its semantics are asserted by the crate's `admission_budget_*` unit tests;
/// this pin covers what those cannot reach, because both arms live inside
/// `async fn`s that need a fully composed service to execute and an end-to-end
/// probe would have to race a near-expired client deadline against catalog
/// preparation (the original flake).
#[test]
fn a_sub_floor_admission_budget_is_shed_as_overload_at_both_admission_stages() {
    assert_eq!(
        COMMAND_SOURCE
            .matches("fn failure(self) -> ServiceFailure {")
            .count(),
        1,
        "one rule owns the admission-budget rejection mapping"
    );

    for (function, stage) in [
        (
            "async fn admit_command_capacity(",
            "CapacityRejectionStage::QueueDepth",
        ),
        (
            "async fn attach_retained_bytes(",
            "CapacityRejectionStage::RetainedBytes",
        ),
    ] {
        let body = function_body(COMMAND_SOURCE, function);
        assert_eq!(
            body.matches("Err(budget) => {").count(),
            1,
            "{function} rejects a sub-floor budget in exactly one arm"
        );
        assert_eq!(
            body.matches("return Err(budget.failure());").count(),
            1,
            "{function} must reject a sub-floor budget through the shared mapping"
        );
        assert!(
            body.contains(stage),
            "{function} must record {stage} before shedding"
        );
        assert!(
            !body.contains("DeadlineExceeded"),
            "{function} must never surface a details-free deadline while unadmitted"
        );
    }
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
fn index_scan_keeps_durable_types_out_and_performs_one_lower_scan() {
    assert!(DTO_SOURCE.contains("pub struct AuthoritativeSchemaBinding"));
    assert!(!DTO_SOURCE.contains("DurableKeySchemaBindingV1"));

    let operation = QUERY_SOURCE
        .split_once("async fn scan_index(")
        .expect("index operation exists")
        .1
        .split_once("async fn query_projection(")
        .expect("index operation has a closed source boundary")
        .0;
    assert_eq!(operation.matches(".reserve_scan_index(").count(), 1);
    assert_eq!(operation.matches("permit.submit(lower_request)").count(), 1);
    let lower = operation
        .find("permit.submit(lower_request)")
        .expect("one lower request is submitted");
    let historical = operation
        .find("validate_authoritative_index_rows(")
        .expect("lower rows are checked under exact historical bindings");
    let return_policy = operation
        .find("let return_authorization =")
        .expect("return-time authorization is present");
    let shaping = operation
        .find("let mut rows = match index_views(")
        .expect("authorized rows are shaped after return policy");
    assert!(lower < historical && historical < return_policy && return_policy < shaping);
}

#[test]
fn operation_specific_traits_expose_the_closed_method_inventory() {
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
        "fn describe_event(",
        "fn replay_events(",
        "fn tail_events(",
        "fn health(",
        "fn statistics(",
        "fn create_capability(",
        "fn revoke_capability(",
        "fn list_pending_outbox_deliveries(",
        "fn create_offline_backup(",
        "fn get_offline_maintenance_operation(",
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
    assert_eq!(
        TRAITS_SOURCE.matches("fn restore_offline_backup(").count(),
        3,
        "normal, current retry, and staged recovery restore methods must exist"
    );

    assert!(!TRAITS_SOURCE.contains("fn execute("));
    assert!(!TRAITS_SOURCE.contains("fn generic_read("));
    assert!(!TRAITS_SOURCE.contains("fn generic_mutation("));
}

#[test]
fn event_tail_registers_its_wakeup_source_before_authoritative_catch_up() {
    let operation = EVENT_SOURCE
        .split_once("async fn execute_event_read(")
        .expect("event read orchestration exists")
        .1
        .split_once("async fn establish_tail_source(")
        .expect("event read orchestration has a closed source boundary")
        .0;
    let subscribe = operation
        .find("establish_tail_source(")
        .expect("tail establishes a bounded commit notification source");
    let replay = operation
        .find("read_event_page(")
        .expect("tail performs authoritative route catch-up");
    assert!(
        subscribe < replay,
        "subscription must close the catch-up race"
    );
    assert!(operation.contains("EventReplayPosition::Continue(continuation)"));
    assert!(operation.contains("EventReplayPosition::Initial {"));
    assert!(operation.contains("after: observed_upper"));
}

#[test]
fn contextual_hydration_cannot_fall_back_to_an_unprotected_query_group() {
    let hydration = CONSUMER_SOURCE
        .split_once("async fn hydrate_contextual_delivery(")
        .expect("contextual hydration orchestration exists")
        .1
        .split_once("fn validate_contextual_snapshot_head(")
        .expect("contextual hydration has a closed boundary")
        .0;
    assert!(hydration.contains("resolve_authorized_contextual_row_policy_context("));
    assert!(hydration.contains("Some(policy) => executor.execute_policy_query_group("));
    assert!(hydration.contains("None => executor.execute_query_group("));

    let release = CONSUMER_SOURCE
        .split_once("async fn finalize_consumer_delivery(")
        .expect("consumer release orchestration exists")
        .1
        .split_once("async fn hydrate_contextual_delivery(")
        .expect("consumer release has a closed boundary")
        .0;
    assert_eq!(
        release
            .matches("begun.reauthorize(service, context)")
            .count(),
        2,
        "contextual delivery must reauthorize immediately before hydration and release"
    );
}

#[test]
fn contextual_reaction_reuses_the_authoritative_command_policy_path() {
    let reaction = CONSUMER_SOURCE
        .split_once("async fn execute_contextual_reaction_operation(")
        .expect("contextual reaction orchestration exists")
        .1
        .split_once("pub(crate) fn bind_reaction_idempotency(")
        .expect("contextual reaction has a closed boundary")
        .0;
    assert!(reaction.contains("EventConsumerPortRequest::ValidateLease"));
    assert!(reaction.contains("CommandInvocationMode::Contextual"));
    assert!(reaction.contains("crate::command_operations::execute_command("));
    assert!(reaction.contains("finish_success(&service, &context, &begun)"));
    assert!(!reaction.contains("execute_evaluated_command"));
}

#[test]
fn maintenance_is_api_neutral_and_disjoint_from_durable_service_audit() {
    assert!(TRAITS_SOURCE.contains("pub trait OfflineMaintenanceApplication"));
    assert!(TRAITS_SOURCE.contains("pub trait RecoveryOfflineMaintenanceApplication"));
    assert!(TRAITS_SOURCE.contains("pub trait RestoreRetryOfflineMaintenanceApplication"));
    let application_service = TRAITS_SOURCE
        .split_once("pub trait ApplicationService:")
        .expect("application service marker")
        .1
        .split_once("impl<T> ApplicationService")
        .expect("application service marker has a closed boundary")
        .0;
    assert!(!application_service.contains("RecoveryOfflineMaintenanceApplication"));
    assert!(!application_service.contains("RestoreRetryOfflineMaintenanceApplication"));
    assert!(MAINTENANCE_SOURCE.contains(".authorize_offline_maintenance("));
    assert!(MAINTENANCE_SOURCE.contains(".reserve_start("));
    assert!(MAINTENANCE_SOURCE.contains(".reserve_observation("));
    assert!(MAINTENANCE_SOURCE.contains("ensure_response_budget(&result)"));
    for forbidden in [
        "ServiceOperationV1",
        "begin_invocation",
        "append_audit",
        "StoredServiceAuditRecordV1",
        "riffdb_storage",
        "redb",
        "tonic",
        "prost",
    ] {
        assert!(
            !MAINTENANCE_SOURCE.contains(forbidden),
            "maintenance acquired forbidden boundary {forbidden}"
        );
    }
}

#[test]
fn recovery_maintenance_is_restore_only_and_capability_separated() {
    let trait_source = TRAITS_SOURCE
        .split_once("pub trait RecoveryOfflineMaintenanceApplication")
        .expect("recovery maintenance trait")
        .1
        .split_once("/// Exact current-database restore retry")
        .expect("recovery trait has a closed boundary")
        .0;
    assert_eq!(
        trait_source.matches("fn restore_offline_backup(").count(),
        1
    );
    for forbidden in [
        "create_offline_backup",
        "get_offline_maintenance_operation",
        "RequestContext",
        "AuthenticatedPrincipal",
    ] {
        assert!(!trait_source.contains(forbidden), "{forbidden}");
    }

    let port = PORTS_SOURCE
        .split_once("pub trait RecoveryOfflineMaintenanceCoordinatorPort")
        .expect("recovery coordinator port")
        .1
        .split_once("/// Closed operational-state")
        .expect("recovery coordinator port has a closed boundary")
        .0;
    assert_eq!(port.matches("fn reserve_restore(").count(), 1);
    assert!(!port.contains("reserve_start"));
    assert!(!port.contains("reserve_observation"));

    let service = MAINTENANCE_SOURCE
        .split_once("struct RecoveryOfflineMaintenanceServiceInner")
        .expect("recovery service type")
        .1
        .split_once("async fn start_create_backup(")
        .expect("recovery service implementation has a closed boundary")
        .0;
    assert!(service.contains("RecoveryOfflineMaintenanceCoordinatorPort"));
    assert!(service.contains("RecoveryRestoreOfflineBackupInvocation"));
    assert!(!service.contains("authorize_current("));
    assert!(!service.contains("OfflineMaintenanceDecision"));
    assert!(!service.contains("RequestContext"));
}

#[test]
fn current_restore_retry_is_exact_and_has_no_broader_maintenance_surface() {
    let trait_source = TRAITS_SOURCE
        .split_once("pub trait RestoreRetryOfflineMaintenanceApplication")
        .expect("restore-retry trait")
        .1
        .split_once("/// Current-policy-filtered")
        .expect("restore-retry trait has a closed boundary")
        .0;
    assert_eq!(
        trait_source.matches("fn restore_offline_backup(").count(),
        1
    );
    for forbidden in [
        "create_offline_backup",
        "get_offline_maintenance_operation",
        "RecoveryRestoreOfflineBackupInvocation",
    ] {
        assert!(!trait_source.contains(forbidden), "{forbidden}");
    }

    let port = PORTS_SOURCE
        .split_once("pub trait RestoreRetryOfflineMaintenanceCoordinatorPort")
        .expect("restore-retry coordinator port")
        .1
        .split_once("/// Recovery-only restore command")
        .expect("retry coordinator port has a closed boundary")
        .0;
    assert_eq!(port.matches("fn reserve_restore(").count(), 1);
    assert!(!port.contains("reserve_start"));
    assert!(!port.contains("reserve_observation"));

    let service = MAINTENANCE_SOURCE
        .split_once("struct RestoreRetryOfflineMaintenanceServiceInner")
        .expect("restore-retry service")
        .1
        .split_once("async fn start_create_backup(")
        .expect("retry service implementation has a closed boundary")
        .0;
    assert!(service.contains("RestoreRetryOfflineMaintenanceCoordinatorPort"));
    assert!(service.contains("OfflineMaintenanceOperationId"));
    assert!(service.contains("OfflineMaintenanceInputHash"));
    assert!(!service.contains("RiffDbServiceInner"));
    assert!(!service.contains("ServiceExecutors"));
}

#[test]
fn dependent_query_batches_cannot_escape_identity_scope_or_final_reauthorization() {
    let execution = SYMBOLIC_QUERY_SOURCE
        .split_once("async fn execute_compiled_query(")
        .expect("compiled query execution")
        .1
        .split_once("async fn begin_symbolic(")
        .expect("compiled query execution has a closed boundary")
        .0;

    for cursor_identity in [
        "program.contract().lineage().clone()",
        "program.contract().version()",
        "program.contract().bundle_hash()",
        "module_hash",
        "program.identity().hash()",
        "parameter_hash",
        "context.principal().capability_id()",
        "context.principal().capability_revision()",
    ] {
        assert!(
            execution.contains(cursor_identity),
            "query cursor omitted {cursor_identity}"
        );
    }
    assert!(execution.contains("execute_authorized_query_page("));
    assert!(execution.contains(".into_application_query()"));
    // The read pipeline reauthorizes through its own revision-checked entry
    // point. Both safe points remain; only their cost differs when the
    // capability view, the validity window, and the request are unchanged.
    assert!(
        execution
            .matches(".reauthorize_read(&service, &context)")
            .count()
            >= 2,
        "execution must reauthorize before the dependent batch and before release"
    );
    assert!(
        !execution.contains(".reauthorize(&service, &context)"),
        "the read pipeline must not reach the shared unconditional entry point"
    );
    let final_authorization = execution
        .rfind("begun.reauthorize_read(&service, &context)")
        .expect("final authorization");
    let result_construction = execution
        .find("ExecuteSymbolicQueryResult::from_snapshot")
        .expect("result construction");
    let cursor_publication = execution
        .find("CursorPublicationGuard::publish")
        .expect("cursor publication");
    assert!(final_authorization < result_construction);
    assert!(result_construction < cursor_publication);

    let proof_consumer = SYMBOLIC_QUERY_SOURCE
        .split_once("fn execute_authorized_query_page(")
        .expect("authorized query proof consumer")
        .1
        .split_once("fn execution_failure(")
        .expect("proof consumer has a closed boundary")
        .0;
    for exact_requirement in [
        "target.lineage() == program.contract().lineage()",
        "target.version() == program.contract().version()",
        "target.bundle_hash() == program.contract().bundle_hash()",
        "target.plan_hash() == program.identity().hash()",
        "target.cost() == program.cost()",
        "target.accesses().len() == program.steps().len()",
        "OutputClassification::PolicyFilteredApplicationData",
        "PartitionConstraint::Exact(target.partition().clone())",
    ] {
        assert!(
            proof_consumer.contains(exact_requirement),
            "query proof consumer omitted {exact_requirement}"
        );
    }
}

/// The revision-checked reauthorization shortcut stays scoped to the read pipeline.
///
/// Every other caller — commits, commands, contracts, discovery, and
/// administration, including the mandatory recheck after a capacity wait —
/// reaches `full_reauthorize` unconditionally. This guards against the
/// shortcut being hoisted back into the shared entry point.
#[test]
fn the_revision_checked_reauthorization_shortcut_is_reachable_only_from_the_read_entry_point() {
    let shortcut_uses = ORCHESTRATION_SOURCE
        .matches("reissue_for_unchanged_view(")
        .count();
    assert_eq!(
        shortcut_uses, 1,
        "the reissue rule must have exactly one call site in service orchestration"
    );

    // The pin covers the whole crate, not just orchestration: a reissue call
    // appearing in any other service source would bypass the read-entry scoping.
    let mut crate_wide_uses = 0;
    for entry in
        std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/src")).expect("service src dir")
    {
        let path = entry.expect("src entry").path();
        if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let source = std::fs::read_to_string(&path).expect("service source");
            crate_wide_uses += source.matches("reissue_for_unchanged_view(").count();
        }
    }
    assert_eq!(
        crate_wide_uses, 1,
        "the reissue rule must have exactly one call site across the whole service crate"
    );

    let invocation = ORCHESTRATION_SOURCE
        .split_once("impl BegunInvocation {")
        .expect("the begun-invocation impl exists")
        .1;
    let read_entry = invocation
        .split_once("pub(crate) async fn reauthorize_read(")
        .expect("the read reauthorization entry point exists")
        .1
        .split_once("\n    /// Reauthorizes newly loaded exact facts")
        .expect("the read entry point has a closed boundary")
        .0;
    assert!(
        read_entry.contains("reissue_for_unchanged_view("),
        "the read entry point owns the only reissue call site"
    );
    assert!(
        read_entry.contains("self.full_reauthorize(service, context, request)"),
        "a declined reissue must fall through to the full evaluation"
    );

    let shared_entry = invocation
        .split_once("pub(crate) async fn reauthorize_request(")
        .expect("the shared reauthorization entry point exists")
        .1
        .split_once("\n    /// Applies the proof-shape")
        .expect("the shared entry point has a closed boundary")
        .0;
    assert!(
        !shared_entry.contains("reissue_for_unchanged_view("),
        "the shared entry point must never take the revision-checked shortcut"
    );
    assert!(
        shared_entry.contains("self.full_reauthorize(service, context, request)"),
        "the shared entry point must always fully re-evaluate"
    );
}
