#![forbid(unsafe_code)]

//! Architecture assertions for the gRPC adapter feature and authority graph.

#[test]
fn grpc_adapter_has_no_lower_semantic_authority_dependency() {
    let manifest = include_str!("../Cargo.toml");
    for banned in [
        "riffdb-catalog",
        "riffdb-commit",
        "riffdb-conflict",
        "riffdb-policy",
        "riffdb-runtime",
        "riffdb-storage-api",
        "riffdb-storage-redb",
    ] {
        assert!(
            !manifest
                .lines()
                .any(|line| line.trim_start().starts_with(banned)),
            "gRPC adapter must not depend on {banned}"
        );
    }
}

#[test]
fn tonic_runtime_features_remain_narrow_and_default_disabled() {
    let manifest = include_str!("../Cargo.toml");
    let runtime_dependencies = manifest
        .split("[build-dependencies]")
        .next()
        .expect("runtime dependency section");
    assert!(manifest.contains("default = []"));
    assert!(manifest.contains("client = [\"riffdb-proto/client\"]"));
    assert!(manifest.contains("\"tonic/router\","));
    assert!(manifest.contains("\"tonic/server\","));
    assert!(runtime_dependencies.contains("features = [\"codegen\"]"));
    assert!(!runtime_dependencies.contains("features = [\"transport\"]"));
    for forbidden in ["tls-", "gzip", "deflate", "zstd"] {
        assert!(!manifest.contains(forbidden));
    }
}

#[test]
fn client_feature_does_not_activate_server_semantic_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    let client = manifest
        .lines()
        .find(|line| line.starts_with("client ="))
        .expect("client feature declaration");
    assert_eq!(client, "client = [\"riffdb-proto/client\"]");

    for dependency in [
        "riffdb-auth",
        "riffdb-errors",
        "riffdb-service",
        "riffdb-types",
    ] {
        let line = manifest
            .lines()
            .find(|line| line.starts_with(dependency))
            .expect("server dependency declaration");
        assert!(
            line.contains("optional = true"),
            "{dependency} must be optional"
        );
        assert!(
            manifest.contains(&format!("\"dep:{dependency}\"")),
            "server feature must explicitly own {dependency}"
        );
    }

    let source = include_str!("../src/lib.rs");
    for module in ["authentication", "conversion", "error", "server"] {
        assert!(
            source.contains(&format!("#[cfg(feature = \"server\")]\nmod {module};")),
            "{module} must remain server-only"
        );
    }
}

#[test]
fn generated_servers_are_wrapped_with_exact_public_message_limits() {
    let source = include_str!("../src/server.rs");
    assert_eq!(
        source
            .matches(".max_decoding_message_size(MAX_PUBLIC_REQUEST_BYTES)")
            .count(),
        4
    );
    assert_eq!(
        source
            .matches(".max_decoding_message_size(MAX_EXECUTE_REQUEST_BYTES)")
            .count(),
        3
    );
    assert_eq!(
        source
            .matches(".max_decoding_message_size(MAX_CONTRACT_MIGRATION_REQUEST_BYTES)")
            .count(),
        1
    );
    assert_eq!(
        source
            .matches(".max_encoding_message_size(MAX_PUBLIC_RESPONSE_BYTES)")
            .count(),
        8
    );
    assert!(!source.contains("accept_compressed"));
    assert!(!source.contains("send_compressed"));
}

#[test]
fn application_session_is_confined_to_existing_application_operations() {
    let protocol = include_str!("../../../proto/riffdb/v1/session.proto");
    assert!(protocol.contains("rpc Open(stream ApplicationSessionRequest)"));
    assert!(protocol.contains("ExecuteCommandRequest command"));
    assert!(protocol.contains("riffdb.app.v1.ExecuteQueryRequest query"));
    for forbidden in [
        "ScanIndex",
        "GetEntity",
        "CreateCapability",
        "DeployContract",
        "Mcp",
        "Transaction",
    ] {
        assert!(
            !protocol.contains(forbidden),
            "session protocol must not expose {forbidden}"
        );
    }

    let source = include_str!("../src/server.rs");
    let dispatch = source
        .split("fn application_session_operation(")
        .nth(1)
        .and_then(|tail| tail.split("async fn run_application_session(").next())
        .expect("bounded session operation adapter");
    assert!(dispatch.contains("CommandService::execute(&application, request)"));
    assert!(dispatch.contains("ApplicationQueryService::execute_query(&application, request)"));
    for forbidden in ["storage", "commit_coordinator", "execute_command(context"] {
        assert!(
            !dispatch.contains(forbidden),
            "session adapter must not acquire lower authority through {forbidden}"
        );
    }
    assert!(source.contains("struct ApplicationSessionMetadata(MetadataMap);"));
    assert!(source.contains("ApplicationSessionMetadata([REDACTED])"));
    assert!(source.contains("metadata.copy_for_operation()"));
    assert!(source.contains("if operations.len() >= scope.maximum_in_flight"));
}

#[test]
fn generated_services_use_strict_preallocation_decode() {
    let build = include_str!("../build.rs");
    assert!(build.contains(".codec_path(\"crate::codec::StrictProstCodec\")"));

    let codec = include_str!("../src/codec.rs");
    let decoder = codec
        .split("impl<MessageType> Decoder for StrictProstDecoder<MessageType>")
        .nth(1)
        .expect("strict decoder implementation");
    let preflight = decoder
        .find("decode_public_message(bytes.as_ref())")
        .expect("allocation-free public preflight");
    let prost_decode = decoder.find("Message::decode");
    assert!(
        prost_decode.is_none_or(|decode| preflight < decode),
        "generated server decode must preflight raw bytes before Prost allocation"
    );
}

#[test]
fn staged_route_does_not_require_a_full_service_at_construction() {
    let source = include_str!("../src/server.rs");
    let application = source
        .split("pub struct GrpcApplication")
        .nth(1)
        .and_then(|tail| tail.split("impl GrpcApplication").next())
        .expect("GrpcApplication declaration");
    assert!(!application.contains("Arc<dyn ApplicationService>"));
    assert!(application.contains("Arc<GrpcDatabaseRoutes>"));
    for deferred in [
        "CredentialAuthenticator",
        "AuthenticationContext",
        "CapabilityDigestKeyProvider",
        "CheckedGrpcSecurityContext",
    ] {
        assert!(
            !application.contains(deferred),
            "initializing adapter must not own deferred {deferred}"
        );
    }

    let constructor = source
        .split("impl GrpcApplication")
        .nth(1)
        .and_then(|tail| tail.split("pub fn new(").nth(1))
        .and_then(|tail| tail.split(") -> Self").next())
        .expect("GrpcApplication constructor");
    assert!(constructor.contains("Arc<dyn GrpcLifecycleRoute>"));
    assert!(constructor.contains("GrpcRequestLimits"));
    assert!(!constructor.contains("CheckedGrpcSecurityContext"));
}

#[test]
fn lifecycle_admission_precedes_security_fetch_and_bootstrap_transition() {
    let source = include_str!("../src/server.rs");
    let normal = source
        .split("fn normal_admission(")
        .nth(1)
        .and_then(|tail| tail.split("fn bootstrap_context(").next())
        .expect("normal admission helper");
    assert!(
        normal.find("admit_authenticated").expect("lifecycle gate")
            < normal.find("security_context").expect("security fetch")
    );

    let bootstrap_security = source
        .split("fn bootstrap_security(")
        .nth(1)
        .and_then(|tail| tail.split("struct BootstrapLifecycleGuard").next())
        .expect("bootstrap security helper");
    assert!(
        bootstrap_security
            .find("bootstrap_available")
            .expect("non-mutating bootstrap preflight")
            < bootstrap_security
                .find("security_context")
                .expect("security fetch")
    );

    let handler = source
        .split("async fn create_capability(")
        .nth(1)
        .and_then(|tail| tail.split("async fn revoke_capability(").next())
        .expect("create-capability handler");
    let security = handler
        .find("bootstrap_security")
        .expect("preflighted security fetch");
    let preparation = handler
        .find("bootstrap_context")
        .expect("bootstrap token preparation");
    let begin = handler
        .find("begin_bootstrap")
        .expect("atomic bootstrap admission");
    assert!(security < preparation);
    assert!(preparation < begin);
}

#[test]
fn bounded_batch_ingress_authenticates_once_without_bypassing_the_application_service() {
    let source = include_str!("../src/server.rs");
    let handler = source
        .split("async fn execute_batch(")
        .nth(1)
        .and_then(|tail| tail.split("async fn get_outcome(").next())
        .expect("execute-batch handler");

    assert_eq!(handler.matches("normal_batch_invocation_with(").count(), 1);
    assert!(!handler.contains("normal_invocation_with("));
    assert!(!handler.contains("tokio::spawn"));
    assert!(!handler.contains("BatchTaskAbortGuard"));
    assert_eq!(handler.matches("service.execute_command(").count(), 1);
    assert_eq!(handler.matches("join_all(invocations).await").count(), 1);

    assert!(
        source
            .split("fn normal_batch_invocation_with(")
            .nth(1)
            .and_then(|tail| tail.split("fn normal_invocation(").next())
            .expect("bounded batch invocation helper")
            .contains("authenticate_normal_request(")
    );
}

#[test]
fn capability_cancellation_guard_outlives_invocation_construction() {
    let source = include_str!("../src/server.rs");
    let handler = source
        .split("async fn create_capability(")
        .nth(1)
        .and_then(|tail| tail.split("async fn revoke_capability(").next())
        .expect("create-capability handler");
    assert_eq!(handler.matches("_cancellation").count(), 2);
    assert_eq!(
        handler
            .matches("service.create_capability(invocation).await")
            .count(),
        2
    );
    assert!(handler.contains("BootstrapLifecycleGuard::new"));
    assert!(handler.contains("lifecycle_guard.complete(completion)"));
}

#[test]
fn operator_campaigns_maintenance_and_migration_are_additive_and_never_an_mcp_surface() {
    let services = include_str!("../../../proto/riffdb/v1/services.proto");
    assert_eq!(
        services
            .lines()
            .filter(|line| line.starts_with("service "))
            .count(),
        6
    );
    assert_eq!(
        services
            .lines()
            .filter(|line| line.trim_start().starts_with("rpc "))
            .count(),
        57
    );
    assert_eq!(services.matches("rpc ExecuteBatch(").count(), 1);
    for rpc in [
        "rpc CreateOfflineBackup(",
        "rpc RestoreOfflineBackup(",
        "rpc RetireOfflineBackup(",
        "rpc GetOfflineMaintenanceOperation(",
        "rpc CheckContractMigration(",
        "rpc ApplyContractMigration(",
        "rpc GetContractMigrationOperation(",
        "rpc StartApplicationInstallation(",
        "rpc GetApplicationInstallation(",
        "rpc StartApplicationExport(",
        "rpc GetApplicationExportPage(",
        "rpc GetApplicationExport(",
        "rpc CancelApplicationExport(",
        "rpc StartApplicationReimport(",
        "rpc ApplyApplicationReimportPage(",
        "rpc GetApplicationReimport(",
        "rpc CancelApplicationReimport(",
    ] {
        assert_eq!(services.matches(rpc).count(), 1, "missing exact {rpc}");
    }

    let mcp_registry = include_str!("../../riffdb-api-mcp/fixtures/fixed-tool-registry-v1.json");
    let normalized = mcp_registry.to_ascii_lowercase();
    assert!(!normalized.contains("backup"));
    assert!(!normalized.contains("maintenance"));
    assert!(!normalized.contains("migration"));
    assert!(!normalized.contains("installation"));
    assert!(!normalized.contains("campaign"));
    assert!(!normalized.contains("export"));
    assert!(!normalized.contains("reimport"));

    for (surface, source) in [
        (
            "generated Rust application client",
            include_str!("../../riffdb-client-rust/src/generated/legal_spend.rs"),
        ),
        (
            "TypeScript application runtime",
            include_str!("../../../clients/typescript/runtime/src/index.ts"),
        ),
        (
            "generated TypeScript application client",
            include_str!("../../../clients/typescript/ticketdesk/client.ts"),
        ),
        (
            "Python application runtime",
            include_str!("../../../clients/python/runtime/src/riffdb_application/_binding.py"),
        ),
        (
            "generated Python application client",
            include_str!("../../../clients/python/ticketdesk/generated.py"),
        ),
        (
            "native Python application boundary",
            include_str!("../../riffdb-client-python-native/src/lib.rs"),
        ),
    ] {
        let normalized = source.to_ascii_lowercase();
        assert!(!normalized.contains("migratecontract"), "{surface}");
        assert!(!normalized.contains("contractmigration"), "{surface}");
        assert!(!normalized.contains("contract_migration"), "{surface}");
    }

    let server = include_str!("../src/server.rs");
    let registry = server
        .split("pub enum GrpcOfflineMaintenanceOperation")
        .nth(1)
        .and_then(|tail| tail.split("pub enum GrpcContractMigrationOperation").next())
        .expect("closed process-local maintenance registry");
    assert_eq!(registry.matches("CreateBackup").count(), 1);
    assert_eq!(registry.matches("RestoreBackup").count(), 1);
    assert_eq!(registry.matches("GetOperation").count(), 1);
    assert!(registry.contains("RestoreBackup {"));
    assert!(registry.contains("operation_id: OfflineMaintenanceOperationId"));
    assert!(registry.contains("input_hash: OfflineMaintenanceInputHash"));
    assert!(!registry.contains("ServiceOperationV1::"));

    let migration_registry = server
        .split("pub enum GrpcContractMigrationOperation")
        .nth(1)
        .and_then(|tail| tail.split("pub enum GrpcApplicationExportOperation").next())
        .expect("closed process-local migration registry");
    assert_eq!(migration_registry.matches("Check").count(), 1);
    assert_eq!(migration_registry.matches("Apply").count(), 1);
    assert_eq!(migration_registry.matches("GetOperation").count(), 1);
    assert!(!migration_registry.contains("ServiceOperationV1::"));

    let export_registry = server
        .split("pub enum GrpcApplicationExportOperation")
        .nth(1)
        .and_then(|tail| {
            tail.split("pub enum GrpcApplicationReimportOperation")
                .next()
        })
        .expect("closed process-local application-export registry");
    for variant in ["Start", "GetPage", "GetOperation", "Cancel"] {
        assert_eq!(
            export_registry.matches(&format!("    {variant},")).count(),
            1,
            "missing exact export admission variant {variant}"
        );
    }
    assert!(!export_registry.contains("ServiceOperationV1::"));

    let reimport_registry = server
        .split("pub enum GrpcApplicationReimportOperation")
        .nth(1)
        .and_then(|tail| tail.split("pub enum GrpcBootstrapCompletion").next())
        .expect("closed process-local application-reimport registry");
    for variant in ["Start", "ApplyPage", "GetOperation", "Cancel"] {
        assert_eq!(
            reimport_registry
                .matches(&format!("    {variant},"))
                .count(),
            1,
            "missing exact reimport admission variant {variant}"
        );
    }
    assert!(!reimport_registry.contains("ServiceOperationV1::"));
}

#[test]
fn restore_handoffs_keep_ready_retry_and_recovery_authority_disjoint() {
    let source = include_str!("../src/server.rs");
    let route = source
        .split("pub trait GrpcLifecycleRoute")
        .nth(1)
        .and_then(|tail| {
            tail.split("pub enum GrpcOfflineMaintenanceOperation")
                .next()
        })
        .expect("lifecycle route");
    assert!(route.contains(
        "fn admit_offline_maintenance(\n        &self,\n        operation: GrpcOfflineMaintenanceOperation,\n    ) -> Option<Arc<dyn ApplicationService>>"
    ));
    assert!(route.contains(
        "fn admit_restore_retry(\n        &self,\n        operation_id: OfflineMaintenanceOperationId,\n        input_hash: OfflineMaintenanceInputHash,\n    ) -> Option<Arc<dyn RestoreRetryOfflineMaintenanceApplication>>"
    ));
    assert!(route.contains(
        "fn admit_recovery_restore(\n        &self,\n        operation_id: OfflineMaintenanceOperationId,\n        input_hash: OfflineMaintenanceInputHash,\n    ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>>"
    ));

    let handler = source
        .split("async fn restore_offline_backup(")
        .nth(1)
        .and_then(|tail| {
            tail.split("async fn get_offline_maintenance_operation(")
                .next()
        })
        .expect("restore handler");
    let decode = handler
        .find("restore_offline_backup_request_from_proto(message)")
        .expect("restore is structurally decoded");
    let checked_identity = handler
        .find("let operation_id = request.operation_id()")
        .expect("checked operation identity is extracted");
    let checked_input = handler
        .find("let input_hash = request.input_hash()")
        .expect("checked semantic input identity is extracted");
    let current_admission = handler
        .find("GrpcOfflineMaintenanceOperation::RestoreBackup {")
        .expect("current route receives exact operation and input identity");
    let recovery_admission = handler
        .find("admit_recovery_restore(operation_id, input_hash)")
        .expect("recovery route receives exact operation and input identity");
    let retry_admission = handler
        .find("admit_restore_retry(operation_id, input_hash)")
        .expect("retry route receives exact operation and input identity");
    let current_authentication = handler
        .find("self.restore_context(")
        .expect("current authentication follows admission");
    let recovery_credential = handler
        .find("self.recovery_restore_context(")
        .expect("recovery credential handoff follows admission");
    let retry_authentication = handler
        .find("self.restore_retry_context(")
        .expect("retry current authentication follows exact admission");
    assert!(decode < checked_identity);
    assert!(checked_identity < checked_input);
    assert!(checked_identity < current_admission);
    assert!(checked_identity < recovery_admission);
    assert!(checked_identity < retry_admission);
    assert!(current_admission < current_authentication);
    assert!(retry_admission < retry_authentication);
    assert!(recovery_admission < recovery_credential);

    let ready = source
        .split("fn restore_context(")
        .nth(1)
        .and_then(|tail| tail.split("fn restore_retry_context(").next())
        .expect("ready restore context");
    assert_eq!(
        ready
            .matches("authenticate_and_retain_normal_request")
            .count(),
        1
    );
    assert!(ready.contains("riffdb_auth::RetainedOpaqueCredential"));

    let retry = source
        .split("fn restore_retry_context(")
        .nth(1)
        .and_then(|tail| tail.split("fn recovery_restore_context(").next())
        .expect("retry restore context");
    assert_eq!(
        retry
            .matches("authenticate_and_retain_normal_request")
            .count(),
        1
    );
    assert!(retry.contains("CheckedGrpcRestoreRetrySecurityContext"));
    assert!(!retry.contains("bootstrap_keys"));

    let recovery = source
        .split("fn recovery_restore_context(")
        .nth(1)
        .and_then(|tail| tail.split("fn bootstrap_context(").next())
        .expect("recovery restore context");
    assert_eq!(
        recovery.matches("retain_normal_request_credential").count(),
        1
    );
    assert!(!recovery.contains("authenticate_normal_request"));
    assert!(!recovery.contains("authenticate_and_retain_normal_request"));
    assert!(!recovery.contains("RequestContext::from_authenticated_grpc"));
}
