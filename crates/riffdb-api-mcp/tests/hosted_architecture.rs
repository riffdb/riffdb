//! Static architecture checks for the hosted MCP transport boundary.

#![forbid(unsafe_code)]

const HOSTED_HTTP: &str = include_str!("../src/hosted_http.rs");
const HOSTED_OBSERVER: &str = include_str!("../src/hosted_observer.rs");
const HOSTED_SESSION: &str = include_str!("../src/hosted_session.rs");
const SERVICE_BACKEND: &str = include_str!("../src/generated/service_backend.rs");
const SERVER_MAIN: &str = include_str!("../../riffdb-server/src/main.rs");

#[test]
fn hosted_authentication_has_one_owner_and_credential_never_enters_session_state() {
    assert!(HOSTED_HTTP.contains("RetainedOpaqueCredential::new"));
    assert_eq!(HOSTED_HTTP.matches(".borrow()").count(), 2);
    assert!(HOSTED_HTTP.contains(".authenticate(retained.borrow(),"));
    assert!(HOSTED_HTTP.contains("headers.remove(AUTHORIZATION)"));
    assert!(HOSTED_HTTP.contains("HostedAuthenticatedPrincipal"));
    assert!(HOSTED_HTTP.contains("RequestContext::from_authenticated_mcp_http"));

    for forbidden in [
        "RawCapabilityToken",
        "CapabilityAuthenticator",
        "impl CredentialAuthenticator",
        "expected_capability_id",
        "RequestContext::new(",
        "riffdb_policy",
        "riffdb_storage",
        "serve_directly",
        "LocalSessionManager",
    ] {
        assert!(!HOSTED_HTTP.contains(forbidden), "{forbidden}");
    }
    for forbidden in [
        "RetainedOpaqueCredential",
        "OpaqueCredential",
        "CredentialAuthenticator",
        "Authorization",
        "RawCapabilityToken",
    ] {
        assert!(!HOSTED_SESSION.contains(forbidden), "{forbidden}");
    }
}

#[test]
fn custom_manager_and_outer_wrapper_freeze_the_accepted_lifecycle_boundaries() {
    for required in [
        "MAX_HOSTED_MCP_SESSIONS: usize = 128",
        "HOSTED_MCP_IDLE_TIMEOUT: Duration = Duration::from_secs(300)",
        "HOSTED_MCP_LIFETIME: Duration = Duration::from_secs(900)",
        "config.keep_alive = None",
        "config.sse_retry = None",
        "contains_key(&id)",
        "SessionEntry::Pending",
        "SessionEntry::Active",
        "capability_id",
        "last_activity_at",
        "validate_sse_binding",
        "expire_due",
        "RestoreOutcome::NotSupported",
    ] {
        assert!(HOSTED_SESSION.contains(required), "{required}");
    }
    for required in [
        "sdk_config.stateful_mode = true",
        "sdk_config.json_response = false",
        "sdk_config.sse_keep_alive = None",
        "sdk_config.sse_retry = None",
        "pub async fn expire_due_sessions",
        "request.uri().path() != MCP_ROUTE",
        "validate_request_origin",
        "valid_effective_authority",
        "pre_authentication_admission",
        "take_authorization",
        "parse_session_header",
        "collect_bounded",
        "MCP_INBOUND_MESSAGE_MAX_BYTES",
        "MCP_OUTBOUND_MESSAGE_MAX_BYTES",
    ] {
        assert!(HOSTED_HTTP.contains(required), "{required}");
    }
    assert!(!HOSTED_SESSION.contains(".await.unwrap()"));
    assert!(!HOSTED_HTTP.contains("expect(\"checked session\")"));
}

#[test]
fn hosted_observer_keeps_credential_authority_in_the_live_response_body() {
    assert!(
        HOSTED_HTTP.contains("struct SseAuthentication {\n    retained: RetainedOpaqueCredential,")
    );
    for required in [
        "HostedInitializationRendezvous",
        "HostedObserverRegistry",
        "HostedObserverResponseState",
        "mpsc::channel(1)",
        "mcp_observer_notification_channel(&initialization.state)",
        "McpObserverSemaphore::new()",
        "drive_mcp_observer",
        "MCP_OBSERVER_TICK_INTERVAL",
        "task_state.cancel()",
        "self.task.abort()",
    ] {
        assert!(HOSTED_OBSERVER.contains(required), "{required}");
    }
    for forbidden in [
        "RetainedOpaqueCredential",
        "OpaqueCredential",
        "CredentialAuthenticator",
        "AuthenticatedPrincipal",
        "McpPostAuthenticationAdmission",
        "PolicyDecision",
        "Authorization",
    ] {
        assert!(!HOSTED_OBSERVER.contains(forbidden), "{forbidden}");
    }
    for required in [
        "register_hosted_service_mcp_http",
        "HostedServiceInvocation::fresh_observer",
        "authenticate_principal()",
        "validate_sse_binding",
        "post_authentication_limiter",
        "service.invoke_observer_service",
        "Current content/policy evidence was",
        "notification delivery adds no service",
    ] {
        assert!(HOSTED_HTTP.contains(required), "{required}");
    }
    assert!(SERVICE_BACKEND.contains("initial_request_id: Mutex::new(None)"));
    assert!(
        !SERVER_MAIN.contains("register_hosted_mcp_http("),
        "production server must never use generic conformance registration"
    );
}

#[test]
fn notification_transport_send_has_no_notification_only_authority_path() {
    let arm = HOSTED_HTTP
        .split_once("HostedObserverBodyAction::Notification { peer, notification } => {")
        .expect("notification arm")
        .1
        .split_once("\n            }\n")
        .expect("notification arm end")
        .0;

    for required in [
        "drop(principal);",
        "emit_hosted_notification(peer, notification)",
    ] {
        assert!(arm.contains(required), "{required}");
    }
    for forbidden in [
        "McpPostAuthenticationAdmission",
        "HostedServiceInvocation",
        "invoke_observer_service",
        "RequestContext",
    ] {
        assert!(!arm.contains(forbidden), "{forbidden}");
    }
    assert!(
        HOSTED_OBSERVER
            .contains("Authority-free marker emitted after a fresh admitted observer pass.")
    );
    assert!(HOSTED_OBSERVER.contains("does not make a notification-only\n    /// service call"));
    assert!(
        include_str!("../src/observer.rs")
            .contains("Acceptance here is the observer's authorized marker emission.")
    );
    for required in [
        "struct McpObserverNotificationMailboxState",
        "resource_updates: BTreeMap<String, u64>",
        "subscription_generation_is_current",
        "remove_queued_resource",
    ] {
        assert!(
            include_str!("../src/observer.rs").contains(required),
            "{required}"
        );
    }
    assert!(!include_str!("../src/observer.rs").contains("mpsc::Sender<McpObserverNotification>"));
}
