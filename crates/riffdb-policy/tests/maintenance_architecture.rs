#![forbid(unsafe_code)]

//! Guards for the process-local offline-maintenance authorization boundary.

const MAINTENANCE_SOURCE: &str = include_str!("../src/maintenance.rs");
const AUTHORIZER_SOURCE: &str = include_str!("../src/authorizer.rs");

#[test]
fn maintenance_policy_does_not_create_a_durable_or_transport_operation() {
    for required in [
        "pub enum OfflineMaintenancePolicyOperation",
        "Start(OfflineMaintenanceOperationKind)",
        "CreateBackup",
        "RestoreBackup",
        "GetOperation",
        "OfflineMaintenanceOperationId",
        "OfflineMaintenanceInputHash",
        "CapabilityPermissionKindV1::AdministerCapabilities",
        "TenantScope::Global",
        "PermissionCheck::ApprovalRequired",
    ] {
        assert!(
            MAINTENANCE_SOURCE.contains(required) || AUTHORIZER_SOURCE.contains(required),
            "maintenance policy boundary changed: {required}"
        );
    }

    for forbidden in [
        "ServiceOperationV1::",
        "CapabilityPermissionKindV1 {",
        "serde::",
        "Serialize",
        "Deserialize",
        "riffdb_storage",
        "riffdb_proto",
        "OpaqueCredential",
        "RetainedOpaqueCredential",
        "RawCapabilityToken",
    ] {
        assert!(
            !MAINTENANCE_SOURCE.contains(forbidden),
            "maintenance policy gained an unreviewed boundary: {forbidden}"
        );
    }
}

#[test]
fn maintenance_authority_is_non_clone_and_redacted() {
    for required in [
        "pub struct AuthorizedOfflineMaintenance",
        "AuthorizedOfflineMaintenance([REDACTED])",
        "pub enum OfflineMaintenanceDecision",
        "OfflineMaintenanceDecision::Allow([REDACTED])",
    ] {
        assert!(
            MAINTENANCE_SOURCE.contains(required),
            "maintenance proof boundary changed: {required}"
        );
    }
    for forbidden in [
        "impl Clone for AuthorizedOfflineMaintenance",
        "derive(Clone, Eq, PartialEq)]\npub struct AuthorizedOfflineMaintenance",
        "impl serde::Serialize for AuthorizedOfflineMaintenance",
    ] {
        assert!(
            !MAINTENANCE_SOURCE.contains(forbidden),
            "maintenance authority became reusable or serializable: {forbidden}"
        );
    }
}
