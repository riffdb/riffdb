//! Authorization telemetry vocabulary owned by the observability leaf.

pub use riffdb_observability::{
    AuthorizationDefect, AuthorizationDenial, AuthorizationTelemetry, AuthorizationTelemetryEvent,
    NoopAuthorizationTelemetry,
};

impl From<crate::PolicyCode> for AuthorizationDenial {
    fn from(code: crate::PolicyCode) -> Self {
        match code {
            crate::PolicyCode::MissingPermission => Self::MissingPermission,
            crate::PolicyCode::TenantScopeMismatch => Self::TenantScopeMismatch,
            crate::PolicyCode::PartitionScopeMismatch => Self::PartitionScopeMismatch,
            crate::PolicyCode::FieldVisibilityDenied => Self::FieldVisibilityDenied,
            crate::PolicyCode::ApprovalRequired => Self::ApprovalRequired,
            crate::PolicyCode::DelegationExceedsAuthority => Self::DelegationExceedsAuthority,
            crate::PolicyCode::InactiveOrStaleCapability => Self::InactiveOrStaleCapability,
        }
    }
}
