//! Closed redaction-safe authorization telemetry.

/// Closed policy denial classification retained by telemetry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AuthorizationDenial {
    /// The current grant lacks the exact required permission atom.
    MissingPermission,
    /// The request is outside the capability's tenant authority.
    TenantScopeMismatch,
    /// The request is outside the capability's exact partition authority.
    PartitionScopeMismatch,
    /// At least one requested non-key field is not visible.
    FieldVisibilityDenied,
    /// The selected permission requires approval that has not been validated.
    ApprovalRequired,
    /// A capability mutation has not proven the required delegation relation.
    DelegationExceedsAuthority,
    /// Current capability state is inactive, expired, or stale.
    InactiveOrStaleCapability,
}

impl AuthorizationDenial {
    /// Every denial class in stable metric order.
    pub const ALL: [Self; 7] = [
        Self::MissingPermission,
        Self::TenantScopeMismatch,
        Self::PartitionScopeMismatch,
        Self::FieldVisibilityDenied,
        Self::ApprovalRequired,
        Self::DelegationExceedsAuthority,
        Self::InactiveOrStaleCapability,
    ];

    /// Stable telemetry tag shared with the policy denial vocabulary.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::MissingPermission => 0x01,
            Self::TenantScopeMismatch => 0x02,
            Self::PartitionScopeMismatch => 0x03,
            Self::FieldVisibilityDenied => 0x04,
            Self::ApprovalRequired => 0x05,
            Self::DelegationExceedsAuthority => 0x06,
            Self::InactiveOrStaleCapability => 0x07,
        }
    }
}

/// Internal authorization defect category without request or capability data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationDefect {
    /// Current capability state could not be reloaded safely.
    CurrentCapabilityUnavailable,
    /// Current authorization time could not be obtained safely.
    ClockUnavailable,
}

/// One bounded authorization telemetry event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationTelemetryEvent {
    /// Current policy denied the request with a closed code.
    Denied(AuthorizationDenial),
    /// Policy evaluation failed internally before deciding.
    Defect(AuthorizationDefect),
}

/// Trusted telemetry sink that never receives raw authorization facts.
pub trait AuthorizationTelemetry: Send + Sync {
    /// Records one closed event.
    fn record(&self, event: AuthorizationTelemetryEvent);
}

/// A no-op sink for compositions without an installed telemetry consumer.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAuthorizationTelemetry;

impl AuthorizationTelemetry for NoopAuthorizationTelemetry {
    fn record(&self, _event: AuthorizationTelemetryEvent) {}
}
