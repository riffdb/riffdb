//! Closed redaction-safe authorization telemetry.

use crate::PolicyCode;

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
    Denied(PolicyCode),
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
