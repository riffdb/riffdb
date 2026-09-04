//! Closed authentication telemetry vocabulary.

/// Redaction-safe reason retained only by trusted authentication telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationRejection {
    /// The credential is absent or not canonical token syntax.
    MalformedCredential,
    /// No configured digest candidate resolved.
    NoMatch,
    /// The resolved capability is no longer active.
    InactiveCapability,
    /// Database, environment, or audience binding does not match.
    BoundaryMismatch,
    /// Authentication time is outside the capability's half-open interval.
    OutsideValidityInterval,
}

/// Redaction-safe internal defect retained only by trusted telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationDefect {
    /// The synchronous authentication clock failed.
    ClockUnavailable,
    /// The capability repository could not complete the lookup.
    RepositoryUnavailable,
    /// The repository reported corrupt or incompatible capability state.
    RepositoryIntegrity,
    /// More than one readable digest candidate resolved.
    MultipleMatches,
    /// A supposedly reciprocal record does not match exactly one candidate.
    ReciprocalLinkMismatch,
}

/// One bounded authentication telemetry signal without credential data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationTelemetryEvent {
    /// Caller-controlled authentication rejection.
    Rejected(AuthenticationRejection),
    /// Internal authentication defect.
    Defect(AuthenticationDefect),
}

/// Trusted redaction-safe authentication telemetry sink.
pub trait AuthenticationTelemetry: Send + Sync {
    /// Records one closed event without principal, credential, digest, or key data.
    fn record(&self, event: AuthenticationTelemetryEvent);
}

/// A no-op telemetry sink for compositions that do not install one.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAuthenticationTelemetry;

impl AuthenticationTelemetry for NoopAuthenticationTelemetry {
    fn record(&self, _event: AuthenticationTelemetryEvent) {}
}
