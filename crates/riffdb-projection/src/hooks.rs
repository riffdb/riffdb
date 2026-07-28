//! Closed failpoint and telemetry hooks.

use crate::ProjectionCoreError;

/// Named projection-core failpoints with no application payload.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProjectionFailpoint {
    /// Immediately before an atomic state/marker/frontier apply request.
    BeforeStateAndFrontierApply,
    /// Immediately after storage reports the atomic apply durable.
    AfterStateAndFrontierApply,
    /// Immediately before a durable lifecycle control transition.
    BeforeControlTransition,
    /// Immediately after a durable lifecycle control transition.
    AfterControlTransition,
}

/// Closed redaction-safe projection telemetry event.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProjectionTelemetryEvent {
    /// A projection control record was initialized.
    Initialized,
    /// Initial contiguous catch-up started.
    InitialCatchUpStarted,
    /// One authoritative sequence was newly applied.
    SequenceApplied,
    /// An equal apply marker resolved an idempotent duplicate.
    DuplicateApplyConfirmed,
    /// A candidate generation was published.
    CandidatePublished,
    /// A same-plan rebuild generation was allocated.
    RebuildAllocated,
    /// A retained generation entered degraded state.
    GenerationDegraded,
    /// Degraded recovery allocated or resumed a replacement.
    RecoveryStarted,
    /// A projection was proven invalid under v1 recovery policy.
    MarkedInvalid,
    /// A waiter was notified after a durable state transition.
    WaitersNotified,
}

/// Projection-owned hooks. Implementations must not attach application data.
pub trait ProjectionHooks {
    /// Runs a named failpoint before or after a semantic boundary.
    fn failpoint(&mut self, _point: ProjectionFailpoint) -> Result<(), ProjectionCoreError> {
        Ok(())
    }

    /// Records one closed redaction-safe event.
    fn record(&mut self, _event: ProjectionTelemetryEvent) {}
}

/// Production no-op hooks.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopProjectionHooks;

impl ProjectionHooks for NoopProjectionHooks {}
