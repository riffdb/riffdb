//! Payload-free telemetry emitted by delivery and recovery.

use riffdb_storage_api::StorageErrorKind;
use riffdb_types::EventId;

use crate::OutboxFailpoint;

/// Closed connector result classification safe for metrics and tracing.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorResultClass {
    /// The destination accepted the event.
    Accepted,
    /// Delivery may be retried under worker policy.
    Retryable,
    /// Delivery was rejected permanently.
    PermanentFailure,
}

/// Closed telemetry event containing no payload, destination, or error text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxTelemetryEvent {
    /// Startup normalization began.
    RecoveryStarted,
    /// One interrupted attempt was returned to explicit pending.
    RecoveryNormalized {
        /// Stable event identity.
        event_id: EventId,
        /// Attempt that was interrupted.
        attempt: u32,
    },
    /// Recovery reached an exact end and readiness may be released.
    RecoveryReady {
        /// Number of statuses normalized in this recovery run.
        normalized: u64,
    },
    /// A bounded storage operation failed.
    StorageFailure {
        /// Closed storage failure kind.
        kind: StorageErrorKind,
    },
    /// An event was durably claimed before external I/O.
    DeliveryClaimed {
        /// Stable event identity.
        event_id: EventId,
        /// Nonzero delivery attempt.
        attempt: u32,
    },
    /// A connector returned one closed classification.
    ConnectorResult {
        /// Stable event identity.
        event_id: EventId,
        /// Nonzero delivery attempt.
        attempt: u32,
        /// Redaction-safe connector classification.
        class: ConnectorResultClass,
    },
    /// A retry was durably scheduled.
    RetryScheduled {
        /// Stable event identity.
        event_id: EventId,
        /// Attempt that failed.
        attempt: u32,
    },
    /// Delivery reached durable success.
    Delivered {
        /// Stable event identity.
        event_id: EventId,
        /// Successful attempt.
        attempt: u32,
    },
    /// Delivery reached worker-selected terminal failure.
    DeadLettered {
        /// Stable event identity.
        event_id: EventId,
        /// Attempts begun before the terminal decision.
        attempts: u32,
    },
    /// Exact compare-and-transition observed concurrent state.
    StateChanged {
        /// Stable event identity.
        event_id: EventId,
    },
    /// A named test interruption fired.
    Interrupted {
        /// Exact named failpoint.
        failpoint: OutboxFailpoint,
        /// Stable event identity, absent only at the final readiness fence.
        event_id: Option<EventId>,
    },
}

/// Trusted sink for closed payload-free outbox events.
pub trait OutboxTelemetry {
    /// Records one bounded event.
    fn record(&mut self, event: OutboxTelemetryEvent);
}

/// Production-safe sink that discards telemetry.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoOutboxTelemetry;

impl OutboxTelemetry for NoOutboxTelemetry {
    fn record(&mut self, _event: OutboxTelemetryEvent) {}
}
