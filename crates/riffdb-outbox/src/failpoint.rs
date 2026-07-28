//! Named deterministic delivery failpoints.

use riffdb_types::EventId;

/// Stable named interruption points used by crash and recovery tests.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OutboxFailpoint {
    /// Recovery scanned an interrupted in-flight status but did not normalize it.
    RecoveryAfterScanBeforeNormalize,
    /// One interrupted status was normalized but recovery did not yet finish.
    RecoveryAfterNormalize,
    /// Every recovery page completed but dispatcher readiness was not released.
    RecoveryBeforeReady,
    /// A durable claim committed but no connector call began.
    DeliveryAfterClaimBeforeConnector,
    /// The connector accepted an event but success status was not persisted.
    DeliveryAfterConnectorAcceptedBeforeSuccess,
    /// A retryable connector result was observed but retry status was not persisted.
    DeliveryAfterConnectorRetryableBeforeRetry,
    /// A permanent connector result was observed but dead-letter status was not persisted.
    DeliveryAfterConnectorRejectedBeforeDeadLetter,
}

impl OutboxFailpoint {
    /// Returns the stable failpoint name used by process harnesses.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::RecoveryAfterScanBeforeNormalize => "outbox.recovery.after_scan_before_normalize",
            Self::RecoveryAfterNormalize => "outbox.recovery.after_normalize",
            Self::RecoveryBeforeReady => "outbox.recovery.before_ready",
            Self::DeliveryAfterClaimBeforeConnector => {
                "outbox.delivery.after_claim_before_connector"
            }
            Self::DeliveryAfterConnectorAcceptedBeforeSuccess => {
                "outbox.delivery.after_connector_accepted_before_success"
            }
            Self::DeliveryAfterConnectorRetryableBeforeRetry => {
                "outbox.delivery.after_connector_retryable_before_retry"
            }
            Self::DeliveryAfterConnectorRejectedBeforeDeadLetter => {
                "outbox.delivery.after_connector_rejected_before_dead_letter"
            }
        }
    }
}

/// Injected deterministic interruption controller.
pub trait OutboxFailpoints {
    /// Returns `true` exactly when processing must stop at this point.
    ///
    /// Only the stable event identity is provided; payloads, destinations, and
    /// connector responses never cross this hook.
    fn should_interrupt(&mut self, failpoint: OutboxFailpoint, event_id: Option<EventId>) -> bool;
}

/// Production default with every failpoint disabled.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoOutboxFailpoints;

impl OutboxFailpoints for NoOutboxFailpoints {
    fn should_interrupt(
        &mut self,
        _failpoint: OutboxFailpoint,
        _event_id: Option<EventId>,
    ) -> bool {
        false
    }
}
