//! Closed catalog deployment telemetry and failpoint hooks.

use std::error::Error;
use std::fmt;

/// Named deployment boundaries used by deterministic and process recovery tests.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CatalogDeploymentFailpoint {
    /// Candidate is checked but has not been submitted to the coordinator.
    AfterPreparationBeforeSubmission,
    /// The active pointer is durable but no notification has been attempted.
    AfterDurableActivationBeforeNotification,
    /// Notification returned successfully but the caller has not been acknowledged.
    AfterNotificationBeforeAcknowledgement,
}

/// Closed redaction-safe catalog telemetry event.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CatalogTelemetryEvent {
    /// A checked activation is ready for coordinator submission.
    ActivationPrepared,
    /// A new active pointer was reported durable.
    ActivationDurable,
    /// A durable activation notification was delivered to the sink.
    NotificationDelivered,
    /// Notification delivery failed after durability.
    NotificationFailed,
    /// A semantic no-write result produced no notification.
    NoCatalogChange,
}

/// Closed failpoint signal. It carries no engine or contract text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogFailpointTriggered {
    failpoint: CatalogDeploymentFailpoint,
}

impl CatalogFailpointTriggered {
    /// Constructs one deterministic failpoint signal.
    #[must_use]
    pub const fn new(failpoint: CatalogDeploymentFailpoint) -> Self {
        Self { failpoint }
    }

    /// Named boundary that interrupted the synthetic deployment.
    #[must_use]
    pub const fn failpoint(self) -> CatalogDeploymentFailpoint {
        self.failpoint
    }
}

impl fmt::Display for CatalogFailpointTriggered {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("catalog deployment interrupted at a named failpoint")
    }
}

impl Error for CatalogFailpointTriggered {}

/// Injected closed hooks; implementations cannot inspect bundle or entity bytes.
pub trait CatalogDeploymentHooks {
    /// Records one closed, redaction-safe phase event.
    fn record(&mut self, event: CatalogTelemetryEvent);

    /// Reaches one named boundary and may inject a deterministic interruption.
    fn reach(
        &mut self,
        failpoint: CatalogDeploymentFailpoint,
    ) -> Result<(), CatalogFailpointTriggered>;
}

/// Default hooks used when no deterministic failpoint is configured.
#[derive(Default)]
pub struct NoopCatalogDeploymentHooks;

impl CatalogDeploymentHooks for NoopCatalogDeploymentHooks {
    fn record(&mut self, _event: CatalogTelemetryEvent) {}

    fn reach(
        &mut self,
        _failpoint: CatalogDeploymentFailpoint,
    ) -> Result<(), CatalogFailpointTriggered> {
        Ok(())
    }
}
