//! Post-durability catalog notifications, failpoints, and safe telemetry hooks.

use std::error::Error;
use std::fmt;

use riffdb_storage_api::{ActiveCatalogPointerV1, CatalogActivationResult};
use riffdb_types::AdministrationSequence;

/// One transport-neutral notification for a newly durable active pointer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogActivationNotification {
    active: ActiveCatalogPointerV1,
    administration_sequence: AdministrationSequence,
}

impl CatalogActivationNotification {
    /// Creates a notification only from the durable `Activated` result.
    #[must_use]
    pub const fn new(
        active: ActiveCatalogPointerV1,
        administration_sequence: AdministrationSequence,
    ) -> Self {
        Self {
            active,
            administration_sequence,
        }
    }

    /// Newly active exact immutable pointer.
    #[must_use]
    pub const fn active(&self) -> &ActiveCatalogPointerV1 {
        &self.active
    }

    /// Durable administration order associated with the change.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }
}

/// A negotiated consumer of catalog resource-list or resource-update changes.
pub trait CatalogNotificationSink {
    /// Delivers one post-durability activation notification.
    fn notify_catalog_activated(
        &mut self,
        notification: &CatalogActivationNotification,
    ) -> Result<(), CatalogNotificationError>;
}

/// Closed sink failure; adapter details remain in trusted adapter telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogNotificationError;

impl fmt::Display for CatalogNotificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("catalog notification delivery failed")
    }
}

impl Error for CatalogNotificationError {}

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

/// Result of observing a coordinator/storage activation result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogPostDurabilityResult {
    /// No durable pointer change occurred, so no notification was sent.
    NoCatalogChange,
    /// The notification sink accepted the durable change.
    NotificationDelivered,
    /// Sink failure occurred after durability and cannot roll back activation.
    NotificationFailed,
    /// A named recovery boundary interrupted processing after durability.
    Interrupted(CatalogDeploymentFailpoint),
}

/// Marks the last catalog-owned boundary before coordinator submission.
pub fn before_catalog_activation_submission<H: CatalogDeploymentHooks>(
    hooks: &mut H,
) -> Result<(), CatalogFailpointTriggered> {
    hooks.record(CatalogTelemetryEvent::ActivationPrepared);
    hooks.reach(CatalogDeploymentFailpoint::AfterPreparationBeforeSubmission)
}

/// Emits notifications only for a real newly durable `Activated` result.
///
/// `AlreadyActive`, expected-version mismatch, and bundle conflict are no-write
/// results and deliberately emit nothing. Notification or failpoint failure never
/// changes the already durable catalog state.
pub fn observe_catalog_activation<N: CatalogNotificationSink, H: CatalogDeploymentHooks>(
    result: &CatalogActivationResult,
    notifications: &mut N,
    hooks: &mut H,
) -> CatalogPostDurabilityResult {
    let CatalogActivationResult::Activated {
        active,
        administration_sequence,
    } = result
    else {
        hooks.record(CatalogTelemetryEvent::NoCatalogChange);
        return CatalogPostDurabilityResult::NoCatalogChange;
    };

    hooks.record(CatalogTelemetryEvent::ActivationDurable);
    if let Err(interrupted) =
        hooks.reach(CatalogDeploymentFailpoint::AfterDurableActivationBeforeNotification)
    {
        return CatalogPostDurabilityResult::Interrupted(interrupted.failpoint());
    }

    let notification = CatalogActivationNotification::new(active.clone(), *administration_sequence);
    if notifications
        .notify_catalog_activated(&notification)
        .is_err()
    {
        hooks.record(CatalogTelemetryEvent::NotificationFailed);
        return CatalogPostDurabilityResult::NotificationFailed;
    }
    hooks.record(CatalogTelemetryEvent::NotificationDelivered);
    if let Err(interrupted) =
        hooks.reach(CatalogDeploymentFailpoint::AfterNotificationBeforeAcknowledgement)
    {
        return CatalogPostDurabilityResult::Interrupted(interrupted.failpoint());
    }
    CatalogPostDurabilityResult::NotificationDelivered
}
