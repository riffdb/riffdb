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

pub use riffdb_observability::{
    CatalogDeploymentFailpoint, CatalogDeploymentHooks, CatalogFailpointTriggered,
    CatalogTelemetryEvent, NoopCatalogDeploymentHooks,
};
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
