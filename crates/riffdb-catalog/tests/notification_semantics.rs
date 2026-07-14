//! Post-durability notification and failpoint semantics.

use riffdb_catalog::{
    CatalogActivationNotification, CatalogDeploymentFailpoint, CatalogDeploymentHooks,
    CatalogFailpointTriggered, CatalogNotificationError, CatalogNotificationSink,
    CatalogPostDurabilityResult, CatalogTelemetryEvent, before_catalog_activation_submission,
    observe_catalog_activation,
};
use riffdb_storage_api::{ActiveCatalogPointerV1, CatalogActivationResult};
use riffdb_types::{AdministrationSequence, ContractBundleHash, ContractLineage, ContractVersion};

#[derive(Default)]
struct Sink {
    notifications: Vec<CatalogActivationNotification>,
    fail: bool,
}

impl CatalogNotificationSink for Sink {
    fn notify_catalog_activated(
        &mut self,
        notification: &CatalogActivationNotification,
    ) -> Result<(), CatalogNotificationError> {
        if self.fail {
            Err(CatalogNotificationError)
        } else {
            self.notifications.push(notification.clone());
            Ok(())
        }
    }
}

#[derive(Default)]
struct Hooks {
    events: Vec<CatalogTelemetryEvent>,
    stop_at: Option<CatalogDeploymentFailpoint>,
}

impl CatalogDeploymentHooks for Hooks {
    fn record(&mut self, event: CatalogTelemetryEvent) {
        self.events.push(event);
    }

    fn reach(
        &mut self,
        failpoint: CatalogDeploymentFailpoint,
    ) -> Result<(), CatalogFailpointTriggered> {
        if self.stop_at == Some(failpoint) {
            Err(CatalogFailpointTriggered::new(failpoint))
        } else {
            Ok(())
        }
    }
}

fn pointer(version: u64, byte: u8) -> ActiveCatalogPointerV1 {
    ActiveCatalogPointerV1::new(
        ContractLineage::new("LegalSpend").expect("lineage"),
        ContractVersion::new(version).expect("version"),
        ContractBundleHash::from_bytes([byte; 32]),
    )
}

#[test]
fn only_a_real_activated_result_notifies() {
    let activated = CatalogActivationResult::Activated {
        active: pointer(2, 2),
        administration_sequence: AdministrationSequence::first(),
    };
    let mut sink = Sink::default();
    let mut hooks = Hooks::default();
    assert_eq!(
        observe_catalog_activation(&activated, &mut sink, &mut hooks),
        CatalogPostDurabilityResult::NotificationDelivered
    );
    assert_eq!(sink.notifications.len(), 1);
    assert_eq!(sink.notifications[0].active(), &pointer(2, 2));

    for no_write in [
        CatalogActivationResult::AlreadyActive {
            active: pointer(2, 2),
            administration_sequence: AdministrationSequence::first(),
        },
        CatalogActivationResult::ExpectedActiveVersionMismatch {
            actual: Some(ContractVersion::new(1).expect("version")),
        },
        CatalogActivationResult::BundleConflict,
    ] {
        assert_eq!(
            observe_catalog_activation(&no_write, &mut sink, &mut hooks),
            CatalogPostDurabilityResult::NoCatalogChange
        );
    }
    assert_eq!(sink.notifications.len(), 1);
}

#[test]
fn notification_and_failpoint_failure_never_reclassify_durable_state() {
    let activated = CatalogActivationResult::Activated {
        active: pointer(1, 1),
        administration_sequence: AdministrationSequence::first(),
    };

    let mut failed_sink = Sink {
        fail: true,
        ..Sink::default()
    };
    let mut hooks = Hooks::default();
    assert_eq!(
        observe_catalog_activation(&activated, &mut failed_sink, &mut hooks),
        CatalogPostDurabilityResult::NotificationFailed
    );
    assert!(
        hooks
            .events
            .contains(&CatalogTelemetryEvent::ActivationDurable)
    );
    assert!(
        hooks
            .events
            .contains(&CatalogTelemetryEvent::NotificationFailed)
    );

    let mut sink = Sink::default();
    let mut interrupted = Hooks {
        stop_at: Some(CatalogDeploymentFailpoint::AfterDurableActivationBeforeNotification),
        ..Hooks::default()
    };
    assert_eq!(
        observe_catalog_activation(&activated, &mut sink, &mut interrupted),
        CatalogPostDurabilityResult::Interrupted(
            CatalogDeploymentFailpoint::AfterDurableActivationBeforeNotification
        )
    );
    assert!(sink.notifications.is_empty());

    let mut before_submission = Hooks {
        stop_at: Some(CatalogDeploymentFailpoint::AfterPreparationBeforeSubmission),
        ..Hooks::default()
    };
    assert_eq!(
        before_catalog_activation_submission(&mut before_submission)
            .expect_err("named failpoint")
            .failpoint(),
        CatalogDeploymentFailpoint::AfterPreparationBeforeSubmission
    );

    let mut sink = Sink::default();
    let mut after_delivery = Hooks {
        stop_at: Some(CatalogDeploymentFailpoint::AfterNotificationBeforeAcknowledgement),
        ..Hooks::default()
    };
    assert_eq!(
        observe_catalog_activation(&activated, &mut sink, &mut after_delivery),
        CatalogPostDurabilityResult::Interrupted(
            CatalogDeploymentFailpoint::AfterNotificationBeforeAcknowledgement
        )
    );
    assert_eq!(sink.notifications.len(), 1);

    let replay = CatalogActivationResult::AlreadyActive {
        active: pointer(1, 1),
        administration_sequence: AdministrationSequence::first(),
    };
    assert_eq!(
        observe_catalog_activation(&replay, &mut sink, &mut Hooks::default()),
        CatalogPostDurabilityResult::NoCatalogChange
    );
    assert_eq!(sink.notifications.len(), 1);
}
