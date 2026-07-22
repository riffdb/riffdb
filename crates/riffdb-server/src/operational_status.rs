//! Cached operational status for the minimal P1 process graph.

#![allow(
    dead_code,
    reason = "WP-130 process composition installs this private provider"
)]

use std::fmt;

use riffdb_service::{
    BoxPortCapacityPermit, ComponentHealth, HealthComponentKind, HealthComponentStatus,
    OperationalHealthSnapshot, OperationalStatisticsSnapshot, OperationalStatusError,
    OperationalStatusPort, PortAdmissionError, PortCapacityPermit, PortFuture, PortReceipt,
    RequestControl, port_completion_channel,
};

use crate::notifications::{FirstCommitNotificationHub, NotificationStatusError};
use crate::runtime_support::{RuntimeRoutingState, RuntimeStopReason};
use crate::startup::ValidatedAllocatorCapacity;

/// Process-local status assembled from checked startup and monotonic routing state.
#[derive(Clone)]
pub(crate) struct ProductionOperationalStatusPort {
    allocator_capacity: ValidatedAllocatorCapacity,
    runtime: RuntimeRoutingState,
    notifications: FirstCommitNotificationHub,
}

impl ProductionOperationalStatusPort {
    pub(crate) fn new(
        allocator_capacity: ValidatedAllocatorCapacity,
        runtime: RuntimeRoutingState,
        notifications: FirstCommitNotificationHub,
    ) -> Self {
        Self {
            allocator_capacity,
            runtime,
            notifications,
        }
    }
}

impl OperationalStatusPort for ProductionOperationalStatusPort {
    fn reserve_health(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<(), OperationalHealthSnapshot, OperationalStatusError>,
        PortAdmissionError,
    > {
        let admission = checked_control(control).map(|()| {
            Box::new(OperationalHealthPermit {
                allocator_capacity: self.allocator_capacity,
                runtime: self.runtime.clone(),
            })
                as BoxPortCapacityPermit<(), OperationalHealthSnapshot, OperationalStatusError>
        });
        Box::pin(async move { admission })
    }

    fn reserve_statistics(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<(), OperationalStatisticsSnapshot, OperationalStatusError>,
        PortAdmissionError,
    > {
        let admission = checked_control(control).map(|()| {
            Box::new(OperationalStatisticsPermit {
                notifications: self.notifications.clone(),
            })
                as BoxPortCapacityPermit<(), OperationalStatisticsSnapshot, OperationalStatusError>
        });
        Box::pin(async move { admission })
    }
}

impl fmt::Debug for ProductionOperationalStatusPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionOperationalStatusPort([REDACTED])")
    }
}

fn checked_control(control: &RequestControl) -> Result<(), PortAdmissionError> {
    if control.is_cancelled() {
        Err(PortAdmissionError::Cancelled)
    } else if control.is_deadline_exceeded() {
        Err(PortAdmissionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

struct OperationalHealthPermit {
    allocator_capacity: ValidatedAllocatorCapacity,
    runtime: RuntimeRoutingState,
}

impl PortCapacityPermit<(), OperationalHealthSnapshot, OperationalStatusError>
    for OperationalHealthPermit
{
    fn submit(
        self: Box<Self>,
        (): (),
    ) -> Result<PortReceipt<OperationalHealthSnapshot, OperationalStatusError>, PortAdmissionError>
    {
        let result = required_health_snapshot(self.allocator_capacity, self.runtime.stop_reason());
        let (completion, receipt) = port_completion_channel();
        completion.complete(result);
        Ok(receipt)
    }
}

fn required_health_snapshot(
    allocator_capacity: ValidatedAllocatorCapacity,
    runtime_stop: Option<RuntimeStopReason>,
) -> Result<OperationalHealthSnapshot, OperationalStatusError> {
    let runtime_running = runtime_stop.is_none();
    let storage_status = required_component_status(runtime_running);
    let catalog_status = required_component_status(runtime_running);
    let coordinator_status = required_component_status(
        runtime_running && allocator_capacity == ValidatedAllocatorCapacity::Available,
    );

    OperationalHealthSnapshot::new(vec![
        ComponentHealth::new(HealthComponentKind::AuthoritativeStorage, storage_status),
        ComponentHealth::new(HealthComponentKind::Catalog, catalog_status),
        ComponentHealth::new(HealthComponentKind::CommitCoordinator, coordinator_status),
    ])
    .map_err(|_| OperationalStatusError::Integrity)
}

const fn required_component_status(healthy: bool) -> HealthComponentStatus {
    if healthy {
        HealthComponentStatus::Healthy
    } else {
        HealthComponentStatus::Unavailable
    }
}

struct OperationalStatisticsPermit {
    notifications: FirstCommitNotificationHub,
}

impl PortCapacityPermit<(), OperationalStatisticsSnapshot, OperationalStatusError>
    for OperationalStatisticsPermit
{
    fn submit(
        self: Box<Self>,
        (): (),
    ) -> Result<
        PortReceipt<OperationalStatisticsSnapshot, OperationalStatusError>,
        PortAdmissionError,
    > {
        let result = self
            .notifications
            .latest_sequence()
            .map(|latest| OperationalStatisticsSnapshot::new(latest, None, None))
            .map_err(map_notification_status_error);
        let (completion, receipt) = port_completion_channel();
        completion.complete(result);
        Ok(receipt)
    }
}

const fn map_notification_status_error(error: NotificationStatusError) -> OperationalStatusError {
    match error {
        NotificationStatusError::Closed => OperationalStatusError::Unavailable,
        NotificationStatusError::Integrity => OperationalStatusError::Integrity,
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};
    use std::time::{Duration, Instant};

    use riffdb_commit::ApplicationCommitNotificationSink;
    use riffdb_service::{AuthoritativeReadinessFailure, PortDriverStopped, ServiceHealthHooks};

    use super::*;

    fn live_control() -> RequestControl {
        RequestControl::new(Instant::now() + Duration::from_secs(30)).0
    }

    fn complete_now<T, E>(receipt: PortReceipt<T, E>) -> Result<Result<T, E>, PortDriverStopped> {
        let mut receipt = Pin::new(Box::new(receipt));
        let mut context = Context::from_waker(Waker::noop());
        match Future::poll(receipt.as_mut(), &mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("process-local completion was not immediate"),
        }
    }

    fn status(
        snapshot: &OperationalHealthSnapshot,
        component: HealthComponentKind,
    ) -> HealthComponentStatus {
        snapshot
            .components()
            .iter()
            .copied()
            .find(|entry| entry.component() == component)
            .expect("required component is present")
            .status()
    }

    #[test]
    fn every_allocator_state_has_the_required_canonical_health_shape() {
        for (capacity, coordinator_status) in [
            (
                ValidatedAllocatorCapacity::Available,
                HealthComponentStatus::Healthy,
            ),
            (
                ValidatedAllocatorCapacity::ApplicationExhausted,
                HealthComponentStatus::Unavailable,
            ),
            (
                ValidatedAllocatorCapacity::AdministrationExhausted,
                HealthComponentStatus::Unavailable,
            ),
            (
                ValidatedAllocatorCapacity::BothExhausted,
                HealthComponentStatus::Unavailable,
            ),
        ] {
            let snapshot = required_health_snapshot(capacity, None).expect("fixed health shape");
            assert_eq!(snapshot.components().len(), 3);
            assert_eq!(
                snapshot.components()[0].component(),
                HealthComponentKind::AuthoritativeStorage
            );
            assert_eq!(
                snapshot.components()[1].component(),
                HealthComponentKind::Catalog
            );
            assert_eq!(
                snapshot.components()[2].component(),
                HealthComponentKind::CommitCoordinator
            );
            assert_eq!(
                status(&snapshot, HealthComponentKind::AuthoritativeStorage),
                HealthComponentStatus::Healthy
            );
            assert_eq!(
                status(&snapshot, HealthComponentKind::Catalog),
                HealthComponentStatus::Healthy
            );
            assert_eq!(
                status(&snapshot, HealthComponentKind::CommitCoordinator),
                coordinator_status
            );
        }
    }

    #[test]
    fn every_runtime_stop_reason_fails_all_required_components_closed() {
        for reason in [
            RuntimeStopReason::AuditUnavailable,
            RuntimeStopReason::CoordinatorFenced,
            RuntimeStopReason::Integrity,
            RuntimeStopReason::AcceptedServiceJobPanicked,
            RuntimeStopReason::AcceptedServiceJobDisappeared,
            RuntimeStopReason::SupervisionStateCorrupted,
            RuntimeStopReason::DiagnosticCapacityExceeded,
        ] {
            let snapshot =
                required_health_snapshot(ValidatedAllocatorCapacity::Available, Some(reason))
                    .expect("fixed health shape");
            assert!(
                snapshot
                    .components()
                    .iter()
                    .all(|component| component.status() == HealthComponentStatus::Unavailable),
                "runtime stop reason remained ready: {reason:?}"
            );
        }
    }

    #[tokio::test]
    async fn admitted_health_samples_a_later_runtime_stop_and_remains_available() {
        let runtime = RuntimeRoutingState::new();
        let port = ProductionOperationalStatusPort::new(
            ValidatedAllocatorCapacity::Available,
            runtime.clone(),
            FirstCommitNotificationHub::new(None, runtime.clone()),
        );
        let permit = port
            .reserve_health(&live_control())
            .await
            .expect("running health admission");

        runtime.fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);

        let snapshot = complete_now(permit.submit(()).expect("health submission"))
            .expect("completion sender retained")
            .expect("fixed health snapshot");
        assert!(
            snapshot
                .components()
                .iter()
                .all(|component| component.status() == HealthComponentStatus::Unavailable)
        );

        let stopped_permit = port
            .reserve_health(&live_control())
            .await
            .expect("authenticated Health remains admitted after runtime stop");
        let stopped = complete_now(stopped_permit.submit(()).expect("health submission"))
            .expect("completion sender retained")
            .expect("fixed health snapshot");
        assert_eq!(snapshot, stopped);

        let statistics_permit = port
            .reserve_statistics(&live_control())
            .await
            .expect("Health statistics remain admitted after runtime stop");
        let statistics = complete_now(statistics_permit.submit(()).expect("statistics submission"))
            .expect("completion sender retained")
            .expect("fixed statistics snapshot");
        assert_eq!(statistics.last_commit_sequence(), None);
    }

    #[tokio::test]
    async fn statistics_do_not_invent_optional_subsystems_or_authoritative_frontier() {
        let runtime = RuntimeRoutingState::new();
        let port = ProductionOperationalStatusPort::new(
            ValidatedAllocatorCapacity::Available,
            runtime.clone(),
            FirstCommitNotificationHub::new(None, runtime),
        );
        let permit = port
            .reserve_statistics(&live_control())
            .await
            .expect("statistics admission");
        let statistics = complete_now(permit.submit(()).expect("statistics submission"))
            .expect("completion sender retained")
            .expect("fixed statistics snapshot");

        assert_eq!(statistics.last_commit_sequence(), None);
        assert_eq!(statistics.pending_outbox_deliveries(), None);
        assert_eq!(statistics.known_projections(), None);
    }

    #[tokio::test]
    async fn statistics_samples_the_monotonic_commit_cache_at_submission() {
        let runtime = RuntimeRoutingState::new();
        let notifications = FirstCommitNotificationHub::new(
            Some(riffdb_types::CommitSequence::new(3).expect("sequence")),
            runtime.clone(),
        );
        let port = ProductionOperationalStatusPort::new(
            ValidatedAllocatorCapacity::Available,
            runtime,
            notifications.clone(),
        );
        let permit = port
            .reserve_statistics(&live_control())
            .await
            .expect("statistics admission");
        let fourth = riffdb_types::CommitSequence::new(4).expect("sequence");
        notifications
            .publish_first_commit(fourth)
            .expect("publication");

        let statistics = complete_now(permit.submit(()).expect("statistics submission"))
            .expect("completion sender retained")
            .expect("fixed statistics snapshot");
        assert_eq!(statistics.last_commit_sequence(), Some(fourth));

        notifications
            .publish_first_commit(riffdb_types::CommitSequence::new(2).expect("stale sequence"))
            .expect("stale publication remains a hint");
        let permit = port
            .reserve_statistics(&live_control())
            .await
            .expect("statistics admission");
        let statistics = complete_now(permit.submit(()).expect("statistics submission"))
            .expect("completion sender retained")
            .expect("fixed statistics snapshot");
        assert_eq!(statistics.last_commit_sequence(), Some(fourth));
        assert_eq!(statistics.pending_outbox_deliveries(), None);
        assert_eq!(statistics.known_projections(), None);
    }

    #[tokio::test]
    async fn closed_status_cache_is_unavailable_without_becoming_empty() {
        let initial = riffdb_types::CommitSequence::new(9).expect("sequence");
        let runtime = RuntimeRoutingState::new();
        let notifications = FirstCommitNotificationHub::new(Some(initial), runtime.clone());
        let port = ProductionOperationalStatusPort::new(
            ValidatedAllocatorCapacity::Available,
            runtime,
            notifications.clone(),
        );
        let permit = port
            .reserve_statistics(&live_control())
            .await
            .expect("statistics admission");
        notifications.shutdown().expect("notification shutdown");

        let result = complete_now(permit.submit(()).expect("statistics submission"))
            .expect("completion sender retained");
        assert_eq!(result, Err(OperationalStatusError::Unavailable));
        assert_eq!(
            map_notification_status_error(NotificationStatusError::Integrity),
            OperationalStatusError::Integrity
        );
    }

    #[tokio::test]
    async fn request_control_is_checked_before_health_or_statistics_admission() {
        let runtime = RuntimeRoutingState::new();
        let port = ProductionOperationalStatusPort::new(
            ValidatedAllocatorCapacity::Available,
            runtime.clone(),
            FirstCommitNotificationHub::new(None, runtime),
        );
        let (cancelled, cancellation) =
            RequestControl::new(Instant::now() + Duration::from_secs(30));
        cancellation.cancel();
        assert!(matches!(
            port.reserve_health(&cancelled).await,
            Err(PortAdmissionError::Cancelled)
        ));
        assert!(matches!(
            port.reserve_statistics(&cancelled).await,
            Err(PortAdmissionError::Cancelled)
        ));

        let (expired, cancellation) = RequestControl::new(Instant::now());
        cancellation.cancel();
        assert!(matches!(
            port.reserve_health(&expired).await,
            Err(PortAdmissionError::Cancelled)
        ));

        let expired = RequestControl::new(Instant::now()).0;
        assert!(matches!(
            port.reserve_health(&expired).await,
            Err(PortAdmissionError::DeadlineExceeded)
        ));
        assert!(matches!(
            port.reserve_statistics(&expired).await,
            Err(PortAdmissionError::DeadlineExceeded)
        ));
    }

    #[test]
    fn debug_output_exposes_no_cached_state() {
        let runtime = RuntimeRoutingState::new();
        let port = ProductionOperationalStatusPort::new(
            ValidatedAllocatorCapacity::BothExhausted,
            runtime.clone(),
            FirstCommitNotificationHub::new(None, runtime),
        );
        assert_eq!(
            format!("{port:?}"),
            "ProductionOperationalStatusPort([REDACTED])"
        );
    }
}
