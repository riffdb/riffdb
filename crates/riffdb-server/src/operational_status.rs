#![expect(
    clippy::unreachable,
    reason = "the closed lifecycle transition table rejects invalid states before status lowering"
)]

//! Cached operational status for the minimal P1 process graph.

#![allow(
    dead_code,
    reason = "WP-130 process composition installs this private provider"
)]

use std::fmt;
#[path = "replication_operational_status.rs"]
mod replication;

use riffdb_catalog::ValidatedContractBundle;
use riffdb_service::{
    BoxPortCapacityPermit, ComponentHealth, HealthComponentKind, HealthComponentStatus,
    OperationalHealthSnapshot, OperationalStatisticsSnapshot, OperationalStatusError,
    OperationalStatusPort, PortAdmissionError, PortCapacityPermit, PortFuture, PortReceipt,
    RequestControl, port_completion_channel,
};
use riffdb_storage_api::{CatalogRepository, VectorObservationRepository};

use crate::columnar_worker::{ColumnarWorkerReadiness, ColumnarWorkerStatus};
use crate::notifications::{FirstCommitNotificationHub, NotificationStatusError};
use crate::outbox_adapter::{NoDestinationOutboxHealth, OutboxDerivedReadiness};
use crate::projection_worker::{ProjectionWorkerReadiness, ProjectionWorkerStatus};
use crate::runtime_support::{RuntimeRoutingState, RuntimeStopReason};
use crate::startup::ValidatedAllocatorCapacity;
use crate::storage::SharedRedbOperationalPorts;

/// Startup recovery state retained separately from authoritative readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboxRecoveryReadiness {
    Ready,
    Degraded,
}

/// Exact aggregate vector-health classification for the active contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VectorStalenessReadiness {
    NotApplicable,
    Healthy,
    Degraded,
    Unavailable,
}

/// Process-local status assembled from checked startup and monotonic routing state.
#[derive(Clone)]
pub(crate) struct ProductionOperationalStatusPort {
    allocator_capacity: ValidatedAllocatorCapacity,
    runtime: RuntimeRoutingState,
    notifications: FirstCommitNotificationHub,
    outbox: NoDestinationOutboxHealth,
    projection: ProjectionWorkerStatus,
    columnar: ColumnarWorkerStatus,
    vector_storage: Option<SharedRedbOperationalPorts>,
}

impl ProductionOperationalStatusPort {
    pub(crate) fn new_with_vector_storage(
        allocator_capacity: ValidatedAllocatorCapacity,
        runtime: RuntimeRoutingState,
        notifications: FirstCommitNotificationHub,
        outbox: NoDestinationOutboxHealth,
        projection: ProjectionWorkerStatus,
        columnar: ColumnarWorkerStatus,
        vector_storage: SharedRedbOperationalPorts,
    ) -> Self {
        Self {
            allocator_capacity,
            runtime,
            notifications,
            outbox,
            projection,
            columnar,
            vector_storage: Some(vector_storage),
        }
    }

    #[cfg(test)]
    fn new(
        allocator_capacity: ValidatedAllocatorCapacity,
        runtime: RuntimeRoutingState,
        notifications: FirstCommitNotificationHub,
        outbox: NoDestinationOutboxHealth,
        projection: ProjectionWorkerStatus,
        columnar: ColumnarWorkerStatus,
    ) -> Self {
        Self {
            allocator_capacity,
            runtime,
            notifications,
            outbox,
            projection,
            columnar,
            vector_storage: None,
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
                outbox: self.outbox.clone(),
                projection: self.projection.clone(),
                columnar: self.columnar.clone(),
                vector_storage: self.vector_storage.clone(),
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
    outbox: NoDestinationOutboxHealth,
    projection: ProjectionWorkerStatus,
    columnar: ColumnarWorkerStatus,
    vector_storage: Option<SharedRedbOperationalPorts>,
}

impl PortCapacityPermit<(), OperationalHealthSnapshot, OperationalStatusError>
    for OperationalHealthPermit
{
    fn submit(
        self: Box<Self>,
        (): (),
    ) -> Result<PortReceipt<OperationalHealthSnapshot, OperationalStatusError>, PortAdmissionError>
    {
        let result = required_health_snapshot(
            self.allocator_capacity,
            self.runtime.stop_reason(),
            self.outbox.readiness(),
            self.projection.readiness(),
            self.columnar.readiness(),
            self.vector_storage.as_ref().map_or(
                VectorStalenessReadiness::NotApplicable,
                vector_staleness_readiness,
            ),
        );
        let (completion, receipt) = port_completion_channel();
        completion.complete(result);
        Ok(receipt)
    }
}

fn required_health_snapshot(
    allocator_capacity: ValidatedAllocatorCapacity,
    runtime_stop: Option<RuntimeStopReason>,
    outbox: OutboxDerivedReadiness,
    projection: ProjectionWorkerReadiness,
    columnar: ColumnarWorkerReadiness,
    vector_staleness: VectorStalenessReadiness,
) -> Result<OperationalHealthSnapshot, OperationalStatusError> {
    let runtime_running = runtime_stop.is_none();
    let storage_status = required_component_status(runtime_running);
    let catalog_status = required_component_status(runtime_running);
    let coordinator_status = required_component_status(
        runtime_running && allocator_capacity == ValidatedAllocatorCapacity::Available,
    );
    let outbox_status = if !runtime_running {
        HealthComponentStatus::Unavailable
    } else if outbox == OutboxDerivedReadiness::Ready {
        HealthComponentStatus::Healthy
    } else {
        HealthComponentStatus::Degraded
    };
    // The Projection component reports the worst of the two projection-family
    // workers: the durable projection worker and the columnar apply worker.
    let projection_status = match (runtime_running, projection) {
        (false, _) | (true, ProjectionWorkerReadiness::Stopped) => {
            HealthComponentStatus::Unavailable
        }
        (true, ProjectionWorkerReadiness::Ready) => HealthComponentStatus::Healthy,
        (true, ProjectionWorkerReadiness::Starting | ProjectionWorkerReadiness::Degraded) => {
            HealthComponentStatus::Degraded
        }
    };
    let columnar_status = match (runtime_running, columnar) {
        (false, _) | (true, ColumnarWorkerReadiness::Stopped) => HealthComponentStatus::Unavailable,
        (true, ColumnarWorkerReadiness::Ready) => HealthComponentStatus::Healthy,
        (true, ColumnarWorkerReadiness::Starting | ColumnarWorkerReadiness::Degraded) => {
            HealthComponentStatus::Degraded
        }
    };
    let projection_status = worst_component_status(projection_status, columnar_status);

    let mut components = vec![
        ComponentHealth::new(HealthComponentKind::AuthoritativeStorage, storage_status),
        ComponentHealth::new(HealthComponentKind::Catalog, catalog_status),
        ComponentHealth::new(HealthComponentKind::CommitCoordinator, coordinator_status),
        ComponentHealth::new(HealthComponentKind::Outbox, outbox_status),
        ComponentHealth::new(HealthComponentKind::Projection, projection_status),
    ];
    if vector_staleness != VectorStalenessReadiness::NotApplicable {
        let status = match (runtime_running, vector_staleness) {
            (false, _) | (true, VectorStalenessReadiness::Unavailable) => {
                HealthComponentStatus::Unavailable
            }
            (true, VectorStalenessReadiness::Degraded) => HealthComponentStatus::Degraded,
            (true, VectorStalenessReadiness::Healthy) => {
                if projection_status == HealthComponentStatus::Healthy {
                    HealthComponentStatus::Healthy
                } else {
                    HealthComponentStatus::Degraded
                }
            }
            (true, VectorStalenessReadiness::NotApplicable) => unreachable!(),
        };
        components.push(ComponentHealth::new(
            HealthComponentKind::VectorStaleness,
            status,
        ));
    }
    OperationalHealthSnapshot::new(components).map_err(|_| OperationalStatusError::Integrity)
}

/// Resolves the active vector contract and its maintained global observation
/// through exact keyed reads only. Any missing, stale, malformed, or
/// inconsistent proof fails closed as `Unavailable`; no probe scans data.
fn vector_staleness_readiness(storage: &SharedRedbOperationalPorts) -> VectorStalenessReadiness {
    let Ok(Some(active)) = CatalogRepository::read_active_catalog(storage) else {
        return VectorStalenessReadiness::NotApplicable;
    };
    let Ok(Some(stored)) = CatalogRepository::read_contract_bundle(
        storage,
        active.lineage(),
        active.contract_version(),
    ) else {
        return VectorStalenessReadiness::Unavailable;
    };
    let Ok(bundle) = ValidatedContractBundle::from_stored(&stored) else {
        return VectorStalenessReadiness::Unavailable;
    };
    let specs = bundle.bundle().schema().vector_field_specs();
    if specs.is_empty() {
        return VectorStalenessReadiness::NotApplicable;
    }
    let Ok(Some(observation)) =
        VectorObservationRepository::read_vector_health_observation(storage, active.lineage())
    else {
        return VectorStalenessReadiness::Unavailable;
    };
    if observation.lineage() != active.lineage() {
        return VectorStalenessReadiness::Unavailable;
    }
    // Absent field summaries are canonical zero-partition observations. A
    // present summary must name an exact active spec; an extra or drifted
    // threshold fails closed.
    let exact = observation.fields().all(|field| {
        specs
            .binary_search_by(|spec| {
                spec.entity()
                    .cmp(&field.entity_type())
                    .then_with(|| spec.field().cmp(&field.vector_field()))
            })
            .ok()
            .and_then(|index| specs.get(index))
            .is_some_and(|spec| {
                field.stale_entity_count_threshold() == spec.stale_entity_count_threshold()
            })
    });
    if !exact {
        VectorStalenessReadiness::Unavailable
    } else if observation.any_partition_breached() {
        VectorStalenessReadiness::Degraded
    } else {
        VectorStalenessReadiness::Healthy
    }
}

const fn required_component_status(healthy: bool) -> HealthComponentStatus {
    if healthy {
        HealthComponentStatus::Healthy
    } else {
        HealthComponentStatus::Unavailable
    }
}

const fn worst_component_status(
    left: HealthComponentStatus,
    right: HealthComponentStatus,
) -> HealthComponentStatus {
    match (left, right) {
        (HealthComponentStatus::Unavailable, _) | (_, HealthComponentStatus::Unavailable) => {
            HealthComponentStatus::Unavailable
        }
        (HealthComponentStatus::Degraded, _) | (_, HealthComponentStatus::Degraded) => {
            HealthComponentStatus::Degraded
        }
        _ => HealthComponentStatus::Healthy,
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

    fn ready_projection() -> ProjectionWorkerStatus {
        let status = ProjectionWorkerStatus::new();
        status.publish(ProjectionWorkerReadiness::Ready);
        status
    }

    fn ready_outbox() -> NoDestinationOutboxHealth {
        NoDestinationOutboxHealth::new(OutboxRecoveryReadiness::Ready)
    }

    fn ready_columnar() -> ColumnarWorkerStatus {
        let status = ColumnarWorkerStatus::new();
        status.publish(ColumnarWorkerReadiness::Ready);
        status
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
            let snapshot = required_health_snapshot(
                capacity,
                None,
                OutboxDerivedReadiness::Ready,
                ProjectionWorkerReadiness::Ready,
                ColumnarWorkerReadiness::Ready,
                VectorStalenessReadiness::NotApplicable,
            )
            .expect("fixed health shape");
            assert_eq!(snapshot.components().len(), 5);
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
                snapshot.components()[3].component(),
                HealthComponentKind::Projection
            );
            assert_eq!(
                snapshot.components()[4].component(),
                HealthComponentKind::Outbox
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
            assert_eq!(
                status(&snapshot, HealthComponentKind::Outbox),
                HealthComponentStatus::Healthy
            );
            assert_eq!(
                status(&snapshot, HealthComponentKind::Projection),
                HealthComponentStatus::Healthy
            );
            assert!(
                snapshot
                    .components()
                    .iter()
                    .all(|component| component.component() != HealthComponentKind::VectorStaleness),
                "vector staleness stays absent until an authoritative observer is wired"
            );
        }
    }

    #[test]
    fn vector_free_application_reports_ready_without_a_staleness_observer() {
        use riffdb_service::{BuildInfo, HealthReport, HealthStatus};
        use riffdb_types::{CommitSequence, ContractVersion, Timestamp};

        let snapshot = required_health_snapshot(
            ValidatedAllocatorCapacity::Available,
            None,
            OutboxDerivedReadiness::Ready,
            ProjectionWorkerReadiness::Ready,
            ColumnarWorkerReadiness::Ready,
            VectorStalenessReadiness::NotApplicable,
        )
        .expect("vector-free health snapshot");
        assert!(
            snapshot
                .components()
                .iter()
                .all(|component| component.component() != HealthComponentKind::VectorStaleness)
        );

        let report = HealthReport::new(
            Some(ContractVersion::new(1).expect("active application")),
            Some(CommitSequence::first()),
            snapshot,
            Timestamp::new(1, 0).expect("process start"),
            BuildInfo::new("0.1.0", "test", "1.97.0", vec![], 1, 1, "2025-11-25")
                .expect("build metadata"),
        );
        assert_eq!(report.status(), HealthStatus::Ready);
    }

    #[test]
    fn vector_health_is_present_and_fails_closed_by_observer_state() {
        for (readiness, expected) in [
            (
                VectorStalenessReadiness::Healthy,
                HealthComponentStatus::Healthy,
            ),
            (
                VectorStalenessReadiness::Degraded,
                HealthComponentStatus::Degraded,
            ),
            (
                VectorStalenessReadiness::Unavailable,
                HealthComponentStatus::Unavailable,
            ),
        ] {
            let snapshot = required_health_snapshot(
                ValidatedAllocatorCapacity::Available,
                None,
                OutboxDerivedReadiness::Ready,
                ProjectionWorkerReadiness::Ready,
                ColumnarWorkerReadiness::Ready,
                readiness,
            )
            .expect("vector health snapshot");
            assert_eq!(snapshot.components().len(), 6);
            assert_eq!(
                status(&snapshot, HealthComponentKind::VectorStaleness),
                expected
            );
        }

        let snapshot = required_health_snapshot(
            ValidatedAllocatorCapacity::Available,
            None,
            OutboxDerivedReadiness::Ready,
            ProjectionWorkerReadiness::Degraded,
            ColumnarWorkerReadiness::Ready,
            VectorStalenessReadiness::Healthy,
        )
        .expect("projection-degraded vector health snapshot");
        assert_eq!(
            status(&snapshot, HealthComponentKind::VectorStaleness),
            HealthComponentStatus::Degraded
        );
    }

    /// Falsifiability: drop the `worst_component_status` fold (report only the
    /// durable projection worker) and every non-Ready columnar row below fails.
    #[test]
    fn columnar_worker_readiness_folds_into_the_projection_component() {
        for (columnar, expected) in [
            (
                ColumnarWorkerReadiness::Ready,
                HealthComponentStatus::Healthy,
            ),
            (
                ColumnarWorkerReadiness::Starting,
                HealthComponentStatus::Degraded,
            ),
            (
                ColumnarWorkerReadiness::Degraded,
                HealthComponentStatus::Degraded,
            ),
            (
                ColumnarWorkerReadiness::Stopped,
                HealthComponentStatus::Unavailable,
            ),
        ] {
            let snapshot = required_health_snapshot(
                ValidatedAllocatorCapacity::Available,
                None,
                OutboxDerivedReadiness::Ready,
                ProjectionWorkerReadiness::Ready,
                columnar,
                VectorStalenessReadiness::NotApplicable,
            )
            .expect("fixed health shape");
            assert_eq!(
                status(&snapshot, HealthComponentKind::Projection),
                expected,
                "columnar readiness {columnar:?} must fold into the Projection component"
            );
            assert_eq!(
                status(&snapshot, HealthComponentKind::AuthoritativeStorage),
                HealthComponentStatus::Healthy,
                "the fold stays confined to the Projection component"
            );
        }
        // The fold is worst-of in both directions.
        let snapshot = required_health_snapshot(
            ValidatedAllocatorCapacity::Available,
            None,
            OutboxDerivedReadiness::Ready,
            ProjectionWorkerReadiness::Degraded,
            ColumnarWorkerReadiness::Ready,
            VectorStalenessReadiness::NotApplicable,
        )
        .expect("fixed health shape");
        assert_eq!(
            status(&snapshot, HealthComponentKind::Projection),
            HealthComponentStatus::Degraded
        );
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
            let snapshot = required_health_snapshot(
                ValidatedAllocatorCapacity::Available,
                Some(reason),
                OutboxDerivedReadiness::Ready,
                ProjectionWorkerReadiness::Ready,
                ColumnarWorkerReadiness::Ready,
                VectorStalenessReadiness::NotApplicable,
            )
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
            ready_outbox(),
            ready_projection(),
            ready_columnar(),
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
            ready_outbox(),
            ready_projection(),
            ready_columnar(),
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
            ready_outbox(),
            ready_projection(),
            ready_columnar(),
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
            ready_outbox(),
            ready_projection(),
            ready_columnar(),
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
            ready_outbox(),
            ready_projection(),
            ready_columnar(),
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
            ready_outbox(),
            ready_projection(),
            ready_columnar(),
        );
        assert_eq!(
            format!("{port:?}"),
            "ProductionOperationalStatusPort([REDACTED])"
        );
    }
}
