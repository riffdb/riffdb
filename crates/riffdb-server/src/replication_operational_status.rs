//! Primary replication observations run on the bounded read workers, never on
//! the async caller or an authoritative write path.
use super::*;
use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::replication_publication::ReplicationPublishedSnapshots;
use riffdb_service::ReplicationStatistics;

pub(crate) struct ReplicationOperationalStatus {
    health: BlockingPortExecutor<(), OperationalHealthSnapshot, OperationalStatusError>,
    statistics: BlockingPortExecutor<(), OperationalStatisticsSnapshot, OperationalStatusError>,
}
impl ProductionOperationalStatusPort {
    pub(crate) fn with_replication(
        self,
        publications: Option<ReplicationPublishedSnapshots>,
        driver: &BlockingPortDriver,
    ) -> ReplicationOperationalStatus {
        let base = self.clone();
        let source = publications.clone();
        let health = driver.executor(move |()| {
            let snapshot = required_health_snapshot(
                base.allocator_capacity,
                base.runtime.stop_reason(),
                base.outbox.readiness(),
                base.projection.readiness(),
                base.columnar.readiness(),
                base.vector_storage.as_ref().map_or(
                    VectorStalenessReadiness::NotApplicable,
                    vector_staleness_readiness,
                ),
            )?;
            let mut components = snapshot.into_components();
            let replication = if base.runtime.stop_reason().is_some() {
                Err(OperationalStatusError::Unavailable)
            } else {
                observe(source.as_ref()).and_then(|(progress, degraded)| {
                    ComponentHealth::replication_with_retention_degradation(progress, degraded)
                        .map_err(|_| OperationalStatusError::Integrity)
                })
            };
            components.push(replication.unwrap_or_else(|_| {
                ComponentHealth::new(
                    HealthComponentKind::Replication,
                    HealthComponentStatus::Unavailable,
                )
            }));
            OperationalHealthSnapshot::new(components)
                .map_err(|_| OperationalStatusError::Integrity)
        });
        let statistics = driver.executor(move |()| {
            // Preserve the existing stopped-cache refusal, then use a single
            // durable publication for both local head and replication counters.
            self.notifications
                .latest_sequence()
                .map_err(map_notification_status_error)?;
            let (progress, _) = observe(publications.as_ref())?;
            Ok(OperationalStatisticsSnapshot::new(
                progress.applied_frontier().application(),
                None,
                None,
            )
            .with_replication(progress))
        });
        ReplicationOperationalStatus { health, statistics }
    }
}

fn observe(
    source: Option<&ReplicationPublishedSnapshots>,
) -> Result<(ReplicationStatistics, bool), OperationalStatusError> {
    let mut source = source.cloned().ok_or(OperationalStatusError::Unavailable)?;
    let pin = source
        .latest()
        .map_err(|_| OperationalStatusError::Unavailable)?
        .ok_or(OperationalStatusError::Unavailable)?;
    let progress = pin
        .replication_source_progress_v3()
        .map_err(|error| match error {
            riffdb_storage_api::ChangelogCursorErrorV3::Storage(error)
                if error.kind() == riffdb_storage_api::StorageErrorKind::Unavailable =>
            {
                OperationalStatusError::Unavailable
            }
            _ => OperationalStatusError::Integrity,
        })?;
    ReplicationStatistics::primary(
        progress.history().tail().frontier(),
        progress.follower_count(),
        progress.oldest_acknowledged().map(|point| point.frontier()),
    )
    .map(|statistics| (statistics, progress.degraded_followers() > 0))
    .map_err(|_| OperationalStatusError::Integrity)
}

impl OperationalStatusPort for ReplicationOperationalStatus {
    fn reserve_health<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<(), OperationalHealthSnapshot, OperationalStatusError>,
        PortAdmissionError,
    > {
        Box::pin(self.health.reserve_async(control))
    }
    fn reserve_statistics<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<(), OperationalStatisticsSnapshot, OperationalStatusError>,
        PortAdmissionError,
    > {
        Box::pin(self.statistics.reserve_async(control))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Waker};
    use std::time::{Duration, Instant};

    #[tokio::test]
    // req: REP-004
    async fn replication_operational_reads_wait_for_worker_capacity() {
        let driver = BlockingPortDriver::new(RuntimeRoutingState::new()).unwrap();
        let provider = ReplicationOperationalStatus {
            health: driver.executor(|()| {
                OperationalHealthSnapshot::new(vec![])
                    .map_err(|_| OperationalStatusError::Integrity)
            }),
            statistics: driver
                .executor(|()| Ok(OperationalStatisticsSnapshot::new(None, None, None))),
        };
        let (control, _) = RequestControl::new(Instant::now() + Duration::from_secs(30));
        for statistics in [false, true] {
            let mut held = Vec::new();
            for _ in 0..crate::port_driver::P1_MAX_BLOCKING_PORT_OPERATIONS {
                held.push(provider.health.reserve(&control).unwrap());
            }
            let request = async {
                if statistics {
                    drop(provider.reserve_statistics(&control).await?);
                } else {
                    drop(provider.reserve_health(&control).await?);
                }
                Ok::<(), PortAdmissionError>(())
            };
            let mut request = pin!(request);
            let mut context = Context::from_waker(Waker::noop());
            assert!(
                request.as_mut().poll(&mut context).is_pending(),
                "read admission must wait while capacity is held"
            );
            drop(held);
            request.await.unwrap();
        }
        driver.shutdown_and_drain().unwrap();
    }
}
