//! Production statistics must report real committed delivery membership.
use super::*;
use crate::replication_publication::ReplicationPublication;
use riffdb_storage_redb::{RedbCommitProfile, RedbStore};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::replication_bootstrap::projection_recovery_tests::command_support::{
    BulkRowsDatabase, FixedAdmissionClock, IncrementingProvenanceSource, command_timestamp,
    open_operational, start_coordinator_with_notifications,
};

// req: END-007, END-008, EFF-002, EFF-003
#[tokio::test]
async fn production_statistics_report_committed_events_without_delivery_or_population_scan() {
    let database = BulkRowsDatabase::create("native-statistics");
    let ports = database.open();
    let preparation = database.prepare_put(&ports, &[[0x41; 16], [0x42; 16]], 0x43, 0x44, 0x45);
    let runtime = RuntimeRoutingState::new();
    let notifications = FirstCommitNotificationHub::new(None, runtime.clone());
    let coordinator = start_coordinator_with_notifications(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(0x46)),
        Arc::new(notifications.clone()),
    );
    let executor = coordinator.command_executor();
    let result = executor
        .reserve_capacity()
        .await
        .unwrap()
        .submit(preparation)
        .unwrap()
        .completion()
        .await
        .unwrap();
    assert!(matches!(
        result,
        riffdb_commit::CommandExecutionResult::Committed(_)
    ));
    drop(executor);
    coordinator.shutdown().unwrap();
    let (publisher, publications) = ReplicationPublication::channel();
    let ports = open_operational(
        RedbStore::open_with_changelog_publication_port(
            database.path(),
            RedbCommitProfile::Standard,
            publisher,
        )
        .expect("reopen native publication source"),
    );
    let shared = SharedRedbOperationalPorts::new(ports, None).expect("shared native source");
    let driver = BlockingPortDriver::new(runtime.clone()).expect("read workers");
    let projection = ProjectionWorkerStatus::new();
    projection.publish(ProjectionWorkerReadiness::Ready);
    let columnar = ColumnarWorkerStatus::new();
    columnar.publish(ColumnarWorkerReadiness::Ready);
    let outbox = NoDestinationOutboxHealth::new(OutboxRecoveryReadiness::Ready);
    let provider = ProductionOperationalStatusPort::new_with_vector_storage(
        ValidatedAllocatorCapacity::Available,
        runtime,
        notifications.clone(),
        outbox.clone(),
        projection,
        columnar,
        shared.clone(),
    )
    .with_replication(Some(publications), &driver);
    let (control, _) = RequestControl::new(Instant::now() + Duration::from_secs(30));
    let cold = provider
        .reserve_statistics(&control)
        .await
        .unwrap()
        .submit(())
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cold.pending_outbox_deliveries(), None);
    assert_eq!(
        shared.pending_outbox_delivery_count().unwrap_err().kind(),
        riffdb_storage_api::StorageErrorKind::Unavailable
    );
    // This is the existing production graph/worker recovery observation, not
    // part of a statistics request. It validates the disposable membership.
    outbox.refresh(&shared);
    assert_eq!(outbox.readiness(), OutboxDerivedReadiness::Degraded);
    for _ in 0..2 {
        let snapshot = provider
            .reserve_statistics(&control)
            .await
            .unwrap()
            .submit(())
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            snapshot
                .last_commit_sequence()
                .map(|sequence| sequence.get()),
            Some(1)
        );
        assert_eq!(snapshot.pending_outbox_deliveries(), Some(2));
        assert_eq!(
            shared.pending_outbox_delivery_count().unwrap(),
            2,
            "observation must not claim or deliver either event"
        );
    }
    driver.shutdown_and_drain().unwrap();
}
