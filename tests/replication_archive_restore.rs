#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]
//! Production collection and public restore, with an explicit durable-frame signal.
// req: REP-007, AFC-007
#[path = "replication_archive_restore/cli.rs"]
mod cli;
#[path = "replication_archive_restore/evidence.rs"]
mod evidence;
#[allow(dead_code)]
#[path = "replication_follower/support.rs"]
mod support;
#[path = "replication_archive_restore/workload.rs"]
mod workload;
use evidence::Damage;

#[derive(Clone, Copy, Eq, PartialEq)]
enum CollectorFault {
    None,
    Crash,
    LoseSink,
}

use riffdb_client_rust::{
    ArchiveNameV1, ArchiveRestoreStopV1, AttemptBudget, BackupNameV1, CallMetadata,
    CreateOfflineBackup, OfflineMaintenanceReplacementConfirmation, RestoreArchivedBackup,
    RiffDbClient, generate_offline_maintenance_operation_id, generate_request_id, v1,
};
use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};
use std::{fs, time::Duration};
use support::{Fixture, seed_primary, stop};

const TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN: [&str; 12] = [
    "riffdb-write-completion-groups-v1\t",
    "riffdb-dispatch-reasons-v1\t",
    "riffdb-shutdown-stages-v1\t",
    "riffdb-read-stages-v1\t",
    "riffdb-write-service-stages-v1\t",
    "riffdb-command-stages-v1\t",
    "riffdb-writer-evidence-v1\t",
    "riffdb-completion-lane-v1\t",
    "riffdb-writer-frame-census-v1\t",
    "riffdb-writer-flush-census-v1\t",
    "riffdb-writer-journal-stages-v1\t",
    "riffdb-writer-publication-stages-v1\t",
];
fn rebound(process: &ChildProcessController) {
    process
        .wait_for_evidence_then_readiness(&SHUTDOWN, "riffdbd-ready-v1\t", TIMEOUT)
        .unwrap();
}
async fn terminal(
    client: &mut RiffDbClient,
    id: riffdb_types::OfflineMaintenanceOperationId,
    metadata: &CallMetadata,
) -> v1::OfflineMaintenanceOperation {
    let response = client
        .get_offline_maintenance_operation(
            v1::GetOfflineMaintenanceOperationRequest {
                request_id: generate_request_id().unwrap().into_bytes().to_vec(),
                operation_id: id.into_bytes().to_vec(),
            },
            metadata,
        )
        .await
        .unwrap();
    let Some(v1::get_offline_maintenance_operation_response::Result::Found(operation)) =
        response.result
    else {
        panic!("terminal maintenance receipt missing")
    };
    assert_eq!(
        operation.phase,
        v1::OfflineMaintenancePhase::Succeeded as i32,
        "{operation:?}"
    );
    operation
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archived_suffix_restore_recovers_to_last_archived_sequence() {
    scenario(
        ArchiveRestoreStopV1::LastArchived,
        3,
        Damage::None,
        CollectorFault::None,
        false,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archived_suffix_restore_stops_at_an_earlier_application_sequence() {
    scenario(
        ArchiveRestoreStopV1::AtApplicationSequence(riffdb_types::CommitSequence::new(2).unwrap()),
        2,
        Damage::None,
        CollectorFault::None,
        false,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_collector_process_crash_preserves_a_restorable_prefix() {
    scenario(
        ArchiveRestoreStopV1::LastArchived,
        3,
        Damage::None,
        CollectorFault::Crash,
        false,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncated_archive_refuses_without_replacing_application_state() {
    scenario(
        ArchiveRestoreStopV1::LastArchived,
        3,
        Damage::Truncated,
        CollectorFault::None,
        false,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reordered_archive_refuses_without_replacing_application_state() {
    scenario(
        ArchiveRestoreStopV1::LastArchived,
        3,
        Damage::Reordered,
        CollectorFault::None,
        false,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real CLI binary; scripts/check-archive-cli runs this in ci-all"]
async fn cli_archive_restore_recovers_last_archived_and_polls_exact_operation() {
    scenario(
        ArchiveRestoreStopV1::LastArchived,
        3,
        Damage::None,
        CollectorFault::None,
        true,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real CLI binary; scripts/check-archive-cli runs this in ci-all"]
async fn cli_archive_restore_honors_the_explicit_earlier_stop() {
    scenario(
        ArchiveRestoreStopV1::AtApplicationSequence(riffdb_types::CommitSequence::new(2).unwrap()),
        2,
        Damage::None,
        CollectorFault::None,
        true,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_sink_loss_keeps_primary_writable_and_confirmed_prefix_restorable() {
    scenario(
        ArchiveRestoreStopV1::LastArchived,
        3,
        Damage::None,
        CollectorFault::LoseSink,
        false,
    )
    .await;
}
async fn scenario(
    stop_at: ArchiveRestoreStopV1,
    expected_sequence: u64,
    damage: Damage,
    fault: CollectorFault,
    use_cli: bool,
) {
    let fixture = Fixture::new();
    let mut process = fixture.start("primary", None);
    stop(&mut process);
    let (_, admin, token) = seed_primary(&fixture.database("primary"));
    process = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let writer = workload::prepare(&mut client, &admin).await;
    let baseline = CreateOfflineBackup::new(
        generate_offline_maintenance_operation_id().unwrap(),
        BackupNameV1::new("baseline").unwrap(),
    );
    client
        .create_offline_backup_with_retry(&baseline, AttemptBudget::new(1).unwrap(), &admin)
        .await
        .unwrap();
    drop(client);
    rebound(&process);
    let mut client = fixture.client("primary").await;
    terminal(&mut client, baseline.operation_id(), &admin).await;
    drop(client);
    stop(&mut process);

    let database = fixture.database("primary");
    let root = database.parent().unwrap();
    let config = root.join("primary.toml");
    let original = fs::read_to_string(&config).unwrap();
    let binding = format!(
        "\n[[maintenance.archives]]\nname = 'daily'\npath = '{}'\nencryption = 'unencrypted'\n",
        root.join("archive").display()
    );
    fs::write(&config, format!("{original}{binding}backup = 'baseline'\n")).unwrap();
    let spec = ChildProcessSpec::new(env!("CARGO_BIN_EXE_riffdbd-archive-fixture"))
        .unwrap()
        .clear_environment()
        .arg("--config")
        .unwrap()
        .arg(&config)
        .unwrap();
    process = ChildProcessController::spawn(&spec).unwrap();
    process
        .wait_for_readiness("riffdbd-ready-v1\t", TIMEOUT)
        .unwrap();
    let mut client = fixture.client("primary").await;
    let second = workload::allocate(&mut client, &writer, 2).await;
    let entity_second = workload::entity(&mut client, &writer).await;
    if damage != Damage::None {
        // Corruption/reordering requires two distinct durable frame files.
        // Coalescing may otherwise collect both commands in one valid frame.
        process
            .wait_for_readiness("riffdb-archive-collected-v1\tcommit=2", TIMEOUT)
            .unwrap();
    }
    let third = workload::allocate(&mut client, &writer, 3).await;
    let entity_third = workload::entity(&mut client, &writer).await;
    if damage == Damage::None {
        // Consume the ordered observation without forcing separate frames in
        // the ordinary restore, crash, sink-loss and CLI scenarios.
        process
            .wait_for_readiness("riffdb-archive-collected-v1\tcommit=2", TIMEOUT)
            .unwrap();
    }
    process
        .wait_for_readiness("riffdb-archive-collected-v1\tcommit=3", TIMEOUT)
        .unwrap();
    let source_sequence = if fault == CollectorFault::LoseSink {
        // The retained descriptor cannot make a withdrawn configured path valid.
        fs::rename(root.join("archive"), root.join("withdrawn-archive")).unwrap();
        workload::allocate(&mut client, &writer, 4).await;
        process
            .wait_for_readiness("riffdb-archive-failed-v1\tclass=sink-unavailable", TIMEOUT)
            .unwrap();
        // A further acknowledged write proves the stopped collector is not a write gate.
        workload::allocate(&mut client, &writer, 5).await;
        assert_ne!(workload::entity(&mut client, &writer).await, entity_third);
        5
    } else {
        3
    };
    drop(client);
    if fault == CollectorFault::Crash {
        assert!(!process.kill(TIMEOUT).unwrap().status.success());
    } else {
        stop(&mut process);
    }
    if fault == CollectorFault::LoseSink {
        fs::rename(root.join("withdrawn-archive"), root.join("archive")).unwrap();
    }
    let archived_application = evidence::application(&database);
    assert_eq!(archived_application.sequence, source_sequence);
    evidence::damage(&root.join("archive"), damage);

    // Freeze this archive, then make a real source change that restore must lose.
    fs::write(&config, format!("{original}{binding}")).unwrap();
    process = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    workload::allocate(&mut client, &writer, source_sequence + 1).await;
    assert_ne!(workload::entity(&mut client, &writer).await, entity_third);
    let before_refusal = if damage != Damage::None {
        drop(client);
        stop(&mut process);
        let observed = evidence::application(&database);
        assert_eq!(observed.sequence, source_sequence + 1);
        process = fixture.start("primary", None);
        client = fixture.client("primary").await;
        Some(observed)
    } else {
        None
    };
    let cli = use_cli.then(|| cli::Cli::new(root, &original, &token));
    let operation = match &cli {
        Some(cli) => cli.restore(stop_at),
        None => generate_offline_maintenance_operation_id().unwrap(),
    };
    let restore = RestoreArchivedBackup::new(
        operation,
        BackupNameV1::new("baseline").unwrap(),
        ArchiveNameV1::new("daily").unwrap(),
        stop_at,
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
    );
    if cli.is_none() {
        client
            .restore_archived_backup_with_retry(&restore, AttemptBudget::new(1).unwrap(), &admin)
            .await
            .unwrap();
    }
    drop(client);
    if let Some(before) = before_refusal {
        assert!(!process.wait_for_exit(TIMEOUT).unwrap().status.success());
        assert_eq!(evidence::application(&database), before);
        use riffdb_storage_api::OfflineArchiveReceiptPersistencePort;
        let (mut storage, _) = riffdb_storage_redb::RedbMaintenanceStorage::open(
            &database,
            root.join("primary-backups"),
        )
        .unwrap();
        let receipt = storage
            .read_archive_receipt(restore.operation_id())
            .unwrap()
            .unwrap();
        assert_eq!(
            receipt.current_phase(),
            riffdb_storage_api::OfflineMaintenanceReceiptPhaseV1::FailedClosed
        );
        assert!(receipt.selection().is_none());
        assert!(receipt.published_history_incarnation().is_none());
        return;
    }
    rebound(&process);
    let mut client = fixture.client("primary").await;
    let receipt = terminal(&mut client, restore.operation_id(), &admin).await;
    if let Some(cli) = &cli {
        cli.terminal(restore.operation_id(), expected_sequence, stop_at);
    }
    let restored = receipt
        .archive_restore
        .as_ref()
        .unwrap()
        .restored_frontier
        .as_ref()
        .unwrap();
    assert_eq!(
        restored.application.as_ref().unwrap().position,
        Some(v1::frontier_position::Position::AppliedThrough(
            expected_sequence
        ))
    );
    let expected_entity = if expected_sequence == 2 {
        entity_second
    } else {
        entity_third
    };
    assert_eq!(
        workload::entity(&mut client, &writer).await,
        expected_entity
    );
    let replay = workload::allocate(&mut client, &writer, expected_sequence).await;
    let original_outcome = if expected_sequence == 2 {
        second
    } else {
        third
    };
    assert_eq!(replay.commit_sequence, expected_sequence);
    assert_eq!(replay.outcome, original_outcome.outcome);
    assert_eq!(replay.outcome_uri, original_outcome.outcome_uri);
    assert_eq!(replay.provenance_uri, original_outcome.provenance_uri);
    assert_eq!(
        replay.status,
        v1::execute_command_response::CompletionStatus::Replayed as i32
    );
    let retry = client
        .restore_archived_backup_with_retry(&restore, AttemptBudget::new(1).unwrap(), &admin)
        .await
        .unwrap();
    assert_eq!(retry.operation, Some(receipt));
    drop(client);
    stop(&mut process);
    let restored_application = evidence::application(&database);
    assert_eq!(restored_application.sequence, expected_sequence);
    assert!(restored_application.incarnation > archived_application.incarnation);
    if expected_sequence == archived_application.sequence {
        assert_eq!(restored_application.rows, archived_application.rows);
    }
}
