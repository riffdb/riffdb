#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]
//! ADR-0203 public offline storage scrub through the production daemon.
#[allow(dead_code)]
#[path = "../replication_follower/support.rs"]
mod support;

use riffdb_client_rust::{
    AttemptBudget, BackupNameV1, CallMetadata, CreateOfflineBackup, RiffDbClient,
    StartOfflineStorageScrub, generate_offline_maintenance_operation_id, generate_request_id, v1,
};
use riffdb_testkit_server::process::ChildProcessController;
use riffdb_types::OfflineMaintenanceOperationId;
use std::{fs, path::Path, time::Duration};
use support::{Fixture, seed_primary, stop};

const TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN: [&str; 13] = [
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
    "riffdb-writer-publication-stages-v2\t",
    "riffdb-writer-checkpoint-census-v1\t",
];

async fn observe(
    client: &mut RiffDbClient,
    id: OfflineMaintenanceOperationId,
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
        panic!("scrub receipt missing")
    };
    operation
}

/// Asserts the selector-free observation shape: fixed kind and input
/// identity, and no backup name, archive detail, or other selector.
fn assert_scrub_shape(
    operation: &v1::OfflineMaintenanceOperation,
    id: OfflineMaintenanceOperationId,
) {
    assert_eq!(operation.operation_id, id.into_bytes().to_vec());
    assert_eq!(
        operation.kind,
        v1::OfflineMaintenanceOperationKind::StorageScrub as i32
    );
    assert!(operation.backup_name.is_empty());
    assert!(operation.archive_restore.is_none());
    assert_eq!(
        operation.input_hash,
        riffdb_types::storage_scrub_input_hash().as_bytes().to_vec()
    );
}

fn scrub_receipt_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.to_string_lossy().ends_with(".receipt-v4") {
                found.push(path);
            }
        }
    }
    found
}

async fn start_scrub(
    fixture: &Fixture,
    admin: &CallMetadata,
) -> (
    OfflineMaintenanceOperationId,
    v1::StartOfflineStorageScrubResponse,
) {
    let mut client = fixture.client("primary").await;
    let scrub = StartOfflineStorageScrub::new(generate_offline_maintenance_operation_id().unwrap());
    let response = client
        .start_offline_storage_scrub_with_retry(&scrub, AttemptBudget::new(1).unwrap(), admin)
        .await
        .unwrap();
    (scrub.operation_id(), response)
}

/// Seeds the bootstrap capability and deploys one contract, the Ready stage
/// that admits offline maintenance.
async fn seeded(fixture: &Fixture) -> (ChildProcessController, CallMetadata) {
    let mut process = fixture.start("primary", None);
    stop(&mut process);
    let (_, admin, _) = seed_primary(&fixture.database("primary"));
    let process = fixture.start("primary", None);
    let deployed = fixture
        .client("primary")
        .await
        .deploy_contract(
            v1::DeployContractRequest {
                request_id: generate_request_id().unwrap().into_bytes().to_vec(),
                source: include_str!("../../contracts/examples/budget.riff").into(),
                ..Default::default()
            },
            &admin,
        )
        .await
        .unwrap();
    assert!(matches!(
        deployed.result,
        Some(v1::deploy_contract_response::Result::Activated(_))
    ));
    (process, admin)
}

// req: REC-004, REC-008
/// OBL-0203-1 proof. The selector-free public scrub is admitted through the
/// authorized maintenance service, drives the database offline through the
/// complete exact-end validator, persists a bounded V4 receipt whose only
/// success sequence ends in `Succeeded`, and returns to ordinary readiness.
/// Exact retries resolve the stored terminal observation; reusing the
/// identity for another kind is the stable input mismatch. A defect that
/// bounded startup admits but only the complete pass reaches fails the scrub
/// closed with `ValidationFailed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_offline_scrub_drives_the_complete_validation_path() {
    let fixture = Fixture::new();
    let (mut process, admin) = seeded(&fixture).await;
    let root = fixture.database("primary").parent().unwrap().to_path_buf();

    let (id, accepted) = start_scrub(&fixture, &admin).await;
    assert_eq!(
        accepted.disposition,
        v1::OfflineMaintenanceStartDisposition::Accepted as i32
    );
    let operation = accepted.operation.expect("accepted observation");
    assert_scrub_shape(&operation, id);
    assert_eq!(
        operation.phase,
        v1::OfflineMaintenancePhase::Draining as i32
    );

    // The daemon drains, validates offline, and reopens to readiness.
    process
        .wait_for_evidence_then_readiness(&SHUTDOWN, "riffdbd-ready-v1\t", TIMEOUT)
        .unwrap();
    let mut client = fixture.client("primary").await;
    let terminal = observe(&mut client, id, &admin).await;
    assert_scrub_shape(&terminal, id);
    assert_eq!(
        terminal.phase,
        v1::OfflineMaintenancePhase::Succeeded as i32
    );
    assert_eq!(
        terminal.failure,
        v1::OfflineMaintenanceFailureClass::Unspecified as i32
    );
    assert_eq!(scrub_receipt_files(&root).len(), 1, "one V4 receipt");

    // An exact retry resolves the stored terminal observation, no new run.
    let retry = client
        .start_offline_storage_scrub_with_retry(
            &StartOfflineStorageScrub::new(id),
            AttemptBudget::new(1).unwrap(),
            &admin,
        )
        .await
        .unwrap();
    assert_eq!(
        retry.disposition,
        v1::OfflineMaintenanceStartDisposition::Terminal as i32
    );
    assert_eq!(retry.operation.as_ref(), Some(&terminal));

    // The identity is the scrub's forever: a backup cannot reuse it.
    let reuse = client
        .create_offline_backup_with_retry(
            &CreateOfflineBackup::new(id, BackupNameV1::new("reuse").unwrap()),
            AttemptBudget::new(1).unwrap(),
            &admin,
        )
        .await
        .expect_err("scrub identity reuse");
    assert_eq!(
        reuse.public_error().map(|error| error.code()),
        Some("idempotency_key_reuse")
    );
    drop(client);
    stop(&mut process);

    // Plant population damage that bounded startup admits; only the complete
    // pass reaches it. The daemon must still reach readiness over it.
    riffdb_storage_redb::plant_malformed_population_row_fixture(&fixture.database("primary"))
        .unwrap();
    let mut process = fixture.start("primary", None);
    let (failed_id, accepted) = start_scrub(&fixture, &admin).await;
    assert_eq!(
        accepted.disposition,
        v1::OfflineMaintenanceStartDisposition::Accepted as i32
    );
    // A failed maintenance run stops the process fail-closed.
    let exit = process.wait_for_exit(TIMEOUT).unwrap();
    assert!(!exit.status.success());

    // The terminal receipt survives; ordinary startup no longer resumes it.
    let _process = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let failed = observe(&mut client, failed_id, &admin).await;
    assert_scrub_shape(&failed, failed_id);
    assert_eq!(
        failed.phase,
        v1::OfflineMaintenancePhase::FailedClosed as i32
    );
    assert_eq!(
        failed.failure,
        v1::OfflineMaintenanceFailureClass::ValidationFailed as i32
    );
    assert_eq!(scrub_receipt_files(&root).len(), 2);
}
