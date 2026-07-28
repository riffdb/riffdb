//! Source-level availability of the WP-155 public maintenance surface.

use riffdb_client_rust::{
    AttemptBudget, BackupNameV1, CallMetadata, CreateOfflineBackup, OfflineMaintenanceOperationId,
    OfflineMaintenanceReplacementConfirmation, RestoreOfflineBackup, RiffDbClient,
    generate_offline_maintenance_operation_id, v1,
};

fn operation_id() -> OfflineMaintenanceOperationId {
    OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [1; 10])
        .expect("operation ID")
}

#[test]
fn checked_templates_and_system_operation_id_source_are_public() {
    let name = BackupNameV1::new("before-upgrade").expect("backup name");
    let create = CreateOfflineBackup::new(operation_id(), name.clone());
    assert_eq!(create.backup_name(), &name);
    let restore = RestoreOfflineBackup::new(
        operation_id(),
        name,
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
    );
    assert_eq!(
        restore.confirmation(),
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget
    );
    let source: fn() -> Result<_, _> = generate_offline_maintenance_operation_id;
    let _ = source;
}

#[allow(dead_code)]
async fn all_maintenance_methods_are_public(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    create: &CreateOfflineBackup,
    restore: &RestoreOfflineBackup,
) {
    let budget = AttemptBudget::new(3).expect("budget");
    let _ = client
        .create_offline_backup_with_retry(create, budget, metadata)
        .await;
    let _ = client
        .restore_offline_backup_with_retry(restore, budget, metadata)
        .await;
    let _ = client
        .get_offline_maintenance_operation(
            v1::GetOfflineMaintenanceOperationRequest {
                request_id: vec![0; 16],
                operation_id: vec![0; 16],
            },
            metadata,
        )
        .await;
}
