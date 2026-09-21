//! Recovery after committed cutover requires exact ledger and full source proof.
// req: REP-005, REC-001, STO-012
use super::*;
use std::sync::{Arc, atomic::AtomicBool};

#[path = "promotion_maintenance_tests.rs"]
mod maintenance_owner;

#[test]
fn committed_promotion_discovery_joins_physical_cutover_and_external_attempt() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-discovery");
    let (path, mut owner, record, _) = fixture(&scope);
    assert!(owner.discover_committed_promotion().unwrap().is_none());
    owner.apply_promotion_cutover(&record).unwrap();
    assert_eq!(
        owner.discover_committed_promotion().unwrap(),
        Some(record.clone())
    );
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts(),
        &[record.attempt().clone()]
    );
    drop(owner);
    std::fs::remove_file(
        scope
            .join("backups/.maintenance/replication_promotion")
            .join(format!("{}.receipt-v1", record.attempt().request_id())),
    )
    .unwrap();
    let owner =
        RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups")).unwrap();
    assert!(owner.discover_committed_promotion().is_err());
}

#[test]
fn committed_promotion_discovery_preserves_ordinary_source_and_absent_target() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-discovery-ordinary");
    let path = scope.join("ordinary.redb");
    let mut source = crate::RedbStore::open(&path).unwrap();
    source
        .initialize_database(DatabaseId::from_unix_milliseconds_and_random(1234, [1; 10]).unwrap())
        .unwrap();
    drop(source);
    let before = database_rows(&path);
    let owner =
        RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups")).unwrap();
    assert!(owner.discover_committed_promotion().unwrap().is_none());
    assert!(owner.promotion_receipts().unwrap().receipts().is_empty());
    assert!(
        database_rows(&path) == before,
        "discovery changed stored rows"
    );
    drop(owner);

    let absent = scope.join("absent.redb");
    let owner =
        RedbMaintenanceStorage::open_for_promotion_recovery(&absent, scope.join("absent-backups"))
            .unwrap();
    assert!(owner.discover_committed_promotion().is_err());
    assert!(!absent.exists());
    assert!(!crate::durable_format_marker_path(&absent).exists());
    assert!(owner.promotion_receipts().unwrap().receipts().is_empty());
}

type TableRows = Vec<(Vec<u8>, Vec<u8>)>;

fn database_rows(path: &std::path::Path) -> std::collections::BTreeMap<String, TableRows> {
    use redb::{ReadableTable, TableHandle};
    // A normal writable redb open can update backend bookkeeping. Compare all
    // logical tables, including metadata, rather than physical file pages.
    let database = redb::ReadOnlyDatabase::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let mut result = std::collections::BTreeMap::new();
    for table in read.list_tables().unwrap() {
        let name = table.name();
        let rows = if name == crate::layout::META.name() {
            read.open_table(crate::layout::META)
                .unwrap()
                .iter()
                .unwrap()
                .map(|entry| {
                    let (key, value) = entry.unwrap();
                    (key.value().as_bytes().to_vec(), value.value().to_vec())
                })
                .collect()
        } else {
            read.open_table(redb::TableDefinition::<&[u8], &[u8]>::new(name))
                .unwrap()
                .iter()
                .unwrap()
                .map(|entry| {
                    let (key, value) = entry.unwrap();
                    (key.value().to_vec(), value.value().to_vec())
                })
                .collect()
        };
        result.insert(name.to_owned(), rows);
    }
    result
}

#[test]
fn committed_promotion_discovery_refuses_incomplete_audit_linkage() {
    for success_index in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("promotion-discovery-audit");
        let (path, mut owner, record, _) = fixture(&scope);
        owner.apply_promotion_cutover(&record).unwrap();
        let database = redb::Database::open(&path).unwrap();
        let write = database.begin_write().unwrap();
        if success_index {
            write
                .open_table(crate::layout::AUDIT_BY_REQUEST)
                .unwrap()
                .remove(
                    crate::keys::encode_audit_by_request_key(
                        record.attempt().request_id(),
                        record.succeeded_sequence(),
                    )
                    .as_slice(),
                )
                .unwrap();
        } else {
            write
                .open_table(crate::layout::AUDIT)
                .unwrap()
                .remove(crate::keys::encode_audit_key(record.started_sequence()).as_slice())
                .unwrap();
        }
        write.commit().unwrap();
        drop(database);
        assert!(owner.discover_committed_promotion().is_err());
        assert_eq!(
            owner.promotion_receipts().unwrap().receipts(),
            &[record.attempt().clone()]
        );
    }
}

fn reconcile(
    owner: &mut RedbMaintenanceStorage,
    record: &StoredPromotionAdministrationV1,
) -> Result<crate::RedbStore, StorageError> {
    owner.reconcile_committed_promotion(
        record,
        inputs(),
        Arc::new(AtomicBool::new(false)),
        crate::RedbCommitProfile::Standard,
        Arc::new(NoChangelogPublicationPort),
    )
}

#[test]
fn committed_promotion_reconciliation_preserves_post_success_source_transactions() {
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let scope = crate::test_path::ScopedDirectory::new("promotion-reconciliation-source");
        let (_, mut owner, record, _) = fixture(&scope);
        owner.apply_promotion_cutover(&record).unwrap();
        let store = owner
            .reconcile_committed_promotion(
                &record,
                inputs(),
                Arc::new(AtomicBool::new(false)),
                profile,
                Arc::new(NoChangelogPublicationPort),
            )
            .unwrap();
        let ports = crate::startup::validate_source_store_fixture(store, inputs());
        let held = ports
            .prepare_replication_bootstrap_v3(
                &scope.join("promoted-transfer"),
                ReplicationSourceHoldIdV1::new([0x74; 16]).unwrap(),
            )
            .unwrap();
        let after = crate::changelog_v3_roots::validate_retained_history(
            &ports.shared.database.begin_read().unwrap(),
        )
        .unwrap()
        .unwrap();
        let original = crate::promotion_cutover::history(&record).unwrap();
        assert_eq!(after.lineage(), original.lineage());
        assert_eq!(after.anchor(), original.anchor());
        assert!(after.tail().sequence() > original.tail().sequence());
        drop(held);
        drop(ports);
        let reopened = owner
            .reconcile_committed_promotion(
                &record,
                inputs(),
                Arc::new(AtomicBool::new(false)),
                profile,
                Arc::new(NoChangelogPublicationPort),
            )
            .unwrap();
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(
                &reopened.shared.database.begin_read().unwrap()
            )
            .unwrap(),
            Some(after)
        );
    }
}

#[test]
fn committed_promotion_reconciliation_requires_cutover_and_reuses_one_lineage() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-reconciliation");
    let (path, mut owner, record, _) = fixture(&scope);
    assert!(reconcile(&mut owner, &record).is_err());
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts(),
        &[record.attempt().clone()]
    );
    owner.apply_promotion_cutover(&record).unwrap();
    let expected = crate::promotion_cutover::history(&record).unwrap();
    for _ in 0..2 {
        let store = reconcile(&mut owner, &record).unwrap();
        let read = store.shared.database.begin_read().unwrap();
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(&read).unwrap(),
            Some(expected)
        );
        assert_eq!(
            owner.promotion_receipts().unwrap().receipts()[0].phase(),
            ReplicationPromotionPhaseV1::Succeeded
        );
        assert!(!store.shared.is_follower_mode());
        drop(read);
        drop(store);
    }
    // Reconciliation is still required on future opens; numeric lineage alone
    // cannot replace the exact external audit.
    assert!(crate::RedbStore::open(&path).is_err());
    assert!(owner.apply_promotion_cutover(&record).is_err());
}

#[test]
fn committed_promotion_full_validation_refuses_corruption_outside_anchor_roots() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-reconciliation-corrupt");
    let (path, mut owner, record, _) = fixture(&scope);
    owner.apply_promotion_cutover(&record).unwrap();
    let database = redb::Database::open(&path).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(crate::layout::ENTITIES)
        .unwrap()
        .insert(
            b"malformed-entity-key".as_slice(),
            b"malformed-value".as_slice(),
        )
        .unwrap();
    write.commit().unwrap();
    drop(database);
    // Discovery proves the bounded authority join, not the whole database.
    // A caller cannot substitute it for reconciliation's complete source scrub.
    assert_eq!(
        owner.discover_committed_promotion().unwrap(),
        Some(record.clone())
    );
    assert!(reconcile(&mut owner, &record).is_err());
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts()[0].phase(),
        ReplicationPromotionPhaseV1::CutoverCommitted
    );
    assert!(crate::RedbStore::open(&path).is_err());
}

#[test]
fn committed_promotion_reconciliation_refuses_cancelled_missing_and_terminal_claims() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-reconciliation-refusals");
    let (path, mut owner, record, _) = fixture(&scope);
    owner.apply_promotion_cutover(&record).unwrap();
    assert!(
        owner
            .reconcile_committed_promotion(
                &record,
                inputs(),
                Arc::new(AtomicBool::new(true)),
                crate::RedbCommitProfile::Standard,
                Arc::new(NoChangelogPublicationPort)
            )
            .is_err()
    );
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts(),
        &[record.attempt().clone()]
    );
    let substituted = StoredPromotionAdministrationV1::new(
        record.attempt().clone(),
        Timestamp::new(1234, 3).unwrap(),
        ServiceIngressKindV1::Grpc,
    )
    .unwrap();
    assert!(reconcile(&mut owner, &substituted).is_err());
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts(),
        &[record.attempt().clone()]
    );
    // The audit adapter is intentionally not a runtime precondition proof.
    let mut claimed = record.attempt().clone();
    for phase in [
        ReplicationPromotionPhaseV1::CutoverCommitted,
        ReplicationPromotionPhaseV1::Validated,
        ReplicationPromotionPhaseV1::Succeeded,
    ] {
        claimed
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
        owner.persist_promotion_receipt(&claimed).unwrap();
    }
    let database = redb::Database::open(&path).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(crate::layout::ENTITIES)
        .unwrap()
        .insert(b"bad-key".as_slice(), b"bad-value".as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(database);
    assert!(reconcile(&mut owner, &record).is_err());
    drop(owner);
    std::fs::remove_file(
        scope
            .join("backups/.maintenance/replication_promotion")
            .join(format!("{}.receipt-v1", record.attempt().request_id())),
    )
    .unwrap();
    let mut owner =
        RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups")).unwrap();
    assert!(reconcile(&mut owner, &record).is_err());
    assert!(crate::RedbStore::open(&path).is_err());
}

#[cfg(unix)]
#[test]
fn committed_promotion_reconciliation_process_crashes_resume_without_restamping() {
    use std::os::unix::process::ExitStatusExt;
    const CHILD: &str = "RIFFDB_PROMOTION_RECONCILIATION_CHILD_ROOT";
    const TEST: &str = "maintenance::bootstrap_materialize_tests::promotion_cutover::reconciliation::committed_promotion_reconciliation_process_crashes_resume_without_restamping";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = std::path::PathBuf::from(root);
        let mut owner = RedbMaintenanceStorage::open_for_promotion_recovery(
            root.join("candidate/follower.redb"),
            root.join("backups"),
        )
        .unwrap();
        let record = StoredPromotionAdministrationV1::new(
            owner.promotion_receipts().unwrap().receipts()[0].clone(),
            Timestamp::new(1234, 2).unwrap(),
            ServiceIngressKindV1::Grpc,
        )
        .unwrap();
        drop(reconcile(&mut owner, &record).unwrap());
        panic!("armed promotion reconciliation edge was not reached");
    }
    for _ in 0..2 {
        for edge in [
            "authority-joined",
            "committed-receipt",
            "source-validated",
            "validated-receipt",
            "succeeded-receipt",
            "source-open-begun",
            "source-opened",
        ] {
            let scope = crate::test_path::ScopedDirectory::new("promotion-reconciliation-crash");
            let (path, mut owner, record, _) = fixture(&scope);
            owner.apply_promotion_cutover(&record).unwrap();
            drop(owner);
            let child = std::process::Command::new("sh")
                .args([
                    "-c",
                    "ulimit -c 0; exec \"$@\"",
                    "promotion-reconciliation-crash",
                ])
                .arg(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture"])
                .env(CHILD, scope.join(""))
                .env("RIFFDB_PROMOTION_RECONCILIATION_CRASH_EDGE", edge)
                .output()
                .unwrap();
            assert_eq!(
                child.status.signal(),
                Some(6),
                "{edge}: {}",
                String::from_utf8_lossy(&child.stderr)
            );
            assert!(crate::RedbStore::open(&path).is_err());
            let mut owner =
                RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups"))
                    .unwrap();
            let store = reconcile(&mut owner, &record).unwrap();
            assert_eq!(
                crate::changelog_v3_roots::validate_retained_history(
                    &store.shared.database.begin_read().unwrap()
                )
                .unwrap(),
                Some(crate::promotion_cutover::history(&record).unwrap())
            );
            let inventory = owner.promotion_receipts().unwrap();
            assert_eq!(inventory.receipts().len(), 1);
            let receipt = &inventory.receipts()[0];
            assert_eq!(receipt.phase(), ReplicationPromotionPhaseV1::Succeeded);
            assert!(receipt.monotonically_extends(record.attempt()));
            assert_eq!(receipt.steps().len(), record.attempt().steps().len() + 3);
            drop(store);
            assert!(owner.apply_promotion_cutover(&record).is_err());
        }
    }
}
