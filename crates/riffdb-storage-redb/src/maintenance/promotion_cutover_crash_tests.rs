//! Process death at every physical promotion edge, including a lost commit reply.
// req: REP-003, REP-005, REC-001
use super::*;

#[cfg(unix)]
#[test]
fn promotion_cutover_process_crashes_keep_one_complete_lineage_and_pending_receipt() {
    use std::os::unix::process::ExitStatusExt;
    const CHILD: &str = "RIFFDB_PROMOTION_CUTOVER_CHILD_ROOT";
    const TEST: &str = "maintenance::bootstrap_materialize_tests::promotion_cutover::crashes::promotion_cutover_process_crashes_keep_one_complete_lineage_and_pending_receipt";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = std::path::PathBuf::from(root);
        let mut owner = RedbMaintenanceStorage::open_for_promotion_recovery(
            root.join("candidate/follower.redb"),
            root.join("backups"),
        )
        .unwrap();
        let attempt = owner.promotion_receipts().unwrap().receipts()[0].clone();
        let record = StoredPromotionAdministrationV1::new(
            attempt,
            Timestamp::new(1234, 2).unwrap(),
            ServiceIngressKindV1::Grpc,
        )
        .unwrap();
        owner.apply_promotion_cutover(&record).unwrap();
        panic!("armed promotion crash edge was not reached");
    }
    let edges = [
        "opened",
        "roots-read",
        "follower-checked",
        "receipt-built",
        "preflight",
        "incarnation",
        "audit",
        "lineage",
        "anchor",
        "committed",
    ];
    for _ in 0..2 {
        for edge in edges {
            let scope = crate::test_path::ScopedDirectory::new("promotion-cutover-crash");
            let (path, owner, record, before) = fixture(&scope);
            drop(owner);
            let child = std::process::Command::new("sh")
                .args(["-c", "ulimit -c 0; exec \"$@\"", "promotion-cutover-crash"])
                .arg(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture"])
                .env(CHILD, scope.join(""))
                .env("RIFFDB_PROMOTION_CUTOVER_CRASH_EDGE", edge)
                .output()
                .unwrap();
            assert_eq!(
                child.status.signal(),
                Some(6),
                "{edge}: {}",
                String::from_utf8_lossy(&child.stderr)
            );
            let database = redb::Database::open(&path).unwrap();
            let after = crate::changelog_v3_roots::validate_retained_history(
                &database.begin_read().unwrap(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                after,
                if edge == "committed" {
                    crate::promotion_cutover::history(&record).unwrap()
                } else {
                    before
                }
            );
            drop(database);
            let mut owner =
                RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups"))
                    .unwrap();
            assert_eq!(
                owner.promotion_receipts().unwrap().receipts(),
                &[record.attempt().clone()]
            );
            assert!(owner.reconcile().is_err());
            if edge != "committed" {
                // Isolated physical retry: production still requires fresh
                // authority and authenticated evidence before entering storage.
                owner.apply_promotion_cutover(&record).unwrap();
            }
            assert!(owner.apply_promotion_cutover(&record).is_err());
            assert!(crate::RedbStore::open(&path).is_err());
            drop(owner);
            // Missing the entire external ledger cannot silently grant startup.
            std::fs::remove_dir_all(scope.join("backups/.maintenance/replication_promotion"))
                .unwrap();
            assert!(crate::RedbStore::open(&path).is_err());
            let database = redb::Database::open(&path).unwrap();
            assert_eq!(
                crate::changelog_v3_roots::validate_retained_history(
                    &database.begin_read().unwrap()
                )
                .unwrap(),
                Some(crate::promotion_cutover::history(&record).unwrap())
            );
        }
    }
}
