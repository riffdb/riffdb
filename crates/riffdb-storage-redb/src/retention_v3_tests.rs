//! Actual offline operator hold entry points over isolated V3 activation.
// req: REP-003, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3, ChangelogAttributionV3 as A,
    ChangelogLineageV3, DatabaseInitializationPort, LeadershipEpochV1,
};
use riffdb_types::{DatabaseId, DualFrontier};

fn initialized(path: &Path) {
    let mut store = RedbStore::open(path).unwrap();
    let id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0xd4; 10]).unwrap();
    store.initialize_database(id).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
}

fn read(path: &Path) -> (Vec<AuthoritativeTransactionV3>, Option<Vec<u8>>) {
    let store = RedbStore::open(path).unwrap();
    let read = store.shared.database.begin_read().unwrap();
    crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    let receipts = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| AuthoritativeTransactionV3::decode(row.unwrap().1.value()).unwrap())
        .collect();
    let holds = read
        .open_table(META)
        .unwrap()
        .get(META_RETENTION_HOLDS)
        .unwrap()
        .map(|value| value.value().to_vec());
    (receipts, holds)
}

#[test]
fn offline_operator_hold_changes_retain_exact_receipts_and_noops_do_not_allocate() {
    let scope = crate::test_path::ScopedDirectory::new("v3-offline-holds");
    let path = scope.join("db.redb");
    initialized(&path);
    let offline = RedbOfflineRetention::bind(&path);
    let mut before = read(&path);
    for step in 0..3 {
        match step {
            0 => offline.add_hold("operator", 0, "hold").unwrap(),
            1 => offline.add_hold("operator", 1, "replacement").unwrap(),
            _ => offline.remove_hold("operator").unwrap(),
        }
        let after = read(&path);
        assert_eq!(
            after.0.len(),
            before.0.len() + 1,
            "each changed hold needs one receipt"
        );
        for (left, right) in before.0.iter().zip(after.0.iter()) {
            assert_eq!(left.encode().unwrap(), right.encode().unwrap());
        }
        let receipt = after.0.last().unwrap();
        assert_eq!(receipt.attribution(), A::RetentionHold);
        assert_eq!(receipt.mutations().len(), 1);
        let mutation = &receipt.mutations()[0];
        assert_eq!(mutation.namespace(), N::RetentionHolds);
        assert!(mutation.matches_prior(before.1.as_deref()));
        assert_eq!(mutation.value(), after.1.as_deref());
        assert_eq!(receipt.binding().covered_frontier, DualFrontier::INITIAL);
        if step == 2 {
            offline.remove_hold("operator").unwrap();
        } else if step == 1 {
            offline.add_hold("operator", 1, "replacement").unwrap();
        }
        assert_eq!(
            read(&path),
            after,
            "an exact no-op must keep allocator and receipts"
        );
        before = after;
    }
}

#[test]
fn offline_hold_receipts_preserve_before_commit_and_unknown_outcomes() {
    for committed in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("v3-offline-hold-hooks");
        let path = scope.join("db.redb");
        initialized(&path);
        let controller = if committed {
            RedbTestController::return_unknown_after_commit(RedbTestOperation::RetentionHold)
        } else {
            RedbTestController::return_before_commit(RedbTestOperation::RetentionHold)
        };
        let offline = RedbOfflineRetention::bind_with_test_controller(&path, controller);
        let error = offline.add_hold("operator", 0, "hold").unwrap_err();
        assert_eq!(
            error.kind(),
            if committed {
                StorageErrorKind::CommitStatusUnknown
            } else {
                StorageErrorKind::Unavailable
            }
        );
        let after = read(&path);
        assert_eq!(after.0.len(), if committed { 2 } else { 1 });
        assert_eq!(after.1.is_some(), committed);
        let retry = RedbOfflineRetention::bind(&path);
        retry.add_hold("operator", 0, "hold").unwrap();
        let stable = read(&path);
        assert_eq!(stable.0.len(), 2);
        retry.add_hold("operator", 0, "hold").unwrap();
        assert_eq!(read(&path), stable);
    }
}

#[test]
fn offline_projection_holds_receipt_audit_allocator_and_hold_together() {
    let scope = crate::test_path::ScopedDirectory::new("v3-offline-projection-hold");
    let path = scope.join("db.redb");
    initialized(&path);
    let offline = RedbOfflineRetention::bind(&path);
    let id = ProjectionId::new(7).unwrap();
    let time = Timestamp::new(1, 0).unwrap();
    offline.detach_projection(id, "detach", time).unwrap();
    let detached = read(&path);
    assert_eq!(detached.0.len(), 2);
    let receipt = detached.0.last().unwrap();
    assert_eq!(receipt.attribution(), A::RetentionHold);
    assert_eq!(
        receipt
            .binding()
            .covered_frontier
            .administration()
            .unwrap()
            .get(),
        1
    );
    for namespace in [N::RetentionHolds, N::NextAdministrationSequence, N::Audit] {
        assert!(
            receipt
                .mutations()
                .iter()
                .any(|mutation| mutation.namespace() == namespace)
        );
    }
    offline.detach_projection(id, "detach", time).unwrap();
    assert_eq!(read(&path), detached);
    assert!(offline.remove_hold("7").is_err());
    assert_eq!(read(&path), detached);
    offline.reattach_projection(id, "reattach", time).unwrap();
    let reattached = read(&path);
    assert_eq!(reattached.0.len(), 3);
    let receipt = reattached.0.last().unwrap();
    assert_eq!(receipt.attribution(), A::RetentionHold);
    assert_eq!(
        receipt
            .binding()
            .covered_frontier
            .administration()
            .unwrap()
            .get(),
        2
    );
    assert_eq!(
        reattached.0[1].encode().unwrap(),
        detached.0[1].encode().unwrap()
    );
}

#[test]
fn offline_hold_refuses_interior_history_corruption_before_any_metadata_change() {
    let scope = crate::test_path::ScopedDirectory::new("v3-offline-hold-refusal");
    let path = scope.join("db.redb");
    initialized(&path);
    let offline = RedbOfflineRetention::bind(&path);
    offline.add_hold("operator", 0, "first").unwrap();
    offline.add_hold("operator", 1, "second").unwrap();
    let before = read(&path).1;
    {
        let database = Database::open(&path).unwrap();
        let write = database.begin_write().unwrap();
        write
            .open_table(crate::changelog_v3_activation::HISTORY)
            .unwrap()
            .remove(2u64.to_be_bytes().as_slice())
            .unwrap();
        write.commit().unwrap();
    }
    for _ in 0..2 {
        assert_eq!(
            offline
                .add_hold("operator", 2, "forbidden")
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            offline.remove_hold("operator").unwrap_err().kind(),
            StorageErrorKind::CorruptData
        );
        let store = RedbStore::open(&path).unwrap();
        let read = store.shared.database.begin_read().unwrap();
        assert_eq!(
            read.open_table(META)
                .unwrap()
                .get(META_RETENTION_HOLDS)
                .unwrap()
                .map(|value| value.value().to_vec()),
            before
        );
        assert_eq!(
            crate::changelog_v3_roots::read_checkpoint_roots(&read)
                .unwrap()
                .unwrap()
                .tail()
                .sequence()
                .get(),
            3
        );
        assert_eq!(
            read.open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap()
                .len()
                .unwrap(),
            2
        );
        assert_eq!(store.shared.durable_commit_epoch(), 0);
    }
}

#[test]
fn offline_v3_hold_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_V3_OFFLINE_HOLD_PATH") else {
        return;
    };
    RedbOfflineRetention::bind(path)
        .add_hold("operator", 0, "hold")
        .unwrap();
    panic!("hold crash edge not reached");
}

#[test]
fn actual_offline_hold_process_crashes_preserve_original_or_complete_receipt() {
    for edge in ["mutations", "receipt", "roots", "committed"] {
        let scope = crate::test_path::ScopedDirectory::new("v3-offline-hold-crash");
        let path = scope.join("db.redb");
        initialized(&path);
        let original = read(&path);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "retention::v3_tests::offline_v3_hold_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_V3_OFFLINE_HOLD_PATH", &path)
            .env("RIFFDB_V3_DIRECT_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        let mut previous = None;
        for _ in 0..2 {
            let after = read(&path);
            assert_eq!(after.0.len(), if edge == "committed" { 2 } else { 1 });
            assert_eq!(after.1.is_some(), edge == "committed");
            assert_eq!(
                after.0[0].encode().unwrap(),
                original.0[0].encode().unwrap()
            );
            if let Some(previous) = &previous {
                assert_eq!(previous, &after);
            }
            previous = Some(after);
        }
        RedbOfflineRetention::bind(&path)
            .add_hold("operator", 0, "hold")
            .unwrap();
        assert_eq!(read(&path).0.len(), 2);
    }
}
