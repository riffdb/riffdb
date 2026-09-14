//! Actual structural session over isolated activation, not fresh activation.
// req: REP-003, REC-001, STO-012, PERF-007
use super::*;
use riffdb_storage_api::{
    AuthoritativeTransactionV3, ChangelogAttributionV3 as A, ChangelogLineageV3, LeadershipEpochV1,
};

fn v3_store(path: &TestDatabasePath, id: DatabaseId) -> RedbStore {
    let store = initialized_store(path, id);
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        riffdb_types::DualFrontier::INITIAL,
    )
    .unwrap();
    store
}

fn receipts(shared: &crate::store::SharedRedb) -> Vec<AuthoritativeTransactionV3> {
    let read = shared.database.begin_read().unwrap();
    crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    read.open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| AuthoritativeTransactionV3::decode(row.unwrap().1.value()).unwrap())
        .collect()
}

fn finish_v3(mut session: RedbStructuralEvidenceSession) -> crate::RedbOperationalPorts {
    let structural_end = finish_structural(&mut session);
    let historical_end = finish_historical(&mut session);
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("complete V3 must not require a legacy migration");
    };
    opened
        .into_parts()
        .3
        .into_operational_after_catalog_validation()
        .unwrap()
}

#[test]
fn actual_v3_startup_full_validation_and_bounded_clean_consumption_keep_exact_receipts() {
    let path = TestDatabasePath::new("v3-startup-full-bounded");
    let store = v3_store(&path, database_id(0xd1));
    let initial_epoch = store.shared.durable_commit_epoch();
    let original = receipts(&store.shared)[0].encode().unwrap();
    let session = store
        .begin_structural_evidence(inputs())
        .expect("V3 structural session");
    assert!(!session.clean_close_fast);
    let ports = finish_v3(session);
    assert_eq!(
        ports.shared.durable_commit_epoch(),
        initial_epoch + 2,
        "one prefix and one DIRTY transaction"
    );
    let first = receipts(&ports.shared);
    assert_eq!(first.len(), 3);
    assert_eq!(first[0].encode().unwrap(), original);
    assert_eq!(first[0].attribution(), A::V3Activation);
    assert_eq!(first[2].attribution(), A::DirtyActivation);
    ports.write_clean_close_lifecycle().unwrap();
    let closed = receipts(&ports.shared);
    assert_eq!(closed.len(), 4);
    assert_eq!(closed[3].attribution(), A::CleanClose);
    drop(ports);

    for expected_len in [5, 7] {
        let store = RedbStore::open(&path.0).unwrap();
        let session = store.begin_structural_evidence(inputs()).unwrap();
        assert!(
            session.clean_close_fast,
            "complete V3 roots retain bounded startup"
        );
        assert_eq!(
            session.structural_counts[3..],
            [0; STRUCTURAL_TABLE_COUNT - 3]
        );
        assert_eq!(
            session.additive_structural_counts,
            [0; ADDITIVE_STRUCTURAL_TABLE_COUNT]
        );
        let ports = finish_v3(session);
        assert_eq!(ports.shared.durable_commit_epoch(), 1, "only consume CLEAN");
        let after = receipts(&ports.shared);
        assert_eq!(after.len(), expected_len);
        assert_eq!(after[0].encode().unwrap(), original);
        assert_eq!(after.last().unwrap().attribution(), A::DirtyActivation);
        for (before, after) in closed.iter().zip(after.iter()) {
            assert_eq!(before.encode().unwrap(), after.encode().unwrap());
        }
        ports.write_clean_close_lifecycle().unwrap();
    }
}

#[test]
fn actual_v3_full_startup_refuses_missing_or_corrupt_interior_history_before_writes() {
    for arm in 0..3 {
        let path = TestDatabasePath::new("v3-startup-interior-refusal");
        let store = v3_store(&path, database_id(0xd2));
        let ports = finish_v3(store.begin_structural_evidence(inputs()).unwrap());
        assert_eq!(receipts(&ports.shared).len(), 3);
        let write = ports.shared.database.begin_write().unwrap();
        {
            let mut history = write
                .open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap();
            match arm {
                0 => {
                    history.remove(1u64.to_be_bytes().as_slice()).unwrap();
                }
                1 => {
                    history.remove(2u64.to_be_bytes().as_slice()).unwrap();
                }
                _ => {
                    history
                        .insert(2u64.to_be_bytes().as_slice(), b"invalid receipt".as_slice())
                        .unwrap();
                }
            }
        }
        write.commit().unwrap();
        let before = control_bytes(&ports.shared);
        drop(ports);
        for _ in 0..2 {
            let store = RedbStore::open(&path.0).expect("bounded tail remains intact");
            let shared = Arc::clone(&store.shared);
            let epoch = shared.durable_commit_epoch();
            match store.begin_structural_evidence(inputs()) {
                Err(error) => assert_eq!(error.kind(), StorageErrorKind::CorruptData),
                Ok(_) => panic!("full startup accepted corrupt interior arm {arm}"),
            }
            assert_eq!(shared.durable_commit_epoch(), epoch);
            assert_eq!(control_bytes(&shared), before);
        }
    }
}

type ControlBytes = (Vec<(String, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>)>);

fn control_bytes(shared: &crate::store::SharedRedb) -> ControlBytes {
    let read = shared.database.begin_read().unwrap();
    let meta = read
        .open_table(META)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.value().to_owned(), value.value().to_vec())
        })
        .collect();
    let history = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect();
    (meta, history)
}

#[test]
fn v3_actual_startup_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_V3_STARTUP_PATH") else {
        return;
    };
    let store = RedbStore::open(path).unwrap();
    let ports = finish_v3(store.begin_structural_evidence(inputs()).unwrap());
    ports.write_clean_close_lifecycle().unwrap();
    panic!("requested startup/close crash edge was not reached");
}

#[test]
fn actual_v3_startup_process_crashes_recover_complete_prefix_and_lifecycle_receipts() {
    for (variable, edge, expected) in [
        ("RIFFDB_V3_DIRECT_EDGE", "mutations", 1),
        ("RIFFDB_V3_DIRECT_EDGE", "receipt", 1),
        ("RIFFDB_V3_DIRECT_EDGE", "roots", 1),
        ("RIFFDB_V3_DIRECT_EDGE", "committed", 2),
        ("RIFFDB_V3_LIFECYCLE_EDGE", "dirty-staged", 2),
        ("RIFFDB_V3_LIFECYCLE_EDGE", "dirty-committed", 3),
        ("RIFFDB_V3_LIFECYCLE_EDGE", "clean-staged", 3),
        ("RIFFDB_V3_LIFECYCLE_EDGE", "clean-committed", 4),
    ] {
        let path = TestDatabasePath::new("v3-actual-startup-crash");
        let store = v3_store(&path, database_id(0xd3));
        let original = receipts(&store.shared)[0].encode().unwrap();
        drop(store);
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup::tests::v3::v3_actual_startup_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_V3_STARTUP_PATH", &path.0)
            .env(variable, edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        let mut previous = None;
        for _ in 0..2 {
            let store = RedbStore::open(&path.0).unwrap();
            let current = receipts(&store.shared);
            assert_eq!(current.len(), expected, "edge {edge}");
            assert_eq!(current[0].encode().unwrap(), original);
            let bytes = control_bytes(&store.shared);
            if let Some(previous) = &previous {
                assert_eq!(previous, &bytes);
            }
            previous = Some(bytes);
        }
        // Retry through the actual complete/clean selector, not a raw engine
        // read or an isolated receipt helper, then prove retained original bytes.
        let store = RedbStore::open(&path.0).unwrap();
        let ports = finish_v3(store.begin_structural_evidence(inputs()).unwrap());
        let after = receipts(&ports.shared);
        assert_eq!(after[0].encode().unwrap(), original);
        assert_eq!(after.last().unwrap().attribution(), A::DirtyActivation);
        assert!(after.len() > expected);
    }
}
