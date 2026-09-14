//! Actual structural/catalog activation, cancellation, and V3 lifecycle evidence.
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
    let (catalog, historical_end) = validate_catalog_history(&mut session).unwrap().into_parts();
    let CatalogHistoryOutcome::Ready(catalog) = catalog else {
        panic!("V3 fixture catalog must validate");
    };
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("complete V3 must not require a legacy migration");
    };
    let (database_id, session_id, _, dormant) = opened.into_parts();
    assert!(catalog.matches(database_id, session_id));
    dormant.into_operational_after_catalog_validation().unwrap()
}

#[test]
fn fresh_v3_activation_waits_for_catalog_join_and_cancelled_handoff_is_read_only() {
    let metadata = |shared: &crate::store::SharedRedb| {
        shared
            .database
            .begin_read()
            .unwrap()
            .open_table(META)
            .unwrap()
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                (key.value().to_owned(), value.value().to_vec())
            })
            .collect::<BTreeMap<_, _>>()
    };
    let path = TestDatabasePath::new("v3-startup-catalog-handoff");
    let store = initialized_store(&path, database_id(0xd2));
    let shared = Arc::clone(&store.shared);
    let before = metadata(&shared);
    let epoch = shared.durable_commit_epoch();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    assert!(!session.clean_close_fast);
    assert!(!session.checkpoint_verified);
    let structural_end = finish_structural(&mut session);
    let historical_end = finish_historical(&mut session);
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("fresh database must not require an index migration");
    };
    assert_eq!(shared.durable_commit_epoch(), epoch);
    assert_eq!(metadata(&shared), before);
    assert!(opened.into_parts().3.pending_v3_activation.is_some());
    // A cancelled catalog join must release its lease even while a diagnostic
    // reader retains SharedRedb. Re-entering the session is the deterministic
    // cancellation check; no sleeps or timing assumptions.
    let store = RedbStore {
        shared: Arc::clone(&shared),
    };
    let ports = finish_v3(store.begin_structural_evidence(inputs()).unwrap());
    let after = receipts(&shared);
    assert_eq!(after.len(), 3);
    assert_eq!(after[0].attribution(), A::V3Activation);
    assert_eq!(after[0].binding().sequence.get(), 1);
    assert_eq!(after.last().unwrap().attribution(), A::DirtyActivation);
    assert_eq!(shared.durable_commit_epoch(), epoch + 3);
    drop(ports);
}

#[test]
fn actual_fresh_activation_crashes_leave_exact_inactive_or_complete_v3_and_retry_once() {
    use riffdb_storage_api::proto_codec::{
        current_record_registry_digest, decode_record_registry_v2,
    };
    for edge in ["preflight", "roots", "receipt", "committed"] {
        let path = TestDatabasePath::new("v3-fresh-activation-crash");
        let store = initialized_store(&path, database_id(0xd4));
        let before_registry = {
            let read = store.shared.database.begin_read().unwrap();
            let meta = read.open_table(META).unwrap();
            meta.get(META_RECORD_REGISTRY)
                .unwrap()
                .unwrap()
                .value()
                .to_vec()
        };
        drop(store);
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup::tests::v3::v3_actual_startup_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_V3_STARTUP_PATH", &path.0)
            .env("RIFFDB_WP772_ACTIVATION_CRASH", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(91), "edge {edge}");
        let mut original = None;
        for _ in 0..2 {
            let store = RedbStore::open(&path.0).unwrap();
            let read = store.shared.database.begin_read().unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&read).unwrap();
            let meta = read.open_table(META).unwrap();
            let registry = meta.get(META_RECORD_REGISTRY).unwrap().unwrap();
            if edge == "committed" {
                assert_eq!(history.unwrap().tail().sequence().get(), 1);
                assert_eq!(
                    *decode_record_registry_v2(registry.value()).unwrap().value(),
                    current_record_registry_digest()
                );
                let receipt = receipts(&store.shared).remove(0);
                assert_eq!(receipt.attribution(), A::V3Activation);
                assert_eq!(receipt.mutations().len(), 1);
                assert!(receipt.mutations()[0].matches_prior(Some(&before_registry)));
                let encoded = receipt.encode().unwrap();
                if let Some(previous) = &original {
                    assert_eq!(previous, &encoded);
                }
                original = Some(encoded);
            } else {
                assert!(history.is_none());
                assert_eq!(registry.value(), before_registry);
                assert!(
                    read.open_table(crate::changelog_v3_activation::HISTORY)
                        .is_err()
                );
                assert!(
                    read.open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
                        .is_err()
                );
            }
        }
        let store = RedbStore::open(&path.0).unwrap();
        let ports = finish_v3(store.begin_structural_evidence(inputs()).unwrap());
        let after = receipts(&ports.shared);
        assert_eq!(
            after
                .iter()
                .filter(|r| r.attribution() == A::V3Activation)
                .count(),
            1
        );
        assert_eq!(after.len(), 3);
        if let Some(original) = original {
            assert_eq!(after[0].encode().unwrap(), original);
        }
        assert_eq!(after.last().unwrap().attribution(), A::DirtyActivation);
    }
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
fn actual_v3_full_startup_refuses_unknown_source_holds_before_lifecycle_writes() {
    let path = TestDatabasePath::new("v3-startup-source-hold-refusal");
    let store = v3_store(&path, database_id(0xe1));
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap()
        .insert(
            b"unknown hold".as_slice(),
            b"unknown hold encoding".as_slice(),
        )
        .unwrap();
    write.commit().unwrap();
    let before = control_bytes(&store.shared);
    let shared = Arc::clone(&store.shared);
    let epoch = shared.durable_commit_epoch();
    match store.begin_structural_evidence(inputs()) {
        Err(error) => assert_eq!(error.kind(), StorageErrorKind::CorruptData),
        Ok(_) => panic!("full validation accepted an unknown source-hold record"),
    }
    assert_eq!(shared.durable_commit_epoch(), epoch);
    assert_eq!(control_bytes(&shared), before);
}

fn source_hold(shared: &crate::store::SharedRedb) -> riffdb_storage_api::ReplicationSourceHoldV1 {
    use riffdb_storage_api::{
        ReplicationSourceHoldIdV1, ReplicationSourceHoldKindV1, ReplicationSourceHoldV1,
    };
    let history = crate::changelog_v3_roots::validate_retained_history(
        &shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    ReplicationSourceHoldV1::new(
        ReplicationSourceHoldIdV1::new([0x91; 16]).unwrap(),
        ReplicationSourceHoldKindV1::Bootstrap,
        history.lineage(),
        history.tail(),
    )
}

#[test]
fn actual_v3_full_startup_preserves_all_three_exact_source_fences() {
    use riffdb_storage_api::{
        ReplicationSourceHoldKindV1 as Kind, ReplicationSourceHoldV1 as Hold, proto_codec::*,
    };
    let path = TestDatabasePath::new("v3-startup-valid-source-holds");
    let store = v3_store(&path, database_id(0xe2));
    let h = source_hold(&store.shared);
    let expected = [
        Kind::FollowerAcknowledgement,
        Kind::ArchiveAcknowledgement,
        Kind::Bootstrap,
    ]
    .map(|kind| Hold::new(h.id(), kind, h.lineage(), h.fence()));
    let write = store.shared.database.begin_write().unwrap();
    for hold in expected {
        write
            .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .unwrap()
            .insert(
                hold.storage_key().as_slice(),
                encode_replication_source_hold_v1(hold).unwrap().as_bytes(),
            )
            .unwrap();
    }
    write.commit().unwrap();
    let ports = finish_v3(store.begin_structural_evidence(inputs()).unwrap());
    receipts(&ports.shared);
    let read = ports.shared.database.begin_read().unwrap();
    let table = read
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap();
    assert_eq!(table.len().unwrap(), 3);
    for hold in expected {
        assert_eq!(
            table
                .get(hold.storage_key().as_slice())
                .unwrap()
                .unwrap()
                .value(),
            encode_replication_source_hold_v1(hold).unwrap().as_bytes()
        );
    }
}

#[test]
fn actual_v3_full_startup_refuses_substituted_source_fences_without_writes() {
    use riffdb_storage_api::{
        ChangelogHistoryPointV3 as Point, ChangelogTransactionSequence as Seq,
        ReplicationSourceHoldV1 as Hold, proto_codec::*,
    };
    for arm in 0..7 {
        let path = TestDatabasePath::new("v3-startup-substituted-source-holds");
        let store = v3_store(&path, database_id(0xe3));
        let ports = finish_v3(store.begin_structural_evidence(inputs()).unwrap());
        let interior = Point::from_receipt(&receipts(&ports.shared)[1]).unwrap();
        drop(ports);
        let store = RedbStore::open(&path.0).unwrap();
        let root_hold = source_hold(&store.shared);
        let h = Hold::new(
            root_hold.id(),
            root_hold.kind(),
            root_hold.lineage(),
            interior,
        );
        let mut lineage = h.lineage();
        let mut fence = h.fence();
        let mut key = h.storage_key();
        match arm {
            0 => key[0] = 2,
            1 => key[1] ^= 1,
            2 => {
                lineage =
                    ChangelogLineageV3::new(database_id(0xe4), 1, LeadershipEpochV1::initial())
                        .unwrap()
            }
            3 => {
                lineage =
                    ChangelogLineageV3::new(lineage.database_id(), 2, LeadershipEpochV1::initial())
                        .unwrap()
            }
            4 => {
                lineage = ChangelogLineageV3::new(
                    lineage.database_id(),
                    1,
                    LeadershipEpochV1::new(2).unwrap(),
                )
                .unwrap()
            }
            5 => fence = Point::new(fence.sequence(), [0xff; 32], fence.frontier()),
            _ => fence = Point::new(Seq::new(4).unwrap(), fence.history_hash(), fence.frontier()),
        }
        let bad = Hold::new(h.id(), h.kind(), lineage, fence);
        let write = store.shared.database.begin_write().unwrap();
        write
            .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .unwrap()
            .insert(
                key.as_slice(),
                encode_replication_source_hold_v1(bad).unwrap().as_bytes(),
            )
            .unwrap();
        write.commit().unwrap();
        let shared = Arc::clone(&store.shared);
        let before = control_bytes(&shared);
        let epoch = shared.durable_commit_epoch();
        match store.begin_structural_evidence(inputs()) {
            Err(error) => assert_eq!(error.kind(), StorageErrorKind::CorruptData, "arm {arm}"),
            Ok(_) => panic!("startup accepted substituted source fence arm {arm}"),
        }
        assert_eq!(shared.durable_commit_epoch(), epoch);
        assert_eq!(control_bytes(&shared), before);
    }
}

#[test]
fn actual_v3_full_startup_bounds_combined_source_hold_population() {
    use riffdb_storage_api::{
        MAX_REPLICATION_SOURCE_HOLDS_V1 as MAX, ReplicationSourceHoldIdV1 as Id,
        ReplicationSourceHoldV1 as Hold, proto_codec::*,
    };
    for count in [MAX, MAX + 1] {
        let path = TestDatabasePath::new("v3-startup-source-hold-count");
        let store = v3_store(&path, database_id(0xe5));
        let h = source_hold(&store.shared);
        let write = store.shared.database.begin_write().unwrap();
        {
            let mut table = write
                .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
                .unwrap();
            for index in 1..=count {
                let hold = Hold::new(
                    Id::new(u128::from(index).to_be_bytes()).unwrap(),
                    h.kind(),
                    h.lineage(),
                    h.fence(),
                );
                table
                    .insert(
                        hold.storage_key().as_slice(),
                        encode_replication_source_hold_v1(hold).unwrap().as_bytes(),
                    )
                    .unwrap();
            }
        }
        write.commit().unwrap();
        let shared = Arc::clone(&store.shared);
        let before = control_bytes(&shared);
        let epoch = shared.durable_commit_epoch();
        match (count, store.begin_structural_evidence(inputs())) {
            (MAX, Ok(session)) => {
                finish_v3(session);
            }
            (_, Err(error)) if count > MAX => {
                assert_eq!(error.kind(), StorageErrorKind::LimitExceeded);
                assert_eq!(shared.durable_commit_epoch(), epoch);
                assert_eq!(control_bytes(&shared), before);
            }
            _ => panic!("source-hold bound did not match exact population"),
        }
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
