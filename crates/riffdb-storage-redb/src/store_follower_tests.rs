//! Isolated applier transaction evidence; fixtures do not grant server readiness.
// req: REP-002, REP-003, REC-001
#[path = "store_follower_tests/indexes.rs"]
mod indexes;
use super::*;
use riffdb_storage_api::{
    AuthoritativeMutationV3 as Mutation, AuthoritativeNamespaceV1 as N,
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogFrameBindingV3, ChangelogFrameV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    LeadershipEpochV1, ReplicationFollowerStateV3,
    proto_codec::encode_replication_follower_state_v3,
};

fn fixture(path: &Path) -> ChangelogHistoryStateV3 {
    let mut store = RedbStore::open(path).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x76; 10]).unwrap();
    store.initialize_database(database_id).unwrap();
    let history = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            N::ReplicationFollowerState.metadata_key().unwrap(),
            encode_replication_follower_state_v3(
                ReplicationFollowerStateV3::attached(history.lineage(), history.tail(), None)
                    .unwrap(),
            )
            .unwrap()
            .as_bytes(),
        )
        .unwrap();
    // Source-only populations are never part of a follower bootstrap.
    write
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .retain(|_, _| false)
        .unwrap();
    store.shared.commit_durable(write).unwrap();
    history
}

fn frame(history: ChangelogHistoryStateV3, mutations: Vec<Vec<Mutation>>) -> Vec<u8> {
    let mut successor = history;
    let receipts = mutations
        .into_iter()
        .map(|mutations| {
            let tail = successor.tail();
            let receipt = AuthoritativeTransactionV3::new(
                AuthoritativeTransactionBindingV3 {
                    database_id: history.lineage().database_id(),
                    history_incarnation: 1,
                    predecessor: Some(tail.sequence()),
                    sequence: tail.sequence().checked_next().unwrap(),
                    predecessor_frontier: tail.frontier(),
                    covered_frontier: tail.frontier(),
                    prior_history_hash: tail.history_hash(),
                },
                ChangelogAttributionV3::OutboxTransition,
                mutations,
            )
            .unwrap();
            successor = successor.advance(&receipt).unwrap();
            receipt
        })
        .collect();
    ChangelogFrameV3::new(
        ChangelogFrameBindingV3::new(
            history.lineage().database_id(),
            1,
            1,
            history.lineage().catalog_digest(),
            history.tail().history_hash(),
        )
        .unwrap(),
        receipts,
    )
    .unwrap()
    .encode()
    .unwrap()
}

fn isolated_applier(store: RedbFollowerStore) -> RedbFollowerApplier {
    // Only this module bypasses the separate structural/catalog proof join to
    // test physical transactions over deliberately opaque row values.
    RedbFollowerApplier::from_validated_shared(store.0.shared).unwrap()
}

#[test]
fn follower_handle_refuses_source_state_and_never_exposes_source_activation() {
    let scope = crate::test_path::ScopedDirectory::new("follower-open");
    let path = scope.join("db.redb");
    assert!(RedbFollowerStore::open(&path).is_err());
    assert!(!path.exists());
    let mut source = RedbStore::open(&path).unwrap();
    source
        .initialize_database(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x76; 10]).unwrap(),
        )
        .unwrap();
    drop(source);
    assert!(RedbFollowerStore::open(&path).is_err());
    let attached = scope.join("attached.redb");
    let history = fixture(&attached);
    assert!(RedbStore::open(&attached).is_err());
    let follower = RedbFollowerStore::open(&attached).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(
            &follower.0.shared.database.begin_read().unwrap(),
        )
        .unwrap(),
        Some(history)
    );
    assert_eq!(follower.0.shared.durable_commit_epoch(), 0);
    let dormant = RedbDormantPorts {
        shared: follower.0.shared,
        pending_v3_activation: None,
    };
    assert!(dormant.into_operational_after_catalog_validation().is_err());
}

#[test]
fn follower_frame_is_one_durable_transaction_and_repeated_receipts_are_read_only() {
    let scope = crate::test_path::ScopedDirectory::new("follower-atomic-frame");
    let path = scope.join("db.redb");
    let history = fixture(&path);
    let bytes = frame(
        history,
        vec![
            vec![Mutation::put(N::OutboxStatus, b"key", None, b"first").unwrap()],
            vec![Mutation::replace(N::OutboxStatus, b"key", b"first", b"second").unwrap()],
        ],
    );
    let mut applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    let point = applier.apply_frame(&bytes).unwrap();
    assert_eq!(point.sequence().get(), history.tail().sequence().get() + 2);
    assert_eq!(applier.shared.durable_commit_epoch(), 1);
    assert_eq!(applier.apply_frame(&bytes).unwrap(), point);
    assert_eq!(applier.shared.durable_commit_epoch(), 1);
    assert_eq!(applier.durable_position().unwrap(), point);
    let read = applier.shared.database.begin_read().unwrap();
    assert_eq!(
        read.open_table(OUTBOX_STATUS)
            .unwrap()
            .get(b"key".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"second"
    );
    assert!(
        read.open_table(META)
            .unwrap()
            .get(META_CLEAN_CLOSE_LIFECYCLE)
            .unwrap()
            .is_none()
    );
    drop(read);
    applier.close().unwrap();
    let mut reopened = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    assert_eq!(reopened.durable_position().unwrap(), point);
    assert_eq!(reopened.apply_frame(&bytes).unwrap(), point);
    assert_eq!(reopened.shared.durable_commit_epoch(), 0);
}

#[test]
fn follower_late_predecessor_failure_aborts_all_rows_and_fuses_the_applier() {
    let scope = crate::test_path::ScopedDirectory::new("follower-aborted-frame");
    let path = scope.join("db.redb");
    let history = fixture(&path);
    let bytes = frame(
        history,
        vec![
            vec![Mutation::put(N::OutboxStatus, b"key", None, b"first").unwrap()],
            vec![Mutation::replace(N::OutboxStatus, b"key", b"wrong", b"second").unwrap()],
        ],
    );
    let mut applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    assert!(applier.apply_frame(&bytes).is_err());
    assert_eq!(applier.shared.durable_commit_epoch(), 0);
    let read = applier.shared.database.begin_read().unwrap();
    assert!(
        read.open_table(OUTBOX_STATUS)
            .unwrap()
            .get(b"key".as_slice())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&read).unwrap(),
        Some(history)
    );
    drop(read);
    assert!(applier.apply_frame(&frame(history, vec![vec![]])).is_err());
    drop(applier);
    let reopened = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    assert_eq!(reopened.durable_position().unwrap(), history.tail());
}

#[test]
fn follower_apply_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_FOLLOWER_APPLY_DATABASE") else {
        return;
    };
    let store = RedbFollowerStore::open(&path).unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(
        &store.0.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    let bytes = frame(
        history,
        vec![
            vec![Mutation::put(N::OutboxStatus, b"key", None, b"first").unwrap()],
            vec![Mutation::replace(N::OutboxStatus, b"key", b"first", b"second").unwrap()],
        ],
    );
    let mut applier = isolated_applier(store);
    applier.apply_frame(&bytes).unwrap();
    applier.acknowledge_durable_position().unwrap();
    panic!("requested follower crash edge did not fire");
}

#[test]
fn follower_process_crashes_preserve_whole_frames_and_exact_retry_positions() {
    for edge in ["receipt", "roots", "committed"] {
        let scope = crate::test_path::ScopedDirectory::new("follower-frame-crash");
        let path = scope.join("db.redb");
        let original = fixture(&path);
        let bytes = frame(
            original,
            vec![
                vec![Mutation::put(N::OutboxStatus, b"key", None, b"first").unwrap()],
                vec![Mutation::replace(N::OutboxStatus, b"key", b"first", b"second").unwrap()],
            ],
        );
        let expected = riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(
            ChangelogFrameV3::decode(&bytes)
                .unwrap()
                .receipts()
                .last()
                .unwrap(),
        )
        .unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::follower_tests::follower_apply_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_FOLLOWER_APPLY_DATABASE", &path)
            .env("RIFFDB_FOLLOWER_APPLY_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        for attempt in 0..3 {
            let mut applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
            let committed = edge == "committed" || attempt > 0;
            assert_eq!(
                applier.durable_position().unwrap(),
                if committed { expected } else { original.tail() }
            );
            let read = applier.shared.database.begin_read().unwrap();
            let table = read.open_table(OUTBOX_STATUS).unwrap();
            let row = table.get(b"key".as_slice()).unwrap();
            assert_eq!(
                row.as_ref().map(|row| row.value()),
                committed.then_some(b"second".as_slice())
            );
            drop(row);
            drop(table);
            drop(read);
            assert_eq!(applier.apply_frame(&bytes).unwrap(), expected);
            assert_eq!(applier.shared.durable_commit_epoch(), u64::from(!committed));
            applier.close().unwrap();
        }
    }
}

#[test]
// req: REP-004, REC-002
fn follower_acknowledgement_crashes_never_allocate_or_rewrite_source_receipts() {
    use riffdb_storage_api::proto_codec::decode_replication_follower_state_v3;
    for edge in ["acknowledgement", "acknowledgement-committed"] {
        let scope = crate::test_path::ScopedDirectory::new("follower-ack-crash");
        let path = scope.join("db.redb");
        let original = fixture(&path);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::follower_tests::follower_apply_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_FOLLOWER_APPLY_DATABASE", &path)
            .env("RIFFDB_FOLLOWER_APPLY_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        for attempt in 0..3 {
            let mut applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
            let read = applier.shared.database.begin_read().unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap();
            assert_eq!(
                history.tail().sequence().get(),
                original.tail().sequence().get() + 2
            );
            let meta = read.open_table(META).unwrap();
            let row = meta
                .get(N::ReplicationFollowerState.metadata_key().unwrap())
                .unwrap()
                .unwrap();
            let (_, applied, acknowledged) = decode_replication_follower_state_v3(row.value())
                .unwrap()
                .value()
                .attached_state()
                .unwrap();
            assert_eq!(applied, history.tail());
            let committed = edge == "acknowledgement-committed" || attempt > 0;
            assert_eq!(acknowledged, committed.then_some(applied));
            let (observed_history, observed_state, _snapshot) =
                applier.capture_read_progress_snapshot().unwrap();
            assert_eq!(observed_history, history);
            assert_eq!(observed_state.attached_state().unwrap().1, applied);
            assert_eq!(observed_state.attached_state().unwrap().2, acknowledged);
            assert_eq!(applier.shared.durable_commit_epoch(), 0);
            drop(row);
            drop(meta);
            drop(read);
            assert_eq!(applier.acknowledge_durable_position().unwrap(), applied);
            assert_eq!(applier.acknowledge_durable_position().unwrap(), applied);
            assert_eq!(
                applier
                    .capture_read_progress_snapshot()
                    .unwrap()
                    .1
                    .attached_state()
                    .unwrap()
                    .2,
                Some(applied)
            );
            assert_eq!(applier.shared.durable_commit_epoch(), u64::from(!committed));
            let read = applier.shared.database.begin_read().unwrap();
            assert_eq!(
                crate::changelog_v3_roots::validate_retained_history(&read).unwrap(),
                Some(history)
            );
            drop(read);
            applier.close().unwrap();
        }
    }
}

#[test]
fn follower_refuses_substituted_frames_lineage_epoch_and_sequence_without_mutation() {
    for arm in 0..5 {
        let scope = crate::test_path::ScopedDirectory::new("follower-binding-refusal");
        let path = scope.join("db.redb");
        let history = fixture(&path);
        let original = frame(history, vec![vec![]]);
        let checked = ChangelogFrameV3::decode(&original).unwrap();
        let binding = checked.binding();
        let mut bytes = match arm {
            0 => {
                let mut bytes = original.clone();
                bytes[0] ^= 1;
                bytes
            }
            1 | 2 => ChangelogFrameV3::new(
                ChangelogFrameBindingV3::new(
                    binding.database_id(),
                    1,
                    if arm == 1 { 2 } else { 1 },
                    binding.catalog_digest(),
                    if arm == 2 {
                        [0x88; 32]
                    } else {
                        binding.prior_frame_hash()
                    },
                )
                .unwrap(),
                checked.receipts().to_vec(),
            )
            .unwrap()
            .encode()
            .unwrap(),
            3 => {
                let foreign = ChangelogLineageV3::new(
                    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x79; 10])
                        .unwrap(),
                    1,
                    LeadershipEpochV1::initial(),
                )
                .unwrap();
                frame(
                    ChangelogHistoryStateV3::new(
                        foreign,
                        history.anchor(),
                        history.tail(),
                        history.minimum_resume(),
                    )
                    .unwrap(),
                    vec![vec![]],
                )
            }
            _ => {
                let future = history.advance(&checked.receipts()[0]).unwrap();
                frame(future, vec![vec![]])
            }
        };
        let mut applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
        assert!(applier.apply_frame(&bytes).is_err(), "arm {arm}");
        assert_eq!(applier.shared.durable_commit_epoch(), 0);
        bytes.clear();
        assert!(applier.apply_frame(&original).is_err());
        drop(applier);
        let applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
        assert_eq!(applier.durable_position().unwrap(), history.tail());
    }
}

#[test]
fn follower_consumer_port_preserves_durable_position_across_reconnect() {
    let scope = crate::test_path::ScopedDirectory::new("follower-consumer-port");
    let path = scope.join("db.redb");
    let original = fixture(&path);
    let applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    let mut port: Box<dyn riffdb_storage_api::ChangelogFollowerApplyPortV3> = Box::new(applier);
    assert_eq!(port.resume_stream().unwrap(), original.tail());
    let bytes = frame(original, vec![vec![]]);
    let applied = port.apply_frame(&bytes).unwrap();
    assert_eq!(port.acknowledge_durable_position().unwrap(), applied);
    assert_eq!(port.resume_stream().unwrap(), applied);
    let history = original
        .advance(&ChangelogFrameV3::decode(&bytes).unwrap().receipts()[0])
        .unwrap();
    let next = port.apply_frame(&frame(history, vec![vec![]])).unwrap();
    assert_eq!(next.sequence(), applied.sequence().checked_next().unwrap());
    port.close().unwrap();
    let reopened = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    assert_eq!(reopened.durable_position().unwrap(), next);
}

#[test]
fn follower_never_copies_or_materializes_source_history_rows() {
    let scope = crate::test_path::ScopedDirectory::new("follower-source-only-history");
    let path = scope.join("db.redb");
    let original = fixture(&path);
    let bytes = frame(original, vec![vec![]]);
    let mut applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    applier.apply_frame(&bytes).unwrap();
    let read = applier.shared.database.begin_read().unwrap();
    assert!(
        read.open_table(crate::changelog_v3_activation::HISTORY)
            .unwrap()
            .is_empty()
            .unwrap()
    );
    assert!(
        read.open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .unwrap()
            .is_empty()
            .unwrap()
    );
}

#[test]
fn empty_history_requires_exact_attached_progress_and_no_source_controls() {
    use riffdb_storage_api::proto_codec::encode_replication_source_hold_v1;
    for arm in 0..5 {
        let scope = crate::test_path::ScopedDirectory::new("follower-empty-history-refusal");
        let path = scope.join("db.redb");
        let original = fixture(&path);
        let database = Database::open(&path).unwrap();
        let write = database.begin_write().unwrap();
        match arm {
            0 => {
                write
                    .open_table(META)
                    .unwrap()
                    .remove(N::ReplicationFollowerState.metadata_key().unwrap())
                    .unwrap();
            }
            1 | 2 => {
                let state = if arm == 1 {
                    ReplicationFollowerStateV3::detached()
                } else {
                    let point = riffdb_storage_api::ChangelogHistoryPointV3::new(
                        original.tail().sequence(),
                        [0x99; 32],
                        original.tail().frontier(),
                    );
                    ReplicationFollowerStateV3::attached(original.lineage(), point, None).unwrap()
                };
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        N::ReplicationFollowerState.metadata_key().unwrap(),
                        encode_replication_follower_state_v3(state)
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
            }
            3 => {
                let hold = riffdb_storage_api::ReplicationSourceHoldV1::new(
                    riffdb_storage_api::ReplicationSourceHoldIdV1::new([0x21; 16]).unwrap(),
                    riffdb_storage_api::ReplicationSourceHoldKindV1::Bootstrap,
                    original.lineage(),
                    original.tail(),
                );
                write
                    .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
                    .unwrap()
                    .insert(
                        hold.storage_key().as_slice(),
                        encode_replication_source_hold_v1(hold).unwrap().as_bytes(),
                    )
                    .unwrap();
            }
            _ => {
                write
                    .open_table(META)
                    .unwrap()
                    .insert(META_CLEAN_CLOSE_LIFECYCLE, b"source certificate".as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        assert!(
            crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
                .is_err(),
            "arm {arm}"
        );
        drop(database);
        assert!(RedbFollowerStore::open(&path).is_err(), "arm {arm}");
    }
}

#[test]
fn durable_applied_hash_rejects_changed_retry_payload_after_restart() {
    let scope = crate::test_path::ScopedDirectory::new("follower-retry-hash");
    let path = scope.join("db.redb");
    let original = fixture(&path);
    let bytes = frame(
        original,
        vec![vec![
            Mutation::put(N::OutboxStatus, b"key", None, b"real").unwrap(),
        ]],
    );
    let mut applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    let applied = applier.apply_frame(&bytes).unwrap();
    applier.close().unwrap();
    let changed = frame(
        original,
        vec![vec![
            Mutation::put(N::OutboxStatus, b"key", None, b"substituted").unwrap(),
        ]],
    );
    let mut applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    assert!(applier.apply_frame(&changed).is_err());
    assert_eq!(applier.shared.durable_commit_epoch(), 0);
    drop(applier);
    let mut applier = isolated_applier(RedbFollowerStore::open(&path).unwrap());
    assert_eq!(applier.apply_frame(&bytes).unwrap(), applied);
    assert_eq!(applier.shared.durable_commit_epoch(), 0);
}
