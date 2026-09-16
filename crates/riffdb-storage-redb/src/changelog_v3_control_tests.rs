//! Retention mechanics over isolated physical receipt fixtures. Application and
//! journal authority are additionally covered by the real-command recovery gate.
// req: REP-003, REC-001, PERF-007, STO-012

use redb::{ReadableDatabase, ReadableTable};
use riffdb_storage_api::{
    ChangelogAttributionV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    DatabaseInitializationPort, LeadershipEpochV1, ReplicationSourceHoldIdV1,
    ReplicationSourceHoldKindV1 as Kind, ReplicationSourceHoldV1 as Hold,
};
use riffdb_types::{DatabaseId, DualFrontier};

use crate::{RedbOperationalPorts, RedbStore};

#[path = "changelog_bootstrap_attachment_tests.rs"]
mod bootstrap_attachment_tests;
#[path = "changelog_source_control_refusal_tests.rs"]
mod refusal_tests;

fn history(ports: &RedbOperationalPorts) -> ChangelogHistoryStateV3 {
    crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap()
}

fn append(ports: &RedbOperationalPorts, value: &[u8]) -> ChangelogHistoryStateV3 {
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        crate::RedbCommitProfile::Hardened,
        ChangelogAttributionV3::OutboxTransition,
    )
    .unwrap();
    write
        .open_table(crate::layout::OUTBOX_STATUS)
        .unwrap()
        .insert(b"opaque-retention-key".as_slice(), value)
        .unwrap();
    write.finish().unwrap().commit(&ports.shared).unwrap();
    history(ports)
}

pub(crate) fn fixture() -> (
    crate::test_path::ScopedDirectory,
    RedbOperationalPorts,
    Vec<ChangelogHistoryStateV3>,
) {
    let scope = crate::test_path::ScopedDirectory::new("v3-source-control");
    let (ports, states) = initialize(&scope.join("db.redb"));
    (scope, ports, states)
}

fn initialize(path: &std::path::Path) -> (RedbOperationalPorts, Vec<ChangelogHistoryStateV3>) {
    let mut store = RedbStore::open(path).unwrap();
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10]).unwrap();
    store.initialize_database(database).unwrap();
    let mut transaction = store.shared.database.begin_write().unwrap();
    let anchor = crate::changelog_v3_activation::stage_validated(
        &mut transaction,
        ChangelogLineageV3::new(database, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    store.shared.commit_durable(transaction).unwrap();
    let ports = RedbOperationalPorts {
        shared: store.shared,
    };
    let mut states = vec![anchor];
    for byte in 1..=6 {
        states.push(append(&ports, &[byte]));
    }
    (ports, states)
}

fn hold(kind: Kind, state: ChangelogHistoryStateV3) -> Hold {
    Hold::new(
        ReplicationSourceHoldIdV1::new([kind as u8; 16]).unwrap(),
        kind,
        state.lineage(),
        state.tail(),
    )
}

#[test]
fn v3_materialized_reclamation_waits_for_later_checkpoint_and_every_kind_of_hold() {
    let (_scope, ports, states) = fixture();
    let mut control = ports.replication_source_control();
    let follower = hold(Kind::FollowerAcknowledgement, states[1]);
    let archive = hold(Kind::ArchiveAcknowledgement, states[2]);
    let bootstrap = hold(Kind::Bootstrap, states[3]);
    for fence in [follower, archive, bootstrap] {
        assert!(control.register(fence).unwrap());
        let exact = history(&ports);
        assert!(!control.register(fence).unwrap());
        assert_eq!(history(&ports), exact);
    }
    assert!(
        !control.reclaim_history().unwrap(),
        "first observation proves no later checkpoint"
    );
    assert!(
        !control.reclaim_history().unwrap(),
        "re-observing the same root is not a checkpoint"
    );
    assert_eq!(history(&ports).minimum_resume(), states[0].tail());
    append(&ports, b"later checkpoint");
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), follower.fence());
    assert!(
        !control.reclaim_history().unwrap(),
        "reclamation cannot stimulate itself"
    );

    let advanced = hold(Kind::FollowerAcknowledgement, states[4]);
    assert!(control.advance_acknowledgement(advanced).unwrap());
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), archive.fence());
    assert!(
        control
            .advance_acknowledgement(hold(Kind::ArchiveAcknowledgement, states[5]))
            .unwrap()
    );
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), bootstrap.fence());
    let exact = history(&ports);
    assert!(
        control
            .advance_acknowledgement(hold(Kind::Bootstrap, states[6]))
            .is_err()
    );
    assert!(control.register(hold(Kind::Bootstrap, states[6])).is_err());
    assert_eq!(
        history(&ports),
        exact,
        "a bootstrap fence never moves implicitly"
    );
}

#[test]
fn v3_reclamation_never_uses_newer_materialization_as_its_prior_checkpoint() {
    let (_scope, ports, states) = fixture();
    let mut control = ports.replication_source_control();
    assert!(!control.reclaim_history().unwrap());
    let later = append(&ports, b"newer materialized receipt");
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), states[6].tail());
    assert!(history(&ports).minimum_resume().sequence() < later.tail().sequence());
    assert!(!control.reclaim_history().unwrap());
    let mut fresh_handle = ports.replication_source_control();
    assert!(
        !fresh_handle.reclaim_history().unwrap(),
        "lost observation delays pruning safely"
    );
}

// req: PRJ-001, PRJ-002, PRJ-004, REP-003
#[test]
fn derived_source_pin_retains_exact_tail_until_successor_selection() {
    use riffdb_storage_api::OwnedSnapshotReader;
    let (_scope, ports, states) = fixture();
    let captured = ports
        .open_owned_snapshot()
        .unwrap()
        .pin_derived_source_v3()
        .unwrap();
    let selected = captured.at(states[2].tail()).unwrap();
    let mut control = ports.replication_source_control();
    assert!(!control.reclaim_history().unwrap());
    append(&ports, b"writes after selected provider");
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), states[2].tail());
    let successor = ports
        .open_owned_snapshot()
        .unwrap()
        .pin_derived_source_v3()
        .unwrap();
    let mut cursor = successor.receipts_after(&selected).unwrap();
    let mut observed = selected.position();
    while let Some(receipt) = cursor.next_receipt().unwrap() {
        assert_eq!(receipt.binding().predecessor, Some(observed.sequence()));
        observed = riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(&receipt).unwrap();
    }
    assert_eq!(observed, successor.position());
    // A cloned query/candidate custody keeps the old fence until its last owner.
    let retained = selected.clone();
    drop(selected);
    append(&ports, b"successor checkpoint durable");
    assert!(!control.reclaim_history().unwrap());
    drop(retained);
    append(&ports, b"later checkpoint after custody release");
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), captured.position());
    drop(captured);
    append(&ports, b"successor selected");
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), successor.position());
}

// req: PRJ-001, PRJ-002, PRJ-004, REP-003
#[test]
fn derived_source_pin_cannot_resurrect_pruned_history_or_substitute_a_source() {
    use riffdb_storage_api::{
        ChangelogCursorErrorV3, ChangelogHistoryPointV3, OwnedSnapshotReader, StorageErrorKind,
    };
    let (_scope, ports, states) = fixture();
    let old = ports
        .open_owned_snapshot()
        .unwrap()
        .pin_derived_source_v3()
        .unwrap();
    let mut control = ports.replication_source_control();
    assert!(!control.reclaim_history().unwrap());
    append(&ports, b"later durable root");
    assert!(control.reclaim_history().unwrap());
    assert_eq!(history(&ports).minimum_resume(), old.position());
    assert!(
        matches!(old.at(states[2].tail()), Err(ChangelogCursorErrorV3::Storage(error)) if error.kind() == StorageErrorKind::HistoryPruned)
    );
    let bad = ChangelogHistoryPointV3::new(
        old.position().sequence(),
        [0; 32],
        old.position().frontier(),
    );
    assert!(matches!(
        old.at(bad),
        Err(ChangelogCursorErrorV3::InvalidPosition)
    ));
    let (_other_scope, other, _) = fixture();
    let foreign = other
        .open_owned_snapshot()
        .unwrap()
        .pin_derived_source_v3()
        .unwrap();
    assert!(matches!(
        foreign.receipts_after(&old),
        Err(ChangelogCursorErrorV3::ForeignLineage)
    ));
}

#[test]
fn v3_reclamation_is_bounded_and_pinned_readers_keep_original_receipts() {
    use crate::{
        changelog_v3_activation::HISTORY, checkpoint_root::CheckpointRoot, store::RedbReadAccess,
    };
    use std::sync::Arc;
    let (_scope, ports, states) = fixture();
    for sequence in 7_u64..=270 {
        append(&ports, &sequence.to_be_bytes());
    }
    let root = Arc::new(CheckpointRoot::new(
        ports.shared.database.begin_read().unwrap(),
        1,
    ));
    let before = history(&ports);
    let mut cursor = crate::changelog_v3_cursor::open(
        &RedbReadAccess::Durable(Arc::clone(&root)),
        before.lineage(),
        states[0].tail(),
    )
    .unwrap();
    let exact: Vec<_> = root
        .open_table(HISTORY)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect();
    let mut control = ports.replication_source_control();
    assert!(!control.reclaim_history().unwrap());
    append(&ports, b"later bounded checkpoint");
    assert!(control.reclaim_history().unwrap());
    let after = history(&ports);
    assert_eq!(
        after.minimum_resume().sequence().get(),
        before.minimum_resume().sequence().get() + 256
    );
    assert_eq!(
        after.tail().sequence().get(),
        before.tail().sequence().get() + 2
    );
    let pin = ports.shared.database.begin_read().unwrap();
    let table = pin.open_table(HISTORY).unwrap();
    for (index, (key, bytes)) in exact.iter().enumerate() {
        if index < 256 {
            assert!(table.get(key.as_slice()).unwrap().is_none());
        } else {
            assert_eq!(table.get(key.as_slice()).unwrap().unwrap().value(), bytes);
        }
        assert_eq!(
            root.open_table(HISTORY)
                .unwrap()
                .get(key.as_slice())
                .unwrap()
                .unwrap()
                .value(),
            bytes
        );
    }
    for (_, bytes) in exact.iter().skip(1) {
        assert_eq!(
            cursor.next_receipt().unwrap().unwrap().encode().unwrap(),
            *bytes
        );
    }
    assert!(cursor.next_receipt().unwrap().is_none());
    let latest = RedbReadAccess::Durable(Arc::new(CheckpointRoot::new(pin, 2)));
    assert!(
        matches!(crate::changelog_v3_cursor::open(&latest, before.lineage(), before.minimum_resume()),
        Err(riffdb_storage_api::ChangelogCursorErrorV3::Storage(error)) if error.kind() == riffdb_storage_api::StorageErrorKind::HistoryPruned)
    );
}

#[test]
fn v3_source_control_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_V3_SOURCE_CONTROL_PATH") else {
        return;
    };
    let store = RedbStore::open(path).unwrap();
    let ports = RedbOperationalPorts {
        shared: store.shared,
    };
    let initial = history(&ports);
    let mut control = ports.replication_source_control();
    let edge = std::env::var("RIFFDB_V3_SOURCE_CONTROL_EDGE").unwrap();
    if edge.starts_with("hold-") {
        control.register(hold(Kind::Bootstrap, initial)).unwrap();
    } else {
        assert!(!control.reclaim_history().unwrap());
        append(&ports, b"known-later-checkpoint");
        control.reclaim_history().unwrap();
    }
    panic!("selected crash edge was not reached");
}

#[test]
pub(crate) fn v3_source_control_crashes_leave_whole_holds_or_whole_reclamation() {
    use crate::changelog_v3_activation::{HISTORY, SOURCE_HOLDS};
    use riffdb_storage_api::{
        AuthoritativeTransactionV3, proto_codec::decode_replication_source_hold_v1,
    };
    use std::process::{Command, Stdio};
    for edge in [
        "hold-staged",
        "hold-receipted",
        "hold-committed",
        "reclamation-planned",
        "reclamation-deleted",
        "reclamation-staged",
        "reclamation-committed",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("v3-control-crash");
        let path = scope.join("db.redb");
        let (ports, states) = initialize(&path);
        let initial = history(&ports);
        let original: Vec<_> = ports
            .shared
            .database
            .begin_read()
            .unwrap()
            .open_table(HISTORY)
            .unwrap()
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .collect();
        drop(ports);
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "changelog_v3_control_tests::v3_source_control_crash_child",
            ])
            .env("RIFFDB_V3_SOURCE_CONTROL_PATH", &path)
            .env("RIFFDB_V3_SOURCE_CONTROL_EDGE", edge)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "{edge}");
        let committed = edge.ends_with("-committed");
        let holding = edge.starts_with("hold-");
        let mut observed = None;
        for _ in 0..2 {
            let store = RedbStore::open(&path).unwrap();
            let ports = RedbOperationalPorts {
                shared: store.shared,
            };
            let recovered = history(&ports);
            if let Some(previous) = observed {
                assert_eq!(recovered, previous);
            }
            observed = Some(recovered);
            assert_eq!(
                recovered.tail().sequence().get(),
                initial.tail().sequence().get() + u64::from(!holding) + u64::from(committed)
            );
            assert_eq!(
                recovered.minimum_resume(),
                if !holding && committed {
                    initial.tail()
                } else {
                    states[0].tail()
                }
            );
            let pin = ports.shared.database.begin_read().unwrap();
            let table = pin.open_table(HISTORY).unwrap();
            for (key, bytes) in &original {
                let value = table.get(key.as_slice()).unwrap();
                if !holding && committed && *key != initial.tail().sequence().get().to_be_bytes() {
                    assert!(value.is_none());
                } else {
                    assert_eq!(value.unwrap().value(), bytes);
                }
            }
            let source = pin.open_table(SOURCE_HOLDS).unwrap();
            let fence = hold(Kind::Bootstrap, initial);
            let retained = source.get(fence.storage_key().as_slice()).unwrap();
            if holding && committed {
                assert_eq!(
                    *decode_replication_source_hold_v1(retained.unwrap().value())
                        .unwrap()
                        .value(),
                    fence
                );
            } else {
                assert!(retained.is_none());
            }
            if committed {
                let receipt = AuthoritativeTransactionV3::decode(
                    table
                        .get(recovered.tail().sequence().get().to_be_bytes().as_slice())
                        .unwrap()
                        .unwrap()
                        .value(),
                )
                .unwrap();
                assert!(receipt.mutations().is_empty());
                assert_eq!(
                    receipt.attribution(),
                    if holding {
                        ChangelogAttributionV3::ReplicationSourceHold
                    } else {
                        ChangelogAttributionV3::HistoryReclamation
                    }
                );
                assert_eq!(
                    receipt.binding().predecessor_frontier,
                    receipt.binding().covered_frontier
                );
            }
        }
    }
}
