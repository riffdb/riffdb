//! Real process termination at journaled source and checkpoint boundaries.
// req: STO-030, STO-031, STO-032, REC-001, REC-002
use super::*;
use crate::changelog_v3_activation::HISTORY;
use riffdb_storage_api::AuthoritativeTransactionV3;

#[test]
fn journaled_source_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_SOURCE_JOURNAL_CRASH_PATH") else {
        return;
    };
    let store = RedbStore::open(path).unwrap();
    let ports = RedbOperationalPorts {
        shared: store.shared,
    };
    let mut control = ports.replication_source_control();
    observe_first_online_checkpoint(&mut control);
    append(&ports, b"known-later-checkpoint");
    let mut removed = 0;
    for _ in 0..16 {
        removed += control.reclaim_history_online().unwrap().0;
        if removed > 0 {
            break;
        }
    }
    assert!(removed > 0, "fixture must reach its licensed journaled cut");
    // A certificate for the acknowledged cut survives materialization of that
    // same cut. Checkpointing cannot count as independent new source progress.
    let qualification = ports
        .shared
        .source_continuation_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let write = ports
        .begin_attributed_write_unless_checkpointing(
            ChangelogAttributionV3::OutboxTransition,
            true,
            &mut crate::store::WriteBeginTiming::default(),
        )
        .unwrap()
        .unwrap();
    write.abort().unwrap();
    assert_eq!(
        ports
            .shared
            .source_continuation_epoch
            .load(std::sync::atomic::Ordering::Acquire),
        qualification
    );
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    if std::env::var("RIFFDB_SOURCE_JOURNAL_NORMAL").as_deref() == Ok("true") {
        return;
    }
    panic!("selected journaled source crash edge was not reached");
}

#[test]
fn journaled_source_reclamation_crashes_preserve_atomic_floor_and_receipt() {
    use std::process::{Command, Stdio};
    // The before-publication edge follows the actual durable journal receipt;
    // it must recover the whole cut even though no public ack was returned.
    for edge in [
        "reclamation-planned",
        "reclamation-before-publication",
        "reclamation-committed",
        "checkpoint-staged",
        "checkpoint-committed",
        "normal",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("journaled-source-crash");
        let path = scope.join("db.redb");
        let (ports, _) = initialize(&path);
        let before = history(&ports);
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
                (
                    key.value().to_vec(),
                    crate::history_stub::resolve_history_value(value.value())
                        .unwrap()
                        .to_vec(),
                )
            })
            .collect();
        drop(ports);
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args([
                "--exact",
                "changelog_v3_control_tests::journal_crashes::journaled_source_crash_child",
                "--nocapture",
            ])
            .env("RIFFDB_SOURCE_JOURNAL_CRASH_PATH", &path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if edge.starts_with("checkpoint-") {
            child.env("RIFFDB_V3_RECOVERY_EDGE", edge);
        } else if edge == "normal" {
            child.env("RIFFDB_SOURCE_JOURNAL_NORMAL", "true");
        } else {
            child.env("RIFFDB_V3_SOURCE_CONTROL_EDGE", edge);
        }
        assert_eq!(
            child.status().unwrap().code(),
            Some(if edge == "normal" { 0 } else { 93 }),
            "{edge}"
        );
        let cut = edge != "reclamation-planned";
        let mut previous = None;
        for _ in 0..2 {
            let store = RedbStore::open(&path).unwrap();
            let ports = RedbOperationalPorts {
                shared: store.shared,
            };
            let actual = history(&ports);
            assert_eq!(
                actual.tail().sequence().get(),
                before.tail().sequence().get() + 1 + u64::from(cut),
                "{edge}"
            );
            assert_eq!(actual.tail().frontier(), before.tail().frontier());
            assert_eq!(
                actual.minimum_resume(),
                if cut {
                    before.tail()
                } else {
                    before.minimum_resume()
                },
                "{edge}"
            );
            if let Some(previous) = previous {
                assert_eq!(actual, previous, "recovery is idempotent");
            }
            previous = Some(actual);
            let root = ports.shared.database.begin_read().unwrap();
            let table = root.open_table(HISTORY).unwrap();
            for (key, canonical) in &original {
                let row = table.get(key.as_slice()).unwrap();
                if cut && key.as_slice() < before.tail().sequence().get().to_be_bytes().as_slice() {
                    assert!(
                        row.is_none(),
                        "all and only licensed prefix rows are removed"
                    );
                } else {
                    assert_eq!(
                        &*crate::history_stub::resolve_history_value(row.unwrap().value()).unwrap(),
                        canonical
                    );
                }
            }
            let terminal = table
                .get(actual.tail().sequence().get().to_be_bytes().as_slice())
                .unwrap()
                .unwrap();
            let receipt = AuthoritativeTransactionV3::decode(
                &crate::history_stub::resolve_history_value(terminal.value()).unwrap(),
            )
            .unwrap();
            assert_eq!(
                receipt.attribution(),
                if cut {
                    ChangelogAttributionV3::HistoryReclamation
                } else {
                    ChangelogAttributionV3::OutboxTransition
                }
            );
            if cut {
                assert!(receipt.mutations().is_empty());
            }
            assert_eq!(
                root.open_table(crate::layout::OUTBOX_STATUS)
                    .unwrap()
                    .get(b"opaque-retention-key".as_slice())
                    .unwrap()
                    .unwrap()
                    .value(),
                b"known-later-checkpoint"
            );
        }
    }
}
