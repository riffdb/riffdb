//! Sink refusal and lost confirmation cannot gate source durability or overstate recovery.
// req: REP-007, REC-001, AFC-007
use super::*;
use riffdb_storage_api::*;
use riffdb_storage_redb::{
    RedbArchiveRepository, RedbMaintenanceStorage, RedbOfflineBackup, RedbVerifiedArchiveBackup,
};
use riffdb_types::{ArchiveRestoreStopV1, BackupNameV1, OfflineMaintenanceOperationId};
use std::sync::atomic::AtomicBool;

const ROOT: &str = "RIFFDB_ARCHIVE_SINK_CRASH_ROOT";
const AFTER_PERSIST: &str = "RIFFDB_ARCHIVE_SINK_CRASH_AFTER_PERSIST";

// Exercise both legal unavailable outcomes at the real repository boundary:
// no second publication, or durable publication whose confirmation was lost.
struct FailingSink {
    repository: RedbArchiveRepository,
    calls: usize,
    after_persist: bool,
}
impl ArchiveFrameSinkV1 for FailingSink {
    fn persist(&mut self, frame: &ArchiveFrameV1) -> Result<(), ArchiveConsumerErrorV1> {
        self.calls += 1;
        assert!(self.calls <= 2);
        if self.calls == 1 || self.after_persist {
            self.repository.persist(frame)?;
        }
        if self.calls == 2 {
            Err(ArchiveConsumerErrorV1::SinkUnavailable)
        } else {
            Ok(())
        }
    }
}

fn emit(
    ports: &RedbOperationalPorts,
    anchor: ChangelogHistoryStateV3,
    after: ChangelogHistoryPointV3,
) -> Vec<u8> {
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let handshake = ReplicationHandshakeV3::new(
        anchor.lineage(),
        after,
        ChangelogFrameV3::IDENTITY,
        anchor.lineage().catalog_digest(),
        MAX_CHANGELOG_FRAME_BYTES as u64,
        MAX_STAGED_COMMANDS as u64,
    )
    .unwrap();
    let mut cursor = ChangelogFrameCursorV3::open(pin.as_ref(), handshake).unwrap();
    let frame = cursor.next_coalesced_frame().unwrap().unwrap().into_bytes();
    assert!(cursor.next_coalesced_frame().unwrap().is_none());
    frame
}

#[test]
fn archive_sink_failure_crash_child() {
    let Ok(root) = std::env::var(ROOT) else {
        return;
    };
    let root = PathBuf::from(root);
    let profile = match std::env::var(CHILD_COMMIT_PROFILE).unwrap().as_str() {
        "standard" => RedbCommitProfile::Standard,
        "hardened" => RedbCommitProfile::Hardened,
        _ => panic!("unknown test profile"),
    };
    let path = root.join("source.redb");
    let anchor = install_fixture(&path);
    let backups = root.join("backups");
    std::fs::create_dir(&backups).unwrap();
    let backup = backups.join("baseline");
    RedbOfflineBackup::bind(&path, &backup)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "0123456789abcdef", "rustc-1.97.0", 1, vec![])
                .unwrap(),
        )
        .unwrap();
    let repository = RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(
            &root.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
    let mut consumer = ArchiveConsumerV1::new(
        FailingSink {
            repository,
            calls: 0,
            after_persist: std::env::var(AFTER_PERSIST).unwrap() == "true",
        },
        anchor.lineage(),
        anchor.tail(),
    );
    let (ports, _receiver) = observed_ports(&path, profile);
    commit_command_group(&ports, &[command_fixture_at(1)]);
    consumer.begin_stream().unwrap();
    let confirmed = consumer
        .append(emit(&ports, anchor, anchor.tail()))
        .unwrap();
    assert_eq!(confirmed.frontier().application(), CommitSequence::new(1));

    commit_command_group(&ports, &[command_fixture_at(2)]);
    consumer.begin_stream().unwrap();
    assert_eq!(
        consumer.append(emit(&ports, anchor, confirmed)),
        Err(ArchiveConsumerErrorV1::SinkUnavailable)
    );
    assert_eq!(consumer.position(), confirmed);
    assert!(consumer.has_pending_frame());

    // Still commit and prove complete command authority with the unavailable
    // sink and its uncertain pending frame alive. Do not cleanly close either.
    commit_command_group(&ports, &[command_fixture_at(3)]);
    for ordinal in 1..=3 {
        assert_command_graph(&ports, &command_fixture_at(ordinal));
    }
    assert_eq!(consumer.position(), confirmed);
    std::process::exit(98);
}

#[test]
fn archive_sink_failure_crash_preserves_source_and_only_durable_archive_prefix() {
    for profile in ["standard", "hardened"] {
        for after_persist in [false, true] {
            let scope = ScratchScope::new("archive-sink-crash");
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "v3_command_receipts::archive_sink_crash::archive_sink_failure_crash_child",
                ])
                .env(ROOT, scope.path())
                .env(CHILD_COMMIT_PROFILE, profile)
                .env(AFTER_PERSIST, after_persist.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .status()
                .unwrap();
            assert_eq!(
                status.code(),
                Some(98),
                "{profile}, after_persist={after_persist}"
            );
            // Repeated ordinary source startup preserves acknowledged commands,
            // even though commands 2/3 never received collector confirmation.
            for _ in 0..2 {
                let ports =
                    open_operational(RedbStore::open(scope.path().join("source.redb")).unwrap());
                for ordinal in 1..=3 {
                    let fixture = command_fixture_at(ordinal);
                    assert_command_graph(&ports, &fixture);
                    assert_eq!(
                        ports.read_entity(&fixture.target).unwrap(),
                        fixture.records.entities()[0].live_post_image().cloned()
                    );
                }
            }
            let expected = if after_persist { 2 } else { 1 };
            let backups = scope.path().join("backups");
            let repository = RedbVerifiedArchiveBackup::open(&backups.join("baseline"))
                .unwrap()
                .open_archive(
                    &scope.path().join("archive"),
                    ArchiveEncryptionPostureV1::Unencrypted,
                )
                .unwrap();
            assert_eq!(
                repository.position().frontier().application(),
                CommitSequence::new(expected)
            );
            let target = scope.path().join("restored.redb");
            let (maintenance, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
            let stage = maintenance
                .stage_recovery_restore_candidate(
                    OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
                        1000, [42; 10],
                    )
                    .unwrap(),
                    &BackupNameV1::new("baseline").unwrap(),
                    prefix_predecessors::inputs(),
                )
                .unwrap()
                .begin_archive_replay(repository, &AtomicBool::new(false))
                .unwrap();
            let candidate = stage
                .prepare_restore(
                    ArchiveRestoreStopV1::LastArchived,
                    prefix_predecessors::inputs(),
                    Arc::new(AtomicBool::new(false)),
                )
                .unwrap();
            let snapshot = candidate.authorization_snapshot().unwrap();
            assert_eq!(
                snapshot.application_frontier().unwrap(),
                CommitSequence::new(expected)
            );
            for ordinal in 1..=3 {
                let fixture = command_fixture_at(ordinal);
                assert_eq!(
                    snapshot.read_entity(&fixture.target).unwrap(),
                    if ordinal <= expected {
                        fixture.records.entities()[0].live_post_image().cloned()
                    } else {
                        None
                    }
                );
            }
            assert!(
                !target.exists(),
                "validated replay grants no publication authority"
            );
        }
    }
}
