//! A private exact-stop artifact is not a source or a follower.
// req: REP-007, REC-001
use super::*;
use riffdb_storage_api::*;
use riffdb_storage_redb::{RedbMaintenanceStorage, RedbOfflineBackup, RedbVerifiedArchiveBackup};
use riffdb_types::{BackupNameV1, OfflineMaintenanceOperationId};
use std::sync::atomic::AtomicBool;

#[test]
fn private_archive_reconstruction_restores_standard_group_without_source_identity() {
    check_private_archive(RedbCommitProfile::Standard, None);
}

#[test]
fn private_archive_reconstruction_restores_hardened_group_without_source_identity() {
    check_private_archive(RedbCommitProfile::Hardened, None);
}

fn check_private_archive(profile: RedbCommitProfile, root: Option<PathBuf>) {
    check_private_archive_case(profile, root, None);
}

fn check_private_archive_case(
    profile: RedbCommitProfile,
    root: Option<PathBuf>,
    fault: Option<&str>,
) {
    let scope = root.map_or_else(|| ScratchScope::new("private-archive-prefix"), ScratchScope);
    let path = scope.path().join("source.redb");
    let mut anchor = install_fixture(&path);
    if fault == Some("prepare-backup") {
        let (ports, receiver) = observed_ports(&path, profile);
        commit_command_group(&ports, &[command_fixture_at(1)]);
        drop(receiver);
        drop(ports);
    }
    let backups = scope.path().join("backups");
    std::fs::create_dir(&backups).unwrap();
    let backup = backups.join("baseline");
    RedbOfflineBackup::bind(&path, &backup)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "0123456789abcdef", "rustc-1.97.0", 1, vec![])
                .unwrap(),
        )
        .unwrap();
    if fault == Some("prepare-backup") {
        anchor = RedbVerifiedArchiveBackup::open(&backup).unwrap().history();
    }
    let backup_bytes = std::fs::read(backup.join("database.redb")).unwrap();
    let (mut ports, receiver) = observed_ports(&path, profile);
    let predecessor = command_fixture_at(1);
    if fault != Some("prepare-backup") {
        commit_command_group(&ports, std::slice::from_ref(&predecessor));
    }
    let first = superseding_command_fixture_at(2, 1, &predecessor);
    let second = superseding_command_fixture_at(3, 1, &first);
    let third = deleting_command_fixture_at(4, &second);
    let fourth = build_command_fixture(5, 1, Some(&third), AdmissionShape::VacantTerminal, false);
    let fifth = superseding_command_fixture_at(6, 1, &fourth);
    let commands = [first, second, third, fourth, fifth];
    if profile == RedbCommitProfile::Standard {
        let epoch = ports.begin_deferred_command_epoch().unwrap();
        let batch = stage_command_group(
            DeferredCommandEpoch::begin_empty_batch(epoch).unwrap(),
            &commands,
        );
        let epoch = batch
            .apply_unpublished_with_service_audit_transitions(
                DurabilityMode::Sync,
                commands.iter().map(command_audit_transition).collect(),
            )
            .unwrap();
        DeferredCommandEpoch::fence(epoch).unwrap();
    } else {
        commit_command_group(&ports, &commands);
    }
    if fault.is_some_and(|f| f.starts_with("prepare")) {
        match ports
            .submit_service_audit_group(&[standalone_audit_intent(0xa1, 1_700_000_161)])
            .unwrap()
        {
            ServiceAuditGroupAppend::Submitted(fence) => {
                fence.wait().unwrap();
            }
            ServiceAuditGroupAppend::Complete(_) => {}
        }
    }
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let mut cursor = pin
        .changelog_receipts_v3(anchor.lineage(), anchor.tail())
        .unwrap();
    let mut rows = Vec::new();
    while let Some(receipt) = cursor.next_receipt().unwrap() {
        rows.push(receipt);
    }
    drop(cursor);
    let original = rows
        .iter()
        .flat_map(|r| r.mutations())
        .filter(|m| m.namespace() == N::Commits)
        .map(|m| {
            decode_command_segment_v1(m.value().unwrap())
                .unwrap()
                .into_parts()
                .0
        })
        .find(|s| s.commands().len() == 5)
        .unwrap();
    let frame = ChangelogFrameV3::new(
        ChangelogFrameBindingV3::new(
            anchor.lineage().database_id(),
            anchor.lineage().history_incarnation(),
            anchor.lineage().leadership_epoch().get(),
            anchor.lineage().catalog_digest(),
            anchor.tail().history_hash(),
        )
        .unwrap(),
        rows,
    )
    .unwrap()
    .encode()
    .unwrap();
    drop(pin);
    drop(receiver);
    drop(ports);
    let archive = scope.path().join("archive");
    let repository = RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
        .unwrap();
    let mut consumer = ArchiveConsumerV1::new(repository, anchor.lineage(), anchor.tail());
    consumer.begin_stream().unwrap();
    consumer.append(frame).unwrap();
    drop(consumer);
    let target = scope.path().join("configured.redb");
    for (stop, cancel) in [
        (0, false),
        (1, false),
        (2, false),
        (3, false),
        (4, false),
        (5, false),
        (6, false),
        (7, false),
        (2, true),
    ] {
        let preparation = fault.is_some_and(|f| f.starts_with("prepare"));
        if matches!(fault, Some("prepare-held" | "prepare-changed")) && (stop != 1 || cancel) {
            continue;
        }
        if !preparation && (stop == 0 || (fault.is_some() && (stop != 2 || cancel))) {
            continue;
        }
        let target = if preparation {
            scope.path().join(format!("prepared-{stop}-{cancel}.redb"))
        } else {
            target.clone()
        };
        let backups = if preparation {
            let case_root = scope.path().join(format!("case-{stop}-{cancel}"));
            let case_backup = case_root.join("baseline");
            std::fs::create_dir_all(&case_backup).unwrap();
            for name in [
                "database.redb",
                "journal.riffextent",
                "format.riffdb",
                "manifest.riffdb",
            ] {
                std::fs::copy(backup.join(name), case_backup.join(name)).unwrap();
            }
            case_root
        } else {
            backups.clone()
        };
        let (mut maintenance, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
        let repository = RedbVerifiedArchiveBackup::open(&backup)
            .unwrap()
            .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
            .unwrap();
        let stage = maintenance
            .stage_recovery_restore_candidate(
                OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
                    1000,
                    [stop as u8; 10],
                )
                .unwrap(),
                &BackupNameV1::new("baseline").unwrap(),
                prefix_predecessors::inputs(),
            )
            .unwrap()
            .begin_archive_replay(repository, &AtomicBool::new(false))
            .unwrap();
        if preparation {
            preparation::check(
                stage,
                &mut maintenance,
                &target,
                &predecessor,
                &commands,
                stop,
                cancel,
                fault.unwrap(),
            );
            assert_eq!(
                std::fs::read(backup.join("database.redb")).unwrap(),
                backup_bytes
            );
            continue;
        }
        let applier = prefix_predecessors::open_follower(stage.staged_database_file());
        let replayed = stage.replay(applier, &AtomicBool::new(false)).unwrap();
        let selection = replayed.selection().clone();
        let candidate_path = replayed.staged_database_file().to_path_buf();
        let result = replayed.reconstruct_inside_group(
            CommitSequence::new(stop).unwrap(),
            prefix_predecessors::inputs(),
            &AtomicBool::new(cancel),
        );
        if cancel || !(2..=5).contains(&stop) {
            assert_eq!(
                result.err().unwrap().kind(),
                if cancel {
                    StorageErrorKind::Unavailable
                } else {
                    StorageErrorKind::CorruptData
                }
            );
            assert!(!candidate_path.exists());
            assert!(!target.exists());
            continue;
        }
        let candidate = result.unwrap();
        let before_validation = validation_refusals::rows(&candidate_path);
        if matches!(
            fault,
            Some("changed-frontier" | "missing-locator" | "restore-marker")
        ) {
            validation_refusals::tamper(&candidate_path, fault.unwrap());
        }
        let result = candidate.validate(
            prefix_predecessors::inputs(),
            Arc::new(AtomicBool::new(fault == Some("cancel-validation"))),
        );
        if matches!(
            fault,
            Some("changed-frontier" | "missing-locator" | "restore-marker" | "cancel-validation")
        ) {
            assert_eq!(
                result.err().unwrap().kind(),
                if fault == Some("cancel-validation") {
                    StorageErrorKind::Unavailable
                } else {
                    StorageErrorKind::CorruptData
                }
            );
            assert!(!candidate_path.exists());
            assert!(!target.exists());
            continue;
        }
        let candidate = result.unwrap();
        assert_eq!(
            validation_refusals::rows(&candidate_path),
            before_validation
        );
        if fault == Some("changed-after-validation") {
            validation_refusals::tamper(&candidate_path, "missing-locator");
            assert_eq!(
                candidate.authorization_snapshot().err().unwrap().kind(),
                StorageErrorKind::CorruptData
            );
            candidate.discard().unwrap();
            assert!(!candidate_path.exists());
            assert!(!target.exists());
            continue;
        }
        let snapshot = candidate.authorization_snapshot().unwrap();
        assert_eq!(
            snapshot.read_entity(&commands[0].target).unwrap(),
            commands[stop as usize - 2].records.entities()[0]
                .live_post_image()
                .cloned()
        );
        assert_eq!(
            snapshot.application_frontier().unwrap(),
            CommitSequence::new(stop)
        );
        if fault == Some("held-publication-reader") {
            let clone = snapshot.clone();
            drop(snapshot);
            assert!(
                candidate
                    .seal_after_validation(
                        anchor.lineage().database_id(),
                        prefix_predecessors::inputs()
                    )
                    .is_err()
            );
            drop(clone);
            assert!(!target.exists());
            continue;
        }
        drop(snapshot);
        if matches!(fault, Some("publish" | "no-publication-receipt")) {
            publication::publish(
                candidate,
                &mut maintenance,
                &target,
                &commands[0],
                fault.unwrap(),
            );
            assert_eq!(
                std::fs::read(backup.join("database.redb")).unwrap(),
                backup_bytes
            );
            continue;
        }
        assert_eq!(candidate.selection(), &selection);
        assert_eq!(
            candidate.restored_frontier().application(),
            CommitSequence::new(stop)
        );
        let candidate_path = candidate.staged_database_file().to_path_buf();
        assert!(RedbStore::open(&candidate_path).is_err());
        assert!(riffdb_storage_redb::RedbFollowerStore::open(&candidate_path).is_err());
        let database = redb::ReadOnlyDatabase::open(&candidate_path).unwrap();
        let read = database.begin_read().unwrap();
        let entities = read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(N::Entities.table()))
            .unwrap();
        let value = entities.get(commands[0].target.key().as_bytes()).unwrap();
        assert_eq!(
            value
                .as_ref()
                .map(|v| decode_entity_record_v1(v.value()).unwrap().into_parts().0),
            commands[stop as usize - 2].records.entities()[0]
                .live_post_image()
                .cloned()
        );
        let commits = read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(N::Commits.table()))
            .unwrap();
        assert_eq!(commits.len().unwrap(), 2);
        let segment_row = commits.last().unwrap().unwrap();
        let segment = decode_command_segment_v1(segment_row.1.value()).unwrap();
        assert_eq!(
            segment.value().commands(),
            &original.commands()[..stop as usize - 1]
        );
        assert_eq!(
            segment.value().predecessor_segment_digest(),
            original.predecessor_segment_digest()
        );
        assert_ne!(segment.value().segment_digest(), original.segment_digest());
        assert_eq!(
            candidate.restored_frontier(),
            segment
                .value()
                .commands()
                .last()
                .unwrap()
                .prefix_evidence()
                .unwrap()
                .covered()
        );
        let history = read.open_table(HISTORY).unwrap();
        assert!(history.is_empty().unwrap());
        drop(history);
        for namespace in [N::EntityChainHeads, N::IndexEpochs, N::SecondaryIndexes] {
            let table = read
                .open_table(TableDefinition::<&[u8], &[u8]>::new(namespace.table()))
                .unwrap();
            for mutation in original.commands()[stop as usize - 2]
                .prefix_evidence()
                .unwrap()
                .mutations()
                .iter()
                .filter(|m| m.namespace() == namespace)
            {
                assert_eq!(
                    table
                        .get(mutation.key())
                        .unwrap()
                        .as_ref()
                        .map(|v| v.value()),
                    mutation.value()
                );
            }
        }
        for namespace in [
            N::IdempotencyLocators,
            N::ProvenanceLocators,
            N::AuditByRequestLocators,
        ] {
            let table = read
                .open_table(TableDefinition::<&[u8], &[u8]>::new(namespace.table()))
                .unwrap();
            assert_eq!(
                table.len().unwrap(),
                stop * if namespace == N::AuditByRequestLocators {
                    2
                } else {
                    1
                }
            );
        }
        let meta = read.open_table(META).unwrap();
        assert!(
            meta.get(N::ReplicationFollowerState.metadata_key().unwrap())
                .unwrap()
                .is_none()
        );
        drop(meta);
        drop(segment);
        drop(segment_row);
        drop(commits);
        drop(value);
        drop(entities);
        drop(read);
        drop(database);
        candidate.discard().unwrap();
        assert!(!candidate_path.exists());
        assert!(!target.exists());
        assert_eq!(
            std::fs::read(backup.join("database.redb")).unwrap(),
            backup_bytes
        );
    }
}

#[cfg(feature = "test-fixtures")]
#[path = "v3_private_archive_crash.rs"]
mod crash;

#[path = "v3_private_archive_validation.rs"]
mod validation_refusals;

#[path = "v3_private_archive_publication.rs"]
mod publication;

#[path = "v3_archive_preparation.rs"]
mod preparation;
