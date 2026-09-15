//! Exact immutable archive selection, request stops and separate restored frontiers.
// req: REP-007, AFC-007
use riffdb_storage_api::*;
use riffdb_types::{
    AdministrationSequence, ArchiveRestoreStopV1, CommitSequence, DatabaseId, DualFrontier,
};

fn manifest() -> ArchiveManifestV1 {
    let hex = include_str!("../../../fixtures/replication/archive-manifest-v1-unencrypted-3.hex");
    let bytes = hex
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    ArchiveManifestV1::decode(&bytes).unwrap()
}
fn identity(manifest: ArchiveManifestV1) -> OfflineBackupManifestIdentityV1 {
    OfflineBackupManifestIdentityV1::new(
        BackupIntegrityChecksumV1::new(manifest.full_backup_manifest_digest().to_vec()).unwrap(),
        manifest.lineage().database_id(),
        None,
    )
}
fn selection() -> ArchiveRestoreSelectionV3 {
    let terminal = manifest();
    ArchiveRestoreSelectionV3::new(
        identity(terminal),
        terminal.lineage(),
        terminal.backup_fence(),
        ArchiveRestoreSuffixV3::Terminal(Box::new(terminal)),
    )
    .unwrap()
}

#[test]
fn archive_selection_retains_exact_terminal_bytes_and_distinct_restored_frontier() {
    let selected = selection();
    let terminal = manifest();
    let ArchiveRestoreSuffixV3::Terminal(retained) = selected.suffix() else {
        panic!("terminal expected")
    };
    assert_eq!(retained.encode(), terminal.encode());
    assert_eq!(selected.terminal_frontier(), terminal.covered().frontier());
    assert_eq!(
        selected
            .target_application(ArchiveRestoreStopV1::LastArchived)
            .unwrap(),
        CommitSequence::new(3)
    );
    let exact = ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(2).unwrap());
    let restored = DualFrontier::new(CommitSequence::new(2), None);
    assert!(selected.validate_restored_frontier(exact, restored).is_ok());
    assert!(
        selected
            .validate_restored_frontier(exact, terminal.covered().frontier())
            .is_err()
    );
    assert!(
        selected
            .validate_restored_frontier(ArchiveRestoreStopV1::LastArchived, restored)
            .is_err()
    );
    assert_eq!(
        retained.encode(),
        terminal.encode(),
        "an earlier stop never relabels the source receipt"
    );
    assert_eq!(
        format!("{selected:?}"),
        "ArchiveRestoreSelectionV3([redacted])"
    );
}

#[test]
fn archive_selection_refuses_foreign_backup_lineage_and_fence() {
    let terminal = manifest();
    let other_database = DatabaseId::from_unix_milliseconds_and_random(2000, [2; 10]).unwrap();
    let wrong_identities = [
        OfflineBackupManifestIdentityV1::new(
            BackupIntegrityChecksumV1::new(vec![0x44; 32]).unwrap(),
            terminal.lineage().database_id(),
            None,
        ),
        OfflineBackupManifestIdentityV1::new(
            BackupIntegrityChecksumV1::new(terminal.full_backup_manifest_digest().to_vec())
                .unwrap(),
            other_database,
            None,
        ),
        OfflineBackupManifestIdentityV1::new(
            BackupIntegrityChecksumV1::new(vec![0x55; 31]).unwrap(),
            terminal.lineage().database_id(),
            None,
        ),
    ];
    for backup in wrong_identities {
        assert!(
            ArchiveRestoreSelectionV3::new(
                backup,
                terminal.lineage(),
                terminal.backup_fence(),
                ArchiveRestoreSuffixV3::Terminal(Box::new(terminal))
            )
            .is_err()
        );
    }
    for lineage in [
        ChangelogLineageV3::new(
            terminal.lineage().database_id(),
            2,
            LeadershipEpochV1::new(1).unwrap(),
        )
        .unwrap(),
        ChangelogLineageV3::new(
            terminal.lineage().database_id(),
            1,
            LeadershipEpochV1::new(2).unwrap(),
        )
        .unwrap(),
    ] {
        assert!(
            ArchiveRestoreSelectionV3::new(
                identity(terminal),
                lineage,
                terminal.backup_fence(),
                ArchiveRestoreSuffixV3::Terminal(Box::new(terminal))
            )
            .is_err()
        );
    }
    let wrong_fence = ChangelogHistoryPointV3::new(
        terminal.backup_fence().sequence(),
        [0x99; 32],
        terminal.backup_fence().frontier(),
    );
    assert!(
        ArchiveRestoreSelectionV3::new(
            identity(terminal),
            terminal.lineage(),
            wrong_fence,
            ArchiveRestoreSuffixV3::Terminal(Box::new(terminal))
        )
        .is_err()
    );
}

#[test]
fn archive_selection_empty_suffix_keeps_complete_backup_frontier_after_pruning() {
    let terminal = manifest();
    let frontier = DualFrontier::new(CommitSequence::new(5), AdministrationSequence::new(7));
    let fence = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(10).unwrap(),
        [0x33; 32],
        frontier,
    );
    // The manifest's last retained command can be absent after pruning.
    let selected = ArchiveRestoreSelectionV3::new(
        identity(terminal),
        terminal.lineage(),
        fence,
        ArchiveRestoreSuffixV3::Empty,
    )
    .unwrap();
    assert_eq!(
        selected
            .target_application(ArchiveRestoreStopV1::LastArchived)
            .unwrap(),
        CommitSequence::new(5)
    );
    assert!(
        selected
            .validate_restored_frontier(ArchiveRestoreStopV1::LastArchived, frontier)
            .is_ok()
    );
    let exact = ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(5).unwrap());
    assert!(selected.validate_restored_frontier(exact, frontier).is_ok());
    for value in [1, 4, 6, u64::MAX] {
        assert!(
            selected
                .target_application(ArchiveRestoreStopV1::AtApplicationSequence(
                    CommitSequence::new(value).unwrap()
                ))
                .is_err()
        );
    }
    assert!(
        selected
            .validate_restored_frontier(
                exact,
                DualFrontier::new(CommitSequence::new(5), AdministrationSequence::new(8))
            )
            .is_err()
    );
    assert!(
        selected
            .validate_restored_frontier(
                exact,
                DualFrontier::new(CommitSequence::new(5), AdministrationSequence::new(6))
            )
            .is_err()
    );

    // Retained command history may lag the complete backup, but cannot lead it.
    for (retained, valid) in [(3, true), (5, true), (6, false)] {
        let backup = OfflineBackupManifestIdentityV1::new(
            identity(terminal).manifest_checksum().clone(),
            terminal.lineage().database_id(),
            CommitSequence::new(retained),
        );
        assert_eq!(
            ArchiveRestoreSelectionV3::new(
                backup,
                terminal.lineage(),
                fence,
                ArchiveRestoreSuffixV3::Empty
            )
            .is_ok(),
            valid
        );
    }
    let empty_frontier = DualFrontier::new(None, AdministrationSequence::new(7));
    let empty_fence = ChangelogHistoryPointV3::new(fence.sequence(), [0x44; 32], empty_frontier);
    let empty = ArchiveRestoreSelectionV3::new(
        identity(terminal),
        terminal.lineage(),
        empty_fence,
        ArchiveRestoreSuffixV3::Empty,
    )
    .unwrap();
    assert_eq!(
        empty
            .target_application(ArchiveRestoreStopV1::LastArchived)
            .unwrap(),
        None
    );
    assert!(
        empty
            .validate_restored_frontier(ArchiveRestoreStopV1::LastArchived, empty_frontier)
            .is_ok()
    );
    assert!(
        empty
            .target_application(ArchiveRestoreStopV1::AtApplicationSequence(
                CommitSequence::first()
            ))
            .is_err()
    );
}
