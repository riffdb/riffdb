//! One exact-stop preparation owner covers receipt boundaries and private cuts.
// req: REP-007, REC-001, AFC-007
use super::*;
use riffdb_types::*;

#[test]
fn archive_preparation_covers_standard_physical_and_interior_stops() {
    check_private_archive_case(RedbCommitProfile::Standard, None, Some("prepare"));
}
#[test]
fn archive_preparation_covers_hardened_physical_and_interior_stops() {
    check_private_archive_case(RedbCommitProfile::Hardened, None, Some("prepare"));
}
#[test]
fn archive_preparation_requires_closed_readers_and_unchanged_validated_bytes() {
    for fault in ["prepare-held", "prepare-changed"] {
        check_private_archive_case(RedbCommitProfile::Hardened, None, Some(fault));
    }
}
#[test]
fn archive_preparation_covers_the_exact_positive_backup_fence() {
    check_private_archive_case(RedbCommitProfile::Hardened, None, Some("prepare-backup"));
}

#[allow(clippy::too_many_arguments)]
pub(super) fn check(
    stage: riffdb_storage_redb::RedbArchiveRestoreStage,
    maintenance: &mut RedbMaintenanceStorage,
    target: &Path,
    predecessor: &CommandFixture,
    commands: &[CommandFixture],
    stop: u64,
    cancel: bool,
    fault: &str,
) {
    let choice = if stop == 0 {
        ArchiveRestoreStopV1::LastArchived
    } else {
        ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(stop).unwrap())
    };
    let path = stage.staged_database_file().to_path_buf();
    let result = stage.prepare_restore(
        choice,
        prefix_predecessors::inputs(),
        Arc::new(AtomicBool::new(cancel)),
    );
    if cancel || stop == 7 {
        assert_eq!(
            result.err().unwrap().kind(),
            if cancel {
                StorageErrorKind::Unavailable
            } else {
                StorageErrorKind::CorruptData
            }
        );
        assert!(!target.exists());
        assert!(!path.exists());
        return;
    }
    let candidate = result.unwrap();
    let actual = if stop == 0 { 6 } else { stop };
    let expected = if actual == 1 {
        predecessor
    } else {
        &commands[actual as usize - 2]
    };
    if fault == "prepare-changed" {
        validation_refusals::tamper(&path, "missing-locator");
        assert!(candidate.authorization_snapshot().is_err());
        let database = candidate_database(&candidate);
        assert!(candidate.seal_after_authorization(database).is_err());
        assert!(!target.exists());
        return;
    }
    let snapshot = candidate.authorization_snapshot().unwrap();
    if fault == "prepare-held" {
        let database = candidate_database(&candidate);
        let reader = snapshot.clone();
        drop(snapshot);
        assert!(candidate.seal_after_authorization(database).is_err());
        drop(reader);
        assert!(!target.exists());
        return;
    }
    assert_eq!(
        snapshot.read_entity(&predecessor.target).unwrap(),
        expected.records.entities()[0].live_post_image().cloned()
    );
    assert_eq!(
        snapshot.application_frontier().unwrap(),
        CommitSequence::new(actual)
    );
    drop(snapshot);
    let frontier = candidate.restored_frontier();
    let selection = candidate.selection().clone();
    assert_eq!(
        selection.terminal_frontier().application(),
        CommitSequence::new(6)
    );
    assert_eq!(frontier.application(), CommitSequence::new(actual));
    if stop == 0 {
        assert_eq!(frontier, selection.terminal_frontier());
    } else {
        assert!(frontier.administration() < selection.terminal_frontier().administration());
    }
    let database_id = selection.lineage().database_id();
    let id =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [stop as u8; 10])
            .unwrap();
    let name = BackupNameV1::new("baseline").unwrap();
    let archive = ArchiveNameV1::new("daily").unwrap();
    let confirmation = OfflineMaintenanceReplacementConfirmation::NotProvided;
    let mut receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
        id,
        name.clone(),
        archive.clone(),
        choice,
        archive_restore_input_hash(&name, &archive, choice, confirmation),
        confirmation,
        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1000, [32; 10]).unwrap(),
            None,
        ),
        None,
    )
    .unwrap();
    receipt.record_selection(selection.clone()).unwrap();
    receipt
        .record_validated_restore(database_id, frontier)
        .unwrap();
    maintenance
        .create_or_read_archive_receipt(&receipt)
        .unwrap();
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ))
        .unwrap();
    receipt.record_published_incarnation(2).unwrap();
    maintenance.replace_archive_receipt(&receipt).unwrap();
    let sealed = candidate.seal_after_authorization(database_id).unwrap();
    assert_eq!(sealed.selection(), &selection);
    assert_eq!(sealed.restored_frontier(), frontier);
    assert!(
        matches!(maintenance.publish_sealed_archive_restore(sealed).unwrap(),
        OfflineArchiveRestorePublicationV3::Published {restored_frontier, published_history_incarnation:2, ..} if restored_frontier == frontier)
    );
    drop(RedbStore::open(target).unwrap());
    let database = redb::ReadOnlyDatabase::open(target).unwrap();
    let read = database.begin_read().unwrap();
    let entities = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new(N::Entities.table()))
        .unwrap();
    let row = entities.get(predecessor.target.key().as_bytes()).unwrap();
    assert_eq!(
        row.map(|v| decode_entity_record_v1(v.value()).unwrap().into_parts().0),
        expected.records.entities()[0].live_post_image().cloned()
    );
}

fn candidate_database(candidate: &riffdb_storage_redb::RedbPreparedArchiveRestore) -> DatabaseId {
    candidate.selection().lineage().database_id()
}
