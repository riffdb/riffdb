//! Receipt-authorized publication of an interior logical stop.
// req: REP-007, REC-001, AFC-007
use super::*;
use riffdb_types::*;

#[test]
fn private_archive_publication_restores_exact_prefix_in_both_profiles() {
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        check_private_archive_case(profile, None, Some("publish"));
    }
}

#[test]
fn private_archive_publication_requires_receipt_and_closed_authorization_readers() {
    for fault in ["no-publication-receipt", "held-publication-reader"] {
        check_private_archive_case(RedbCommitProfile::Hardened, None, Some(fault));
    }
}

pub(super) fn publish(
    candidate: riffdb_storage_redb::RedbValidatedPrivateArchiveRestore,
    maintenance: &mut RedbMaintenanceStorage,
    target: &Path,
    command: &CommandFixture,
    mode: &str,
) {
    let selection = candidate.selection().clone();
    let frontier = candidate.restored_frontier();
    assert_eq!(
        selection.terminal_frontier().application(),
        CommitSequence::new(6)
    );
    assert_eq!(frontier.application(), CommitSequence::new(2));
    let database_id = selection.lineage().database_id();
    let stop = ArchiveRestoreStopV1::AtApplicationSequence(frontier.application().unwrap());
    let id =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [2; 10]).unwrap();
    let name = BackupNameV1::new("baseline").unwrap();
    let archive = ArchiveNameV1::new("daily").unwrap();
    let confirmation = OfflineMaintenanceReplacementConfirmation::NotProvided;
    let mut receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
        id,
        name.clone(),
        archive.clone(),
        stop,
        archive_restore_input_hash(&name, &archive, stop, confirmation),
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
    let sealed = candidate
        .seal_after_validation(database_id, prefix_predecessors::inputs())
        .unwrap();
    assert_eq!(sealed.restored_frontier(), frontier);
    assert_eq!(sealed.selection(), &selection);
    if mode == "no-publication-receipt" {
        assert!(maintenance.publish_sealed_archive_restore(sealed).is_err());
        assert!(!target.exists());
        return;
    }
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
    let result = maintenance.publish_sealed_archive_restore(sealed).unwrap();
    assert!(
        matches!(result, OfflineArchiveRestorePublicationV3::Published {
        restored_frontier, published_history_incarnation: 2, backup_manifest
    } if restored_frontier == frontier
        && backup_manifest.database_id() == selection.backup().database_id()
        && backup_manifest.last_commit_sequence() == selection.backup().included_application_frontier())
    );
    drop(RedbStore::open(target).unwrap());
    let database = redb::ReadOnlyDatabase::open(target).unwrap();
    let read = database.begin_read().unwrap();
    let entities = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new(N::Entities.table()))
        .unwrap();
    let entity = entities
        .get(command.target.key().as_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_entity_record_v1(entity.value()).unwrap().value(),
        command.records.entities()[0].live_post_image().unwrap()
    );
    let meta = read
        .open_table(TableDefinition::<&str, &[u8]>::new("meta"))
        .unwrap();
    let root = meta
        .get(N::ChangelogHistoryState.metadata_key().unwrap())
        .unwrap()
        .unwrap();
    let history = *decode_changelog_history_state_v3(root.value())
        .unwrap()
        .value();
    assert_eq!(history.lineage().history_incarnation(), 2);
    assert_eq!(history.tail().frontier(), frontier);
    assert_eq!(history.tail().sequence().get(), 1);
    let rows = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new(
            N::ChangelogHistory.table(),
        ))
        .unwrap();
    assert_eq!(rows.len().unwrap(), 1);
    let row = rows.iter().unwrap().next().unwrap().unwrap();
    let anchor = AuthoritativeTransactionV3::decode(row.1.value()).unwrap();
    assert_eq!(anchor.attribution(), ChangelogAttributionV3::RestoreAnchor);
    assert!(anchor.mutations().is_empty());
    assert_eq!(anchor.binding().predecessor, None);
    assert_eq!(anchor.binding().prior_history_hash, [0; 32]);
    assert_eq!(anchor.binding().covered_frontier, frontier);
    assert_ne!(history.lineage(), selection.lineage());
}

#[cfg(feature = "test-fixtures")]
#[path = "v3_private_publication_crash.rs"]
mod crash;
