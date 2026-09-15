//! Archive-only receipt phase, exact retry and immutable replay evidence.
// req: REP-007, AFC-007
use riffdb_storage_api::*;
use riffdb_types::*;

fn selected(last: u64) -> ArchiveRestoreSelectionV3 {
    let text = match last {
        3 => include_str!("../../../fixtures/replication/archive-manifest-v1-unencrypted-3.hex"),
        5 => include_str!("../../../fixtures/replication/archive-manifest-v1-unencrypted-5.hex"),
        _ => panic!("fixture terminal"),
    };
    let bytes = text
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    let manifest = ArchiveManifestV1::decode(&bytes).unwrap();
    let backup = OfflineBackupManifestIdentityV1::new(
        BackupIntegrityChecksumV1::new(manifest.full_backup_manifest_digest().to_vec()).unwrap(),
        manifest.lineage().database_id(),
        None,
    );
    ArchiveRestoreSelectionV3::new(
        backup,
        manifest.lineage(),
        manifest.backup_fence(),
        ArchiveRestoreSuffixV3::Terminal(Box::new(manifest)),
    )
    .unwrap()
}
fn database(seed: u8) -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1234, [seed; 10]).unwrap()
}
fn accepted(source: bool) -> OfflineMaintenanceReceiptV3 {
    let backup = BackupNameV1::new("before-upgrade").unwrap();
    let archive = ArchiveNameV1::new("daily").unwrap();
    let stop = ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(2).unwrap());
    let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
    OfflineMaintenanceReceiptV3::accepted_archive_restore(
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1234, [4; 10]).unwrap(),
        backup.clone(),
        archive.clone(),
        stop,
        archive_restore_input_hash(&backup, &archive, stop, confirmation),
        confirmation,
        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("private-operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1234, [5; 10]).unwrap(),
            None,
        ),
        source.then(|| database(2)),
    )
    .unwrap()
}
fn phase(receipt: &mut OfflineMaintenanceReceiptV3, phase: OfflineMaintenanceReceiptPhaseV1) {
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
        .unwrap();
}
fn restored() -> DualFrontier {
    DualFrontier::new(CommitSequence::new(2), None)
}

#[test]
fn archive_receipt_requires_offline_exact_selection_replay_and_incarnation_before_publication() {
    use OfflineMaintenanceReceiptPhaseV1::*;
    let mut receipt = accepted(true);
    let original = receipt.clone();
    assert!(receipt.record_selection(selected(3)).is_err());
    assert_eq!(receipt, original);
    assert!(
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(Offline))
            .is_err()
    );
    phase(&mut receipt, Draining);
    assert!(receipt.record_selection(selected(3)).is_err());
    phase(&mut receipt, Offline);
    let offline = receipt.clone();
    assert!(
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                ArtifactPublished
            ))
            .is_err()
    );
    assert_eq!(receipt, offline);
    assert!(
        receipt
            .record_validated_restore(selected(3).lineage().database_id(), restored())
            .is_err()
    );
    receipt.record_selection(selected(3)).unwrap();
    let selected_receipt = receipt.clone();
    assert!(
        receipt.record_selection(selected(5)).is_err(),
        "later CURRENT cannot alter accepted selection"
    );
    assert_eq!(receipt, selected_receipt);
    assert!(receipt.record_published_incarnation(2).is_err());
    assert!(
        receipt
            .record_validated_restore(database(9), restored())
            .is_err()
    );
    assert!(
        receipt
            .record_validated_restore(
                selected(3).lineage().database_id(),
                selected(3).terminal_frontier()
            )
            .is_err()
    );
    receipt
        .record_validated_restore(selected(3).lineage().database_id(), restored())
        .unwrap();
    assert!(receipt.record_published_incarnation(1).is_err());
    receipt.record_published_incarnation(2).unwrap();
    assert!(receipt.record_published_incarnation(3).is_err());
    assert!(receipt.monotonically_extends(&original));
    assert!(receipt.monotonically_extends(&selected_receipt));
    assert!(!selected_receipt.monotonically_extends(&receipt));
    for next in [ArtifactPublished, Validating, Succeeded] {
        phase(&mut receipt, next);
    }
    assert_eq!(receipt.restored_frontier(), Some(restored()));
    assert_eq!(receipt.selection(), Some(&selected(3)));
    assert_eq!(receipt.published_history_incarnation(), Some(2));
    assert!(!format!("{receipt:?}").contains("private-operator"));
    let terminal = receipt.clone();
    assert!(
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::failed(
                OfflineMaintenanceReceiptFailureV1::InternalFailure
            ))
            .is_err()
    );
    assert_eq!(receipt, terminal);
}

#[test]
fn archive_receipt_source_less_candidate_can_be_bound_before_admission_but_cannot_publish_early() {
    use OfflineMaintenanceReceiptPhaseV1::*;
    let mut receipt = accepted(false);
    receipt.record_selection(selected(3)).unwrap();
    receipt
        .record_validated_restore(selected(3).lineage().database_id(), restored())
        .unwrap();
    assert_eq!(receipt.current_phase(), Accepted);
    assert!(receipt.record_published_incarnation(2).is_err());
    assert!(
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                ArtifactPublished
            ))
            .is_err()
    );
    phase(&mut receipt, Offline);
    receipt.record_published_incarnation(2).unwrap();
    phase(&mut receipt, ArtifactPublished);
    phase(&mut receipt, Validating);
    phase(&mut receipt, Succeeded);
    assert_eq!(receipt.source_database_id(), None);
}

#[test]
fn archive_receipt_failed_before_selection_cannot_acquire_new_replay_evidence() {
    let mut receipt = accepted(false);
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::failed(
            OfflineMaintenanceReceiptFailureV1::ArtifactInvalid,
        ))
        .unwrap();
    let failed = receipt.clone();
    assert!(receipt.record_selection(selected(3)).is_err());
    assert!(
        receipt
            .record_validated_restore(selected(3).lineage().database_id(), restored())
            .is_err()
    );
    assert!(receipt.record_published_incarnation(2).is_err());
    assert_eq!(receipt, failed);
}

#[test]
fn archive_receipt_canonical_reconstruction_refuses_impossible_presence_and_regressing_history() {
    use OfflineMaintenanceReceiptPhaseV1::*;
    let receipt = accepted(false);
    let started = vec![OfflineMaintenanceReceiptTransitionV1::phase(Accepted)];
    let offline = vec![
        OfflineMaintenanceReceiptTransitionV1::phase(Accepted),
        OfflineMaintenanceReceiptTransitionV1::phase(Offline),
    ];
    let staged = selected(3).lineage().database_id();
    let cases = [
        (None, None, None, None, Vec::new()),
        (
            None,
            None,
            None,
            None,
            vec![OfflineMaintenanceReceiptTransitionV1::phase(Accepted); 17],
        ),
        (
            None,
            None,
            None,
            None,
            vec![
                OfflineMaintenanceReceiptTransitionV1::phase(Accepted),
                OfflineMaintenanceReceiptTransitionV1::phase(Offline),
                OfflineMaintenanceReceiptTransitionV1::phase(Accepted),
            ],
        ),
        (None, Some(staged), Some(restored()), None, offline.clone()),
        (
            Some(selected(3)),
            None,
            Some(restored()),
            None,
            offline.clone(),
        ),
        (Some(selected(3)), Some(staged), None, None, offline.clone()),
        (None, None, None, Some(2), offline.clone()),
        (
            Some(selected(3)),
            Some(staged),
            Some(selected(3).terminal_frontier()),
            None,
            offline.clone(),
        ),
        (
            Some(selected(3)),
            Some(database(9)),
            Some(restored()),
            None,
            offline.clone(),
        ),
        (
            Some(selected(3)),
            Some(staged),
            Some(restored()),
            Some(1),
            offline.clone(),
        ),
        (
            Some(selected(3)),
            Some(staged),
            Some(restored()),
            Some(2),
            started,
        ),
        // A failure after publication cannot discard evidence required for that earlier phase.
        (
            Some(selected(3)),
            Some(staged),
            Some(restored()),
            None,
            vec![
                OfflineMaintenanceReceiptTransitionV1::phase(Accepted),
                OfflineMaintenanceReceiptTransitionV1::phase(Offline),
                OfflineMaintenanceReceiptTransitionV1::phase(ArtifactPublished),
                OfflineMaintenanceReceiptTransitionV1::failed(
                    OfflineMaintenanceReceiptFailureV1::ValidationFailed,
                ),
            ],
        ),
    ];
    for (selection, staged, restored, incarnation, transitions) in cases {
        assert!(
            OfflineMaintenanceReceiptV3::from_canonical_parts(
                receipt.operation_id(),
                receipt.backup_name().clone(),
                receipt.archive_name().clone(),
                receipt.stop(),
                receipt.input_hash(),
                receipt.replacement_confirmation(),
                receipt.admission().clone(),
                receipt.source_database_id(),
                selection,
                staged,
                restored,
                incarnation,
                transitions,
            )
            .is_err()
        );
    }
    assert!(
        !accepted(true).monotonically_extends(&receipt),
        "source-less and ordinary routes are immutable"
    );
    assert!(!receipt.monotonically_extends(&accepted(true)));
    assert!(
        OfflineMaintenanceReceiptV3::from_canonical_parts(
            receipt.operation_id(),
            receipt.backup_name().clone(),
            receipt.archive_name().clone(),
            receipt.stop(),
            OfflineMaintenanceInputHash::from_bytes([0; 32]),
            receipt.replacement_confirmation(),
            receipt.admission().clone(),
            receipt.source_database_id(),
            None,
            None,
            None,
            None,
            receipt.transitions().to_vec(),
        )
        .is_err()
    );
    assert_eq!(
        OfflineMaintenanceReceiptV3::from_canonical_parts(
            receipt.operation_id(),
            receipt.backup_name().clone(),
            receipt.archive_name().clone(),
            receipt.stop(),
            receipt.input_hash(),
            receipt.replacement_confirmation(),
            receipt.admission().clone(),
            receipt.source_database_id(),
            None,
            None,
            None,
            None,
            receipt.transitions().to_vec(),
        )
        .unwrap(),
        receipt
    );
}
