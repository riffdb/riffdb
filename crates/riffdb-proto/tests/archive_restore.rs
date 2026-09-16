//! Archive restore identity, exact-frontier and nested wire validation.
use prost::Message;
use riffdb_proto::{
    decode_public_message, v1, validate_public_message, validate_restore_archived_backup_exchange,
    validate_restore_offline_backup_exchange,
};
use riffdb_types::{
    ArchiveNameV1, ArchiveRestoreStopV1, BackupNameV1, CommitSequence,
    OfflineMaintenanceReplacementConfirmation, archive_restore_input_hash,
};

fn position(sequence: u64) -> v1::FrontierPosition {
    v1::FrontierPosition {
        position: Some(if sequence == 0 {
            v1::frontier_position::Position::BeforeFirst(v1::Unit {})
        } else {
            v1::frontier_position::Position::AppliedThrough(sequence)
        }),
    }
}
fn exchange(
    stop_at_sequence: Option<u64>,
) -> (
    v1::RestoreArchivedBackupRequest,
    v1::RestoreArchivedBackupResponse,
) {
    let id = vec![
        1, 155, 246, 170, 166, 64, 125, 230, 137, 201, 138, 127, 112, 187, 189, 35,
    ];
    let request = v1::RestoreArchivedBackupRequest {
        request_id: id.clone(),
        operation_id: id.clone(),
        backup_name: "backup".into(),
        archive_name: "archive".into(),
        replacement_confirmation:
            v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
        stop_at_sequence,
    };
    let stop = stop_at_sequence.map_or(ArchiveRestoreStopV1::LastArchived, |sequence| {
        ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(sequence).unwrap())
    });
    let hash = archive_restore_input_hash(
        &BackupNameV1::new("backup").unwrap(),
        &ArchiveNameV1::new("archive").unwrap(),
        stop,
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
    );
    let response = v1::RestoreArchivedBackupResponse {
        disposition: v1::OfflineMaintenanceStartDisposition::Terminal as i32,
        operation: Some(v1::OfflineMaintenanceOperation {
            operation_id: id,
            kind: v1::OfflineMaintenanceOperationKind::RestoreBackup as i32,
            backup_name: "backup".into(),
            input_hash: hash.as_bytes().to_vec(),
            phase: v1::OfflineMaintenancePhase::Succeeded as i32,
            failure: v1::OfflineMaintenanceFailureClass::Unspecified as i32,
            archive_restore: Some(v1::ArchiveRestoreObservation {
                archive_name: "archive".into(),
                stop_at_sequence,
                backup_frontier: Some(v1::ArchiveBackupFrontier {
                    application: Some(position(3)),
                }),
                restored_frontier: Some(v1::ReplicationFrontier {
                    application: Some(position(7)),
                    administration: Some(position(2)),
                }),
            }),
        }),
    };
    (request, response)
}

#[test]
// req: REP-007, AFC-007
fn archive_restore_exchange_binds_every_input_and_rejects_ordinary_restore() {
    for stop in [None, Some(7)] {
        let (request, response) = exchange(stop);
        validate_restore_archived_backup_exchange(&request, &response).unwrap();
        assert_eq!(
            decode_public_message::<v1::RestoreArchivedBackupResponse>(&response.encode_to_vec())
                .unwrap(),
            response
        );
        let mut changed_requests = Vec::new();
        let mut changed = request.clone();
        changed.backup_name = "different".into();
        changed_requests.push(changed);
        let mut changed = request.clone();
        changed.archive_name = "different".into();
        changed_requests.push(changed);
        let mut changed = request.clone();
        changed.stop_at_sequence = Some(8);
        changed_requests.push(changed);
        let mut changed = request.clone();
        changed.replacement_confirmation = 0;
        changed_requests.push(changed);
        let mut changed = request.clone();
        changed.operation_id[15] ^= 1;
        changed_requests.push(changed);
        for changed in changed_requests {
            assert!(validate_restore_archived_backup_exchange(&changed, &response).is_err());
        }
        let ordinary = v1::RestoreOfflineBackupRequest {
            request_id: request.request_id.clone(),
            operation_id: request.operation_id.clone(),
            backup_name: request.backup_name.clone(),
            replacement_confirmation: request.replacement_confirmation,
        };
        assert!(
            validate_restore_offline_backup_exchange(
                &ordinary,
                &v1::RestoreOfflineBackupResponse {
                    disposition: response.disposition,
                    operation: response.operation.clone()
                }
            )
            .is_err()
        );
        let mut missing = response.clone();
        missing.operation.as_mut().unwrap().archive_restore = None;
        assert!(validate_restore_archived_backup_exchange(&request, &missing).is_err());
        let mut different_archive = response.clone();
        different_archive
            .operation
            .as_mut()
            .unwrap()
            .archive_restore
            .as_mut()
            .unwrap()
            .archive_name = "substitute".into();
        assert!(validate_restore_archived_backup_exchange(&request, &different_archive).is_err());
    }
}

#[test]
// req: REP-007, AFC-007
fn archive_restore_wire_preserves_unknown_and_before_first_and_refuses_false_success() {
    let (mut request, response) = exchange(Some(7));
    request.stop_at_sequence = Some(0);
    assert!(validate_public_message(&request).is_err());
    for case in 0..6 {
        let mut invalid = response.clone();
        let detail = invalid
            .operation
            .as_mut()
            .unwrap()
            .archive_restore
            .as_mut()
            .unwrap();
        match case {
            0 => detail.backup_frontier = None,
            1 => detail.restored_frontier = None,
            2 => detail.backup_frontier.as_mut().unwrap().application = Some(position(8)),
            3 => detail.restored_frontier.as_mut().unwrap().application = Some(position(6)),
            4 => detail.restored_frontier.as_mut().unwrap().administration = None,
            _ => detail.backup_frontier.as_mut().unwrap().application = None,
        }
        assert!(validate_public_message(&invalid).is_err(), "case {case}");
    }
    let (_, mut accepted) = exchange(None);
    accepted.disposition = v1::OfflineMaintenanceStartDisposition::Accepted as i32;
    accepted.operation.as_mut().unwrap().phase = v1::OfflineMaintenancePhase::Accepted as i32;
    let detail = accepted
        .operation
        .as_mut()
        .unwrap()
        .archive_restore
        .as_mut()
        .unwrap();
    detail.backup_frontier = None;
    detail.restored_frontier = None;
    validate_public_message(&accepted).unwrap();
    let unknown_bytes = accepted.encode_to_vec();
    accepted
        .operation
        .as_mut()
        .unwrap()
        .archive_restore
        .as_mut()
        .unwrap()
        .backup_frontier = Some(v1::ArchiveBackupFrontier {
        application: Some(position(0)),
    });
    validate_public_message(&accepted).unwrap();
    assert_ne!(unknown_bytes, accepted.encode_to_vec());
    assert_eq!(
        decode_public_message::<v1::RestoreArchivedBackupResponse>(&accepted.encode_to_vec())
            .unwrap(),
        accepted
    );
}

fn nested(field: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    prost::encoding::encode_key(
        field,
        prost::encoding::WireType::LengthDelimited,
        &mut bytes,
    );
    prost::encoding::encode_varint(payload.len() as u64, &mut bytes);
    bytes.extend_from_slice(payload);
    bytes
}
#[test]
// req: REP-007, AFC-007
fn archive_restore_nested_wire_ignores_unknown_and_refuses_duplicate_fields() {
    let (_, response) = exchange(Some(7));
    let operation = response.operation.clone().unwrap();
    let detail = operation.archive_restore.as_ref().unwrap();
    for (unknown, suffix) in [
        (true, nested(99, b"hidden")),
        (false, nested(1, b"archive")),
    ] {
        let mut detail_bytes = detail.encode_to_vec();
        detail_bytes.extend(suffix);
        let mut base = operation.clone();
        base.archive_restore = None;
        let mut operation_bytes = base.encode_to_vec();
        operation_bytes.extend(nested(7, &detail_bytes));
        let mut response_bytes = vec![8, v1::OfflineMaintenanceStartDisposition::Terminal as u8];
        response_bytes.extend(nested(2, &operation_bytes));
        let decoded = decode_public_message::<v1::RestoreArchivedBackupResponse>(&response_bytes);
        if unknown {
            // ADR-0040 ignores public unknown fields and never relays them.
            assert_eq!(decoded.unwrap(), response);
        } else {
            assert!(decoded.is_err());
        }
    }
}
