// req: REP-005
use super::codec::{decode_promotion_receipt, encode_promotion_receipt};
use riffdb_storage_api::ReplicationPromotionFailureV1 as Failure;
use riffdb_storage_api::{
    AuditPrincipalV1, AuthoritativeStateCatalogV2, ChangelogHistoryPointV3 as Point,
    ChangelogHistoryStateV3 as History, ChangelogLineageV3 as Lineage,
    ChangelogTransactionSequence as Sequence, PrimaryFenceSourceEvidenceV1 as Evidence,
    ReplicationFollowerStateV3 as Follower, ReplicationPromotionRequestV1 as Request,
    ReplicationPromotionSelectionV1 as Selection, StoredPrimaryFenceAdministrationV1 as Fence,
};
use riffdb_storage_api::{
    ReplicationPromotionPhaseV1 as Phase, ReplicationPromotionReceiptV1 as Receipt,
    ReplicationPromotionStepV1 as Step,
};
use riffdb_types::ApprovalId;
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, CapabilityId, CommitSequence, DatabaseId,
    DualFrontier, LeadershipEpochV1, ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1,
    ReplicationPromotionOperationId, ReplicationSourceHoldIdV1, RequestId, Timestamp,
};
use sha2::{Digest, Sha256};
use std::num::NonZeroU64;
use std::{
    fs,
    path::{Path, PathBuf},
};

fn point(sequence: u64, application: u64, administration: u64) -> Point {
    Point::new(
        Sequence::new(sequence).unwrap(),
        [sequence as u8; 32],
        DualFrontier::new(
            CommitSequence::new(application),
            AdministrationSequence::new(administration),
        ),
    )
}
fn fixture(
    incarnation: u64,
    epoch: u64,
    source_head: u64,
    applied: u64,
) -> (Request, Follower, Evidence) {
    let lineage = Lineage::new_with_catalog(
        DatabaseId::from_unix_milliseconds_and_random(1234, [1; 10]).unwrap(),
        incarnation,
        LeadershipEpochV1::new(epoch).unwrap(),
        AuthoritativeStateCatalogV2.digest(),
    )
    .unwrap();
    let target = ReplicationFollowerAuditTargetV1::new(
        lineage.database_id(),
        incarnation,
        lineage.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([2; 16]).unwrap(),
    )
    .unwrap();
    let fence_id =
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1234, [3; 10]).unwrap();
    let generation = Sequence::new(4).unwrap();
    let fence = Fence::new(
        AdministrationSequence::new(6).unwrap(),
        Timestamp::new(1234, 0).unwrap(),
        fence_id,
        RequestId::from_unix_milliseconds_and_random(1234, [4; 10]).unwrap(),
        AuditPrincipalV1::new(
            ActorId::new("fence-operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1234, [5; 10]).unwrap(),
            NonZeroU64::new(1).unwrap(),
        ),
        None,
        target,
        generation,
        point(9, source_head, 5),
    )
    .unwrap();
    let anchor = point(1, 0, 0);
    let evidence = Evidence::new(
        fence,
        point(8, applied, 4),
        History::new(lineage, anchor, point(10, source_head, 6), anchor).unwrap(),
    )
    .unwrap();
    let request = Request::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(1234, [6; 10]).unwrap(),
        fence_id,
        target,
        generation,
    );
    let follower = Follower::attached(lineage, evidence.applied(), Some(anchor)).unwrap();
    (request, follower, evidence)
}

fn attempted() -> Receipt {
    let (request, _, _) = fixture(2, 7, 11, 3);
    Receipt::attempted(
        request,
        RequestId::from_unix_milliseconds_and_random(1234, [7; 10]).unwrap(),
        AuditPrincipalV1::new(
            ActorId::new("promotion-operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1234, [8; 10]).unwrap(),
            NonZeroU64::new(3).unwrap(),
        ),
        Some(ApprovalId::new("promotion-approval").unwrap()),
        Timestamp::new(1234, 5).unwrap(),
    )
}
fn selected() -> Selection {
    let (request, follower, evidence) = fixture(2, 7, 11, 3);
    Selection::new(request, follower, evidence).unwrap()
}
fn offline() -> Receipt {
    let mut r = attempted();
    r.advance(Step::Phase(Phase::Draining)).unwrap();
    r.advance(Step::Phase(Phase::Offline)).unwrap();
    r
}
#[test]
fn promotion_receipt_codec_preserves_all_selected_evidence_and_ordered_history() {
    let initial = attempted();
    let mut receipt = offline();
    receipt.record_selection(selected()).unwrap();
    for phase in [
        Phase::CutoverPending,
        Phase::CutoverCommitted,
        Phase::Validated,
        Phase::Succeeded,
    ] {
        receipt.advance(Step::Phase(phase)).unwrap();
    }
    for expected in [initial, receipt] {
        let encoded = encode_promotion_receipt(&expected).unwrap();
        assert_eq!(decode_promotion_receipt(&encoded).unwrap(), expected);
        for end in 0..encoded.len() {
            assert!(decode_promotion_receipt(&encoded[..end]).is_err());
        }
        let mut damaged = encoded.clone();
        damaged[40] ^= 1;
        assert!(decode_promotion_receipt(&damaged).is_err());
    }
}

#[test]
fn promotion_ledger_holds_existing_owner_and_fences_ordinary_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("db.redb");
    let backup = dir.path().join("backups");
    let mut owner =
        super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
    let mut receipt = attempted();
    owner.persist_promotion_receipt(&receipt).unwrap();
    assert!(
        super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).is_err()
    );
    assert!(owner.reconcile().is_err());
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts(),
        &[receipt.clone()]
    );
    drop(owner);
    assert!(super::RedbMaintenanceStorage::open(&database, &backup).is_err());
    let mut owner =
        super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
    receipt
        .advance(Step::Denied(
            riffdb_storage_api::ReplicationPromotionFailureV1::AuthorizationDenied,
        ))
        .unwrap();
    owner.persist_promotion_receipt(&receipt).unwrap();
    owner.persist_promotion_receipt(&receipt).unwrap();
    assert!(owner.persist_promotion_receipt(&attempted()).is_err());
    drop(owner);
    let (owner, _) = super::RedbMaintenanceStorage::open(&database, &backup).unwrap();
    assert_eq!(owner.promotion_receipts().unwrap().receipts(), &[receipt]);
}

fn checksum(mut encoded: Vec<u8>) -> Vec<u8> {
    let end = encoded.len() - 32;
    let digest = Sha256::digest(&encoded[..end]);
    encoded[end..].copy_from_slice(&digest);
    encoded
}

#[test]
fn promotion_receipt_codec_refuses_resigned_unknown_tags_bounds_and_false_rpo() {
    let initial = encode_promotion_receipt(&attempted()).unwrap();
    let end = initial.len() - 32;
    for (offset, value) in [
        (b"RIFFDB-PROMOTION-RECEIPT\0".len() + 3, 2),
        (end - 3, 0),
        (end - 3, 17), // zero / excessive step count
        (end - 2, 0xff),
        (end - 1, 0xff), // step kind / phase
        (end - 1, 8),    // a fabricated terminal success without its required history
        (end - 4, 2),    // unknown selection-presence tag
    ] {
        let mut encoded = initial.clone();
        encoded[offset] = value;
        assert!(decode_promotion_receipt(&checksum(encoded)).is_err());
    }
    let mut extra = initial.clone();
    extra.insert(end, 0);
    assert!(decode_promotion_receipt(&checksum(extra)).is_err());
    assert!(
        decode_promotion_receipt(&vec![0; super::codec::MAX_PROMOTION_RECEIPT_BYTES + 1]).is_err()
    );
    let mut selected_receipt = offline();
    selected_receipt.record_selection(selected()).unwrap();
    let mut encoded = encode_promotion_receipt(&selected_receipt).unwrap();
    let rpo_last_byte = encoded.len() - 32 - (1 + selected_receipt.steps().len() * 2) - 1;
    encoded[rpo_last_byte] ^= 1;
    assert!(decode_promotion_receipt(&checksum(encoded)).is_err());
    for failure in [
        Failure::FenceUnavailable,
        Failure::FenceInvalid,
        Failure::DrainFailed,
        Failure::CounterExhausted,
        Failure::StorageUnavailable,
        Failure::ValidationFailed,
    ] {
        let mut receipt = attempted();
        receipt.advance(Step::FailedClosed(failure)).unwrap();
        assert_eq!(
            decode_promotion_receipt(&encode_promotion_receipt(&receipt).unwrap()).unwrap(),
            receipt
        );
    }
}

fn all_progress() -> Vec<Receipt> {
    let mut receipt = attempted();
    let mut values = vec![receipt.clone()];
    for phase in [Phase::Draining, Phase::Offline] {
        receipt.advance(Step::Phase(phase)).unwrap();
        values.push(receipt.clone());
    }
    receipt.record_selection(selected()).unwrap();
    values.push(receipt.clone());
    receipt.advance(Step::Phase(Phase::CutoverPending)).unwrap();
    values.push(receipt.clone());
    receipt
        .advance(Step::Uncertain(Failure::StorageUnavailable))
        .unwrap();
    values.push(receipt.clone());
    for phase in [Phase::CutoverCommitted, Phase::Validated, Phase::Succeeded] {
        receipt.advance(Step::Phase(phase)).unwrap();
        values.push(receipt.clone());
    }
    values
}

#[test]
fn promotion_receipt_v1_exact_compatibility_fixtures() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/replication");
    let mut denied = attempted();
    denied
        .advance(Step::Denied(Failure::AuthorizationDenied))
        .unwrap();
    for (name, receipt) in [
        ("attempted", attempted()),
        ("denied", denied),
        ("succeeded", all_progress().pop().unwrap()),
    ] {
        let encoded = encode_promotion_receipt(&receipt).unwrap();
        let hex = encoded
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            + "\n";
        let path = root.join(format!("promotion-receipt-v1-{name}.hex"));
        if std::env::var_os("RIFFDB_UPDATE_PROMOTION_RECEIPT_FIXTURES").is_some() {
            fs::write(&path, &hex).unwrap();
        }
        assert_eq!(hex, fs::read_to_string(&path).unwrap());
        assert_eq!(decode_promotion_receipt(&encoded).unwrap(), receipt);
    }
}

fn ledger_path(backup: &Path) -> PathBuf {
    backup.join(".maintenance/replication_promotion")
}
fn receipt_path(backup: &Path, receipt: &Receipt) -> PathBuf {
    ledger_path(backup).join(format!("{}.receipt-v1", receipt.request_id()))
}
fn temp_path(backup: &Path, receipt: &Receipt) -> PathBuf {
    ledger_path(backup).join(format!(".{}.receipt-v1.tmp", receipt.request_id()))
}
fn with_invocation(receipt: &Receipt, request: Request, marker: u8) -> Receipt {
    Receipt::from_canonical_parts(
        request,
        RequestId::from_unix_milliseconds_and_random(1234, [marker; 10]).unwrap(),
        receipt.principal().clone(),
        receipt.approval_id().cloned(),
        receipt.timestamp(),
        receipt.selection().cloned(),
        receipt.steps().to_vec(),
    )
    .unwrap()
}

#[test]
fn promotion_ledger_retries_freeze_selection_and_retain_conflicting_denials() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("db.redb");
    let backup = dir.path().join("backups");
    let mut owner =
        super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
    let mut first = all_progress()[3].clone();
    owner.persist_promotion_receipt(&attempted()).unwrap();
    owner.persist_promotion_receipt(&first).unwrap();
    first
        .advance(Step::FailedClosed(Failure::FenceUnavailable))
        .unwrap();
    owner.persist_promotion_receipt(&first).unwrap();
    let initial = with_invocation(&attempted(), first.request(), 9);
    owner.persist_promotion_receipt(&initial).unwrap();
    let (request, follower, evidence) = fixture(2, 7, 11, 4);
    let mut changed = with_invocation(&offline(), request, 9);
    changed
        .record_selection(Selection::new(request, follower, evidence).unwrap())
        .unwrap();
    assert!(owner.persist_promotion_receipt(&changed).is_err());
    let exact = with_invocation(&all_progress()[3], request, 9);
    owner.persist_promotion_receipt(&exact).unwrap();
    let different = Request::new(
        request.operation_id(),
        request.fence_operation_id(),
        request.target(),
        Sequence::new(5).unwrap(),
    );
    let mut denied = with_invocation(&attempted(), different, 10);
    assert!(owner.persist_promotion_receipt(&denied).is_err());
    denied
        .advance(Step::Denied(Failure::SelectionConflict))
        .unwrap();
    owner.persist_promotion_receipt(&denied).unwrap();
    let expected = owner.promotion_receipts().unwrap();
    assert_eq!(expected.receipts().len(), 3);
    assert_eq!(
        expected.selection_for(request.operation_id()),
        Some(&selected())
    );
    drop(owner);
    let owner =
        super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
    assert_eq!(owner.promotion_receipts().unwrap(), expected);
}

#[test]
fn promotion_ledger_validates_every_published_file_before_discarding_staging() {
    for damage in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("db.redb");
        let backup = dir.path().join("backups");
        let mut owner =
            super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
        let receipt = attempted();
        owner.persist_promotion_receipt(&receipt).unwrap();
        drop(owner);
        let path = receipt_path(&backup, &receipt);
        let temp = temp_path(&backup, &receipt);
        fs::write(&temp, b"partial unpublished bytes").unwrap();
        match damage {
            0 => fs::write(&path, b"corrupt published audit").unwrap(),
            1 => {
                fs::rename(&path, path.with_extension("receipt-v9")).unwrap();
            }
            2 => {
                fs::rename(&path, ledger_path(&backup).join("not-a-uuid.receipt-v1")).unwrap();
            }
            3 => {
                fs::create_dir(ledger_path(&backup).join("unexpected")).unwrap();
            }
            4 => fs::write(
                &path,
                vec![0; super::codec::MAX_PROMOTION_RECEIPT_BYTES + 1],
            )
            .unwrap(),
            _ => unreachable!(),
        }
        assert!(
            super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).is_err()
        );
        assert!(
            temp.exists(),
            "refused inventory must leave evidence untouched"
        );
        assert!(super::RedbMaintenanceStorage::open(&database, &backup).is_err());
    }
}

#[test]
fn promotion_ledger_discards_only_unpublished_partial_write_and_never_published_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("db.redb");
    let backup = dir.path().join("backups");
    let mut owner =
        super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
    owner.persist_promotion_receipt(&attempted()).unwrap();
    drop(owner);
    let before = fs::read(receipt_path(&backup, &attempted())).unwrap();
    fs::write(temp_path(&backup, &attempted()), b"torn replacement").unwrap();
    let mut owner =
        super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
    assert_eq!(
        fs::read(receipt_path(&backup, &attempted())).unwrap(),
        before
    );
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts(),
        &[attempted()]
    );
    assert!(!temp_path(&backup, &attempted()).exists());
    owner.persist_promotion_receipt(&all_progress()[1]).unwrap();
}

#[cfg(unix)]
#[test]
fn promotion_ledger_refuses_symlinks_and_directory_or_owner_substitution() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for damage in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("db.redb");
        let backup = dir.path().join("backups");
        let mut owner =
            super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
        owner.persist_promotion_receipt(&attempted()).unwrap();
        let outside = dir.path().join("outside");
        fs::write(&outside, b"untouched").unwrap();
        match damage {
            0 => {
                let path = receipt_path(&backup, &attempted());
                fs::remove_file(&path).unwrap();
                symlink(&outside, &path).unwrap();
            }
            1 => {
                fs::rename(ledger_path(&backup), dir.path().join("held-ledger")).unwrap();
                fs::create_dir(ledger_path(&backup)).unwrap();
            }
            2 => {
                let path = backup.join(".maintenance/owner.lock");
                fs::rename(&path, dir.path().join("held-lock")).unwrap();
                fs::write(path, b"").unwrap();
            }
            3 => fs::set_permissions(ledger_path(&backup), fs::Permissions::from_mode(0o755))
                .unwrap(),
            _ => unreachable!(),
        }
        assert!(owner.promotion_receipts().is_err());
        assert!(owner.persist_promotion_receipt(&all_progress()[1]).is_err());
        assert_eq!(fs::read(outside).unwrap(), b"untouched");
    }
}

#[cfg(unix)]
#[test]
fn promotion_ledger_crash_boundaries_preserve_exact_attempt_evidence() {
    use super::{RedbMaintenanceFailpoint as F, RedbMaintenanceTestController as Controller};
    use std::os::unix::process::ExitStatusExt;
    const CHILD: &str = "RIFFDB_PROMOTION_LEDGER_CRASH_ROOT";
    let edges = [
        F::BeforeReceiptFileSync,
        F::AfterReceiptFileSync,
        F::BeforeReceiptRename,
        F::AfterReceiptRename,
        F::AfterReceiptParentSync,
    ];
    let values = all_progress();
    let mut cases = values
        .iter()
        .enumerate()
        .map(|(index, receipt)| {
            (
                index.checked_sub(1).map(|prior| values[prior].clone()),
                receipt.clone(),
            )
        })
        .collect::<Vec<_>>();
    let mut denied = attempted();
    denied
        .advance(Step::Denied(Failure::AuthorizationDenied))
        .unwrap();
    cases.push((None, denied.clone()));
    cases.push((Some(attempted()), denied));
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let stage: usize = std::env::var("RIFFDB_PROMOTION_LEDGER_STAGE")
            .unwrap()
            .parse()
            .unwrap();
        let edge: usize = std::env::var("RIFFDB_PROMOTION_LEDGER_EDGE")
            .unwrap()
            .parse()
            .unwrap();
        let mut owner = super::RedbMaintenanceStorage::open_for_promotion_recovery(
            root.join("db.redb"),
            root.join("backups"),
        )
        .unwrap();
        owner.arm_promotion_test_controller(Controller::abort_at(edges[edge]));
        owner.persist_promotion_receipt(&cases[stage].1).unwrap();
        panic!("armed crash edge did not execute");
    }
    for _ in 0..2 {
        for (stage, (prior, target)) in cases.iter().enumerate() {
            for edge in 0..edges.len() {
                let dir = tempfile::tempdir().unwrap();
                let database = dir.path().join("db.redb");
                let backup = dir.path().join("backups");
                if let Some(prior) = prior {
                    let mut owner = super::RedbMaintenanceStorage::open_for_promotion_recovery(
                        &database, &backup,
                    )
                    .unwrap();
                    owner.persist_promotion_receipt(&values[0]).unwrap();
                    owner.persist_promotion_receipt(prior).unwrap();
                }
                // Bound disk evidence: expected process aborts must not create core dumps.
                let child = std::process::Command::new("sh")
                    .args(["-c", "ulimit -c 0; exec \"$@\"", "promotion-ledger-crash"])
                    .arg(std::env::current_exe().unwrap())
                    .args(["--exact", "maintenance::promotion_receipt_tests::promotion_ledger_crash_boundaries_preserve_exact_attempt_evidence", "--nocapture"])
                    .env(CHILD, dir.path()).env("RIFFDB_PROMOTION_LEDGER_STAGE", stage.to_string())
                    .env("RIFFDB_PROMOTION_LEDGER_EDGE", edge.to_string()).output().unwrap();
                assert_eq!(
                    child.status.signal(),
                    Some(6),
                    "{}",
                    String::from_utf8_lossy(&child.stderr)
                );
                let mut owner =
                    super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup)
                        .unwrap();
                let inventory = owner.promotion_receipts().unwrap();
                let expected = if edge >= 3 {
                    Some(target)
                } else {
                    prior.as_ref()
                };
                assert_eq!(inventory.receipts().first(), expected);
                assert_eq!(inventory.receipts().len(), usize::from(expected.is_some()));
                owner.persist_promotion_receipt(target).unwrap();
                owner.persist_promotion_receipt(target).unwrap();
                assert_eq!(
                    owner.promotion_receipts().unwrap().receipts(),
                    std::slice::from_ref(target)
                );
                assert_eq!(
                    owner.reconcile().is_err(),
                    !target.is_terminal() || target.phase() == Phase::Succeeded
                );
            }
        }
    }
}

#[test]
fn failed_selected_promotion_keeps_ordinary_startup_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("db.redb");
    let backup = dir.path().join("backups");
    let mut owner =
        super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
    owner.persist_promotion_receipt(&attempted()).unwrap();
    let mut receipt = all_progress()[3].clone();
    owner.persist_promotion_receipt(&receipt).unwrap();
    receipt
        .advance(Step::FailedClosed(Failure::StorageUnavailable))
        .unwrap();
    owner.persist_promotion_receipt(&receipt).unwrap();
    assert!(
        owner.reconcile().is_err(),
        "a frozen selection survives failure"
    );
    drop(owner);
    assert!(super::RedbMaintenanceStorage::open(&database, &backup).is_err());
    let mut owner =
        super::RedbMaintenanceStorage::open_for_promotion_recovery(&database, &backup).unwrap();
    assert_eq!(owner.promotion_receipts().unwrap().receipts(), &[receipt]);
    assert!(owner.reconcile_for_startup().is_err());
}
